#!/usr/bin/env python3
"""
================================================================================
 OI MONITOR BOT - Binance Futures USDT-M (Open Interest Alert System)
================================================================================
Monitorea el Open Interest (OI) y volumen de los pares USDT-M de Binance
Futuros usando SOLO endpoints públicos (sin API Keys) y envía alertas a
Telegram cuando detecta variaciones bruscas de OI.

Autor: Claude (Anthropic) - Script generado para uso educativo/personal.
Uso bajo tu propia responsabilidad. No es asesoría financiera.
================================================================================
"""

import os
import time
import logging
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
class Config:
    # --- Binance ---
    BINANCE_BASE = "https://fapi1.binance.com"

    # --- Telegram (usa variables de entorno en producción, nunca hardcodees
    #     tu token/chat_id si vas a subir el código a un repo público) ---
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "PON_TU_TOKEN_AQUI")
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "PON_TU_CHAT_ID_AQUI")

    # --- Filtrado de pares ---
    TOP_N_PAIRS = 200
    MIN_VOLUME_USD = 10_000_000  # Descarta pares con menos de $10M de volumen 24h

    # --- Intervalos (segundos) ---
    SYMBOL_REFRESH_INTERVAL = 6 * 60 * 60   # Refrescar lista de pares cada 6h
    OI_CHECK_INTERVAL = 5 * 60              # Revisar OI cada 5 minutos

    # --- Umbrales de alerta ---
    OI_THRESHOLD_5M = 2.5   # % variación en 5 minutos
    OI_THRESHOLD_1H = 6.0   # % variación en 1 hora

    # --- Anti-spam ---
    ALERT_COOLDOWN = 15 * 60  # 15 minutos por par

    # --- Historial en memoria ---
    # Guardamos ~80 minutos de histórico (16 muestras de 5 min) para poder
    # calcular con margen la ventana de 1 hora aunque haya algún retraso.
    HISTORY_MAXLEN = 16

    # --- Red / concurrencia ---
    MAX_WORKERS = 10          # hilos concurrentes para pedir OI por símbolo
    REQUEST_TIMEOUT = 10
    MAX_RETRIES = 3
    RETRY_BACKOFF_BASE = 2    # segundos, backoff exponencial

    # --- Servidor "keep-alive" (necesario en Render free tier) ---
    ENABLE_HEALTH_SERVER = True


# ==============================================================================
# LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("oi_monitor")


# ==============================================================================
# ESTADO GLOBAL (en memoria)
# ==============================================================================
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"})

symbols_lock = threading.Lock()
active_symbols = []          # lista de pares filtrados (top N por volumen)

oi_history = {}               # symbol -> deque[(timestamp, oi_usdt)]
volume_history = {}           # symbol -> deque[(timestamp, quote_volume)]
last_alert_time = {}          # symbol -> timestamp del último alert enviado


# ==============================================================================
# HELPERS DE RED CON REINTENTOS
# ==============================================================================
def http_get_json(url, params=None):
    """GET con reintentos y backoff exponencial. Devuelve None si falla todo."""
    for attempt in range(1, Config.MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=Config.REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429 or resp.status_code == 418:
                # Rate limit / IP ban temporal de Binance
                wait = Config.RETRY_BACKOFF_BASE ** attempt
                log.warning(
                    "Rate limit de Binance (status %s). Esperando %ss...",
                    resp.status_code, wait,
                )
                time.sleep(wait)
            else:
                log.warning(
                    "Respuesta inesperada de Binance (status %s) en %s",
                    resp.status_code, url,
                )
                time.sleep(Config.RETRY_BACKOFF_BASE)
        except requests.exceptions.RequestException as e:
            wait = Config.RETRY_BACKOFF_BASE ** attempt
            log.warning(
                "Error de red (%s) llamando %s [intento %s/%s]. Reintentando en %ss...",
                e, url, attempt, Config.MAX_RETRIES, wait,
            )
            time.sleep(wait)
    log.error("Fallaron todos los reintentos para %s", url)
    return None


# ==============================================================================
# TELEGRAM
# ==============================================================================
def send_telegram_message(text):
    if "PON_TU" in Config.TELEGRAM_TOKEN or "PON_TU" in Config.TELEGRAM_CHAT_ID:
        log.warning("Telegram no configurado (token/chat_id por defecto). Alerta no enviada.")
        return False

    url = f"https://api.telegram.org/bot{Config.TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": Config.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        resp = session.post(url, json=payload, timeout=Config.REQUEST_TIMEOUT)
        if resp.status_code != 200:
            log.error("Error enviando a Telegram: %s - %s", resp.status_code, resp.text)
            return False
        return True
    except requests.exceptions.RequestException as e:
        log.error("Excepción enviando a Telegram: %s", e)
        return False


def format_usd(value):
    """Formatea números grandes en formato legible: 1.2M, 850.3K, etc."""
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.2f}K"
    return f"${value:.2f}"


def send_oi_alert(symbol, change_5m, change_1h, oi_usdt, quote_volume, vol_ratio):
    base = symbol.replace("USDT", "")
    chart_url = f"https://www.binance.com/en/futures/{symbol}"

    def fmt_change(v):
        if v is None:
            return "N/A"
        sign = "🟢+" if v >= 0 else "🔴"
        return f"{sign}{v:.2f}%"

    msg = (
        f"🚨 *ALERTA DE OPEN INTEREST*\n\n"
        f"*Par:* #{symbol}\n"
        f"*OI 5m:* {fmt_change(change_5m)}\n"
        f"*OI 1h:* {fmt_change(change_1h)}\n"
        f"*OI Actual:* {format_usd(oi_usdt)}\n"
        f"*Volumen 24h:* {format_usd(quote_volume)}\n"
        f"*Ratio Vol/Promedio:* {vol_ratio:.2f}x\n\n"
        f"📊 [Ver gráfico en Binance Futuros]({chart_url})"
    )
    if send_telegram_message(msg):
        log.info("Alerta enviada -> %s | 5m:%s 1h:%s", symbol, change_5m, change_1h)


# ==============================================================================
# BINANCE: FILTRADO DINÁMICO DE PARES
# ==============================================================================
def fetch_exchange_info():
    """Devuelve el set de símbolos USDT-M PERPETUAL actualmente TRADING."""
    data = http_get_json(f"{Config.BINANCE_BASE}/fapi/v1/exchangeInfo")
    if not data:
        return None
    symbols = set()
    for s in data.get("symbols", []):
        if (
            s.get("quoteAsset") == "USDT"
            and s.get("contractType") == "PERPETUAL"
            and s.get("status") == "TRADING"
        ):
            symbols.add(s["symbol"])
    return symbols


def fetch_24h_tickers():
    """Devuelve dict symbol -> {price, quote_volume} de TODOS los pares."""
    data = http_get_json(f"{Config.BINANCE_BASE}/fapi/v1/ticker/24hr")
    if not data:
        return None
    result = {}
    for item in data:
        try:
            result[item["symbol"]] = {
                "price": float(item["lastPrice"]),
                "quote_volume": float(item["quoteVolume"]),
            }
        except (KeyError, ValueError, TypeError):
            continue
    return result


def refresh_symbols():
    """Actualiza la lista global de pares filtrados (Top 200 por volumen, >$10M)."""
    log.info("Refrescando lista de pares (exchangeInfo + volumen 24h)...")

    valid_symbols = fetch_exchange_info()
    tickers = fetch_24h_tickers()

    if not valid_symbols or not tickers:
        log.error("No se pudo refrescar la lista de pares. Se mantiene la anterior.")
        return

    candidates = []
    for symbol in valid_symbols:
        t = tickers.get(symbol)
        if not t:
            continue
        if t["quote_volume"] >= Config.MIN_VOLUME_USD:
            candidates.append((symbol, t["quote_volume"]))

    candidates.sort(key=lambda x: x[1], reverse=True)
    top_symbols = [s for s, _ in candidates[: Config.TOP_N_PAIRS]]

    with symbols_lock:
        global active_symbols
        active_symbols = top_symbols

    log.info(
        "Lista de pares actualizada: %s pares activos (de %s candidatos con volumen >= %s).",
        len(top_symbols), len(candidates), format_usd(Config.MIN_VOLUME_USD),
    )


# ==============================================================================
# BINANCE: OPEN INTEREST
# ==============================================================================
def fetch_open_interest(symbol):
    """Devuelve el OI actual en contratos (base asset) para un símbolo."""
    data = http_get_json(f"{Config.BINANCE_BASE}/fapi/v1/openInterest", params={"symbol": symbol})
    if not data:
        return None
    try:
        return float(data["openInterest"])
    except (KeyError, ValueError, TypeError):
        return None


def find_closest_sample(history, target_seconds_ago, tolerance=150):
    """Busca en el histórico la muestra más cercana a 'target_seconds_ago'
    dentro de una tolerancia (en segundos). Devuelve (ts, valor) o None."""
    now = time.time()
    target_ts = now - target_seconds_ago
    best = None
    best_diff = None
    for ts, value in history:
        diff = abs(ts - target_ts)
        if diff <= tolerance and (best_diff is None or diff < best_diff):
            best = (ts, value)
            best_diff = diff
    return best


def process_symbol(symbol, price, quote_volume):
    """Obtiene el OI de un símbolo, calcula variaciones y dispara alerta si aplica."""
    oi_contracts = fetch_open_interest(symbol)
    if oi_contracts is None:
        return

    oi_usdt = oi_contracts * price
    now = time.time()

    hist = oi_history.setdefault(symbol, deque(maxlen=Config.HISTORY_MAXLEN))
    vol_hist = volume_history.setdefault(symbol, deque(maxlen=Config.HISTORY_MAXLEN))

    # --- Variación 5 minutos (comparado con la muestra inmediatamente anterior) ---
    change_5m = None
    sample_5m = find_closest_sample(hist, target_seconds_ago=300, tolerance=150)
    if sample_5m and sample_5m[1] > 0:
        change_5m = (oi_usdt - sample_5m[1]) / sample_5m[1] * 100

    # --- Variación 1 hora ---
    change_1h = None
    sample_1h = find_closest_sample(hist, target_seconds_ago=3600, tolerance=450)
    if sample_1h and sample_1h[1] > 0:
        change_1h = (oi_usdt - sample_1h[1]) / sample_1h[1] * 100

    # --- Ratio de volumen vs promedio histórico ---
    if vol_hist:
        avg_vol = sum(v for _, v in vol_hist) / len(vol_hist)
        vol_ratio = (quote_volume / avg_vol) if avg_vol > 0 else 1.0
    else:
        vol_ratio = 1.0

    # Guardar muestras nuevas (después de calcular, para no comparar contra sí mismo)
    hist.append((now, oi_usdt))
    vol_hist.append((now, quote_volume))

    # --- Evaluar condiciones de alerta ---
    triggered = (
        (change_5m is not None and abs(change_5m) >= Config.OI_THRESHOLD_5M)
        or (change_1h is not None and abs(change_1h) >= Config.OI_THRESHOLD_1H)
    )

    if triggered:
        last_sent = last_alert_time.get(symbol, 0)
        if now - last_sent >= Config.ALERT_COOLDOWN:
            send_oi_alert(symbol, change_5m, change_1h, oi_usdt, quote_volume, vol_ratio)
            last_alert_time[symbol] = now
        else:
            log.debug("Alerta de %s en cooldown (%.0fs restantes).",
                      symbol, Config.ALERT_COOLDOWN - (now - last_sent))


def check_oi_cycle():
    """Ciclo completo: obtiene tickers 24h + OI de cada par filtrado (en paralelo)."""
    with symbols_lock:
        symbols_snapshot = list(active_symbols)

    if not symbols_snapshot:
        log.warning("No hay pares activos todavía. Saltando ciclo de OI.")
        return

    tickers = fetch_24h_tickers()
    if not tickers:
        log.error("No se pudo obtener ticker/24hr. Saltando ciclo de OI.")
        return

    start = time.time()
    processed = 0

    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        futures = {}
        for symbol in symbols_snapshot:
            t = tickers.get(symbol)
            if not t:
                continue
            futures[executor.submit(process_symbol, symbol, t["price"], t["quote_volume"])] = symbol

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                future.result()
                processed += 1
            except Exception as e:
                log.exception("Error procesando %s: %s", symbol, e)

    elapsed = time.time() - start
    log.info("Ciclo de OI completado: %s/%s pares procesados en %.1fs.",
              processed, len(symbols_snapshot), elapsed)


# ==============================================================================
# SERVIDOR HTTP MÍNIMO (para plataformas tipo Render que requieren un puerto
# abierto en servicios "Web Service" y para poder hacer keep-alive con un
# servicio externo de ping como UptimeRobot / cron-job.org)
# ==============================================================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        with symbols_lock:
            n = len(active_symbols)
        self.wfile.write(f"OK - OI Monitor Bot activo. Pares monitoreados: {n}".encode())

    def log_message(self, format, *args):
        pass  # silenciar logs de acceso HTTP


def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    log.info("Servidor de salud escuchando en el puerto %s (para keep-alive).", port)
    server.serve_forever()


# ==============================================================================
# LOOP PRINCIPAL
# ==============================================================================
def main():
    log.info("=== Iniciando OI Monitor Bot para Binance Futuros ===")
    log.info(
        "Config: TOP_N=%s | MIN_VOL=%s | Umbrales 5m/1h = %.1f%% / %.1f%% | Cooldown=%smin",
        Config.TOP_N_PAIRS, format_usd(Config.MIN_VOLUME_USD),
        Config.OI_THRESHOLD_5M, Config.OI_THRESHOLD_1H, Config.ALERT_COOLDOWN // 60,
    )

    if Config.ENABLE_HEALTH_SERVER:
        threading.Thread(target=start_health_server, daemon=True).start()

    send_telegram_message("✅ *OI Monitor Bot iniciado correctamente.*")

    refresh_symbols()
    last_symbol_refresh = time.time()
    last_oi_check = 0  # forzar primera ejecución inmediata

    while True:
        try:
            now = time.time()

            if now - last_symbol_refresh >= Config.SYMBOL_REFRESH_INTERVAL:
                refresh_symbols()
                last_symbol_refresh = now

            if now - last_oi_check >= Config.OI_CHECK_INTERVAL:
                check_oi_cycle()
                last_oi_check = time.time()

        except Exception as e:
            log.exception("Error inesperado en el loop principal: %s", e)
            # No detenemos el bot: seguimos intentando en el próximo ciclo.

        time.sleep(10)  # chequeo ligero cada 10s para no gastar CPU/red


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Bot detenido manualmente.")
