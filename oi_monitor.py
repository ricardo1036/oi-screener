#!/usr/bin/env python3
"""
================================================================================
 BINANCE FUTURES MONITOR BOT - OI + Volumen (WebSocket + REST híbrido)
================================================================================
Arquitectura (2 capas):

  CAPA 1 — ¿Qué monedas vigilo? (universo dinámico)
    Se construye a partir de CoinMarketCap (listings/latest): se filtran las
    monedas dentro de una BANDA de capitalización de mercado configurable
    (ej. $50M-$500M), se cruzan contra los pares realmente disponibles en
    Binance Futuros USDT-M, y se exige liquidez mínima (piso absoluto de
    volumen 24h + ratio Volumen/Market Cap). El match símbolo->proyecto es
    CONSERVADOR: si un ticker es ambiguo (varios proyectos lo comparten), se
    descarta en vez de arriesgarse a monitorear el proyecto equivocado.
    Este universo se refresca en horas ANCLADAS de reloj (UTC), no por
    intervalo relativo al arranque del bot, para garantizar que esté fresco
    antes de ventanas horarias específicas (ej. antes de la apertura de NY).

  CAPA 2 — ¿Está pasando algo interesante AHORA en esas monedas?
    - WebSocket (wss://fstream.binance.com) -> velas de 1 minuto en tiempo
      real para detectar spikes de volumen y RVOL de corto plazo, sin gastar
      peso de la API REST de Binance.
    - REST pública de Binance -> Open Interest, consultado en bucle
      secuencial con pausa fija entre requests (anti rate-limit).
    - Telegram con DOS canales: uno urgente (con sonido) para spikes de
      volumen explosivos, y uno informativo (silencioso) para OI y RVOL.

No usa API Keys privadas de Binance: solo endpoints públicos de Futuros.
Sí requiere una API Key gratuita de CoinMarketCap (CMC_API_KEY) para poder
construir el universo por capitalización de mercado.
No es asesoría financiera. Uso bajo tu propia responsabilidad.
================================================================================
"""

import os
import json
import time
import logging
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
import websocket  # pip install websocket-client


# ==============================================================================
# ====================  BLOQUE DE CONFIGURACIÓN (EDITAR AQUÍ)  ================
# ==============================================================================
class Config:
    # ---------------------------------------------------------------------
    # TELEGRAM — credenciales del bot y de los DOS canales de alertas.
    # Se recomienda definirlas como variables de entorno en el servidor
    # (Render, etc.) en vez de escribirlas directamente aquí.
    # ---------------------------------------------------------------------
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "PON_TU_TOKEN_AQUI")

    # Canal URGENTE: spikes de volumen explosivos en velas de 1m. Suena.
    CHAT_ID_URGENTE = os.environ.get("CHAT_ID_URGENTE", "PON_TU_CHAT_ID_URGENTE")

    # Canal INFO: monitor general de Open Interest y RVOL. Silencioso.
    CHAT_ID_INFO = os.environ.get("CHAT_ID_INFO", "PON_TU_CHAT_ID_INFO")

    # ---------------------------------------------------------------------
    # BINANCE — endpoints base (públicos, sin API Key)
    # ---------------------------------------------------------------------
    REST_BASE = "https://fapi.binance.com"
    WS_BASE = "wss://fstream.binance.com/stream"

    # ---------------------------------------------------------------------
    # COINMARKETCAP — requiere API Key gratuita (pro-api, plan Basic).
    # Regístrate en https://coinmarketcap.com/api/ y pon la key como
    # variable de entorno CMC_API_KEY en tu servidor.
    # ---------------------------------------------------------------------
    CMC_API_KEY = os.environ.get("CMC_API_KEY", "")
    CMC_BASE = "https://pro-api.coinmarketcap.com"

    # ---------------------------------------------------------------------
    # CAPA 1 — UNIVERSO POR BANDA DE CAPITALIZACIÓN DE MERCADO
    # ---------------------------------------------------------------------
    # Banda de market cap a monitorear (ajustable sin tocar lógica).
    MIN_MARKET_CAP_USD = 50_000_000
    MAX_MARKET_CAP_USD = 500_000_000

    # Filtro de liquidez en Binance Futuros (red de seguridad, no el criterio
    # principal): piso absoluto en USD + ratio mínimo Volumen24h/MarketCap.
    MIN_BINANCE_VOLUME_USD = 3_000_000
    MIN_VOLUME_TO_MCAP_RATIO = 0.05   # 5%: al menos ese % del cap se "rota" en 24h

    # Tamaño del universo descargado de CoinMarketCap antes de filtrar por
    # banda (grande a propósito: la banda puede caer en cualquier ranking).
    MARKET_UNIVERSE_FETCH_LIMIT = 5000

    # Horas de reloj UTC en las que se refresca el universo (qué monedas se
    # monitorean). Ancladas a reloj -no a "cada N horas desde el arranque"-
    # para garantizar frescura antes de ventanas horarias específicas.
    # Por defecto: cada 6h, con una justo ~2.5-3.5h antes de la apertura de
    # Nueva York durante todo el año (11:00 UTC), sin necesidad de ajustarla
    # por cambios de horario (ni de Chile ni de EE.UU.).
    UNIVERSE_REFRESH_HOURS_UTC = [5, 11, 17, 23]

    # Cache del VALOR de Market Cap mostrado en las alertas (no cambia qué
    # monedas se monitorean, solo mantiene el número fresco). Este sí puede
    # ser un intervalo simple, no necesita anclarse a horas de reloj.
    MARKET_CAP_DISPLAY_REFRESH_INTERVAL = 30 * 60

    # ---------------------------------------------------------------------
    # OPEN INTEREST — frecuencia de consulta y pausa anti rate-limit
    # ---------------------------------------------------------------------
    OI_CHECK_INTERVAL = 5 * 60        # cada cuánto se recorren TODOS los pares
    OI_REQUEST_PAUSE = 0.08           # pausa entre cada request de OI (segundos)
    OI_HISTORY_MAXLEN = 300           # ~25h de histórico a razón de 1 muestra/5min

    # Umbrales de variación de OI, evaluados de forma INDEPENDIENTE por
    # temporalidad. Solo se dispara ante SUBIDAS (acumulación), nunca caídas.
    OI_THRESHOLD_5M = 3.0
    OI_THRESHOLD_15M = 5.0
    OI_THRESHOLD_30M = 7.0
    OI_THRESHOLD_1H = 10.0
    OI_THRESHOLD_4H = 15.0
    OI_THRESHOLD_24H = 20.0

    # ---------------------------------------------------------------------
    # DETECTOR DE SPIKES DE VOLUMEN EN VELAS DE 1 MINUTO (canal urgente)
    # ---------------------------------------------------------------------
    VOL_SPIKE_THRESHOLD_PCT = 150     # % de exceso sobre el promedio para alertar
    LOOKBACK_BARS_1M = 10             # nº de velas previas usadas como referencia

    # ---------------------------------------------------------------------
    # DETECTOR DE RVOL — Volumen Relativo de CORTO PLAZO (canal info)
    # ---------------------------------------------------------------------
    # A propósito NO es RVOL "de sesión" (hoy vs. misma hora de ayer): esa
    # variante exige guardar varios días de historial minuto a minuto y se
    # resetea en cada reinicio (el bot no tiene almacenamiento persistente).
    # Esta versión de corto plazo detecta explosiones de volumen frente a
    # los minutos inmediatamente anteriores, que es justo lo que se busca
    # para cazar arranques de tendencia en scalping de 1 minuto.
    RVOL_WINDOW_BARS = 5              # tamaño de la ventana acumulada (minutos)
    RVOL_THRESHOLD = 2.0              # dispara si RVOL >= 2.0x (200%) lo esperado

    # ---------------------------------------------------------------------
    # ANTI-SPAM (cooldowns independientes por tipo de alerta)
    # ---------------------------------------------------------------------
    SPIKE_ALERT_COOLDOWN = 15 * 60
    OI_ALERT_COOLDOWN = 15 * 60
    RVOL_ALERT_COOLDOWN = 15 * 60

    # ---------------------------------------------------------------------
    # RED / ROBUSTEZ
    # ---------------------------------------------------------------------
    REQUEST_TIMEOUT = 10
    MAX_RETRIES = 3
    RETRY_BACKOFF_BASE = 2

    # Servidor HTTP mínimo (necesario para plataformas tipo Render que
    # exigen un puerto abierto en servicios "Web Service" + keep-alive)
    ENABLE_HEALTH_SERVER = True


# Temporalidades de OI a evaluar: nombre -> (segundos, umbral %, tolerancia seg.)
OI_TIMEFRAMES = {
    "5m":  (300,   Config.OI_THRESHOLD_5M,  90),
    "15m": (900,   Config.OI_THRESHOLD_15M, 180),
    "30m": (1800,  Config.OI_THRESHOLD_30M, 300),
    "1h":  (3600,  Config.OI_THRESHOLD_1H,  450),
    "4h":  (14400, Config.OI_THRESHOLD_4H,  900),
    "24h": (86400, Config.OI_THRESHOLD_24H, 1800),
}


# ==============================================================================
# LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("futures_monitor")
logging.getLogger("websocket").setLevel(logging.WARNING)


# ==============================================================================
# ESTADO GLOBAL EN MEMORIA
# ==============================================================================
http_session = requests.Session()
http_session.headers.update({"User-Agent": "futures-monitor-bot/3.0"})

symbols_lock = threading.Lock()
active_symbols = []                 # universo filtrado (banda de market cap + liquidez)

oi_history = {}                     # symbol -> deque[(ts, oi_usdt)]
kline_volume_history = {}           # symbol -> deque[quote_volume de velas cerradas]
kline_lock = threading.Lock()

last_spike_alert = {}               # symbol -> ts último alert de spike
last_oi_alert = {}                  # symbol -> ts último alert de OI
last_rvol_alert = {}                # symbol -> ts último alert de RVOL

current_ws_app = None               # referencia al WebSocketApp activo
ws_restart_lock = threading.Lock()

market_cap_cache = {}               # binance_symbol -> market cap USD (para mostrar en alertas)
symbol_cmc_id = {}                  # binance_symbol -> id de CoinMarketCap (para refrescos baratos/sin ambigüedad)
market_cap_lock = threading.Lock()


# ==============================================================================
# HELPERS GENERALES
# ==============================================================================
def format_usd(value):
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.2f}K"
    return f"${value:.2f}"


def tradingview_link(symbol):
    """Enlace directo a TradingView para el perpetuo de Binance."""
    return f"https://es.tradingview.com/chart/?symbol=BINANCE:{symbol}.P"


def base_asset(symbol):
    """Convierte un símbolo de futuros (ej. 'QNTUSDT') en su activo base ('QNT')."""
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def http_get_json(url, params=None, headers=None):
    """GET con reintentos y backoff exponencial. Devuelve None si falla todo."""
    for attempt in range(1, Config.MAX_RETRIES + 1):
        try:
            resp = http_session.get(url, params=params, headers=headers, timeout=Config.REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 418):
                wait = Config.RETRY_BACKOFF_BASE ** attempt
                log.warning("Rate limit (status %s) en %s. Esperando %ss...", resp.status_code, url, wait)
                time.sleep(wait)
            else:
                log.warning("Respuesta inesperada (status %s) en %s", resp.status_code, url)
                time.sleep(Config.RETRY_BACKOFF_BASE)
        except requests.exceptions.RequestException as e:
            wait = Config.RETRY_BACKOFF_BASE ** attempt
            log.warning("Error de red (%s) en %s [intento %s/%s]. Reintentando en %ss...",
                        e, url, attempt, Config.MAX_RETRIES, wait)
            time.sleep(wait)
    log.error("Fallaron todos los reintentos para %s", url)
    return None


def find_closest_sample(history, target_seconds_ago, tolerance):
    """Busca en el histórico la muestra más cercana a 'target_seconds_ago'
    dentro de una tolerancia (segundos). Devuelve (ts, valor) o None."""
    now = time.time()
    target_ts = now - target_seconds_ago
    best, best_diff = None, None
    for ts, value in history:
        diff = abs(ts - target_ts)
        if diff <= tolerance and (best_diff is None or diff < best_diff):
            best, best_diff = (ts, value), diff
    return best


# ==============================================================================
# TELEGRAM (doble canal)
# ==============================================================================
def send_telegram(text, chat_id, silent=False):
    if "PON_TU" in Config.TELEGRAM_TOKEN or "PON_TU" in str(chat_id):
        log.warning("Telegram no configurado (token/chat_id por defecto). Mensaje no enviado.")
        return False

    url = f"https://api.telegram.org/bot{Config.TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
        "disable_notification": silent,
    }
    try:
        resp = http_session.post(url, json=payload, timeout=Config.REQUEST_TIMEOUT)
        if resp.status_code != 200:
            log.error("Error enviando a Telegram: %s - %s", resp.status_code, resp.text)
            return False
        return True
    except requests.exceptions.RequestException as e:
        log.error("Excepción enviando a Telegram: %s", e)
        return False


def send_urgent_alert(text):
    return send_telegram(text, Config.CHAT_ID_URGENTE, silent=False)


def send_info_alert(text):
    return send_telegram(text, Config.CHAT_ID_INFO, silent=True)


# ==============================================================================
# BINANCE REST: exchangeInfo + tickers 24h (insumos para el universo y el OI)
# ==============================================================================
def fetch_exchange_info():
    """Devuelve el set de símbolos USDT-M PERPETUAL actualmente TRADING en Binance."""
    data = http_get_json(f"{Config.REST_BASE}/fapi/v1/exchangeInfo")
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
    data = http_get_json(f"{Config.REST_BASE}/fapi/v1/ticker/24hr")
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


# ==============================================================================
# COINMARKETCAP: universo por capitalización + cache de Market Cap
# ==============================================================================
def cmc_headers():
    return {"X-CMC_PRO_API_KEY": Config.CMC_API_KEY, "Accept": "application/json"}


def fetch_cmc_listings(limit):
    """Descarga un lote grande de CoinMarketCap ordenado por market cap, para
    poder filtrar después por banda (la banda puede caer en cualquier rango
    de ranking, no necesariamente en el Top 200)."""
    if not Config.CMC_API_KEY:
        log.error("CMC_API_KEY no configurada. No se puede refrescar el universo.")
        return None
    data = http_get_json(
        f"{Config.CMC_BASE}/v1/cryptocurrency/listings/latest",
        params={"start": 1, "limit": limit, "convert": "USD"},
        headers=cmc_headers(),
    )
    if not data:
        return None
    return data.get("data")


def fetch_cmc_quotes_by_id(ids):
    """Refresco barato y SIN ambigüedad: consulta por ID de CoinMarketCap
    (único por definición), no por símbolo."""
    if not ids or not Config.CMC_API_KEY:
        return None
    data = http_get_json(
        f"{Config.CMC_BASE}/v1/cryptocurrency/quotes/latest",
        params={"id": ",".join(str(i) for i in ids), "convert": "USD"},
        headers=cmc_headers(),
    )
    if not data:
        return None
    return data.get("data")


def refresh_universe():
    """CAPA 1: reconstruye qué monedas se monitorean.
    1) Descarga un lote grande de CoinMarketCap.
    2) Descarta tickers ambiguos (varios proyectos con el mismo símbolo).
    3) Filtra por banda de market cap.
    4) Cruza contra los pares realmente activos en Binance Futuros USDT-M.
    5) Exige liquidez mínima (piso absoluto + ratio Volumen/MarketCap).
    Si algo falla, se mantiene el universo anterior (nunca se vacía por un
    error puntual de red)."""
    log.info("Refrescando universo por capitalización de mercado (CoinMarketCap)...")

    listings = fetch_cmc_listings(Config.MARKET_UNIVERSE_FETCH_LIMIT)
    valid_binance_symbols = fetch_exchange_info()
    tickers = fetch_24h_tickers()

    if not listings or not valid_binance_symbols or not tickers:
        log.error("No se pudo refrescar el universo (CMC/Binance no respondió). Se mantiene el anterior.")
        return

    # --- Agrupar por símbolo para detectar ambigüedad ---
    symbol_entries = {}
    for item in listings:
        try:
            sym = item["symbol"].upper()
            cmc_id = item["id"]
            mcap = item["quote"]["USD"]["market_cap"]
            if mcap:
                symbol_entries.setdefault(sym, []).append((cmc_id, mcap))
        except (KeyError, TypeError):
            continue

    ambiguous_count = sum(1 for entries in symbol_entries.values() if len(entries) > 1)

    # --- Solo tickers únicos y dentro de la banda de capitalización ---
    band_candidates = {}
    for sym, entries in symbol_entries.items():
        if len(entries) != 1:
            continue  # ambiguo: se descarta (match conservador)
        cmc_id, mcap = entries[0]
        if Config.MIN_MARKET_CAP_USD <= mcap <= Config.MAX_MARKET_CAP_USD:
            band_candidates[sym] = (cmc_id, mcap)

    # --- Intersección con Binance Futuros + filtro de liquidez ---
    new_symbols = []
    new_market_caps = {}
    new_cmc_ids = {}

    for binance_symbol in valid_binance_symbols:
        base = base_asset(binance_symbol)
        candidate = band_candidates.get(base)
        if not candidate:
            continue
        cmc_id, mcap = candidate

        t = tickers.get(binance_symbol)
        if not t:
            continue
        binance_volume = t["quote_volume"]

        if binance_volume < Config.MIN_BINANCE_VOLUME_USD:
            continue

        ratio = (binance_volume / mcap) if mcap > 0 else 0.0
        if ratio < Config.MIN_VOLUME_TO_MCAP_RATIO:
            continue

        new_symbols.append(binance_symbol)
        new_market_caps[binance_symbol] = mcap
        new_cmc_ids[binance_symbol] = cmc_id

    with symbols_lock:
        global active_symbols
        changed = set(new_symbols) != set(active_symbols)
        active_symbols = new_symbols

    with market_cap_lock:
        global market_cap_cache, symbol_cmc_id
        market_cap_cache = new_market_caps
        symbol_cmc_id = new_cmc_ids

    log.info(
        "Universo actualizado: %s pares | banda $%s-$%s | ratio vol/mcap >= %.0f%% | "
        "piso vol >= %s | %s tickers ambiguos descartados.",
        len(new_symbols), format_usd(Config.MIN_MARKET_CAP_USD), format_usd(Config.MAX_MARKET_CAP_USD),
        Config.MIN_VOLUME_TO_MCAP_RATIO * 100, format_usd(Config.MIN_BINANCE_VOLUME_USD), ambiguous_count,
    )

    if changed:
        restart_websocket()


def refresh_market_cap_display():
    """Refresca SOLO el valor de Market Cap mostrado en las tarjetas de
    Telegram, para los símbolos ya activos. No cambia qué monedas se
    monitorean (eso lo hace refresh_universe). Usa ID de CMC: sin ambigüedad
    posible porque el ID es único por definición."""
    with market_cap_lock:
        ids_by_symbol = dict(symbol_cmc_id)

    if not ids_by_symbol:
        return

    unique_ids = sorted(set(ids_by_symbol.values()))
    data = fetch_cmc_quotes_by_id(unique_ids)
    if not data:
        log.warning("No se pudo refrescar el cache de Market Cap. Se mantienen los valores anteriores.")
        return

    updated = {}
    for symbol, cmc_id in ids_by_symbol.items():
        entry = data.get(str(cmc_id))
        if not entry:
            continue
        try:
            updated[symbol] = entry["quote"]["USD"]["market_cap"]
        except (KeyError, TypeError):
            continue

    with market_cap_lock:
        market_cap_cache.update(updated)

    log.info("Cache de Market Cap (display) actualizado: %s símbolos.", len(updated))


def get_market_cap(symbol):
    with market_cap_lock:
        return market_cap_cache.get(symbol)


def run_market_cap_display_refresher():
    """Hilo de fondo: refresca el valor de Market Cap cada MARKET_CAP_DISPLAY_REFRESH_INTERVAL."""
    while True:
        try:
            refresh_market_cap_display()
        except Exception as e:
            log.exception("Error refrescando Market Cap (display): %s", e)
        time.sleep(Config.MARKET_CAP_DISPLAY_REFRESH_INTERVAL)


def next_universe_refresh_after(dt):
    """Devuelve el próximo datetime UTC (de UNIVERSE_REFRESH_HOURS_UTC)
    estrictamente posterior a dt. Anclar a horas de reloj (en vez de 'cada N
    horas desde el arranque') garantiza frescura antes de ventanas horarias
    específicas, sin importar cuándo se reinicie el bot."""
    candidates = []
    for h in Config.UNIVERSE_REFRESH_HOURS_UTC:
        candidate = dt.replace(hour=h, minute=0, second=0, microsecond=0)
        if candidate <= dt:
            candidate += timedelta(days=1)
        candidates.append(candidate)
    return min(candidates)


# ==============================================================================
# BINANCE REST: OPEN INTEREST (bucle secuencial con pausa anti rate-limit)
# ==============================================================================
def fetch_open_interest(symbol):
    data = http_get_json(f"{Config.REST_BASE}/fapi/v1/openInterest", params={"symbol": symbol})
    if not data:
        return None
    try:
        return float(data["openInterest"])
    except (KeyError, ValueError, TypeError):
        return None


def evaluate_oi_timeframes(symbol, hist, oi_usdt):
    """Evalúa el cambio de OI contra CADA temporalidad de forma independiente.
    Solo dispara ante SUBIDAS de OI (acumulación) — las caídas se ignoran a
    propósito, ya que el objetivo es detectar entradas de dinero, no salidas.
    Devuelve una lista de tuplas (label, pct_change) de las que superaron su umbral."""
    triggered = []
    for label, (secs, threshold, tolerance) in OI_TIMEFRAMES.items():
        sample = find_closest_sample(hist, secs, tolerance)
        if sample and sample[1] > 0:
            change = (oi_usdt - sample[1]) / sample[1] * 100
            if change >= threshold:  # solo variación POSITIVA (acumulación de OI)
                triggered.append((label, change))
    return triggered


def compute_rvol_1m(symbol):
    """RVOL 'Fórmula Tipo 1': volumen de la ÚLTIMA vela cerrada de 1m vs. el
    promedio de las LOOKBACK_BARS_1M velas inmediatamente anteriores (la
    misma lógica que usa el detector de spikes). Devuelve None si todavía no
    hay historial suficiente (ej. tras un reinicio reciente del bot), para
    que la tarjeta de alerta simplemente omita esa línea."""
    with kline_lock:
        hist = kline_volume_history.get(symbol)
        if not hist or len(hist) < Config.LOOKBACK_BARS_1M + 1:
            return None
        snapshot = list(hist)

    last_bar = snapshot[-1]
    reference = snapshot[-(Config.LOOKBACK_BARS_1M + 1):-1]
    avg = sum(reference) / len(reference)
    if avg <= 0:
        return None
    return last_bar / avg


def process_symbol_oi(symbol, price, quote_volume):
    oi_contracts = fetch_open_interest(symbol)
    if oi_contracts is None:
        return

    oi_usdt = oi_contracts * price
    now = time.time()
    hist = oi_history.setdefault(symbol, deque(maxlen=Config.OI_HISTORY_MAXLEN))

    triggered = evaluate_oi_timeframes(symbol, hist, oi_usdt)
    hist.append((now, oi_usdt))  # guardar DESPUÉS de comparar, para no compararse consigo mismo

    if not triggered:
        return

    last_sent = last_oi_alert.get(symbol, 0)
    if now - last_sent < Config.OI_ALERT_COOLDOWN:
        return

    # Todas las alertas triggered son subidas (ver evaluate_oi_timeframes), así
    # que el marcador siempre es verde.
    lines = "\n".join(f"• *{label}:* 🟢 +{chg:.2f}%" for label, chg in triggered)

    mcap = get_market_cap(symbol)
    mcap_line = f"*Market Cap:* {format_usd(mcap)}\n" if mcap else ""

    # Ratio OI/Volumen 24h: qué tan grande es el Open Interest en relación al
    # volumen negociado en el día.
    oi_vol_ratio = (oi_usdt / quote_volume * 100) if quote_volume > 0 else 0.0

    rvol_1m = compute_rvol_1m(symbol)
    rvol_line = f"*RVOL (1m):* {rvol_1m:.2f}x\n" if rvol_1m is not None else ""

    msg = (
        f"📈 *ALERTA DE OPEN INTEREST (Acumulación)*\n\n"
        f"*Par:* #{symbol}\n"
        f"*Temporalidad(es) activada(s):*\n{lines}\n\n"
        f"*OI Actual:* {format_usd(oi_usdt)}\n"
        f"{mcap_line}"
        f"*Volumen 24h:* {format_usd(quote_volume)}\n"
        f"*Ratio OI/Vol:* {oi_vol_ratio:.1f}%\n"
        f"{rvol_line}"
        f"\n📊 [Ver en TradingView]({tradingview_link(symbol)})"
    )
    if send_info_alert(msg):
        last_oi_alert[symbol] = now
        log.info("Alerta OI enviada -> %s | %s", symbol, [l for l, _ in triggered])


def check_oi_cycle():
    """Recorre TODOS los pares activos de forma SECUENCIAL, con una pausa fija
    entre cada request para evitar bloqueos/HTTP 418/429 de Binance."""
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
    for symbol in symbols_snapshot:
        t = tickers.get(symbol)
        if not t:
            continue
        try:
            process_symbol_oi(symbol, t["price"], t["quote_volume"])
            processed += 1
        except Exception as e:
            log.exception("Error procesando OI de %s: %s", symbol, e)
        time.sleep(Config.OI_REQUEST_PAUSE)  # <-- pausa anti rate-limit

    log.info("Ciclo de OI completado: %s/%s pares procesados en %.1fs.",
              processed, len(symbols_snapshot), time.time() - start)


# ==============================================================================
# WEBSOCKET: VELAS DE 1 MINUTO (spikes de volumen + RVOL de corto plazo)
# ==============================================================================
def build_stream_url(symbols):
    streams = "/".join(f"{s.lower()}@kline_1m" for s in symbols)
    return f"{Config.WS_BASE}?streams={streams}"


def evaluate_volume_bar(symbol, quote_volume):
    """Usa el histórico REAL de barras cerradas guardado en memoria (deque),
    calculado ANTES de insertar la barra actual (nunca se compara una vela
    consigo misma)."""
    with kline_lock:
        hist = kline_volume_history.setdefault(
            symbol, deque(maxlen=max(Config.LOOKBACK_BARS_1M, Config.RVOL_WINDOW_BARS) + 5)
        )
        hist_snapshot = list(hist)  # copia para no bloquear mientras evaluamos

    now = time.time()

    # ---------------- SPIKE DETECTOR (canal urgente) ----------------
    if len(hist_snapshot) >= Config.LOOKBACK_BARS_1M:
        reference_bars = hist_snapshot[-Config.LOOKBACK_BARS_1M:]
        avg_vol = sum(reference_bars) / len(reference_bars)
        if avg_vol > 0:
            spike_pct = (quote_volume - avg_vol) / avg_vol * 100
            if spike_pct >= Config.VOL_SPIKE_THRESHOLD_PCT:
                last_sent = last_spike_alert.get(symbol, 0)
                if now - last_sent >= Config.SPIKE_ALERT_COOLDOWN:
                    msg = (
                        f"🔥 *SPIKE DE VOLUMEN (1m)*\n\n"
                        f"*Par:* #{symbol}\n"
                        f"*Volumen última vela:* {format_usd(quote_volume)}\n"
                        f"*Promedio ({Config.LOOKBACK_BARS_1M} velas):* {format_usd(avg_vol)}\n"
                        f"*Exceso:* +{spike_pct:.0f}%\n\n"
                        f"📊 [Ver en TradingView]({tradingview_link(symbol)})"
                    )
                    if send_urgent_alert(msg):
                        last_spike_alert[symbol] = now
                        log.info("Alerta SPIKE enviada -> %s | +%.0f%%", symbol, spike_pct)

            # ---------------- RVOL DETECTOR corto plazo (canal info) ----------------
            if len(hist_snapshot) >= Config.RVOL_WINDOW_BARS - 1:
                window = hist_snapshot[-(Config.RVOL_WINDOW_BARS - 1):] + [quote_volume]
                actual_sum = sum(window)
                expected_sum = avg_vol * Config.RVOL_WINDOW_BARS
                if expected_sum > 0:
                    rvol = actual_sum / expected_sum
                    if rvol >= Config.RVOL_THRESHOLD:
                        last_sent_rvol = last_rvol_alert.get(symbol, 0)
                        if now - last_sent_rvol >= Config.RVOL_ALERT_COOLDOWN:
                            msg = (
                                f"📊 *RVOL ANÓMALO*\n\n"
                                f"*Par:* #{symbol}\n"
                                f"*RVOL:* {rvol:.2f}x\n"
                                f"*Volumen acumulado ({Config.RVOL_WINDOW_BARS}m):* {format_usd(actual_sum)}\n"
                                f"*Volumen esperado:* {format_usd(expected_sum)}\n\n"
                                f"📊 [Ver en TradingView]({tradingview_link(symbol)})"
                            )
                            if send_info_alert(msg):
                                last_rvol_alert[symbol] = now
                                log.info("Alerta RVOL enviada -> %s | %.2fx", symbol, rvol)

    with kline_lock:
        hist.append(quote_volume)


def on_ws_message(ws, message):
    try:
        payload = json.loads(message)
        data = payload.get("data", {})
        if data.get("e") != "kline":
            return
        k = data.get("k", {})
        if not k.get("x"):  # solo procesar velas CERRADAS
            return
        symbol = k.get("s")
        quote_volume = float(k.get("q", 0))
        if symbol:
            evaluate_volume_bar(symbol, quote_volume)
    except Exception as e:
        log.exception("Error procesando mensaje de WebSocket: %s", e)


def on_ws_error(ws, error):
    log.warning("WebSocket error: %s", error)


def on_ws_close(ws, close_status_code, close_msg):
    log.warning("WebSocket cerrado (code=%s, msg=%s). Se reconectará.", close_status_code, close_msg)


def on_ws_open(ws):
    log.info("WebSocket conectado correctamente.")


def restart_websocket():
    """Fuerza el cierre del WebSocket activo para que el hilo lo reconstruya
    con la lista de símbolos actualizada."""
    with ws_restart_lock:
        global current_ws_app
        if current_ws_app is not None:
            try:
                current_ws_app.close()
            except Exception:
                pass


def run_websocket_forever():
    """Hilo de fondo: mantiene el WebSocket vivo y lo reconecta ante cualquier
    caída o cambio en la lista de símbolos monitoreados."""
    global current_ws_app
    while True:
        with symbols_lock:
            symbols_snapshot = list(active_symbols)

        if not symbols_snapshot:
            time.sleep(5)
            continue

        url = build_stream_url(symbols_snapshot)
        log.info("Conectando WebSocket con %s streams de velas 1m...", len(symbols_snapshot))

        ws_app = websocket.WebSocketApp(
            url,
            on_open=on_ws_open,
            on_message=on_ws_message,
            on_error=on_ws_error,
            on_close=on_ws_close,
        )
        current_ws_app = ws_app

        try:
            ws_app.run_forever(ping_interval=180, ping_timeout=10)
        except Exception as e:
            log.exception("Excepción en WebSocket run_forever: %s", e)

        time.sleep(5)  # pequeño backoff antes de reconectar


# ==============================================================================
# SERVIDOR HTTP MÍNIMO (keep-alive para Render u otras plataformas)
# ==============================================================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        with symbols_lock:
            n = len(active_symbols)
        self.wfile.write(f"OK - Futures Monitor Bot activo. Pares monitoreados: {n}".encode())

    def log_message(self, format, *args):
        pass


def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    log.info("Servidor de salud escuchando en el puerto %s (para keep-alive).", port)
    server.serve_forever()


# ==============================================================================
# LOOP PRINCIPAL
# ==============================================================================
def main():
    log.info("=== Iniciando Binance Futures Monitor Bot (universo por market cap + WebSocket + REST) ===")
    log.info(
        "Universo: banda $%s-$%s | ratio vol/mcap >= %.0f%% | piso vol >= %s | anclas UTC %s",
        format_usd(Config.MIN_MARKET_CAP_USD), format_usd(Config.MAX_MARKET_CAP_USD),
        Config.MIN_VOLUME_TO_MCAP_RATIO * 100, format_usd(Config.MIN_BINANCE_VOLUME_USD),
        Config.UNIVERSE_REFRESH_HOURS_UTC,
    )
    log.info(
        "Spike: >=%.0f%% sobre %s velas | RVOL: >=%.1fx sobre %s velas | "
        "OI umbrales 5m/15m/30m/1h/4h/24h = %.1f/%.1f/%.1f/%.1f/%.1f/%.1f %%",
        Config.VOL_SPIKE_THRESHOLD_PCT, Config.LOOKBACK_BARS_1M,
        Config.RVOL_THRESHOLD, Config.RVOL_WINDOW_BARS,
        Config.OI_THRESHOLD_5M, Config.OI_THRESHOLD_15M, Config.OI_THRESHOLD_30M,
        Config.OI_THRESHOLD_1H, Config.OI_THRESHOLD_4H, Config.OI_THRESHOLD_24H,
    )

    if Config.ENABLE_HEALTH_SERVER:
        threading.Thread(target=start_health_server, daemon=True).start()

    refresh_universe()  # primera carga del universo (dispara conexión WS más abajo)

    threading.Thread(target=run_websocket_forever, daemon=True).start()
    threading.Thread(target=run_market_cap_display_refresher, daemon=True).start()

    send_info_alert("✅ *Futures Monitor Bot iniciado correctamente.*")

    next_universe_refresh = next_universe_refresh_after(datetime.now(timezone.utc))
    last_oi_check = 0  # forzar primera ejecución inmediata

    while True:
        try:
            now_dt = datetime.now(timezone.utc)
            now_ts = time.time()

            if now_dt >= next_universe_refresh:
                refresh_universe()
                next_universe_refresh = next_universe_refresh_after(now_dt)

            if now_ts - last_oi_check >= Config.OI_CHECK_INTERVAL:
                check_oi_cycle()
                last_oi_check = time.time()

        except Exception as e:
            log.exception("Error inesperado en el loop principal: %s", e)

        time.sleep(10)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Bot detenido manualmente.")
