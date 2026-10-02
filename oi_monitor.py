#!/usr/bin/env python3
"""
===============================================================================
BINANCE FUTURES MONITOR BOT - OI + Volumen (WebSocket + REST híbrido)
REFACTORIZACIÓN: PATRÓN DISYUNTOR GLOBAL (CIRCUIT BREAKER) ANTI-BAN 418 / 429
===============================================================================
Arquitectura (2 capas):

CAPA 1 — ¿Qué monedas vigilo? (universo dinámico)
Se construye a partir de CoinMarketCap (listings/latest): se filtran las monedas
dentro de una BANDA de capitalización de mercado configurable (ej. $50M-$500M),
se cruzan contra los pares realmente disponibles en Binance Futuros USDT-M, y se
exige liquidez mínima (piso absoluto de volumen 24h + ratio Volumen/Market Cap).
El match símbolo->proyecto es CONSERVADOR: si un ticker es ambiguo (varios proyectos
lo comparten), se descarta en vez de arriesgarse a monitorear el proyecto equivocado.
Este universo se refresca en horas ANCLADAS de reloj (UTC), no por intervalo relativo
al arranque del bot, para garantizar que esté fresco antes de ventanas horarias
específicas (ej. antes de la apertura de NY).

CAPA 2 — ¿Está pasando algo interesante AHORA en esas monedas?
- WebSocket (wss://fstream.binance.com) -> velas de 1 minuto en tiempo real
  para detectar spikes de volumen y RVOL de corto plazo, sin gastar peso de la
  API REST de Binance.
- REST pública de Binance -> Open Interest, consultado en bucle secuencial
  con pausa fija entre requests (anti rate-limit).
- Telegram con DOS canales: uno urgente (con sonido) para spikes de volumen
  explosivos, y uno informativo (silencioso) para OI y RVOL.

MEJORA IMPLEMENTADA: DISYUNTOR GLOBAL (CIRCUIT BREAKER):
1. Estado Global de Bloqueo (Cooldown de IP):
   - Variable `ip_blocked_until` gestionada de forma atómica y thread-safe.
   - Detección inmediata de HTTP 418 (IP Ban) y HTTP 429 (Rate Limit).
   - Lectura prioritaria del header `Retry-After` con tope de seguridad configurable.
2. Bypassing e Interrupción Inmediata:
   - Verificación de disyuntor al inicio y durante los ciclos de OI y refresco de universo.
   - Si la IP está en cooldown, no se ejecutan reintentos inútiles símbolo por símbolo.
   - Emite una sola advertencia clara en logs y aborta el ciclo de inmediato.
===============================================================================
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

# =============================================================================
# BLOQUE DE CONFIGURACIÓN (EDITAR AQUÍ)
# =============================================================================
class Config:
    # -------------------------------------------------------------------------
    # TELEGRAM — credenciales del bot y de los DOS canales de alertas.
    # -------------------------------------------------------------------------
    TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "PON_TU_TOKEN_AQUI")
    # Canal URGENTE: spikes de volumen explosivos en velas de 1m. Suena.
    CHAT_ID_URGENTE = os.environ.get("CHAT_ID_URGENTE", "PON_TU_CHAT_ID_URGENTE")
    # Canal INFO: monitor general de Open Interest y RVOL. Silencioso.
    CHAT_ID_INFO = os.environ.get("CHAT_ID_INFO", "PON_TU_CHAT_ID_INFO")

    # -------------------------------------------------------------------------
    # BINANCE — endpoints base (públicos, sin API Key)
    # -------------------------------------------------------------------------
    REST_BASE = "https://fapi.binance.com"
    WS_BASE = "wss://fstream.binance.com/stream"

    # -------------------------------------------------------------------------
    # COINMARKETCAP — requiere API Key gratuita (pro-api, plan Basic).
    # -------------------------------------------------------------------------
    CMC_API_KEY = os.environ.get("CMC_API_KEY", "")
    CMC_BASE = "https://pro-api.coinmarketcap.com"

    # -------------------------------------------------------------------------
    # CAPA 1 — UNIVERSO POR BANDA DE CAPITALIZACIÓN DE MERCADO
    # -------------------------------------------------------------------------
    MIN_MARKET_CAP_USD = 50_000_000
    MAX_MARKET_CAP_USD = 500_000_000
    MIN_BINANCE_VOLUME_USD = 3_000_000
    MIN_VOLUME_TO_MCAP_RATIO = 0.05  # 5%: al menos ese % del cap se "rota" en 24h
    MARKET_UNIVERSE_FETCH_LIMIT = 5000
    UNIVERSE_REFRESH_HOURS_UTC = [5, 11, 17, 23]
    MARKET_CAP_DISPLAY_REFRESH_INTERVAL = 30 * 60

    # -------------------------------------------------------------------------
    # OPEN INTEREST — frecuencia de consulta y pausa anti rate-limit
    # -------------------------------------------------------------------------
    OI_CHECK_INTERVAL = 5 * 60  # cada cuánto se recorren TODOS los pares
    OI_REQUEST_PAUSE = 0.08      # pausa entre cada request de OI (segundos)
    OI_HISTORY_MAXLEN = 300     # ~25h de histórico a razón de 1 muestra/5min

    # Umbrales de variación de OI
    OI_THRESHOLD_5M = 3.0
    OI_THRESHOLD_15M = 5.0
    OI_THRESHOLD_30M = 7.0
    OI_THRESHOLD_1H = 10.0
    OI_THRESHOLD_4H = 15.0
    OI_THRESHOLD_24H = 20.0

    # -------------------------------------------------------------------------
    # DETECTOR DE SPIKES DE VOLUMEN EN VELAS DE 1 MINUTO (canal urgente)
    # -------------------------------------------------------------------------
    VOL_SPIKE_THRESHOLD_PCT = 150  # % de exceso sobre el promedio para alertar
    LOOKBACK_BARS_1M = 10          # nº de velas previas usadas como referencia

    # -------------------------------------------------------------------------
    # DETECTOR DE RVOL — Volumen Relativo de CORTO PLAZO (canal info)
    # -------------------------------------------------------------------------
    RVOL_WINDOW_BARS = 5   # tamaño de la ventana acumulada (minutos)
    RVOL_THRESHOLD = 2.0   # dispara si RVOL >= 2.0x (200%) lo esperado

    # -------------------------------------------------------------------------
    # ANTI-SPAM (cooldowns independientes por tipo de alerta)
    # -------------------------------------------------------------------------
    SPIKE_ALERT_COOLDOWN = 15 * 60
    OI_ALERT_COOLDOWN = 15 * 60
    RVOL_ALERT_COOLDOWN = 15 * 60

    # -------------------------------------------------------------------------
    # RED / ROBUSTEZ Y CIRCUIT BREAKER (DISYUNTOR GLOBAL)
    # -------------------------------------------------------------------------
    REQUEST_TIMEOUT = 10
    MAX_RETRIES = 3
    RETRY_BACKOFF_BASE = 2
    # Tope de segundos a esperar cuando Binance indica el tiempo vía "Retry-After"
    MAX_RETRY_AFTER_WAIT = 120
    # Tiempo por defecto de enfriamiento si Binance no provee header Retry-After
    DEFAULT_BAN_COOLDOWN_SECONDS = 60
    HEAVY_MAX_RETRIES = 4
    HEAVY_RETRY_BACKOFF = [5, 15, 30, 60]
    EMPTY_UNIVERSE_RETRY_BASE = 120
    EMPTY_UNIVERSE_RETRY_MAX = 1200
    ENABLE_HEALTH_SERVER = True


# Temporalidades de OI a evaluar: nombre -> (segundos, umbral %, tolerancia seg.)
OI_TIMEFRAMES = {
    "5m": (300, Config.OI_THRESHOLD_5M, 90),
    "15m": (900, Config.OI_THRESHOLD_15M, 180),
    "30m": (1800, Config.OI_THRESHOLD_30M, 300),
    "1h": (3600, Config.OI_THRESHOLD_1H, 450),
    "4h": (14400, Config.OI_THRESHOLD_4H, 900),
    "24h": (86400, Config.OI_THRESHOLD_24H, 1800),
}

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("futures_monitor")
logging.getLogger("websocket").setLevel(logging.WARNING)

# =============================================================================
# ESTADO GLOBAL EN MEMORIA & DISYUNTOR DE IP (CIRCUIT BREAKER)
# =============================================================================
http_session = requests.Session()
http_session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

symbols_lock = threading.Lock()
active_symbols = []  # universo filtrado (banda de market cap + liquidez)
oi_history = {}      # symbol -> deque[(ts, oi_usdt)]
kline_volume_history = {}  # symbol -> deque[quote_volume de velas cerradas]
kline_lock = threading.Lock()

last_spike_alert = {}  # symbol -> ts último alert de spike
last_oi_alert = {}     # symbol -> ts último alert de OI
last_rvol_alert = {}   # symbol -> ts último alert de RVOL

current_ws_app = None  # referencia al WebSocketApp activo
ws_restart_lock = threading.Lock()

market_cap_cache = {}  # binance_symbol -> market cap USD (para mostrar en alertas)
symbol_cmc_id = {}     # binance_symbol -> id de CoinMarketCap
market_cap_lock = threading.Lock()

# -----------------------------------------------------------------------------
# DISYUNTOR GLOBAL (CIRCUIT BREAKER) PARA BLOQUEOS DE IP (HTTP 418 / 429)
# -----------------------------------------------------------------------------
circuit_breaker_lock = threading.Lock()
ip_blocked_until = None  # datetime | None: marca de tiempo UTC/local hasta la cual no tocar Binance


def is_ip_blocked() -> bool:
    """
    Verifica de forma thread-safe si el Disyuntor Global está ACTIVO.
    Retorna True si la IP está en cooldown por rate limit o ban de Binance.
    Si el tiempo ya pasó, limpia el estado automáticamente.
    """
    global ip_blocked_until
    with circuit_breaker_lock:
        if ip_blocked_until is None:
            return False
        if datetime.now() < ip_blocked_until:
            return True
        # El tiempo de espera ya venció -> Cerramos el disyuntor (IP operativa de nuevo)
        ip_blocked_until = None
        log.info("🟢 [DISYUNTOR] Tiempo de cooldown finalizado. Restableciendo llamadas a Binance.")
        return False


def get_blocked_until_time_str() -> str:
    """Retorna la hora formateada HH:MM:SS de expiración del bloqueo."""
    with circuit_breaker_lock:
        if ip_blocked_until is not None:
            return ip_blocked_until.strftime("%H:%M:%S")
        return ""


def trip_circuit_breaker(wait_seconds: float, status_code: int = 429, source_url: str = ""):
    """
    Dispara el Disyuntor Global ante HTTP 418 o 429:
    1. Calcula el tiempo de enfriamiento (respetando MAX_RETRY_AFTER_WAIT).
    2. Establece ip_blocked_until = ahora + tiempo_espera.
    3. Registra una advertencia clara para monitoreo en logs.
    """
    global ip_blocked_until
    cooldown = wait_seconds if (wait_seconds and wait_seconds > 0) else Config.DEFAULT_BAN_COOLDOWN_SECONDS
    cooldown = min(cooldown, float(Config.MAX_RETRY_AFTER_WAIT))
    unblock_time = datetime.now() + timedelta(seconds=cooldown)

    with circuit_breaker_lock:
        if ip_blocked_until is None or unblock_time > ip_blocked_until:
            ip_blocked_until = unblock_time

    log.warning(
        "🚨 [DISYUNTOR GLOBAL ACTIVADO] Binance respondió HTTP %s (IP Limit/Ban) en %s. "
        "Enfriando IP por %.0fs. Todas las llamadas a Binance quedan bloqueadas hasta las %s.",
        status_code,
        source_url or "Binance Futures REST",
        cooldown,
        unblock_time.strftime("%H:%M:%S")
    )


# =============================================================================
# HELPERS GENERALES
# =============================================================================
def format_usd(value):
    if value is None:
        return "$0.00"
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


def _resolve_wait_time(resp, attempt, fallback_backoff):
    """
    Decide cuánto esperar ante un 418/429: prioriza el header Retry-After
    (Binance/CMC indican ahí el tiempo EXACTO necesario, incluyendo cuánto
    falta para que termine un baneo 418) en vez de adivinar con un backoff fijo.
    Si el header no viene, cae al backoff de respaldo o al valor por defecto.
    """
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return min(float(retry_after), Config.MAX_RETRY_AFTER_WAIT)
        except ValueError:
            pass
    if fallback_backoff and len(fallback_backoff) > 0:
        return fallback_backoff[min(attempt - 1, len(fallback_backoff) - 1)]
    return Config.DEFAULT_BAN_COOLDOWN_SECONDS


def http_get_json(url, params=None, headers=None, max_retries=None, fallback_backoff=None):
    """
    GET con reintentos e integración con Disyuntor Global.
    Devuelve None si la IP está bloqueada, si falla todo o si se dispara un 418/429.
    """
    is_binance = Config.REST_BASE in url

    # 1. BYPASSING PREVENTIVO: Si la IP de Binance está actualmente en cooldown,
    # no tocar la API bajo ninguna circunstancia.
    if is_binance and is_ip_blocked():
        log.warning(
            "⚠️ IP bloqueada por Binance. Omitiendo petición a %s hasta %s para enfriar la IP.",
            url, get_blocked_until_time_str()
        )
        return None

    attempts = max_retries or Config.MAX_RETRIES
    backoff = fallback_backoff or [Config.RETRY_BACKOFF_BASE ** i for i in range(1, attempts + 1)]

    for attempt in range(1, attempts + 1):
        if is_binance and is_ip_blocked():
            return None

        try:
            resp = http_session.get(url, params=params, headers=headers, timeout=Config.REQUEST_TIMEOUT)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code in (429, 418):
                wait = _resolve_wait_time(resp, attempt, backoff)

                # Si Binance devuelve 418 o 429, activamos el Disyuntor Global y NO seguimos reintentando
                if is_binance:
                    trip_circuit_breaker(wait_seconds=wait, status_code=resp.status_code, source_url=url)
                    return None  # Abortar inmediatamente esta llamada

                # Si es otra API (ej. CoinMarketCap)
                log.warning("Rate limit (status %s) en %s. Esperando %.0fs...", resp.status_code, url, wait)
                time.sleep(wait)
            else:
                log.warning("Respuesta inesperada (status %s) en %s", resp.status_code, url)
                time.sleep(Config.RETRY_BACKOFF_BASE)

        except requests.exceptions.RequestException as e:
            wait = backoff[min(attempt - 1, len(backoff) - 1)]
            log.warning(
                "Error de red (%s) en %s [intento %s/%s]. Reintentando en %ss...",
                e, url, attempt, attempts, wait
            )
            time.sleep(wait)

    log.error("Fallaron todos los reintentos para %s", url)
    return None


def find_closest_sample(history, target_seconds_ago, tolerance):
    now = time.time()
    target_ts = now - target_seconds_ago
    best, best_diff = None, None
    for ts, value in history:
        diff = abs(ts - target_ts)
        if diff <= tolerance and (best_diff is None or diff < best_diff):
            best, best_diff = (ts, value), diff
    return best


# =============================================================================
# TELEGRAM (doble canal)
# =============================================================================
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


# =============================================================================
# BINANCE REST: exchangeInfo + tickers 24h
# =============================================================================
def fetch_exchange_info(heavy=False):
    """Devuelve el set de símbolos USDT-M PERPETUAL actualmente TRADING en Binance."""
    if is_ip_blocked():
        return None

    kwargs = {
        "max_retries": Config.HEAVY_MAX_RETRIES,
        "fallback_backoff": Config.HEAVY_RETRY_BACKOFF,
    } if heavy else {}

    data = http_get_json(f"{Config.REST_BASE}/fapi/v1/exchangeInfo", **kwargs)
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


def fetch_24h_tickers(heavy=False):
    """Obtiene los precios y volúmenes 24h de futuros Binance."""
    if is_ip_blocked():
        return None

    kwargs = {
        "max_retries": Config.HEAVY_MAX_RETRIES,
        "fallback_backoff": Config.HEAVY_RETRY_BACKOFF,
    } if heavy else {}

    data = http_get_json(f"{Config.REST_BASE}/fapi/v1/ticker/24hr", **kwargs)
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


# =============================================================================
# COINMARKETCAP: universo por capitalización + cache de Market Cap
# =============================================================================
def cmc_headers():
    return {"X-CMC_PRO_API_KEY": Config.CMC_API_KEY, "Accept": "application/json"}


def fetch_cmc_listings(limit):
    if not Config.CMC_API_KEY:
        log.error("CMC_API_KEY no configurada. No se puede refrescar el universo.")
        return None
    data = http_get_json(
        f"{Config.CMC_BASE}/v1/cryptocurrency/listings/latest",
        params={"start": 1, "limit": limit, "convert": "USD"},
        headers=cmc_headers(),
        max_retries=Config.HEAVY_MAX_RETRIES,
        fallback_backoff=Config.HEAVY_RETRY_BACKOFF,
    )
    if not data:
        return None
    return data.get("data")


def fetch_cmc_quotes_by_id(ids):
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
    """
    CAPA 1: reconstruye qué monedas se monitorean.
    Protegido con Disyuntor Global: si la IP está bloqueada, aborta sin tocar Binance.
    """
    if is_ip_blocked():
        log.warning(
            "⚠️ IP bloqueada por Binance. Omitiendo refresco de universo hasta %s para enfriar la IP.",
            get_blocked_until_time_str()
        )
        return

    log.info("Refrescando universo por capitalización de mercado (CoinMarketCap)...")
    listings = fetch_cmc_listings(Config.MARKET_UNIVERSE_FETCH_LIMIT)

    if is_ip_blocked():
        log.warning("⚠️ IP bloqueada por Binance. Omitiendo llamadas a Binance exchangeInfo.")
        return

    valid_binance_symbols = fetch_exchange_info(heavy=True)
    tickers = fetch_24h_tickers(heavy=True)

    if not listings or not valid_binance_symbols or not tickers:
        log.error("No se pudo refrescar el universo (CMC/Binance no respondió). Se mantiene el anterior.")
        return

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

    band_candidates = {}
    for sym, entries in symbol_entries.items():
        if len(entries) != 1:
            continue
        cmc_id, mcap = entries[0]
        if Config.MIN_MARKET_CAP_USD <= mcap <= Config.MAX_MARKET_CAP_USD:
            band_candidates[sym] = (cmc_id, mcap)

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
        len(new_symbols),
        format_usd(Config.MIN_MARKET_CAP_USD),
        format_usd(Config.MAX_MARKET_CAP_USD),
        Config.MIN_VOLUME_TO_MCAP_RATIO * 100,
        format_usd(Config.MIN_BINANCE_VOLUME_USD),
        ambiguous_count,
    )

    if changed:
        restart_websocket()


def refresh_market_cap_display():
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
    while True:
        try:
            refresh_market_cap_display()
        except Exception as e:
            log.exception("Error refrescando Market Cap (display): %s", e)
        time.sleep(Config.MARKET_CAP_DISPLAY_REFRESH_INTERVAL)


def next_universe_refresh_after(dt):
    candidates = []
    for h in Config.UNIVERSE_REFRESH_HOURS_UTC:
        candidate = dt.replace(hour=h, minute=0, second=0, microsecond=0)
        if candidate <= dt:
            candidate += timedelta(days=1)
        candidates.append(candidate)
    return min(candidates)


# =============================================================================
# BINANCE REST: OPEN INTEREST (Bucle protegido por Disyuntor Global)
# =============================================================================
def fetch_open_interest(symbol):
    if is_ip_blocked():
        return None
    data = http_get_json(
        f"{Config.REST_BASE}/fapi/v1/openInterest",
        params={"symbol": symbol}
    )
    if not data:
        return None
    try:
        return float(data["openInterest"])
    except (KeyError, ValueError, TypeError):
        return None


def evaluate_oi_timeframes(symbol, hist, oi_usdt):
    triggered = []
    for label, (secs, threshold, tolerance) in OI_TIMEFRAMES.items():
        sample = find_closest_sample(hist, secs, tolerance)
        if sample and sample[1] > 0:
            change = (oi_usdt - sample[1]) / sample[1] * 100
            if change >= threshold:
                triggered.append((label, change))
    return triggered


def compute_rvol_1m(symbol):
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
    hist.append((now, oi_usdt))

    if not triggered:
        return

    last_sent = last_oi_alert.get(symbol, 0)
    if now - last_sent < Config.OI_ALERT_COOLDOWN:
        return

    lines = "\n".join(f"• *{label}:* 🟢 +{chg:.2f}%" for label, chg in triggered)
    mcap = get_market_cap(symbol)
    mcap_line = f"*Market Cap:* {format_usd(mcap)}\n" if mcap else ""
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
    """
    Recorre TODOS los pares activos de forma SECUENCIAL con pausa fija.
    
    IMPLEMENTACIÓN DEL DISYUNTOR GLOBAL:
    a) Si datetime.now() < ip_blocked_until, emite un único log claro y cancela el ciclo completo.
    b) Si durante el recorrido por los 85 símbolos alguno recibe 418/429, el disyuntor se activa
       y se interrumpe de inmediato el bucle, evitando peticiones subsecuentes inútiles.
    """
    # 1. Comprobación PREVIA del Disyuntor Global
    if is_ip_blocked():
        log.warning(
            "⚠️ IP bloqueada por Binance. Omitiendo ciclo de Open Interest hasta %s para enfriar la IP.",
            get_blocked_until_time_str()
        )
        return

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
        # 2. Interrupción INMEDIATA si el disyuntor se activó en una llamada previa del ciclo
        if is_ip_blocked():
            log.warning(
                "⚠️ IP bloqueada por Binance durante el ciclo de OI. Interrumpiendo recorrido "
                "(%s/%s procesados) hasta %s para enfriar la IP.",
                processed, len(symbols_snapshot), get_blocked_until_time_str()
            )
            break

        t = tickers.get(symbol)
        if not t:
            continue

        try:
            process_symbol_oi(symbol, t["price"], t["quote_volume"])
            processed += 1
        except Exception as e:
            log.exception("Error procesando OI de %s: %s", symbol, e)

        time.sleep(Config.OI_REQUEST_PAUSE)

    log.info(
        "Ciclo de OI completado: %s/%s pares procesados en %.1fs.",
        processed,
        len(symbols_snapshot),
        time.time() - start
    )


# =============================================================================
# WEBSOCKET: VELAS DE 1 MINUTO
# =============================================================================
def build_stream_url(symbols):
    streams = "/".join(f"{s.lower()}@kline_1m" for s in symbols)
    return f"{Config.WS_BASE}?streams={streams}"


def evaluate_volume_bar(symbol, quote_volume):
    with kline_lock:
        hist = kline_volume_history.setdefault(
            symbol,
            deque(maxlen=max(Config.LOOKBACK_BARS_1M, Config.RVOL_WINDOW_BARS) + 5)
        )
        hist_snapshot = list(hist)

    now = time.time()

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

    if len(hist_snapshot) >= Config.RVOL_WINDOW_BARS - 1:
        window = hist_snapshot[-(Config.RVOL_WINDOW_BARS - 1):] + [quote_volume]
        actual_sum = sum(window)
        reference_bars = hist_snapshot[-Config.LOOKBACK_BARS_1M:] if len(hist_snapshot) >= Config.LOOKBACK_BARS_1M else hist_snapshot
        avg_vol = sum(reference_bars) / len(reference_bars) if reference_bars else 0
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
        if not k.get("x"):
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
    with ws_restart_lock:
        global current_ws_app
        if current_ws_app is not None:
            try:
                current_ws_app.close()
            except Exception:
                pass


def run_websocket_forever():
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

        time.sleep(5)


# =============================================================================
# SERVIDOR HTTP MÍNIMO (keep-alive para Render)
# =============================================================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        with symbols_lock:
            n = len(active_symbols)
        
        status_cb = (
            f"DISYUNTOR ACTIVO (IP en enfriamiento hasta las {get_blocked_until_time_str()})"
            if is_ip_blocked()
            else "NORMAL (IP limpia)"
        )
        body = f"OK - Futures Monitor Bot activo. Pares monitoreados: {n} | Estado IP Binance: {status_cb}\n"
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format, *args):
        pass


def start_health_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    log.info("Servidor de salud escuchando en el puerto %s (para keep-alive).", port)
    server.serve_forever()


# =============================================================================
# LOOP PRINCIPAL
# =============================================================================
def main():
    log.info("=== Iniciando Binance Futures Monitor Bot (Circuit Breaker Edition) ===")
    log.info(
        "Universo: banda $%s-$%s | ratio vol/mcap >= %.0f%% | piso vol >= %s | anclas UTC %s",
        format_usd(Config.MIN_MARKET_CAP_USD),
        format_usd(Config.MAX_MARKET_CAP_USD),
        Config.MIN_VOLUME_TO_MCAP_RATIO * 100,
        format_usd(Config.MIN_BINANCE_VOLUME_USD),
        Config.UNIVERSE_REFRESH_HOURS_UTC,
    )
    log.info(
        "Spike: >=%.0f%% sobre %s velas | RVOL: >=%.1fx sobre %s velas | "
        "OI umbrales 5m/15m/30m/1h/4h/24h = %.1f/%.1f/%.1f/%.1f/%.1f/%.1f%%",
        Config.VOL_SPIKE_THRESHOLD_PCT,
        Config.LOOKBACK_BARS_1M,
        Config.RVOL_THRESHOLD,
        Config.RVOL_WINDOW_BARS,
        Config.OI_THRESHOLD_5M,
        Config.OI_THRESHOLD_15M,
        Config.OI_THRESHOLD_30M,
        Config.OI_THRESHOLD_1H,
        Config.OI_THRESHOLD_4H,
        Config.OI_THRESHOLD_24H,
    )
    log.info(
        "Disyuntor Global configurado: MAX_RETRY_AFTER_WAIT=%ss, DEFAULT_COOLDOWN=%ss",
        Config.MAX_RETRY_AFTER_WAIT,
        Config.DEFAULT_BAN_COOLDOWN_SECONDS,
    )

    if Config.ENABLE_HEALTH_SERVER:
        threading.Thread(target=start_health_server, daemon=True).start()

    refresh_universe()  # primera carga del universo

    threading.Thread(target=run_websocket_forever, daemon=True).start()
    threading.Thread(target=run_market_cap_display_refresher, daemon=True).start()

    send_info_alert("✅ *Futures Monitor Bot iniciado correctamente con Circuit Breaker.*")

    next_universe_refresh = next_universe_refresh_after(datetime.now(timezone.utc))
    last_oi_check = 0  # forzar primera ejecución inmediata

    empty_retry_wait = Config.EMPTY_UNIVERSE_RETRY_BASE
    next_empty_retry = None

    with symbols_lock:
        if not active_symbols:
            next_empty_retry = time.time() + empty_retry_wait
            log.warning("Universo vacío tras el primer refresco. Reintentando en %ss...", empty_retry_wait)

    while True:
        try:
            now_dt = datetime.now(timezone.utc)
            now_ts = time.time()

            with symbols_lock:
                universe_is_empty = not active_symbols

            if universe_is_empty and next_empty_retry and now_ts >= next_empty_retry:
                # Si la IP está bloqueada, postergar reintento de emergencia sin saturar
                if is_ip_blocked():
                    log.warning(
                        "⚠️ IP bloqueada. Postergando reintento de universo de emergencia hasta %s.",
                        get_blocked_until_time_str()
                    )
                    time.sleep(10)
                    continue

                refresh_universe()
                with symbols_lock:
                    still_empty = not active_symbols

                if still_empty:
                    empty_retry_wait = min(empty_retry_wait * 2, Config.EMPTY_UNIVERSE_RETRY_MAX)
                    next_empty_retry = now_ts + empty_retry_wait
                    log.warning("Universo sigue vacío. Próximo reintento en %ss...", empty_retry_wait)
                else:
                    next_empty_retry = None
                    empty_retry_wait = Config.EMPTY_UNIVERSE_RETRY_BASE
                    log.info("Universo recuperado tras reintento de emergencia.")

            elif not universe_is_empty and now_dt >= next_universe_refresh:
                if not is_ip_blocked():
                    refresh_universe()
                    next_universe_refresh = next_universe_refresh_after(now_dt)
                    with symbols_lock:
                        if not active_symbols:
                            next_empty_retry = time.time() + empty_retry_wait
                            log.warning("El refresco anclado dejó el universo vacío. Activando reintentos de emergencia.")
                else:
                    log.warning(
                        "⚠️ IP bloqueada. Postergando refresco anclado hasta %s.",
                        get_blocked_until_time_str()
                    )

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
