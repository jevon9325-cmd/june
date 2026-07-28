#!/usr/bin/env python3
"""
June — IG CFD Signal Publisher (beta v0.2)
Observer-only: polls IG demo API 24/7 on weekdays, publishes signals to Redis.

No orders. No positions. No risk management.
June watches macro instruments and reports. Others decide.

Redis keys published:
  june_signals          — continuous (TTL 120s): real-time prices + momentum alerts
  june_morning_baseline — 06:55 UTC daily (TTL 2h): overnight price summary per instrument
  june_premarket_gaps   — 06:00-07:00 UTC (TTL 3h): pre-London gaps > 0.5%
  june_spread_baselines — continuous (TTL 25h): rolling 1h spread averages
  june_overnight_context— 06:55 UTC daily (TTL 4h): volatility regime + correlation notes

June demo API note:
  IG demo uses the same real-time price feed as live accounts. Prices are genuine
  market data. Overnight spreads on demo may be marginally wider than live due to
  reduced hedging activity, but the signal quality is equivalent.

Future Miss Secretary integration point (alert_system.py):
  In select_best_trade() or score_candidate(), call:
    june_raw = redis.get("june_signals")
    if june_raw:
        june = json.loads(june_raw)
        if "GOLD" in june["momentum_alerts"]:
            score *= 1.15  # boost GDX3, GDX, NEM, AG, PAAS, etc.
        if "SPX500" in june["momentum_alerts"] and direction == "bull":
            score *= 1.10  # broad market tailwind
    Also read june_overnight_context before morning scoring pass:
      ctx = json.loads(redis.get("june_overnight_context") or "{}")
      if ctx.get("high_overnight_volatility"):
          min_score *= 1.15  # raise bar on volatile overnight
  Wire this in after 2+ weeks of beta signal quality validation.
"""

import json
import math
import os
import sys
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
import redis as redis_lib

# ── Environment ─────────────────────────────────────────────────────────────
IG_BASE    = "https://demo-api.ig.com/gateway/deal"
IG_API_KEY = os.environ.get("IG_API_KEY", "")
IG_USER    = os.environ.get("IG_USERNAME", "")
IG_PASS    = os.environ.get("IG_PASSWORD", "")
IG_ACCOUNT = os.environ.get("IG_ACCOUNT_ID", "Z6CPCQ")

# IG Live account (W-8BEN activated — intelligence only, no orders placed)
IG_LIVE_BASE = os.environ.get("IG_LIVE_BASE_URL", "")
IG_LIVE_KEY  = os.environ.get("IG_LIVE_API_KEY", "")
IG_LIVE_USER = os.environ.get("IG_LIVE_USERNAME", "")
IG_LIVE_PASS     = os.environ.get("IG_LIVE_PASSWORD", "")
MARKETAUX_API_KEY = os.environ.get("MARKETAUX_API_KEY", "")
FINNHUB_KEY       = os.environ.get("FINNHUB_API_KEY", "")   # equity REST quotes

REDIS_HOST  = os.environ.get("REDIS_HOST", "")
REDIS_PORT  = int(os.environ.get("REDIS_PORT", 15074))
REDIS_PASS  = os.environ.get("REDIS_PASSWORD", "")

# ── LLM providers (future AI P&L monitor) ─────────────────────────────────
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
GEMINI_API_KEY     = os.environ.get("GEMINI_API_KEY", "")
OPENROUTER_DAILY_CAP = 180   # shared with Miss Secretary and Claudia via Redis
GEMINI_DAILY_CAP     = 950   # Gemini Flash free tier


# ── Poll timing ──────────────────────────────────────────────────────────────
POLL_ACTIVE = int(os.environ.get("JUNE_POLL_ACTIVE", 60))  # weekday normal cadence
POLL_MAINT  = 5 * 60    # 5-min cadence during detected maintenance window
SIGNAL_TTL  = 120       # june_signals Redis TTL — stale data is worse than no data
SESSION_MAX = 6 * 3600 - 1800  # re-auth 30 min before 6h IG token expiry
HISTORY_LEN = 20        # rolling price readings per instrument (~20 min at 60s)

# ── Weekend / session windows (all in UTC minutes-since-midnight) ────────────
# IG CFD closure: Friday 21:15 UTC → Sunday 21:00 UTC
WEEKEND_CLOSE_MIN = 21 * 60 + 15   # Friday 21:15 UTC — IG CFD close
WEEKEND_OPEN_MIN  = 21 * 60        # Sunday 21:00 UTC — IG CFD reopen

# Overnight window used for baseline tracking and gap detection
OVERNIGHT_START_MIN   = 21 * 60       # 21:00 UTC — after NY close
OVERNIGHT_END_MIN     = 7 * 60        # 07:00 UTC — London open

# Pre-London gap window
PREMARKET_START_MIN   = 6 * 60        # 06:00 UTC — gap window opens
PREMARKET_END_MIN     = 7 * 60        # 07:00 UTC — London opens

# Morning publish window (5-min window to guarantee we don't miss it at 60s cadence)
MORNING_PUB_START_MIN = 6 * 60 + 55  # 06:55 UTC
MORNING_PUB_END_MIN   = 7 * 60       # 07:00 UTC

# ── Maintenance detection ────────────────────────────────────────────────────
MAINT_CONSEC_THRESHOLD = 3          # consecutive empty cycles → enter maintenance
MAINT_MAX_SECS         = 90 * 60    # force exit maintenance after 90 min

# ── Signal thresholds ────────────────────────────────────────────────────────
MOMENTUM_PCT        = 0.30   # |change_5m| % to flag in momentum_alerts

SPREAD_HISTORY_LEN  = 60     # readings kept per instrument (~1h at 60s)
SPREAD_ALERT_FACTOR = 3.0    # current spread > 3× avg → spread_alert: True
SPREAD_MIN_READINGS = 5      # minimum readings before anomaly detection active

PREMARKET_GAP_PCT   = 0.50   # |change vs 06:00 baseline| to flag pre-London gap
DIRECT_CFD_GAP_PCT  = 1.5    # daily % change to flag individual stock as gap candidate
DIRECT_CFD_SEARCH_INTERVAL = 30        # seconds between IG search API calls
DIRECT_CFD_MISS_TTL        = 6 * 3600  # 6h before re-searching a not-found symbol
DIRECT_CFD_CONFIRMED_MISS_TTL = 24 * 3600  # 24h after all search strategies exhausted
DIRECT_CFD_REDIS_TTL       = 24 * 3600 # Redis TTL for june_direct_cfd_map
NAV_CACHE_TTL              = 6 * 3600  # Redis TTL for june_market_nav_cache
US_PREMARKET_START_MIN = 10 * 60       # 10:00 UTC = 05:00 ET (US pre-market opens)
US_PREMARKET_END_MIN   = 14 * 60 + 30  # 14:30 UTC = 09:30 ET (US regular session)

# Post-filter keyword after ticker-symbol search: ticker -> lowercase fragment in IG name.
# Rejects wrong-company hits before the detail API call
# (e.g. "XOM"->Xometry, "CVX"->CVRx, "BA"->BAE Systems).
_DIRECT_CFD_KEYWORDS: dict = {
    "NVDA": "nvidia",      "AAPL": "apple",           "MSFT": "microsoft",
    "AVGO": "broadcom",    "AMZN": "amazon",          "GOOGL": "alphabet",
    "META": "meta",        "TSLA": "tesla",           "AMD": "advanced micro",
    "INTC": "intel",       "MU": "micron",            "DAL": "delta",
    "UAL": "united air",   "NEM": "newmont",          "AKAM": "akamai",
    "RKLB": "rocket lab",  "AEIS": "advanced energy", "SCHD": "schwab",
    "XOM": "exxon",        "CVX": "chevron",          "BA": "boeing",
    "RTX": "raytheon",     "LMT": "lockheed",         "OXY": "occidental",
    "SLB": "schlumberger", "STNG": "scorpio",         "PBI": "pitney",
    "INOD": "innodata",    "GOOG": "alphabet",        "AMGN": "amgen",
}

# Alternative company-name search terms tried after ticker-symbol search fails.
# Confirmed on IG demo (2026-07-16): XOM/CVX/BA/NEM/RTX/LMT/DAL/UAL and all other
# proxy-mapped symbols absent from demo universe (0 SHARES DFB results).
# These terms are forward-compatible for a funded live IG account.
_CFD_ALT_NAMES: dict = {
    "XOM":   ["Exxon Mobil",            "Exxon Mobil Corp"],
    "CVX":   ["Chevron",                "Chevron Corp"],
    "BA":    ["Boeing",                 "Boeing Co"],
    "NEM":   ["Newmont",                "Newmont Corp"],
    "RTX":   ["Raytheon Technologies",  "RTX Corp"],
    "LMT":   ["Lockheed Martin"],
    "OXY":   ["Occidental Petroleum",   "Occidental"],
    "SLB":   ["Schlumberger",           "SLB Ltd"],
    "DAL":   ["Delta Air Lines",        "Delta Air"],
    "UAL":   ["United Airlines",        "United Air Lines"],
    "AMZN":  ["Amazon"],
    "GOOGL": ["Alphabet"],
    "GOOG":  ["Alphabet"],
    "TSLA":  ["Tesla"],
    "AMD":   ["Advanced Micro Devices"],
    "INTC":  ["Intel Corp"],
    "MU":    ["Micron Technology"],
    "META":  ["Meta Platforms"],
    "AVGO":  ["Broadcom"],
    "STNG":  ["Scorpio Tankers"],
    "INOD":  ["Innodata"],
    "AEIS":  ["Advanced Energy Industries"],
    "AKAM":  ["Akamai Technologies"],
    "RKLB":  ["Rocket Lab"],
    "PBI":   ["Pitney Bowes"],
    "SCHD":  ["Schwab US Dividend Equity"],
    "AMGN":  ["Amgen"],
}

# Known alternative epics for symbols where IG's chartCode does not match the NYSE ticker.
# Validated 2026-07-16 via live IG account: CVX=FIO, RTX=UTX(legacy), DAL=MMR.
# Strategy 5 in _search_direct_cfd() uses these when all other strategies fail.
_CFD_ALTERNATIVE_EPICS: dict = {
    "CVX": ("SB.D.CVX.CASH.IP", "chevron"),   # chartCode=FIO — IG data quirk
    "RTX": ("SH.D.UTX.CASH.IP", "rtx"),        # chartCode=UTX — pre-merger legacy ticker
    "DAL": ("SB.D.DAL.CASH.IP", "delta"),       # chartCode=MMR — IG data quirk
}

# Bases probed proactively each poll cycle even when MS is not holding them.
# Includes known-good symbols (NVDA/AAPL/AVGO/MSFT) to recover after Redis TTL expiry.
# Includes proxy-mapped symbols that gain direct CFDs on a live IG account.
_CFD_PROACTIVE_BASES: frozenset = frozenset({
    # Known direct CFDs (re-discover after Redis expiry)
    "NVDA", "AAPL", "AVGO", "MSFT",
    # Proxy-mapped: expected absent on demo, available on funded live account
    "XOM", "CVX", "BA", "NEM", "RTX", "LMT", "DAL", "UAL",
    "OXY", "SLB", "AMZN", "GOOGL", "TSLA", "META",
    "STNG", "AKAM", "AEIS", "RKLB", "INOD", "PBI",
})

OVERNIGHT_VOL_THRESH = 1.0   # % overnight move to count an instrument as volatile
OVERNIGHT_VOL_COUNT  = 3     # instruments needed to declare high_overnight_volatility
CORR_DIVERGENCE_PCT  = 1.0   # % divergence between a pair to generate a note

# ── IG Epic codes ────────────────────────────────────────────────────────────
# Verified and corrected against IG demo API on 2026-07-13.
# verify_epics() runs on startup and calls discover_epic() for any 404s.
INSTRUMENTS: dict = {
    "EURUSD": "CS.D.EURUSD.CFD.IP",    # verified demo
    "GBPUSD": "CS.D.GBPUSD.CFD.IP",    # verified demo
    "USDJPY": "CS.D.USDJPY.CFD.IP",    # verified demo
    "SPX500": "IX.D.SPTRD.IFM.IP",     # smaller variant: US 500 Cash ($50) -- $14,802 min vs $1,850,278 for .IFD.IP
    "GER40":  "IX.D.DAX.IFD.IP",       # discovered: Germany 40 Cash (E25)
    "UK100":  "IX.D.FTSE.CFD.IP",      # verified demo
    "GOLD":   "CS.D.CFDGOLD.BMU.IP",   # smaller variant: Spot Gold ($1) -- $406 min vs $4,059 for .MFI.IP
    "SILVER": "CS.D.CFDSILVER.CFM.IP", # discovered: Mini Spot Silver (500oz)
    "OIL":    "CC.D.LCO.BMU.IP",       # smaller variant: Oil - Brent Crude ($1) -- $9,244 min vs $123,197 for .USS.IP
    # Additional forex majors/minors — eligibility checked at startup against IG min notional
    "AUDUSD": "CS.D.AUDUSD.CFD.IP",    # AUD/USD — ~$6 min notional (similar lot structure to EURUSD)
    "USDCAD": "CS.D.USDCAD.CFD.IP",    # USD/CAD — ~$14 min notional
    "EURGBP": "CS.D.EURGBP.CFD.IP",    # EUR/GBP — GBP-denominated, ~$11 min notional
    "NZDUSD": "CS.D.NZDUSD.CFD.IP",    # NZD/USD — ~$6 min notional
    "USDCHF": "CS.D.USDCHF.CFD.IP",    # USD/CHF — ~$9 min notional
    # US equity CFDs — priced via Finnhub REST (IG REST returns streamingPricesAvailable=False)
    # Epics confirmed from direct_cfd_map on live IG account
    "NVDA": "UC.D.NVDA.CASH.IP",       # NVIDIA Corp
    "TSLA": "UD.D.TSLA.CASH.IP",       # Tesla Inc
    "AAPL": "UA.D.AAPL.CASH.IP",       # Apple Inc
    "MSFT": "UC.D.MSFT.CASH.IP",       # Microsoft Corp
    "AMD":  "SA.D.AMD.CASH.IP",        # Advanced Micro Devices
    "INTC": "UB.D.INTC.CASH.IP",       # Intel Corp
    "MU":   "UC.D.MU.CASH.IP",         # Micron Technology
}

# Reverse lookup: epic → base symbol (used to route .CASH.IP epics to Finnhub)
_INSTRUMENTS_REVERSE: dict = {v: k for k, v in INSTRUMENTS.items()}

_SEARCH_FALLBACKS: dict = {
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "SPX500": "US 500",
    "GER40":  "Germany 40",
    "UK100":  "UK 100",
    "GOLD":   "Gold",
    "SILVER": "Silver",
    "OIL":    "Brent Crude",
    "AUDUSD": "AUD/USD",
    "USDCAD": "USD/CAD",
    "EURGBP": "EUR/GBP",
    "NZDUSD": "NZD/USD",
    "USDCHF": "USD/CHF",
}

# Historically correlated pairs to watch for overnight divergence.
# (sym_a, sym_b, plain-English description of expected correlation)
CORR_PAIRS = [
    ("GOLD",   "SILVER",  "Gold and Silver normally move together"),
    ("OIL",    "GBPUSD",  "Oil weakness often correlates with GBP weakness"),
    ("SPX500", "USDJPY",  "SPX500 and USD/JPY are a risk-on/risk-off proxy"),
]


# ── T212 to June instrument reverse mapping (for exit warning monitor) ────────
# Derived from JUNE_INSTRUMENT_SECTORS / _JUNE_GAP_T212 in alert_system.py.
# Maps T212 base ticker -> June instrument name for CFD deterioration crosscheck.
T212_TO_JUNE_INSTRUMENT: dict = {
    # Gold proxies
    "NEM": "GOLD", "GDX": "GOLD", "GLD": "GOLD", "GDX3": "GOLD", "SGDE": "GOLD",
    # Silver proxies
    "AG": "SILVER", "PAAS": "SILVER", "SLV": "SILVER", "SILG": "SILVER",
    "SSLN": "SILVER", "3LSI": "SILVER",
    # Oil proxies
    "XOM": "OIL", "CVX": "OIL", "USO": "OIL", "3OIL": "OIL",
    "OXY": "OIL", "SLB": "OIL", "STNG": "OIL",
    # S&P 500 proxies
    "SPY": "SPX500", "VTI": "SPX500", "XS2D": "SPX500",
    "SPXU": "SPX500",
    # QQQ proxies -> SPX500 (no Nasdaq CFD, use SPX500 as broad tech proxy)
    "QQQ": "SPX500", "QQQ3": "SPX500", "SQQQ": "SPX500",
    # Europe/UK proxies
    "CSPX": "UK100", "IWDA": "UK100", "EQQQ": "GER40",
    # Tech via SPX500
    "NVDA": "SPX500", "AAPL": "SPX500", "MSFT": "SPX500",
    "AMD": "SPX500", "INTC": "SPX500", "MU": "SPX500",
    # Finance via SPX500
    "XLF": "SPX500", "XLF3": "SPX500", "XL3S": "SPX500",
    # Defense via UK100/GER40 (3EDF is European Defence ETF)
    "BA": "SPX500", "RTX": "SPX500", "LMT": "SPX500",
    "3EDF": "UK100",
}

# Reverse: June instrument -> T212 equivalents for exit warning t212_equivalents field
_JUNE_TO_T212_EQUIVALENTS: dict = {}
for _sym, _inst in T212_TO_JUNE_INSTRUMENT.items():
    _JUNE_TO_T212_EQUIVALENTS.setdefault(_inst, []).append(_sym)

# Three-way confirmation instrument groups for macro regime detection
_THREE_WAY_GROUPS = [
    {"name": "safe_haven",     "instruments": ["GOLD", "SILVER", "USDJPY"],
     "direction": "up",  "note": "Safe-haven flight: Gold + Silver + JPY strength"},
    {"name": "risk_on",        "instruments": ["SPX500", "GER40", "GBPUSD"],
     "direction": "up",  "note": "Risk-on: equities + GBP rising together"},
    {"name": "commodity_bull", "instruments": ["GOLD", "SILVER", "OIL"],
     "direction": "up",  "note": "Commodity bull: Gold + Silver + Oil rising"},
    {"name": "dollar_strength", "instruments": ["USDJPY", "EURUSD", "GBPUSD"],
     "direction": "mixed", "note": "Dollar strength: USDJPY up while EURUSD + GBPUSD down"},
]

# ── Module-level state ───────────────────────────────────────────────────────

# IG session
_sess: dict      = {"cst": None, "token": None, "born": 0.0}
_live_sess: dict = {"cst": None, "token": None, "born": 0.0}
_live_available: bool = False  # True after successful live account auth

# Rolling price history: {sym: deque([(epoch, mid), ...])}
_history: dict = {sym: deque(maxlen=HISTORY_LEN) for sym in INSTRUMENTS}
_no_history_warned: set = set()   # instruments logged no-5m-history this run

# Rolling spread history: {sym: deque([spread_pct, ...])} — ~1h at 60s
_spread_hist: dict = {sym: deque(maxlen=SPREAD_HISTORY_LEN) for sym in INSTRUMENTS}

# Overnight state: reset each time NY close is captured (21:00 UTC)
_overnight: dict = {
    "ny_close": {},   # {sym: price} — snapshot at 21:00 UTC
    "high":     {},   # {sym: peak_mid} since overnight start
    "low":      {},   # {sym: trough_mid} since overnight start
}

# Pre-London baseline: price at 06:00 UTC for gap_pct computation
_premarket_baseline: dict = {}   # {sym: price}

# Daily flags — date strings prevent re-firing per-day actions; reset at midnight UTC
_flags: dict = {
    "date":             "",   # current UTC date (detects midnight rollover)
    "ny_close_date":    "",   # date NY close was captured
    "premarket_date":   "",   # date 06:00 pre-London baseline was captured
    "morning_pub_date": "",   # date 06:55 morning publish completed
    "gap_syms":         set(),# symbols already published to june_premarket_gaps today
}

# Maintenance backoff state
_maint: dict = {"consec": 0, "since": 0.0}

# Overnight news cache — populated by fetch_overnight_news() once per hour during 21:00-07:00 UTC
_overnight_news_cache:      dict  = {}   # {instrument.lower(): {sentiment, headline}} + optional notable_risk
_last_overnight_news_fetch: float = 0.0  # epoch of last successful fetch

# ── Direct CFD discovery state ────────────────────────────────────────────────
_direct_cfd_map: dict      = {}    # {t212_base: epic}  e.g. {"NVDA": "UC.D.NVDA.CASH.IP"}
_cfd_miss_cache: dict      = {}    # {t212_base: expiry_epoch}  6h cache for not-found
_cfd_last_search: float    = 0.0   # epoch of last IG search call (rate-limiter)
_direct_cfd_signals: dict  = {}    # {t212_base: {pct, direction, ts}}  daily snapshots
_cfd_signal_refresh: float = 0.0   # epoch of last signal refresh pass
_gap_disc_date: str        = ""    # UTC date _gap_disc_seen was reset
_gap_disc_seen: set        = set() # bases already published to june_gap_discoveries today
_nav_cache: dict           = {}    # {chartCode.upper(): epic} from /marketnavigation
_nav_tried: bool           = False  # True once navigation was attempted this session
_sync_state: dict          = {}     # {base: {agree: int, disagree: int}}
_sync_last_check: float    = 0.0   # epoch of last sync check pass


# ── Redis ────────────────────────────────────────────────────────────────────
def _redis() -> redis_lib.Redis:
    return redis_lib.Redis(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


# ── LLM helper (foundation for AI P&L monitor — not yet wired) ──────────────
def _call_llm(provider: str, prompt: str, max_tokens: int = 200) -> str:
    """Generic LLM caller. Raises on failure. Not wired into any active path yet.
    Providers: openrouter | anthropic | groq | gemini
    """
    if provider == "openrouter":
        if not OPENROUTER_API_KEY:
            raise RuntimeError("OPENROUTER_API_KEY not set")
        r = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
            json={"model": "openrouter/free", "messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens},
            timeout=12,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

    elif provider == "anthropic":
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY not set")
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "Content-Type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": max_tokens, "messages": [{"role": "user", "content": prompt}]},
            timeout=10,
        )
        r.raise_for_status()
        blocks = [b["text"] for b in r.json()["content"] if b.get("type") == "text"]
        return " ".join(blocks)

    elif provider == "groq":
        groq_key = os.environ.get("GROQ_API_KEY", "")
        if not groq_key:
            raise RuntimeError("GROQ_API_KEY not set")
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={"model": "openai/gpt-oss-120b", "messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens, "temperature": 0},
            timeout=8,
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

    elif provider == "gemini":
        if not GEMINI_API_KEY:
            raise RuntimeError("GEMINI_API_KEY not set")
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}",
            headers={"Content-Type": "application/json"},
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=8,
        )
        r.raise_for_status()
        return r.json()["candidates"][0]["content"]["parts"][0]["text"]

    else:
        raise ValueError(f"Unknown LLM provider: {provider!r}")


# ── IG session management ────────────────────────────────────────────────────
def _base_headers(with_auth: bool = True) -> dict:
    h = {
        "X-IG-API-KEY":  IG_API_KEY,
        "Content-Type":  "application/json; charset=UTF-8",
        "Accept":        "application/json; charset=UTF-8",
    }
    if with_auth and _sess.get("cst"):
        h["CST"]              = _sess["cst"]
        h["X-SECURITY-TOKEN"] = _sess["token"]
    return h


def authenticate() -> bool:
    """POST /session (Version: 2) — capture CST + X-SECURITY-TOKEN."""
    url = f"{IG_BASE}/session"
    body = {"identifier": IG_USER, "password": IG_PASS, "encryptedPassword": False}
    headers = {**_base_headers(False), "Version": "2"}
    try:
        r = requests.post(url, json=body, headers=headers, timeout=15)
        if r.status_code == 200:
            _sess["cst"]   = r.headers.get("CST")
            _sess["token"] = r.headers.get("X-SECURITY-TOKEN")
            _sess["born"]  = time.time()
            acct_type = r.json().get("accountType", "UNKNOWN")
            print(f"[{_ts()}] ✅ IG authenticated — account {IG_ACCOUNT} ({acct_type})", flush=True)
            return True
        print(f"[{_ts()}] ❌ IG auth HTTP {r.status_code}: {r.text[:300]}", flush=True)
        return False
    except Exception as exc:
        print(f"[{_ts()}] ❌ IG auth error: {exc}", flush=True)
        return False


def maybe_refresh():
    """Re-authenticate if we have a session approaching the 6h expiry."""
    if not _sess.get("cst"):
        return
    if time.time() - _sess["born"] > SESSION_MAX:
        print(f"[{_ts()}] 🔄 Session approaching 6h expiry — refreshing tokens", flush=True)
        authenticate()


# ── IG API calls ─────────────────────────────────────────────────────────────
def _ig_get(path: str, params: Optional[dict] = None, version: str = "1") -> Optional[dict]:
    """Authenticated GET; handles 401 re-auth and 429 backoff."""
    maybe_refresh()
    url = f"{IG_BASE}{path}"
    headers = {**_base_headers(), "Version": version}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code == 429:
            print(f"[{_ts()}] ⚠️  IG rate limit (429) — backing off 120s", flush=True)
            time.sleep(120)
            return None
        if r.status_code == 401:
            print(f"[{_ts()}] 🔄 IG 401 — re-authenticating", flush=True)
            if not authenticate():
                return None
            headers = {**_base_headers(), "Version": version}
            r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        print(f"[{_ts()}] ⚠️  {path}: HTTP {r.status_code} {r.text[:120]}", flush=True)
        return None
    except Exception as exc:
        print(f"[{_ts()}] ⚠️  {path} error: {exc}", flush=True)
        return None



def authenticate_live() -> bool:
    # Authenticate with IG live account (W-8BEN activated).
    # Intelligence-only: no orders placed via live endpoint under any circumstances.
    global _live_available
    if not all([IG_LIVE_BASE, IG_LIVE_KEY, IG_LIVE_USER, IG_LIVE_PASS]):
        print(f"[{_ts()}] ℹ️  Live account not configured — demo only", flush=True)
        return False
    url  = f"{IG_LIVE_BASE}/session"
    body = {"identifier": IG_LIVE_USER, "password": IG_LIVE_PASS, "encryptedPassword": False}
    hdrs = {
        "X-IG-API-KEY":  IG_LIVE_KEY,
        "Content-Type":  "application/json; charset=UTF-8",
        "Accept":        "application/json; charset=UTF-8",
        "Version":       "2",
    }
    try:
        r = requests.post(url, json=body, headers=hdrs, timeout=15)
        if r.status_code == 200:
            data = r.json()
            _live_sess["cst"]   = r.headers.get("CST")
            _live_sess["token"] = r.headers.get("X-SECURITY-TOKEN")
            _live_sess["born"]  = time.time()
            _live_available = True
            acct_id   = data.get("currentAccountId", "?")
            acct_type = data.get("accountType", "?")
            avail     = data.get("accountInfo", {}).get("available", 0)
            ccy       = data.get("currencyIsoCode", "USD")
            print(
                f"[{_ts()}] ✅ Live account connected: {IG_LIVE_USER} "
                f"(account {acct_id}, {acct_type}, balance: {avail} {ccy}) "
                f"— intelligence mode only (no orders placed)",
                flush=True,
            )
            return True
        print(
            f"[{_ts()}] ℹ️  Live account auth failed (HTTP {r.status_code}) — demo only",
            flush=True,
        )
        _live_available = False
        return False
    except Exception as exc:
        print(f"[{_ts()}] ℹ️  Live account unavailable: {exc} — demo only", flush=True)
        _live_available = False
        return False


def _ensure_live_session() -> bool:
    # Return True if live session tokens are fresh; re-auth if near 6h expiry.
    global _live_available
    if not _live_available:
        return False
    if not _live_sess.get("cst"):
        return False
    if time.time() - _live_sess.get("born", 0) > SESSION_MAX:
        print(f"[{_ts()}] 🔄 Live session near 6h expiry — refreshing", flush=True)
        return authenticate_live()
    return True


def _ig_live_get(path: str, params: Optional[dict] = None, version: str = "1") -> Optional[dict]:
    # GET against IG live endpoint. Intelligence-only, no orders.
    # Mirrors _ig_get() using live base URL and live session tokens.
    # 403/429 silently return None (rate-limit recovery expected).
    if not _ensure_live_session():
        return None
    url  = f"{IG_LIVE_BASE}{path}"
    hdrs = {
        "X-IG-API-KEY":     IG_LIVE_KEY,
        "CST":              _live_sess["cst"],
        "X-SECURITY-TOKEN": _live_sess["token"],
        "Content-Type":     "application/json; charset=UTF-8",
        "Accept":           "application/json; charset=UTF-8",
        "Version":          version,
    }
    try:
        r = requests.get(url, headers=hdrs, params=params, timeout=10)
        if r.status_code == 401:
            print(f"[{_ts()}] 🔄 Live 401 — re-authenticating", flush=True)
            if not authenticate_live():
                return None
            hdrs["CST"]              = _live_sess["cst"]
            hdrs["X-SECURITY-TOKEN"] = _live_sess["token"]
            r = requests.get(url, headers=hdrs, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        if r.status_code not in (403, 429):
            print(f"[{_ts()}] ⚠️  LIVE {path}: HTTP {r.status_code} {r.text[:80]}", flush=True)
        return None
    except Exception as exc:
        print(f"[{_ts()}] ⚠️  LIVE {path} error: {exc}", flush=True)
        return None


def _finnhub_price(sym: str) -> Optional[dict]:
    """Return bid/offer/mid/spread for a US equity using Finnhub REST.
    Used for .CASH.IP epics where IG REST returns streamingPricesAvailable=False.
    Spread is synthetic: max(1% of day range, 0.05% of mid).
    """
    if not FINNHUB_KEY:
        return None
    try:
        r = requests.get(
            f"https://finnhub.io/api/v1/quote?symbol={sym}&token={FINNHUB_KEY}",
            timeout=8,
        )
        if r.status_code != 200:
            print(f"[{_ts()}] ⚠️  Finnhub {sym}: HTTP {r.status_code}", flush=True)
            return None
        d = r.json()
        mid = float(d.get("c") or 0.0)
        if mid <= 0:
            return None
        h      = float(d.get("h") or mid)
        l      = float(d.get("l") or mid)
        # Synthetic spread: 1% of day range, floored at 0.05% of mid
        spread = max((h - l) * 0.01, mid * 0.0005)
        half   = spread / 2.0
        return {"bid": mid - half, "offer": mid + half, "mid": mid, "spread": spread}
    except Exception as exc:
        print(f"[{_ts()}] ⚠️  Finnhub {sym}: {exc}", flush=True)
        return None


def fetch_price(epic: str) -> Optional[dict]:
    """Return bid/offer/mid/spread for an epic, or None on any error."""
    if ".CASH." in epic:
        sym = _INSTRUMENTS_REVERSE.get(epic)
        return _finnhub_price(sym) if sym else None
    data = _ig_get(f"/markets/{epic}")
    if not data:
        return None
    snap  = data.get("snapshot", {})
    bid   = snap.get("bid")
    offer = snap.get("offer")
    if bid is None or offer is None:
        return None
    bid, offer = float(bid), float(offer)
    mid = (bid + offer) / 2.0
    return {"bid": bid, "offer": offer, "mid": mid, "spread": offer - bid}


def discover_epic(sym: str) -> Optional[str]:
    """Search /markets?searchTerm= for a symbol and return the best-matching epic."""
    term = _SEARCH_FALLBACKS.get(sym, sym)
    data = _ig_get("/markets", params={"searchTerm": term})
    if not data:
        return None
    markets = data.get("markets", [])
    if not markets:
        print(f"[{_ts()}]   🔍 No results for '{term}'", flush=True)
        return None
    # Exclude share CFDs (.CASH. epics) which pollute commodity/metal searches
    non_stock = [m for m in markets if ".CASH." not in m.get("epic", "")]
    cfd  = [m for m in non_stock if "CFD" in m.get("instrumentType", "")]
    pool = cfd if cfd else (non_stock if non_stock else markets)
    best = pool[0]
    epic = best.get("epic", "")
    name = best.get("instrumentName", "?")
    print(f"[{_ts()}]   🔍 '{term}' → {epic} ({name})", flush=True)
    return epic if epic else None


# ── Startup epic verification ─────────────────────────────────────────────────
def verify_epics():
    """Verify each configured epic against IG API; discover replacements for failures."""
    print(f"[{_ts()}] 🔍 Verifying IG epic codes ({len(INSTRUMENTS)} instruments)...", flush=True)
    failed = []
    for sym, epic in list(INSTRUMENTS.items()):
        result = fetch_price(epic)
        if result:
            print(f"[{_ts()}]   ✅ {sym:7s} {epic:30s} mid={result['mid']:.5f}", flush=True)
        else:
            print(f"[{_ts()}]   ❌ {sym:7s} {epic:30s} FAILED", flush=True)
            failed.append(sym)
        time.sleep(1.5)

    for sym in failed:
        print(f"[{_ts()}]   🔍 Attempting discovery for {sym}...", flush=True)
        new_epic = discover_epic(sym)
        if new_epic:
            INSTRUMENTS[sym] = new_epic
            if sym not in _history:
                _history[sym] = deque(maxlen=HISTORY_LEN)
            if sym not in _spread_hist:
                _spread_hist[sym] = deque(maxlen=SPREAD_HISTORY_LEN)
            print(f"[{_ts()}]   🔁 {sym} epic updated → {new_epic}", flush=True)
        else:
            print(f"[{_ts()}]   ⚠️  {sym} could not be verified — dropping from this session", flush=True)
            INSTRUMENTS.pop(sym, None)
            _history.pop(sym, None)
            _spread_hist.pop(sym, None)

    print(f"[{_ts()}] ✅ Epic verification complete — {len(INSTRUMENTS)} active instruments", flush=True)


# ── Market session helpers ────────────────────────────────────────────────────
def is_weekend_closure() -> bool:
    """True during IG CFD weekend closure: Fri 21:15 UTC → Sun 21:00 UTC."""
    now  = datetime.now(timezone.utc)
    dow  = now.weekday()   # 0=Mon … 4=Fri, 5=Sat, 6=Sun
    mins = now.hour * 60 + now.minute
    if dow == 5:                                      # full Saturday
        return True
    if dow == 4 and mins >= WEEKEND_CLOSE_MIN:        # Friday after 21:15 UTC
        return True
    if dow == 6 and mins < WEEKEND_OPEN_MIN:          # Sunday before 21:00 UTC
        return True
    return False


def _now_mins() -> int:
    now = datetime.now(timezone.utc)
    return now.hour * 60 + now.minute


def is_overnight() -> bool:
    """True between 21:00 UTC (NY close) and 07:00 UTC (London open)."""
    m = _now_mins()
    return m >= OVERNIGHT_START_MIN or m < OVERNIGHT_END_MIN


def is_premarket() -> bool:
    """True during the 06:00-07:00 UTC pre-London gap window."""
    m = _now_mins()
    return PREMARKET_START_MIN <= m < PREMARKET_END_MIN


# ── Maintenance backoff ───────────────────────────────────────────────────────
def in_maintenance() -> bool:
    """True while maintenance backoff is active (resets after MAINT_MAX_SECS)."""
    if _maint["since"] == 0.0:
        return False
    if time.time() - _maint["since"] > MAINT_MAX_SECS:
        print(f"[{_ts()}] ⚙️  Maintenance max duration ({MAINT_MAX_SECS//60} min) reached — forcing resume", flush=True)
        _maint["since"]  = 0.0
        _maint["consec"] = 0
        return False
    return True


def update_maintenance(had_prices: bool):
    """Track consecutive empty cycles; enter or exit maintenance backoff."""
    if not had_prices:
        _maint["consec"] += 1
        if _maint["consec"] >= MAINT_CONSEC_THRESHOLD and _maint["since"] == 0.0:
            _maint["since"] = time.time()
            print(
                f"[{_ts()}] ⚙️  Maintenance pattern detected "
                f"({_maint['consec']} consecutive empty cycles) "
                f"— backing off to {POLL_MAINT // 60}-min polling (max {MAINT_MAX_SECS // 60} min)",
                flush=True,
            )
    else:
        if _maint["since"] > 0.0:
            elapsed = int(time.time() - _maint["since"])
            print(f"[{_ts()}] ✅ Prices resumed after {elapsed}s — exiting maintenance, resuming {POLL_ACTIVE}s polling", flush=True)
        _maint["consec"] = 0
        _maint["since"]  = 0.0


# ── Daily flag management ─────────────────────────────────────────────────────
def _maybe_reset_daily_flags():
    """At midnight UTC, reset per-day flags so actions can re-fire."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _flags["date"] != today:
        _flags["date"]      = today
        _flags["gap_syms"]  = set()
        # ny_close_date / premarket_date / morning_pub_date intentionally not reset here —
        # they track whether today's capture already happened and are keyed by date string.


# ── Overnight state capture ───────────────────────────────────────────────────
def maybe_capture_ny_close():
    """At 21:00 UTC on weekdays, snapshot current prices as the NY close baseline."""
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return
    if _now_mins() < OVERNIGHT_START_MIN:
        return
    today = now.strftime("%Y-%m-%d")
    if _flags["ny_close_date"] == today:
        return

    captured = []
    for sym, hist in _history.items():
        if hist:
            _overnight["ny_close"][sym] = hist[-1][1]
            captured.append(sym)
    _overnight["high"].clear()
    _overnight["low"].clear()
    _flags["ny_close_date"] = today
    print(f"[{_ts()}] 🌙 NY close prices captured for {len(captured)} instruments — overnight tracking active", flush=True)


def maybe_capture_premarket_baseline():
    """In the 06:00-06:05 UTC window, snapshot prices as the pre-London gap baseline."""
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return
    m = _now_mins()
    if not (PREMARKET_START_MIN <= m < PREMARKET_START_MIN + 5):
        return
    today = now.strftime("%Y-%m-%d")
    if _flags["premarket_date"] == today:
        return

    _premarket_baseline.clear()
    captured = []
    for sym, hist in _history.items():
        if hist:
            _premarket_baseline[sym] = hist[-1][1]
            captured.append(sym)
    _flags["premarket_date"] = today
    print(f"[{_ts()}] 🌅 Pre-London 06:00 baseline captured ({len(captured)} instruments)", flush=True)


# ── Morning publish orchestration ─────────────────────────────────────────────
def maybe_publish_morning():
    """In the 06:55-07:00 UTC window, publish morning baseline + overnight context."""
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return
    m = _now_mins()
    if not (MORNING_PUB_START_MIN <= m < MORNING_PUB_END_MIN):
        return
    today = now.strftime("%Y-%m-%d")
    if _flags["morning_pub_date"] == today:
        return

    _publish_morning_baseline()
    _publish_overnight_context()
    _flags["morning_pub_date"] = today


# ── Morning baseline publish ──────────────────────────────────────────────────
def _publish_morning_baseline():
    """Publish june_morning_baseline: per-instrument overnight price summary."""
    baselines = {}
    for sym in INSTRUMENTS:
        ny_close = _overnight["ny_close"].get(sym)
        hist     = _history.get(sym)
        if ny_close is None or not hist:
            continue
        current    = hist[-1][1]
        chg        = (current - ny_close) / ny_close * 100.0 if ny_close else 0.0
        high       = _overnight["high"].get(sym, current)
        low        = _overnight["low"].get(sym, current)
        direction  = "bull" if chg > 0.05 else "bear" if chg < -0.05 else "neutral"
        baselines[sym] = {
            "ny_close_price":       round(ny_close, 6),
            "current_price":        round(current, 6),
            "overnight_change_pct": round(chg, 4),
            "overnight_high":       round(high, 6),
            "overnight_low":        round(low, 6),
            "direction":            direction,
        }

    payload = {"timestamp": int(time.time()), "baselines": baselines}
    try:
        r = _redis()
        r.set("june_morning_baseline", json.dumps(payload), ex=2 * 3600)
        print(f"[{_ts()}] 🌅 Morning baseline published ({len(baselines)} instruments, TTL 2h)", flush=True)
        for sym, b in baselines.items():
            arrow = "↑" if b["direction"] == "bull" else "↓" if b["direction"] == "bear" else "→"
            print(
                f"[{_ts()}]    {sym}: NY close {b['ny_close_price']} → now {b['current_price']} "
                f"({b['overnight_change_pct']:+.3f}%) {arrow}  "
                f"[H:{b['overnight_high']} L:{b['overnight_low']}]",
                flush=True,
            )
    except Exception as exc:
        print(f"[{_ts()}] ❌ Redis error (morning_baseline): {exc}", flush=True)


# ── Overnight context publish ─────────────────────────────────────────────────
# ── Overnight news intelligence ───────────────────────────────────────────────
# Marketaux equity proxies for June's macro instruments.
# Forex pairs queried directly; commodities/indices via ETF proxies.
_JUNE_TO_MARKETAUX: dict = {
    "GOLD":   "GLD",     "SILVER": "SLV",    "OIL":    "USO",
    "EURUSD": "EURUSD",  "GBPUSD": "GBPUSD", "USDJPY": "USDJPY",
    "SPX500": "SPY",     "UK100":  "EWU",    "GER40":  "EWG",
}
_MARKETAUX_TO_JUNE: dict = {v: k for k, v in _JUNE_TO_MARKETAUX.items()}

_KEY_EARNINGS_SYMS = {
    "NVDA","AAPL","MSFT","AMD","AMZN","GOOGL","META","TSLA",
    "XOM","CVX","BA","LMT","RTX","NEM","GDX","XLF","AMGN",
}


def fetch_overnight_news() -> dict:
    """
    Fetch overnight news from Marketaux for June's macro instruments.
    Returns {instrument.lower(): {sentiment, headline}} + optional notable_risk.
    Respects shared marketaux_daily_calls Redis counter (also incremented by Claudia).
    """
    if not MARKETAUX_API_KEY:
        return {}
    try:
        r = _redis()
        _mx_count = int(r.get("marketaux_daily_calls") or 0)
        if _mx_count >= 90:
            print(f"[{_ts()}] ⚠️ Marketaux daily limit ({_mx_count}/90) — skipping overnight news", flush=True)
            return {}

        _now_utc = datetime.now(timezone.utc)
        _tmr_utc = (_now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        _ttl     = max(60, int((_tmr_utc - _now_utc).total_seconds()))
        _since   = (_now_utc - timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M")

        resp = requests.get(
            "https://api.marketaux.com/v1/news/all",
            params={"symbols": ",".join(_JUNE_TO_MARKETAUX.values()), "filter_entities": "true",
                    "language": "en", "published_after": _since, "limit": 10,
                    "api_token": MARKETAUX_API_KEY},
            timeout=12,
        )
        if resp.status_code != 200:
            print(f"[{_ts()}] ⚠️ Marketaux overnight: HTTP {resp.status_code}", flush=True)
            return {}

        _new_count = r.incr("marketaux_daily_calls")
        r.expire("marketaux_daily_calls", _ttl)
        print(f"[{_ts()}] 📰 Marketaux overnight news: {_new_count}/90 daily calls used", flush=True)

        news_by_instrument: dict = {}
        for article in resp.json().get("data", []):
            title = article.get("title", "")
            for entity in article.get("entities", []):
                sym   = entity.get("symbol", "")
                score = entity.get("sentiment_score")
                if sym not in _MARKETAUX_TO_JUNE or score is None or abs(score) < 0.4:
                    continue
                june_instr = _MARKETAUX_TO_JUNE[sym].lower()
                if june_instr not in news_by_instrument or abs(score) > abs(news_by_instrument[june_instr]["sentiment"]):
                    news_by_instrument[june_instr] = {"sentiment": round(score, 3), "headline": title[:120]}

        # Add earnings risk from Redis cache (written by Claudia's morning Finnhub call)
        try:
            _er = r.get("finnhub_earnings")
            if _er:
                _e_list = json.loads(_er)
                _today  = _now_utc.strftime("%Y-%m-%d")
                _tmr_s  = (_now_utc + timedelta(days=1)).strftime("%Y-%m-%d")
                _risky  = [e["symbol"] for e in _e_list
                           if e.get("symbol") in _KEY_EARNINGS_SYMS and e.get("date") in (_today, _tmr_s)]
                if _risky:
                    news_by_instrument["notable_risk"] = (
                        f"Earnings: {', '.join(_risky[:3])} report today/tomorrow — monitor sector exposure"
                    )
        except Exception:
            pass

        if news_by_instrument:
            print(f"[{_ts()}] 📰 Overnight news: {len(news_by_instrument)} instrument signals", flush=True)
        return news_by_instrument

    except Exception as exc:
        print(f"[{_ts()}] ⚠️ fetch_overnight_news failed: {exc}", flush=True)
        return {}


def maybe_fetch_overnight_news():
    """Once per hour during overnight (21:00-07:00 UTC), refresh Marketaux news cache."""
    global _last_overnight_news_fetch, _overnight_news_cache
    if not is_overnight():
        return
    if time.time() - _last_overnight_news_fetch < 3600:
        return
    news = fetch_overnight_news()
    if news:
        _overnight_news_cache = news
    _last_overnight_news_fetch = time.time()


def _publish_overnight_context():
    """Publish june_overnight_context: volatility regime and correlation notes."""
    overnight_moves = {}
    for sym in INSTRUMENTS:
        ny_close = _overnight["ny_close"].get(sym)
        hist     = _history.get(sym)
        if ny_close and hist:
            current = hist[-1][1]
            overnight_moves[sym] = round((current - ny_close) / ny_close * 100.0, 4)

    volatile_syms = [s for s, m in overnight_moves.items() if abs(m) >= OVERNIGHT_VOL_THRESH]
    high_vol      = len(volatile_syms) >= OVERNIGHT_VOL_COUNT

    if high_vol:
        print(f"[{_ts()}] ⚡ High overnight volatility detected: {volatile_syms}", flush=True)

    corr_note = _detect_correlation_shifts(overnight_moves)

    payload = {
        "timestamp":                int(time.time()),
        "high_overnight_volatility": high_vol,
        "volatile_instruments":     volatile_syms,
        "overnight_moves":          overnight_moves,
        "correlation_note":         corr_note,
        "news_summary":             _overnight_news_cache,
    }
    try:
        r = _redis()
        r.set("june_overnight_context", json.dumps(payload), ex=4 * 3600)
        print(
            f"[{_ts()}] ⚡ Overnight context published "
            f"(high_vol={high_vol}, volatile={volatile_syms})",
            flush=True,
        )
        if corr_note:
            print(f"[{_ts()}]    correlation_note: \"{corr_note}\"", flush=True)
    except Exception as exc:
        print(f"[{_ts()}] ❌ Redis error (overnight_context): {exc}", flush=True)


# ── Correlation shift detection ───────────────────────────────────────────────
def _detect_correlation_shifts(overnight_moves: dict) -> str:
    """Return a plain-English note about unusual overnight pair divergences."""
    notes = []
    for sym_a, sym_b, description in CORR_PAIRS:
        move_a = overnight_moves.get(sym_a)
        move_b = overnight_moves.get(sym_b)
        if move_a is None or move_b is None:
            continue
        divergence = abs(move_a - move_b)
        if divergence < CORR_DIVERGENCE_PCT:
            continue
        dir_a = f"{'up' if move_a >= 0 else 'down'} {abs(move_a):.2f}%"
        dir_b = f"{'up' if move_b >= 0 else 'down'} {abs(move_b):.2f}%"
        if (move_a >= 0) != (move_b >= 0):
            note = f"{sym_a} {dir_a} while {sym_b} {dir_b} — {description.lower()}, unusual decoupling"
        else:
            note = f"{sym_a} {dir_a} vs {sym_b} {dir_b} — {description.lower()}, unusual magnitude divergence"
        notes.append(note)
        print(f"[{_ts()}] 🔗 Correlation shift: {note}", flush=True)
    return "; ".join(notes)


# ── Pre-London gap detection ──────────────────────────────────────────────────
def _check_premarket_gaps(current_mids: dict):
    """Compare current prices to the 06:00 baseline; publish gaps > 0.5%."""
    if not _premarket_baseline:
        return
    new_gaps = {}
    for sym, baseline in _premarket_baseline.items():
        current = current_mids.get(sym)
        if current is None or baseline <= 0 or sym in _flags["gap_syms"]:
            continue
        gap_pct = (current - baseline) / baseline * 100.0
        if abs(gap_pct) < PREMARKET_GAP_PCT:
            continue
        direction = "bull" if gap_pct > 0 else "bear"
        new_gaps[sym] = {"gap_pct": round(gap_pct, 4), "direction": direction, "price": round(current, 6)}
        _flags["gap_syms"].add(sym)
        print(
            f"[{_ts()}] 🌅 Pre-London gap detected: {sym} {gap_pct:+.2f}% ({direction}) "
            f"— publishing to june_premarket_gaps",
            flush=True,
        )

    if not new_gaps:
        return
    # Merge with any gaps already published today (TTL may still be live)
    try:
        r       = _redis()
        existing = r.get("june_premarket_gaps")
        if existing:
            prev = json.loads(existing).get("gaps", {})
            prev.update(new_gaps)
            new_gaps = prev
        r.set("june_premarket_gaps", json.dumps({"timestamp": int(time.time()), "gaps": new_gaps}), ex=3 * 3600)
    except Exception as exc:
        print(f"[{_ts()}] ❌ Redis error (premarket_gaps): {exc}", flush=True)


# ── Spread baselines publish ───────────────────────────────────────────────────
def _publish_spread_baselines(spread_avgs: dict):
    """Publish rolling 1-hour spread averages to june_spread_baselines (TTL 25h)."""
    payload = {"timestamp": int(time.time()), "baselines": {s: round(a, 6) for s, a in spread_avgs.items()}}
    try:
        r = _redis()
        r.set("june_spread_baselines", json.dumps(payload), ex=25 * 3600)
    except Exception as exc:
        print(f"[{_ts()}] ❌ Redis error (spread_baselines): {exc}", flush=True)


# ── Signal computation ────────────────────────────────────────────────────────
def _price_n_minutes_ago(sym: str, minutes: float) -> Optional[float]:
    """Find the price reading closest to N minutes ago in the rolling buffer."""
    hist = _history.get(sym)
    if not hist:
        return None
    target     = time.time() - minutes * 60.0
    candidates = [(abs(ep - target), px) for ep, px in hist if ep <= target]
    if not candidates:
        return None
    return min(candidates, key=lambda x: x[0])[1]


def compute_signal(sym: str, price: dict, spread_alert: bool = False) -> dict:
    mid        = price["mid"]
    spread_pct = (price["spread"] / mid * 100.0) if mid > 0 else 0.0
    px5        = _price_n_minutes_ago(sym, 5)
    px15       = _price_n_minutes_ago(sym, 15)
    change_5m  = ((mid - px5)  / px5  * 100.0) if px5  is not None else None
    change_15m = ((mid - px15) / px15 * 100.0) if px15 is not None else None
    if change_5m is None:
        direction = "no_history"
    elif change_5m >  0.05:
        direction = "bull"
    elif change_5m < -0.05:
        direction = "bear"
    else:
        direction = "neutral"
    return {
        "price":        round(mid, 6),
        "change_5m":    (round(change_5m,  4) if change_5m  is not None else None),
        "change_15m":   (round(change_15m, 4) if change_15m is not None else None),
        "direction":    direction,
        "spread_pct":   round(spread_pct, 4),
        "spread_alert": spread_alert,
    }


# ── Poll cycle ────────────────────────────────────────────────────────────────

# ── Direct CFD discovery and gap detection ────────────────────────────────────

def is_us_premarket() -> bool:
    """True during US pre-market: 10:00–14:30 UTC (05:00–09:30 ET)."""
    m = _now_mins()
    return US_PREMARKET_START_MIN <= m < US_PREMARKET_END_MIN


def _load_direct_cfd_cache() -> None:
    """Load previously discovered direct CFD epics from Redis on startup."""
    global _direct_cfd_map
    try:
        raw = _redis().get("june_direct_cfd_map")
        if raw:
            _direct_cfd_map = json.loads(raw)
            print(
                f"[{_ts()}] 📋 Loaded {len(_direct_cfd_map)} direct CFD mappings: "
                f"{list(_direct_cfd_map.keys())}",
                flush=True,
            )
    except Exception as exc:
        print(f"[{_ts()}] ⚠️  Could not load june_direct_cfd_map: {exc}", flush=True)


def _save_direct_cfd_cache() -> None:
    """Persist _direct_cfd_map to Redis (TTL 24h)."""
    try:
        _redis().set("june_direct_cfd_map", json.dumps(_direct_cfd_map), ex=DIRECT_CFD_REDIS_TTL)
    except Exception as exc:
        print(f"[{_ts()}] ⚠️  Could not save june_direct_cfd_map: {exc}", flush=True)


def _build_market_nav_cache() -> dict:
    """Build a chartCode->epic lookup from IG /marketnavigation.

    Queries the navigation hierarchy to find SHARES DFB instruments by browsing
    rather than text search, bypassing the index that returns wrong-company hits
    (XOM->Xometry, CVX->CVRx, BA->BAE Systems). Validates each instrument via
    detail call: chartCode, country==US, USD currency.

    Investigation (2026-07-16): /marketnavigation returns HTTP 404 on both the
    IG demo and live REST API endpoints. The endpoint is not part of the external
    REST API. This function fails gracefully and returns {} on demo. Code is
    forward-compatible for any future API version that exposes navigation.

    Results cached in-memory (_nav_cache) and Redis june_market_nav_cache (6h TTL).
    After the first call per session, subsequent calls return the cached result.
    """
    global _nav_cache, _nav_tried
    if _nav_tried:
        return _nav_cache
    _nav_tried = True

    # Try Redis first (populated by a previous session or live account run)
    try:
        raw = _redis().get("june_market_nav_cache")
        if raw:
            _nav_cache = json.loads(raw)
            print(
                f"[{_ts()}] 📚 Market nav cache loaded from Redis: "
                f"{len(_nav_cache)} US SHARES instruments",
                flush=True,
            )
            return _nav_cache
    except Exception:
        pass

    # Query root navigation node — returns None (404) on demo and standard REST API
    root = _ig_get("/marketnavigation")
    if not root:
        print(
            f"[{_ts()}] ℹ️  /marketnavigation unavailable — "
            f"nav strategy inactive (live account with navigation API required)",
            flush=True,
        )
        return {}

    # Walk hierarchy for US SHARES nodes
    lookup: dict = {}
    visited: set = set()

    def _walk(node_id: str, depth: int) -> None:
        if depth > 3 or node_id in visited:
            return
        visited.add(node_id)
        time.sleep(0.3)
        sub = _ig_get(f"/marketnavigation/{node_id}")
        if not sub:
            return
        for m in sub.get("markets", []):
            if (m.get("instrumentType") == "SHARES"
                    and m.get("expiry", "").upper() in ("-", "DFB", "")):
                epic = m.get("epic", "")
                if epic:
                    time.sleep(0.3)
                    det = _ig_get(f"/markets/{epic}")
                    if det:
                        inst    = det.get("instrument") or {}
                        chart   = (inst.get("chartCode") or "").upper()
                        country = inst.get("country") or ""
                        ccys    = inst.get("currencies") or []
                        ccy     = ccys[0].get("name", "") if ccys else ""
                        if chart and country == "US" and "USD" in ccy:
                            lookup[chart] = epic
        for sn in sub.get("nodes", []):
            _walk(str(sn.get("id", "")), depth + 1)

    nodes = root.get("nodes", [])
    for n in nodes:
        name = (n.get("name") or "").lower()
        if any(kw in name for kw in ("share", "us ", "equit", "stock", "amer")):
            _walk(str(n.get("id", "")), 0)

    if lookup:
        try:
            _redis().set("june_market_nav_cache", json.dumps(lookup), ex=NAV_CACHE_TTL)
        except Exception:
            pass
        print(
            f"[{_ts()}] 📚 Market nav cache built: {len(lookup)} US SHARES indexed",
            flush=True,
        )
    else:
        print(
            f"[{_ts()}] ℹ️  Market nav walked {len(nodes)} root nodes — "
            f"0 US SHARES DFB found (demo or no US equity hierarchy exposed)",
            flush=True,
        )
    _nav_cache = lookup
    return _nav_cache


def _search_direct_cfd(base: str) -> Optional[str]:
    """Search IG for a direct SHARES DFB CFD for the given T212 base ticker.

    Strategy 1: search by ticker symbol; apply _DIRECT_CFD_KEYWORDS filter to
    reject wrong-company hits (XOM->Xometry, CVX->CVRx, BA->BAE Systems, etc.);
    validate chartCode/country/USD on up to 4 candidates.

    Strategy 2+: search by company name variants from _CFD_ALT_NAMES. Same
    validation — chartCode must still equal base, so false-company hits are
    automatically rejected.

    IG demo finding (2026-07-16): only NVDA/AAPL/MSFT/AVGO confirmed as DFB
    SHARES. All other proxy-mapped symbols (XOM, CVX, BA, NEM, RTX, LMT, DAL,
    UAL ...) absent from the demo universe; company-name searches also return
    0 SHARES DFB results. The multi-strategy approach is forward-compatible with
    a funded live IG account.

    Logs a warning if the symbol exists only as a knockout (probable live-only
    restriction). Returns the epic string or None.
    """
    keyword   = _DIRECT_CFD_KEYWORDS.get(base, "").lower()
    alt_names = _CFD_ALT_NAMES.get(base, [])
    ko_hits: list = []

    def _try_candidate_list(search_term: str, is_ticker: bool,
                            ig_get_fn=None, label: str = "DEMO") -> Optional[str]:
        if ig_get_fn is None:
            ig_get_fn = _ig_get
        data = ig_get_fn("/markets", params={"searchTerm": search_term})
        if not data:
            return None
        markets = data.get("markets", [])
        if is_ticker:
            ko_hits.extend(
                m for m in markets
                if "KNOCKOUT" in m.get("instrumentType", "")
                and (base.lower() in m.get("epic", "").lower()
                     or (keyword and keyword in m.get("instrumentName", "").lower()))
            )
        shares = [
            m for m in markets
            if m.get("instrumentType") == "SHARES"
            and m.get("expiry", "").upper() in ("-", "DFB", "")
        ]
        if not shares:
            return None
        if is_ticker and keyword:
            filtered = [m for m in shares if keyword in m.get("instrumentName", "").lower()]
            if filtered:
                shares = filtered
        via = "" if is_ticker else f" [via '{search_term}']"
        for m in shares[:4]:
            epic = m.get("epic", "")
            if not epic:
                continue
            time.sleep(1)
            detail = ig_get_fn(f"/markets/{epic}")
            if not detail:
                continue
            inst       = detail.get("instrument") or {}
            chart_code = (inst.get("chartCode") or "").upper()
            country    = inst.get("country") or ""
            ccy_list   = inst.get("currencies") or []
            ccy_name   = ccy_list[0].get("name", "") if ccy_list else ""
            if chart_code == base.upper() and country == "US" and "USD" in ccy_name:
                snap = detail.get("snapshot") or {}
                note = " [demo: daily snapshot only]" if snap.get("bid") is None else ""
                print(
                    f"[{_ts()}]   \u2705 Direct CFD found ({label}): {base}_US_EQ \u2192 {epic} "
                    f"({inst.get('name', '?')[:45]}){note}{via}",
                    flush=True,
                )
                return epic
        return None

    # Strategy 1: ticker symbol search
    epic = _try_candidate_list(base, is_ticker=True)
    if epic:
        return epic

    # Strategy 2+: company name variant searches
    for name_term in alt_names:
        epic = _try_candidate_list(name_term, is_ticker=False)
        if epic:
            return epic

    # Strategy 4: market navigation cache
    # /marketnavigation returns 404 on demo/standard REST API; returns {} gracefully.
    _nav = _build_market_nav_cache()
    if _nav:
        nav_epic = _nav.get(base.upper())
        if nav_epic:
            print(
                f"[{_ts()}]   ✅ Direct CFD found (market nav): {base}_US_EQ → {nav_epic}",
                flush=True,
            )
            return nav_epic

    # Live endpoint fallback: retry same search strategies via W-8BEN live account.
    # Confirmed live-only symbols (2026-07-16): XOM LMT BA NEM UAL OXY SLB.
    if _live_available and _ensure_live_session():
        epic = _try_candidate_list(base, is_ticker=True, ig_get_fn=_ig_live_get, label="LIVE")
        if not epic:
            for name_term in alt_names:
                epic = _try_candidate_list(name_term, is_ticker=False,
                                           ig_get_fn=_ig_live_get, label="LIVE")
                if epic:
                    break
        if epic:
            return epic

    # Strategy 5: known alternative epics — IG chartCode doesn't match NYSE ticker.
    # Validated 2026-07-16: CVX=FIO, RTX=UTX(legacy), DAL=MMR.
    # Validates by name keyword + country + USD (bypasses chartCode check for these only).
    alt = _CFD_ALTERNATIVE_EPICS.get(base)
    if alt:
        alt_epic, alt_keyword = alt
        ig_fn = _ig_live_get if (_live_available and _ensure_live_session()) else _ig_get
        detail = ig_fn(f"/markets/{alt_epic}")
        if detail:
            inst = detail.get("instrument") or {}
            name_lower = (inst.get("name") or "").lower()
            country    = inst.get("country") or ""
            ccys       = inst.get("currencies") or []
            ccy        = ccys[0].get("name", "") if ccys else ""
            snap       = detail.get("snapshot") or {}
            if (country == "US" and "USD" in ccy
                    and (alt_keyword in name_lower or base.lower() in name_lower)):
                note = " [demo: daily snapshot only]" if snap.get("bid") is None else ""
                print(
                    f"[{_ts()}]   \u2705 Direct CFD found (alternative search): "
                    f"{base}_US_EQ \u2192 {alt_epic} "
                    f"({inst.get('name', '?')[:45]}){note}",
                    flush=True,
                )
                return alt_epic

    # All strategies exhausted
    n_strategies = 1 + len(alt_names) + (1 if _nav else 0)
    if ko_hits:
        ko_name = (ko_hits[0].get("instrumentName") or "")[:50]
        print(
            f"[{_ts()}]   \u26a0\ufe0f  {base}_US_EQ: exists on IG as knockout only "
            f"({ko_name}) \u2014 DFB SHARES CFD unavailable on demo; "
            f"proxy mapping retained",
            flush=True,
        )
    else:
        live_note = " + live" if _live_available else ""
        print(
            f"[{_ts()}]   \u2139\ufe0f  {base}_US_EQ: not found across "
            f"{n_strategies} demo{live_note} search strategies \u2014 proxy mapping retained",
            flush=True,
        )
    return None
def _get_needed_symbols() -> set:
    """Return T212 base tickers from held positions and check_requests for CFD discovery."""
    needed: set = set()
    try:
        r = _redis()
        pos_raw = r.get("ms_held_positions")
        if pos_raw:
            for ticker in json.loads(pos_raw).keys():
                base = ticker.split("_")[0].upper()
                if len(base) >= 2:
                    needed.add(base)
        req_raw = r.get("june_check_requests")
        if req_raw:
            for c in json.loads(req_raw).get("candidates", []):
                base = c.get("symbol", "").split("_")[0].upper()
                if len(base) >= 2:
                    needed.add(base)
    except Exception:
        pass
    # Probe proactive bases even when MS is not holding them; miss cache
    # (24h TTL) prevents re-querying confirmed-absent symbols.
    needed.update(b for b in _CFD_PROACTIVE_BASES if b not in _direct_cfd_map)
    return needed


def _maybe_discover_cfd(needed_bases: set) -> None:
    """Rate-limited CFD discovery: one IG search per call, at most every 30 seconds."""
    global _cfd_last_search
    now = time.time()
    if now - _cfd_last_search < DIRECT_CFD_SEARCH_INTERVAL:
        return
    for base in sorted(needed_bases):
        if base in _direct_cfd_map:
            continue
        if _cfd_miss_cache.get(base, 0) > now:
            continue
        _cfd_last_search = now
        epic = _search_direct_cfd(base)
        if epic:
            _direct_cfd_map[base] = epic
            _save_direct_cfd_cache()
        else:
            _cfd_miss_cache[base] = now + DIRECT_CFD_CONFIRMED_MISS_TTL
        return  # one search per invocation


def _startup_discovery_pass() -> None:
    """Synchronous discovery pass for all proactive bases at startup.

    Change 1 (W-8BEN): clears stale confirmed-miss state by re-searching all
    proactive bases. In-memory _cfd_miss_cache is always empty on restart, so
    every previously-failed symbol is tried fresh.

    Change 3 (W-8BEN): runs before the main poll loop to eager-load results
    rather than waiting for symbols to appear in ms_held_positions. Uses a
    short 2s inter-call delay instead of the 30s poll rate limit.

    Logs a summary: 'Startup discovery complete: N/M direct CFDs found'.
    """
    global _cfd_last_search
    to_search = sorted(b for b in _CFD_PROACTIVE_BASES if b not in _direct_cfd_map)
    total     = len(_CFD_PROACTIVE_BASES)
    if not to_search:
        print(
            f"[{_ts()}] 🔍 Startup discovery: all {total} proactive bases already mapped — skipping",
            flush=True,
        )
        return
    print(
        f"[{_ts()}] 🔍 Startup discovery: checking {len(to_search)}/{total} "
        f"unmapped proactive bases ...",
        flush=True,
    )
    # Build nav cache once before symbol loop (returns {} on demo)
    _build_market_nav_cache()
    found = 0
    for base in to_search:
        time.sleep(2)
        epic = _search_direct_cfd(base)
        if epic:
            _direct_cfd_map[base] = epic
            _save_direct_cfd_cache()
            found += 1
        # No miss-cache ban here: startup failures may be transient 403s.
        # _maybe_discover_cfd() sets the 24h ban after confirmed multi-strategy
        # failure in the main loop, once the rate limit has recovered.
    _cfd_last_search = time.time()
    mapped = len([b for b in _CFD_PROACTIVE_BASES if b in _direct_cfd_map])
    print(
        f"[{_ts()}] 🔍 Startup discovery complete: "
        f"{mapped}/{total} direct CFDs found, {total - mapped} proxy-only",
        flush=True,
    )


def _refresh_direct_cfd_signals() -> None:
    """Fetch daily snapshot signals for all known direct CFD epics (every 5 min).

    IG demo limitation: stock CFDs return null bid/offer (streamingPricesAvailable=False).
    percentageChange reflects today's move from prior session close — directionally
    accurate but not intraday real-time. Sync validation (Change 2) requires live
    bid/offer and is therefore not implemented on IG demo.
    """
    global _cfd_signal_refresh
    if not _direct_cfd_map:
        return
    if time.time() - _cfd_signal_refresh < 5 * 60:
        return
    updated = []
    for base, epic in list(_direct_cfd_map.items()):
        detail = _ig_get(f"/markets/{epic}")
        if not detail:
            continue
        snap = detail.get("snapshot", {})
        pct  = snap.get("percentageChange")
        if pct is None:
            continue
        pct       = float(pct)
        direction = "bull" if pct > 0.1 else ("bear" if pct < -0.1 else "flat")
        _direct_cfd_signals[base] = {"pct": round(pct, 3), "direction": direction, "ts": int(time.time())}
        updated.append(f"{base}={pct:+.2f}%")
        time.sleep(1)
    if updated:
        _cfd_signal_refresh = time.time()
        print(f"[{_ts()}] 📈 Direct CFD daily signals: {', '.join(updated)}", flush=True)


def _check_direct_cfd_gaps() -> None:
    """Detect pre-market gaps on individual stocks via direct CFD daily snapshots.

    Gap threshold: |percentageChange from prior close| >= DIRECT_CFD_GAP_PCT (1.5%).
    Confidence: 'medium' — IG demo has no live bid/offer for stock CFDs, so the
    spread liquidity check and sustained-2-cycles requirement cannot be verified.
    Publishes new gaps to june_gap_discoveries (TTL 2h), merging with any existing.
    """
    global _gap_disc_date, _gap_disc_seen
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _gap_disc_date != today:
        _gap_disc_date = today
        _gap_disc_seen = set()
    if not _direct_cfd_signals:
        return
    new_gaps = []
    for base, sig in _direct_cfd_signals.items():
        if base in _gap_disc_seen:
            continue
        pct = sig.get("pct", 0.0)
        if abs(pct) < DIRECT_CFD_GAP_PCT:
            continue
        direction   = "bull" if pct > 0 else "bear"
        t212_ticker = f"{base}_US_EQ"
        new_gaps.append({
            "t212_ticker":            t212_ticker,
            "cfd_epic":               _direct_cfd_map.get(base, ""),
            "gap_pct":                round(pct, 3),
            "direction":              direction,
            "prior_close_pct_change": round(pct, 3),
            "confidence":             "medium",
        })
        _gap_disc_seen.add(base)
        print(
            f"[{_ts()}] 📊 Direct CFD gap: {t212_ticker} {pct:+.2f}% ({direction}) "
            f"— publishing to june_gap_discoveries",
            flush=True,
        )
    if not new_gaps:
        return
    try:
        r         = _redis()
        existing  = r.get("june_gap_discoveries")
        all_gaps  = json.loads(existing).get("gaps", []) if existing else []
        seen_tkrs = {g["t212_ticker"] for g in all_gaps}
        all_gaps.extend(g for g in new_gaps if g["t212_ticker"] not in seen_tkrs)
        r.set(
            "june_gap_discoveries",
            json.dumps({"timestamp": int(time.time()), "gaps": all_gaps}),
            ex=2 * 3600,
        )
    except Exception as exc:
        print(f"[{_ts()}] ⚠️  Redis error (gap_discoveries): {exc}", flush=True)


def _validate_gap_requests() -> None:
    """Read Claudia's june_gap_validation_requests and publish agreement results.

    Uses direct CFD daily signal when available; falls back to sector-proxy 5-min change.
    Publishes to june_gap_validations (TTL 90s).
    """
    try:
        r   = _redis()
        raw = r.get("june_gap_validation_requests")
        if not raw:
            return
        req        = json.loads(raw)
        candidates = req.get("candidates", [])
        if not candidates:
            return
        ts_now  = int(time.time())
        results = {}
        for c in candidates:
            sym     = c.get("t212_symbol", "")
            gap_pct = c.get("gap_pct", 0.0)
            c_dir   = c.get("direction", "bull")
            base    = sym.split("_")[0].upper()
            direct  = _direct_cfd_signals.get(base)
            if direct:
                june_pct = direct["pct"]
                june_dir = direct["direction"]
                agree    = (c_dir == june_dir) and abs(june_pct) >= 0.5
                entry    = {
                    "claudia_gap_pct": gap_pct, "june_cfd_pct": june_pct,
                    "june_source": "direct_cfd", "agreement": agree,
                    "confidence": "medium", "ts": ts_now,
                }
                if not agree:
                    entry["note"] = f"CFD {june_pct:+.2f}% vs Claudia {c_dir}"
                results[sym] = entry
            else:
                june_inst = T212_TO_JUNE_INSTRUMENT.get(base)
                if june_inst and june_inst in INSTRUMENTS:
                    hist      = _history.get(june_inst)
                    now_price = hist[-1][1] if hist else None
                    price_5m  = _price_n_minutes_ago(june_inst, 5.0)
                    if now_price and price_5m and price_5m != 0:
                        chg_5m   = (now_price - price_5m) / price_5m * 100.0
                        june_dir = "bull" if chg_5m > 0.15 else ("bear" if chg_5m < -0.15 else "flat")
                        results[sym] = {
                            "claudia_gap_pct": gap_pct, "june_cfd_pct": round(chg_5m, 3),
                            "june_source": f"proxy_{june_inst}", "agreement": c_dir == june_dir,
                            "confidence": "low", "ts": ts_now,
                        }
        if results:
            r.set("june_gap_validations", json.dumps(results), ex=90)
            agreed       = [s for s, v in results.items() if v["agreement"]]
            contradicted = [s for s, v in results.items() if not v["agreement"]]
            if agreed:
                print(f"[{_ts()}] ✅ june_gap_validations: agreed {agreed}", flush=True)
            if contradicted:
                print(f"[{_ts()}] ⚠️  june_gap_validations: contradicted {contradicted}", flush=True)
    except Exception as exc:
        print(f"[{_ts()}] ⚠️  _validate_gap_requests error (non-fatal): {exc}", flush=True)


# ── Sister-bot crosscheck functions ───────────────────────────────────────────

def _check_equity_signals() -> None:
    """Read Claudia's june_check_requests and publish per-symbol CFD crosscheck signals.
    Uses sector-proxy mapping since June has no individual equity CFDs."""
    THREE_WAY_THRESHOLD = 0.15
    try:
        r = _redis()
        raw = r.get("june_check_requests")
        if not raw:
            return
        req = json.loads(raw)
        candidates = req.get("candidates", [])
        if not candidates:
            return

        confirmed = []
        contradicted = []
        ts_now = int(time.time())

        for c in candidates:
            sym = c.get("symbol", "")
            change_pct_fmp = c.get("change_pct", 0.0)

            # Direct CFD daily signal (more precise than sector proxy mapping)
            direct = _direct_cfd_signals.get(sym)
            if direct:
                fmp_up = change_pct_fmp > 0.2
                fmp_dn = change_pct_fmp < -0.2
                entry  = {
                    "symbol": sym, "cfd_instrument": f"{sym}_direct_cfd",
                    "pct": direct["pct"], "direction": direct["direction"],
                    "source": "direct_cfd", "ts": ts_now,
                }
                if (fmp_up and direct["direction"] == "bull") or (fmp_dn and direct["direction"] == "bear"):
                    confirmed.append(entry)
                elif (fmp_up and direct["direction"] == "bear") or (fmp_dn and direct["direction"] == "bull"):
                    contradicted.append(entry)
                continue
            june_inst = T212_TO_JUNE_INSTRUMENT.get(sym)
            if june_inst not in INSTRUMENTS:
                continue

            hist = _history.get(june_inst)
            now_price = hist[-1][1] if hist else None
            price_5m = _price_n_minutes_ago(june_inst, 5.0)
            if now_price is None or price_5m is None or price_5m == 0:
                continue

            change_5m = (now_price - price_5m) / price_5m * 100.0

            fmp_up = change_pct_fmp > 0.2
            fmp_dn = change_pct_fmp < -0.2
            cfd_up = change_5m > THREE_WAY_THRESHOLD
            cfd_dn = change_5m < -THREE_WAY_THRESHOLD

            entry = {
                "symbol": sym,
                "cfd_instrument": june_inst,
                "pct": round(change_5m, 3),
                "direction": "up" if cfd_up else ("down" if cfd_dn else "flat"),
                "ts": ts_now,
            }
            if (fmp_up and cfd_up) or (fmp_dn and cfd_dn):
                confirmed.append(entry)
            elif (fmp_up and cfd_dn) or (fmp_dn and cfd_up):
                contradicted.append(entry)

        if confirmed or contradicted:
            payload = {"confirmed": confirmed, "contradicted": contradicted, "ts": ts_now}
            r.set("june_equity_signals", json.dumps(payload), ex=90)
            if confirmed:
                print(f"[{_ts()}] \u2705 june_equity_signals: confirmed {[e['symbol'] for e in confirmed]}", flush=True)
            if contradicted:
                print(f"[{_ts()}] \u26a0\ufe0f  june_equity_signals: contradicted {[e['symbol'] for e in contradicted]}", flush=True)
    except Exception as exc:
        print(f"[{_ts()}] \u26a0\ufe0f  _check_equity_signals error (non-fatal): {exc}", flush=True)


def _check_exit_warnings() -> None:
    """Read ms_held_positions and publish CFD-based exit warnings for deteriorating proxies."""
    try:
        r = _redis()
        raw = r.get("ms_held_positions")
        if not raw:
            return
        positions = json.loads(raw)
        if not positions:
            return

        warnings = []
        ts_now = int(time.time())

        for t212_ticker, pos in positions.items():
            base = t212_ticker.split("_")[0].upper()
            # Direct CFD daily signal (more precise than sector proxy mapping)
            direct = _direct_cfd_signals.get(base)
            if direct:
                pct_val  = direct["pct"]
                if pct_val >= -1.5:
                    continue
                abs_chg  = abs(pct_val)
                severity = "severe" if abs_chg >= 5.0 else ("moderate" if abs_chg >= 3.0 else "mild")
                warnings.append({
                    "cfd_instrument":   f"{base}_direct_cfd",
                    "t212_equivalents": [t212_ticker],
                    "direction":        "down",
                    "pct":              round(pct_val, 3),
                    "severity":         severity,
                    "source":           "direct_cfd",
                    "ts":               ts_now,
                })
                continue
            june_inst = T212_TO_JUNE_INSTRUMENT.get(base)
            if june_inst not in INSTRUMENTS:
                continue

            hist = _history.get(june_inst)
            now_price = hist[-1][1] if hist else None
            price_5m = _price_n_minutes_ago(june_inst, 5.0)
            if now_price is None or price_5m is None or price_5m == 0:
                continue

            change_5m = (now_price - price_5m) / price_5m * 100.0
            if change_5m >= -0.5:
                continue

            abs_chg = abs(change_5m)
            severity = "severe" if abs_chg >= 2.0 else ("moderate" if abs_chg >= 1.0 else "mild")
            warnings.append({
                "cfd_instrument": june_inst,
                "t212_equivalents": _JUNE_TO_T212_EQUIVALENTS.get(june_inst, []),
                "direction": "down",
                "pct": round(change_5m, 3),
                "severity": severity,
                "ts": ts_now,
            })

        if warnings:
            r.set("june_exit_warnings", json.dumps({"warnings": warnings}), ex=120)
            summary = [(w["cfd_instrument"], w["severity"], f"{w['pct']:+.2f}%") for w in warnings]
            print(f"[{_ts()}] \U0001f6a8 june_exit_warnings: {summary}", flush=True)
        else:
            r.delete("june_exit_warnings")
    except Exception as exc:
        print(f"[{_ts()}] \u26a0\ufe0f  _check_exit_warnings error (non-fatal): {exc}", flush=True)


def _publish_macro_regime() -> None:
    """Compute and publish a structured macro regime assessment to june_macro_regime (TTL 90s)."""
    CONFIRM_THRESHOLD = 0.15
    try:
        ts_now = int(time.time())
        signals_by_sym = {}
        for sym in INSTRUMENTS:
            hist = _history.get(sym)
            if not hist:
                continue
            now_px = hist[-1][1] if hist else None
            px_5m  = _price_n_minutes_ago(sym, 5.0)
            if now_px is None or px_5m is None or px_5m == 0:
                continue
            signals_by_sym[sym] = (now_px - px_5m) / px_5m * 100.0

        if not signals_by_sym:
            return

        three_way = []
        for group in _THREE_WAY_GROUPS:
            insts = group["instruments"]
            if group["direction"] == "mixed":
                if (
                    signals_by_sym.get("USDJPY", 0) > CONFIRM_THRESHOLD and
                    signals_by_sym.get("EURUSD", 0) < -CONFIRM_THRESHOLD and
                    signals_by_sym.get("GBPUSD", 0) < -CONFIRM_THRESHOLD
                ):
                    three_way.append({
                        "instruments": insts,
                        "direction": "dollar_strength",
                        "note": group["note"],
                    })
            else:
                ci = [
                    i for i in insts if i in signals_by_sym and (
                        (group["direction"] == "up"   and signals_by_sym[i] >  CONFIRM_THRESHOLD) or
                        (group["direction"] == "down" and signals_by_sym[i] < -CONFIRM_THRESHOLD)
                    )
                ]
                if len(ci) >= 2:
                    three_way.append({
                        "instruments": ci,
                        "direction": group["direction"],
                        "note": group["note"],
                    })

        corr_breaks = []
        for sym_a, sym_b, description in CORR_PAIRS:
            a = signals_by_sym.get(sym_a)
            b = signals_by_sym.get(sym_b)
            if a is None or b is None:
                continue
            if (
                abs(a - b) >= CORR_DIVERGENCE_PCT and
                abs(a) > CONFIRM_THRESHOLD and
                abs(b) > CONFIRM_THRESHOLD and
                (a > 0) != (b > 0)
            ):
                corr_breaks.append({"pair": [sym_a, sym_b], "note": description})

        liq_warnings = []
        for sym, hist_sp in _spread_hist.items():
            if len(hist_sp) < SPREAD_MIN_READINGS:
                continue
            avg_sp = sum(hist_sp) / len(hist_sp)
            if avg_sp <= 0:
                continue
            current_sp = hist_sp[-1] if hist_sp else 0
            if current_sp > SPREAD_ALERT_FACTOR * avg_sp:
                liq_warnings.append({
                    "instrument": sym,
                    "spread_pct": round(current_sp, 4),
                    "vs_avg": round(avg_sp, 4),
                    "note": f"Spread {current_sp:.3f}% vs avg {avg_sp:.3f}%",
                })

        spx_chg = signals_by_sym.get("SPX500")
        spx_signal = None
        if spx_chg is not None:
            spx_signal = {
                "direction": "up" if spx_chg > 0 else ("down" if spx_chg < 0 else "flat"),
                "change_5m": round(spx_chg, 3),
                "note": f"SPX500 {spx_chg:+.3f}% last 5 min",
            }

        pos_count = sum(1 for v in signals_by_sym.values() if v > CONFIRM_THRESHOLD)
        neg_count = sum(1 for v in signals_by_sym.values() if v < -CONFIRM_THRESHOLD)
        total = len(signals_by_sym)
        all_moves = list(signals_by_sym.values())
        avg_abs = sum(abs(v) for v in all_moves) / len(all_moves) if all_moves else 0

        if pos_count >= total * 0.7:
            regime = "bull"
        elif neg_count >= total * 0.7:
            regime = "bear"
        elif avg_abs > 0.5:
            regime = "volatile"
        else:
            regime = "neutral"

        if three_way:
            confidence = "high"
        elif len(corr_breaks) == 0 and (pos_count >= total * 0.6 or neg_count >= total * 0.6):
            confidence = "medium"
        else:
            confidence = "low"

        summary_parts = [f"Regime: {regime} (confidence: {confidence})."]
        if spx_signal:
            summary_parts.append(f"SPX500 {spx_signal['change_5m']:+.3f}% (5m).")
        if three_way:
            summary_parts.append(f"Three-way: {[g['note'] for g in three_way]}.")
        if corr_breaks:
            summary_parts.append(f"Corr breaks: {[b['pair'] for b in corr_breaks]}.")
        if liq_warnings:
            summary_parts.append(f"Spread alerts: {[w['instrument'] for w in liq_warnings]}.")

        # Enrich with Claudia sector momentum direction
        try:
            _csm_raw = _redis().get("claudia_sector_momentum")
            if _csm_raw:
                _csm       = json.loads(_csm_raw)
                _csm_accel = _csm.get("accelerating", [])
                _csm_decel = _csm.get("decelerating", [])
                _csm_secs  = _csm.get("sectors", {})
                if len(_csm_accel) >= 3:
                    summary_parts.append(
                        f"Broad sector acceleration across {len(_csm_accel)} sectors "
                        f"({', '.join(_csm_accel[:3])})."
                    )
                elif _csm_accel:
                    _sustained_accel = [
                        s for s in _csm_accel
                        if _csm_secs.get(s, {}).get("cycles_in_direction", 0) >= 3
                    ]
                    if _sustained_accel:
                        summary_parts.append(
                            f"Sustained sector acceleration: {', '.join(_sustained_accel)} "
                            f"({_csm_secs[_sustained_accel[0]].get('cycles_in_direction', 0)}+ cycles)."
                        )
                _sustained_decel = [
                    s for s in _csm_decel
                    if _csm_secs.get(s, {}).get("cycles_in_direction", 0) >= 3
                ]
                if _sustained_decel:
                    summary_parts.append(f"Sustained sector deceleration: {', '.join(_sustained_decel)}.")
        except Exception:
            pass

        summary = " ".join(summary_parts)

        payload = {
            "timestamp": ts_now,
            "regime": regime,
            "confidence": confidence,
            "three_way_confirmations": three_way,
            "correlation_breaks": corr_breaks,
            "liquidity_warnings": liq_warnings,
            "spx_futures_signal": spx_signal,
            "summary": summary,
        }
        _redis().set("june_macro_regime", json.dumps(payload), ex=90)
    except Exception as exc:
        print(f"[{_ts()}] \u26a0\ufe0f  _publish_macro_regime error (non-fatal): {exc}", flush=True)

def _run_cfd_sync_check() -> None:
    """Compare direct CFD daily % vs T212 price changes. Publishes june_cfd_sync_status.
    Passive only — does not affect any other Redis key except to annotate existing
    june_exit_warnings entries when DESYNCED."""
    global _sync_last_check
    if time.time() - _sync_last_check < 5 * 60:
        return
    if not _direct_cfd_signals:
        return

    # Gather T212 price-change data from available Redis sources.
    t212_changes: dict = {}
    try:
        r = _redis()
        req_raw = r.get("june_check_requests")
        if req_raw:
            for c in json.loads(req_raw).get("candidates", []):
                base = c.get("symbol", "").split("_")[0].upper()
                pct  = c.get("change_pct")
                if base and pct is not None:
                    t212_changes[base] = float(pct)
        pos_raw = r.get("ms_held_positions")
        if pos_raw:
            for ticker, pos in json.loads(pos_raw).items():
                base = ticker.split("_")[0].upper()
                if base not in t212_changes:
                    for field in ("pnl_pct", "pnl_percentage", "unrealised_pnl_pct"):
                        v = pos.get(field)
                        if v is not None:
                            t212_changes[base] = float(v)
                            break
    except Exception:
        pass

    ts_now       = int(time.time())
    sync_status  = {"timestamp": ts_now}
    desynced_bases: set = set()

    for base, sig in _direct_cfd_signals.items():
        epic    = _direct_cfd_map.get(base, "")
        cfd_pct = sig.get("pct", 0.0)
        key     = base + "_US_EQ"
        t212_pct = t212_changes.get(base)

        if t212_pct is None:
            sync_status[key] = {
                "status": "unknown",
                "reason": "no T212 price data available",
                "cfd_epic": epic, "source": "demo", "last_check": ts_now,
            }
            if base in _sync_state:
                _sync_state[base]["agree"]    = 0
                _sync_state[base]["disagree"] = 0
            continue

        cfd_dir  = "bull" if cfd_pct > 0.1 else ("bear" if cfd_pct < -0.1 else "flat")
        t212_dir = "bull" if t212_pct > 0.1 else ("bear" if t212_pct < -0.1 else "flat")
        opposing = (
            cfd_dir != "flat" and t212_dir != "flat" and cfd_dir != t212_dir
            and abs(cfd_pct) >= 0.5 and abs(t212_pct) >= 0.5
        )

        state = _sync_state.setdefault(base, {"agree": 0, "disagree": 0})
        if opposing:
            state["disagree"] += 1
            state["agree"]     = 0
        else:
            state["agree"] += 1
            state["disagree"] = 0

        if state["disagree"] >= 2:
            desynced_bases.add(base)
            sync_status[key] = {
                "status": "desynced",
                "cfd_change": cfd_pct, "t212_change": t212_pct,
                "note": "opposing direction " + str(state["disagree"]) + " checks",
                "cfd_epic": epic, "source": "demo", "last_check": ts_now,
            }
            print(
                f"[{_ts()}] \u26a0\ufe0f  Sync check: {key} \u2014 "
                f"CFD {cfd_pct:+.2f}% vs T212 {t212_pct:+.2f}% "
                f"(opposing, check {state['disagree']}/2) \u2014 flagging DESYNCED",
                flush=True,
            )
        elif state["agree"] >= 3:
            sync_status[key] = {
                "status": "synced",
                "cfd_epic": epic, "source": "demo", "last_check": ts_now,
            }
            print(
                f"[{_ts()}] \u2705 Sync check: {key} \u2014 "
                f"CFD and T212 both {cfd_dir} \u2014 SYNCED",
                flush=True,
            )
        else:
            sync_status[key] = {
                "status": "checking",
                "cfd_change": cfd_pct, "t212_change": t212_pct,
                "checks_agree": state["agree"], "checks_disagree": state["disagree"],
                "cfd_epic": epic, "source": "demo", "last_check": ts_now,
            }

    try:
        _redis().set("june_cfd_sync_status", json.dumps(sync_status), ex=10 * 60)
    except Exception as exc:
        print(f"[{_ts()}] \u26a0\ufe0f  Redis write error (cfd_sync_status): {exc}",
              flush=True)

    if desynced_bases:
        _annotate_exit_warnings_with_sync(desynced_bases, sync_status)

    _sync_last_check = time.time()


def _annotate_exit_warnings_with_sync(desynced_bases: set, sync_status: dict) -> None:
    """Add sync_warning to existing exit warnings for desynced instruments.
    Never creates new warnings — passive annotation only."""
    try:
        r = _redis()
        raw = r.get("june_exit_warnings")
        if not raw:
            return
        warnings_data = json.loads(raw)
        warnings      = warnings_data.get("warnings", [])
        modified      = False
        for w in warnings:
            cfd_inst = w.get("cfd_instrument", "")
            base     = cfd_inst.replace("_direct_cfd", "").split("_")[0].upper()
            if base in desynced_bases:
                key = base + "_US_EQ"
                se  = sync_status.get(key, {})
                w["sync_warning"] = {
                    "status":      "desynced",
                    "cfd_change":  se.get("cfd_change"),
                    "t212_change": se.get("t212_change"),
                    "note":        se.get("note", ""),
                }
                modified = True
        if modified:
            r.set("june_exit_warnings", json.dumps(warnings_data), ex=120)
    except Exception as exc:
        print(f"[{_ts()}] \u26a0\ufe0f  _annotate_exit_warnings_with_sync error: {exc}",
              flush=True)


def poll_cycle() -> bool:
    """Fetch all instrument prices, update state, publish june_signals. Returns True if prices returned."""
    now          = time.time()
    signals       = {}
    alerts        = []
    spread_avgs   = {}
    current_mids  = {}
    warmup_skipped = 0

    for sym, epic in list(INSTRUMENTS.items()):
        price = fetch_price(epic)
        if price is None:
            continue

        mid = price["mid"]
        current_mids[sym] = mid

        # Rolling price history (for change_5m / change_15m)
        _history[sym].append((now, mid))

        # Overnight high / low tracking
        if is_overnight():
            oh, ol = _overnight["high"], _overnight["low"]
            if sym not in oh or mid > oh[sym]:
                oh[sym] = mid
            if sym not in ol or mid < ol[sym]:
                ol[sym] = mid

        # Spread tracking — rolling average for anomaly detection
        spread_pct = (price["spread"] / mid * 100.0) if mid > 0 else 0.0
        _spread_hist[sym].append(spread_pct)
        hist_sp    = _spread_hist[sym]
        avg_spread = sum(hist_sp) / len(hist_sp) if hist_sp else spread_pct
        spread_avgs[sym] = avg_spread

        spread_alert = (
            len(hist_sp) >= SPREAD_MIN_READINGS
            and avg_spread > 0
            and spread_pct > SPREAD_ALERT_FACTOR * avg_spread
        )
        if spread_alert:
            print(
                f"[{_ts()}] 📊 Spread alert: {sym} current {spread_pct:.4f}% "
                f"vs {SPREAD_ALERT_FACTOR:.0f}× avg {avg_spread:.4f}%",
                flush=True,
            )

        sig = compute_signal(sym, price, spread_alert=spread_alert)
        if sig["change_5m"] is None:
            if sym not in _no_history_warned:
                print(f"[{_ts()}] {sym}: warming up -- no 5m price history yet", flush=True)
                _no_history_warned.add(sym)
            warmup_skipped += 1
            continue
        signals[sym] = sig
        if abs(sig["change_5m"]) >= MOMENTUM_PCT:
            alerts.append(sym)

    if not signals:
        if warmup_skipped > 0:
            return True
        print(f"[{_ts()}] ⚠️  Poll cycle: no prices returned", flush=True)
        return False

    # Publish june_signals
    payload = {"timestamp": int(now), "signals": signals, "momentum_alerts": alerts}
    try:
        r = _redis()
        r.set("june_signals", json.dumps(payload), ex=SIGNAL_TTL)
    except Exception as exc:
        print(f"[{_ts()}] ❌ Redis write error (signals): {exc}", flush=True)

    # Publish spread baselines (updated every cycle, TTL 25h)
    if spread_avgs:
        _publish_spread_baselines(spread_avgs)

    # Pre-London gap check (only during 06:00-07:00 UTC window)
    if is_premarket():
        _check_premarket_gaps(current_mids)

    # Sister-bot crosscheck (equity signals, exit warnings, macro regime)
    _check_equity_signals()
    _check_exit_warnings()
    _publish_macro_regime()

    # Direct CFD discovery and daily signal refresh (rate-limited)
    _maybe_discover_cfd(_get_needed_symbols())
    _refresh_direct_cfd_signals()
    _run_cfd_sync_check()
    if is_us_premarket():
        _check_direct_cfd_gaps()
    _validate_gap_requests()

    # Virtual trading simulation (paper trades only — no real orders)
    if _sim:
        run_simulation_step(signals)

    # Log summary
    alert_tag = f"  🚨 ALERTS → {alerts}" if alerts else ""
    price_row = " | ".join(f"{s}={v['price']:.4f}({v['change_5m']:+.3f}%)" for s, v in signals.items())
    print(f"[{_ts()}] 📡 {len(signals)} signals published (TTL {SIGNAL_TTL}s){alert_tag}", flush=True)
    print(f"[{_ts()}]    {price_row}", flush=True)
    return True


# ── Utility ───────────────────────────────────────────────────────────────────

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# VIRTUAL TRADING SIMULATION
# Self-graduating paper trading framework with persistent Redis state.
# Stages: SPROUT -> SEEDLING -> GERMINATION -> VEGETATIVE -> FULL BLOOM
# State key: june_sim_state (TTL 72h) -- survives June restarts.
# Results key: june_sim_results (TTL 48h) -- written on stop/graduation end.
# Manual stop: set Redis key june_sim_stop = "1"
# All output tagged [STAGE/Pn]. Filter: journalctl -u june | grep "SIM"
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ── Global stops ──────────────────────────────────────────────────────────────
_SIM_START_BALANCE   = 50.0
_SIM_RESET_AFTER     = 1784332875.0  # 2026-07-18 00:00 UTC — one-time reset for new session
_SIM_PROFIT_STOP     = 100.0    # doubled from start
_SIM_LOSS_STOP       = -30.0   # 60% loss from start

# ── Entry / exit parameters ───────────────────────────────────────────────────
_SIM_ENTRY_VOL_MIN   = 0.15    # |change_5m| >= 0.15% to consider entry
_SIM_MAX_HOLD_SECS   = 4 * 3600
_SIM_TP_WIN_WINDOW   = 10      # rolling win-move window per combo
_SIM_TP_MIN_SAMPLES  = 3       # wins before full-confidence TP fraction applies

# Dynamic TP (all fractions — same units as pnl_pct; vol_history is in %, divided /100)
_SIM_TP_WIN_FRACTION = 0.70    # TP from win history: 70% of avg winning move
_SIM_TP_VOL_FRACTION = 0.30    # TP from vol_history: 30% of vol_mean (converted /100)
_SIM_TP_FLOOR        = 0.0002  # 0.02% absolute floor (~2 pips GBPUSD, $0.011 Silver)
_SIM_TP_CAP          = 0.010   # 1.00% max TP per trade

# Dynamic stop (fractions; vol_history in % — divided /100 inside function)
_SIM_STOP_VOL_MULT   = 2.0     # stop = vol_mean + MULT * vol_std
_SIM_STOP_FLOOR      = 0.0008  # 0.08% floor (covers spread+noise for all instruments)
_SIM_STOP_CAP        = 0.005   # 0.50% max stop (at 10x = 5% leveraged)
_SIM_STOP_COLD       = 0.0020  # 0.20% cold-start when no vol_history available

# Spread-aware stop floors — fallback when june_spread_baselines unavailable (fractions)
_SIM_SPREAD_FLOORS = {
    "SILVER": 0.0015,   # 0.15%
    "GBPUSD": 0.0005,   # 0.05%
    "EURUSD": 0.0004,   # 0.04%
    "USDJPY": 0.0004,   # 0.04%
    "AUDUSD": 0.0006,   # 0.06% — slightly wider than majors
    "USDCAD": 0.0007,   # 0.07%
    "EURGBP": 0.0006,   # 0.06%
    "NZDUSD": 0.0009,   # 0.09% — thinner liquidity
    "USDCHF": 0.0006,   # 0.06%
    # US equity CFDs — synthetic spread from Finnhub (IG bid/offer unavailable)
    "NVDA": 0.0010, "TSLA": 0.0012, "AAPL": 0.0008,
    "MSFT": 0.0008, "AMD":  0.0010, "INTC": 0.0012, "MU": 0.0010,
}

# Asymmetric reversal: losing positions exit faster than winning ones
_SIM_REV_PATIENCE_WIN  = 2     # consecutive opposing cycles before cutting a winner
_SIM_REV_PATIENCE_LOSS = 1     # exit loser on first opposing cycle

# ── Phase timing ──────────────────────────────────────────────────────────────
_SIM_CONSERVATIVE_LEV  = 3
_SIM_PHASE2_LEV        = 5
_SIM_AGGRESSIVE_LEV    = 10
# Phase advancement: performance-based (replaces time-based system)
_SIM_P1_TO_P2_TRADES   = 10
_SIM_P1_TO_P2_WR       = 0.50
_SIM_P2_TO_P3_TRADES   = 20
_SIM_P2_TO_P3_WR       = 0.55
_SIM_P2_TO_P3_PNL      = 0.05   # +5% from phase entry balance required
# Phase drop-back: 5 consecutive losses OR -5% from phase entry
_SIM_PHASE_DROP_LOSSES = 5
_SIM_PHASE_DROP_PNL    = -0.05

# ── Sprout sizing rotation ─────────────────────────────────────────────────────
_SIM_SIZING_ORDER        = ["fixed_5", "fixed_10", "pct_5", "pct_10"]
_SIM_MIN_APPROACH_TRADES = 5    # min per-instrument trades before approach is trusted
_SIM_SKIP_THRESHOLD      = 3    # notional-miss skips before approach excluded for this instrument
_SIM_SKIP_BALANCE_TOL    = 1.20 # re-enable skipped approach once balance grows >20%

# ── Volatility buckets ────────────────────────────────────────────────────────
_SIM_HIGH_VOL_THRESH = 0.50
_SIM_LOW_VOL_THRESH  = 0.20

# ── Dynamic threshold / streak parameters ────────────────────────────────────
_SIM_THRESH_BASE     = 0.05   # minimum vol threshold (%) — safety floor only
_SIM_THRESH_CAP      = 0.50   # maximum vol threshold (%)
_SIM_THRESH_MULT     = 1.5    # std-dev multiplier for threshold calc
_SIM_THRESH_MIN_HIST = 10     # min vol history samples before dynamic thresh
_SIM_STREAK_BOOST    = 3      # loss streak count that raises threshold
_SIM_STREAK_PAUSE    = 5      # loss streak count that pauses entries
_SIM_BOOST_DUR       = 30 * 60    # boost duration (30m)
_SIM_PAUSE_DUR       = 45 * 60    # pause duration (45m)
_SIM_BALANCE_FLOOR   = 40.0   # auto-reset if balance drops below this
_SIM_CONCENTRATION_CAP = 0.20  # max (min_notional / leverage) as fraction of balance

# ── Dynamic 15m reliability gate ───────────────────────────────────────────────
_SIM_15M_MIN_SAMPLES = 5     # trades before reliability scoring activates; cold-start = relaxed
_SIM_15M_LOOKBACK    = 20    # rolling history window per instrument+direction
_SIM_15M_STRICT_MIN  = 0.20  # reliability score >= this -> STRICT mode (require 15m alignment)
_SIM_15M_DEADZONE    = 0.05  # MODERATE mode: |change_15m| < deadzone treated as flat (passes)
_SIM_1M_MIN_REVERSAL = 0.030 # 1m gate: min % price move to qualify as a real reversal

# ── Stage definitions ─────────────────────────────────────────────────────────
_SIM_STAGE_DEFS = {
    "sprout":      {"label": "SPROUT",      "min_trades": 7,  "min_wr": 0.50, "min_pnl_pct": 0.00, "fail_after": 30},
    "seedling":    {"label": "SEEDLING",    "min_trades": 10, "min_wr": 0.55, "min_pnl_pct": 0.005, "fail_after": 40},
    "germination": {"label": "GERMINATION", "min_trades": 20, "min_wr": 0.55, "min_pnl_pct": 0.30, "fail_after": 50},
    "vegetative":  {"label": "VEGETATIVE",  "min_trades": 25, "min_wr": 0.60, "min_pnl_pct": 0.25, "fail_after": 60},
    "full_bloom":  {"label": "FULL BLOOM",  "min_trades":  0, "min_wr": 0.00, "min_pnl_pct": 0.00, "fail_after": None},
}
_SIM_STAGE_ORDER = ["sprout", "seedling", "germination", "vegetative", "full_bloom"]

# Balance floors: on graduation, inject to max(organic_balance, floor) so the sim
# operates at realistic dollar levels for each stage's intended leverage tier.
_SIM_STAGE_FLOORS = {
    "seedling":    70.0,
    "germination": 300.0,
    "vegetative":  1_000.0,
    "full_bloom":  5_000.0,
}

# ── Stage approach map (non-sprout sizing) ────────────────────────────────────
_SIM_STAGE_APPROACH = {
    "seedling": "pct_10", "germination": "pct_3",
    "vegetative": "pct_2", "full_bloom": "pct_2",
}

# ── Approximate FX rates for notional estimation ──────────────────────────────
_SIM_APPROX_GBPUSD   = 1.34
_SIM_APPROX_EURUSD   = 1.14
_SIM_APPROX_USDJPY   = 162.0

# ── Module-level state ────────────────────────────────────────────────────────
_sim_eligible:          set   = set()
_sim_min_notional:      dict  = {}
_sim:                   dict  = {}    # empty = not started; truthy = running (used by poll_cycle)
_sim_regime_three_way:  list  = []   # three-way confirmations, updated each poll cycle
_sim_weekend_log_next:  float = 0.0  # rate-limits weekend closure log to once per hour


# ── Logging ───────────────────────────────────────────────────────────────────
def _sim_log(msg: str) -> None:
    stage = _sim.get("stage", "?").upper()
    phase = _sim.get("phase", 1)
    print(f"[{_ts()}] \U0001f9ea SIM [{stage}/P{phase}]: {msg}", flush=True)


# ── State persistence ─────────────────────────────────────────────────────────
def _sim_save_state() -> None:
    if not _sim:
        return
    payload = {
        "balance":              _sim["balance"],
        "stage":                _sim["stage"],
        "stage_entry_balance":  _sim["stage_entry_balance"],
        "stage_trades":         _sim["stage_trades"],
        "stage_wins":           _sim["stage_wins"],
        "stage_losses":         _sim["stage_losses"],
        "total_wins":           _sim["total_wins"],
        "total_losses":         _sim["total_losses"],
        "phase":                _sim["phase"],
        "phase_start_time":     _sim["phase_start_time"],
        "phase_entry_balance":  _sim.get("phase_entry_balance", _sim.get("stage_entry_balance", _SIM_START_BALANCE)),
        "phase_consec_losses":  _sim.get("phase_consec_losses", 0),
        "phase_trades":         _sim.get("phase_trades",  0),
        "phase_wins":           _sim.get("phase_wins",    0),
        "phase_losses":         _sim.get("phase_losses",  0),
        "sim_start_time":       _sim["sim_start_time"],
        "eligible_instruments": sorted(_sim_eligible),
        "min_notionals":        _sim_min_notional,
        "open_position":        _sim.get("open_position"),
        "trade_history":        _sim.get("trade_history", [])[-50:],
        "stage_history":        _sim.get("stage_history", []),
        "long_pnl":             _sim.get("long_pnl", 0.0),
        "long_trades":          _sim.get("long_trades", 0),
        "long_wins":            _sim.get("long_wins", 0),
        "short_pnl":            _sim.get("short_pnl", 0.0),
        "short_trades":         _sim.get("short_trades", 0),
        "short_wins":           _sim.get("short_wins", 0),
        "approach_stats":       _sim.get("approach_stats", {}),
        "vol_stats":            _sim.get("vol_stats", {}),
        "reset_count":          _sim.get("reset_count", 0),
        "vol_history":          _sim.get("vol_history", {}),
        "streak_state":         _sim.get("streak_state", {}),
        "boost_expiry":         _sim.get("boost_expiry", {}),
        "pause_expiry":         _sim.get("pause_expiry", {}),
        "last_entry_time":      _sim.get("last_entry_time", 0.0),
        "15m_reliability":      _sim.get("15m_reliability", {}),
        "win_moves":            _sim.get("win_moves", {}),
        "loss_moves":           _sim.get("loss_moves", {}),
        "failure_snapshot":     _sim.get("failure_snapshot"),
        "failure_context_checked": _sim.get("failure_context_checked", True),
        "approach_skip_counts": _sim.get("approach_skip_counts", {}),
    }
    try:
        _redis().set("june_sim_state", json.dumps(payload), ex=72 * 3600)
    except Exception as exc:
        print(f"[{_ts()}] \U0001f9ea SIM: state save failed: {exc}", flush=True)


def _sim_load_state():
    try:
        raw = _redis().get("june_sim_state")
        if raw:
            return json.loads(raw)
    except Exception as exc:
        print(f"[{_ts()}] \U0001f9ea SIM: state load failed: {exc}", flush=True)
    return None


# ── Stage helpers ─────────────────────────────────────────────────────────────
def _sim_check_graduation() -> str:
    """Return 'graduate', 'graduate_rolling', 'fail', or 'continue'.

    Path A (cumulative): stage_trades >= min AND cumulative WR >= threshold AND P&L positive.
    Path B (rolling window): stage_trades >= min AND last-10-same-stage WR >= threshold AND
        rolling P&L positive. Prevents pre-fix historical losses from permanently blocking
        graduation when recent trades clearly meet criteria.
    """
    stage = _sim.get("stage", "sprout")
    if stage == "full_bloom":
        return "continue"
    defn    = _SIM_STAGE_DEFS[stage]
    n       = _sim["stage_trades"]
    wr      = _sim["stage_wins"] / n if n else 0.0
    entry_b = _sim["stage_entry_balance"]
    pnl_pct = (_sim["balance"] - entry_b) / entry_b if entry_b > 0 else 0.0
    # Path A: cumulative stage performance
    if n >= defn["min_trades"] and wr >= defn["min_wr"] and pnl_pct >= defn["min_pnl_pct"]:
        return "graduate"
    # Path B: rolling window -- last 10 trades recorded under the current stage label.
    # trade_history entries carry a "stage" field set at exit time, so filtering by stage
    # prevents trades from a previous stage contaminating this check after graduation.
    if n >= defn["min_trades"]:
        recent = [t for t in _sim.get("trade_history", []) if t.get("stage") == stage][-10:]
        if len(recent) >= 10:
            rw = sum(1 for t in recent if t["dollar_pnl"] > 0) / 10
            rp = sum(t["dollar_pnl"] for t in recent)
            if rw >= defn["min_wr"] and rp > 0:
                return "graduate_rolling"
    if defn["fail_after"] and n >= defn["fail_after"]:
        return "fail"
    return "continue"


def _sim_do_graduate(signals, via_rolling: bool = False) -> bool:
    """Advance to next stage. Returns True if simulation should stop (full_bloom reached)."""
    stage    = _sim.get("stage", "sprout")
    n        = _sim["stage_trades"]
    wr       = _sim["stage_wins"] / n if n else 0.0
    pnl      = _sim["balance"] - _sim["stage_entry_balance"]
    label    = _SIM_STAGE_DEFS[stage]["label"]
    cur_idx  = _SIM_STAGE_ORDER.index(stage)
    if cur_idx + 1 >= len(_SIM_STAGE_ORDER):
        return True
    next_stage = _SIM_STAGE_ORDER[cur_idx + 1]
    next_label = _SIM_STAGE_DEFS[next_stage]["label"]

    _sim["stage_history"].append({
        "stage": stage, "trades": n, "wins": _sim["stage_wins"],
        "win_rate": round(wr, 4), "pnl": round(pnl, 4),
        "balance_exit": round(_sim["balance"], 2),
    })
    _sim["stage"]               = next_stage
    # Balance injection: lift balance to stage floor if organic trading left it below
    _floor = _SIM_STAGE_FLOORS.get(next_stage, 0.0)
    if _floor > 0 and _sim["balance"] < _floor:
        _old_b = _sim["balance"]
        _sim["balance"] = _floor
        _sim_log(f"💰 Balance injection: ${_old_b:.2f} → ${_floor:.2f} ({next_label} stage floor)")
    _sim["stage_entry_balance"] = _sim["balance"]
    _sim["stage_trades"]        = 0
    _sim["stage_wins"]          = 0
    _sim["stage_losses"]        = 0
    _sim["phase"]               = 1
    _sim["phase_start_time"]    = time.time()
    _sim["phase_entry_balance"] = _sim["balance"]
    _sim["phase_consec_losses"] = 0
    _sim["phase_trades"]        = 0
    _sim["phase_wins"]          = 0
    _sim["phase_losses"]        = 0
    _sim.pop("approach_skip_counts", None)
    _sim.pop("approach_stats",       None)

    if via_rolling:
        recent = [t for t in _sim.get("trade_history", []) if t.get("stage") == stage][-10:]
        rw = sum(1 for t in recent if t["dollar_pnl"] > 0) / len(recent) if recent else 0.0
        rp = sum(t["dollar_pnl"] for t in recent)
        _sim_log(
            f"🌱 {label} COMPLETE (rolling window): last 10 trades {rw:.0%} WR, "
            f"{rp:+.2f} -- cumulative {wr:.0%} WR dragged by pre-fix losses. "
            f"{next_label} STAGE UNLOCKED"
        )
    else:
        _sim_log(
            f"{label} COMPLETE: {n} trades, {wr:.0%} WR, balance "
            f"${_sim['balance']:.2f} ({pnl:+.2f}) -- {next_label} UNLOCKED"
        )

    if next_stage == "full_bloom":
        _sim_log("\U0001f338 FULL BLOOM REACHED -- simulation complete")
        _sim_stop("full_bloom_reached", signals)
        return True

    _sim_save_state()
    return False



# ── Phase management ──────────────────────────────────────────────────────────
def _sim_phase_leverage(phase: int) -> int:
    """Return leverage multiplier for the given phase number."""
    if phase == 1: return _SIM_CONSERVATIVE_LEV
    if phase == 2: return _SIM_PHASE2_LEV
    return _SIM_AGGRESSIVE_LEV


def _sim_check_phase() -> None:
    """Performance-based phase advancement and drop-back."""
    phase   = _sim.get("phase", 1)
    n       = _sim.get("phase_trades", 0)
    wins    = _sim.get("phase_wins",   0)
    wr      = wins / n if n else 0.0
    entry_b = _sim.get("phase_entry_balance", _sim.get("stage_entry_balance", _SIM_START_BALANCE))
    pnl_pct = (_sim["balance"] - entry_b) / entry_b if entry_b > 0 else 0.0
    consec  = _sim.get("phase_consec_losses", 0)

    # Drop-back runs first — catches losing runs before considering advancement
    if phase > 1:
        if consec >= _SIM_PHASE_DROP_LOSSES or pnl_pct <= _SIM_PHASE_DROP_PNL:
            new_phase = phase - 1
            reason    = (f"{consec} consecutive losses"
                         if consec >= _SIM_PHASE_DROP_LOSSES
                         else f"P&L {pnl_pct:+.1%} from phase entry")
            _sim["phase"]               = new_phase
            _sim["phase_entry_balance"] = _sim["balance"]
            _sim["phase_consec_losses"] = 0
            _sim["phase_start_time"]    = time.time()
            _sim["phase_trades"]        = 0
            _sim["phase_wins"]          = 0
            _sim["phase_losses"]        = 0
            _sim_log(
                f"📉 SIM: Phase {phase} → Phase {new_phase} ({_sim_phase_leverage(new_phase)}:1) "
                f"— {reason} — dropping back to conservative leverage"
            )
            _sim_save_state()
            return

    # Advancement
    if phase == 1 and n >= _SIM_P1_TO_P2_TRADES and wr >= _SIM_P1_TO_P2_WR and pnl_pct > 0:
        _sim["phase"]               = 2
        _sim["phase_entry_balance"] = _sim["balance"]
        _sim["phase_consec_losses"] = 0
        _sim["phase_start_time"]    = time.time()
        _sim["phase_trades"]        = 0
        _sim["phase_wins"]          = 0
        _sim["phase_losses"]        = 0
        _sim_log(
            f"📈 SIM: Phase 1 → Phase 2 (5:1) "
            f"— {n} trades, {wr:.0%} WR, +${_sim['balance'] - entry_b:.2f} P&L — criteria met"
        )
        _sim_save_state()
    elif phase == 2 and n >= _SIM_P2_TO_P3_TRADES and wr >= _SIM_P2_TO_P3_WR and pnl_pct >= _SIM_P2_TO_P3_PNL:
        _sim["phase"]               = 3
        _sim["phase_entry_balance"] = _sim["balance"]
        _sim["phase_consec_losses"] = 0
        _sim["phase_start_time"]    = time.time()
        _sim["phase_trades"]        = 0
        _sim["phase_wins"]          = 0
        _sim["phase_losses"]        = 0
        _sim_log(
            f"📈 SIM: Phase 2 → Phase 3 (10:1) "
            f"— {n} trades, {wr:.0%} WR, +${_sim['balance'] - entry_b:.2f} P&L — criteria met"
        )
        _sim_save_state()


# ── Price helpers ─────────────────────────────────────────────────────────────
def _sim_reconstruct_prices(sym: str, signals: dict) -> dict:
    sig        = signals.get(sym, {})
    mid        = sig.get("price", 0.0)
    spread_pct = sig.get("spread_pct", 0.0)
    half       = mid * spread_pct / 200.0
    return {"bid": mid - half, "ask": mid + half, "mid": mid}


# ── Position sizing ───────────────────────────────────────────────────────────
def _sim_position_size(balance: float, approach: str) -> float:
    stage = _sim.get("stage", "sprout")
    if stage == "sprout":
        if approach == "fixed_5":  return 5.0
        if approach == "fixed_10": return 10.0
        if approach == "pct_5":    return round(balance * 0.05, 2)
        if approach == "pct_10":   return round(balance * 0.10, 2)
        return 5.0
    if stage == "seedling":    return round(balance * 0.10, 2)
    if stage == "germination": return round(balance * 0.03, 2)
    if stage == "vegetative":  return round(balance * 0.02, 2)
    return round(balance * 0.05, 2)


def _sim_check_min_feasible(sym: str, pos_size: float, leverage: int) -> bool:
    return (pos_size * leverage) >= _sim_min_notional.get(sym, 0.0)


def _sim_is_eligible(sym: str, balance: float, leverage: int) -> bool:
    """Concentration-cap formula: True when min_notional/leverage <= 20% of balance.
    Replaces curated stage-specific eligible-instrument lists.
    Any instrument with known min_notional data is evaluated automatically.
    """
    min_n = _sim_min_notional.get(sym)
    if min_n is None:
        return False
    return (min_n / leverage) <= (_SIM_CONCENTRATION_CAP * balance)


def _sim_vol_bucket(vol: float) -> str:
    if vol > _SIM_HIGH_VOL_THRESH: return "high"
    if vol >= _SIM_LOW_VOL_THRESH: return "mid"
    return "low"


# ── Dynamic threshold / streak helpers ───────────────────────────────────────
def _sim_combo_key(sym: str, direction: str) -> str:
    return f"{sym}_{direction}"


def _sim_is_paused(combo: str) -> bool:
    exp = (_sim.get("pause_expiry") or {}).get(combo, 0.0)
    return time.time() < exp


def _sim_has_boost(combo: str) -> bool:
    exp = (_sim.get("boost_expiry") or {}).get(combo, 0.0)
    return time.time() < exp


def _sim_get_threshold(sym: str, direction: str = "", low_tier: bool = False) -> float:
    hist = (_sim.get("vol_history") or {}).get(sym, [])
    mult = 2.0 if low_tier else _SIM_THRESH_MULT
    if len(hist) < _SIM_THRESH_MIN_HIST:
        base_thresh = 0.15
    else:
        n = len(hist)
        mean = sum(hist) / n
        var = sum((x - mean) ** 2 for x in hist) / (n - 1) if n > 1 else 0.0
        base_thresh = max(_SIM_THRESH_BASE, min(_SIM_THRESH_CAP, var ** 0.5 * mult))
    if direction:
        combo = _sim_combo_key(sym, direction)
        if _sim_has_boost(combo):
            base_thresh = min(_SIM_THRESH_CAP, base_thresh * 1.5)
    return round(base_thresh, 4)


def _sim_update_vol_history(signals: dict) -> None:
    if not _sim:
        return
    vol_hist = _sim.setdefault("vol_history", {})
    for sym in _sim_eligible:
        if sym not in signals:
            continue
        abs_chg = abs(signals[sym].get("change_5m", 0.0))
        bucket = vol_hist.setdefault(sym, [])
        bucket.append(abs_chg)
        if len(bucket) > 20:
            bucket.pop(0)


def _sim_update_streak(sym: str, direction: str, won: bool) -> None:
    combo        = _sim_combo_key(sym, direction)
    streak_state = _sim.setdefault("streak_state", {})
    boost_expiry = _sim.setdefault("boost_expiry", {})
    pause_expiry = _sim.setdefault("pause_expiry", {})
    if won:
        streak_state[combo] = 0
        boost_expiry[combo] = 0.0
        pause_expiry[combo] = 0.0
        return
    streak_state[combo] = streak_state.get(combo, 0) + 1
    n = streak_state[combo]
    if n == _SIM_STREAK_BOOST:
        old_thresh = _sim_get_threshold(sym)
        new_thresh = min(_SIM_THRESH_CAP, old_thresh * 1.5)
        boost_end  = time.time() + _SIM_BOOST_DUR
        boost_expiry[combo] = boost_end
        resume = datetime.fromtimestamp(boost_end, tz=timezone.utc).strftime("%H:%M UTC")
        _sim_log(
            f"\u26a0\ufe0f  {sym} {direction.upper()} streak {n} "
            f"\u2014 raising threshold {old_thresh:.2f}% \u2192 {new_thresh:.2f}% for {int(_SIM_BOOST_DUR // 60)}m "
            f"(resumes {resume})"
        )
    elif n >= _SIM_STREAK_PAUSE:
        pause_end = time.time() + _SIM_PAUSE_DUR
        pause_expiry[combo] = pause_end
        boost_expiry[combo] = 0.0
        action = "pausing" if n == _SIM_STREAK_PAUSE else "extending pause"
        resume = datetime.fromtimestamp(pause_end, tz=timezone.utc).strftime("%H:%M UTC")
        _sim_log(
            f"\U0001f6d1 {sym} {direction.upper()} streak {n} "
            f"\u2014 {action} entries for {int(_SIM_PAUSE_DUR // 60)}m (resumes {resume})"
        )


def _sim_15m_record(sym, direction, change_15m, won):
    """Record 15m signal vs outcome for per-instrument directional reliability scoring."""
    rel      = _sim.setdefault("15m_reliability", {})
    sym_data = rel.setdefault(sym, {})
    dir_data = sym_data.setdefault(direction, {"history": []})
    agreed   = (direction == "long" and change_15m > 0) or (direction == "short" and change_15m < 0)
    dir_data["history"].append({"agreed": agreed, "won": won, "change_15m": round(change_15m, 4)})
    if len(dir_data["history"]) > _SIM_15M_LOOKBACK:
        dir_data["history"] = dir_data["history"][-_SIM_15M_LOOKBACK:]


def _sim_15m_gate_mode(sym, direction):
    """Returns (mode, score_or_None). Cold-start and low-reliability => relaxed.

    strict   -- 15m must agree with 5m direction (reliability >= _SIM_15M_STRICT_MIN)
    moderate -- 15m may be slightly opposing within deadzone
    relaxed  -- no 15m requirement (cold-start or negative reliability)
    """
    dir_data = (_sim.get("15m_reliability") or {}).get(sym, {}).get(direction, {})
    hist     = dir_data.get("history", [])
    if len(hist) < _SIM_15M_MIN_SAMPLES:
        return "relaxed", None
    window = hist[-_SIM_15M_LOOKBACK:]
    aw = al = dw = dl = 0
    for rec in window:
        if rec["agreed"]:
            if rec["won"]: aw += 1
            else:          al += 1
        else:
            if rec["won"]: dw += 1
            else:          dl += 1
    agreed_wr    = aw / (aw + al) if (aw + al) else 0.5
    disagreed_wr = dw / (dw + dl) if (dw + dl) else 0.5
    reliability  = round(agreed_wr - disagreed_wr, 3)
    if reliability >= _SIM_15M_STRICT_MIN:
        return "strict", reliability
    if reliability >= 0.0:
        return "moderate", reliability
    return "relaxed", reliability


# ── Instrument selection ──────────────────────────────────────────────────────
def _sim_get_tp(sym: str, direction: str) -> float:
    """Dynamic TP from actual instrument move history — no global cold-start constant.
    Vol_history values are in percent; win/loss_moves are fractions (pnl_pct scale).
    Takes the minimum across all available estimates to stay within achievable range."""
    combo  = f"{sym}_{direction}"
    wins   = (_sim.get("win_moves")  or {}).get(combo, [])
    losses = (_sim.get("loss_moves") or {}).get(combo, [])
    hist   = (_sim.get("vol_history") or {}).get(sym, [])

    estimates = []

    # Primary: winning trade history — 70% of avg win (80% when <3 samples)
    if wins:
        fraction = _SIM_TP_WIN_FRACTION if len(wins) >= _SIM_TP_MIN_SAMPLES else 0.80
        estimates.append((sum(wins) / len(wins)) * fraction)

    # Secondary: 25% of avg losing move — achievable target within the adverse range
    if len(losses) >= 3:
        estimates.append((sum(losses) / len(losses)) * 0.25)

    # Tertiary: 30% of vol_mean (vol_history in % — divide /100 to get fraction)
    if len(hist) >= 5:
        estimates.append((sum(hist) / len(hist)) / 100.0 * _SIM_TP_VOL_FRACTION)

    if estimates:
        return round(max(_SIM_TP_FLOOR, min(_SIM_TP_CAP, min(estimates))), 6)

    # True cold-start (zero history): instrument-type micro-defaults
    if "JPY" in sym:
        return 0.0003
    if sym == "SILVER":
        return 0.0004   # commodity with wide spread — needs bigger move to profit
    return 0.0002   # all forex pairs (EURUSD, GBPUSD, AUDUSD, USDCAD, EURGBP, NZDUSD, USDCHF…)


def _sim_get_dynamic_stop(sym: str) -> float:
    """Vol-based stop-loss per instrument. Returns fraction (0.002 = 0.2%).
    Vol_history values are in percent — divided /100 to convert to fraction."""
    hist = (_sim.get("vol_history") or {}).get(sym, [])
    if len(hist) < 5:
        return _SIM_STOP_COLD
    n      = len(hist)
    mean_p = sum(hist) / n
    var_p  = sum((x - mean_p) ** 2 for x in hist) / (n - 1) if n > 1 else 0.0
    stop_p = mean_p + _SIM_STOP_VOL_MULT * (var_p ** 0.5)   # still in percent
    return round(max(_SIM_STOP_FLOOR, min(_SIM_STOP_CAP, stop_p / 100.0)), 6)


def _sim_get_spread_floor(sym: str) -> float:
    """Minimum stop = 2× baseline spread. Returns fraction (0.0015 = 0.15%).
    Reads june_spread_baselines (values already in % units e.g. 0.0638 = 0.0638%).
    Falls back to hardcoded _SIM_SPREAD_FLOORS on Redis miss."""
    try:
        raw = _redis().get("june_spread_baselines")
        if raw:
            spread_pct = json.loads(raw).get("baselines", {}).get(sym)
            if spread_pct is not None:
                return round(2.0 * spread_pct / 100.0, 6)
    except Exception:
        pass
    return _SIM_SPREAD_FLOORS.get(sym, _SIM_STOP_FLOOR)


def _sim_regime_weight(sym: str, direction: str) -> float:
    """Return signal-strength multiplier based on active macro three-way confirmations.
    Returns 1.0 when no matching regime is active or instrument not specifically weighted."""
    if not _sim_regime_three_way:
        return 1.0
    for conf in _sim_regime_three_way:
        note = conf.get("note", "").lower()
        dirn = conf.get("direction", "")
        if dirn == "dollar_strength" or "dollar strength" in note:
            if sym == "USDJPY":                                       return 1.25
            if sym == "SILVER" and direction == "short":             return 1.35
            if sym in ("EURUSD", "GBPUSD") and direction == "long": return 0.70
        elif "commodity bull" in note:
            if sym == "SILVER":                                       return 1.50
            if sym in ("EURUSD", "GBPUSD"):                          return 0.85
            if sym == "USDJPY":                                       return 0.80
        elif "risk-on" in note:
            if sym in ("EURUSD", "GBPUSD"):                          return 1.20
            if sym == "USDJPY" and direction == "long":              return 1.15
            if sym == "SILVER":                                       return 0.90
        elif "safe-haven" in note:
            if sym == "SILVER" and direction == "long":              return 1.40
            if sym == "USDJPY" and direction == "long":              return 1.20
            if sym in ("EURUSD", "GBPUSD") and direction == "long": return 0.75
        return 1.0
    return 1.0


def _sim_select_instrument(signals: dict, regime: str):
    best_sym, best_vol = None, 0.0
    _sel_bal = _sim.get("balance", _SIM_START_BALANCE)
    _sel_lev = _sim_phase_leverage(_sim.get("phase", 1))
    for sym in _sim_eligible:
        if not _sim_is_eligible(sym, _sel_bal, _sel_lev):
            continue
        if sym not in signals:
            continue
        sig  = signals[sym]
        vol  = abs(sig.get("change_5m", 0.0))
        dirn = sig.get("direction", "neutral")
        direction_str = "long" if dirn == "bull" else ("short" if dirn == "bear" else "")
        vbkt   = _sim_vol_bucket(vol)
        thresh = _sim_get_threshold(sym, direction_str, low_tier=(vbkt == "low"))
        if vol < thresh:
            continue
        if sig.get("spread_alert"):
            continue  # spread currently elevated (>3x avg) — skip to avoid wide fill cost
        if direction_str and _sim_is_paused(_sim_combo_key(sym, direction_str)):
            exp = (_sim.get("pause_expiry") or {}).get(_sim_combo_key(sym, direction_str), 0.0)
            resume = datetime.fromtimestamp(exp, tz=timezone.utc).strftime("%H:%M UTC")
            if vol >= 2 * thresh:
                _sim_log(
                    f"\U0001f680 SIM: {sym} {direction_str.upper()} override "
                    f"\u2014 signal {vol:.2f}% exceeds 2x threshold {thresh:.2f}% "
                    f"\u2014 bypassing pause"
                )
            else:
                _sim_log(
                    f"\u23ed\ufe0f Skip {sym} {direction_str.upper()} "
                    f"-- paused until {resume}"
                )
                continue
        if regime == "bull"  and dirn != "bull":
            continue
        if regime == "bear"  and dirn != "bear":
            continue
        if regime in ("volatile", "neutral") and dirn == "neutral":
            continue
        weight        = _sim_regime_weight(sym, direction_str)
        effective_vol = vol * weight
        if weight != 1.0:
            _rw_note = next((c.get("note", "")[:22] for c in _sim_regime_three_way), "?")
            _sim_log(f"\U0001f30d {sym} {direction_str.upper()} weight x{weight:.2f}"
                     f" ({_rw_note}...) effective {effective_vol:.3f}%")
        if effective_vol > best_vol:
            best_vol = effective_vol
            best_sym = sym
    return best_sym


# ── P&L calculation ───────────────────────────────────────────────────────────
def _sim_compute_pnl_pct(prices: dict) -> float:
    pos   = _sim.get("open_position") or {}
    entry = pos.get("fill_price", 0)
    dirn  = pos.get("direction", "long")
    if not entry:
        return 0.0
    return (prices["bid"] - entry) / entry if dirn == "long" else (entry - prices["ask"]) / entry


# ── Position close ────────────────────────────────────────────────────────────
def _sim_close_position(prices: dict, exit_reason: str) -> None:
    pos = (_sim.get("open_position") or {}).copy()
    if not pos:
        return

    dirn     = pos["direction"]
    entry    = pos["fill_price"]
    size     = pos["size"]
    lev      = pos["leverage"]
    approach = pos["approach"]
    vol_bkt  = pos["vol_bucket"]
    entry_t  = pos["entry_time"]
    pnl_pct  = _sim_compute_pnl_pct(prices)
    hold_min = (time.time() - entry_t) / 60.0

    if dirn == "long":
        exit_px = pos["stop_price"] if exit_reason == "stop_loss"    else \
                  pos["tp_price"]   if exit_reason == "take_profit"   else \
                  prices["bid"]
    else:
        exit_px = pos["stop_price"] if exit_reason == "stop_loss"    else \
                  pos["tp_price"]   if exit_reason == "take_profit"   else \
                  prices["ask"]

    dollar_pnl = size * lev * pnl_pct
    won        = dollar_pnl > 0

    # Dynamic TP calibration: record absolute move size per combo
    _dtp_combo = pos["instrument"] + "_" + dirn
    if won:
        _wm = _sim.setdefault("win_moves", {})
        _wb = _wm.setdefault(_dtp_combo, [])
        _wb.append(round(abs(pnl_pct), 6))
        if len(_wb) > _SIM_TP_WIN_WINDOW:
            _wm[_dtp_combo] = _wb[-_SIM_TP_WIN_WINDOW:]
    else:
        _lm = _sim.setdefault("loss_moves", {})
        _lb = _lm.setdefault(_dtp_combo, [])
        _lb.append(round(abs(pnl_pct), 6))
        if len(_lb) > _SIM_TP_WIN_WINDOW:
            _lm[_dtp_combo] = _lb[-_SIM_TP_WIN_WINDOW:]

    _sim["balance"]       += dollar_pnl
    _sim["stage_trades"]  += 1
    _sim["stage_wins"]    += int(won)
    _sim["stage_losses"]  += int(not won)
    _sim["phase_trades"]   = _sim.get("phase_trades", 0) + 1
    _sim["phase_wins"]     = _sim.get("phase_wins",   0) + int(won)
    _sim["phase_losses"]   = _sim.get("phase_losses", 0) + int(not won)
    if won:
        _sim["phase_consec_losses"] = 0
    else:
        _sim["phase_consec_losses"] = _sim.get("phase_consec_losses", 0) + 1
    _sim["total_wins"]    += int(won)
    _sim["total_losses"]  += int(not won)

    trade_rec = {
        "instrument": pos["instrument"], "direction": dirn,
        "entry_price": entry, "exit_price": exit_px,
        "size": size, "leverage": lev,
        "pnl_pct": round(pnl_pct, 6), "dollar_pnl": round(dollar_pnl, 4),
        "hold_min": round(hold_min, 1), "exit_reason": exit_reason,
        "approach": approach, "vol_bucket": vol_bkt,
        "entry_vol": pos.get("entry_vol", 0),
        "stage": _sim.get("stage", "sprout"), "phase": _sim.get("phase", 1),
    }
    history = _sim.setdefault("trade_history", [])
    history.append(trade_rec)
    if len(history) > 50:
        _sim["trade_history"] = history[-50:]

    if dirn == "long":
        _sim["long_pnl"]    += dollar_pnl
        _sim["long_trades"] += 1
        if won: _sim["long_wins"] += 1
    else:
        _sim["short_pnl"]    += dollar_pnl
        _sim["short_trades"] += 1
        if won: _sim["short_wins"] += 1

    _instr_app = _sim.setdefault("approach_stats", {}).setdefault(pos["instrument"], {})
    st = _instr_app.setdefault(approach, {"pnl": 0.0, "trades": 0, "wins": 0})
    st["pnl"] += dollar_pnl; st["trades"] += 1
    if won: st["wins"] += 1

    vs = _sim.setdefault("vol_stats", {}).setdefault(vol_bkt, {"pnl": 0.0, "trades": 0, "wins": 0})
    vs["pnl"] += dollar_pnl; vs["trades"] += 1
    if won: vs["wins"] += 1

    sign = "+" if dollar_pnl >= 0 else ""
    _sim_log(
        f"EXIT: {pos['instrument']} @ {exit_px:.6g} | "
        f"P&L {sign}{dollar_pnl:.2f} ({pnl_pct:+.2%}) | "
        f"hold {hold_min:.0f}min | {exit_reason}"
    )
    _sim_log(
        f"BALANCE: ${_sim['balance']:.2f} | "
        f"stage {_sim['stage_wins']}W/{_sim['stage_losses']}L | "
        f"total {_sim['total_wins']}W/{_sim['total_losses']}L"
    )
    _sim["open_position"] = None
    _sim_update_streak(pos["instrument"], dirn, won)
    _sim_15m_record(pos["instrument"], dirn, pos.get("entry_change_15m", 0.0), won)
    _sim_save_state()


# ── Entry ─────────────────────────────────────────────────────────────────────
def _sim_try_entry(signals: dict, regime: str, leverage: int) -> None:
    balance = _sim["balance"]
    sym     = _sim_select_instrument(signals, regime)
    if not sym or sym not in signals:
        return

    stage    = _sim.get("stage", "sprout")
    approach = _sim_select_approach(stage, sym)
    if approach == "__all_infeasible__":
        _sim_log(
            f"Skip {sym}: all sizing approaches excluded at balance ${balance:.2f} -- "
            f"waiting for balance growth to clear IG min ${_sim_min_notional.get(sym, 0):.2f}"
        )
        return

    sig       = signals[sym]
    chg       = sig.get("change_5m", 0.0)
    vol       = abs(chg)
    direction = "long" if chg > 0 else "short"

    # 1m gate: only block if opposing move is >= _SIM_1M_MIN_REVERSAL%
    # Noise-filtered -- tiny spread-level ticks no longer block entries.
    price_1m = _price_n_minutes_ago(sym, 1)
    if price_1m is not None and price_1m > 0:
        current_px   = sig.get("price", 0.0)
        reversal_pct = abs(current_px - price_1m) / price_1m * 100.0
        if direction == "long" and current_px < price_1m and reversal_pct >= _SIM_1M_MIN_REVERSAL:
            _sim_log(
                f"\u23ed\ufe0f Skip {sym} -- 5m bull signal but 1m reversing "
                f"({price_1m:.4f} -> {current_px:.4f}, -{reversal_pct:.3f}%) (entry timing gate)"
            )
            return
        if direction == "short" and current_px > price_1m and reversal_pct >= _SIM_1M_MIN_REVERSAL:
            _sim_log(
                f"\u23ed\ufe0f Skip {sym} -- 5m bear signal but 1m reversing "
                f"({price_1m:.4f} -> {current_px:.4f}, +{reversal_pct:.3f}%) (entry timing gate)"
            )
            return

    # 15-minute dynamic reliability gate
    # Mode (strict/moderate/relaxed) self-calibrates per instrument+direction
    # based on observed 15m predictive accuracy. Cold-start and low-reliability
    # combos use relaxed mode so volume accumulates to build the scoring model.
    change_15m   = sig.get("change_15m", 0.0)
    hist_sym     = _history.get(sym)
    has_15m_hist = hist_sym is not None and len(hist_sym) >= 15
    gate_mode, rel_score = _sim_15m_gate_mode(sym, direction)
    rel_tag = f"rel={rel_score:.2f}" if rel_score is not None else "cold-start"

    if has_15m_hist and gate_mode != "relaxed":
        blocked = False
        if gate_mode == "strict":
            blocked = (direction == "long" and change_15m <= 0) or \
                      (direction == "short" and change_15m >= 0)
        else:  # moderate: only block meaningfully opposing 15m signal
            blocked = (direction == "long" and change_15m <= -_SIM_15M_DEADZONE) or \
                      (direction == "short" and change_15m >= _SIM_15M_DEADZONE)
        if blocked:
            oppose_desc = f"15m negative ({change_15m:+.3f}%)" if direction == "long" \
                          else f"15m positive ({change_15m:+.3f}%)"
            _sim_log(
                f"\u23ed\ufe0f Skip {sym} -- {oppose_desc} [{gate_mode}/{rel_tag}] (multi-TF gate)"
            )
            return
    elif not has_15m_hist:
        count_h = len(hist_sym) if hist_sym else 0
        _sim_log(
            f"\u2139\ufe0f  {sym} 15m history insufficient ({count_h}/15 readings) "
            f"-- using 5m+1m only [{gate_mode}/{rel_tag}]"
        )

    # Dynamic threshold diagnostic (log when thresh meaningfully above base)
    thresh_d = _sim_get_threshold(sym, direction)
    if thresh_d > 0.16:
        hist_d = (_sim.get("vol_history") or {}).get(sym, [])
        if len(hist_d) >= _SIM_THRESH_MIN_HIST:
            nd = len(hist_d)
            meand = sum(hist_d) / nd
            vard = sum((x - meand) ** 2 for x in hist_d) / (nd - 1) if nd > 1 else 0.0
            _sim_log(
                f"\U0001f4ca {sym} threshold: {thresh_d:.2f}% "
                f"(vol_std={vard**0.5:.2f}%, \u00d7{_SIM_THRESH_MULT:.1f}) "
                f"\u2014 above base {_SIM_THRESH_BASE:.2f}%"
            )

    stage     = _sim.get("stage", "sprout")

    if stage == "sprout":
        # Rotation schedule with escalation if notional too small
        start_idx       = _SIM_SIZING_ORDER.index(approach) if approach in _SIM_SIZING_ORDER else 0
        chosen_approach = None
        pos_size        = None
        for try_approach in _SIM_SIZING_ORDER[start_idx:]:
            candidate = min(_sim_position_size(balance, try_approach), balance)
            if candidate < 1.0:
                continue
            if _sim_check_min_feasible(sym, candidate, leverage):
                chosen_approach = try_approach
                pos_size        = candidate
                break
        if chosen_approach is None:
            min_n   = _sim_min_notional.get(sym, 0)
            max_eff = min(_sim_position_size(balance, _SIM_SIZING_ORDER[-1]), balance) * leverage
            _sc_sp = _sim.setdefault("approach_skip_counts", {}).setdefault(sym, {})
            for _a_sp in _SIM_SIZING_ORDER:
                _sd_sp = _sc_sp.setdefault(_a_sp, {"count": 0, "balance": 0.0})
                _sd_sp["count"] += 1
                _sd_sp["balance"] = balance
            _sim_log(
                f"Skip {sym}: no sizing approach meets IG min ${min_n:.2f} "
                f"(max effective ${max_eff:.2f} with {_SIM_SIZING_ORDER[-1]})"
            )
            return
        if chosen_approach != approach:
            _sim_log(f"Size up: {approach} -> {chosen_approach} to meet {sym} min notional")
        approach = chosen_approach
    else:
        # Formula-based sizing for later stages
        pos_size = min(_sim_position_size(balance, approach), balance)
        if pos_size < 1.0:
            _sim_log(f"Skip {sym}: position size ${pos_size:.2f} < $1 floor")
            return
        if not _sim_check_min_feasible(sym, pos_size, leverage):
            min_n      = _sim_min_notional.get(sym, 0)
            max_cap    = balance * 0.20
            min_needed = math.ceil(min_n / leverage * 100) / 100
            if min_needed <= max_cap:
                _sim_log(
                    f"Size up {sym}: {approach} ${pos_size:.2f} → ${min_needed:.2f} "
                    f"(min notional escalation, {min_needed / balance * 100:.0f}% of balance)"
                )
                pos_size = min_needed
            else:
                _sc_ns = _sim.setdefault("approach_skip_counts", {}).setdefault(sym, {})
                for _a_ns in _SIM_SIZING_ORDER:
                    _sd_ns = _sc_ns.setdefault(_a_ns, {"count": 0, "balance": 0.0})
                    _sd_ns["count"] += 1
                    _sd_ns["balance"] = balance
                _sim_log(
                    f"Skip {sym}: effective ${pos_size * leverage:.2f} < IG min ${min_n:.2f} "
                    f"(min needed ${min_needed:.2f} exceeds 20% cap ${max_cap:.2f})"
                )
                return

    prices  = _sim_reconstruct_prices(sym, signals)
    fill    = prices["ask"] if direction == "long" else prices["bid"]
    if fill <= 0:
        return

    _stop_vol    = _sim_get_dynamic_stop(sym)
    _spread_flr  = _sim_get_spread_floor(sym)
    stop_pct     = max(_stop_vol, _spread_flr)
    if stop_pct > _stop_vol:
        _sim_log(
            f"📊 SIM: {sym} stop raised {_stop_vol*100:.2f}% → {stop_pct*100:.2f}% "
            f"(spread floor: baseline spread {_spread_flr / 2 * 100:.2f}%)"
        )
    stop_px  = fill * (1 - stop_pct) if direction == "long" else fill * (1 + stop_pct)
    tp_pct   = _sim_get_tp(sym, direction)
    # Ensure TP covers at least 1x baseline spread (entry fill is at offer/bid, so
    # the price must move >= 1x spread from mid before TP becomes reachable).
    _tp_spread_floor = _sim_get_spread_floor(sym) / 2   # spread_floor = 2x spread -> half = 1x
    if tp_pct < _tp_spread_floor:
        tp_pct = _tp_spread_floor
    tp_px    = fill * (1 + tp_pct)   if direction == "long" else fill * (1 - tp_pct)
    _tp_wins = (_sim.get("win_moves") or {}).get(sym + "_" + direction, [])
    _tp_src  = (f"avg_win {(sum(_tp_wins)/len(_tp_wins))*100:.3f}%×{int(_SIM_TP_WIN_FRACTION*100)}%"
                if _tp_wins else "vol/loss-history")
    _stop_tag = "spread-adj" if stop_pct > _stop_vol else "vol-dynamic"
    _sim_log(f"\U0001f4ca {sym} {direction.upper()} TP {tp_pct*100:.3f}% ({_tp_src}) | "
             f"stop {stop_pct*100:.3f}% ({_stop_tag})")
    vbkt    = _sim_vol_bucket(vol)
    notl    = round(pos_size * leverage, 2)

    _sim["open_position"] = {
        "instrument": sym, "direction": direction,
        "fill_price": fill, "stop_price": stop_px, "tp_price": tp_px,
        "size": pos_size, "leverage": leverage,
        "entry_time": time.time(), "entry_vol": vol,
        "vol_bucket": vbkt, "approach": approach, "regime": regime,
        "entry_change_15m": change_15m, "tp_pct": tp_pct, "stop_pct": stop_pct,
    }

    action = "BUY" if direction == "long" else "SELL"
    _sim_log(
        f"{action}: {sym} @ {fill:.6g} | size ${pos_size:.2f} | "
        f"{leverage}:1 lev | notional ${notl:.0f} | vol {vol:.2f}% | {approach}"
    )
    _sim_save_state()


# ── Exit checks ───────────────────────────────────────────────────────────────
def _sim_check_exit(signals: dict, regime: str) -> None:
    pos = _sim.get("open_position")
    if not pos:
        return
    sym = pos["instrument"]
    if sym not in signals:
        return
    prices   = _sim_reconstruct_prices(sym, signals)
    pnl_pct  = _sim_compute_pnl_pct(prices)
    hold_sec = time.time() - pos["entry_time"]
    dirn     = pos["direction"]
    sig_dir  = signals[sym].get("direction", "neutral")

    # Hard exits — instrument-calibrated thresholds (stored at entry, fallback to live calc)
    stop_pct = pos.get("stop_pct", _sim_get_dynamic_stop(sym))
    if pnl_pct <= -stop_pct:
        _sim_close_position(prices, "stop_loss"); return
    if pnl_pct >= pos.get("tp_pct", _sim_get_tp(sym, dirn)):
        _sim_close_position(prices, "take_profit"); return
    if hold_sec >= _SIM_MAX_HOLD_SECS:
        _sim_close_position(prices, "max_hold"); return

    # Asymmetric reversal: losers cut on first opposing cycle, winners need confirmation
    opposing = (dirn == "long"  and (regime == "bear" or sig_dir == "bear")) or \
               (dirn == "short" and (regime == "bull"  or sig_dir == "bull"))
    if opposing:
        patience  = _SIM_REV_PATIENCE_WIN if pnl_pct > 0 else _SIM_REV_PATIENCE_LOSS
        rev_count = pos.get("reversal_count", 0) + 1
        _sim["open_position"]["reversal_count"] = rev_count
        if rev_count >= patience:
            _sim_close_position(prices, "reversal"); return
    elif pos.get("reversal_count", 0):
        _sim["open_position"]["reversal_count"] = 0   # signal re-aligned, reset counter


# ── Hourly summary ────────────────────────────────────────────────────────────
def _sim_hourly_log() -> None:
    stage     = _sim.get("stage", "sprout")
    st_t      = _sim.get("stage_trades", 0)
    st_w      = _sim.get("stage_wins", 0)
    balance   = _sim.get("balance", _SIM_START_BALANCE)
    stage_pnl = balance - _sim.get("stage_entry_balance", _SIM_START_BALANCE)
    grad_min  = _SIM_STAGE_DEFS.get(stage, {}).get("min_trades", 0)
    wr_str    = f"{st_w/st_t:.0%}" if st_t else "n/a"

    _app_raw = _sim.get("approach_stats", {})
    _app_agg: dict = {}
    for _idict in _app_raw.values():
        for _a, _s in _idict.items():
            _agg = _app_agg.setdefault(_a, {"pnl": 0.0, "trades": 0, "wins": 0})
            _agg["pnl"] += _s["pnl"]; _agg["trades"] += _s["trades"]; _agg["wins"] += _s["wins"]
    best_app = (max(_app_agg.items(), key=lambda x: x[1]["pnl"])[0]
                if any(v.get("trades", 0) for v in _app_agg.values()) else "none")

    lt = _sim.get("long_trades", 0);  lw = _sim.get("long_wins", 0)
    st = _sim.get("short_trades", 0); sw = _sim.get("short_wins", 0)
    lr = f"{lw}/{lt} ({lw/lt:.0%})" if lt else "0/0"
    sr = f"{sw}/{st} ({sw/st:.0%})" if st else "0/0"

    vol_stats = _sim.get("vol_stats", {})
    def _wv(k): v = vol_stats.get(k, {}); return v.get("wins", 0) / max(v.get("trades", 1), 1)
    best_vol = max(["high", "mid", "low"], key=_wv)

    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    _sim_log(
        f"SUMMARY [{now_str}] {stage.upper()} STAGE ({st_t}/{grad_min} trades, "
        f"{wr_str} WR, {stage_pnl:+.2f}): Balance ${balance:.2f} | "
        f"P{_sim.get('phase', 1)} | best approach: {best_app} | "
        f"long {lr} | short {sr} | best vol: {best_vol}"
    )

    # Self-assessment: thresholds, streak state, and 15m gate mode per instrument
    sa_lines = []
    vol_hist_sa   = _sim.get("vol_history", {})
    streak_sa     = _sim.get("streak_state", {})
    pause_exp_sa  = _sim.get("pause_expiry", {})
    boost_exp_sa  = _sim.get("boost_expiry", {})
    now_sa = time.time()
    for sym_sa in sorted(_sim_eligible):
        thresh_sa  = _sim_get_threshold(sym_sa)
        samples_sa = len(vol_hist_sa.get(sym_sa, []))
        parts = []
        for dir_sa in ("long", "short"):
            combo_sa = _sim_combo_key(sym_sa, dir_sa)
            p_exp = pause_exp_sa.get(combo_sa, 0.0)
            b_exp = boost_exp_sa.get(combo_sa, 0.0)
            stk   = streak_sa.get(combo_sa, 0)
            gmode_sa, grel_sa = _sim_15m_gate_mode(sym_sa, dir_sa)
            grel_str = f"{grel_sa:.2f}" if grel_sa is not None else "cold"
            gate_tag = f"15m:{gmode_sa}/{grel_str}"
            if now_sa < p_exp:
                res = datetime.fromtimestamp(p_exp, tz=timezone.utc).strftime("%H:%M UTC")
                parts.append(f"{dir_sa}:PAUSED(resume {res})[{gate_tag}]")
            elif now_sa < b_exp:
                parts.append(f"{dir_sa}:boost(streak={stk})[{gate_tag}]")
            elif stk > 0:
                parts.append(f"{dir_sa}:streak={stk}[{gate_tag}]")
            else:
                parts.append(f"{dir_sa}:[{gate_tag}]")
        combo_str_sa = " | ".join(parts)
        sa_lines.append(
            f"  {sym_sa}: thresh={thresh_sa:.2f}% ({samples_sa}s) | {combo_str_sa}"
        )
    if sa_lines:
        _sim_log("SELF-ASSESSMENT:\n" + "\n".join(sa_lines))


def _sim_select_approach(stage: str, sym: str) -> str:
    """Select sizing approach for (stage, instrument).

    Exploration: iterate _SIM_SIZING_ORDER until each approach has
    _SIM_MIN_APPROACH_TRADES trades for this instrument. Approaches with
    zero trades after total_traded >= 2x the minimum are treated as infeasible
    (IG notional minimum always forces escalation past them). Approaches that
    repeatedly fail the IG minimum notional check (_SIM_SKIP_THRESHOLD skips)
    are excluded until balance grows by _SIM_SKIP_BALANCE_TOL.

    Returns "__all_infeasible__" if every approach is currently excluded by
    skip counts -- caller should skip this instrument for the cycle.

    Exploitation: highest win rate among non-excluded approaches; P&L tiebreaker.

    Non-sprout stages explore their _SIM_STAGE_APPROACH default first so they
    begin with their intended conservative sizing.
    """
    if stage != "sprout":
        stage_default = _SIM_STAGE_APPROACH.get(stage, _SIM_SIZING_ORDER[0])
        _skip_sc      = _sim.get("approach_skip_counts", {}).get(sym, {})
        _sd           = _skip_sc.get(stage_default, {})
        _bal          = _sim.get("balance", 0.0)
        if (_sd.get("count", 0) >= _SIM_SKIP_THRESHOLD
                and _bal <= _sd.get("balance", 0.0) * _SIM_SKIP_BALANCE_TOL):
            return "__all_infeasible__"
        return stage_default

    instr_stats  = _sim.get("approach_stats", {}).get(sym, {})
    skip_counts  = _sim.get("approach_skip_counts", {}).get(sym, {})
    balance      = _sim.get("balance", 0.0)
    total_traded = sum(s.get("trades", 0) for s in instr_stats.values())
    infeas_floor = _SIM_MIN_APPROACH_TRADES * 2

    def _skip_excluded(a: str) -> bool:
        sd = skip_counts.get(a, {})
        if sd.get("count", 0) < _SIM_SKIP_THRESHOLD:
            return False
        return balance <= sd.get("balance", 0.0) * _SIM_SKIP_BALANCE_TOL

    stage_default = _SIM_STAGE_APPROACH.get(stage)
    explore_order = (
        [stage_default] + [a for a in _SIM_SIZING_ORDER if a != stage_default]
        if stage_default else list(_SIM_SIZING_ORDER)
    )

    for a in explore_order:
        if _skip_excluded(a):
            continue
        t = instr_stats.get(a, {}).get("trades", 0)
        if t >= _SIM_MIN_APPROACH_TRADES:
            continue
        if t == 0 and total_traded >= infeas_floor:
            continue  # likely infeasible -- IG minimum always escalates past it
        return a

    viable = [a for a in _SIM_SIZING_ORDER if not _skip_excluded(a)]
    if not viable:
        return "__all_infeasible__"

    def _score(a):
        s = instr_stats.get(a, {})
        t = s.get("trades", 0)
        return (s.get("wins", 0) / t, s.get("pnl", 0.0)) if t > 0 else (0.0, 0.0)

    return max(viable, key=_score)


# ── Simulation stop ───────────────────────────────────────────────────────────
def _sim_stop(reason: str, signals=None) -> None:
    pos = _sim.get("open_position")
    if pos and signals:
        sym = pos.get("instrument", "")
        if sym and sym in signals:
            _sim_close_position(_sim_reconstruct_prices(sym, signals), "simulation_stop")

    # Build failure context snapshot before marking stopped
    _snap_th_s = _sim.get("trade_history", [])
    _snap_it:  dict = {}
    _snap_iw:  dict = {}
    for _t in _snap_th_s:
        _sym_t = _t["instrument"]
        _snap_it[_sym_t] = _snap_it.get(_sym_t, 0) + 1
        _snap_iw[_sym_t] = _snap_iw.get(_sym_t, 0) + int(_t.get("dollar_pnl", 0) > 0)
    _snap_vh_s = _sim.get("vol_history", {})
    _failure_snapshot = {
        "timestamp":         int(time.time()),
        "stop_reason":       reason,
        "vol_regime":        {_s: round(sum(_b) / len(_b), 4) for _s, _b in _snap_vh_s.items() if _b},
        "instrument_trades": _snap_it,
        "instrument_wr":     {_s: round(_snap_iw.get(_s, 0) / _snap_it[_s], 3)
                              for _s in _snap_it if _snap_it[_s] > 0},
    }

    _sim["stopped"]     = True
    _sim["stop_reason"] = reason

    balance  = _sim["balance"]
    pnl      = balance - _SIM_START_BALANCE
    pnl_pct  = pnl / _SIM_START_BALANCE * 100.0
    total    = _sim.get("total_wins", 0) + _sim.get("total_losses", 0)
    wins     = _sim.get("total_wins", 0)

    _app_raw_s = _sim.get("approach_stats", {})
    _app_agg_s: dict = {}
    for _idict_s in _app_raw_s.values():
        for _a_s, _s_s in _idict_s.items():
            _ag_s = _app_agg_s.setdefault(_a_s, {"pnl": 0.0, "trades": 0, "wins": 0})
            _ag_s["pnl"] += _s_s["pnl"]; _ag_s["trades"] += _s_s["trades"]; _ag_s["wins"] += _s_s["wins"]
    app_stats = _app_agg_s
    best_app  = (max(app_stats.items(), key=lambda x: x[1]["pnl"])
                 if any(v.get("trades", 0) for v in app_stats.values())
                 else ("no_trades", {"pnl": 0.0}))

    lt = _sim.get("long_trades", 0);  lw = _sim.get("long_wins", 0)
    st = _sim.get("short_trades", 0); sw = _sim.get("short_wins", 0)
    vs = _sim.get("vol_stats", {})
    def _wv(k): v = vs.get(k, {}); return v.get("wins", 0) / max(v.get("trades", 1), 1)
    best_vol = max(["high", "mid", "low"], key=_wv)

    trades    = _sim.get("trade_history", [])
    instr_pnl: dict = {}
    lev_pnl:   dict = {}
    for t in trades:
        instr_pnl[t["instrument"]] = instr_pnl.get(t["instrument"], 0.0) + t["dollar_pnl"]
        lev_pnl[t["leverage"]]     = lev_pnl.get(t["leverage"],     0.0) + t["dollar_pnl"]

    best_instr  = max(instr_pnl.items(), key=lambda x: x[1]) if instr_pnl else ("none", 0)
    worst_instr = min(instr_pnl.items(), key=lambda x: x[1]) if instr_pnl else ("none", 0)
    best_lev    = max(lev_pnl.items(),   key=lambda x: x[1]) if lev_pnl   else (3, 0)

    if total:
        rec = (
            f"Best sizing: {best_app[0]} (P&L ${best_app[1]['pnl']:+.2f}). "
            f"Longs: ${_sim.get('long_pnl', 0):+.2f} ({lw}/{lt}). "
            f"Shorts: ${_sim.get('short_pnl', 0):+.2f} ({sw}/{st}). "
            f"Best vol entry: {best_vol}. "
            f"Best instrument: {best_instr[0]} (${best_instr[1]:+.2f}). "
            f"Worst instrument: {worst_instr[0]} (${worst_instr[1]:+.2f}). "
            f"Recommended leverage: {best_lev[0]}:1."
        )
    else:
        rec = "No trades completed -- insufficient data for sizing recommendation."

    _sim_log(f"STOPPED: Balance ${balance:.2f} ({pnl:+.2f} / {pnl_pct:+.1f}%) | {reason}")
    _sim_log(f"RECOMMENDATION: {rec}")

    result_payload = {
        "timestamp":            int(time.time()),
        "stop_reason":          reason,
        "final_balance":        round(balance, 4),
        "pnl_usd":              round(pnl, 4),
        "pnl_pct":              round(pnl_pct, 2),
        "total_trades":         total,
        "win_rate_pct":         round(wins / max(total, 1) * 100, 1),
        "stage_reached":        _sim.get("stage", "sprout"),
        "stage_history":        _sim.get("stage_history", []),
        "long_pnl":             round(_sim.get("long_pnl", 0), 4),
        "short_pnl":            round(_sim.get("short_pnl", 0), 4),
        "approach_stats":       {instr: {k: {kk: round(vv, 4) if isinstance(vv, float) else vv
                                              for kk, vv in v.items()}
                                          for k, v in idict.items()}
                                  for instr, idict in _app_raw_s.items()},
        "vol_stats":            {k: {kk: round(vv, 4) if isinstance(vv, float) else vv
                                     for kk, vv in v.items()}
                                 for k, v in vs.items()},
        "instrument_pnl":       {k: round(v, 4) for k, v in instr_pnl.items()},
        "leverage_pnl":         {str(k): round(v, 4) for k, v in lev_pnl.items()},
        "recommendation":       rec,
        "eligible_instruments": sorted(_sim_eligible),
        "trades":               trades[-100:],
    }

    try:
        r = _redis()
        r.set("june_sim_results", json.dumps(result_payload), ex=48 * 3600)
        _cal = {
            "vol_history":      _sim.get("vol_history", {}),
            "win_moves":        _sim.get("win_moves", {}),
            "loss_moves":       _sim.get("loss_moves", {}),
            "failure_snapshot": _failure_snapshot,
        }
        r.set("june_sim_calibration", json.dumps(_cal), ex=7 * 24 * 3600)
        r.delete("june_sim_state")
        _sim_log("Results published -> Redis june_sim_results | calibration preserved (7d TTL)")
    except Exception as exc:
        _sim_log(f"Redis results write failed: {exc}")


# ── Main simulation step ──────────────────────────────────────────────────────
def run_simulation_step(signals: dict) -> None:
    """Called from poll_cycle() after intelligence functions. Paper trades only."""
    if not _sim or _sim.get("stopped"):
        return

    # Manual stop via Redis
    try:
        if _redis().get("june_sim_stop") == b"1":
            _sim_log("Manual stop signal (june_sim_stop=1)")
            _sim_stop("manual_stop", signals)
            return
    except Exception:
        pass

    now     = time.time()
    elapsed = now - _sim["sim_start_time"]

    # Phase check (runs through overnight so timer stays accurate)
    _sim_check_phase()

    # Global duration stop (runs through overnight so 24h wall-clock is respected)
    if elapsed >= 24 * 3600:
        # Capture state before _sim_stop() marks stopped=True (does not clear other fields)
        _24h_stage   = _sim.get("stage", "sprout")
        _24h_phase   = _sim.get("phase", 1)
        _24h_bal     = _sim.get("balance", _SIM_START_BALANCE)
        _24h_st_eb   = _sim.get("stage_entry_balance", _SIM_START_BALANCE)
        _24h_st_t    = _sim.get("stage_trades", 0)
        _24h_st_w    = _sim.get("stage_wins", 0)
        _24h_st_l    = _sim.get("stage_losses", 0)
        _24h_tot_w   = _sim.get("total_wins", 0)
        _24h_tot_l   = _sim.get("total_losses", 0)
        _24h_lpnl    = _sim.get("long_pnl", 0.0)
        _24h_lt      = _sim.get("long_trades", 0)
        _24h_lw      = _sim.get("long_wins", 0)
        _24h_spnl    = _sim.get("short_pnl", 0.0)
        _24h_strd    = _sim.get("short_trades", 0)
        _24h_sw      = _sim.get("short_wins", 0)
        _24h_rc      = _sim.get("reset_count", 0) + 1
        _24h_vh      = dict(_sim.get("vol_history", {}))
        _24h_wm      = dict(_sim.get("win_moves", {}))
        _24h_lm      = dict(_sim.get("loss_moves", {}))
        _24h_ast     = dict(_sim.get("approach_stats", {}))
        _24h_vs      = dict(_sim.get("vol_stats", {}))
        _24h_th      = list(_sim.get("trade_history", []))
        _24h_sh      = list(_sim.get("stage_history", []))
        _24h_snap_it: dict = {}
        _24h_snap_iw: dict = {}
        for _t24 in _24h_th:
            _s24 = _t24["instrument"]
            _24h_snap_it[_s24] = _24h_snap_it.get(_s24, 0) + 1
            _24h_snap_iw[_s24] = _24h_snap_iw.get(_s24, 0) + int(_t24.get("dollar_pnl", 0) > 0)
        _24h_fail_snap = {
            "timestamp":         int(time.time()),
            "stop_reason":       "24h duration",
            "vol_regime":        {_s: round(sum(_b) / len(_b), 4) for _s, _b in _24h_vh.items() if _b},
            "instrument_trades": _24h_snap_it,
            "instrument_wr":     {_s: round(_24h_snap_iw.get(_s, 0) / _24h_snap_it[_s], 3)
                                  for _s in _24h_snap_it if _24h_snap_it[_s] > 0},
        }
        _sim_stop("24h duration", signals)  # closes position, publishes results, marks stopped=True
        # Auto-restart: same stage/phase, fresh 24h clock. A time-based stop is not a
        # strategy failure -- graduation progress and balance carry forward.
        _24h_now = time.time()
        _sim_log(
            f"AUTO-RESTART #{_24h_rc}: 24h clock expired -- resuming {_24h_stage.upper()} "
            f"stage P{_24h_phase} (balance ${_24h_bal:.2f}, "
            f"{_24h_st_t} stage trades) -- calibration preserved"
        )
        _sim.update({
            "active":               True,
            "stopped":              False,
            "stop_reason":          "",
            "balance":              _24h_bal,
            "stage":                _24h_stage,
            "stage_entry_balance":  _24h_st_eb,
            "stage_trades":         _24h_st_t,
            "stage_wins":           _24h_st_w,
            "stage_losses":         _24h_st_l,
            "total_wins":           _24h_tot_w,
            "total_losses":         _24h_tot_l,
            "phase":                _24h_phase,
            "phase_start_time":     _24h_now,
            "phase_entry_balance":  _24h_bal,
            "phase_consec_losses":  0,
            "sim_start_time":       _24h_now,
            "open_position":        None,
            "trade_history":        _24h_th,
            "stage_history":        _24h_sh,
            "hourly_next":          _24h_now + 3600,
            "long_pnl":             _24h_lpnl,
            "long_trades":          _24h_lt,
            "long_wins":            _24h_lw,
            "short_pnl":            _24h_spnl,
            "short_trades":         _24h_strd,
            "short_wins":           _24h_sw,
            "approach_stats":       _24h_ast,
            "vol_stats":            _24h_vs,
            "reset_count":          _24h_rc,
            "streak_state":         {},
            "boost_expiry":         {},
            "pause_expiry":         {},
            "last_entry_time":      _24h_now,
            "15m_reliability":      {},
            "vol_history":          _24h_vh,
            "win_moves":            _24h_wm,
            "loss_moves":           _24h_lm,
            "failure_snapshot":     _24h_fail_snap,
            "failure_context_checked": False,
            "approach_skip_counts": {},
        })
        _sim_save_state()
        return

    # Balance floor auto-reset (runs through overnight)
    if _sim["balance"] < _SIM_BALANCE_FLOOR:
        pos_r = _sim.get("open_position")
        if pos_r:
            sym_r = pos_r.get("instrument", "")
            if sym_r and sym_r in signals:
                _sim_close_position(_sim_reconstruct_prices(sym_r, signals), "auto_reset")
        reset_count_r = _sim.get("reset_count", 0) + 1
        old_bal_r = _sim["balance"]
        now_r = time.time()
        print(
            f"[{_ts()}] \U0001f504 SIM AUTO-RESET: Balance ${old_bal_r:.2f} fell below "
            f"${_SIM_BALANCE_FLOOR:.0f} floor \u2014 resetting to "
            f"${_SIM_START_BALANCE:.2f} Sprout Stage",
            flush=True,
        )
        # Build failure snapshot so next run can compare conditions against this failure
        _snap_th_bf: list = _sim.get("trade_history", [])
        _snap_it_bf: dict = {}
        _snap_iw_bf: dict = {}
        for _t_bf in _snap_th_bf:
            _sym_bf = _t_bf["instrument"]
            _snap_it_bf[_sym_bf] = _snap_it_bf.get(_sym_bf, 0) + 1
            _snap_iw_bf[_sym_bf] = _snap_iw_bf.get(_sym_bf, 0) + int(_t_bf.get("dollar_pnl", 0) > 0)
        _snap_vh_bf = _sim.get("vol_history", {})
        _floor_fail_snap = {
            "timestamp":         int(now_r),
            "stop_reason":       "balance_floor",
            "vol_regime":        {_s: round(sum(_b) / len(_b), 4) for _s, _b in _snap_vh_bf.items() if _b},
            "instrument_trades": _snap_it_bf,
            "instrument_wr":     {_s: round(_snap_iw_bf.get(_s, 0) / _snap_it_bf[_s], 3)
                                  for _s in _snap_it_bf if _snap_it_bf[_s] > 0},
        }
        # Write calibration so failure context survives a service restart
        try:
            _r_bf = _redis()
            _cal_bf = {
                "vol_history":      _sim.get("vol_history", {}),
                "win_moves":        _sim.get("win_moves", {}),
                "loss_moves":       _sim.get("loss_moves", {}),
                "failure_snapshot": _floor_fail_snap,
            }
            _r_bf.set("june_sim_calibration", json.dumps(_cal_bf), ex=7 * 24 * 3600)
        except Exception as _exc_bf:
            _sim_log(f"Redis calibration write failed (balance_floor): {_exc_bf}")
        _sim.update({
            "balance":              _SIM_START_BALANCE,
            "stage":                "sprout",
            "stage_entry_balance":  _SIM_START_BALANCE,
            "stage_trades":         0,
            "stage_wins":           0,
            "stage_losses":         0,
            "phase":                1,
            "phase_start_time":     now_r,
            "phase_entry_balance":  _SIM_START_BALANCE,
            "phase_consec_losses":  0,
            "open_position":        None,
            "reset_count":          reset_count_r,
            "streak_state":         {},
            "boost_expiry":         {},
            "pause_expiry":         {},
            "last_entry_time":      now_r,
            "15m_reliability":      {},
            "failure_snapshot":     _floor_fail_snap,
            "failure_context_checked": False,
            "approach_skip_counts": {},
        })
        _sim_save_state()
        return

    # P&L boundary stops (runs through overnight)
    pnl = _sim["balance"] - _SIM_START_BALANCE
    if pnl >= _SIM_PROFIT_STOP:
        _sim_stop(f"profit boundary (+${pnl:.2f})", signals); return
    if pnl <= _SIM_LOSS_STOP:
        _sim_stop(f"loss boundary (${pnl:.2f})", signals); return

    # Hourly log (fires even during weekend/overnight so the sim stays visible)
    if now >= _sim["hourly_next"]:
        _sim_hourly_log()
        _sim["hourly_next"] = now + 3600

    # Compute stage / leverage for this cycle
    # Approach is selected per-instrument inside _sim_try_entry
    stage    = _sim.get("stage", "sprout")
    leverage = _sim_phase_leverage(_sim.get("phase", 1))

    # Read macro regime (full payload for regime-weighted selection)
    global _sim_regime_three_way
    regime = "neutral"
    try:
        raw = _redis().get("june_macro_regime")
        if raw:
            _mpayload             = json.loads(raw)
            regime                = _mpayload.get("regime", "neutral")
            _sim_regime_three_way = _mpayload.get("three_way_confirmations", [])
    except Exception:
        _sim_regime_three_way = []
        pass

    # Handle open position -- exit check fires even during weekend closure
    if _sim.get("open_position"):
        _sim_check_exit(signals, regime)
        if _sim.get("open_position"):
            return  # still holding, nothing else to do this cycle
        # Position just closed -- check graduation before next entry
        result = _sim_check_graduation()
        if result in ("graduate", "graduate_rolling"):
            if _sim_do_graduate(signals, via_rolling=(result == "graduate_rolling")):
                return  # simulation ended (full_bloom)
            return  # graduated: next cycle uses new stage params
        elif result == "fail":
            n   = _sim["stage_trades"]
            wr  = _sim["stage_wins"] / n if n else 0.0
            lbl = _SIM_STAGE_DEFS.get(stage, {}).get("label", stage.upper())
            _sim_log(
                f"{lbl} STAGE: {n} trades, {wr:.0%} WR -- "
                f"graduation criteria not met after {n} attempts"
            )
            # Capture calibration before _sim_stop modifies stopped flag.
            # _sim_stop reads (not writes) vol_history/win_moves/loss_moves.
            _fail_cal_vol = dict(_sim.get("vol_history", {}))
            _fail_cal_win = dict(_sim.get("win_moves", {}))
            _fail_cal_los = dict(_sim.get("loss_moves", {}))
            _fail_rc      = _sim.get("reset_count", 0) + 1
            # Capture failure snapshot before _sim_stop publishes calibration
            _snap_th_f = _sim.get("trade_history", [])
            _snap_it_f:  dict = {}
            _snap_iw_f:  dict = {}
            for _t_f in _snap_th_f:
                _sym_f = _t_f["instrument"]
                _snap_it_f[_sym_f] = _snap_it_f.get(_sym_f, 0) + 1
                _snap_iw_f[_sym_f] = _snap_iw_f.get(_sym_f, 0) + int(_t_f.get("dollar_pnl", 0) > 0)
            _fail_snapshot = {
                "timestamp":         int(time.time()),
                "stop_reason":       f"{stage}_stage_fail",
                "vol_regime":        {_s: round(sum(_b) / len(_b), 4) for _s, _b in _fail_cal_vol.items() if _b},
                "instrument_trades": _snap_it_f,
                "instrument_wr":     {_s: round(_snap_iw_f.get(_s, 0) / _snap_it_f[_s], 3)
                                      for _s in _snap_it_f if _snap_it_f[_s] > 0},
            }
            _sim_stop(f"{stage}_stage_fail", signals)  # logs STOPPED+RECOMMENDATION, publishes results
            # Auto-restart: reset to Sprout Stage P1 without manual intervention.
            # vol_history / win_moves / loss_moves carry forward so each retry
            # starts with better-tuned stop/TP values rather than relearning blind.
            # approach_stats resets so new trial re-explores all approaches per instrument.
            # failure_snapshot persists so failure-context comparison fires after vol warmup.
            _now_r = time.time()
            _sim_log(f"AUTO-RESTART #{_fail_rc}: resetting to Sprout Stage P1 — calibration preserved")
            _sim.update({
                "active":               True,
                "stopped":              False,
                "stop_reason":          "",
                "balance":              _SIM_START_BALANCE,
                "stage":                "sprout",
                "stage_entry_balance":  _SIM_START_BALANCE,
                "stage_trades":         0,
                "stage_wins":           0,
                "stage_losses":         0,
                "total_wins":           0,
                "total_losses":         0,
                "phase":                1,
                "phase_start_time":     _now_r,
                "phase_entry_balance":  _SIM_START_BALANCE,
                "phase_consec_losses":  0,
                "sim_start_time":       _now_r,
                "open_position":        None,
                "long_pnl":             0.0,
                "long_trades":          0,
                "long_wins":            0,
                "short_pnl":            0.0,
                "short_trades":         0,
                "short_wins":           0,
                "approach_stats":       {},
                "vol_stats":            {b: {"pnl": 0.0, "trades": 0, "wins": 0} for b in ("high", "mid", "low")},
                "reset_count":          _fail_rc,
                "streak_state":         {},
                "boost_expiry":         {},
                "pause_expiry":         {},
                "last_entry_time":      _now_r,
                "15m_reliability":      {},
                "vol_history":          _fail_cal_vol,
                "win_moves":            _fail_cal_win,
                "loss_moves":           _fail_cal_los,
                "failure_snapshot":     _fail_snapshot,
                "failure_context_checked": False,
                "approach_skip_counts": {},
            })
            _sim_save_state()
            return
        # "continue" -- fall through to entry in same cycle
        # Recompute in case stage changed
        stage    = _sim.get("stage", "sprout")
        leverage = _sim_phase_leverage(_sim.get("phase", 1))

    # Weekend block: IG CFD markets close Fri 21:15 UTC → Sun 21:00 UTC.
    # Uses is_weekend_closure() which is UTC-based and DST-safe.
    # Open-position exits have already been handled above; only new entries are blocked.
    if is_weekend_closure():
        global _sim_weekend_log_next
        if now >= _sim_weekend_log_next:
            _sim_log("💤 Weekend market closure — entries blocked until Sunday 21:00 UTC")
            _sim_weekend_log_next = now + 3600
        return

    # Vol history: weekdays only — weekend synthetic prices contaminate the signal
    _sim_update_vol_history(signals)

    # One-time failure-context comparison: fires once after vol_history warms up
    if not _sim.get("failure_context_checked", True):
        _snap_fc    = _sim.get("failure_snapshot") or {}
        _cur_vh_f   = _sim.get("vol_history", {})
        _snap_reg_f = _snap_fc.get("vol_regime", {})
        if _snap_fc and any(len(_b) >= _SIM_THRESH_MIN_HIST for _b in _cur_vh_f.values()):
            _sim_log(
                f"FAILURE-CONTEXT: Prior stop: {_snap_fc.get('stop_reason', '?')}"
                f" at {datetime.fromtimestamp(_snap_fc.get('timestamp', 0), tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
            )
            _sim_log(
                f"FAILURE-CONTEXT: Instrument trades: {_snap_fc.get('instrument_trades', {})}"
                f" | WR: {_snap_fc.get('instrument_wr', {})}"
            )
            for _sym_fc, _cur_hist_fc in _cur_vh_f.items():
                if not _cur_hist_fc or _sym_fc not in _snap_reg_f:
                    continue
                _cur_mean_fc  = sum(_cur_hist_fc) / len(_cur_hist_fc)
                _fail_mean_fc = _snap_reg_f[_sym_fc]
                _ratio_fc     = _cur_mean_fc / _fail_mean_fc if _fail_mean_fc > 0 else float("inf")
                if _ratio_fc > 1.5:
                    _note_fc = (
                        f"vol HIGHER now ({_cur_mean_fc:.3f}% vs {_fail_mean_fc:.3f}% at failure)"
                        " -- conditions shifted, treat calibration as prior"
                    )
                elif _ratio_fc < 0.67:
                    _note_fc = (
                        f"vol LOWER now ({_cur_mean_fc:.3f}% vs {_fail_mean_fc:.3f}% at failure)"
                        " -- conditions shifted, treat calibration as prior"
                    )
                else:
                    _note_fc = (
                        f"vol SIMILAR now ({_cur_mean_fc:.3f}% vs {_fail_mean_fc:.3f}% at failure)"
                        " -- similar conditions, treat calibration with caution"
                    )
                _sim_log(f"FAILURE-CONTEXT: {_sym_fc}: {_note_fc}")
            _sim["failure_context_checked"] = True
            _sim_save_state()

    # No open position -- look for entry
    _sim_try_entry(signals, regime, leverage)


# ── Startup / resume ──────────────────────────────────────────────────────────
def sim_startup() -> None:
    """Try to resume from saved Redis state, otherwise start fresh."""
    global _sim_eligible, _sim_min_notional

    saved = _sim_load_state()
    if saved:
        _sim_eligible     = set(saved.get("eligible_instruments", []))
        _sim_min_notional = saved.get("min_notionals", {})
        # Migration: expand _sim_eligible to all known-notional instruments;
        # formula in _sim_is_eligible() gates actual selection per cycle.
        for _s in _sim_min_notional:
            _sim_eligible.add(_s)

        # Backfill eligibility for instruments added to INSTRUMENTS since last save.
        # min_notionals stores all queried instruments (eligible or not) so we only
        # query instruments that have never been seen before.
        _missing_syms = [s for s in INSTRUMENTS
                         if s not in _sim_min_notional and s not in _sim_eligible]
        if _missing_syms:
            _max_eff = _SIM_START_BALANCE * 0.10 * _SIM_AGGRESSIVE_LEV
            print(f"[{_ts()}] \U0001f9ea SIM: Backfilling eligibility for {len(_missing_syms)} new instruments: {_missing_syms}", flush=True)
            for _sym in _missing_syms:
                _epic = INSTRUMENTS[_sym]
                _data = _ig_get(f"/markets/{_epic}")
                if not _data:
                    print(f"[{_ts()}] \U0001f9ea SIM:   {_sym}: API query failed -- skipping", flush=True)
                    continue
                _inst    = _data.get("instrument", {})
                _deal    = _data.get("dealingRules", {})
                _snap    = _data.get("snapshot", {})
                _min_val = float(_deal.get("minDealSize", {}).get("value", 1.0))
                _lot_sz  = float(_inst.get("lotSize", 1.0))
                _bid     = float(_snap.get("bid") or 0)
                _offer   = float(_snap.get("offer") or 0)
                _mid     = (_bid + _offer) / 2.0 if _bid and _offer else 0.0
                # Equity CFDs: IG returns null bid/offer — fetch price via Finnhub
                if _mid == 0.0 and ".CASH." in _epic:
                    _fh_sym = _INSTRUMENTS_REVERSE.get(_epic)
                    if _fh_sym:
                        _fh = _finnhub_price(_fh_sym)
                        if _fh:
                            _mid = _fh["mid"]
                _ccy0     = _inst.get("currencies", [{}])[0] if _inst.get("currencies") else {}
                _fx_base  = float(_ccy0.get("baseExchangeRate") or 1.0) or 1.0
                _min_usd  = (_min_val * _lot_sz * _mid) / _fx_base
                _sim_min_notional[_sym] = round(_min_usd, 2)
                _sim_eligible.add(_sym)  # all queried; eligibility via formula at selection time
                print(f"[{_ts()}] \U0001f9ea SIM:   {_sym}: queried -- IG min notional ~${_min_usd:.2f}", flush=True)
                import time as _time; _time.sleep(0.3)
            # Persist eligibility now -- _sim is empty at this point so _sim_save_state()
            # would be a no-op. Do a targeted Redis patch instead.
            try:
                _cur_raw = _redis().get("june_sim_state")
                if _cur_raw:
                    _cur_st = json.loads(_cur_raw)
                    _cur_st["eligible_instruments"] = sorted(_sim_eligible)
                    _cur_st["min_notionals"] = _sim_min_notional
                    _redis().set("june_sim_state", json.dumps(_cur_st), ex=72 * 3600)
                    print(f"[{_ts()}] 🧪 SIM: Eligibility backfill saved to Redis ({len(_sim_eligible)} eligible)", flush=True)
            except Exception as _e:
                print(f"[{_ts()}] 🧪 SIM: Eligibility backfill save failed: {_e}", flush=True)

        now = time.time()

        # One-time fresh reset: wipe state if saved before _SIM_RESET_AFTER, preserve vol/move data
        if saved.get("sim_start_time", 0) < _SIM_RESET_AFTER:
            print(
                f"[{_ts()}] 🔄 SIM FRESH START: resetting for new session "
                f"(saved state predates {datetime.fromtimestamp(_SIM_RESET_AFTER, tz=timezone.utc).strftime('%Y-%m-%d')} cutoff) "
                f"— preserving vol_history, win_moves, loss_moves",
                flush=True,
            )
            _sim.update({
                "active":               True,
                "balance":              _SIM_START_BALANCE,
                "stage":                "sprout",
                "stage_entry_balance":  _SIM_START_BALANCE,
                "stage_trades":         0,
                "stage_wins":           0,
                "stage_losses":         0,
                "total_wins":           0,
                "total_losses":         0,
                "phase":                1,
                "phase_start_time":     now,
                "phase_entry_balance":  _SIM_START_BALANCE,
                "phase_consec_losses":  0,
                "sim_start_time":       now,
                "open_position":        None,
                "trade_history":        [],
                "stage_history":        [],
                "hourly_next":          now + 3600,
                "stopped":              False,
                "stop_reason":          "",
                "long_pnl":             0.0,
                "long_trades":          0,
                "long_wins":            0,
                "short_pnl":            0.0,
                "short_trades":         0,
                "short_wins":           0,
                "approach_stats":       {},
                "vol_stats":            {b: {"pnl": 0.0, "trades": 0, "wins": 0}
                                         for b in ("high", "mid", "low")},
                "reset_count":          0,
                "streak_state":         {},
                "boost_expiry":         {},
                "pause_expiry":         {},
                "last_entry_time":      now,
                "15m_reliability":      {},
                "vol_history":          saved.get("vol_history", {}),
                "win_moves":            saved.get("win_moves", {}),
                "loss_moves":           saved.get("loss_moves", {}),
                "failure_snapshot":     None,
                "failure_context_checked": True,
                "approach_skip_counts": {},
            })
            _sim_save_state()
            return

        _sim.update({
            "active":               True,
            "balance":              saved["balance"],
            "stage":                saved.get("stage", "sprout"),
            "stage_entry_balance":  saved.get("stage_entry_balance", _SIM_START_BALANCE),
            "stage_trades":         saved.get("stage_trades", 0),
            "stage_wins":           saved.get("stage_wins", 0),
            "stage_losses":         saved.get("stage_losses", 0),
            "total_wins":           saved.get("total_wins", 0),
            "total_losses":         saved.get("total_losses", 0),
            "phase":                saved.get("phase", 1),
            "phase_start_time":     saved.get("phase_start_time", now),
            "phase_entry_balance":  saved.get("phase_entry_balance", saved.get("stage_entry_balance", _SIM_START_BALANCE)),
            "phase_consec_losses":  saved.get("phase_consec_losses", 0),
            "sim_start_time":       saved.get("sim_start_time", now),
            "open_position":        saved.get("open_position"),
            "trade_history":        saved.get("trade_history", []),
            "stage_history":        saved.get("stage_history", []),
            "hourly_next":          now + 3600,
            "stopped":              False,
            "stop_reason":          "",
            "long_pnl":             saved.get("long_pnl", 0.0),
            "long_trades":          saved.get("long_trades", 0),
            "long_wins":            saved.get("long_wins", 0),
            "short_pnl":            saved.get("short_pnl", 0.0),
            "short_trades":         saved.get("short_trades", 0),
            "short_wins":           saved.get("short_wins", 0),
            "approach_stats":       {},
            "vol_stats":            saved.get("vol_stats",
                                              {b: {"pnl": 0.0, "trades": 0, "wins": 0}
                                               for b in ("high", "mid", "low")}),
            "reset_count":          saved.get("reset_count", 0),
            "vol_history":          saved.get("vol_history", {}),
            "streak_state":         saved.get("streak_state", {}),
            "boost_expiry":         saved.get("boost_expiry", {}),
            "pause_expiry":         saved.get("pause_expiry", {}),
            "last_entry_time":      saved.get("last_entry_time", 0.0),
            "15m_reliability":      saved.get("15m_reliability", {}),
            "win_moves":            saved.get("win_moves", {}),
            "loss_moves":           saved.get("loss_moves", {}),
            "failure_snapshot":     saved.get("failure_snapshot"),
            "failure_context_checked": saved.get("failure_context_checked", True),
            "approach_skip_counts": saved.get("approach_skip_counts", {}),
        })

        # Validate sim_start_time -- future timestamps mean state was reconstructed
        total_trades = _sim["total_wins"] + _sim["total_losses"]
        if _sim["sim_start_time"] > now:
            bad_ts = _sim["sim_start_time"]
            est_s  = total_trades * 8 * 60   # ~8 min average per completed trade
            _sim["sim_start_time"] = now - est_s
            print(
                f"[{_ts()}] \u26a0\ufe0f SIM: Bad sim_start_time detected "
                f"(future ts {bad_ts:.0f}) -- correcting elapsed to "
                f"~{est_s/3600:.1f}h ({total_trades} trades x 8 min avg)",
                flush=True,
            )

        # Validate open_position.entry_time -- also correct if in the future
        open_pos = _sim.get("open_position")
        if open_pos and open_pos.get("entry_time", 0) > now:
            open_pos["entry_time"] = now - 30 * 60  # treat as 30 min ago
            _sim["open_position"]  = open_pos
            print(
                f"[{_ts()}] \u26a0\ufe0f SIM: Bad open_position.entry_time (future) -- "
                f"treated as 30 min ago",
                flush=True,
            )

        elapsed_s = now - _sim["sim_start_time"]
        elapsed_h = elapsed_s / 3600.0

        balance = _sim["balance"]
        stage   = _sim["stage"].upper()
        print(
            f"[{_ts()}] \U0001f9ea SIM: Resuming from saved state -- "
            f"Balance ${balance:.2f} | Stage: {stage} | "
            f"Trades: {total_trades} | Elapsed: {elapsed_h:.1f}h",
            flush=True,
        )

        if _sim.get("open_position"):
            pos  = _sim["open_position"]
            held = (now - pos["entry_time"]) / 60.0
            print(
                f"[{_ts()}] \U0001f9ea SIM: Restoring open {pos['direction'].upper()} "
                f"{pos['instrument']} @ {pos['fill_price']:.6g} "
                f"(held {held:.0f} min) -- monitoring for exits",
                flush=True,
            )

        return

    # ── Fresh start ───────────────────────────────────────────────────────────
    print(f"[{_ts()}] \U0001f9ea SIM: Fresh start -- no saved state found", flush=True)
    print(
        f"[{_ts()}] \U0001f9ea SIM: Balance ${_SIM_START_BALANCE} | "
        f"Stop: +${_SIM_PROFIT_STOP} (doubled) or ${_SIM_LOSS_STOP} (60% loss)",
        flush=True,
    )
    print(f"[{_ts()}] \U0001f9ea SIM: Querying IG API for minimum deal sizes ...", flush=True)

    for sym, epic in list(INSTRUMENTS.items()):
        data = _ig_get(f"/markets/{epic}")
        if not data:
            print(f"[{_ts()}] \U0001f9ea SIM:   {sym}: API query failed -- excluded", flush=True)
            continue

        inst    = data.get("instrument", {})
        deal    = data.get("dealingRules", {})
        snap    = data.get("snapshot", {})
        min_val = float(deal.get("minDealSize", {}).get("value", 1.0))
        lot_sz  = float(inst.get("lotSize", 1.0))
        bid     = float(snap.get("bid") or 0)
        offer   = float(snap.get("offer") or 0)
        mid     = (bid + offer) / 2.0 if bid and offer else 0.0
        # Equity CFDs: IG returns null bid/offer — fetch price via Finnhub
        if mid == 0.0 and ".CASH." in epic:
            fh_sym = _INSTRUMENTS_REVERSE.get(epic)
            if fh_sym:
                fh = _finnhub_price(fh_sym)
                if fh:
                    mid = fh["mid"]
        ccy0     = inst.get("currencies", [{}])[0] if inst.get("currencies") else {}
        fx_base  = float(ccy0.get("baseExchangeRate") or 1.0) or 1.0
        min_usd  = (min_val * lot_sz * mid) / fx_base

        # Store ALL instruments; eligibility determined by formula at selection time
        _sim_eligible.add(sym)
        _sim_min_notional[sym] = round(min_usd, 2)
        print(f"[{_ts()}] \U0001f9ea SIM:   {sym}: queried -- IG min notional ~${min_usd:.2f}", flush=True)
        time.sleep(0.3)

    if not _sim_eligible:
        print(f"[{_ts()}] \U0001f9ea SIM: WARNING: no eligible instruments found", flush=True)

    now = time.time()
    _sim.update({
        "active":               True,
        "balance":              _SIM_START_BALANCE,
        "stage":                "sprout",
        "stage_entry_balance":  _SIM_START_BALANCE,
        "stage_trades":         0,
        "stage_wins":           0,
        "stage_losses":         0,
        "total_wins":           0,
        "total_losses":         0,
        "phase":                1,
        "phase_start_time":     now,
        "phase_entry_balance":  _SIM_START_BALANCE,
        "phase_consec_losses":  0,
        "sim_start_time":       now,
        "open_position":        None,
        "trade_history":        [],
        "stage_history":        [],
        "hourly_next":          now + 3600,
        "stopped":              False,
        "stop_reason":          "",
        "long_pnl":             0.0,
        "long_trades":          0,
        "long_wins":            0,
        "short_pnl":            0.0,
        "short_trades":         0,
        "short_wins":           0,
        "approach_stats":       {},
        "vol_stats":            {b: {"pnl": 0.0, "trades": 0, "wins": 0} for b in ("high", "mid", "low")},
        "reset_count":          0,
        "vol_history":          {},
        "streak_state":         {},
        "boost_expiry":         {},
        "pause_expiry":         {},
        "last_entry_time":      now,
        "15m_reliability":      {},
        "win_moves":            {},
        "loss_moves":           {},
        "failure_snapshot":     None,
        "failure_context_checked": True,
        "approach_skip_counts": {},
    })

    # Restore calibration data from previous session if available
    _cal_loaded = False
    try:
        _cal_raw = _redis().get("june_sim_calibration")
        if _cal_raw:
            _cal = json.loads(_cal_raw)
            _sim["vol_history"]  = _cal.get("vol_history", {})
            _sim["win_moves"]    = _cal.get("win_moves", {})
            _sim["loss_moves"]   = _cal.get("loss_moves", {})
            _snap_from_cal       = _cal.get("failure_snapshot")
            _sim["failure_snapshot"]        = _snap_from_cal
            _sim["failure_context_checked"] = _snap_from_cal is None
            _cal_loaded = True
            print(
                f"[{_ts()}] \U0001f9ea SIM: Restored calibration — "
                f"vol_history={list(_cal.get('vol_history', {}).keys())} "
                f"win_combos={list(_cal.get('win_moves', {}).keys())}",
                flush=True,
            )
    except Exception:
        pass

    print(f"[{_ts()}] \U0001f9ea SIM: Eligible instruments: {sorted(_sim_eligible)}", flush=True)
    _cal_note = "Calibration loaded from prior session." if _cal_loaded else "No prior calibration."
    print(
        f"[{_ts()}] 🔄 SIM: Fresh start — spread-aware stops, performance-based phases, "
        f"Silver prioritized. {_cal_note}",
        flush=True,
    )
    _sim_save_state()

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ── Startup checks ────────────────────────────────────────────────────────────
def startup_check() -> bool:
    required = ["IG_API_KEY", "IG_USERNAME", "IG_PASSWORD", "REDIS_HOST", "REDIS_PASSWORD"]
    missing  = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"❌ June cannot start — missing env vars: {', '.join(missing)}", flush=True)
        print(f"   Set them in /opt/bots/june.env and restart the service.", flush=True)
        return False
    return True


def redis_check() -> bool:
    try:
        r = _redis()
        r.ping()
        print(f"[{_ts()}] ✅ Redis connected ({REDIS_HOST}:{REDIS_PORT})", flush=True)
        return True
    except Exception as exc:
        print(f"[{_ts()}] ❌ Redis connection failed: {exc}", flush=True)
        return False


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 62, flush=True)
    print("  June — IG CFD Signal Publisher  beta v0.2", flush=True)
    print("  Observer only — no orders, no positions, no risk", flush=True)
    print("  Schedule: weekdays 24h/60s | weekends paused", flush=True)
    print("  Redis keys: june_signals · morning_baseline · premarket_gaps", flush=True)
    print("              spread_baselines · overnight_context", flush=True)
    print("=" * 62, flush=True)

    if not startup_check():
        sys.exit(1)
    if not redis_check():
        sys.exit(1)
    if not authenticate():
        print(f"[{_ts()}] ❌ IG authentication failed — cannot start", flush=True)
        sys.exit(1)
    authenticate_live()  # optional — logs ✅ or ℹ️; does not block startup

    verify_epics()
    _load_direct_cfd_cache()
    _startup_discovery_pass()   # Change 3: eager search all proactive bases (2s/symbol)

    # Start virtual trading simulation (runs in parallel with intelligence)
    sim_startup()

    if not INSTRUMENTS:
        print(f"[{_ts()}] ❌ No valid instruments after verification — exiting", flush=True)
        sys.exit(1)

    print(f"[{_ts()}] 🚀 Main loop starting — {POLL_ACTIVE}s weekday / paused weekends", flush=True)

    consec_errors = 0
    while True:
        try:
            # Weekend closure — pause entirely, check every 30 min
            if is_weekend_closure():
                print(
                    f"[{_ts()}] 💤 Weekend market closure — June pausing until Sunday 21:00 UTC",
                    flush=True,
                )
                time.sleep(30 * 60)
                continue

            # Daily flag reset at midnight UTC
            _maybe_reset_daily_flags()

            # Overnight / morning orchestration (no-ops outside their time windows)
            maybe_capture_ny_close()
            maybe_fetch_overnight_news()
            maybe_capture_premarket_baseline()
            maybe_publish_morning()

            # Poll and detect maintenance
            had_prices = poll_cycle()
            update_maintenance(had_prices)
            consec_errors = 0

            interval = POLL_MAINT if in_maintenance() else POLL_ACTIVE
            time.sleep(interval)

        except KeyboardInterrupt:
            print(f"\n[{_ts()}] June stopping (SIGINT/SIGTERM)", flush=True)
            sys.exit(0)
        except Exception as exc:
            consec_errors += 1
            print(f"[{_ts()}] ❌ Main loop error #{consec_errors}: {exc}", flush=True)
            if consec_errors >= 5:
                print(f"[{_ts()}] 💀 5 consecutive errors — sleeping 5 min before retry", flush=True)
                time.sleep(300)
                consec_errors = 0
            else:
                time.sleep(30)


if __name__ == "__main__":
    main()
