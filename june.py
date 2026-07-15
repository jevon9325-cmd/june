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
import os
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import requests
import redis as redis_lib

# ── Environment ─────────────────────────────────────────────────────────────
IG_BASE    = "https://demo-api.ig.com/gateway/deal"
IG_API_KEY = os.environ.get("IG_API_KEY", "")
IG_USER    = os.environ.get("IG_USERNAME", "")
IG_PASS    = os.environ.get("IG_PASSWORD", "")
IG_ACCOUNT = os.environ.get("IG_ACCOUNT_ID", "Z6CPCQ")

REDIS_HOST  = os.environ.get("REDIS_HOST", "")
REDIS_PORT  = int(os.environ.get("REDIS_PORT", 15074))
REDIS_PASS  = os.environ.get("REDIS_PASSWORD", "")

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
    "SPX500": "IX.D.SPTRD.IFD.IP",     # discovered: US 500 Cash ($250)
    "GER40":  "IX.D.DAX.IFD.IP",       # discovered: Germany 40 Cash (E25)
    "UK100":  "IX.D.FTSE.CFD.IP",      # verified demo
    "GOLD":   "CS.D.IN_GOLD.MFI.IP",   # discovered: Spot Gold
    "SILVER": "CS.D.CFDSILVER.CFM.IP", # discovered: Mini Spot Silver (500oz)
    "OIL":    "CC.D.LCO.USS.IP",       # verified demo
}

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
_sess: dict = {"cst": None, "token": None, "born": 0.0}

# Rolling price history: {sym: deque([(epoch, mid), ...])}
_history: dict = {sym: deque(maxlen=HISTORY_LEN) for sym in INSTRUMENTS}

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


# ── Redis ────────────────────────────────────────────────────────────────────
def _redis() -> redis_lib.Redis:
    return redis_lib.Redis(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


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


def fetch_price(epic: str) -> Optional[dict]:
    """Return bid/offer/mid/spread for an epic, or None on any error."""
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
    cfd  = [m for m in markets if "CFD" in m.get("instrumentType", "")]
    pool = cfd if cfd else markets
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
    change_5m  = ((mid - px5)  / px5  * 100.0) if px5  else 0.0
    change_15m = ((mid - px15) / px15 * 100.0) if px15 else 0.0
    if   change_5m >  0.05:
        direction = "bull"
    elif change_5m < -0.05:
        direction = "bear"
    else:
        direction = "neutral"
    return {
        "price":        round(mid, 6),
        "change_5m":    round(change_5m,  4),
        "change_15m":   round(change_15m, 4),
        "direction":    direction,
        "spread_pct":   round(spread_pct, 4),
        "spread_alert": spread_alert,
    }


# ── Poll cycle ────────────────────────────────────────────────────────────────

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

def poll_cycle() -> bool:
    """Fetch all instrument prices, update state, publish june_signals. Returns True if prices returned."""
    now          = time.time()
    signals      = {}
    alerts       = []
    spread_avgs  = {}
    current_mids = {}

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
        signals[sym] = sig
        if abs(sig["change_5m"]) >= MOMENTUM_PCT:
            alerts.append(sym)

    if not signals:
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
# Paper trades only — zero real orders, zero writes to intelligence Redis keys.
# Reads june_macro_regime + signals each cycle. Writes june_sim_results on stop.
# State is in-memory only; simulation resets if June restarts (acceptable).
# All output uses 🧪 SIM: prefix. Filter: journalctl -u june | grep "🧪 SIM"
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_SIM_START_BALANCE     = 50.0
_SIM_PROFIT_STOP       = 100.0      # stop if virtual P&L reaches +$100 (doubled)
_SIM_LOSS_STOP         = -30.0      # stop if virtual P&L reaches -$30 (60% loss)
_SIM_MAX_DURATION      = 24 * 3600  # hard stop at 24h

_SIM_SIZING_ORDER      = ["fixed_5", "fixed_10", "pct_5", "pct_10"]
_SIM_SIZING_CYCLE      = 2 * 3600   # rotate sizing approach every 2 hours

_SIM_ENTRY_VOL_MIN     = 0.15       # |change_5m| >= 0.15% required to enter
_SIM_STOP_PCT          = 0.02       # 2% adverse move → stop loss
_SIM_TP_PCT            = 0.03       # 3% favorable move → take profit
_SIM_MAX_HOLD_SECS     = 4 * 3600   # force-close after 4 hours

_SIM_CONSERVATIVE_LEV  = 3          # phase 1: first 12h
_SIM_AGGRESSIVE_LEV    = 10         # phase 2: after 12h
_SIM_PHASE_SWITCH_SECS = 12 * 3600

_SIM_HIGH_VOL_THRESH   = 0.50       # |change_5m| > 0.50% = high volatility entry
_SIM_LOW_VOL_THRESH    = 0.20       # |change_5m| < 0.20% = low volatility entry

# Approximate FX rates used only for minimum notional calculation at startup
_SIM_APPROX_GBPUSD = 1.34
_SIM_APPROX_EURUSD = 1.14
_SIM_APPROX_USDJPY = 162.0

# Populated by sim_startup() from IG API; not hardcoded
_sim_eligible:      set  = set()
_sim_min_notional:  dict = {}   # {sym: approx_usd_min_notional}

# All state in memory
_sim:     dict = {}   # simulation meta-state (empty = not yet started)
_sim_pos: dict = {}   # current open paper position (empty = no position)


def _sim_log(msg: str) -> None:
    print(f"[{_ts()}] \U0001f9ea SIM: {msg}", flush=True)


def _sim_reconstruct_prices(sym: str, signals: dict) -> dict:
    """Reconstruct approximate bid/ask from mid + spread_pct in the signals dict."""
    sig         = signals.get(sym, {})
    mid         = sig.get("price", 0.0)
    spread_pct  = sig.get("spread_pct", 0.0)   # spread as % of mid
    half_spread = mid * spread_pct / 200.0
    return {"bid": mid - half_spread, "ask": mid + half_spread, "mid": mid}


def _sim_position_size(balance: float, approach: str) -> float:
    """Return virtual position size ($) for the current sizing approach."""
    if approach == "fixed_5":  return 5.0
    if approach == "fixed_10": return 10.0
    if approach == "pct_5":    return round(balance * 0.05, 2)
    if approach == "pct_10":   return round(balance * 0.10, 2)
    return 5.0


def _sim_check_min_feasible(sym: str, pos_size: float, leverage: int) -> bool:
    """True if effective exposure (pos_size × leverage) meets IG minimum notional."""
    return (pos_size * leverage) >= _sim_min_notional.get(sym, 0.0)


def _sim_vol_bucket(vol: float) -> str:
    if vol > _SIM_HIGH_VOL_THRESH: return "high"
    if vol < _SIM_LOW_VOL_THRESH:  return "low"
    return "mid"


def _sim_select_instrument(signals: dict, regime: str):
    """Pick eligible instrument with highest |change_5m| whose direction fits regime."""
    best_sym = None
    best_vol = 0.0
    for sym in _sim_eligible:
        if sym not in signals:
            continue
        sig  = signals[sym]
        vol  = abs(sig["change_5m"])
        dirn = sig["direction"]
        if vol < _SIM_ENTRY_VOL_MIN:
            continue
        if regime == "bull"   and dirn not in ("bull",):
            continue
        if regime == "bear"   and dirn not in ("bear",):
            continue
        if regime in ("volatile", "neutral") and dirn == "neutral":
            continue
        if vol > best_vol:
            best_vol = vol
            best_sym = sym
    return best_sym


def _sim_compute_pnl_pct(prices: dict) -> float:
    """Current P&L % for the open position (negative = loss)."""
    pos   = _sim_pos
    entry = pos["fill_price"]
    dirn  = pos["direction"]
    if dirn == "long":
        return (prices["bid"] - entry) / entry
    else:
        return (entry - prices["ask"]) / entry


def _sim_close_position(prices: dict, exit_reason: str) -> None:
    """Close open paper position, record trade, update stats, log."""
    pos      = _sim_pos.copy()
    dirn     = pos["direction"]
    entry    = pos["fill_price"]
    size     = pos["size"]
    lev      = pos["leverage"]
    approach = pos["approach"]
    vol_bkt  = pos["vol_bucket"]
    entry_t  = pos["entry_time"]

    pnl_pct  = _sim_compute_pnl_pct(prices)
    hold_min = (time.time() - entry_t) / 60.0

    # Snap exit price to stop/TP if that was the trigger
    if dirn == "long":
        exit_price = prices["bid"]
        if exit_reason == "stop_loss":    exit_price = pos["stop_price"]
        elif exit_reason == "take_profit": exit_price = pos["tp_price"]
    else:
        exit_price = prices["ask"]
        if exit_reason == "stop_loss":    exit_price = pos["stop_price"]
        elif exit_reason == "take_profit": exit_price = pos["tp_price"]

    dollar_pnl = size * lev * pnl_pct
    won        = dollar_pnl > 0

    _sim["balance"] += dollar_pnl

    trade_rec = {
        "instrument":  pos["instrument"],
        "direction":   dirn,
        "entry_price": entry,
        "exit_price":  exit_price,
        "size":        size,
        "leverage":    lev,
        "pnl_pct":     round(pnl_pct, 6),
        "dollar_pnl":  round(dollar_pnl, 4),
        "hold_min":    round(hold_min, 1),
        "exit_reason": exit_reason,
        "approach":    approach,
        "vol_bucket":  vol_bkt,
        "entry_vol":   pos["entry_vol"],
    }
    _sim["trades"].append(trade_rec)

    if dirn == "long":
        _sim["long_pnl"]    += dollar_pnl
        _sim["long_trades"] += 1
        if won: _sim["long_wins"] += 1
    else:
        _sim["short_pnl"]    += dollar_pnl
        _sim["short_trades"] += 1
        if won: _sim["short_wins"] += 1

    st  = _sim["approach_stats"][approach]
    st["pnl"] += dollar_pnl; st["trades"] += 1
    if won: st["wins"] += 1

    vs  = _sim["vol_stats"][vol_bkt]
    vs["pnl"] += dollar_pnl; vs["trades"] += 1
    if won: vs["wins"] += 1

    total = len(_sim["trades"])
    wins  = sum(1 for t in _sim["trades"] if t["dollar_pnl"] > 0)
    sign  = "+" if dollar_pnl >= 0 else ""

    _sim_log(
        f"EXIT: {pos['instrument']} @ {exit_price:.6g} | "
        f"P&L {sign}{dollar_pnl:.2f} ({pnl_pct:+.2%}) | "
        f"hold {hold_min:.0f}min | exit: {exit_reason}"
    )
    _sim_log(
        f"BALANCE: ${_sim['balance']:.2f} | trades: {wins}W/{total - wins}L | "
        f"long P&L: {_sim['long_pnl']:+.2f} | short P&L: {_sim['short_pnl']:+.2f}"
    )
    _sim_pos.clear()


def _sim_try_entry(signals: dict, regime: str, leverage: int, approach: str) -> None:
    """Try to open a simulated paper position if conditions are met."""
    balance = _sim["balance"]
    sym     = _sim_select_instrument(signals, regime)
    if not sym:
        return

    sig       = signals[sym]
    prices    = _sim_reconstruct_prices(sym, signals)
    vol       = abs(sig["change_5m"])
    direction = "long" if sig["change_5m"] > 0 else "short"
    # Try current sizing approach; if effective notional is too small, escalate.
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
        _sim_log(
            f"Skip {sym}: no sizing approach meets IG min notional ${min_n:.2f} "
            f"(max effective ${max_eff:.2f} with {_SIM_SIZING_ORDER[-1]})"
        )
        return

    if chosen_approach != approach:
        _sim_log(f"Size up: {approach} -> {chosen_approach} to meet {sym} min notional")
    approach = chosen_approach

    fill = prices["ask"] if direction == "long" else prices["bid"]
    if fill <= 0:
        return

    stop_px = fill * (1 - _SIM_STOP_PCT) if direction == "long" else fill * (1 + _SIM_STOP_PCT)
    tp_px   = fill * (1 + _SIM_TP_PCT)   if direction == "long" else fill * (1 - _SIM_TP_PCT)
    vbkt    = _sim_vol_bucket(vol)

    _sim_pos.update({
        "instrument":  sym,
        "direction":   direction,
        "fill_price":  fill,
        "stop_price":  stop_px,
        "tp_price":    tp_px,
        "size":        pos_size,
        "leverage":    leverage,
        "entry_time":  time.time(),
        "entry_vol":   vol,
        "vol_bucket":  vbkt,
        "approach":    approach,
        "regime":      regime,
    })

    action = "BUY" if direction == "long" else "SELL"
    _sim_log(
        f"{action}: {sym} @ {fill:.6g} | size ${pos_size:.2f} | "
        f"leverage {leverage}:1 | volatility {vol:.2f}% | approach: {approach}"
    )


def _sim_check_exit(signals: dict, regime: str) -> None:
    """Check all exit conditions for the open paper position."""
    if not _sim_pos:
        return
    pos     = _sim_pos
    sym     = pos["instrument"]
    if sym not in signals:
        return

    prices   = _sim_reconstruct_prices(sym, signals)
    pnl_pct  = _sim_compute_pnl_pct(prices)
    hold_sec = time.time() - pos["entry_time"]
    sig_dir  = signals[sym]["direction"]
    dirn     = pos["direction"]

    if pnl_pct <= -_SIM_STOP_PCT:
        _sim_close_position(prices, "stop_loss")
        return
    if pnl_pct >= _SIM_TP_PCT:
        _sim_close_position(prices, "take_profit")
        return
    if hold_sec >= _SIM_MAX_HOLD_SECS:
        _sim_close_position(prices, "max_hold")
        return
    if dirn == "long"  and (regime == "bear" or sig_dir == "bear"):
        _sim_close_position(prices, "reversal")
        return
    if dirn == "short" and (regime == "bull" or sig_dir == "bull"):
        _sim_close_position(prices, "reversal")
        return


def _sim_hourly_log() -> None:
    """Print hourly simulation summary."""
    trades = _sim.get("trades", [])
    total  = len(trades)
    wins   = sum(1 for t in trades if t["dollar_pnl"] > 0)

    best_app = max(
        _sim["approach_stats"].items(), key=lambda x: x[1]["pnl"],
        default=("none", {"pnl": 0})
    )[0]

    lt, lw = _sim["long_trades"],  _sim["long_wins"]
    st, sw = _sim["short_trades"], _sim["short_wins"]
    lr = f"{lw}/{lt} ({lw/lt*100:.0f}%)" if lt else "0/0"
    sr = f"{sw}/{st} ({sw/st*100:.0f}%)" if st else "0/0"

    def wr(k):
        v = _sim["vol_stats"].get(k, {})
        return v["wins"] / max(v["trades"], 1)
    best_vol = max(["high", "mid", "low"], key=wr)

    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    _sim_log(
        f"SUMMARY [{now_str}]: Balance ${_sim['balance']:.2f} | "
        f"trades {wins}W/{total - wins}L | best approach: {best_app} | "
        f"long {lr} | short {sr} | best vol: {best_vol}"
    )


def _sim_stop(reason: str, signals=None) -> None:
    """Force-close any open position, generate final report, publish to Redis."""
    if _sim_pos and signals:
        sym = _sim_pos.get("instrument", "")
        if sym and sym in signals:
            _sim_close_position(_sim_reconstruct_prices(sym, signals), "simulation_stop")

    _sim["stopped"]     = True
    _sim["stop_reason"] = reason

    balance = _sim["balance"]
    pnl     = balance - _SIM_START_BALANCE
    pnl_pct = pnl / _SIM_START_BALANCE * 100.0
    trades  = _sim["trades"]
    total   = len(trades)
    wins    = sum(1 for t in trades if t["dollar_pnl"] > 0)

    app_stats = _sim["approach_stats"]
    best_app  = max(app_stats.items(), key=lambda x: x[1]["pnl"]) if any(
        v["trades"] for v in app_stats.values()
    ) else ("no_trades", {"pnl": 0})

    lt, lw = _sim["long_trades"],  _sim["long_wins"]
    st, sw = _sim["short_trades"], _sim["short_wins"]

    vs = _sim["vol_stats"]
    def wr(k):
        v = vs.get(k, {})
        return v["wins"] / max(v["trades"], 1)
    best_vol = max(["high", "mid", "low"], key=wr)

    instr_pnl = {}
    for t in trades:
        instr_pnl[t["instrument"]] = instr_pnl.get(t["instrument"], 0.0) + t["dollar_pnl"]

    lev_pnl = {}
    for t in trades:
        lev_pnl[t["leverage"]] = lev_pnl.get(t["leverage"], 0.0) + t["dollar_pnl"]

    best_instr  = max(instr_pnl.items(), key=lambda x: x[1]) if instr_pnl else ("none", 0)
    worst_instr = min(instr_pnl.items(), key=lambda x: x[1]) if instr_pnl else ("none", 0)
    best_lev    = max(lev_pnl.items(),   key=lambda x: x[1]) if lev_pnl  else (3, 0)

    if total:
        rec = (
            f"Best sizing: {best_app[0]} (P&L ${best_app[1]['pnl']:+.2f}). "
            f"Longs: ${_sim['long_pnl']:+.2f} ({lw}/{lt} trades). "
            f"Shorts: ${_sim['short_pnl']:+.2f} ({sw}/{st} trades) — "
            f"{'net positive' if _sim['short_pnl'] > 0 else 'net negative'}. "
            f"Best vol entry: {best_vol} (|change_5m| "
            f"{'>' + str(_SIM_HIGH_VOL_THRESH) if best_vol == 'high' else '<' + str(_SIM_LOW_VOL_THRESH) if best_vol == 'low' else str(_SIM_LOW_VOL_THRESH) + '-' + str(_SIM_HIGH_VOL_THRESH)}%). "
            f"Best instrument: {best_instr[0]} (${best_instr[1]:+.2f}). "
            f"Worst: {worst_instr[0]} (${worst_instr[1]:+.2f}). "
            f"Recommended leverage: {best_lev[0]}:1."
        )
    else:
        rec = "No trades completed — insufficient data for recommendations."

    _sim_log(f"STOPPED: Balance ${balance:.2f} ({pnl:+.2f} / {pnl_pct:+.1f}%) | Reason: {reason}")
    _sim_log(f"RECOMMENDATION: {rec}")

    result_payload = {
        "timestamp":            int(time.time()),
        "stop_reason":          reason,
        "final_balance":        round(balance, 4),
        "pnl_usd":              round(pnl, 4),
        "pnl_pct":              round(pnl_pct, 2),
        "total_trades":         total,
        "win_rate_pct":         round(wins / max(total, 1) * 100, 1),
        "long_pnl":             round(_sim["long_pnl"], 4),
        "short_pnl":            round(_sim["short_pnl"], 4),
        "approach_stats":       {k: {kk: round(vv, 4) if isinstance(vv, float) else vv
                                     for kk, vv in v.items()}
                                 for k, v in app_stats.items()},
        "vol_stats":            {k: {kk: round(vv, 4) if isinstance(vv, float) else vv
                                     for kk, vv in v.items()}
                                 for k, v in vs.items()},
        "instrument_pnl":       {k: round(v, 4) for k, v in instr_pnl.items()},
        "leverage_pnl":         {str(k): round(v, 4) for k, v in lev_pnl.items()},
        "recommendation":       rec,
        "eligible_instruments": sorted(_sim_eligible),
        "trades":               trades,
    }

    try:
        r = _redis()
        r.set("june_sim_results", json.dumps(result_payload), ex=48 * 3600)
        _sim_log("Results published → Redis key june_sim_results (TTL 48h)")
    except Exception as exc:
        _sim_log(f"Redis write failed (results lost): {exc}")


def run_simulation_step(signals: dict) -> None:
    """Called from poll_cycle() after intelligence functions. Paper trades only."""
    if not _sim or _sim.get("stopped"):
        return

    now     = time.time()
    elapsed = now - _sim["start_time"]

    # Phase transition at 12h (runs through overnight so timer stays accurate)
    if _sim["phase"] == "conservative" and elapsed >= _SIM_PHASE_SWITCH_SECS:
        _sim["phase"] = "aggressive"
        _sim_log("Phase switch -> AGGRESSIVE (10:1 leverage)")

    # Rotate sizing approach every 2h (runs through overnight)
    if now - _sim["sizing_start"] >= _SIM_SIZING_CYCLE:
        _sim["sizing_idx"]   = (_sim["sizing_idx"] + 1) % len(_SIM_SIZING_ORDER)
        _sim["sizing_start"] = now
        _sim_log(f"Sizing approach -> {_SIM_SIZING_ORDER[_sim['sizing_idx']]}")

    # Duration stop (runs through overnight so 24h wall-clock is respected)
    if elapsed >= _SIM_MAX_DURATION:
        _sim_stop("24h duration", signals)
        return

    # P&L boundary stops (runs through overnight)
    pnl = _sim["balance"] - _SIM_START_BALANCE
    if pnl >= _SIM_PROFIT_STOP:
        _sim_stop(f"profit boundary (+${pnl:.2f})", signals)
        return
    if pnl <= _SIM_LOSS_STOP:
        _sim_stop(f"loss boundary (${pnl:.2f})", signals)
        return

    # Hourly log (fires even during overnight so the sim stays visible)
    if now >= _sim["hourly_next"]:
        _sim_hourly_log()
        _sim["hourly_next"] = now + 3600

    # Pause entries and exits while both sessions are closed
    if is_overnight():
        return

    # Read current macro regime
    regime = "neutral"
    try:
        r   = _redis()
        raw = r.get("june_macro_regime")
        if raw:
            regime = json.loads(raw).get("regime", "neutral")
    except Exception:
        pass

    leverage = _SIM_CONSERVATIVE_LEV if _sim["phase"] == "conservative" else _SIM_AGGRESSIVE_LEV
    approach = _SIM_SIZING_ORDER[_sim["sizing_idx"]]

    # Handle open position (check exit first — max 1 concurrent position)
    if _sim_pos:
        _sim_check_exit(signals, regime)
        return

    # No position — look for entry signal (skip neutral regime)
    _sim_try_entry(signals, regime, leverage, approach)


def sim_startup() -> None:
    """Query IG API for minimum deal sizes, determine eligible instruments, init state."""
    global _sim_eligible, _sim_min_notional

    _sim_log("Initializing virtual trading simulation...")
    _sim_log(
        f"Balance ${_SIM_START_BALANCE} | "
        f"Stop: +${_SIM_PROFIT_STOP} (doubled) or ${_SIM_LOSS_STOP} (60% loss) | "
        f"Duration: 24h"
    )
    _sim_log("Querying IG API for minimum deal sizes (determining eligible instruments)...")

    for sym, epic in list(INSTRUMENTS.items()):
        data = _ig_get(f"/markets/{epic}")
        if not data:
            _sim_log(f"  {sym}: API query failed — excluded")
            continue

        inst    = data.get("instrument", {})
        deal    = data.get("dealingRules", {})
        snap    = data.get("snapshot", {})
        min_val = float(deal.get("minDealSize", {}).get("value", 1.0))
        lot_sz  = float(inst.get("lotSize", 1.0))
        bid     = float(snap.get("bid") or 0)
        offer   = float(snap.get("offer") or 0)
        mid     = (bid + offer) / 2.0 if bid and offer else 0.0
        ccy0    = inst.get("currencies", [{}])[0] if inst.get("currencies") else {}
        ccy     = ccy0.get("code", "$.")

        # Estimate minimum notional in USD
        # Rule: min_val contracts × lot_sz units × mid_price_in_native_ccy → convert to USD
        if sym == "EURUSD" and mid > 1000:
            # IG quotes EURUSD ×10000 (e.g. 11441 = 1.1441 USD/EUR)
            actual_rate = mid / 10000.0
            min_usd     = min_val * lot_sz * actual_rate
        elif sym == "USDJPY":
            # Exposure is JPY (lot_sz × contracts); divide by USDJPY to get USD
            jpy_rate = mid if mid > 50 else _SIM_APPROX_USDJPY
            min_usd  = (min_val * lot_sz) / jpy_rate
        elif "£" in ccy or "#" in ccy or sym in ("OIL", "UK100"):
            # GBP-denominated (mid may be in pence: divide by 100 first)
            pence_scale = 100.0 if mid > 1000 else 1.0
            min_usd     = (min_val * lot_sz * mid / pence_scale) * _SIM_APPROX_GBPUSD
        elif "E" in ccy and "E." in ccy or sym == "GER40":
            min_usd = min_val * lot_sz * mid * _SIM_APPROX_EURUSD
        else:
            # USD-denominated (GOLD, SILVER, SPX500, GBPUSD, etc.)
            min_usd = min_val * lot_sz * mid

        _sim_min_notional[sym] = round(min_usd, 2)

        # Eligible if effective $10 position at max leverage (10:1) covers minimum
        max_effective = 10.0 * _SIM_AGGRESSIVE_LEV  # $100
        if min_usd <= max_effective:
            _sim_eligible.add(sym)
            _sim_log(f"  {sym}: ELIGIBLE  — IG min notional ~${min_usd:.2f}")
        else:
            _sim_log(f"  {sym}: EXCLUDED  — IG min notional ~${min_usd:.2f} exceeds ${max_effective:.0f} effective max")

        time.sleep(0.3)

    if not _sim_eligible:
        _sim_log("WARNING: No eligible instruments — simulation will run but cannot trade")

    now = time.time()
    _sim.update({
        "active":         True,
        "balance":        _SIM_START_BALANCE,
        "start_time":     now,
        "phase":          "conservative",
        "sizing_idx":     0,
        "sizing_start":   now,
        "trades":         [],
        "long_pnl":       0.0,
        "short_pnl":      0.0,
        "long_trades":    0,
        "long_wins":      0,
        "short_trades":   0,
        "short_wins":     0,
        "hourly_next":    now + 3600,
        "stopped":        False,
        "stop_reason":    "",
        "approach_stats": {k: {"pnl": 0.0, "trades": 0, "wins": 0} for k in _SIM_SIZING_ORDER},
        "vol_stats":      {b: {"pnl": 0.0, "trades": 0, "wins": 0} for b in ("high", "mid", "low")},
    })
    _sim_pos.clear()

    _sim_log(f"Eligible instruments: {sorted(_sim_eligible)}")
    _sim_log("Phase 1 (conservative, 3:1 leverage) — running alongside normal intelligence")


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

    verify_epics()

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
