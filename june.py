#!/usr/bin/env python3
"""
June — IG CFD Signal Publisher (beta v0.1)
Observer-only: polls IG demo API, publishes real-time momentum signals to Redis.

No orders. No positions. No risk management.
June watches macro instruments and reports. Others decide.

Redis output key: june_signals (TTL: 120s)
Schema:
  {
    "timestamp": int,          # Unix epoch
    "signals": {
      "EURUSD": {
        "price":      float,   # mid-price
        "change_5m":  float,   # % change vs 5 min ago
        "change_15m": float,   # % change vs 15 min ago
        "direction":  "bull"|"bear"|"neutral",
        "spread_pct": float,   # (ask-bid)/mid * 100
      }, ...
    },
    "momentum_alerts": [str, ...]  # symbols where |change_5m| > MOMENTUM_PCT
  }

Future Miss Secretary integration point (alert_system.py):
  In select_best_trade() or score_candidate(), call:
    june_raw = redis.get("june_signals")
    if june_raw:
        june = json.loads(june_raw)
        if "GOLD" in june["momentum_alerts"]:
            score *= 1.15  # boost GDX3, GDX, NEM, AG, PAAS, etc.
        if "SPX500" in june["momentum_alerts"] and direction == "bull":
            score *= 1.10  # broad market tailwind
  Only wire this in after 2+ weeks of beta signal quality validation.
"""

import json
import math
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

# ── Timing ──────────────────────────────────────────────────────────────────
POLL_ACTIVE  = int(os.environ.get("JUNE_POLL_ACTIVE", 60))    # seconds — London/NY sessions
POLL_QUIET   = int(os.environ.get("JUNE_POLL_QUIET",  600))   # 10 min — outside sessions
SIGNAL_TTL   = 120    # Redis key TTL in seconds — stale data is worse than no data
SESSION_MAX  = 6 * 3600 - 1800  # re-auth 30 min before 6-hour IG token expiry
HISTORY_LEN  = 20    # rolling price readings per instrument (~20 min at 60s)

# ── Signal threshold ────────────────────────────────────────────────────────
MOMENTUM_PCT = 0.30   # |change_5m| % to flag as momentum alert

# ── IG Epic codes ───────────────────────────────────────────────────────────
# Epic format reference:
#   Forex:       CS.D.{PAIR}.CFD.IP
#   Indices:     IX.D.{INDEX}.{TYPE}.IP
#   Commodities: CC.D.{COM}.{TYPE}.IP  or  CS.D.CFE{COM}.CFE.IP
#
# verify_epics() runs on startup and calls discover_epic() to fix any that
# return 404 or error. Correct epics are logged for future reference.
INSTRUMENTS: dict = {
    "EURUSD": "CS.D.EURUSD.CFD.IP",    # verified demo
    "GBPUSD": "CS.D.GBPUSD.CFD.IP",    # verified demo
    "USDJPY": "CS.D.USDJPY.CFD.IP",    # verified demo
    "SPX500": "IX.D.SPTRD.IFD.IP",     # discovered: US 500 Cash (50)
    "GER40":  "IX.D.DAX.IFD.IP",       # discovered: Germany 40 Cash (E25)
    "UK100":  "IX.D.FTSE.CFD.IP",      # verified demo
    "GOLD":   "CS.D.IN_GOLD.MFI.IP",   # discovered: Spot Gold
    "SILVER": "CS.D.CFDSILVER.CFM.IP", # discovered: Mini Spot Silver (500oz)
    "OIL":    "CC.D.LCO.USS.IP",       # verified demo
}

# Fallback search terms if an epic is rejected — passed to /markets?searchTerm=
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

# ── Session state (module-level, refreshed in-place) ────────────────────────
_sess: dict = {"cst": None, "token": None, "born": 0.0}

# ── Rolling price history: {symbol: deque([(epoch, mid), ...])} ─────────────
_history: dict = {sym: deque(maxlen=HISTORY_LEN) for sym in INSTRUMENTS}


# ── Redis ───────────────────────────────────────────────────────────────────
def _redis() -> redis_lib.Redis:
    return redis_lib.Redis(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )


# ── IG session management ───────────────────────────────────────────────────
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
    """Re-authenticate if we have a session and it's within 30 min of the 6h expiry."""
    if not _sess.get("cst"):
        return
    if time.time() - _sess["born"] > SESSION_MAX:
        print(f"[{_ts()}] 🔄 Session approaching 6h expiry — refreshing tokens", flush=True)
        authenticate()


# ── IG API calls ────────────────────────────────────────────────────────────
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
    snap = data.get("snapshot", {})
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
    # Prefer CFD instruments and exact matches
    cfd = [m for m in markets if "CFD" in m.get("instrumentType", "")]
    pool = cfd if cfd else markets
    best = pool[0]
    epic = best.get("epic", "")
    name = best.get("instrumentName", "?")
    print(f"[{_ts()}]   🔍 '{term}' → {epic} ({name})", flush=True)
    return epic if epic else None


# ── Startup epic verification ───────────────────────────────────────────────
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
        time.sleep(1.5)   # gentle during startup verification

    for sym in failed:
        print(f"[{_ts()}]   🔍 Attempting discovery for {sym}...", flush=True)
        new_epic = discover_epic(sym)
        if new_epic:
            INSTRUMENTS[sym] = new_epic
            # Ensure history buffer exists for new sym
            if sym not in _history:
                _history[sym] = deque(maxlen=HISTORY_LEN)
            print(f"[{_ts()}]   🔁 {sym} epic updated → {new_epic}", flush=True)
        else:
            print(f"[{_ts()}]   ⚠️  {sym} could not be verified — dropping from this session", flush=True)
            INSTRUMENTS.pop(sym, None)
            _history.pop(sym, None)

    print(f"[{_ts()}] ✅ Epic verification complete — {len(INSTRUMENTS)} active instruments", flush=True)


# ── Signal computation ──────────────────────────────────────────────────────
def _price_n_minutes_ago(sym: str, minutes: float) -> Optional[float]:
    """Find the price reading closest to N minutes ago in the rolling buffer."""
    hist = _history.get(sym)
    if not hist:
        return None
    target = time.time() - minutes * 60.0
    # Keep only readings that are old enough (at or before the target time)
    candidates = [(abs(ep - target), px) for ep, px in hist if ep <= target]
    if not candidates:
        return None
    return min(candidates, key=lambda x: x[0])[1]


def compute_signal(sym: str, price: dict) -> dict:
    mid       = price["mid"]
    spread_pct = (price["spread"] / mid * 100.0) if mid > 0 else 0.0

    px5  = _price_n_minutes_ago(sym, 5)
    px15 = _price_n_minutes_ago(sym, 15)

    change_5m  = ((mid - px5)  / px5  * 100.0) if px5  else 0.0
    change_15m = ((mid - px15) / px15 * 100.0) if px15 else 0.0

    if   change_5m >  0.05:
        direction = "bull"
    elif change_5m < -0.05:
        direction = "bear"
    else:
        direction = "neutral"

    return {
        "price":      round(mid, 6),
        "change_5m":  round(change_5m,  4),
        "change_15m": round(change_15m, 4),
        "direction":  direction,
        "spread_pct": round(spread_pct, 4),
    }


# ── Poll cycle ──────────────────────────────────────────────────────────────
def poll_cycle():
    now     = time.time()
    signals = {}
    alerts  = []

    for sym, epic in list(INSTRUMENTS.items()):
        price = fetch_price(epic)
        if price is None:
            continue
        _history[sym].append((now, price["mid"]))
        sig = compute_signal(sym, price)
        signals[sym] = sig
        if abs(sig["change_5m"]) >= MOMENTUM_PCT:
            alerts.append(sym)

    if not signals:
        print(f"[{_ts()}] ⚠️  Poll cycle: no prices returned", flush=True)
        return

    payload = {
        "timestamp":       int(now),
        "signals":         signals,
        "momentum_alerts": alerts,
    }

    try:
        r = _redis()
        r.set("june_signals", json.dumps(payload), ex=SIGNAL_TTL)
    except Exception as exc:
        print(f"[{_ts()}] ❌ Redis write error: {exc}", flush=True)
        return

    alert_tag = f"  🚨 ALERTS → {alerts}" if alerts else ""
    price_row = " | ".join(
        f"{s}={v['price']:.4f}({v['change_5m']:+.3f}%)" for s, v in signals.items()
    )
    print(f"[{_ts()}] 📡 {len(signals)} signals published (TTL {SIGNAL_TTL}s){alert_tag}", flush=True)
    print(f"[{_ts()}]    {price_row}", flush=True)


# ── Market session awareness ────────────────────────────────────────────────
def is_active() -> bool:
    """True during London (07:00–16:00 UTC) or NY (13:30–21:00 UTC) sessions."""
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:       # Saturday=5, Sunday=6
        return False
    mins = now.hour * 60 + now.minute
    london = (7 * 60) <= mins < (16 * 60)
    ny     = (13 * 60 + 30) <= mins < (21 * 60)
    return london or ny


def session_label() -> str:
    now  = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return "WEEKEND"
    mins = now.hour * 60 + now.minute
    if (7 * 60) <= mins < (16 * 60) and (13 * 60 + 30) <= mins < (21 * 60):
        return "LONDON+NY"
    if (7 * 60) <= mins < (16 * 60):
        return "LONDON"
    if (13 * 60 + 30) <= mins < (21 * 60):
        return "NY"
    return "OVERNIGHT"


# ── Startup checks ──────────────────────────────────────────────────────────
def startup_check() -> bool:
    required = ["IG_API_KEY", "IG_USERNAME", "IG_PASSWORD",
                "REDIS_HOST", "REDIS_PASSWORD"]
    missing = [v for v in required if not os.environ.get(v)]
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


# ── Utility ─────────────────────────────────────────────────────────────────
def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 62, flush=True)
    print("  June — IG CFD Signal Publisher  beta v0.1", flush=True)
    print("  Observer only — no orders, no positions, no risk", flush=True)
    print("  Redis key: june_signals (TTL 120s)", flush=True)
    print("=" * 62, flush=True)

    if not startup_check():
        sys.exit(1)

    if not redis_check():
        sys.exit(1)

    if not authenticate():
        print(f"[{_ts()}] ❌ IG authentication failed — cannot start", flush=True)
        sys.exit(1)

    verify_epics()

    if not INSTRUMENTS:
        print(f"[{_ts()}] ❌ No valid instruments after verification — exiting", flush=True)
        sys.exit(1)

    print(f"[{_ts()}] 🚀 Entering main loop — {POLL_ACTIVE}s active / {POLL_QUIET}s quiet", flush=True)

    consecutive_errors = 0
    while True:
        try:
            active   = is_active()
            interval = POLL_ACTIVE if active else POLL_QUIET
            label    = session_label()

            if not active:
                print(f"[{_ts()}] 🌙 {label} — quiet polling ({POLL_QUIET}s interval)", flush=True)

            poll_cycle()
            consecutive_errors = 0
            time.sleep(interval)

        except KeyboardInterrupt:
            print(f"\n[{_ts()}] June stopping (SIGINT/SIGTERM)", flush=True)
            sys.exit(0)
        except Exception as exc:
            consecutive_errors += 1
            print(f"[{_ts()}] ❌ Main loop error #{consecutive_errors}: {exc}", flush=True)
            if consecutive_errors >= 5:
                print(f"[{_ts()}] 💀 5 consecutive errors — sleeping 5 min before retry", flush=True)
                time.sleep(300)
                consecutive_errors = 0
            else:
                time.sleep(30)


if __name__ == "__main__":
    main()
