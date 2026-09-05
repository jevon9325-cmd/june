#!/usr/bin/env python3
"""
June — IG CFD Signal Publisher (beta v0.2)
Live CFD trading + signal intelligence bot. Scores momentum, sizes positions,
and executes on the IG live account when june_live_enabled=true in Redis.
Also runs a parallel simulation layer and publishes signals to Redis.

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
from zoneinfo import ZoneInfo
from typing import Optional

import threading
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
_HISTORY_REDIS_KEY       = "june_price_history"  # persisted rolling price deques
_HISTORY_REDIS_TTL       = HISTORY_LEN * 60 * 3  # 60 min TTL — 3x the window
_HISTORY_STALE_CUTOFF    = 300  # discard saved readings older than 5 min (5 missed cycles)

# ── Weekend / session windows (all in UTC minutes-since-midnight) ────────────
# IG CFD closure: Friday 21:15 UTC → Sunday 21:00 UTC
WEEKEND_CLOSE_MIN = 21 * 60 + 15   # Friday 21:15 UTC — IG CFD close
WEEKEND_OPEN_MIN  = 21 * 60        # Sunday 21:00 UTC — superseded; kept for reference only

# DST-aware UK timezone for is_weekend_closure() — replaces hardcoded UTC constants above.
# IG forex-specific hours: open Sunday 21:00 UK local, close Friday 22:00 UK local.
# Europe/London handles BST (UTC+1 summer) and GMT (UTC+0 winter) automatically.
_UK_TZ = ZoneInfo("Europe/London")
# DST-aware US timezone for metals (COMEX/CME) weekend gate — see _is_metals_weekend_closure().
_US_EAST_TZ = ZoneInfo("America/New_York")

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
SPREAD_ALERT_FACTOR  = 3.0    # current spread > 3× avg → spread_alert: True
SPREAD_ATR_THRESHOLD = 1.00   # spread > 100% of 14-period ATR → rank/size penalty (breakeven: spread=ATR). Calibrated at minDeal floor ($49 bal, $2.78 SILVER notional, $0.0016/trade spread cost = 0.008% of $20 CFD allocation). Re-derive when computed position size exceeds minDeal floor (~$280 account for SILVER ig_size>0.04).
ATR_PERIOD           = 14     # periods for ATR from rolling mid-price history
# Hybrid tiered Spread/ATR thresholds — per asset class, used by entry gate with 5m ATR
_SPREAD_ATR_TIERS:        dict  = {"FX": 0.35, "METAL": 0.60, "ENERGY": 0.85, "CRYPTO": 1.50}  # CRYPTO tier confirmed for BTC (ratio 0.531) and ETH (ratio 0.598) at worst-case Asian session; XRP/SOL pending measurement
_SPREAD_ATR_ASSET_CLASS:  dict  = {"GOLD": "METAL", "SILVER": "METAL", "OIL": "ENERGY", "NATGAS": "ENERGY", "WHEAT": "METAL", "COCOA": "METAL", "LWB": "METAL", "SUGAR": "METAL", "HO": "ENERGY", "BTC": "CRYPTO", "ETH": "CRYPTO"}
_SPREAD_ATR_FALLBACK_BUMP: float = 0.15  # added to tier threshold when using 1m-ATR fallback
SPREAD_MIN_READINGS = 5      # minimum readings before anomaly detection active

PREMARKET_GAP_PCT   = 0.50   # |change vs 06:00 baseline| to flag pre-London gap
DIRECT_CFD_GAP_PCT  = 1.5    # daily % change to flag individual stock as gap candidate
DIRECT_CFD_SEARCH_INTERVAL = 30        # seconds between IG search API calls
DIRECT_CFD_MISS_TTL        = 6 * 3600  # 6h before re-searching a not-found symbol
DIRECT_CFD_CONFIRMED_MISS_TTL = 24 * 3600  # 24h after all search strategies exhausted

# Minimum notional values for core instruments — SIM bootstrap (live API overwrites at startup).
# FX values use CORRECT formula (lot_sz/pip_sz)×minDeal×mid; confirmed 2026-09-04.
_KNOWN_MIN_NOTIONALS: dict = {
    # FX formula: (lot_sz/pip_sz) × minDeal × mid  [NOT lot_sz × minDeal × mid — that was 10,000x wrong]
    # unit_val = lot_sz/pip_sz = 10/0.0001 = 100,000 base-currency units per lot for 4-decimal pairs
    # Values pegged to ~2026-09-04 prices; live API overwrites at startup via _live_fetch_market_data.
    # *minDeal unconfirmed on live — assumed 0.04 (same as EURUSD/GBPUSD; demo returned wrong 1.0)
    "EURUSD": 4648.0,   # (10/0.0001)×0.04×1.162 = $4,648  | minDeal=0.04 live-confirmed
    "GBPUSD": 5409.0,   # (10/0.0001)×0.04×1.352 = $5,409  | minDeal=0.04 live-confirmed
    "USDJPY": 20000.0,  # (1000/0.01)×0.2×1.0 = $20,000    | base=USD; minDeal=0.2 live-confirmed
    "AUDUSD": 2883.0,   # (10/0.0001)×0.04×0.721 = $2,883*  | base=AUD; minDeal=0.04 assumed*
    "USDCAD": 10000.0,  # (10/0.0001)×0.1×1.0 = $10,000    | base=USD; minDeal=0.1 demo-confirmed
    "EURGBP": 4648.0,   # (10/0.0001)×0.04×EURUSD = $4,648* | base=EUR; minDeal=0.04 assumed*
    "NZDUSD": 5889.0,   # (10/0.0001)×0.1×0.589 = $5,889   | minDeal=0.1 demo-confirmed
    "USDCHF": 4000.0,   # (10/0.0001)×0.04×1.0 = $4,000*   | base=USD; minDeal=0.04 assumed*
    "SILVER":  0.05,  # eligibility floor: bypasses 20% concentration cap; actual IG min ~$2.79 (minDeal×lot×spot×pu)
    "OIL":     0.04,  # eligibility floor: bypasses 20% concentration cap; actual IG min ~$2.75 (minDeal×lot×spot×pu)
    "BTC":   807.20,  # minDeal=0.01 × lot=1 × ~$80,720 (live API) — blocks at <~$1,345 balance (20% cap, 10% margin, lev capped at 3)
    "ETH":   100.16,  # minDeal=0.04 × lot=1 × ~$2,504 (live API) — blocks at <~$167 balance (20% cap, 10% margin, lev capped at 3)
}
_NOTIONAL_REDIS_KEY = "june_min_notionals"  # separate key — survives sim resets
_NOTIONAL_REDIS_TTL = 7 * 24 * 3600        # 7 days

DIRECT_CFD_REDIS_TTL       = 24 * 3600 # Redis TTL for june_direct_cfd_map
NAV_CACHE_TTL              = 6 * 3600  # Redis TTL for june_market_nav_cache
# US premarket window (kept for reference; is_us_premarket() now uses _US_EAST_TZ directly):
#   EDT (UTC-4, summer): 05:00-09:30 ET = 09:00-13:30 UTC
#   EST (UTC-5, winter): 05:00-09:30 ET = 10:00-14:30 UTC
US_PREMARKET_START_MIN = 10 * 60       # winter (EST) anchor -- superseded by DST-aware is_us_premarket()
US_PREMARKET_END_MIN   = 14 * 60 + 30  # winter (EST) anchor -- superseded by DST-aware is_us_premarket()

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
    "SPCX": "spacex",
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
    "SPCX":  ["SpaceX"],
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
    "SPCX",
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
    "SILVER": "CS.D.CFDSILVER.BMU.IP",   # Spot Silver ($1) -- BMU matches Gold pattern; CFM (500oz) has null live bid/offer, causes 403
    "OIL":    "CC.D.LCO.BMU.IP",       # smaller variant: Oil - Brent Crude ($1) -- $9,244 min vs $123,197 for .USS.IP
    "NATGAS": "CC.D.NG.BMU.IP",         # Natural Gas ($1) -- 3% margin, min_margin=$3.53 at $36 balance; ENERGY class
    "WHEAT":  "CC.D.W.BMU.IP",          # Chicago Wheat ($1) -- 2% margin, S/ATR 0.557; METAL class
    "COCOA":  "CC.D.LCC.BMU.IP",        # London Cocoa ($1) -- 10% margin, S/ATR 0.343; METAL class
    "LWB":    "CO.D.LWB.FBMU3.IP",      # London Wheat -- 10% margin, ICE London 07:00-17:30 UTC; METAL class
    "SUGAR":  "CC.D.LSU.BMU.IP",        # Sugar London No.5 -- ICE London 07:00-17:30 UTC; METAL class
    "HO":     "CC.D.HO.BMU.IP",         # Heating Oil -- NYMEX 09:00-21:00 UTC; ENERGY class
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
    "SPCX": "UD.D.SPCXUS.CASH.IP",     # SpaceX — IPO June 12 2026, Nasdaq
    # Crypto CFDs — 24/7, bypasses FX weekend gate via _CONTINUOUS_INSTRUMENTS
    "BTC":  "CS.D.BITCOIN.CFD.IP",      # Bitcoin ($1) — lot=1, minDeal=0.001
    "ETH":  "CS.D.ETHUSD.CFD.IP",       # Ether ($1) — lot=1, minDeal=0.04 (live) — demo showed 0.0001 (wrong)
}

# Reverse lookup: epic → base symbol (used to route .CASH.IP epics to Finnhub)
_INSTRUMENTS_REVERSE: dict = {v: k for k, v in INSTRUMENTS.items()}

_SEARCH_FALLBACKS: dict = {
    "BTC":    "Bitcoin",
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "SPX500": "US 500",
    "GER40":  "Germany 40",
    "UK100":  "UK 100",
    "GOLD":   "Gold",
    "SILVER": "Silver",
    "OIL":    "Brent Crude",
    "NATGAS": "Natural Gas",
    "WHEAT":  "Chicago Wheat",
    "COCOA":  "London Cocoa",
    "LWB":    "London Wheat",
    "SUGAR":  "Sugar No 5",
    "HO":     "Heating Oil",
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

# ── Lightstreamer Phase 1 state ───────────────────────────────────────────────
_ls_client:          object = None           # LightstreamerClient instance (or None)
_ls_account_id:      str    = ""             # live account ID from API response
_ls_account:         dict   = {}             # latest ACCOUNT subscription fields
_ls_account_lock            = threading.Lock()
_ls_connected:       bool   = False          # True when LS status starts with CONNECTED
_ls_confirms_closed: set    = set()          # deal_ids confirmed FULLY_CLOSED via TRADE stream
_ls_confirms_lock           = threading.Lock()
_june_live_trading_enabled: bool = False  # kill switch: must be True via Redis to place live orders
_live_lot_sizes: dict = {}   # sym -> IG lotSize from LIVE API (populated in _live_startup)
_live_min_deal:  dict = {}   # sym -> minDealSize.value from LIVE API (populated in _live_startup)
_live_pip_sizes: dict = {}   # sym -> pip size in price units from LIVE API (populated in _live_startup)
_live_price_unit: dict = {}  # sym -> USD-per-native-price-unit (0.01 for cents, 1.0 otherwise)
_live_margin:    dict = {}   # sym -> IG margin rate (0.0-1.0 fraction) from LIVE API, e.g. 0.8=80%
_live_equity_cfd: set = set() # syms whose epic ends .CASH.IP — IG size field is shares, not lots
_live_fx_instruments: set = set() # syms whose epic matches CS.D.*.CFD.IP — FX pairs (correct sizing: lot_sz/pip_sz)
_METALS_INSTRUMENTS: frozenset = frozenset({"SILVER", "OIL"})  # CME/COMEX-linked; separate weekend gate from FX
_CONTINUOUS_INSTRUMENTS: frozenset = frozenset({"BTC", "ETH"})  # 24/7 markets (crypto CFDs) — bypasses FX weekend closure gate
_IG_EQUITY_COMMISSION_USD = 9.0       # IG charges $9/side = $18 round-trip on equity CFDs
_live_min_stop_pts: dict = {}  # sym -> minNormalStopOrLimitDistance (pts) from IG at startup
_live_elig_publish_next: float = 0.0  # rate-limiter for barbie_june_eligible_instruments (1h)
_MACRO_STALE_SECS  = 2 * 3600   # Claudia freshness gate: beyond this treat directional bias as stale

# Rolling price history: {sym: deque([(epoch, mid), ...])}
_history: dict = {sym: deque(maxlen=HISTORY_LEN) for sym in INSTRUMENTS}
_no_history_warned: set = set()   # instruments logged no-5m-history this run

# Rolling spread history: {sym: deque([spread_pct, ...])} — ~1h at 60s
_spread_hist: dict = {sym: deque(maxlen=SPREAD_HISTORY_LEN) for sym in INSTRUMENTS}

# Live-price fallback: activated per-epic when demo marketStatus == OFFLINE.
# _live_price_scale caches correction = live_SF / demo_SF (derived from IG metadata).
# _live_fallback_active tracks which epics are currently sourced from live /prices/.
_live_price_scale:    dict = {}  # epic -> float correction factor
_live_fallback_active: set = set()  # epics currently bypassing demo pricing

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
_cfd_signal_cursor: int    = 0     # rotating index for cursor-batched signal refresh (3/cycle)
_notional_pending: list    = []    # queue of (sym, epic) for gradual min-notional backfill
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



def _refresh_live_kill_switch() -> None:
    """Read june_live_enabled from Redis each cycle; update the module flag.

    Default is OFF. Enable with: redis-cli SET june_live_enabled true
    Disable with:               redis-cli SET june_live_enabled false
    Fail-safe: any exception leaves the flag unchanged (stays False if never set).
    """
    global _june_live_trading_enabled
    try:
        val = _redis().get("june_live_enabled")
        _june_live_trading_enabled = val in (b"true", b"1", "true", "1")
    except Exception:
        pass


def _live_trade_guard() -> bool:
    """Return True only if live trading is explicitly enabled.

    Call this at the top of any function that places a live order.
    Returns False (blocking the trade) when kill switch is off.
    """
    if not _june_live_trading_enabled:
        import logging
        logging.warning("🔒 live_trade_guard: june_live_enabled=false — order blocked")
        return False
    return True


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
            # Lightstreamer: start ACCOUNT+TRADE subscriptions
            _ls_ep = data.get("lightstreamerEndpoint", "")
            if _ls_ep and acct_id and acct_id != "?":
                _ls_init_session(_ls_ep, _live_sess["cst"], _live_sess["token"], acct_id)
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


def _ig_live_get(path: str, params: Optional[dict] = None, version: str = "1", not_found_default: Optional[dict] = None) -> Optional[dict]:
    # GET against IG live endpoint. Intelligence-only, no orders.
    # Mirrors _ig_get() using live base URL and live session tokens.
    # 403/429 silently return None (rate-limit recovery expected).
    global _live_api_paused_until
    if time.time() < _live_api_paused_until:
        print(f"[{_ts()}] GET {path} skipped -- 429 rate-limit pause active", flush=True)
        return None
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
        if r.status_code == 429:
            _live_api_paused_until = time.time() + 60.0
            print(f"[{_ts()}] ⚠️  LIVE GET 429 rate-limit -- API paused 60s", flush=True)
            return None
        if r.status_code == 404 and not_found_default is not None:
            return not_found_default
        if r.status_code not in (403,):
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
    """Return bid/offer/mid/spread for an epic, or None on any error.

    Normal path: demo /markets/{epic} snapshot (unchanged for TRADEABLE instruments).
    Fallback:    when demo marketStatus == OFFLINE and live session is available,
                 uses live /prices/{epic} (MINUTE resolution, most recent candle).
                 Scale correction applied via IG-authoritative scalingFactor metadata:
                   correction = live_SF / demo_SF
                 Confirmed 7-instrument test (2026-08-17):
                   Silver: demo_SF=100, live_SF=1 -> correction=0.01
                   All others (GOLD, OIL, AUDUSD, GER40, SPX500, EURUSD): SF match -> 1.0
                 _history cleared on first cycle a given epic enters fallback,
                 preventing stale-scale contamination in the 5m/15m change calculations.
    """
    global _live_price_scale, _live_fallback_active
    if ".CASH." in epic:
        sym = _INSTRUMENTS_REVERSE.get(epic)
        return _finnhub_price(sym) if sym else None
    data = _ig_get(f"/markets/{epic}")
    if not data:
        return None
    snap  = data.get("snapshot", {})
    bid   = snap.get("bid")
    offer = snap.get("offer")

    if snap.get("marketStatus") == "OFFLINE" and _live_available:
        return _fetch_price_live_fallback(epic, snap)

    # Demo is TRADEABLE -- use it as normal.
    # If this epic was previously in fallback, mark it as exited so the next
    # OFFLINE episode will clear history again on entry.
    if epic in _live_fallback_active:
        _live_fallback_active.discard(epic)
        sym = _INSTRUMENTS_REVERSE.get(epic, epic)
        print(f"[{_ts()}] Live price: {sym} demo back TRADEABLE -- resuming demo pricing", flush=True)

    if bid is None or offer is None:
        return None
    bid, offer = float(bid), float(offer)
    mid = (bid + offer) / 2.0
    return {"bid": bid, "offer": offer, "mid": mid, "spread": offer - bid}


def _fetch_price_live_fallback(epic: str, demo_snap: dict) -> Optional[dict]:
    """Live /prices/ fallback for instruments OFFLINE on demo.

    Derives scale correction from IG scalingFactor fields (fetched once per
    fallback session via live /markets/, then cached). Clears _history for this
    epic on first entry to prevent mixing old-scale and new-scale values in the
    rolling window used by compute_signal() to calculate change_5m / change_15m.
    """
    global _live_price_scale, _live_fallback_active

    # Derive and cache scale correction (once per fallback session per epic)
    if epic not in _live_price_scale:
        live_data = _ig_live_get(f"/markets/{epic}", version="1")
        if not live_data:
            return None
        demo_sf = float(demo_snap.get("scalingFactor") or 1)
        live_sf = float(live_data.get("snapshot", {}).get("scalingFactor") or 1)
        correction = live_sf / demo_sf if demo_sf != 0 else 1.0
        _live_price_scale[epic] = correction
        sym = _INSTRUMENTS_REVERSE.get(epic, epic)
        print(
            f"[{_ts()}] Live price fallback: {sym} demo OFFLINE "
            f"(demo_SF={int(demo_sf)} live_SF={int(live_sf)} correction={correction:.6f})",
            flush=True,
        )

    correction = _live_price_scale[epic]

    # Clear stale history on first cycle entering fallback for this epic
    if epic not in _live_fallback_active:
        sym = _INSTRUMENTS_REVERSE.get(epic)
        if sym and sym in _history:
            _history[sym].clear()
            print(f"[{_ts()}] {sym}: history cleared (entering live fallback)", flush=True)
        _live_fallback_active.add(epic)

    # Fetch current price from live /prices/ (MINUTE resolution, most recent candle)
    price_data = _ig_live_get(
        f"/prices/{epic}",
        params={"resolution": "MINUTE", "max": 1, "pageSize": 1},
        version="3",
    )
    if not price_data:
        return None
    prices = price_data.get("prices", [])
    if not prices:
        return None
    cp      = prices[-1].get("closePrice", {})
    raw_bid = cp.get("bid")
    raw_ask = cp.get("ask")
    if raw_bid is None or raw_ask is None:
        return None

    bid   = raw_bid * correction
    offer = raw_ask * correction
    mid   = (bid + offer) / 2.0
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
        # CRYPTO instruments have stable hardcoded epics — a transient 403 during
        # verify must NOT trigger search-based discovery, which risks finding unrelated
        # instruments (e.g. 'Ether' -> Etherstack PLC via IG search).
        if _SPREAD_ATR_ASSET_CLASS.get(sym) == 'CRYPTO':
            print(f"[{_ts()}]   ⚠️  {sym} verify rate-limited — keeping hardcoded epic (no search fallback for CRYPTO)", flush=True)
            continue
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
    """True during IG forex weekend closure: Fri 22:00 UK local → Sun 21:00 UK local.

    Uses Europe/London (via _UK_TZ) for DST-aware conversion so the reopen time
    tracks IG's published 9pm UK forex hours year-round:
      BST (UTC+1, summer): 21:00 UK = 20:00 UTC = 15:00 Jamaica
      GMT (UTC+0, winter): 21:00 UK = 21:00 UTC = 16:00 Jamaica
    Jamaica is UTC-5 with no DST, so the Jamaica offset is always -5 from UTC.
    """
    now_uk = datetime.now(_UK_TZ)
    dow    = now_uk.weekday()            # 0=Mon … 4=Fri, 5=Sat, 6=Sun
    uk_h   = now_uk.hour + now_uk.minute / 60.0
    if dow == 5:                         # full Saturday
        return True
    if dow == 4 and uk_h >= 22.0:       # Friday after 22:00 UK (IG forex close)
        return True
    if dow == 6 and uk_h < 21.0:        # Sunday before 21:00 UK (IG forex open)
        return True
    return False


def _is_metals_weekend_closure() -> bool:
    """True during CME metals weekend closure: Fri 17:00 ET -> Sun 18:00 ET.

    COMEX/CME precious metals (SILVER) and IG Brent crude (OIL) follow the
    US market calendar. IG CFDs reopen Sunday 18:00 ET, roughly 2 hours after
    forex (21:00 UK local). Using America/New_York for DST-aware conversion:
      EDT (UTC-4, summer): 18:00 ET = 22:00 UTC = 17:00 Jamaica
      EST (UTC-5, winter): 18:00 ET = 23:00 UTC = 18:00 Jamaica
    Mirrors the is_weekend_closure() pattern using Europe/London.
    """
    now_et = datetime.now(_US_EAST_TZ)
    dow    = now_et.weekday()            # 0=Mon ... 4=Fri, 5=Sat, 6=Sun
    et_h   = now_et.hour + now_et.minute / 60.0
    if dow == 5:                         # full Saturday
        return True
    if dow == 4 and et_h >= 17.0:        # Friday after 17:00 ET (CME metals close)
        return True
    if dow == 6 and et_h < 18.0:         # Sunday before 18:00 ET (CME metals open)
        return True
    return False


def _now_mins() -> int:
    now = datetime.now(timezone.utc)
    return now.hour * 60 + now.minute


def is_overnight() -> bool:
    """True between 21:00 UTC (NY close) and 07:00 UTC (London open)."""
    m = _now_mins()
    return m >= OVERNIGHT_START_MIN or m < OVERNIGHT_END_MIN


def _current_sub_session(sym: str) -> str:
    """Trading sub-session label for SAR block bucketing.

    OIL / SILVER: day session split into three independent buckets so that
    afternoon chop cannot block the next morning's primary trading window.
      pre_nyse:       07:00 UTC - NYSE open     (London + pre-market hours)
      nyse_morning:   NYSE open - 12:00 ET noon (highest volume, tightest spreads)
      nyse_afternoon: 12:00 ET  - 21:00 UTC     (lower volume, wider spreads)
    Other instruments: plain "day" (single bucket, unchanged behaviour).
    Overnight always returns "overnight" for all instruments.
    """
    if is_overnight():
        return "overnight"
    if sym not in ("OIL", "SILVER"):
        return "day"
    now_et       = datetime.now(_US_EAST_TZ)
    nyse_open_et = now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
    nyse_mid_et  = now_et.replace(hour=12, minute=0,  second=0, microsecond=0)
    if now_et < nyse_open_et:
        return "pre_nyse"
    if now_et < nyse_mid_et:
        return "nyse_morning"
    return "nyse_afternoon"


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
        _htf_self_calibrate()  # re-run on date rollover (no-op if <10 new matched events)
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


    payload = {
        "timestamp":                int(time.time()),
        "high_overnight_volatility": high_vol,
        "volatile_instruments":     volatile_syms,
        "overnight_moves":          overnight_moves,
        "correlation_note":         "",   # deprecated; see june_correlation_map
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
    except Exception as exc:
        print(f"[{_ts()}] ❌ Redis error (overnight_context): {exc}", flush=True)


# _detect_correlation_shifts() removed — replaced by june_correlation_map
# (real Pearson correlation across 24 instruments, published by Claudia every 15 min)


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



def _compute_atr(sym: str) -> float | None:
    """Approximate 14-period ATR from rolling mid-price history.
    Uses |consecutive mid changes| as a proxy for True Range (no OHLC available).
    Returns None when fewer than ATR_PERIOD+1 readings exist — caller skips penalty.
    """
    hist = _history.get(sym)
    if not hist or len(hist) < ATR_PERIOD + 1:
        return None
    prices = [px for _, px in hist]
    trs = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
    recent = trs[-ATR_PERIOD:]
    if not recent:
        return None
    return sum(recent) / len(recent)


def _compute_atr_5m(sym: str) -> tuple:
    """5-minute ATR from rolling mid-price history.
    Computes mean |price[i] - price[i-5]| (five-minute deltas) over the deque.
    Returns (atr, is_fallback=False) when >= 15 five-minute deltas are available.
    Falls back to 14-period 1-minute ATR with is_fallback=True when history is thin.
    Returns (None, False) when insufficient data exists for either calculation.
    """
    hist = _history.get(sym)
    if not hist:
        return None, False
    prices = [px for _, px in hist]
    n = len(prices)
    STEP     = 5    # five-minute window
    REQUIRED = 15   # minimum five-minute deltas for full 5m ATR
    deltas_5m = [abs(prices[i] - prices[i - STEP]) for i in range(STEP, n)]
    if len(deltas_5m) >= REQUIRED:
        return sum(deltas_5m) / len(deltas_5m), False   # full 5m ATR
    # Fewer than 15 five-minute deltas — fall back to 1-minute ATR
    if n < ATR_PERIOD + 1:
        return None, True   # not enough data even for 1m ATR
    trs_1m = [abs(prices[i] - prices[i - 1]) for i in range(1, n)]
    recent = trs_1m[-ATR_PERIOD:]
    return (sum(recent) / len(recent) if recent else None), True


def _spread_atr_threshold(sym: str, is_fallback: bool) -> float:
    """Tiered Spread/ATR entry threshold by asset class.
    ENERGY (OIL): 0.85 | METAL (GOLD, SILVER): 0.60 | FX/EQUITY: 0.35
    SILVER 00:00-02:59 UTC: 0.75 (overnight window; ATR briefly expands)
    +0.15 added during 1m-ATR fallback while 5m history warms up.
    """
    tier = _SPREAD_ATR_TIERS.get(_SPREAD_ATR_ASSET_CLASS.get(sym, "FX"), 0.35)
    # 00:00-02:59 UTC: SILVER p50 ratio reaches 74-86%, justified by real data.
    # Raise hard-block from 0.60 to 0.75 for this window only.
    # OIL (ENERGY 0.85) and FX (0.35) tiers are untouched.
    if sym == "SILVER" and datetime.now(timezone.utc).hour < 3:
        tier = 0.75
    return tier + (_SPREAD_ATR_FALLBACK_BUMP if is_fallback else 0.0)


def compute_signal(sym: str, price: dict, spread_alert: bool = False) -> dict:
    mid        = price["mid"]
    spread_pct = (price["spread"] / mid * 100.0) if mid > 0 else 0.0
    px5        = _price_n_minutes_ago(sym, 5)
    px15       = _price_n_minutes_ago(sym, 15)
    change_5m  = ((mid - px5)  / px5  * 100.0) if px5  is not None else None
    change_15m = ((mid - px15) / px15 * 100.0) if px15 is not None else None
    _dir_thresh = 0.02 if sym in _FX_INSTRUMENTS else 0.05
    if change_5m is None:
        direction = "no_history"
    elif change_5m >  _dir_thresh:
        direction = "bull"
    elif change_5m < -_dir_thresh:
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
    """True during US pre-market: 05:00-09:30 ET (DST-aware via America/New_York).

    Uses _US_EAST_TZ for DST-aware conversion, mirroring _is_metals_weekend_closure():
      EDT (UTC-4, summer): 05:00-09:30 ET = 09:00-13:30 UTC
      EST (UTC-5, winter): 05:00-09:30 ET = 10:00-14:30 UTC
    Replaces hardcoded US_PREMARKET_START/END_MIN UTC constants.
    """
    now_et = datetime.now(_US_EAST_TZ)
    et_h   = now_et.hour + now_et.minute / 60.0
    return 5.0 <= et_h < 9.5


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
    """Fetch daily snapshot signals for known direct CFD epics (cursor-batched, 3/cycle).

    Processes 3 epics per 5-min cycle, rotating through _direct_cfd_map via
    _cfd_signal_cursor. A full pass takes ~20 cycles (100 min), using 36 calls/hr
    vs the prior burst of 720+/hr that exhausted both IG rate-limit layers.

    _cfd_signal_refresh is always reset after each call — the prior implementation
    only reset inside `if updated:`, so the 5-min cooldown never applied when all
    calls failed with 403, causing immediate re-bursting on the next cycle.
    """
    global _cfd_signal_refresh, _cfd_signal_cursor
    if not _direct_cfd_map:
        return
    if time.time() - _cfd_signal_refresh < 5 * 60:
        return
    all_pairs = sorted(_direct_cfd_map.items())  # deterministic order for cursor rotation
    n = len(all_pairs)
    if n == 0:
        _cfd_signal_refresh = time.time()
        return
    BATCH = 3
    updated = []
    for i in range(BATCH):
        base, epic = all_pairs[(_cfd_signal_cursor + i) % n]
        detail = _ig_get(f"/markets/{epic}")
        if not detail:
            continue
        snap = detail.get("snapshot", {})
        pct  = snap.get("percentageChange")
        if pct is None:
            continue
        pct       = float(pct)
        direction = "bull" if pct > 0.1 else ("bear" if pct < -0.1 else "flat")
        _bid_raw  = snap.get("bid")
        _off_raw  = snap.get("offer")  # IG uses "offer" for ask
        _bid_f    = float(_bid_raw) if _bid_raw is not None else 0.0
        _off_f    = float(_off_raw) if _off_raw is not None else 0.0
        _mid_f    = round((_bid_f + _off_f) / 2.0, 6) if _bid_f > 0 and _off_f > 0 else 0.0
        _direct_cfd_signals[base] = {
            "pct":       round(pct, 3),
            "direction": direction,
            "ts":        int(time.time()),
            "bid":       _bid_f,
            "offer":     _off_f,
            "mid":       _mid_f,
        }
        updated.append(f"{base}={pct:+.2f}%")
        time.sleep(1)
    _cfd_signal_cursor = (_cfd_signal_cursor + BATCH) % n
    _cfd_signal_refresh = time.time()  # always reset — prevents 403-starvation loop
    if updated:
        print(
            f"[{_ts()}] 📈 Direct CFD signals (batch, cursor {_cfd_signal_cursor}/{n}): "
            f"{', '.join(updated)}",
            flush=True,
        )

def _advance_notional_backfill() -> None:
    """Pop up to 2 instruments from _notional_pending and query /markets/ (Parts 2+3).

    Called once per poll_cycle at 60s intervals — 2 calls/cycle = 120/hr, empirically
    safe against IG rate limits (IG exposes no rate-limit headers; 2/cycle confirmed
    conservative via prior session analysis).

    On success, persists learned min_notionals to june_min_notionals Redis key (7-day
    TTL) so they survive sim resets and service restarts (Part 4).
    """
    global _notional_pending, _sim_min_notional, _sim_eligible
    if not _notional_pending:
        return
    batch, _notional_pending[:] = _notional_pending[:2], _notional_pending[2:]
    changed = False
    for sym, epic in batch:
        if sym in _sim_min_notional:
            continue
        data = _ig_get(f"/markets/{epic}")
        if not data:
            _notional_pending.append((sym, epic))  # re-queue for next cycle
            continue
        inst    = data.get("instrument", {})
        deal    = data.get("dealingRules", {})
        snap    = data.get("snapshot", {})
        min_val = float(deal.get("minDealSize", {}).get("value", 1.0))
        lot_sz  = float(inst.get("lotSize", 1.0))
        bid     = float(snap.get("bid") or 0)
        offer   = float(snap.get("offer") or 0)
        mid     = (bid + offer) / 2.0 if bid and offer else 0.0
        if mid == 0.0 and ".CASH." in epic:
            fh_sym = _INSTRUMENTS_REVERSE.get(epic)
            if fh_sym:
                fh = _finnhub_price(fh_sym)
                if fh:
                    mid = fh["mid"]
        ccy0    = inst.get("currencies", [{}])[0] if inst.get("currencies") else {}
        fx_base = float(ccy0.get("baseExchangeRate") or 1.0) or 1.0
        pu      = _live_price_unit.get(sym, 1.0)  # 0.01 for cent-denominated (e.g. OIL CC.D.*)
        if ".CASH.IP" in epic:
            min_usd = (min_val * mid * pu) / fx_base          # equity shares: size = shares, no lot multiplier
        else:
            min_usd = (min_val * lot_sz * mid * pu) / fx_base
        _sim_min_notional[sym] = round(min_usd, 2)
        _sim_eligible.add(sym)
        changed = True
        remaining = len(_notional_pending)
        print(
            f"[{_ts()}] 🧪 SIM backfill: {sym} — IG min ~${min_usd:.2f} "
            f"({'done' if remaining == 0 else f'{remaining} remaining'})",
            flush=True,
        )
    if changed:
        try:
            _redis().set(_NOTIONAL_REDIS_KEY, json.dumps(_sim_min_notional),
                         ex=_NOTIONAL_REDIS_TTL)
        except Exception:
            pass


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
        # Spread-to-ATR boundary check — default to no penalty when ATR not yet available
        _atr = _compute_atr(sym)
        if _atr is not None and _atr > 0:
            _sar = price["spread"] / _atr
            sig["spread_atr_ratio"] = round(_sar, 4)
            if _sar > SPREAD_ATR_THRESHOLD:
                sig["spread_atr_wide"] = True
                print(
                    f"[{_ts()}] 📐 Spread/ATR boundary: {sym} "
                    f"spread={price['spread']:.6f} ATR={_atr:.6f} "
                    f"ratio={_sar:.1%} > {SPREAD_ATR_THRESHOLD:.0%}",
                    flush=True,
                )
            else:
                sig["spread_atr_wide"] = False
        else:
            sig["spread_atr_ratio"] = None
            sig["spread_atr_wide"] = False
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
    _advance_notional_backfill()
    _run_cfd_sync_check()
    if is_us_premarket():
        _check_direct_cfd_gaps()
    _validate_gap_requests()

    # Virtual trading simulation (paper trades only — no real orders)
    if _sim:
        run_simulation_step(signals)

    # Live trading step (real orders gated behind june_live_enabled kill switch)
    if _live:
        run_live_step(signals)

    # Log summary
    alert_tag = f"  🚨 ALERTS → {alerts}" if alerts else ""
    price_row = " | ".join(f"{s}={v['price']:.4f}({v['change_5m']:+.3f}%)" for s, v in signals.items())
    print(f"[{_ts()}] 📡 {len(signals)} signals published (TTL {SIGNAL_TTL}s){alert_tag}", flush=True)
    print(f"[{_ts()}]    {price_row}", flush=True)
    _history_save()
    return True


def _history_save() -> None:
    """Persist all _history deques to Redis so the grind detector survives restarts."""
    try:
        payload = {sym: [[e, m] for e, m in hist] for sym, hist in _history.items() if hist}
        _redis().set(_HISTORY_REDIS_KEY, __import__("json").dumps(payload), ex=_HISTORY_REDIS_TTL)
    except Exception as _exc:
        import logging; logging.getLogger().warning(f"_history_save failed: {_exc}")


def _history_load() -> None:
    """Restore _history deques from Redis on startup.

    Per-instrument staleness check: if the most recent reading for a given
    instrument is older than _HISTORY_STALE_CUTOFF seconds (default 5 min /
    5 missed cycles), that instrument's history is discarded and cold-starts.
    Fresh instruments load immediately -- the grind detector can fire as soon
    as 20 readings are present, with no 20-minute wait after a brief restart.
    """
    try:
        raw = _redis().get(_HISTORY_REDIS_KEY)
        if not raw:
            return
        import json as _json, time as _time
        stored = _json.loads(raw)
        now = _time.time()
        loaded = discarded = 0
        for sym, readings in stored.items():
            if sym not in _history:
                continue
            if not readings:
                continue
            last_epoch = readings[-1][0]
            if now - last_epoch > _HISTORY_STALE_CUTOFF:
                discarded += 1
                continue
            for epoch, mid in readings:
                _history[sym].append((epoch, mid))
            loaded += 1
        if loaded or discarded:
            import logging
            logging.getLogger().info(
                f"_history_load: {loaded} instruments restored, "
                f"{discarded} discarded (stale >{_HISTORY_STALE_CUTOFF}s)"
            )
            print(
                f"[{{__import__('time').strftime('%Y-%m-%d %H:%M UTC', __import__('time').gmtime())}}] "
                f"\U0001f9e0 Price history: {loaded} instruments restored, "
                f"{discarded} discarded (stale >{_HISTORY_STALE_CUTOFF}s)",
                flush=True
            )
    except Exception as _exc:
        import logging; logging.getLogger().warning(f"_history_load failed: {_exc}")


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
_SIM_TP_WIN_WINDOW   = 10      # rolling win-move window per combo (TP/stop sizing)
_SIM_TP_MIN_SAMPLES  = 3       # wins before full-confidence TP fraction applies

# ── Per-combo breakeven gate ──────────────────────────────────────────────────
_SIM_COMBO_MIN_SAMPLES   = 15   # min outcomes before gate applies (8 too small: ±34% CI)
_SIM_COMBO_DECAY         = 0.90  # exp decay (newest trade weight=1.0, 10 back=0.35)
_SIM_COMBO_WINDOW        = 50   # rolling storage for combo_outcomes (separate from TP window)
_BARBIE_COMBO_THRESH_KEY = "barbie_june_combo_thresholds"
_BARBIE_COMBO_THRESH_MIN = 0.10  # safety floor for Barbie override (~10% = floor meaningful)
_BARBIE_COMBO_THRESH_MAX = 0.55  # safety ceiling (~55% = stricter than any real breakeven)

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
    "SPCX": 0.0012,  # synthetic spread — high-vol equity, similar to TSLA
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

# == Live leverage phase system (separate from sim — different track records) ===
_LIVE_PHASE_GATE_BAL         = 200.0  # below: dormant (minDeal floor renders lev differentiation moot)
_LIVE_PHASE_CONSERVATIVE_LEV = 3      # Phase 1 ceiling (matches _SIM_CONSERVATIVE_LEV)
_LIVE_PHASE2_LEV             = 5      # Phase 2 ceiling
_LIVE_PHASE3_LEV             = 10     # Phase 3 ceiling (matches _SIM_AGGRESSIVE_LEV)
_LIVE_P1_TO_P2_TRADES        = 10
_LIVE_P1_TO_P2_WR            = 0.50
_LIVE_P2_TO_P3_TRADES        = 20
_LIVE_P2_TO_P3_WR            = 0.55
_LIVE_P2_TO_P3_PNL           = 0.05   # +5% from phase entry balance
_LIVE_PHASE_DROP_LOSSES      = 5      # consecutive losses triggers drop-back
_LIVE_PHASE_DROP_PNL         = -0.05  # -5% from phase entry triggers drop-back

# ── Sprout sizing rotation ─────────────────────────────────────────────────────
_SIM_SIZING_ORDER        = ["fixed_5", "fixed_10", "pct_5", "pct_10"]
_SIM_MIN_APPROACH_TRADES = 5    # min per-instrument trades before approach is trusted
_SIM_SKIP_THRESHOLD      = 3    # notional-miss skips before approach excluded for this instrument
_SIM_SKIP_BALANCE_TOL    = 1.20 # re-enable skipped approach once balance grows >20%

# ── Volatility buckets ────────────────────────────────────────────────────────
_SIM_HIGH_VOL_THRESH = 0.50
_SIM_LOW_VOL_THRESH  = 0.20

# ── Weekend market signal integration ────────────────────────────────────────
# -- Slow-grind detector: cumulative directional momentum over 4-cycle window
_GRIND_WINDOW      = 19    # rolling cycle count (uses all 20 deque readings -> 19 changes, ~19 min at 60s/cycle)
_GRIND_CONSISTENCY = 0.75  # >=75% of cycles must agree on direction (3 of 4)
_GRIND_THRESH: dict = {    # minimum net cumulative % move to trigger boost
    "OIL":    0.28,         # ~2.5x OIL typical 1-cycle move; recalibrate at bal > $250
    "SILVER": 0.22,         # ~2.5x SILVER typical 1-cycle move; recalibrate at bal > $250
}
_GRIND_MAX_PTS = 1.0       # bounded cap: conviction boost never exceeds 1pt

_SIM_WKND_MAX_PTS = 1.0   # max conviction pts from weekend IG signal; matches streak_pts/rel_pts peers
_WKND_SIGNAL_MAP  = {      # June instrument name → weekend_market_signals key
    "SILVER":  "gold",     # Silver CFD is EDITS_ONLY on weekends; Gold is proxy
    "GOLD":    "gold",
    "EURUSD":  "eurusd",
    "USDJPY":  "usdjpy",
}
_wknd_cache: dict = {"data": None, "t": 0.0}
_WKND_CACHE_TTL   = 300    # re-read Redis at most every 5 min

# ── Dynamic threshold / streak parameters ────────────────────────────────────
_SIM_THRESH_BASE     = 0.05   # minimum vol threshold (%) — safety floor only
_SIM_THRESH_BASE_FX  = 0.02   # lower floor for FX pairs — spread gate handles noise
_FX_INSTRUMENTS      = frozenset({  # FX pairs use lower vol/direction thresholds
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD",
    "USDCAD", "EURGBP", "NZDUSD", "USDCHF",
})
_SIM_THRESH_CAP      = 0.50   # maximum vol threshold (%)
_SIM_THRESH_MULT     = 1.5    # std-dev multiplier for threshold calc
_SIM_THRESH_MIN_HIST = 10     # min vol history samples before dynamic thresh
_SIM_STREAK_BOOST    = 3      # loss streak count that raises threshold
_SIM_STREAK_PAUSE    = 5      # loss streak count that pauses entries
_SIM_BOOST_DUR       = 30 * 60    # boost duration (30m)
_SIM_PAUSE_DUR       = 45 * 60    # pause duration (45m)
_SIM_BALANCE_FLOOR   = 40.0   # auto-reset if balance drops below this
_SIM_CONCENTRATION_CAP = 0.20  # max (min_notional / leverage) as fraction of balance

# Conviction-scaled sizing floors: min position % at conviction=1; ceiling=20% at conviction=10
_SIM_SIZE_FLOORS = {
    "seedling":    0.05,
    "germination": 0.02,
    "vegetative":  0.01,
    "full_bloom":  0.01,
}

# ── Dynamic 15m reliability gate ───────────────────────────────────────────────
_SIM_15M_MIN_SAMPLES = 5     # trades before reliability scoring activates; cold-start = relaxed
_SIM_15M_LOOKBACK    = 20    # rolling history window per instrument+direction
_SIM_15M_STRICT_MIN  = 0.20  # reliability score >= this -> STRICT mode (require 15m alignment)
_SIM_15M_DEADZONE    = 0.05  # MODERATE mode: |change_15m| < deadzone treated as flat (passes)
_SIM_1M_MIN_REVERSAL = 0.030 # 1m gate: min % price move to qualify as a real reversal

# ── Stage definitions ─────────────────────────────────────────────────────────
_SIM_STAGE_DEFS = {
    # min_pnl_pct: fraction of stage_entry_balance required as net gain for graduation.
    # With balance injection (Part 1), these now reflect real dollar thresholds:
    # Sprout: no requirement (entry stage). Seedling: +1% of $70 = $0.70.
    # Germination: +2% of $300 = $6. Vegetative: +2% of $1,000 = $20.
    "sprout":      {"label": "SPROUT",      "min_trades": 7,  "min_wr": 0.50, "min_pnl_pct": 0.00, "fail_after": 30},
    "seedling":    {"label": "SEEDLING",    "min_trades": 10, "min_wr": 0.55, "min_pnl_pct": 0.01, "fail_after": 40},
    "germination": {"label": "GERMINATION", "min_trades": 20, "min_wr": 0.55, "min_pnl_pct": 0.02, "fail_after": 50},
    "vegetative":  {"label": "VEGETATIVE",  "min_trades": 25, "min_wr": 0.60, "min_pnl_pct": 0.02, "fail_after": 60},
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

# Conviction-based leverage ranges per stage.
# floor = current conservative base; ceiling scales with stage maturity.
_SIM_LEV_RANGES = {
    "sprout":      (3, 10),
    "seedling":    (3, 10),
    "germination": (3, 15),
    "vegetative":  (5, 20),
    "full_bloom":  (5, 20),
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

# ── Correlation signal from Claudia ──────────────────────────────────────────
_correlation_map: dict = {}   # loaded from june_correlation_map each sim cycle

# Maps June instrument name → FMP ticker used in june_corr_history_cache
_JUNE_TO_FMP: dict = {
    "SILVER":  "XAGUSD",  "GOLD":    "XAUUSD",  "OIL":     "USO",
    "SPX500":  "SPY",     "NVDA":    "NVDA",     "TSLA":    "TSLA",
    "AAPL":    "AAPL",    "MSFT":    "MSFT",     "AMD":     "AMD",
    "INTC":    "INTC",    "MU":      "MU",       "SPCX":    "SPCX",
    "EURUSD":  "EURUSD",  "GBPUSD":  "GBPUSD",  "USDJPY":  "USDJPY",
    "AUDUSD":  "AUDUSD",  "USDCAD":  "USDCAD",  "NZDUSD":  "NZDUSD",
    "USDCHF":  "USDCHF",  "GLD":     "GLD",      "SLV":     "SLV",
    "QQQ":     "QQQ",     "TLT":     "TLT",      "XLF":     "XLF",
}

# ── Barbie override signal ───────────────────────────────────────────────────
_barbie_overrides:         dict = {}  # loaded from barbie_june_overrides each sim cycle
_barbie_combo_thresholds:  dict = {}  # loaded from barbie_june_combo_thresholds each cycle
_BARBIE_OVERRIDE_MIN_SECS: int  = 60  # hard floor: 1 cycle minimum
_BARBIE_OVERRIDE_MAX_SECS: int  = 600 # hard ceiling: 10 cycles maximum
# Stop/TP multiplier safety bounds (Barbie-settable via barbie_june_overrides)
_BARBIE_STOP_MULT_MIN:  float = 1.0   # floor: 1σ stop — inside noise below this
_BARBIE_STOP_MULT_MAX:  float = 4.0   # ceiling: 4σ stop — extreme-event-only above
_BARBIE_TP_FRAC_MIN:    float = 0.50  # floor: half avg-win (sub-breakeven below)
_BARBIE_TP_FRAC_MAX:    float = 1.50  # ceiling: 1.5x avg-win (unreachable above)
# Alarm-clock escalation (June writes, Barbie polls)
_BARBIE_ALARM_KEY:     str   = "barbie_alarm"
_BARBIE_ALARM_TTL:     int   = 3600   # 1-hour expiry; Barbie deletes on read
_BARBIE_ALARM_DEFAULT_THRESHOLD: float = 0.50  # 50% vol deviation triggers alarm

_BARBIE_PNL_THRESHOLD_KEY: str = "barbie_pnl_threshold"
# P&L alarm bounds (fraction of session start balance, negative = drawdown).
# CEIL tightest: 5 max-stop trades (0.5% stop * 10x lev * 20% position = 1%/trade -> 5%).
# FLOOR loosest: derived from -20% auto-reset ($40/$50): alarm must fire with 35% headroom
#   before the floor fires, so 65% * 20% = 13% max looseness.
_PNLWAKE_CEIL_PCT:    float = -0.05
_PNLWAKE_FLOOR_PCT:   float = -((1.0 - _SIM_BALANCE_FLOOR / _SIM_START_BALANCE) * 0.65)
_PNLWAKE_DEFAULT_PCT: float = -0.10
# Open-position adjustment (Barbie writes, June applies; separate from entry baseline)
_BARBIE_POS_ADJUST_KEY: str  = "barbie_june_pos_adjust"


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
        "combo_outcomes":       _sim.get("combo_outcomes", {}),
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


_SIM_TRADE_HISTORY_KEY = "june_sim_trade_history"
_SIM_TRADE_HISTORY_CAP = 500
_SIM_TRADE_HISTORY_TTL = 30 * 24 * 3600  # 30 days


def _sim_save_trade(trade_rec: dict) -> None:
    """Append one completed SIM trade to the persistent cross-run Redis list."""
    try:
        r = _redis()
        r.lpush(_SIM_TRADE_HISTORY_KEY, json.dumps(trade_rec))
        r.ltrim(_SIM_TRADE_HISTORY_KEY, 0, _SIM_TRADE_HISTORY_CAP - 1)
        r.expire(_SIM_TRADE_HISTORY_KEY, _SIM_TRADE_HISTORY_TTL)
    except Exception:
        pass


def _sim_load_trade_history() -> list:
    """Load persisted SIM trade records from Redis (chronological order)."""
    try:
        raw_list = _redis().lrange(_SIM_TRADE_HISTORY_KEY, 0, _SIM_TRADE_HISTORY_CAP - 1)
        return list(reversed([json.loads(r) for r in raw_list]))
    except Exception:
        return []


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
            rw = sum(1 for t in recent if (t.get("dollar_pnl") or 0) > 0) / 10
            rp = sum((t.get("dollar_pnl") or 0) for t in recent)
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
        rw = sum(1 for t in recent if (t.get("dollar_pnl") or 0) > 0) / len(recent) if recent else 0.0
        rp = sum((t.get("dollar_pnl") or 0) for t in recent)
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
        _sim_log("🌸 FULL BLOOM REACHED -- simulation continues at full bloom stage")
        _sim_save_state()
        return False

    _sim_save_state()
    return False



# ── Phase management ──────────────────────────────────────────────────────────
def _sim_phase_leverage(phase: int) -> int:
    """Return leverage multiplier for the given phase number."""
    if phase == 1: return _SIM_CONSERVATIVE_LEV
    if phase == 2: return _SIM_PHASE2_LEV
    return _SIM_AGGRESSIVE_LEV


def _sim_conviction_leverage(stage: str, conviction: int) -> int:
    """Map conviction 1–10 to leverage within the stage's floor–ceiling range."""
    floor, ceiling = _SIM_LEV_RANGES.get(stage, (3, 10))
    t = (conviction - 1) / 9.0
    return max(floor, min(ceiling, floor + round(t * (ceiling - floor))))


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
def _sim_position_size(balance: float, approach: str, conviction: int = 5) -> float:
    stage = _sim.get("stage", "sprout")
    if stage == "sprout":
        if approach == "fixed_5":  return 5.0
        if approach == "fixed_10": return 10.0
        if approach == "pct_5":    return round(balance * 0.05, 2)
        if approach == "pct_10":   return round(balance * 0.10, 2)
        return 5.0
    # Conviction-scaled: floor_pct (conviction=1) → 20% concentration cap (conviction=10)
    pct_floor = _SIM_SIZE_FLOORS.get(stage, 0.05)
    pct = pct_floor + (_SIM_CONCENTRATION_CAP - pct_floor) * (conviction - 1) / 9.0
    return round(balance * pct, 2)


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
def _sim_conviction_gauge(
    sym: str, direction: str, vol: float, thresh: float,
    weight: float, combo: str, gate_mode: str, rel_score
) -> int:
    """1-10 conviction score from already-computed signal inputs. No new indicators.
    clearance (0-5) + regime weight (-1 to +2) + vol bucket (0-2)
    + streak (0-1) + 15m reliability (-0.5 to +1). Clamped to [1, 10]."""
    clearance  = min(vol / thresh, 5.0) if thresh > 0 else 1.0
    regime_pts = max(-1.0, min(2.0, (weight - 1.0) * 4.0))
    # Graduated ramp — piecewise linear through old calibration anchors.
    # (0%, 0pt) → (LOW_THRESH=0.20%, 1pt) → (HIGH_THRESH=0.50%, 2pt).
    # Eliminates the hard cliff at 0.20% without reducing any existing signal.
    if vol <= 0.0:
        bucket_pts = 0.0
    elif vol < _SIM_LOW_VOL_THRESH:
        bucket_pts = round(vol / _SIM_LOW_VOL_THRESH, 2)
    elif vol < _SIM_HIGH_VOL_THRESH:
        bucket_pts = round(1.0 + (vol - _SIM_LOW_VOL_THRESH) / (_SIM_HIGH_VOL_THRESH - _SIM_LOW_VOL_THRESH), 2)
    else:
        bucket_pts = 2.0
    streak_pts = 1.0 if _sim_has_boost(combo) else 0.0
    if gate_mode == "strict" and rel_score is not None and rel_score > 0.5:
        rel_pts = 1.0
    elif gate_mode == "relaxed":
        if rel_score is not None and rel_score < 0:
            rel_pts = min(-0.5, rel_score * 4.0)  # confirmed anti-correlated: scale with magnitude
        else:
            rel_pts = -0.5  # insufficient data (n<5, unknown)
    else:
        rel_pts = 0.0
    wknd_pts    = _sim_weekend_pts(sym, direction)
    claudia_pts = _sim_claudia_pts(sym, direction)
    grind_pts   = _slow_grind_pts(sym, direction)
    raw = clearance + regime_pts + bucket_pts + streak_pts + rel_pts + wknd_pts + claudia_pts + grind_pts
    if wknd_pts:
        import logging; logging.getLogger().debug(f"  wknd_pts={wknd_pts:+.3f} for {sym}/{direction}")
    if claudia_pts:
        import logging; logging.getLogger().debug(f"  claudia_pts={claudia_pts:+.3f} for {sym}/{direction}")
    if grind_pts:
        import logging; logging.getLogger().debug(f"  grind_pts={grind_pts:+.3f} for {sym}/{direction}")
    return max(1, min(10, round(raw)))


def _sim_combo_key(sym: str, direction: str) -> str:
    return f"{sym}_{direction}"


def _sim_combo_wr_gate(sym: str, direction: str) -> tuple:
    """Per-combo breakeven gate using exponentially-weighted win rate.

    Returns (should_skip: bool, log_line: str).
    Gate only fires when combo has >= _SIM_COMBO_MIN_SAMPLES outcomes.

    Threshold priority:
      1. Barbie override from barbie_june_combo_thresholds (clamped to safety bounds)
      2. Formula: avg_loss / (avg_win + avg_loss) from win/loss move sizes (>= 5 each)
      3. Fallback 0.30 when size data insufficient

    WR uses exponential decay (alpha=_SIM_COMBO_DECAY) so recent outcomes dominate
    without the cliff-edge eviction problem of a hard rolling window.
    """
    combo    = f"{sym}_{direction}"
    outcomes = (_sim.get("combo_outcomes") or {}).get(combo, [])
    n        = len(outcomes)
    if n < _SIM_COMBO_MIN_SAMPLES:
        return False, ""

    # Exponentially weighted win rate (i=0 oldest, i=n-1 newest → weight 1.0)
    alpha   = _SIM_COMBO_DECAY
    w_sum   = w_wins = 0.0
    for i, outcome in enumerate(outcomes):
        w       = alpha ** (n - 1 - i)
        w_sum  += w
        w_wins += w * outcome
    weighted_wr = w_wins / w_sum if w_sum > 0 else 0.0

    # Threshold: Barbie override → formula → fallback
    thresh_src = "fallback"
    threshold  = None
    brb_t = (_barbie_combo_thresholds or {}).get(combo)
    if brb_t is not None:
        threshold  = max(_BARBIE_COMBO_THRESH_MIN, min(_BARBIE_COMBO_THRESH_MAX, float(brb_t)))
        thresh_src = "barbie"
    if threshold is None:
        wins_l   = (_sim.get("win_moves")  or {}).get(combo, [])
        losses_l = (_sim.get("loss_moves") or {}).get(combo, [])
        if len(wins_l) >= 5 and len(losses_l) >= 5:
            avg_win  = sum(wins_l)  / len(wins_l)
            avg_loss = sum(losses_l) / len(losses_l)
            denom    = avg_win + avg_loss
            threshold  = round(avg_loss / denom, 4) if denom > 0 else 0.30
            thresh_src = "formula"
        else:
            threshold  = 0.30
            thresh_src = "fallback"

    if weighted_wr < threshold:
        return True, (
            f"⏭️  Skip {sym} {direction.upper()}: "
            f"weighted WR {weighted_wr:.0%} (n={n}) "
            f"< {threshold:.0%} breakeven [{thresh_src}]"
        )
    return False, ""


def _sim_is_paused(combo: str) -> bool:
    exp = (_sim.get("pause_expiry") or {}).get(combo, 0.0)
    return time.time() < exp


def _sim_has_boost(combo: str) -> bool:
    exp = (_sim.get("boost_expiry") or {}).get(combo, 0.0)
    return time.time() < exp


def _read_weekend_signals() -> dict | None:
    """Read weekend_market_signals from Redis with 5-min module-level cache."""
    now = time.time()
    if now - _wknd_cache["t"] < _WKND_CACHE_TTL and _wknd_cache["data"] is not None:
        return _wknd_cache["data"]
    try:
        raw = _redis().get("weekend_market_signals")
        if not raw:
            _wknd_cache.update({"data": None, "t": now})
            return None
        data = json.loads(raw)
        _wknd_cache.update({"data": data, "t": now})
        return data
    except Exception:
        return None


def _sim_weekend_pts(sym: str, direction: str) -> float:
    """Conviction contribution from weekend IG market signal. Returns 0.0 if absent."""
    instr_key = _WKND_SIGNAL_MAP.get(sym)
    if not instr_key:
        return 0.0
    signals = _read_weekend_signals()
    if not signals:
        return 0.0
    sig = signals.get("instruments", {}).get(instr_key)
    if not sig or sig.get("confidence", 0) <= 0.1:
        return 0.0
    confidence = sig["confidence"]
    sig_dir    = sig.get("direction", "flat")
    if sig_dir == direction:
        return round(confidence * _SIM_WKND_MAX_PTS, 3)
    if sig_dir not in ("flat", direction):
        return round(-confidence * _SIM_WKND_MAX_PTS * 0.5, 3)
    return 0.0


def _sim_get_threshold(sym: str, direction: str = "", low_tier: bool = False) -> float:
    hist = (_sim.get("vol_history") or {}).get(sym, [])
    mult = 2.0 if low_tier else _SIM_THRESH_MULT
    if len(hist) < _SIM_THRESH_MIN_HIST:
        base_thresh = 0.15
    else:
        n = len(hist)
        mean = sum(hist) / n
        var = sum((x - mean) ** 2 for x in hist) / (n - 1) if n > 1 else 0.0
        _floor = _SIM_THRESH_BASE_FX if sym in _FX_INSTRUMENTS else _SIM_THRESH_BASE
        base_thresh = max(_floor, min(_SIM_THRESH_CAP, var ** 0.5 * mult))
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
def _sim_get_tp(sym: str, direction: str, conviction: int = 5) -> float:
    """Dynamic TP — conviction-scaled fractions and cap.
    Vol_history values are in percent; win/loss_moves are fractions (pnl_pct scale).
    conviction=1: 0.5% cap; conviction=10: 3.0% cap. Takes max of estimates."""
    combo  = f"{sym}_{direction}"
    wins   = (_sim.get("win_moves")  or {}).get(combo, [])
    losses = (_sim.get("loss_moves") or {}).get(combo, [])
    hist   = (_sim.get("vol_history") or {}).get(sym, [])

    t        = (conviction - 1) / 9.0
    tp_cap   = 0.005 + 0.025 * t          # 0.5% (conv=1) → 3.0% (conv=10)
    # Barbie may set per-instrument tp_win_fraction via barbie_june_overrides (entry-baseline path)
    _brb_tp_frac = _barbie_overrides.get(sym, {}).get("tp_win_fraction")
    if _brb_tp_frac is not None:
        _brb_tp_frac = max(_BARBIE_TP_FRAC_MIN, min(_BARBIE_TP_FRAC_MAX, float(_brb_tp_frac)))
    else:
        _brb_tp_frac = _SIM_TP_WIN_FRACTION
    win_frac = min(_BARBIE_TP_FRAC_MAX, _brb_tp_frac + 0.30 * t)  # scales with conviction
    los_frac = 0.25 + 0.25 * t            # 0.25 → 0.50

    estimates = []

    # Primary: winning trade history — fraction scales up with conviction
    if wins:
        fraction = win_frac if len(wins) >= _SIM_TP_MIN_SAMPLES else 0.80
        estimates.append((sum(wins) / len(wins)) * fraction)

    # Secondary: conviction-scaled fraction of avg losing move
    if len(losses) >= 3:
        estimates.append((sum(losses) / len(losses)) * los_frac)

    # Tertiary: 30% of vol_mean (vol_history in % — divide /100 to get fraction)
    if len(hist) >= 5:
        estimates.append((sum(hist) / len(hist)) / 100.0 * _SIM_TP_VOL_FRACTION)

    if estimates:
        return round(max(_SIM_TP_FLOOR, min(tp_cap, max(estimates))), 6)

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
    # Barbie may set per-instrument stop_vol_mult via barbie_june_overrides (entry-baseline path)
    _brb_mult = _barbie_overrides.get(sym, {}).get("stop_vol_mult")
    if _brb_mult is not None:
        _brb_mult = max(_BARBIE_STOP_MULT_MIN, min(_BARBIE_STOP_MULT_MAX, float(_brb_mult)))
    else:
        _brb_mult = _SIM_STOP_VOL_MULT
    stop_p = mean_p + _brb_mult * (var_p ** 0.5)   # still in percent
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
            if sym in ("EURUSD", "GBPUSD") and direction == "long": return 0.70
        elif "commodity bull" in note:
            if sym in ("EURUSD", "GBPUSD"):                          return 0.85
            if sym == "USDJPY":                                       return 0.80
        elif "risk-on" in note:
            if sym in ("EURUSD", "GBPUSD"):                          return 1.20
            if sym == "USDJPY" and direction == "long":              return 1.15
        elif "safe-haven" in note:
            if sym == "USDJPY" and direction == "long":              return 1.20
            if sym in ("EURUSD", "GBPUSD") and direction == "long": return 0.75
        return 1.0
    return 1.0


def _load_correlation_map() -> None:
    """Read june_correlation_map from Redis into _correlation_map.
    Graceful degradation: leaves map empty if key missing/stale (no weight adjustment).
    """
    global _correlation_map
    try:
        raw = _redis().get("june_correlation_map")
        if raw:
            _correlation_map = json.loads(raw)
            n_pairs = len(_correlation_map.get("pairs", {}))
            if n_pairs:
                print(
                    f"[{_ts()}] 🔗 Correlation map loaded: {n_pairs} pairs "
                    f"(min_r={_correlation_map.get('min_r', '?')})",
                    flush=True,
                )
        else:
            _correlation_map = {}
    except Exception:
        _correlation_map = {}


def _load_barbie_overrides() -> None:
    """Read barbie_june_overrides and barbie_june_combo_thresholds into globals.
    Graceful degradation: leaves dicts empty if key missing or Redis unavailable.
    """
    global _barbie_overrides, _barbie_combo_thresholds
    try:
        raw = _redis().get("barbie_june_overrides")
        if raw:
            _barbie_overrides = json.loads(raw)
            if _barbie_overrides:
                print(
                    f"[{_ts()}] 🎀 Barbie overrides loaded: {list(_barbie_overrides)}",
                    flush=True,
                )
        else:
            _barbie_overrides = {}
    except Exception:
        _barbie_overrides = {}
    try:
        raw_ct = _redis().get(_BARBIE_COMBO_THRESH_KEY)
        if raw_ct:
            _barbie_combo_thresholds = json.loads(raw_ct)
            if _barbie_combo_thresholds:
                print(
                    f"[{_ts()}] 🎀 Barbie combo thresholds loaded: {list(_barbie_combo_thresholds)}",
                    flush=True,
                )
        else:
            _barbie_combo_thresholds = {}
    except Exception:
        _barbie_combo_thresholds = {}



_claudia_corr_notes: dict = {}   # qualitative correlation context from Claudia's directive


def _load_claudia_corr_notes() -> None:
    """Read claudia_correlation_notes from Redis. Informational only — surfaces
    Claudia's qualitative correlation reasoning in June's logs. No numeric weighting.
    Graceful degradation: leaves dict empty if key missing or stale.
    """
    global _claudia_corr_notes
    try:
        raw = _redis().get("claudia_correlation_notes")
        if raw:
            _claudia_corr_notes = json.loads(raw).get("notes", {})
            if _claudia_corr_notes:
                for sym, note in _claudia_corr_notes.items():
                    print(
                        f"[{_ts()}] 🔗 Claudia corr note [{sym}]: {note[:80]}",
                        flush=True,
                    )
        else:
            _claudia_corr_notes = {}
    except Exception:
        _claudia_corr_notes = {}


_claudia_directive_notes: dict = {}  # session_directive snapshot for soft conviction signal


# June-instrument → Claudia sector labels for avoid/thesis mapping
_JUNE_SECTOR_MAP: dict = {
    "GOLD":   {"Gold", "Metals", "Mining"},
    "SILVER": {"Silver", "Metals", "Mining"},
    "EURUSD": {"FX", "Dollar", "USD"},
    "USDJPY": {"FX", "Dollar", "USD"},
}


def _load_claudia_directive_notes() -> None:
    """Read session_directive from Redis into _claudia_directive_notes.
    Extracts avoid, thesis_sectors, high_conviction, confidence, june_notes.
    Graceful degradation: leaves dict empty if key missing or stale.
    Called each cycle alongside _load_claudia_corr_notes().
    """
    global _claudia_directive_notes
    try:
        raw = _redis().get("session_directive")
        if not raw:
            _claudia_directive_notes = {}
            return
        _d = json.loads(raw)
        _claudia_directive_notes = {
            "avoid":          _d.get("avoid", []),
            "thesis_sectors": _d.get("thesis_sectors", []),
            "high_conviction": _d.get("high_conviction", []),
            "confidence":     _d.get("confidence", "low"),
            "june_notes":     _d.get("june_notes", {}),
        }
    except Exception:
        _claudia_directive_notes = {}


def _slow_grind_pts(sym: str, direction: str) -> float:
    """Cumulative slow-grind conviction boost (max +_GRIND_MAX_PTS = 1.0 pt).

    Detects concentrated directional momentum over the last _GRIND_WINDOW
    one-minute cycles using the existing _history deque. No new data source.

    Fires when ALL three hold:
      1. |net_sum| >= _GRIND_THRESH[sym]  -- sufficient cumulative move
      2. consistent_fraction >= 0.75      -- >=3 of 4 cycles same direction
      3. net_sum sign matches direction    -- correct direction only

    Distinct from persistence_confirmed: that flag checks ONE prior cycle for
    direction agreement. This function requires BOTH magnitude (net_sum
    threshold) AND consistency (>=75%) across a 4-cycle window.

    Additive only -- never a gate. spread_alert, HYBRID spread gate, and
    SAR/perf-block all override this boost unconditionally.
    Returns 0.0 for instruments without a configured threshold.
    """
    if sym not in _GRIND_THRESH:
        return 0.0
    hist = _history.get(sym)
    if not hist or len(hist) < _GRIND_WINDOW + 1:
        return 0.0
    prices = [px for _, px in hist]
    recent = prices[-(_GRIND_WINDOW + 1):]   # last N+1 readings -> N changes
    changes = [
        (recent[i + 1] - recent[i]) / recent[i] * 100.0
        for i in range(_GRIND_WINDOW)
        if recent[i] > 0
    ]
    if len(changes) < _GRIND_WINDOW:
        return 0.0
    net_sum = sum(changes)
    same_dir = sum(
        1 for c in changes
        if (c > 0 and direction == 'long') or (c < 0 and direction == 'short')
    )
    consistent_fraction = same_dir / len(changes)
    target_sign = 1 if direction == 'long' else -1
    if (
        abs(net_sum) >= _GRIND_THRESH[sym]
        and consistent_fraction >= _GRIND_CONSISTENCY
        and net_sum * target_sign > 0
    ):
        return _GRIND_MAX_PTS
    return 0.0


def _exhaustion_ratio(sym: str, direction: str) -> float:
    """Ratio of net directional price move (full _history window) to current ATR_5m.

    Measures how much of the current trend has already played out before entry.
      0.0   — price moved against our direction (fresh entry, no exhaustion)
      >0    — price already moved in our direction; larger = more exhausted
      >=3.5 — chasing: blocked by live entry pipeline
      2.5–3.5 — conviction reduced 1pt

    Uses the same _history deque and _compute_atr_5m() as the HYBRID spread gate
    and slow-grind detector — no new data source.

    Three distinct, complementary signal-quality checks:
      persistence_confirmed — single prior-cycle direction agreement
      _slow_grind_pts       — sustained multi-cycle consistency + magnitude
      _exhaustion_ratio     — total elapsed move vs ATR (chasing detection)
    """
    hist = _history.get(sym)
    if not hist or len(hist) < 6:
        return 0.0   # warming up — skip check
    atr_5m, _ = _compute_atr_5m(sym)
    if not atr_5m or atr_5m == 0.0:
        return 0.0
    prices = [px for _, px in hist]
    net_move = prices[-1] - prices[0]
    # Sign in direction of trade: positive = price already moved our way
    net_signed = net_move if direction == "long" else -net_move
    if net_signed <= 0.0:
        return 0.0   # price moved against direction — no exhaustion for this trade side
    return net_signed / atr_5m


def _sim_claudia_pts(sym: str, direction: str) -> float:
    """Soft conviction adjustment from Claudia's session directive.
    Bounded to ±0.3 (below _SIM_WKND_MAX_PTS=1.0 peer ceiling).
    Returns 0.0 on missing/stale directive or unmapped instrument.
    Never a gate — additive only.
    """
    if not _claudia_directive_notes:
        return 0.0
    _MAX = 0.3
    _avoid          = _claudia_directive_notes.get("avoid", [])
    _thesis_sectors = _claudia_directive_notes.get("thesis_sectors", [])
    _confidence     = _claudia_directive_notes.get("confidence", "low")
    _scale = {"high": 1.0, "medium": 0.67, "low": 0.33}.get(_confidence, 0.33)
    _sym_sectors = _JUNE_SECTOR_MAP.get(sym, set())
    # Direct symbol avoid — strongest signal
    if sym in _avoid:
        return round(-_MAX * _scale, 3)
    # Sector-level avoid — half weight (avoids are Miss-Secretary-centric)
    if _sym_sectors and any(s in _sym_sectors for s in _avoid):
        return round(-_MAX * _scale * 0.5, 3)
    # Thesis sector alignment — direction-aware
    # thesis_sectors is bullish context; only boost the aligned (long) direction.
    # Short in a thesis sector gets 0.0: no penalty (thesis != avoid) but no boost.
    if _sym_sectors and any(s in _sym_sectors for s in _thesis_sectors):
        if direction == "long":
            return round(_MAX * _scale * 0.5, 3)
        return 0.0  # short opposes bullish thesis — no boost, not penalised
    return 0.0


def _sim_check_barbie_alarm(signals: dict) -> None:
    """Compare live vol against what Barbie assumed when she set stop_vol_mult overrides.
    Writes barbie_alarm to Redis if deviation exceeds threshold. Pure flag — never blocks
    or modifies any trade decision. Called every 60-second cycle from run_simulation_step().
    """
    try:
        for sym in list(_sim_eligible):
            override  = _barbie_overrides.get(sym, {})
            assumed   = override.get("vol_assumed_pct")    # recorded at Barbie reasoning time
            mult      = override.get("stop_vol_mult")      # alarm only meaningful if override active
            if assumed is None or mult is None or assumed <= 0:
                continue
            threshold = float(override.get("vol_alarm_threshold", _BARBIE_ALARM_DEFAULT_THRESHOLD))
            hist = (_sim.get("vol_history") or {}).get(sym, [])
            if len(hist) < 5:
                continue
            current_vol = sum(hist) / len(hist)
            deviation   = abs(current_vol - assumed) / assumed
            if deviation > threshold:
                alarm_payload = {
                    "instrument":       sym,
                    "reason":           (
                        f"vol_mean deviated {deviation:+.0%} from assumed "
                        f"({assumed:.4f}% -> {current_vol:.4f}%)"
                    ),
                    "vol_current_pct":  round(current_vol, 6),
                    "vol_assumed_pct":  round(assumed, 6),
                    "stop_mult_active": mult,
                    "set_at":           int(time.time()),
                }
                _redis().set(_BARBIE_ALARM_KEY, json.dumps(alarm_payload), ex=_BARBIE_ALARM_TTL)
                print(
                    f"[{_ts()}] \U0001f514 Barbie alarm: {sym} vol deviated {deviation:.0%} "
                    f"from assumed ({assumed:.4f}% -> {current_vol:.4f}%) -- Barbie notified",
                    flush=True,
                )
                break   # one alarm at a time; Barbie re-evaluates all on wake
    except Exception:
        pass   # alarm is informational; never crash the cycle


def _sim_check_pnl_alarm() -> None:
    """Check session P&L drawdown against Barbie's noise-tolerance threshold.

    Writes barbie_alarm (alarm_type=pnl_drawdown) if drawdown from session start
    exceeds Barbie's threshold. Reuses the same Redis key as vol-deviation alarms —
    one alarm at a time; Barbie reads and consumes on next poll.

    Bounds enforced on read of barbie_pnl_threshold (clamped server-side):
      CEIL  = -5%  (tightest: 5 max-stop trades at full leverage)
      FLOOR = -13% (loosest: 65% of June's -20% auto-reset budget)
    """
    try:
        balance      = _sim.get("balance", _SIM_START_BALANCE)
        session_base = _sim.get("stage_entry_balance", _SIM_START_BALANCE)
        if session_base <= 0:
            return

        # Read Barbie's threshold from Redis; clamp to safe bounds regardless of what she sent
        try:
            raw_th = _redis().get(_BARBIE_PNL_THRESHOLD_KEY)
            barbie_th = float(raw_th) if raw_th else _PNLWAKE_DEFAULT_PCT
        except Exception:
            barbie_th = _PNLWAKE_DEFAULT_PCT
        threshold = max(_PNLWAKE_FLOOR_PCT, min(_PNLWAKE_CEIL_PCT, barbie_th))

        drawdown_pct = (balance - session_base) / session_base
        if drawdown_pct >= threshold:
            return   # within tolerance

        alarm_payload = {
            "alarm_type":    "pnl_drawdown",
            "reason":        (
                f"session P&L drawdown {drawdown_pct:+.1%} crossed threshold {threshold:+.1%} "
                f"(balance ${balance:.2f}, session_base ${session_base:.2f})"
            ),
            "drawdown_pct":  round(drawdown_pct, 4),
            "threshold_pct": round(threshold, 4),
            "balance":       round(balance, 2),
            "session_base":  round(session_base, 2),
            "set_at":        int(time.time()),
        }
        _redis().set(_BARBIE_ALARM_KEY, json.dumps(alarm_payload), ex=_BARBIE_ALARM_TTL)
        print(
            f"[{_ts()}] 🔴 Barbie P&L alarm: session drawdown {drawdown_pct:+.1%} "
            f"crossed {threshold:+.1%} threshold (${balance:.2f}) -- Barbie notified",
            flush=True,
        )
    except Exception:
        pass   # alarm is informational; never crash the cycle


def _sim_apply_pos_adjust() -> None:
    """Apply Barbie's barbie_june_pos_adjust to open position(s) -- sim AND live.

    TWO STRUCTURALLY SEPARATE PATHS -- this is the LIVE-POSITION path only:
      - Reads: barbie_june_pos_adjust  (distinct Redis key from entry-baseline)
      - Writes: _sim["open_position"] AND _live["open_position"] stop_pct / tp_pct
      - Called from: _sim_check_exit() -- exit evaluation time only
      - Single Redis read+delete per cycle; sim runs before live so this function
        is the sole consumer of the key -- live side never needs a separate read.

    The ENTRY-BASELINE path is entirely separate and does not share any code:
      - Reads: barbie_june_overrides -> _barbie_overrides global
      - Applied in: _sim_get_dynamic_stop() / _sim_get_tp() at entry time
      - Called from: _sim_try_entry() only
      - The tighten-only restriction on this function cannot physically affect
        that path because this function is never called from _sim_try_entry().

    Stop-loss: TIGHTEN ONLY for open position (new_stop_pct < curr_stop_pct required).
               Enforced identically for both sim and live sides.
    TP: either direction within [_SIM_TP_FLOOR, _SIM_TP_CAP].
    """
    try:
        # GETDEL: atomic read-and-delete prevents a Barbie NX write racing the
        # old GET+DELETE window. If nothing is pending, returns None immediately.
        raw = _redis().getdel(_BARBIE_POS_ADJUST_KEY)
        if not raw:
            return
        adj     = json.loads(raw)
        adj_sym = adj.get("instrument")

        sim_pos  = _sim.get("open_position")
        live_pos = _live.get("open_position") if _live else None

        # Nothing open anywhere -- clean up stale key
        if not sim_pos and not live_pos:
            _redis().delete(_BARBIE_POS_ADJUST_KEY)
            return

        sim_match  = bool(sim_pos  and sim_pos.get("instrument")  == adj_sym)
        live_match = bool(live_pos and live_pos.get("instrument") == adj_sym)

        # Key targets a different instrument than every open position -- keep for later
        if not sim_match and not live_match:
            return

        sim_changed = live_changed = False

        # ── Apply to sim position ───────────────────────────────────────────────────────────────────────────────────
        if sim_match:
            sym  = adj_sym
            dirn = sim_pos.get("direction")
            fill = sim_pos.get("fill_price", 0)
            curr_stop_pct = sim_pos.get("stop_pct", 0.0)
            curr_tp_pct   = sim_pos.get("tp_pct",   0.0)

            new_stop_pct = adj.get("stop_pct")
            if new_stop_pct is not None:
                new_stop_pct = max(_SIM_STOP_FLOOR, min(_SIM_STOP_CAP, float(new_stop_pct)))
                if new_stop_pct < curr_stop_pct:
                    _sim["open_position"]["stop_pct"]   = new_stop_pct
                    _sim["open_position"]["stop_price"] = (
                        fill * (1 - new_stop_pct) if dirn == "long"
                        else fill * (1 + new_stop_pct)
                    )
                    print(
                        f"[{_ts()}] 🎀 Barbie pos-adjust [sim]: {sym} stop "
                        f"{curr_stop_pct*100:.3f}% -> {new_stop_pct*100:.3f}% (tightened)",
                        flush=True,
                    )
                    sim_changed = True
                else:
                    print(
                        f"[{_ts()}] ⚠️  Barbie pos-adjust REJECTED loose stop for {sym} [sim]: "
                        f"{new_stop_pct*100:.3f}% >= current {curr_stop_pct*100:.3f}% "
                        f"-- live-position stop may only tighten (baseline mult still free)",
                        flush=True,
                    )

            new_tp_pct = adj.get("tp_pct")
            if new_tp_pct is not None:
                new_tp_pct = max(_SIM_TP_FLOOR, min(_SIM_TP_CAP, float(new_tp_pct)))
                _sim["open_position"]["tp_pct"]   = new_tp_pct
                _sim["open_position"]["tp_price"] = (
                    fill * (1 + new_tp_pct) if dirn == "long"
                    else fill * (1 - new_tp_pct)
                )
                print(
                    f"[{_ts()}] 🎀 Barbie pos-adjust [sim]: {sym} TP "
                    f"{curr_tp_pct*100:.3f}% -> {new_tp_pct*100:.3f}%",
                    flush=True,
                )
                sim_changed = True

        # ── Apply to live position (same tighten-only stop rule, same TP bounds) ─────────────────────────────────────────────────────────────
        # _live_check_exit() evaluates _live["open_position"]["stop_pct"] and ["tp_pct"]
        # directly. No stop_price equivalent needed -- exit computes distance via pct.
        if live_match:
            sym  = adj_sym
            dirn = live_pos.get("direction")
            curr_stop_pct = live_pos.get("stop_pct", 0.0)
            curr_tp_pct   = live_pos.get("tp_pct",   0.0)

            new_stop_pct = adj.get("stop_pct")
            if new_stop_pct is not None:
                new_stop_pct = max(_SIM_STOP_FLOOR, min(_SIM_STOP_CAP, float(new_stop_pct)))
                if new_stop_pct < curr_stop_pct:
                    _live["open_position"]["stop_pct"] = new_stop_pct
                    _live_log(
                        f"🎀 Barbie pos-adjust [live]: {sym} stop "
                        f"{curr_stop_pct*100:.3f}% -> {new_stop_pct*100:.3f}% (tightened)"
                    )
                    live_changed = True
                else:
                    _live_log(
                        f"⚠️ Barbie pos-adjust REJECTED loose stop for {sym} [live]: "
                        f"{new_stop_pct*100:.3f}% >= current {curr_stop_pct*100:.3f}% "
                        f"-- live-position stop may only tighten"
                    )

            new_tp_pct = adj.get("tp_pct")
            if new_tp_pct is not None:
                new_tp_pct = max(_SIM_TP_FLOOR, min(_SIM_TP_CAP, float(new_tp_pct)))
                _live["open_position"]["tp_pct"] = new_tp_pct
                _live_log(
                    f"🎀 Barbie pos-adjust [live]: {sym} TP "
                    f"{curr_tp_pct*100:.3f}% -> {new_tp_pct*100:.3f}%"
                )
                live_changed = True

        # Key already deleted by GETDEL above — no second delete needed
        if sim_changed:
            _sim_save_state()
        if live_changed:
            _live_save_state()
    except Exception:
        pass   # never crash the exit cycle

def _sim_corr_weight(sym: str, direction_str: str, signals: dict) -> float:
    """Return a correlation-based weight multiplier for a sim candidate.

    For each discovered pair involving this instrument:
    - confirming (both moving as historical correlation predicts): 1.1x boost
    - diverging (moving against historical correlation): 0.7x penalty
    Penalty wins over boost when both apply. Falls back to 1.0x if no map/signal.
    """
    if not _correlation_map or not direction_str:
        return 1.0
    my_fmp    = _JUNE_TO_FMP.get(sym, sym)
    pairs     = _correlation_map.get("pairs", {})
    _fmp_to_j = {v: k for k, v in _JUNE_TO_FMP.items()}
    has_conf  = False
    has_div   = False
    for pair_data in pairs.values():
        sym_a = pair_data.get("sym_a", "")
        sym_b = pair_data.get("sym_b", "")
        if my_fmp not in (sym_a, sym_b):
            continue
        other_fmp  = sym_b if my_fmp == sym_a else sym_a
        other_june = _fmp_to_j.get(other_fmp, other_fmp)
        if other_june not in signals:
            continue
        other_dirn = signals[other_june].get("direction", "neutral")
        if other_dirn not in ("bull", "bear"):
            continue
        r_val         = pair_data.get("r", 0.0)
        my_is_bull    = (direction_str == "long")
        other_is_bull = (other_dirn == "bull")
        # Positive r: same direction = confirming; Negative r: opposite = confirming
        is_confirming = (r_val > 0) == (my_is_bull == other_is_bull)
        if is_confirming:
            has_conf = True
        else:
            has_div = True
    if has_div:
        return 0.7   # penalty wins over any boost
    if has_conf:
        return 1.1
    return 1.0


def _sim_select_instrument(signals: dict, regime: str):
    best_sym, best_vol = None, 0.0
    _sel_bal   = _sim.get("balance", _SIM_START_BALANCE)
    _sel_stage = _sim.get("stage", "sprout")
    _sel_lev   = _SIM_LEV_RANGES.get(_sel_stage, (3, 10))[1]  # ceiling: broadest eligibility
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
        if direction_str:
            _co_skip, _co_reason = _sim_combo_wr_gate(sym, direction_str)
            if _co_skip:
                _sim_log(_co_reason)
                continue
        weight        = _sim_regime_weight(sym, direction_str)
        corr_adj      = _sim_corr_weight(sym, direction_str, signals)
        effective_vol = vol * weight * corr_adj
        if weight != 1.0:
            _rw_note = next((c.get("note", "")[:22] for c in _sim_regime_three_way), "?")
            _sim_log(f"\U0001f30d {sym} {direction_str.upper()} weight x{weight:.2f}"
                     f" ({_rw_note}...) effective {effective_vol:.3f}%")
        if corr_adj != 1.0:
            _sim_log(f"\U0001f517 {sym} {direction_str.upper()} corr x{corr_adj:.1f}"
                     f" → effective {effective_vol:.3f}%")
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

    dollar_pnl   = size * lev * pnl_pct
    _partial_pnl = pos.get("partial_dollar_pnl", 0.0)
    won          = (dollar_pnl + _partial_pnl) > 0  # combined P&L for win/loss decision

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
    _co = _sim.setdefault("combo_outcomes", {})
    _co_b = _co.setdefault(_dtp_combo, [])
    _co_b.append(1 if won else 0)
    if len(_co_b) > _SIM_COMBO_WINDOW:
        _co[_dtp_combo] = _co_b[-_SIM_COMBO_WINDOW:]

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
        "pnl_pct": round(pnl_pct, 6), "dollar_pnl": round(dollar_pnl + _partial_pnl, 4),
        "hold_min": round(hold_min, 1), "exit_epoch": int(time.time()), "exit_reason": exit_reason,
        "approach": approach, "vol_bucket": vol_bkt,
        "entry_vol": pos.get("entry_vol", 0),
        "stage": _sim.get("stage", "sprout"), "phase": _sim.get("phase", 1),
        "conviction": pos.get("conviction", 5),
        "claudia_pts": pos.get("claudia_pts", 0.0),
    }
    history = _sim.setdefault("trade_history", [])
    history.append(trade_rec)
    _sim_save_trade(trade_rec)
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
    _sim_15m_record(pos["instrument"], dirn, pos.get("entry_change_15m") or 0.0, won)
    _htf_b_close = pos.get("htf_bias", "unknown")
    if _htf_b_close not in ("unknown", None):
        _live_write_htf_event(pos["instrument"], dirn, _htf_b_close, 0.0, pos["fill_price"])
    _sim_save_state()


# ── Entry ─────────────────────────────────────────────────────────────────────
def _sim_try_entry(signals: dict, regime: str, leverage: int) -> None:
    balance = _sim["balance"]
    # Build extended signals: merge fresh equity CFD snapshots (bid/offer known, <20min)
    # into the streaming dict so equity candidates enter the same selection loop.
    # Streaming prices take priority; equity CFDs only added when not already present.
    _ext = dict(signals)
    _now_ext = time.time()
    for _eq_b, _eq_d in _direct_cfd_signals.items():
        if _eq_b in _ext:
            continue
        if _now_ext - _eq_d.get("ts", 0) > 20 * 60:
            continue  # stale snapshot
        _eq_mid = _eq_d.get("mid", 0.0)
        if _eq_mid <= 0.0:
            continue  # no usable price yet from IG snapshot
        _eq_dir_raw = _eq_d.get("direction", "flat")
        _ext[_eq_b] = {
            "change_5m":    _eq_d["pct"],
            "direction":    "neutral" if _eq_dir_raw == "flat" else _eq_dir_raw,
            "spread_alert": False,
            "price":        _eq_mid,
            "spread_pct":   0.1,  # conservative placeholder; equity CFD spreads ~0.1%
            "change_15m":   None,
        }
    sym     = _sim_select_instrument(_ext, regime)
    if not sym or sym not in _ext:
        return

    # Per-sym FX weekend gate: continuous instruments (24/7 markets) bypass.
    # Fires only when _CONTINUOUS_INSTRUMENTS is non-empty (outer gate already
    # blocked for all-non-continuous case). Currently inert: set is empty.
    if sym not in _CONTINUOUS_INSTRUMENTS and is_weekend_closure():
        return

    stage    = _sim.get("stage", "sprout")
    approach = _sim_select_approach(stage, sym)
    if approach == "__all_infeasible__":
        _sim_log(
            f"Skip {sym}: all sizing approaches excluded at balance ${balance:.2f} -- "
            f"waiting for balance growth to clear IG min ${_sim_min_notional.get(sym, 0):.2f}"
        )
        return

    sig       = _ext[sym]
    chg       = sig.get("change_5m", 0.0)
    vol       = abs(chg)
    direction = "long" if chg > 0 else "short"

    # Hybrid Spread/ATR gate — tiered threshold using 5-minute ATR baseline (fail-open)
    _atr5, _atr5_fb = _compute_atr_5m(sym)
    if _atr5 is not None and _atr5 > 0:
        _sp_raw = (sig.get("spread_pct", 0.0) or 0.0) * (sig.get("price", 0.0) or 0.0) / 100.0
        _sar5   = _sp_raw / _atr5
        _thr5   = _spread_atr_threshold(sym, _atr5_fb)
        if _sar5 > _thr5:
            _sim_log(
                f"🚫 [HYBRID SPREAD GATE] {sym}: Spread/ATR(5m) ratio {_sar5:.2f} "
                f"> tier cap {_thr5:.2f} | Entry suppressed"
            )
            return

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
    change_15m   = sig.get("change_15m") or 0.0
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

    # Conviction gauge (1-10): all inputs already computed above in this function
    _cv_thresh = _sim_get_threshold(sym, direction)
    _cv_weight = _sim_regime_weight(sym, direction)
    _cv_combo  = sym + "_" + direction
    conviction = _sim_conviction_gauge(
        sym, direction, vol, _cv_thresh, _cv_weight, _cv_combo, gate_mode, rel_score
    )
    _bkt_c = "H" if vol > _SIM_HIGH_VOL_THRESH else "M" if vol >= _SIM_LOW_VOL_THRESH else "L"
    _gate_tag = "anti" if (gate_mode == "relaxed" and rel_score is not None and rel_score < 0) else gate_mode[:3]
    _sim_log(
        f"🎯 SIM: {sym} {direction.upper()} conviction {conviction}/10 "
        f"(clr={vol / _cv_thresh:.1f}x wt={_cv_weight:.2f} "
        f"bkt={_bkt_c} 15m={_gate_tag})"
    )

    stage     = _sim.get("stage", "sprout")
    leverage  = _sim_conviction_leverage(stage, conviction)

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
        pos_size = min(_sim_position_size(balance, approach, conviction), balance)
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

    prices  = _sim_reconstruct_prices(sym, _ext)
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
    tp_pct   = _sim_get_tp(sym, direction, conviction)
    # Ensure TP covers at least 1x baseline spread (entry fill is at offer/bid, so
    # the price must move >= 1x spread from mid before TP becomes reachable).
    _tp_spread_floor = _sim_get_spread_floor(sym) / 2   # spread_floor = 2x spread -> half = 1x
    if tp_pct < _tp_spread_floor:
        tp_pct = _tp_spread_floor
    tp_px    = fill * (1 + tp_pct)   if direction == "long" else fill * (1 - tp_pct)
    _tp_wins = (_sim.get("win_moves") or {}).get(sym + "_" + direction, [])
    _tp_src  = (f"avg_win {(sum(_tp_wins)/len(_tp_wins))*100:.3f}% (conv={conviction})"
                if _tp_wins else "vol/loss-history")
    _stop_tag = "spread-adj" if stop_pct > _stop_vol else "vol-dynamic"
    _sim_log(f"\U0001f4ca {sym} {direction.upper()} TP {tp_pct*100:.3f}% ({_tp_src}) | "
             f"stop {stop_pct*100:.3f}% ({_stop_tag})")
    vbkt    = _sim_vol_bucket(vol)
    notl    = round(pos_size * leverage, 2)

    _htf_b_sim, _, _ = _compute_htf_alignment(sym, direction)
    _sim["open_position"] = {
        "instrument": sym, "direction": direction,
        "fill_price": fill, "stop_price": stop_px, "tp_price": tp_px,
        "size": pos_size, "leverage": leverage,
        "entry_time": time.time(), "entry_vol": vol,
        "vol_bucket": vbkt, "approach": approach, "regime": regime,
        "entry_change_15m": change_15m, "tp_pct": tp_pct, "stop_pct": stop_pct,
        "conviction": conviction,
        "initial_sl_pct": stop_pct,  # baseline for time-decay SL compression
        "claudia_pts": _sim_claudia_pts(sym, direction),
        "htf_bias": _htf_b_sim,
    }

    action = "BUY" if direction == "long" else "SELL"
    _sim_log(
        f"{action}: {sym} @ {fill:.6g} | size ${pos_size:.2f} | "
        f"{leverage}:1 lev | notional ${notl:.0f} | vol {vol:.2f}% | {approach}"
    )
    _sim_save_state()


# ── Exit checks ───────────────────────────────────────────────────────────────
def _sim_partial_tp_exit(prices: dict) -> None:
    """Close 50% of position at TP; let remainder run with break-even stop.
    Balance credited immediately. Stats counted only on final close."""
    pos = (_sim.get("open_position") or {}).copy()
    if not pos:
        return
    sym   = pos["instrument"]
    dirn  = pos["direction"]
    size  = pos["size"]
    lev   = pos["leverage"]
    entry = pos["fill_price"]

    pnl_pct   = _sim_compute_pnl_pct(prices)
    half_size = round(size / 2, 2)
    if half_size < 1.0:  # too small to split -- full close
        _sim_close_position(prices, "take_profit"); return

    partial_pnl = round(half_size * lev * pnl_pct, 6)
    _sim["balance"] += partial_pnl

    # Tighten stop to break-even (entry +/- spread floor, so worst case ~flat)
    _spread_flr  = _sim_get_spread_floor(sym)
    new_stop_px  = entry * (1 + _spread_flr) if dirn == "long" else entry * (1 - _spread_flr)

    # Record partial win for TP calibration (does not increment trade counters)
    _dtp_combo = sym + "_" + dirn
    if partial_pnl > 0:
        _wm = _sim.setdefault("win_moves", {})
        _wb = _wm.setdefault(_dtp_combo, [])
        _wb.append(round(abs(pnl_pct), 6))
        if len(_wb) > _SIM_TP_WIN_WINDOW:
            _wm[_dtp_combo] = _wb[-_SIM_TP_WIN_WINDOW:]

    _sim["open_position"]["size"]               = half_size
    _sim["open_position"]["stop_price"]         = new_stop_px
    _sim["open_position"]["stop_pct"]           = _spread_flr
    _sim["open_position"]["partial_exit_done"]  = True
    _sim["open_position"]["partial_dollar_pnl"] = partial_pnl

    fill_exit = prices["bid"] if dirn == "long" else prices["ask"]
    _sim_log(
        f"✂️ SIM: {sym} PARTIAL TP -- closed ${half_size:.2f} @ {fill_exit:.6g} "
        f"(+${partial_pnl:.4f}, {pnl_pct*100:.3f}%) | remaining ${half_size:.2f} "
        f"| stop -> break-even {new_stop_px:.6g}"
    )
    _sim_save_state()


def _sim_check_exit(signals: dict, regime: str) -> None:
    pos = _sim.get("open_position")
    if not pos:
        return
    sym      = pos["instrument"]
    hold_sec = time.time() - pos["entry_time"]

    # Apply Barbie live-position adjustments (LIVE-POSITION path -- separate from entry-baseline).
    _sim_apply_pos_adjust()

    # ── Max-hold safety exit — unconditional, checked before signal-availability gates.
    # A position exceeding the time limit must always be closeable, even when its
    # streaming symbol has dropped from the signals dict (e.g. IG rate-limit at
    # startup drops FX instruments from Lightstreamer subscription). Falls back to
    # direct_cfd_signals (equity CFDs) then entry price (flat, zero P&L) so the
    # sim is never frozen by a data-availability gap in a safety mechanism.
    if hold_sec >= _SIM_MAX_HOLD_SECS:
        _mh_sig = dict(signals)
        if sym not in _mh_sig:
            _mh_cfd = _direct_cfd_signals.get(sym)
            if (_mh_cfd
                    and time.time() - _mh_cfd.get("ts", 0) <= 20 * 60
                    and _mh_cfd.get("mid", 0.0) > 0.0):
                _mh_dir = _mh_cfd.get("direction", "flat")
                _mh_sig[sym] = {
                    "change_5m": _mh_cfd["pct"],
                    "direction": "neutral" if _mh_dir == "flat" else _mh_dir,
                    "spread_alert": False,
                    "price":       _mh_cfd.get("mid", 0.0),
                    "spread_pct":  0.1,
                    "change_15m":  None,
                }
            else:
                _entry_px = pos.get("fill_price", 1.0)
                _sim_log(
                    f"⏰ {sym}: max_hold exceeded ({hold_sec/3600:.1f}h) — "
                    f"no streaming/CFD price available; closing at entry (flat P&L)"
                )
                _mh_sig[sym] = {
                    "change_5m": 0.0, "direction": "neutral", "spread_alert": False,
                    "price": _entry_px, "spread_pct": 0.0, "change_15m": None,
                }
        _sim_close_position(_sim_reconstruct_prices(sym, _mh_sig), "max_hold")
        return

    # Extend streaming signals with equity CFD snapshots for held equity positions.
    _exit_sig = dict(signals)
    if sym not in _exit_sig:
        _eq_cx = _direct_cfd_signals.get(sym)
        if not _eq_cx:
            return
        if time.time() - _eq_cx.get("ts", 0) > 20 * 60:
            _sim_log(f"⏸ {sym}: equity CFD price stale (>20min) — deferring exit check")
            return
        _eq_mid_cx = _eq_cx.get("mid", 0.0)
        if _eq_mid_cx <= 0.0:
            return
        _eq_dir_cx = _eq_cx.get("direction", "flat")
        _exit_sig[sym] = {
            "change_5m":    _eq_cx["pct"],
            "direction":    "neutral" if _eq_dir_cx == "flat" else _eq_dir_cx,
            "spread_alert": False,
            "price":        _eq_mid_cx,
            "spread_pct":   0.1,
            "change_15m":   None,
        }
    prices   = _sim_reconstruct_prices(sym, _exit_sig)
    pnl_pct  = _sim_compute_pnl_pct(prices)
    dirn     = pos["direction"]
    sig_dir  = _exit_sig[sym].get("direction", "neutral")

    # ── Time-Decay Exit Compression ─────────────────────────────────────────────────────────────────
    # For positions active >30 min without reaching 50% TP distance:
    # compress SL 15% per 15-min window (floor 50% initial SL);
    # decay reversal patience by 1 cycle per window (floor 1).
    age_min = hold_sec / 60.0
    _tdec_windows = 0
    if age_min >= 30.0:
        _init_sl  = pos.get('initial_sl_pct', pos.get('stop_pct', _sim_get_dynamic_stop(sym)))
        _tp_val   = pos.get('tp_pct', _sim_get_tp(sym, dirn, pos.get('conviction', 5)))
        _tp_prog  = pnl_pct / _tp_val if _tp_val > 0 else 0.0
        if _tp_prog < 0.50:
            _tdec_windows = int((age_min - 30.0) / 15.0) + 1
            _compression  = max(0.50, 1.0 - 0.15 * _tdec_windows)
            _eff_sl       = _init_sl * _compression
            _fill_p       = pos.get('fill_price', 0.0)
            if _eff_sl < pos.get('stop_pct', 0.0) and _fill_p > 0:
                _new_sp = _fill_p * (1.0 - _eff_sl) if dirn == 'long' else _fill_p * (1.0 + _eff_sl)
                _sim['open_position']['stop_pct']   = _eff_sl
                _sim['open_position']['stop_price'] = _new_sp
                pos = _sim['open_position']
            _brb_td   = _barbie_overrides.get(sym, {}).get('reversal_confirm_secs')
            if _brb_td is not None:
                _cl_td    = max(_BARBIE_OVERRIDE_MIN_SECS, min(_BARBIE_OVERRIDE_MAX_SECS, int(_brb_td)))
                _base_pat = max(1, round(_cl_td / POLL_ACTIVE))
            else:
                _base_pat = _SIM_REV_PATIENCE_WIN if pnl_pct > 0 else _SIM_REV_PATIENCE_LOSS
            _rev_secs = max(1, _base_pat - _tdec_windows) * POLL_ACTIVE
            if _tdec_windows > pos.get('tdec_windows_logged', -1):
                _sim['open_position']['tdec_windows_logged'] = _tdec_windows
                pos = _sim['open_position']
                print(
                    f'[{_ts()}] ⏳ [TIME DECAY] {sym}: active for {int(age_min)}m '
                    f'without 50% TP progress. '
                    f'Compressed SL to {round(_eff_sl * 100, 3)}% and '
                    f'reversal exit wait to {_rev_secs}s.',
                    flush=True,
                )
    # ── Dynamic Profit Lock-in Engine (DPLE) ─────────────────────────────────
    # Milestone 1 (>=50% of TP): move effective_sl to breakeven + spread buffer.
    # Milestone 2 (>=75% of TP): trail at 50% of peak unrealized profit.
    # dple_effective_sl is in P&L-fraction space (same units as pnl_pct):
    #   negative = tolerate a small loss (M1 spread-buffer floor)
    #   positive = trail a profit floor (M2 half-peak trail)
    # Both milestones only RAISE the floor -- never move it backward.
    _dple_tp = pos.get('tp_pct', _sim_get_tp(sym, dirn, pos.get('conviction', 5)))
    if _dple_tp > 0:
        if pnl_pct > 0:
            _peak = max(pos.get('peak_pnl_pct', 0.0), pnl_pct)
            if _peak > pos.get('peak_pnl_pct', 0.0):
                _sim['open_position']['peak_pnl_pct'] = _peak
                pos = _sim['open_position']
        _peak    = pos.get('peak_pnl_pct', 0.0)
        _dple_sl = pos.get('dple_effective_sl', None)
        if pnl_pct >= 0.75 * _dple_tp:
            _trail_sl = 0.5 * _peak
            if _dple_sl is None or _trail_sl > _dple_sl:
                _sim['open_position']['dple_effective_sl'] = _trail_sl
                _sim['open_position']['breakeven_locked']  = True
                pos = _sim['open_position']
                _dple_sl = _trail_sl
                _sim_log(
                    f'💰 [PROFIT TRAIL] {sym}: Locked in 50% of peak profit '
                    f'at {_trail_sl*100:.3f}%% floor'
                )
        elif pnl_pct >= 0.5 * _dple_tp and not pos.get('breakeven_locked'):
            _spread_buf = _sim_get_spread_floor(sym)
            _be_sl = -_spread_buf
            if _dple_sl is None or _be_sl > _dple_sl:
                _sim['open_position']['dple_effective_sl'] = _be_sl
                _sim['open_position']['breakeven_locked']  = True
                pos = _sim['open_position']
                _dple_sl = _be_sl
            _sim_log(f'🛡️ [BREAKEVEN LOCK] {sym}: 50% TP distance reached. Stop moved to entry.')
        if _dple_sl is not None and pnl_pct < _dple_sl:
            _sim_close_position(prices, 'dple_trail'); return

    # ── Micro-Profit Defense Engine (MPD) — sim mirror ──────────────────────────
    # Arms once bid/ask clears the total transaction friction threshold above entry.
    # Thereafter enforces a ratcheting P_stop floor (pnl_pct fraction space, one-way).
    _mpd_fp = pos.get("fill_price", 0.0)
    if _mpd_fp > 0:
        _mpd_mid_s = (_exit_sig[sym].get("price", 0.0) or 0.0)
        _mpd_sp_s  = (_exit_sig[sym].get("spread_pct", 0.0) or 0.0)
        _mpd_pip_s = _live_pip_sizes.get(sym, _LIVE_FX_PIP)
        _mpd_spn_s = _mpd_mid_s * _mpd_sp_s / 100.0         # spread in native price units
        _mpd_slp_s = _MPD_SLIPPAGE_PIPS * _mpd_pip_s         # slippage buffer (native)
        _mpd_mgp_s = _MPD_MIN_PROFIT_PIPS * _mpd_pip_s       # min guaranteed profit (native)
        _mpd_fric_s = (_mpd_spn_s + _mpd_slp_s + _mpd_mgp_s) / _mpd_fp  # as pnl_pct
        if _mpd_fric_s > 0 and pnl_pct >= _mpd_fric_s:
            _p_stop_s   = (_mpd_spn_s + _mpd_mgp_s) / _mpd_fp  # P_stop as pnl_pct
            _cur_dsl_s  = pos.get("defensive_stop_level_pct", None)
            if _cur_dsl_s is None or _p_stop_s > _cur_dsl_s:   # ratchet: only raise
                _sim["open_position"]["defensive_stop_active"]    = True
                _sim["open_position"]["defensive_stop_level_pct"] = _p_stop_s
                pos = _sim["open_position"]
                _sim_log(
                    f"\U0001f6e1\ufe0f [PROFIT DEFENSE] {sym}: Micro-profit floor armed at "
                    f"{_p_stop_s*100:.3f}% P&L"
                )
        if pos.get("defensive_stop_active"):
            _dsl_s = pos.get("defensive_stop_level_pct", 0.0)
            if _dsl_s and pnl_pct < _dsl_s:
                _sim_close_position(prices, "mpd_floor")
                return

    # Hard exits — instrument-calibrated thresholds (stored at entry, fallback to live calc)
    stop_pct = pos.get("stop_pct", _sim_get_dynamic_stop(sym))
    if pnl_pct <= -stop_pct:
        _sim_close_position(prices, "stop_loss"); return
    if pnl_pct >= pos.get("tp_pct", _sim_get_tp(sym, dirn, pos.get("conviction", 5))):
        if not pos.get("partial_exit_done"):
            _sim_partial_tp_exit(prices); return
        _sim_close_position(prices, "take_profit"); return
    if hold_sec >= _SIM_MAX_HOLD_SECS:
        _sim_close_position(prices, "max_hold"); return

    # Asymmetric reversal: losers cut on first opposing cycle, winners need confirmation
    opposing = (dirn == "long"  and (regime == "bear" or sig_dir == "bear")) or \
               (dirn == "short" and (regime == "bull"  or sig_dir == "bull"))
    if opposing:
        _brb = _barbie_overrides.get(sym, {}).get("reversal_confirm_secs")
        if _brb is not None:
            _raw = int(_brb)
            _clamped = max(_BARBIE_OVERRIDE_MIN_SECS,
                           min(_BARBIE_OVERRIDE_MAX_SECS, _raw))
            if _clamped != _raw:
                print(
                    f"[{_ts()}] ⚠️  Barbie override clamped for {sym}: "
                    f"reversal_confirm_secs {_raw} → {_clamped} "
                    f"(bounds [{_BARBIE_OVERRIDE_MIN_SECS}, {_BARBIE_OVERRIDE_MAX_SECS}])",
                    flush=True,
                )
            patience = max(1, round(_clamped / POLL_ACTIVE))
        else:
            patience = _SIM_REV_PATIENCE_WIN if pnl_pct > 0 else _SIM_REV_PATIENCE_LOSS
        patience = max(1, patience - _tdec_windows)  # time-decay window decay
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
            _15m_n   = len((_sim.get("15m_reliability") or {}).get(sym_sa, {}).get(dir_sa, {}).get("history", []))
            gate_tag = f"15m:{gmode_sa}/{grel_str}(n={_15m_n})"
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
            "combo_outcomes":   _sim.get("combo_outcomes", {}),
            "failure_snapshot": _failure_snapshot,
        }
        r.set("june_sim_calibration", json.dumps(_cal), ex=90 * 24 * 3600)
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
        _24h_co      = {k: list(v) for k, v in (_sim.get("combo_outcomes") or {}).items()}
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
            "combo_outcomes":       _24h_co,
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
                "combo_outcomes":   _sim.get("combo_outcomes", {}),
                "failure_snapshot": _floor_fail_snap,
            }
            _r_bf.set("june_sim_calibration", json.dumps(_cal_bf), ex=90 * 24 * 3600)
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
    _load_correlation_map()
    _load_barbie_overrides()
    _load_claudia_corr_notes()
    _load_claudia_directive_notes()
    _refresh_live_kill_switch()
    _sim_check_barbie_alarm(signals)   # flag-only; never blocks trade logic
    _sim_check_pnl_alarm()             # P&L drawdown alarm; reuses barbie_alarm key

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
            _fail_cal_co  = {k: list(v) for k, v in (_sim.get("combo_outcomes") or {}).items()}
            _fail_cal_15m = _sim.get("15m_reliability", {})
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
            # vol_history / win_moves / loss_moves / 15m_reliability carry forward so each retry
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
                "15m_reliability":      _fail_cal_15m,
                "vol_history":          _fail_cal_vol,
                "win_moves":            _fail_cal_win,
                "loss_moves":           _fail_cal_los,
                "combo_outcomes":       _fail_cal_co,
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
    # If any 24/7 instruments are registered, skip this outer gate and let
    # _sim_try_entry apply a per-sym check instead.
    if not _CONTINUOUS_INSTRUMENTS and is_weekend_closure():
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
    global _sim_eligible, _sim_min_notional, _notional_pending

    # Extend reverse map to cover direct_cfd_map epics (Finnhub fallback in backfill).
    # _load_direct_cfd_cache() runs before sim_startup(), so _direct_cfd_map is ready.
    _INSTRUMENTS_REVERSE.update({epic: sym for sym, epic in _direct_cfd_map.items()
                                 if sym not in INSTRUMENTS})

    # Pre-populate from hardcoded known values and persisted Redis key (Parts 1+4).
    # Runs before sim_state load — FX+Silver are always immediately available.
    _sim_min_notional.update(_KNOWN_MIN_NOTIONALS)
    try:
        _persist_raw = _redis().get(_NOTIONAL_REDIS_KEY)
        if _persist_raw:
            import json as _json
            _sim_min_notional.update(_json.loads(_persist_raw))
    except Exception:
        pass
    for _s in list(_sim_min_notional):
        _sim_eligible.add(_s)

    saved = _sim_load_state()
    if saved:
        # Merge saved min_notionals into pre-populated dict.
        # API-confirmed values from prior runs override hardcoded estimates.
        _sim_min_notional.update(saved.get("min_notionals", {}))
        if _persist_raw:  # re-apply: Redis overrides beat saved API-discovered values
            _sim_min_notional.update(_json.loads(_persist_raw))
        _sim_eligible = set(saved.get("eligible_instruments", []))
        for _s in _sim_min_notional:
            _sim_eligible.add(_s)
        # Safety: re-assert hardcoded known values — Redis/saved-state may carry stale overrides
        _sim_min_notional.update(_KNOWN_MIN_NOTIONALS)

        # Queue truly unknown instruments for gradual background backfill (Part 3).
        # No API calls at startup — _advance_notional_backfill() drains 2/cycle.
        # Includes direct_cfd_map entries (Option A) to grow pool beyond INSTRUMENTS.
        _notional_pending[:] = (
            [(s, INSTRUMENTS[s]) for s in INSTRUMENTS if s not in _sim_min_notional]
            + [(s, _direct_cfd_map[s]) for s in _direct_cfd_map
               if s not in INSTRUMENTS and s not in _sim_min_notional]
        )
        if _notional_pending:
            print(
                f"[{_ts()}] 🧪 SIM: Queued {len(_notional_pending)} instruments for "
                f"background backfill: {[s for s, _ in _notional_pending]}",
                flush=True,
            )

        now = time.time()

        # One-time fresh reset: wipe state if saved before _SIM_RESET_AFTER, preserve vol/move data
        if saved.get("sim_start_time", 0) < _SIM_RESET_AFTER:
            print(
                f"[{_ts()}] 🔄 SIM FRESH START: resetting for new session "
                f"(saved state predates {datetime.fromtimestamp(_SIM_RESET_AFTER, tz=timezone.utc).strftime('%Y-%m-%d')} cutoff) "
                f"— preserving vol_history, win_moves, loss_moves, 15m_reliability",
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
                "15m_reliability":      saved.get("15m_reliability", {}),
                "vol_history":          saved.get("vol_history", {}),
                "win_moves":            saved.get("win_moves", {}),
                "loss_moves":           saved.get("loss_moves", {}),
                "failure_snapshot":     None,
                "failure_context_checked": True,
                "approach_skip_counts": {},
            })
            _hist = _sim_load_trade_history()
            if _hist:
                _sim["trade_history"] = _hist
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
    # Re-assert hardcoded known values — Redis NOTIONAL_REDIS_KEY may have overwritten them above
    _sim_min_notional.update(_KNOWN_MIN_NOTIONALS)
    # No startup API burst — _advance_notional_backfill() drains 2/cycle.
    # Includes direct_cfd_map entries (Option A) to grow pool beyond INSTRUMENTS.
    _notional_pending[:] = (
        [(s, INSTRUMENTS[s]) for s in INSTRUMENTS if s not in _sim_min_notional]
        + [(s, _direct_cfd_map[s]) for s in _direct_cfd_map
           if s not in INSTRUMENTS and s not in _sim_min_notional]
    )
    print(
        f"[{_ts()}] 🧪 SIM: {len(_sim_min_notional)} min notionals pre-loaded "
        f"({list(_sim_min_notional)}). "
        f"Queued {len(_notional_pending)} remaining for background backfill.",
        flush=True,
    )

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
            _sim["vol_history"]      = _cal.get("vol_history", {})
            _sim["win_moves"]        = _cal.get("win_moves", {})
            _sim["loss_moves"]       = _cal.get("loss_moves", {})
            _sim["combo_outcomes"]   = _cal.get("combo_outcomes", {})
            _snap_from_cal           = _cal.get("failure_snapshot")
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
    _hist = _sim_load_trade_history()
    if _hist:
        _sim["trade_history"] = _hist
        print(f"[{_ts()}] 🧪 SIM: Restored {len(_hist)} trade records from prior runs", flush=True)
    print(
        f"[{_ts()}] 🔄 SIM: Fresh start — spread-aware stops, performance-based phases, "
        f"Silver prioritized. {_cal_note}",
        flush=True,
    )
    _sim_save_state()

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ══════════════════════════════════════════════════════════════════════════════
# LIVE TRADING — June v0.3
# Real-money order placement via IG live account, gated behind june_live_enabled.
#
# Architecture:
#   - Own Redis state (june_live_state), never touches june_sim_state
#   - Reuses sim's proven decision functions (eligibility, conviction, combo_wr_gate,
#     threshold, TP/stop, 15m-gate, regime_weight, corr_weight, claudia_pts)
#   - Every order-placement call starts with _live_trade_guard() — structurally
#     impossible to place a real order while june_live_enabled=false
#   - With switch OFF: full decision logic runs, logs "WOULD" actions, no real orders
#   - With switch ON:  real IG OTC orders placed, positions tracked in _live dict
#
# Kill switch: redis-cli SET june_live_enabled true   (enable)
#              redis-cli SET june_live_enabled false  (disable, default)
# ══════════════════════════════════════════════════════════════════════════════

# ── Live state ────────────────────────────────────────────────────────────────
_live: dict             = {}      # runtime state; empty = not started
_live_balance_polled_at: float = 0.0
_live_pnl_polled_at:    float  = 0.0
_last_cycle_direction: dict       = {}   # sig["direction"] per sym from prior scan cycle
_current_cycle_signals_snap: dict = {}   # signals snapshot from current cycle
_live_spread_block_cooldown: dict = {}   # sym -> last_logged_ts (5-min dedup for block log)
_htf_ready_alerted: bool = False         # review-readiness alert: fires at most once per process
_htf_calib_last_n:  int  = 0             # N events at last calibration; re-runs every +10 new

_LIVE_REDIS_KEY      = "june_live_state"
_LIVE_REDIS_TTL      = 30 * 24 * 3600  # 30 days
_LIVE_POLL_INTERVAL  = 300             # fetch balance + P&L every 5 minutes
_LIVE_LOT_SIZE_FX    = 10.0           # lotSize for all verified FX pairs (EURUSD/NZDUSD/etc.)
_LIVE_FX_PIP         = 0.0001         # 1 pip for 4-decimal-place FX pairs
_LIVE_JPY_PIP        = 0.01           # 1 pip for JPY pairs (2 decimal places)
_LIVE_MARGIN_FACTOR  = 0.02           # 2% IG retail margin requirement for FX CFDs

# Skim milestones — hardcoded, no other numbers
_LIVE_SKIM_PHASE1_TRIGGER = 300.0   # first skim fires at $300 cumulative earned P&L
_LIVE_SKIM_PHASE1_AMOUNT  = 100.0   # $100 increments until $500
_LIVE_SKIM_PHASE2_TRIGGER = 500.0   # switch to half-mode at $500
_LIVE_SKIM_HALF_MIN_GAP   = 3600    # half-mode: don't re-flag more often than hourly
_SKIM_ENABLED             = False   # kill-switch: set True to re-enable skim (re-evaluate at ~$443 sizing crossover)

# == Pyramid configuration ==================================================
# No-decay pyramid: all legs same minDeal-clamped size.
# Profit gate 0.15%: ~10pts SILVER = 2.5x spread (4pt). Real profit confirmed.
# Aggregate stop 0.5%: ~33pts SILVER. Software check every cycle (belt+suspenders).
# Max legs 2 = 1 primary + 1 addon. 2 deal_ids tracked; margin ~$4.34 of $49.
# Leg 3 unlocks after _PYRAMID_3LEG_THRESHOLD confirmed 2-leg completions (Redis counter).
# Leg 4 unlocks after _PYRAMID_4LEG_THRESHOLD confirmed 2-leg completions.
# All unlock logic in _pyramid_active_max_legs() -- fails safe to 2 on any error.
_PYRAMID_MAX_LEGS        = 2       # current hard floor (code default; counter can raise to 4)
_PYRAMID_HARD_MAX_LEGS   = 4       # absolute ceiling -- counter never raises beyond this
_PYRAMID_PROFIT_GATE_PCT = 0.0015  # +0.15% from fill before adding each leg
_PYRAMID_AGG_STOP_PCT    = 0.005   # 0.5% from blended entry -- aggregate stop
_PYRAMID_UNLOCK_KEY      = "june_pyramid_2leg_completions"  # Redis incr counter
_PYRAMID_3LEG_THRESHOLD  = 10     # completions needed to unlock leg 3
_PYRAMID_4LEG_THRESHOLD  = 20     # completions needed to unlock leg 4
_PYRAMID_3LEG_SIZE_DECAY = 0.75   # leg 3 notional = leg2_notional * 0.75
_PYRAMID_4LEG_SIZE_DECAY = 0.50   # leg 4 notional = leg2_notional * 0.50

# Daily drawdown circuit breaker for the live account.
# Derivation: max per-trade loss = 10% position × 10× leverage × 0.5% max stop = 0.5%/trade.
# Reaching -5% needs 10 consecutive max-stop exits — structurally impossible in one session
# given the 60-second cycle and 15-minute per-combo gate. If reached, something has broken
# mechanically. Threshold is intentionally tighter than Miss Secretary's -10% equity floor
# because June's CFD stops are tighter and losses compound faster at leverage.
_LIVE_CIRCUIT_BREAKER_PCT = -0.05   # -5% daily drawdown → auto-disable kill switch
_LIVE_CB_FLOOR_USD        = 20.0
# Micro-account CB tier (<_LIVE_CB_MICRO_THRESH): tighter dollar floor calibrated
# to real per-trade loss at the $2 pos_size floor (minDeal-clamped):
#   SILVER: 0.04 contracts x 0.30% x 6936c x $1/c = $0.83/stop-out
#   OIL:    0.03 contracts x 0.30% x 9262c x $1/c = $0.83/stop-out
# Buffer = max($2.00, 30% x day_start). At $7.99: max($2.00, $2.40) = $2.40.
# 2 stop-outs: $1.66 < $2.40 (continues); 3rd: $2.49 > $2.40 (fires). Switches to full-tier at $30+.
_LIVE_CB_MICRO_THRESH    = 30.0   # balance below this -> micro-account CB tier
_LIVE_CB_MICRO_FLOOR_USD = 2.0    # micro floor: matches $2 pos_size floor
_LIVE_CB_MICRO_PCT       = 0.30   # micro pct: 30% daily drawdown — fires on 3rd stop-out
_live_api_paused_until: float = 0.0  # epoch; set on 429; all HTTP wrappers check this
# Micro-Profit Defense Engine — friction threshold and guaranteed floor
_MPD_SLIPPAGE_PIPS   = 2   # extra buffer in price points (activation gate only)
_MPD_MIN_PROFIT_PIPS = 1   # minimum guaranteed profit in points above spread

# Instrument performance filter (Fix 3)
_PERF_BLOCK_WINDOW     = 8       # rolling window of confirmed live trades
_PERF_BLOCK_WR_THRESH  = 0.30    # block if win rate < 30% over window
_PERF_BLOCK_SAR_THRESH = 0.50    # block if avg Spread/ATR ratio > 50% over window
_PERF_BLOCK_TTL             = 86400  # 24-hour block duration (seconds)
_PERF_BLOCK_SAR_SESSION_MIN = 4      # min same-session trades before SAR block fires
_PERF_SAR_OIL_BOUNDARY_FLOOR_SECS = 30 * 60  # OIL NYSE boundary: floor = 6 scan cycles
_PERF_BLOCK_RECENCY_DAYS    = 14     # only trades within this window count for WR/SAR evaluation
_PERF_BLOCK_SAR_EPOCH_CUTOFF = 1787898314  # exclude pre-a62093f trades from SAR eval (2026-08-28 06:25 UTC — OIL/SILVER sizing fix)

# Trend-exhaustion gate thresholds — derived from OIL/SILVER historical move data.
# Ratio = net directional price move (full _history window) / ATR_5m.
# Calibrated against today's 16:14-18:50 OIL cluster: clean entries 1.0–2.5×, exhausted 4.5–8.1×.
_EXHAUST_RATIO_REDUCE = 2.5   # conviction -1 when move has run > 2.5× ATR_5m in direction
_EXHAUST_RATIO_BLOCK  = 3.5   # block entry when move has run > 3.5× ATR_5m in direction

# Defect-quarantine registry — MANUALLY MAINTAINED, never auto-populated.
# Add an entry ONLY when a diagnosed+fixed code defect has already been corrected
# in this same commit; tagging without a corresponding structural fix is not permitted.
# Trades matching each window get excluded_defect_id set in Redis; SAR/WR calcs skip them.
_DEFECT_QUARANTINE: list = [
    {
        "sym":       "OIL",
        "defect_id": "late_entry_exhaustion_2026-09-01",
        "epoch_min": 1788280100,   # 2026-09-01 16:28 UTC — first exhaustion-blocked entry
        "epoch_max": 1788281800,   # 2026-09-01 16:55 UTC — last defect-pattern exit
        "note": (
            "4 OIL trades caused by trend-exhaustion code defect (structural entry lag "
            "+ immediate re-entry chasing exhausted moves): "
            "16:28 BUY 9355.5 (ratio~4.5x), 16:31 BUY 9376 (ratio~8.1x), "
            "16:51 SELL 9319.3 (ratio~3.7x), 16:54 BUY 9345.7 (flip into resumed uptrend). "
            "Fixed by _exhaustion_ratio() gate in same commit — these patterns are now "
            "structurally blocked before any order is placed."
        ),
    },
]
_PERF_BLOCK_MIN_RECENT      = 8      # min recent trades required before WR block can fire (= full rolling window)
_PERF_BLOCK_HARD_MIN_TRADES = 12     # stricter minimum for hard 12h block
_PERF_BLOCK_HARD_WR_THRESH  = 0.20   # hard block if WR < 20%
_PERF_BLOCK_HARD_LOSS_PCT   = 0.07   # hard block if net dollar loss > 7% of balance
_PERF_BLOCK_HARD_TTL        = 43200  # 12-hour hard block duration
_PERF_BLOCK_OBS_LIGHT_PCT   = 0.03   # observer-moderate threshold: net loss >= 3% of balance
_LIVE_MIN_CONVICTION   = 4       # minimum conviction score for live entries; sim unaffected

# Tiered defensive mode (NORMAL -> DEFENSIVE -> CB HALT) -------
# Middle layer between normal operation and the CB killswitch.
# Global trigger: half the CB buffer (fires after ~1 stop-out at micro-balance).
# Per-instrument trigger: 2 stop-outs on same instrument today.
# Recovery: balance_total returns to entry level (global) / win on sym (instrument),
#           OR 30-min time gate elapses -- whichever comes first.
# Sim path completely unaffected -- state lives in _live[], not _sim[].
_LIVE_DEF_MICRO_FLOOR_USD  = 1.00   # global defensive (micro <$30): half CB floor ($2)
_LIVE_DEF_MICRO_PCT        = 0.15   # global defensive (micro <$30): half CB pct (30%->15%)
_LIVE_DEF_FLOOR_USD        = 10.00  # global defensive (full >=30): half CB floor ($20)
_LIVE_DEF_PCT              = 0.025  # global defensive (full >=30): half CB pct (5%->2.5%)
_LIVE_DEF_INSTR_STOPOUTS   = 2      # stop-outs on one instrument before instrument-defensive
_LIVE_DEF_TIMEOUT_SECS     = 1800   # 30-min safety valve (recovery time gate)


def _live_log(msg: str) -> None:
    print(f"[{_ts()}] 🟢 LIVE: {msg}", flush=True)


def _live_write_block_log(sym: str, direction: str, gate: str, values: dict) -> None:
    """Append one structured block/reduce event to the capped Redis list. Fire-and-forget."""
    try:
        rec = json.dumps({"ts": int(time.time()), "sym": sym,
                          "direction": direction, "gate": gate, **values})
        _r = _redis()
        _r.lpush("june_live_block_log", rec)
        _r.ltrim("june_live_block_log", 0, 199)
        _r.expire("june_live_block_log", 86400)
    except Exception:
        pass


def _live_write_ex_ratio_obs(sym: str, direction: str, ex_ratio: float,
                              sar, conv: int) -> None:
    """Continuous ex_ratio + spread_atr_ratio timeseries for OIL/SILVER/NATGAS.
    Fires every evaluation cycle regardless of gate outcome.
    Observation-only; no gate, conviction, or sizing effect.
    Key: june_live_ex_ratio_log (capped 2000, 7-day TTL)."""
    try:
        # Kaufman ER: |net_displacement| / sum(|tick_changes|), range 0-1.
        # Computed from same _history deque as _exhaustion_ratio. Observation-only.
        _er_hist = _history.get(sym)
        _er = None
        if _er_hist and len(_er_hist) >= 2:
            _er_px   = [px for _, px in _er_hist]
            _er_net  = abs(_er_px[-1] - _er_px[0])
            _er_path = sum(abs(_er_px[i] - _er_px[i-1]) for i in range(1, len(_er_px)))
            _er = round(_er_net / _er_path, 3) if _er_path > 0 else 0.0
        rec = json.dumps({
            "ts":        int(time.time()),
            "sym":       sym,
            "direction": direction,
            "ex_ratio":  round(ex_ratio, 3),
            "sar":       round(sar, 4) if sar is not None else None,
            "conv":      conv,
            "er":        _er,
        })
        _r = _redis()
        _r.lpush("june_live_ex_ratio_log", rec)
        _r.ltrim("june_live_ex_ratio_log", 0, 1999)
        _r.expire("june_live_ex_ratio_log", 604800)  # 7-day TTL
    except Exception:
        pass

# ── HTF (Higher-Timeframe) Observation Pipeline ─────────────────────────────
# Observation-only: collects hourly candle data, logs [HTF DIAG], accumulates
# events for self-calibration, and emits a one-shot readiness alert.  Zero effect
# on any conviction value, gate, or trading decision anywhere in this file.

_HTF_INSTRUMENTS = {
    "OIL":    "CC.D.LCO.BMU.IP",
    "SILVER": "CS.D.CFDSILVER.BMU.IP",
    "NATGAS": "CC.D.NG.BMU.IP",
}
_HTF_NOISE_FLOOR_PROV  = 0.003   # 0.3% provisional noise floor
_HTF_SATURATION_PROV   = 0.008   # 0.8% provisional saturation
_HTF_CANDLE_TTL        = 3300    # 55-min Redis TTL for cached hourly candles
_HTF_EVENTS_KEY        = "june_htf_events"
_HTF_READY_KEY         = "june_htf_ready_alert"
_HTF_CALIB_MIN_EVENTS  = 30
_HTF_CALIB_RERUN_EVERY = 10
_HTF_CALIB_MIN_SEP     = 0.10    # 10pp aligned vs opposed WR gap required


def _live_fetch_htf_candles(sym):
    """Return last 4 hourly OHLC mid-price dicts via IG REST.
    Cached in Redis 55 min.  OBSERVATION-ONLY.
    """
    epic = _HTF_INSTRUMENTS.get(sym)
    if not epic:
        return []
    cache_key = "june_htf_candles:" + sym
    try:
        cached = _redis().get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception:
        pass
    data = _ig_live_get("/prices/" + epic,
                        params={"resolution": "HOUR", "max": 4},
                        version="3")
    if not data:
        return []
    raw = data.get("prices", [])
    if len(raw) < 2:
        return []
    candles = []
    for p in raw:
        cp  = p.get("closePrice", {})
        mid = ((cp.get("bid") or 0) + (cp.get("ask") or 0)) / 2
        if mid > 0:
            candles.append({"close": round(mid, 5), "ts": p.get("snapshotTimeUTC", "")})
    try:
        _redis().set(cache_key, json.dumps(candles), ex=_HTF_CANDLE_TTL)
    except Exception:
        pass
    return candles


def _compute_htf_alignment(sym, direction):
    """Compute HTF directional bias from last 3 hourly closes.
    Returns (htf_bias, htf_move, note).  OBSERVATION-ONLY.
    """
    candles = _live_fetch_htf_candles(sym)
    if len(candles) < 2:
        return "unknown", 0.0, "insufficient candles"
    oldest = candles[0]["close"]
    latest = candles[-1]["close"]
    if oldest <= 0:
        return "unknown", 0.0, "zero price"
    htf_move = (latest - oldest) / oldest
    if htf_move > _HTF_NOISE_FLOOR_PROV:
        htf_bias = "bull"
    elif htf_move < -_HTF_NOISE_FLOOR_PROV:
        htf_bias = "bear"
    else:
        htf_bias = "neutral"
    aligned = ((direction == "long"  and htf_bias == "bull") or
               (direction == "short" and htf_bias == "bear"))
    opposed = ((direction == "long"  and htf_bias == "bear") or
               (direction == "short" and htf_bias == "bull"))
    alignment = "aligned" if aligned else ("opposed" if opposed else "neutral/unknown")
    note = ("HTF %s %.2f%% over %dh -> %s"
            % ("bull" if htf_move >= 0 else "bear",
               abs(htf_move) * 100, len(candles) - 1, alignment))
    return htf_bias, htf_move, note


def _live_write_htf_event(sym, direction, htf_bias, htf_move_pct, entry_price):
    """Write completed-trade HTF event to Redis for calibration.  Fire-and-forget.
    OBSERVATION-ONLY.
    """
    try:
        rec = json.dumps({
            "ts":           int(time.time()),
            "sym":          sym,
            "direction":    direction,
            "htf_bias":     htf_bias,
            "htf_move_pct": round(htf_move_pct, 5),
            "entry_price":  round(entry_price, 5),
        })
        _r = _redis()
        _r.lpush(_HTF_EVENTS_KEY, rec)
        _r.ltrim(_HTF_EVENTS_KEY, 0, 499)
        _r.expire(_HTF_EVENTS_KEY, 86400 * 30)
    except Exception:
        pass


def _htf_self_calibrate():
    """Match HTF events against trade history; log WR by alignment bucket.
    NEVER modifies any conviction, gate, or trading decision.
    Emits [HTF CALIB] log and one-shot readiness alert when criteria met.
    """
    global _htf_ready_alerted, _htf_calib_last_n
    try:
        _r = _redis()
        raw_events = _r.lrange(_HTF_EVENTS_KEY, 0, -1)
        if not raw_events:
            return
        n_raw = len(raw_events)
        events = []
        for raw in raw_events:
            try:
                events.append(json.loads(raw))
            except Exception:
                pass
        trade_hist = _live.get("trade_history", []) + _sim.get("trade_history", [])
        results = {"aligned": [], "opposed": [], "neutral": []}
        matched = 0
        for ev in events:
            ev_ts  = ev.get("ts", 0)
            ev_sym = ev.get("sym", "")
            ev_dir = ev.get("direction", "")
            htf_b  = ev.get("htf_bias", "unknown")
            match = None
            for tr in trade_hist:
                if tr.get("instrument") != ev_sym or tr.get("direction") != ev_dir:
                    continue
                if abs(tr.get("exit_epoch", 0) - ev_ts) <= 1800:
                    match = tr
                    break
            if match is None:
                continue
            matched += 1
            won = match.get("dollar_pnl", 0.0) > 0
            pnl = match.get("pnl_pct", 0.0)
            aligned = ((ev_dir == "long"  and htf_b == "bull") or
                       (ev_dir == "short" and htf_b == "bear"))
            opposed = ((ev_dir == "long"  and htf_b == "bear") or
                       (ev_dir == "short" and htf_b == "bull"))
            bucket  = "aligned" if aligned else ("opposed" if opposed else "neutral")
            results[bucket].append((won, pnl))
        if matched - _htf_calib_last_n < _HTF_CALIB_RERUN_EVERY and _htf_calib_last_n > 0:
            return
        _htf_calib_last_n = matched
        _live_log("[HTF CALIB] n_raw=%d matched=%d | aligned=%d opposed=%d neutral=%d"
                  % (n_raw, matched, len(results["aligned"]),
                     len(results["opposed"]), len(results["neutral"])))
        for bucket, trades in results.items():
            if not trades:
                continue
            wr     = sum(1 for w, _ in trades if w) / len(trades)
            avg_pl = sum(p for _, p in trades) / len(trades)
            _live_log("[HTF CALIB] %s: N=%d WR=%.0f%% avg_pnl=%+.3f%%"
                      % (bucket, len(trades), wr * 100, avg_pl * 100))
        al = results["aligned"]
        op = results["opposed"]
        if (matched >= _HTF_CALIB_MIN_EVENTS
                and len(al) >= 5 and len(op) >= 5
                and not _htf_ready_alerted):
            al_wr = sum(1 for w, _ in al if w) / len(al)
            op_wr = sum(1 for w, _ in op if w) / len(op)
            if abs(al_wr - op_wr) >= _HTF_CALIB_MIN_SEP:
                al_avg = sum(p for _, p in al) / len(al)
                op_avg = sum(p for _, p in op) / len(op)
                _live_log(
                    "\U0001f514 [HTF READY FOR REVIEW] N=%d matched | "
                    "aligned_WR=%.0f%% (N=%d) vs opposed_WR=%.0f%% (N=%d) | "
                    "separation=%.0f%% | avg_pnl aligned=%+.3f%% opposed=%+.3f%%"
                    % (matched, al_wr * 100, len(al), op_wr * 100, len(op),
                       abs(al_wr - op_wr) * 100, al_avg * 100, op_avg * 100)
                )
                try:
                    _redis().set(_HTF_READY_KEY, json.dumps({
                        "ts": int(time.time()), "matched": matched,
                        "aligned_wr": round(al_wr, 4), "opposed_wr": round(op_wr, 4),
                        "al_avg_pnl": round(al_avg, 6), "op_avg_pnl": round(op_avg, 6),
                    }), ex=86400 * 7)
                except Exception:
                    pass
                _htf_ready_alerted = True
    except Exception as exc:
        _live_log("[HTF CALIB] error: %s" % exc)


# ── Redis persistence ─────────────────────────────────────────────────────────

def _live_save_state() -> None:
    try:
        _redis().set(_LIVE_REDIS_KEY, json.dumps(_live), ex=_LIVE_REDIS_TTL)
    except Exception as _e:
        import logging
        logging.warning(f"live_save_state failed: {_e}")


def _live_load_state() -> bool:
    """Load persisted state from Redis. Returns True if state was found."""
    global _live
    try:
        raw = _redis().get(_LIVE_REDIS_KEY)
        if raw:
            _live.update(json.loads(raw))
            return True
    except Exception:
        pass
    return False


def _live_update_defensive_mode() -> None:
    """Check and update global and per-instrument defensive mode each cycle.

    Global defensive: fires when balance_total has fallen more than half the CB
    threshold below day_start. Lifts when dollar_loss drops below half the
    defensive threshold (hysteresis band), OR 30-min time gate elapses.

    Per-instrument defensive: fires after _LIVE_DEF_INSTR_STOPOUTS (2) stop-outs
    on the same instrument today. Lifts when a WIN closes on that instrument after
    entry, OR 30-min time gate elapses.

    Sim path completely unaffected. Called from run_live_step after CB check.
    """
    now       = time.time()
    day_start = _live.get("balance_day_start", 0.0)
    current   = _live.get("balance_total", 0.0)

    # Global defensive mode
    if day_start > 0 and current > 0:
        if day_start < _LIVE_CB_MICRO_THRESH:
            def_buf = max(_LIVE_DEF_MICRO_FLOOR_USD, _LIVE_DEF_MICRO_PCT * day_start)
        else:
            def_buf = max(_LIVE_DEF_FLOOR_USD, _LIVE_DEF_PCT * day_start)
        dollar_loss = day_start - current
        gmode = _live.get("global_mode", "normal")
        if gmode == "normal" and dollar_loss >= def_buf:
            _live["global_mode"]            = "defensive"
            _live["global_mode_bal_entry"]  = current
            _live["global_mode_entered_at"] = now
            _live_log(
                f"⛔️ [DEFENSIVE] Global: NORMAL -> DEFENSIVE "
                f"(loss ${dollar_loss:.2f} >= threshold ${def_buf:.2f}; "
                f"day_start=${day_start:.2f})"
            )
        elif gmode == "defensive":
            entered_at = _live.get("global_mode_entered_at", now)
            recovered_pnl  = (day_start - current) < (def_buf * 0.5)
            recovered_time = (now - entered_at) >= _LIVE_DEF_TIMEOUT_SECS
            if recovered_pnl or recovered_time:
                _live["global_mode"] = "normal"
                why = "hysteresis cleared" if recovered_pnl else "30-min gate elapsed"
                _live_log(f"🟢 [DEFENSIVE] Global: DEFENSIVE -> NORMAL ({why})")

    # Per-instrument defensive mode recovery
    instr_mode    = _live.setdefault("instrument_mode", {})
    entered_at_m  = _live.setdefault("instrument_mode_entered_at", {})
    won_after_def = _live.setdefault("instrument_won_after_def", {})

    for sym, imode in list(instr_mode.items()):
        if imode != "defensive":
            continue
        ienter = entered_at_m.get(sym, now)
        recovered_win  = bool(won_after_def.get(sym))
        recovered_time = (now - ienter) >= _LIVE_DEF_TIMEOUT_SECS
        if recovered_win or recovered_time:
            instr_mode[sym] = "normal"
            why = "win recorded" if recovered_win else "30-min gate elapsed"
            _live_log(f"🟢 [DEFENSIVE] {sym}: DEFENSIVE -> NORMAL ({why})")
            won_after_def.pop(sym, None)


# ── IG live POST / DELETE wrappers ────────────────────────────────────────────

def _ig_live_post(path: str, body: dict, version: str = "2") -> Optional[dict]:
    """POST to live IG account. Returns parsed response dict or None on failure.
    Handles 401 re-auth and logs errors. Does NOT call _live_trade_guard() —
    callers must gate before reaching here.
    """
    global _live_api_paused_until
    if time.time() < _live_api_paused_until:
        _live_log(f"POST {path} skipped -- 429 rate-limit pause active")
        return None
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
        r = requests.post(url, headers=hdrs, json=body, timeout=15)
        if r.status_code == 401:
            _live_log(f"401 on {path} — re-authenticating live session")
            if not authenticate_live():
                return None
            hdrs["CST"]              = _live_sess["cst"]
            hdrs["X-SECURITY-TOKEN"] = _live_sess["token"]
            r = requests.post(url, headers=hdrs, json=body, timeout=15)
        if r.status_code in (200, 201):
            return r.json()
        _live_log(f"POST {path}: HTTP {r.status_code} {r.text[:120]}")
        return None
    except Exception as exc:
        _live_log(f"POST {path} error: {exc}")
        return None


def _ig_live_delete(path: str, version: str = "1") -> Optional[dict]:
    """DELETE to live IG account."""
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
        r = requests.delete(url, headers=hdrs, timeout=15)
        if r.status_code == 401:
            if not authenticate_live():
                return None
            hdrs["CST"]              = _live_sess["cst"]
            hdrs["X-SECURITY-TOKEN"] = _live_sess["token"]
            r = requests.delete(url, headers=hdrs, timeout=15)
        if r.status_code in (200, 201):
            return r.json()
        _live_log(f"DELETE {path}: HTTP {r.status_code} {r.text[:120]}")
        return None
    except Exception as exc:
        _live_log(f"DELETE {path} error: {exc}")
        return None


def _ig_live_put(path: str, body: dict, version: str = "2") -> Optional[dict]:
    """PUT to live IG account (amend position stop/limit). Returns parsed response or None.
    Does NOT call _live_trade_guard() — callers must gate before reaching here.
    """
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
        r = requests.put(url, headers=hdrs, json=body, timeout=15)
        if r.status_code == 401:
            _live_log(f"401 on PUT {path} — re-authenticating live session")
            if not authenticate_live():
                return None
            hdrs["CST"]              = _live_sess["cst"]
            hdrs["X-SECURITY-TOKEN"] = _live_sess["token"]
            r = requests.put(url, headers=hdrs, json=body, timeout=15)
        if r.status_code in (200, 201):
            return r.json()
        _live_log(f"PUT {path}: HTTP {r.status_code} {r.text[:120]}")
        return None
    except Exception as exc:
        _live_log(f"PUT {path} error: {exc}")
        return None


# ── Balance and P&L polling (Part 1 + Part 3) ────────────────────────────────

def _live_poll_balance() -> None:
    """Fetch live account balance from IG /accounts every 5 minutes.
    Updates _live['balance'] and _live['balance_fetched_at'].
    """
    global _live_balance_polled_at
    now = time.time()
    if now - _live_balance_polled_at < _LIVE_POLL_INTERVAL:
        return
    data = _ig_live_get("/accounts", version="1")
    if not data:
        return
    for acct in data.get("accounts", []):
        if acct.get("preferred") or acct.get("accountType") == "CFD":
            bal = acct.get("balance", {})
            _live["balance"]           = float(bal.get("available", 0.0))
            _live["balance_pnl"]       = float(bal.get("profitLoss", 0.0))
            _live["balance_total"]     = float(bal.get("balance", 0.0)) + _live["balance_pnl"]
            _live["balance_margin"]    = float(bal.get("deposit", 0.0))
            _live["balance_fetched_at"] = int(now)
            _live_balance_polled_at    = now
            # Seed day-start on first fetch of each UTC day (used by circuit breaker).
            # Persisted to Redis so a service restart on the same calendar day restores
            # the original baseline instead of re-seeding from IG (which can return a
            # stale or margin-inflated balance and re-fire the circuit breaker).
            _today_utc  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            _redis_dkey = f"june_balance_day_start:{_today_utc}"
            if _live.get("balance_day_start_date") != _today_utc or _live.get("balance_day_start", 0.0) <= 0:
                _redis_val = None
                try:
                    _redis_val = _redis().get(_redis_dkey)
                except Exception:
                    pass
                if _redis_val:
                    # Restore persisted baseline — avoids CB re-fire on same-day restart
                    _cash_only = float(_redis_val)
                    _live_log(f"Day-start balance RESTORED from Redis: ${_cash_only:.2f} ({_today_utc})")
                else:
                    # First genuine seed for this UTC day — persist to Redis (TTL 36h)
                    _cash_only = _live["balance"] + _live["balance_margin"]
                    try:
                        _redis().setex(_redis_dkey, 36 * 3600, str(_cash_only))
                    except Exception:
                        pass
                    _live_log(f"Day-start balance recorded: ${_cash_only:.2f} (cash only, excl unrealized P&L) ({_today_utc})")
                _live["balance_day_start"]      = _cash_only
                _live["balance_day_start_date"] = _today_utc
                # New UTC day -- reset defensive modes and per-instrument counters
                _live["global_mode"]               = "normal"
                _live["global_mode_bal_entry"]     = 0.0
                _live["global_mode_entered_at"]    = 0.0
                _live["instrument_mode"]           = {}
                _live["instrument_mode_entered_at"] = {}
                _live["instrument_stopouts_today"] = {}
                _live["instrument_won_after_def"]  = {}
            _live_log(f"Balance: ${_live['balance']:.2f} available, "
                      f"${_live['balance_margin']:.2f} in margin")
            _live_save_state()
            return


def _live_poll_pnl() -> None:
    """Fetch IG transaction history every 5 minutes.

    Filters to DEAL-type transactions only — deposits (DEPOSIT) and withdrawals
    (WITHDRAWAL) are excluded, ensuring cumulative_earned_pnl reflects only
    genuinely earned trading P&L, never account funding events.

    Uses a processed-reference set to avoid double-counting across polls.
    Polling window: 90 days (covers any reasonable gap between restarts).
    """
    global _live_pnl_polled_at
    now = time.time()
    if now - _live_pnl_polled_at < _LIVE_POLL_INTERVAL:
        return
    data = _ig_live_get(
        "/history/transactions",
        params={"type": "ALL", "maxSpanSeconds": 90 * 24 * 3600},
        version="2",
    )
    if not data:
        return
    txs         = data.get("transactions", [])
    seen_refs   = set(_live.get("pnl_seen_refs", []))
    earned_new  = 0.0
    for tx in txs:
        ref  = str(tx.get("reference", ""))
        ttype = tx.get("transactionType", "")
        if ttype != "DEAL":
            continue                # skip DEPOSIT, WITHDRAWAL, EXCHANGE — only real trading P&L
        if ref in seen_refs:
            continue
        pnl_str = tx.get("profitAndLoss", "$0")
        try:
            pnl_val = float(pnl_str.replace("$", "").replace(",", ""))
        except (ValueError, AttributeError):
            continue
        earned_new += pnl_val
        seen_refs.add(ref)

    if earned_new != 0.0:
        _live["cumulative_earned_pnl"] = round(
            _live.get("cumulative_earned_pnl", 0.0) + earned_new, 4
        )
        _live_log(f"Earned P&L updated: +${earned_new:.4f} → "
                  f"cumulative ${_live['cumulative_earned_pnl']:.4f}")

    # Cap seen_refs at 2000 entries (prevent unbounded Redis growth)
    if len(seen_refs) > 2000:
        seen_refs = set(list(seen_refs)[-2000:])
    _live["pnl_seen_refs"]  = list(seen_refs)
    _live["pnl_fetched_at"] = int(now)
    _live_pnl_polled_at     = now
    _live_save_state()


# ── Skim-cycle mechanics (Part 4) ────────────────────────────────────────────

def _live_check_skim() -> None:
    """Apply confirmed skim milestones. No other numbers or tiers.

    Phase pre300    : earned P&L < $300 — no skim
    Phase h100      : earned >= $300, flag $100 increments until earned >= $500
    Phase half      : earned >= $500, flag 50% of available balance (max once/hour)

    Skim execution (actual funds movement) is gated behind _live_trade_guard().
    When the switch is OFF, skim thresholds are logged but not flagged, so Jevon
    reviews the decision before enabling real execution.
    """
    if not _SKIM_ENABLED:
        return  # skim disabled; skimmed_total stays flat, downstream bal formula unaffected
    earned = _live.get("cumulative_earned_pnl", 0.0)
    phase  = _live.get("skim_phase", "pre300")

    if phase == "pre300":
        if earned < _LIVE_SKIM_PHASE1_TRIGGER:
            return
        _live["skim_phase"]            = "h100"
        _live["next_skim_threshold"]   = _LIVE_SKIM_PHASE1_TRIGGER
        _live_log(f"Skim phase → h100 (earned ${earned:.2f})")
        phase = "h100"

    if phase == "h100":
        next_t = _live.get("next_skim_threshold", _LIVE_SKIM_PHASE1_TRIGGER)
        while earned >= next_t and next_t < _LIVE_SKIM_PHASE2_TRIGGER:
            if not _live_trade_guard():
                _live_log(f"SKIM WOULD FLAG: ${_LIVE_SKIM_AMOUNT:.2f} "
                          f"(earned ${earned:.2f} ≥ ${next_t:.0f} threshold) "
                          f"— kill switch off, not flagging yet")
                break
            skim_now                    = _LIVE_SKIM_PHASE1_AMOUNT
            _live["skim_pending"]       = round(_live.get("skim_pending", 0.0) + skim_now, 2)
            _live["skimmed_total"]      = round(_live.get("skimmed_total", 0.0) + skim_now, 2)
            next_t                     += _LIVE_SKIM_PHASE1_AMOUNT
            _live_log(f"💰 SKIM FLAGGED: ${skim_now:.2f} "
                      f"(earned ${earned:.2f}, next at ${next_t:.0f})")
        _live["next_skim_threshold"] = next_t

        if earned >= _LIVE_SKIM_PHASE2_TRIGGER:
            _live["skim_phase"] = "half"
            _live_log("Skim phase → half (earned ≥ $500)")
            phase = "half"
        else:
            _live_save_state()
            return

    if phase == "half":
        last_half = _live.get("last_half_skim_time", 0.0)
        if time.time() - last_half < _LIVE_SKIM_HALF_MIN_GAP:
            return
        avail    = _live.get("balance", 0.0)
        skim_now = round(avail / 2.0, 2)
        if not _live_trade_guard():
            _live_log(f"SKIM WOULD FLAG: ${skim_now:.2f} (half of ${avail:.2f}) "
                      f"— kill switch off, not flagging yet")
            return
        _live["skim_pending"]        = round(_live.get("skim_pending", 0.0) + skim_now, 2)
        _live["skimmed_total"]       = round(_live.get("skimmed_total", 0.0) + skim_now, 2)
        _live["last_half_skim_time"] = time.time()
        _live_log(f"💰 SKIM FLAGGED: ${skim_now:.2f} (half of ${avail:.2f} available)")

    _live_save_state()


# Fix reference error — _LIVE_SKIM_AMOUNT used inside loop but wrong name
_LIVE_SKIM_AMOUNT = _LIVE_SKIM_PHASE1_AMOUNT   # alias for readability inside h100 loop


def _live_check_circuit_breaker() -> None:
    """Daily drawdown circuit breaker — disables june_live_enabled if live account
    falls more than _LIVE_CIRCUIT_BREAKER_PCT below the day's opening balance_total.

    Uses balance_total (cash + unrealized P&L) so an open losing position is caught
    immediately on the next 5-minute balance poll, not only after it closes.

    On breach: sets june_live_enabled=false in Redis AND updates the module flag.
    Does NOT auto-resume — Jevon must re-enable manually after reviewing logs.
    Rate cost: zero (dict lookup only; no new API calls).
    """
    global _june_live_trading_enabled
    if not _june_live_trading_enabled:
        return   # already disabled — nothing to do
    day_start = _live.get("balance_day_start", 0.0)
    current   = _live.get("balance_total", 0.0)
    if day_start <= 0 or current <= 0:
        return   # balance not yet fetched — no baseline to compare
    # Two-tier CB buffer.
    # Micro tier (<$30): 30% drawdown / $2 floor — room for 2–3 stop-outs.
    # At $7.99 day_start: buffer = max($2.00, 30% x $7.99) = $2.40; fires below $5.59.
    # Sub-floor edge case: when day_start < $2 floor, max() returns the floor which
    # exceeds the entire account — CB becomes mathematically unreachable. Fix: use
    # pct-only (30% x day_start) when below the floor. No change above $2.
    # Full tier ($30+): 5% rule; $20 floor capped at 10% x account so the
    # pct threshold is never swamped for accounts below ~$400.
    dollar_loss = day_start - current
    if day_start < _LIVE_CB_MICRO_THRESH:
        _pct_buf = _LIVE_CB_MICRO_PCT * day_start
        if day_start < _LIVE_CB_MICRO_FLOOR_USD:
            # Balance below $2: floor would exceed entire account; scale to pct only
            effective_buffer = _pct_buf
        else:
            effective_buffer = max(_LIVE_CB_MICRO_FLOOR_USD, _pct_buf)  # unchanged
    else:
        # Without cap: at $34.79 the $20 floor requires -57.5% loss before CB fires.
        # Cap at 10% of account; crossover where 5% pct rule binds: ~$400.
        _full_floor = min(_LIVE_CB_FLOOR_USD, 0.10 * day_start)
        effective_buffer = max(_full_floor, abs(_LIVE_CIRCUIT_BREAKER_PCT) * day_start)
    drawdown         = (current - day_start) / day_start
    if dollar_loss < effective_buffer:
        return   # within daily tolerance
    # Threshold breached — disable kill switch via Redis + module flag
    try:
        _redis().set("june_live_enabled", "false")
    except Exception:
        pass
    _june_live_trading_enabled = False
    # Write a durable Redis alert so the next startup cannot miss this silently
    try:
        import datetime as _dt2, json as _json2
        _alert_payload = _json2.dumps({
            "fired_at":        _dt2.datetime.utcnow().isoformat(),
            "day_start":       round(day_start, 2),
            "current":         round(current, 2),
            "drawdown_pct":    round(drawdown * 100, 2),
            "effective_buffer": round(effective_buffer, 2),
        })
        _redis().set("june_circuit_breaker_alert", _alert_payload, ex=48 * 3600)
    except Exception:
        pass
    _live_log(
        f"\U0001f6a8 CIRCUIT BREAKER FIRED \u2014 live trading disabled.\n"
        f"  Day-start balance : ${day_start:.2f}\n"
        f"  Current balance   : ${current:.2f}\n"
        f"  Drawdown          : {drawdown:+.2%}\n"
        f"  Effective buffer  : ${effective_buffer:.2f} ({'micro' if day_start < _LIVE_CB_MICRO_THRESH else 'full'} tier)\n"
        f"  Re-enable requires: redis-cli SET june_live_enabled true (after review)"
    )



# ── Instrument eligibility (Part 2) ──────────────────────────────────────────



def _live_parse_pip_size(one_pip_means) -> float:
    """Parse IG onePipMeans into pip size in price units.

    API examples and their resolved pip_sz:
      "0.0001 USD/EUR"       -> 0.0001  (standard 4-decimal FX)
      "0.01 JPY/USD"         -> 0.01    (JPY pairs, 2-decimal)
      "1 Cents/Troy Ounce"   -> 0.01    (SILVER: 1 cent = $0.01)
      "1 $/Troy Ounce"       -> 1.0     (GOLD)
      "1 Index Point"        -> 1.0     (SPX500/GER40/UK100)
      "1"                    -> 1.0     (OIL)
      None                   -> 0.01    (equity CFDs: US stocks move in $0.01 increments;
                                         IG does not expose onePipMeans for .CASH.IP epics)
    """
    if one_pip_means is None:
        return 0.01   # equity CFD fallback: US stocks price in $0.01 increments
    s = str(one_pip_means).strip()
    if not s:
        return 0.01
    try:
        first = float(s.split()[0])
    except (ValueError, IndexError):
        return 0.01
    # "1 Cents/..." — numeric value is 1, but unit is cents → $0.01
    if first == 1.0 and "cent" in s.lower():
        return 0.01
    return first


def _live_fetch_market_data(sym: str, epic: str) -> bool:
    """Fetch IG lotSize and minDealSize from the LIVE account for one instrument.

    Called at startup for every entry in INSTRUMENTS so _live_compute_ig_size and
    _live_compute_stop_pts use the real live-account values, not the FX default.
    Returns True on success, False on 404 or network error (instrument stays
    ineligible for live until data arrives).
    """
    data = _ig_live_get(f"/markets/{epic}", version="1")
    if not data:
        return False
    inst     = data.get("instrument", {})
    deal     = data.get("dealingRules", {})
    lot_sz   = float(inst.get("lotSize") or 1.0)
    min_obj  = deal.get("minDealSize") or {}
    min_val  = float(min_obj.get("value") or 1.0)
    one_pip = inst.get("onePipMeans")
    pip_sz  = _live_parse_pip_size(one_pip)
    price_unit = 0.01 if (one_pip and "cent" in str(one_pip).lower()) else 1.0
    if price_unit == 1.0 and epic.upper().startswith("CC.") and sym not in ("SUGAR", "COCOA"):  # SUGAR/COCOA quote in $/£ per tonne directly — not cents
        price_unit = 0.01  # CC.D.* commodity epics price in cents; "1" pip lacks "cent" keyword
        pip_sz *= price_unit  # rescale pip_sz to USD so stop formula (price*price_unit*pct/pip_sz) is unit-consistent
        if sym == "NATGAS":
            pip_sz = price_unit  # NATGAS pip from IG is 1e-3 native (not 1.0 like OIL/SILVER);
                                  # CC.D.* rescaling gives 1e-5, inflating stop_pts 1000x -> ATTACHED_ORDER_LEVEL_ERROR
    # v3 API: marginFactor + marginFactorUnit; v1 API: margin — both percentage scale
    _mf_v3   = inst.get("marginFactor")
    _mf_v1   = inst.get("margin")
    _mf      = _mf_v3 if _mf_v3 is not None else _mf_v1
    _mf_unit = inst.get("marginFactorUnit") or "PERCENTAGE"  # None from API treated same as absent — always a percentage scale
    if _mf is not None:
        margin_rate = float(_mf) / 100.0 if _mf_unit == "PERCENTAGE" else float(_mf)
    else:
        margin_rate = 0.0
    _live_lot_sizes[sym]  = lot_sz
    _live_min_deal[sym]   = min_val
    _live_pip_sizes[sym]  = pip_sz
    _live_price_unit[sym] = price_unit
    _live_margin[sym]     = margin_rate
    _ms_obj  = deal.get("minNormalStopOrLimitDistance") or {}
    _ms_unit = _ms_obj.get("unit", "POINTS")
    _ms_val  = float(_ms_obj.get("value") or 4.0)
    if _ms_unit == "PERCENTAGE":
        # PERCENTAGE unit would need a price reference to convert to points;
        # all live instruments observed use POINTS — log and use safe fallback.
        _live_log(f"\u26a0\ufe0f  {sym}: minStop unit=PERCENTAGE, expected POINTS "
                  f"— using 4pt fallback instead of {_ms_val}")
        _ms_val = 4.0
    _live_min_stop_pts[sym] = max(1, int(_ms_val))  # real IG value; +1 buffer in _live_compute_stop_pts
    if epic.upper().endswith(".CASH.IP"):
        _live_equity_cfd.add(sym)
    if (epic.upper().startswith("CS.D.") and epic.upper().endswith("CFD.IP")
            and _SPREAD_ATR_ASSET_CLASS.get(sym) != "CRYPTO"):
        _live_fx_instruments.add(sym)
    _live_log(f"LIVE mkt: {sym} lot={lot_sz} minDeal={min_val} pip={pip_sz} unit={price_unit} margin={margin_rate:.0%} min_stop={_live_min_stop_pts[sym]}pts")
    return True


def _ig_margin_to_max_lev(margin_rate: float, ceiling: int) -> int:
    """Convert IG's margin field to an effective leverage ceiling.
    IG uses two scales depending on instrument category:
      0 < rate <= 1.0 : decimal fraction (0.5 -> 50% margin -> 2x leverage)
      rate > 1.0      : margin percentage (20.0 -> 20% margin -> 5x leverage)
    After _live_fetch_market_data stores decimal fractions (OIL=0.01, SILVER=0.008),
    the <= 1.0 branch always applies: 1/0.01=100x, 1/0.008=125x. The > 1.0 branch
    applies only if a future instrument stores a raw percentage value directly.
    """
    if not margin_rate or margin_rate <= 0:
        return ceiling
    if margin_rate <= 1.0:
        return min(ceiling, max(1, int(1.0 / margin_rate)))
    else:
        return min(ceiling, max(1, int(100.0 / margin_rate)))


def _real_margin_fraction(sym: str, margin_raw: float) -> float:
    """Return the margin as a decimal fraction of USD notional.

    _live_fetch_market_data reads IG's marginFactor field and divides by 100
    when marginFactorUnit=PERCENTAGE (all live instruments observed so far).
    The stored value is already a decimal fraction for every instrument type:
      OIL=0.01 (1%), SILVER=0.008 (0.8%), GOLD=0.005 (0.5%), FX~0.02 (2%).
    """
    return max(0.0, margin_raw)


def _live_publish_eligible_instruments(signals: dict) -> None:
    """Compute and publish barbie_june_eligible_instruments to Redis (2h TTL).

    For each instrument in INSTRUMENTS, determines whether a trade is actually
    viable at the current IG balance using real per-instrument IG API data:
      min_notional  = min_deal x lot_sz x price x price_unit
      min_margin    = min_notional x margin_rate (clamped to [0,1])
      lot_formula_suspect: min_notional < $1.00 on a non-equity instrument with
        margin_rate <= 1.0 -- detects the FX lot sizing formula bug where
        IG API returns lot_sz as a contract multiplier (~10) rather than the
        true base-currency lot size (~100,000 for FX). Derived from live data;
        no hardcoded instrument lists. Self-corrects if bug is fixed later.
      commission_gate: equity CFDs where expected TP gross < $18 round-trip.

    Rate-limited to once per hour. Sim state completely unaffected.
    """
    global _live_elig_publish_next
    now = time.time()
    if now < _live_elig_publish_next:
        return
    _live_elig_publish_next = now + 3600.0

    balance = _live.get("balance_total", 0.0)
    if balance <= 0:
        return

    _ELIG_LOT_SANITY = 1.0
    round_trip_comm  = _IG_EQUITY_COMMISSION_USD * 2

    result: dict = {}
    for sym in list(INSTRUMENTS.keys()):
        sig        = signals.get(sym, {})
        price      = sig.get("price", 0.0)
        if price <= 0:
            continue

        lot_sz     = _live_lot_sizes.get(sym, 0.0)
        min_deal   = _live_min_deal.get(sym, 0.0)
        price_unit = _live_price_unit.get(sym, 1.0)
        margin_raw = _live_margin.get(sym, 0.0)
        is_equity  = sym in _live_equity_cfd

        if lot_sz <= 0 or min_deal <= 0:
            result[sym] = {"eligible": False, "reason": "no_ig_data",
                           "min_notional": 0.0, "min_margin": 0.0, "margin_rate": margin_raw}
            continue

        price_usd    = price * price_unit
        min_notional = (min_deal * price_usd) if is_equity else (min_deal * lot_sz * price_usd)
        margin_rate  = max(0.0, min(1.0, margin_raw))
        min_margin   = min_notional * _real_margin_fraction(sym, margin_raw)

        # FX sizing formula bug: if min_notional < $1 on a non-equity instrument
        # with real margin data, lot_sz from IG API is likely a contract multiplier
        # not the full base-currency lot size. Self-correcting when formula is fixed.
        lot_formula_suspect = (
            not is_equity and
            margin_rate > 0 and margin_rate <= 1.0 and
            min_notional < _ELIG_LOT_SANITY
        )

        commission_blocked = False
        if is_equity:
            expected_gross     = min_notional * 0.02
            commission_blocked = expected_gross < round_trip_comm

        if lot_formula_suspect:
            eligible, reason = False, "fx_lot_formula_suspect"
        elif commission_blocked:
            eligible, reason = False, "commission_gate"
        elif margin_rate > 0 and min_margin > balance:
            eligible, reason = False, "insufficient_balance(need ${:.2f})".format(min_margin)
        elif margin_rate == 0 and not is_equity:
            eligible, reason = False, "no_margin_data"
        else:
            eligible, reason = True, None

        result[sym] = {
            "eligible":     eligible,
            "reason":       reason,
            "min_notional": round(min_notional, 4),
            "min_margin":   round(min_margin, 4),
            "margin_rate":  round(margin_raw, 4),
        }

    eligible_list = [s for s, d in result.items() if d.get("eligible")]
    payload = {
        "ts":               int(now),
        "balance":          round(balance, 2),
        "instruments":      result,
        "eligible_list":    eligible_list,
        "ineligible_count": len(result) - len(eligible_list),
    }
    try:
        _redis().set("barbie_june_eligible_instruments", json.dumps(payload), ex=7200)
        _live_log(
            "\U0001f4cb Eligible instruments: {} "
            "(bal ${:.2f}) | {} ineligible".format(
                eligible_list or ["none"], balance, len(result) - len(eligible_list)
            )
        )
    except Exception as _elig_exc:
        _live_log("\u26a0\ufe0f barbie_june_eligible_instruments publish failed: {}".format(_elig_exc))


def _live_is_eligible(sym: str) -> bool:
    """Eligibility check using effective live balance and IG's real per-instrument margin rate.

    effective_balance = balance_total - skimmed_total
      balance_total : IG total equity (deposits + realized P&L, before unrealized).
                      Includes capital currently in margin use, so eligibility is not
                      incorrectly shrunk when another position is already open.
      skimmed_total : cumulative profit marked set-aside by the skim mechanism --
                      still physically in the IG account but not to be risked on
                      new trades. Subtracting it ensures set-aside funds are genuinely
                      excluded from the concentration-cap denominator.

    Formula: (min_notional / effective_leverage) <= concentration_cap * effective_balance
    where effective_leverage = _ig_margin_to_max_lev(margin_rate, sim_ceiling).

    Covers all IG instrument categories: FX/commodities/indices use decimal fraction
    scale (0.5=50% margin); equity/ETF types use percentage scale (20.0=20% margin).
    """
    total   = _live.get("balance_total", 0.0)
    skimmed = _live.get("skimmed_total", 0.0)
    bal     = max(0.0, total - skimmed)
    if bal <= 0:
        return False
    if sym in _live_fx_instruments:
        return False  # FX structurally blocked: cheapest pair (AUDUSD) needs ~$4,800 balance;
                    # current balance ~$22 (215× below). _KNOWN_MIN_NOTIONALS now uses correct
                    # FX formula — guard stays until balance reaches viable threshold.
    lev = int(_SIM_LEV_RANGES.get("sprout", (3, 10))[1])  # sim ceiling
    margin_rate = _live_margin.get(sym)
    if margin_rate and margin_rate > 0:
        lev = _ig_margin_to_max_lev(margin_rate, lev)
    return _sim_is_eligible(sym, bal, lev)


def _live_is_paused(combo: str) -> bool:
    """Live-specific pause tracking — separate from sim's pause_expiry."""
    exp = (_live.get("pause_expiry") or {}).get(combo, 0.0)
    return time.time() < exp


_perf_block_cache: dict = {}   # {sym: cache_expire_ts} — in-memory, refreshed from Redis


def _perf_block_sar_ttl(sym: str = "", sub_session: str = "") -> int:
    """Seconds until end of the given sub-session for SAR perf blocks.

    Sub-session boundaries (OIL / SILVER):
      overnight:      expires 07:00 UTC (next day when called after 21:00)
      pre_nyse:       expires NYSE open (DST-aware, floor _PERF_SAR_OIL_BOUNDARY_FLOOR_SECS)
      nyse_morning:   expires 12:00 ET (NYSE midday boundary)
      nyse_afternoon: expires 21:00 UTC
    Other instruments (sub_session="day"): expires 21:00 UTC (day) or 07:00 UTC (overnight).
    """
    now_utc = datetime.now(timezone.utc)

    if sub_session == "overnight" or (not sub_session and is_overnight()):
        target = now_utc.replace(hour=OVERNIGHT_END_MIN // 60, minute=0, second=0, microsecond=0)
        if now_utc.hour >= OVERNIGHT_START_MIN // 60:
            target += timedelta(days=1)
        return max(1, int((target - now_utc).total_seconds()))

    if sub_session == "pre_nyse":
        now_et        = datetime.now(_US_EAST_TZ)
        nyse_open_et  = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        nyse_open_utc = nyse_open_et.astimezone(timezone.utc)
        secs_to_nyse  = (nyse_open_utc - now_utc).total_seconds()
        return max(_PERF_SAR_OIL_BOUNDARY_FLOOR_SECS, int(secs_to_nyse))

    if sub_session == "nyse_morning":
        now_et       = datetime.now(_US_EAST_TZ)
        nyse_mid_et  = now_et.replace(hour=12, minute=0, second=0, microsecond=0)
        nyse_mid_utc = nyse_mid_et.astimezone(timezone.utc)
        secs_to_mid  = (nyse_mid_utc - now_utc).total_seconds()
        return max(1, int(secs_to_mid))

    # nyse_afternoon, plain "day", or legacy fallback — expire at 21:00 UTC
    target = now_utc.replace(hour=OVERNIGHT_START_MIN // 60, minute=0, second=0, microsecond=0)
    return max(1, int((target - now_utc).total_seconds()))


def _live_perf_blocked(sym: str) -> bool:
    """True if sym is blocked by the instrument performance filter.
    Checks legacy (june_perf_block:{sym}), WR (june_perf_block_wr:{sym}),
    and SAR (june_perf_block_sar:{sym}:{sub_session}) keys — any active key blocks.
    SAR blocks are sub-session specific (OIL/SILVER): a bad afternoon never blocks the next morning.
    Caches Redis state locally: instrument-wide blocks under sym, SAR blocks under sym:sub_session.
    On Redis error: blocks the trade (fail-closed) to prevent trading through an active block.
    Caches for the full block TTL at fire time — no Redis check needed during the block window.
    """
    now = time.time()
    sub = _current_sub_session(sym)
    if now < _perf_block_cache.get(sym, 0.0):
        return True  # instrument-wide block cache (WR/legacy, or full-TTL from fire time)
    if now < _perf_block_cache.get(f"{sym}:{sub}", 0.0):
        return True  # sub-session SAR block cache — no Redis round-trip during block window
    try:
        r = _redis()
        # Instrument-wide blocks (WR, legacy) apply across all sub-sessions.
        if r.get(f"june_perf_block:{sym}") or r.get(f"june_perf_block_wr:{sym}"):
            _perf_block_cache[sym] = now + 300.0  # 5-min cache for instrument-wide blocks
            return True
        # SAR block is sub-session specific — a bad afternoon never blocks the next morning.
        if r.get(f"june_perf_block_sar:{sym}:{sub}"):
            _perf_block_cache[f"{sym}:{sub}"] = now + 300.0
            return True
    except Exception as exc:
        _live_log(f"⚠️ [PERF BLOCK] {sym}/{sub}: Redis error — treating as BLOCKED (fail-closed): {exc}")
        return True  # fail-closed: never allow a blocked instrument to trade on Redis outage
    _perf_block_cache.pop(sym, None)
    _perf_block_cache.pop(f"{sym}:{sub}", None)
    return False


def _live_get_observer(sym: str):
    """Return 'light', 'moderate', or None."""
    try:
        v = _redis().get(f"june_observer:{sym}")
        return v.decode() if v else None
    except Exception:
        return None

def _live_set_observer(sym: str, tier: str) -> None:
    try:
        _redis().set(f"june_observer:{sym}", tier)
    except Exception as exc:
        _live_log(f"[observer_set] {sym}: Redis error — {exc}")

def _live_clear_observer(sym: str) -> None:
    try:
        _redis().delete(f"june_observer:{sym}")
    except Exception as exc:
        _live_log(f"[observer_clear] {sym}: Redis error — {exc}")

def _live_perf_last_won(sym: str) -> bool:
    """Return True if the most recent recorded live trade for sym was a win."""
    try:
        import json as _json
        raw = _redis().get(f"june_perf_stats:{sym}")
        if raw:
            trades = _json.loads(raw).get("trades", [])
            if trades:
                return bool(trades[-1].get("won", False))
    except Exception:
        pass
    return False

def _live_migrate_perf_blocks() -> None:
    """On startup: re-evaluate all instruments under new severity tiers.
    Clears old 24h WR blocks that do not meet hard-block criteria.
    Sets observer keys for instruments that qualify as observer tiers.
    """
    import json as _json, time as _time
    r = _redis()
    try:
        keys = r.keys("june_perf_block_wr:*")
    except Exception as exc:
        _live_log(f"[migrate_perf] Redis error listing keys: {exc}")
        return

    for key in keys:
        sym = key.decode().split(":", 1)[1] if isinstance(key, bytes) else key.split(":", 1)[1]
        try:
            raw = r.get(f"june_perf_stats:{sym}")
            trades = _json.loads(raw).get("trades", []) if raw else []
        except Exception:
            trades = []

        cutoff = _time.time() - _PERF_BLOCK_RECENCY_DAYS * 86400
        recent = [t for t in trades if t.get("epoch", 0) >= cutoff and not t.get("excluded_defect_id")]
        n = len(recent)
        wins = sum(1 for t in recent if t.get("won"))
        wr = wins / n if n > 0 else 1.0
        net_loss_dollar = sum(
            abs(t.get("pnl_dollar", 0.0))
            for t in recent if t.get("pnl_dollar", 0.0) < 0
        )

        balance = _live.get("balance", 0.0)
        if balance <= 0:
            _live_log(f"[migrate_perf] {sym}: live balance unavailable — loss_pct unknown, pnl_known_count guards observer tier")
        loss_pct = net_loss_dollar / balance if balance > 0 else 0.0

        qualifies_hard = (
            n >= _PERF_BLOCK_HARD_MIN_TRADES
            and wr < _PERF_BLOCK_HARD_WR_THRESH
            and loss_pct > _PERF_BLOCK_HARD_LOSS_PCT
        )
        qualifies_obs_moderate = (
            n >= _PERF_BLOCK_MIN_RECENT
            and wr < _PERF_BLOCK_WR_THRESH
            and loss_pct >= _PERF_BLOCK_OBS_LIGHT_PCT
        )
        pnl_known_count = sum(1 for t in recent if "pnl_dollar" in t)
        qualifies_obs_light = (
            n >= _PERF_BLOCK_MIN_RECENT
            and wr < _PERF_BLOCK_WR_THRESH
            and (loss_pct > 0 or pnl_known_count == 0)
        )

        if qualifies_hard:
            r.expire(key, _PERF_BLOCK_HARD_TTL)
            _live_log(
                f"[migrate_perf] {sym}: old WR block KEPT as HARD "
                f"(n={n}, WR={wr:.0%}, loss={loss_pct:.1%}), TTL reset to 12h"
            )
        else:
            try:
                r.delete(key)
            except Exception:
                pass
            existing_obs = _live_get_observer(sym)
            if existing_obs:
                _live_log(
                    f"[migrate_perf] {sym}: old WR block cleared; "
                    f"observer-{existing_obs} already set"
                )
            elif qualifies_obs_moderate:
                _live_set_observer(sym, "moderate")
                _live_log(
                    f"[migrate_perf] {sym}: old WR block cleared -> observer-MODERATE "
                    f"(n={n}, WR={wr:.0%}, loss={loss_pct:.1%})"
                )
            elif qualifies_obs_light:
                _live_set_observer(sym, "light")
                _note = " [loss unknown — pre-fix records]" if pnl_known_count == 0 else f" loss={loss_pct:.1%}"
                _live_log(
                    f"[migrate_perf] {sym}: old WR block cleared -> observer-LIGHT "
                    f"(n={n}, WR={wr:.0%},{_note})"
                )
            else:
                _live_log(
                    f"[migrate_perf] {sym}: old WR block cleared — no observer tier qualifies "
                    f"(n={n}, WR={wr:.0%}, loss={loss_pct:.1%})"
                )

def _live_perf_record(sym: str, won: bool, sar, pnl_dollar: float = 0.0, entry_sar=None, persistence_confirmed=None, excluded_defect_id: str = None) -> None:
    """Update rolling per-instrument performance stats in Redis.
    Each record carries epoch timestamp and dollar P&L.
    Severity tiers keyed to % of current balance lost:
      Observer Light    -- WR < 30%, >= 8 recent, net loss > 0% (< 3%)
      Observer Moderate -- WR < 30%, >= 8 recent, net loss >= 3%
      Hard Block        -- WR < 20%, >= 12 recent, net loss > 7%, 12h TTL
    SAR block logic unchanged.
    Called only after a confirmed IG position close.
    """
    try:
        r      = _redis()
        key    = f"june_perf_stats:{sym}"
        raw    = r.get(key)
        stats  = json.loads(raw) if raw else {"trades": []}
        trades = stats.get("trades", [])
        session     = "overnight" if is_overnight() else "day"
        sub_session = _current_sub_session(sym)
        trades.append({
            "won": won,
            "sar": round(sar or 0.0, 4),
            "entry_sar": round(entry_sar, 4) if entry_sar is not None else None,
            "persistence_confirmed": persistence_confirmed,
            "session": session,
            "sub_session": sub_session,
            "epoch": int(time.time()),
            "pnl_dollar": round(pnl_dollar, 4),
            **({"excluded_defect_id": excluded_defect_id} if excluded_defect_id else {}),
        })
        keep = max(_PERF_BLOCK_WINDOW, _PERF_BLOCK_HARD_MIN_TRADES)
        if len(trades) > keep:
            trades = trades[-keep:]
        stats["trades"] = trades
        r.set(key, json.dumps(stats))

        # ── Recency filter ─────────────────────────────────────────────────────────────────────────
        cutoff = time.time() - (_PERF_BLOCK_RECENCY_DAYS * 86400)
        _recent_all = [t for t in trades if t.get("epoch", 0) >= cutoff]
        recent = [t for t in _recent_all if not t.get("excluded_defect_id")]
        _qt_wr = len(_recent_all) - len(recent)
        if _qt_wr:
            _live_log(f"  [QUARANTINE] {sym}: {_qt_wr} defect-tagged trade(s) excluded from WR eval")
        n = len(recent)
        wins = sum(1 for t in recent if t.get("won"))
        wr = wins / n if n > 0 else 1.0

        # Net loss in dollars over recent window
        net_loss_dollar = sum(
            abs(t.get("pnl_dollar", 0.0))
            for t in recent if t.get("pnl_dollar", 0.0) < 0
        )
        balance = _live.get("balance", 0.0)
        if balance <= 0:
            _live_log(f"[perf_record] {sym}: live balance unavailable — loss_pct unknown, pnl_known_count guards observer tier")
        loss_pct = net_loss_dollar / balance if balance > 0 else 0.0

        # ── WR severity tiers ─────────────────────────────────────────────────────────────────────────────
        if n >= _PERF_BLOCK_MIN_RECENT and wr < _PERF_BLOCK_WR_THRESH:
            if (n >= _PERF_BLOCK_HARD_MIN_TRADES
                    and wr < _PERF_BLOCK_HARD_WR_THRESH
                    and loss_pct > _PERF_BLOCK_HARD_LOSS_PCT):
                r.setex(f"june_perf_block_wr:{sym}", _PERF_BLOCK_HARD_TTL, "hard")
                _live_clear_observer(sym)
                _perf_block_cache[sym] = time.time() + _PERF_BLOCK_HARD_TTL  # full TTL — no Redis gap
                _live_log(
                    f"⛔ [PERF HARD BLOCK] {sym}: WR {wr:.0%} < {_PERF_BLOCK_HARD_WR_THRESH:.0%}, "
                    f"loss {loss_pct:.1%} > {_PERF_BLOCK_HARD_LOSS_PCT:.0%} "
                    f"({n} recent trades). Suspended 12h."
                )
            elif loss_pct >= _PERF_BLOCK_OBS_LIGHT_PCT:
                _live_set_observer(sym, "moderate")
                _live_log(
                    f"🟡 [OBSERVER MODERATE] {sym}: WR {wr:.0%}, "
                    f"loss {loss_pct:.1%} ({n} trades). Conviction floor x2, size x0.7."
                )
            else:
                _live_set_observer(sym, "light")
                _live_log(
                    f"🟠 [OBSERVER LIGHT] {sym}: WR {wr:.0%}, "
                    f"loss {loss_pct:.1%} ({n} trades). Conviction floor x1.5."
                )

        # ── SAR block (session-scoped TTL) ───────────────────────────────────────────────────────────
        cur_sub_session = _current_sub_session(sym)
        # Exclude pre-a62093f trades (pre-sizing-fix, 100x over-sized lots, epoch cutoff 2026-08-28 06:25 UTC).
        # Records without sub_session fall back to their "session" value for backwards compat.
        session_trades_all = [
            t for t in recent
            if t.get("sub_session", t.get("session", "day")) == cur_sub_session
        ]
        _session_trades_cutoff = [t for t in session_trades_all
                                    if t.get("epoch", 0) >= _PERF_BLOCK_SAR_EPOCH_CUTOFF]
        session_trades = [t for t in _session_trades_cutoff if not t.get("excluded_defect_id")]
        for _qt in _session_trades_cutoff:
            if _qt.get("excluded_defect_id"):
                _live_log(
                    f"  [QUARANTINE] {sym} epoch={_qt['epoch']} "
                    f"defect={_qt['excluded_defect_id']!r} excluded from SAR eval"
                )
        # Gate on VALID-SAR count, not total trade count.
        # Zero-value entries (cold-start sentinels, pre-tracking records) must not count
        # as evidence — a bucket with 4 trades but only 2 valid SAR measurements has
        # 2 data points, not 4, and should not be able to fire the block.
        sar_vals = [
            (t.get("entry_sar") if t.get("entry_sar") else t.get("sar", 0))
            for t in session_trades
            if (t.get("entry_sar") or t.get("sar", 0)) > 0
        ]
        if len(sar_vals) >= _PERF_BLOCK_SAR_SESSION_MIN:
            avg_sar = sum(sar_vals) / len(sar_vals)
            if avg_sar > _PERF_BLOCK_SAR_THRESH:
                sar_ttl   = _perf_block_sar_ttl(sym, cur_sub_session)
                sar_key   = f"june_perf_block_sar:{sym}:{cur_sub_session}"
                cache_key = f"{sym}:{cur_sub_session}"
                r.setex(sar_key, sar_ttl, "1")
                _perf_block_cache[cache_key] = time.time() + sar_ttl  # full TTL — no Redis gap
                _live_log(
                    f"⛔ [PERF BLOCK SAR] {sym}/{cur_sub_session}: Avg Spread/ATR {avg_sar:.0%} above "
                    f"{_PERF_BLOCK_SAR_THRESH:.0%} threshold "
                    f"({len(sar_vals)} valid / {len(session_trades)} post-cutoff {cur_sub_session} trades). "
                    f"Suspended until next sub-session ({sar_ttl}s)."
                )

    except Exception as exc:
        _live_log(f"[perf_record] {sym}: Redis error — {exc}")

def _live_has_boost(combo: str) -> bool:
    """Live-specific boost tracking — separate from sim's boost_expiry."""
    exp = (_live.get("boost_expiry") or {}).get(combo, 0.0)
    return time.time() < exp


def _live_update_streak(sym: str, direction: str, won: bool) -> None:
    """Track live-specific streak state (separate from sim's streak_state).
    Mirrors _sim_update_streak logic against _live dict.
    """
    combo        = _sim_combo_key(sym, direction)
    streak_state = _live.setdefault("streak_state", {})
    boost_expiry = _live.setdefault("boost_expiry", {})
    pause_expiry = _live.setdefault("pause_expiry", {})
    if won:
        streak_state[combo] = 0
        boost_expiry[combo] = 0.0
        pause_expiry[combo] = 0.0
        return
    streak_state[combo] = streak_state.get(combo, 0) + 1
    n = streak_state[combo]
    if n == _SIM_STREAK_BOOST:
        boost_end          = time.time() + _SIM_BOOST_DUR
        boost_expiry[combo] = boost_end
        _live_log(f"⚠️ {sym} {direction.upper()} live streak {n} — "
                  f"threshold raised for {int(_SIM_BOOST_DUR // 60)}m")
    elif n >= _SIM_STREAK_PAUSE:
        pause_end           = time.time() + _SIM_PAUSE_DUR
        pause_expiry[combo] = pause_end
        boost_expiry[combo] = 0.0
        resume = datetime.fromtimestamp(pause_end, tz=timezone.utc).strftime("%H:%M UTC")
        _live_log(f"🛑 {sym} {direction.upper()} live streak {n} — paused until {resume}")


# ── Sizing helpers ────────────────────────────────────────────────────────────

def _live_compute_ig_size(sym: str, desired_notional_usd: float, mid_price: float) -> float:
    """Convert desired USD notional to IG order size using per-instrument lot sizes.

    Formula: ig_size = desired_notional / (lot_sz x mid_price)
    Lot sizes fetched from LIVE IG API at startup and stored in _live_lot_sizes.
    Sizes are fractional (IG supports non-integer sizes for equity CFDs and some FX).
    Minimum clamped to minDealSize from _live_min_deal (default 1.0).

    Example: AMD lot=0.01, mid=$507 -> 1 unit=$5.07. For $10 target:
      ig_size = max(1.0, round(10.0/5.07, 2)) = 1.97 -> notional ~$10.00
    """
    if mid_price <= 0:
        return 0.0
    lot_sz     = _live_lot_sizes.get(sym, _LIVE_LOT_SIZE_FX)
    min_deal   = _live_min_deal.get(sym, 1.0)
    price_unit = _live_price_unit.get(sym, 1.0)
    price_usd  = mid_price * price_unit   # convert native price to USD (e.g. cents -> dollars)
    if price_usd <= 0:
        return 0.0
    if sym in _live_equity_cfd:
        # .CASH.IP equity CFDs: IG size = shares; lot_sz is pip-tick value, not a size multiplier
        sized = round(desired_notional_usd / price_usd, 2)
    elif sym in _live_fx_instruments:
        # FX pairs (CS.D.*.CFD.IP): IG lot_sz is pip VALUE per lot (e.g. $10/pip for EURUSD),
        # NOT the base-currency lot size. Correct unit value = lot_sz / pip_sz = 100,000 units/lot.
        # Using lot_sz * price gives ~10,000x underestimate -> INSUFFICIENT_FUNDS on every FX order.
        pip_sz = _live_pip_sizes.get(sym, _LIVE_FX_PIP)
        unit_val = lot_sz / pip_sz if pip_sz > 0 else lot_sz * price_usd
        if unit_val <= 0:
            return 0.0
        sized = round(desired_notional_usd / unit_val, 2)
    else:
        # IG P&L uses native price points; price_unit in denominator gives 100x over-sizing for
        # OIL/SILVER where pip_sz=0.01 (ratio = desired_notional * stop_pct / pip_sz vs intended = * stop_pct).
        unit_val = lot_sz * mid_price
        if unit_val <= 0:
            return 0.0
        sized = round(desired_notional_usd / unit_val, 2)
    return max(min_deal, sized)


def _live_compute_stop_pts(sym: str, stop_pct: float, mid_price: float = 0.0) -> int:
    """Convert fractional stop loss to IG stop distance in points.

    Points = price x stop_pct / pip_size, where pip_size is per-instrument
    (fetched from IG LIVE /markets/{epic} onePipMeans at startup).
    Falls back to _LIVE_FX_PIP (0.0001) for instruments whose fetch failed.
    Minimum enforced: 4 pts (IG documented minimum stop distance).
    """
    pip_sz     = _live_pip_sizes.get(sym, _LIVE_FX_PIP)
    price_unit = _live_price_unit.get(sym, 1.0)
    ref_price  = mid_price if mid_price > 0 else _live_entry_price_ref(sym)
    pts        = int(ref_price * price_unit * stop_pct / pip_sz)
    return max(_live_min_stop_pts.get(sym, 4) + 1, pts)


def _live_entry_price_ref(sym: str) -> float:
    """Approximate current mid price for sym for stop-point calculation (best-effort).

    Uses live lot size and min_deal fetched from LIVE IG API:
      native_mid ~= min_notional / (min_deal x lot_sz x price_unit)
    price_unit=0.01 for cent-denominated instruments (Silver BMU), 1.0 for others.
    This is exact when min_notional was set at current price; acceptable approximation
    otherwise -- stop distance uses actual fill price inside IG platform.
    """
    mn         = _sim_min_notional.get(sym, 0.0)
    lot_sz     = _live_lot_sizes.get(sym, _LIVE_LOT_SIZE_FX)
    min_deal   = _live_min_deal.get(sym, 1.0)
    price_unit = _live_price_unit.get(sym, 1.0)
    denom      = min_deal * lot_sz * price_unit
    if mn > 0 and denom > 0:
        return mn / denom    # native_mid ~= min_n / (minDeal x lotSz x price_unit)
    return 1.0   # safe fallback



def _live_macro_confluence(sym: str, signal_dir: str) -> tuple:
    """Soft Macro Confluence Model — scale pos_size by Claudia directional alignment.

    Reads _claudia_directive_notes (refreshed each cycle from session_directive)
    and checks claudia_sector_momentum freshness (TTL ~12 min) to guard staleness.

    Returns:
        macro_scale : float — 1.0 (aligned), 0.8 (neutral/stale/missing), 0.5 (conflict)
        claudia_dir : int   — +1 (Bullish), -1 (Bearish), 0 (Neutral/stale)
        note        : str   — log tag for the confluence log line
        compress_sl : bool  — True only when macro_scale == 0.5 (counter-trend)
    """
    notes = _claudia_directive_notes
    if not notes:
        return 0.8, 0, "missing", False

    # Freshness: claudia_sector_momentum has a short TTL (~12 min, refreshed each cycle).
    # If its timestamp is >_MACRO_STALE_SECS old, Claudia has not run recently.
    try:
        _csm_raw = _redis().get("claudia_sector_momentum")
        if not _csm_raw:
            return 0.8, 0, "stale", False
        _csm_ts = json.loads(_csm_raw).get("timestamp", 0)
        if _csm_ts and (time.time() - _csm_ts) > _MACRO_STALE_SECS:
            return 0.8, 0, "stale", False
    except Exception:
        return 0.8, 0, "stale", False

    avoid          = notes.get("avoid", [])
    thesis_sectors = notes.get("thesis_sectors", [])
    high_conv      = notes.get("high_conviction", [])
    sym_sectors    = _JUNE_SECTOR_MAP.get(sym, set())

    # Derive Claudia directional bias: +1 = Bullish, -1 = Bearish, 0 = Neutral
    claudia_dir = 0
    if sym in high_conv:
        claudia_dir = 1                                     # explicitly bullish on this symbol
    elif sym in avoid:
        claudia_dir = -1                                    # explicitly bearish / avoid
    elif sym_sectors:
        if any(s in thesis_sectors for s in sym_sectors):
            claudia_dir = 1                                 # symbol's sector is in thesis
        elif any(s in avoid for s in sym_sectors):
            claudia_dir = -1                                # symbol's sector is avoided

    if claudia_dir == 0:
        return 0.8, 0, "neutral", False

    local_dir = 1 if signal_dir == "long" else -1
    if claudia_dir == local_dir:
        return 1.0, claudia_dir, "aligned", False
    else:
        return 0.5, claudia_dir, "conflict", True           # counter-trend: halve size, tighten SL


# ── Order deal confirmation ───────────────────────────────────────────────────

def _live_confirm_deal(deal_ref: str, retries: int = 4) -> Optional[dict]:
    """Poll /confirms/{dealReference} with exponential backoff until status != PENDING.
    Backoff: [0.2, 0.5, 1.0, 2.0]s. Returns confirmed dict or None after all retries.
    """
    _backoff = [0.2, 0.5, 1.0, 2.0]
    for attempt in range(retries):
        time.sleep(_backoff[min(attempt, len(_backoff) - 1)])
        data = _ig_live_get(f"/confirms/{deal_ref}", version="1")
        if data:
            status = data.get("dealStatus", "")
            if status != "PENDING":
                return data
            _live_log(f"confirm {deal_ref}: PENDING (attempt {attempt+1}/{retries})")
    _live_log(f"confirm {deal_ref}: PENDING after {retries} attempts -- aborting")
    return None


# ── Order placement (Part 5) — gated behind _live_trade_guard() ──────────────

def _live_open_position(sym: str, direction: str, signals: dict,
                        pos_size: float, leverage: int, conviction: int,
                        stop_mult: float = 1.0, htf_bias: str = "unknown") -> None:
    """Place a real BUY/SELL order on the IG live account.

    STRUCTURALLY GATED: _live_trade_guard() is the first call. No code path can
    reach the actual POST without the guard returning True. With the switch OFF,
    a detailed WOULD-BUY log fires instead so Jevon can review decision quality.

    Args:
        sym:       instrument key (e.g. "NZDUSD")
        direction: "long" or "short"
        signals:   current price signals dict
        pos_size:  USD position size (from sizing logic)
        leverage:  conviction-derived leverage multiplier
        conviction: 1-10 conviction score
    """
    sig       = signals.get(sym, {})
    mid_price = sig.get("price", 0.0)
    if mid_price <= 0:
        _live_log(f"open_position aborted: no price for {sym}")
        return

    notional  = pos_size * leverage
    ig_size    = _live_compute_ig_size(sym, notional, mid_price)
    lot_sz     = _live_lot_sizes.get(sym, _LIVE_LOT_SIZE_FX)  # per-instrument lot size
    price_unit = _live_price_unit.get(sym, 1.0)
    if sym in _live_equity_cfd:
        actual_n = ig_size * mid_price * price_unit          # equity CFD: size = shares
    elif sym in _live_fx_instruments:
        _pip_sz_n = _live_pip_sizes.get(sym, _LIVE_FX_PIP)
        actual_n  = ig_size * (lot_sz / _pip_sz_n) if _pip_sz_n > 0 else 0.0  # FX: size × 100k
    else:
        actual_n = ig_size * lot_sz * mid_price  # native-price notional; pnl_pct uses same native prices, price_unit cancels
    # Display-only USD notional for logs — isolated from pos["notional"] and dollar_pnl.
    # Commodity instruments: native notional * price_unit -> USD. Equity/FX: already USD.
    _log_n = (actual_n * price_unit
               if sym not in _live_equity_cfd and sym not in _live_fx_instruments
               else actual_n)
    # Equity CFD leverage gate — minDeal = 1 share inflates effective leverage when
    # account is too small for pos_size × leverage to cover one share. Block cleanly
    # rather than allow effective leverage to silently exceed the phase ceiling.
    # Not triggered for SILVER/OIL (not in _live_equity_cfd).
    if sym in _live_equity_cfd and pos_size > 0:
        _eff_lev = _log_n / pos_size
        if _eff_lev > leverage + 0.5:  # 0.5 tolerance for float rounding at exact boundary
            _live_log(
                f"\U0001f6ab EQUITY LEV GATE: {sym} blocked — minDeal clamp produced "
                f"{_eff_lev:.1f}\u00d7 effective leverage "
                f"(${_log_n:.2f} actual / ${pos_size:.2f} pos) "
                f"vs intended {leverage}:1 — account too small to size one share "
                f"within phase leverage control"
            )
            return
    _spread_floor = _sim_get_spread_floor(sym)
    stop_pct      = max(_sim_get_dynamic_stop(sym), _spread_floor)
    if stop_mult != 1.0:
        stop_pct = round(stop_pct * stop_mult, 6)  # counter-trend SL compression
    # Leverage-scaled stop tightening — dormant below _LIVE_PHASE_GATE_BAL ($200).
    # Normalised to phase floor (3x = _LIVE_PHASE_CONSERVATIVE_LEV): the most
    # conservative phase trade is unaffected; stops tighten proportionally above that.
    # sqrt() dampens the tightening so high-conviction edge is preserved.
    # Spread floor clamped as hard minimum — stops never tighten into spread noise.
    # NOT empirically validated at $200+: reasoned starting point, revisit when data exists.
    if _live.get("balance", 0.0) >= _LIVE_PHASE_GATE_BAL and leverage > _LIVE_PHASE_CONSERVATIVE_LEV:
        _lev_mult = (_LIVE_PHASE_CONSERVATIVE_LEV / leverage) ** 0.5
        stop_pct  = max(round(stop_pct * _lev_mult, 6), _spread_floor)
        _live_log(
            f"  [LEV-STOP] {sym} {leverage}:1 x{_lev_mult:.3f} "
            f"(floor {_LIVE_PHASE_CONSERVATIVE_LEV}:1) -> {stop_pct*100:.4f}% "
            f"(spread floor {_spread_floor*100:.4f}%)"
        )
    tp_pct    = _sim_get_tp(sym, direction, conviction)
    _tp_spread_floor_l = _sim_get_spread_floor(sym) / 2
    if tp_pct < _tp_spread_floor_l:
        tp_pct = _tp_spread_floor_l
    stop_dist = _live_compute_stop_pts(sym, stop_pct, mid_price)

    ig_direction = "BUY" if direction == "long" else "SELL"
    epic         = INSTRUMENTS.get(sym, "")

    _live_log(
        f"{'WOULD-BUY' if not _june_live_trading_enabled else 'BUY'}: "
        f"{sym} {ig_direction} size={ig_size} (notional ~${_log_n:.2f}) | "
        f"conviction {conviction}/10 lev {leverage}:1 | "
        f"stop {stop_pct*100:.2f}% ({stop_dist}pts) TP {tp_pct*100:.2f}%"
    )

    # Commission gate: equity CFDs (MU, INTC, AAPL etc) cost $9/side = $18 round-trip.
    # Block any trade where TP expected gross < $18 — structurally guaranteed to lose.
    if sym in _live_equity_cfd:
        _exp_gross = actual_n * tp_pct
        _rt_comm   = _IG_EQUITY_COMMISSION_USD * 2
        if _exp_gross < _rt_comm:
            _live_log(
                f"🚫 COMMISSION GATE: {sym} blocked — "
                f"expected gross ${_exp_gross:.2f} < ${_rt_comm:.2f} round-trip "
                f"commission (notional ${actual_n:.2f} TP {tp_pct*100:.2f}%)."
            )
            return

    # Pre-order margin check — catches weekend-uplifted margins that slip past eligibility filter.
    # FX excluded: tiny notional sizes make INSUFFICIENT_FUNDS structurally impossible there.
    # Missing margin data (0.0) is fail-CLOSED for non-FX: blocks the trade rather than
    # silently allowing it through unchecked. Arises from rate-limited startup fetch.
    _margin_raw = _live_margin.get(sym, 0.0)
    if sym not in _live_fx_instruments:
        if _margin_raw <= 0:
            _live_log(f"🚫 MARGIN GATE: {sym} skip — margin not loaded (fail-closed) — no cooldown")
            return
        _mfrac  = _real_margin_fraction(sym, _margin_raw)
        _usd_n  = (ig_size * mid_price * price_unit if sym in _live_equity_cfd
                   else ig_size * lot_sz * mid_price * price_unit)
        _req_mg = _usd_n * _mfrac
        _avail  = _live.get("balance_total", 0.0)
        if _req_mg > _avail:
            _live_log(
                f"🚫 MARGIN GATE: {sym} skip — est. margin ${_req_mg:.2f} "
                f"> balance ${_avail:.2f} (rate={_margin_raw:.0%}) — no cooldown"
            )
            return

    if not _live_trade_guard():  # ← structural gate: no order without this passing
        return

    # ── Real order placement ──────────────────────────────────────────────────
    order_body = {
        "epic":          epic,
        "expiry":        "-",
        "direction":     ig_direction,
        "size":          ig_size,
        "orderType":     "MARKET",
        "timeInForce":   "FILL_OR_KILL",
        "guaranteedStop": False,
        "forceOpen":     True,
        "currencyCode":  "USD",
        # stopDistance omitted for FX (CS.D.*.CFD.IP): prior 404-on-confirm rejections
        # were caused by FX lot-formula sizing issues, not the stop field itself.
        # Non-FX instruments (OIL, SILVER, GOLD) attach a broker-side hard stop below.
    }
    if sym not in _live_fx_instruments:
        order_body["stopDistance"] = stop_dist

    resp = _ig_live_post("/positions/otc", order_body, version="2")
    if not resp:
        _live_log(f"open_position: POST failed for {sym}")
        return

    deal_ref = resp.get("dealReference", "")
    if not deal_ref:
        _live_log(f"open_position: no dealReference in response for {sym}")
        return

    # Confirm fill
    confirm = _live_confirm_deal(deal_ref)
    if not confirm:
        _live_log(f"open_position: could not confirm {deal_ref} for {sym}")
        return

    status = confirm.get("dealStatus", "")
    if status != "ACCEPTED":
        _live_log(f"open_position: {sym} deal {status}: {confirm.get('reason', '?')} — 10min cooldown")
        _live.setdefault("pause_expiry", {})[_sim_combo_key(sym, direction)] = time.time() + 600
        return

    fill_price = float(confirm.get("level", mid_price))
    deal_id    = confirm.get("dealId", "")

    # Verify broker-side stop was echoed in the confirm (non-FX only).
    # IG returns stopLevel when the stop was accepted alongside the position.
    _broker_stop_level = None
    if sym not in _live_fx_instruments:
        _broker_stop_level = confirm.get("stopLevel")
        if _broker_stop_level:
            _live_log(
                f"🛑 Broker stop confirmed: {sym} stopLevel={_broker_stop_level} "
                f"(sent stopDistance={stop_dist}pts)"
            )
        else:
            _live_log(
                f"⚠️  open_position: {sym} — broker stop NOT in IG confirm "
                f"(sent {stop_dist}pts, stopLevel absent); polling-only exits active"
            )
    _live["open_position"] = {
        "instrument":   sym,
        "direction":    direction,
        "deal_id":      deal_id,
        "deal_ref":     deal_ref,
        "fill_price":   fill_price,
        "ig_size":      ig_size,
        "pos_size":     pos_size,
        "leverage":     leverage,
        "notional":     actual_n,
        "stop_pct":     stop_pct,
        "tp_pct":       tp_pct,
        "stop_dist":    stop_dist,
        "broker_stop_level": _broker_stop_level,
        "entry_time":   time.time(),
        "entry_vol":    abs(sig.get("change_5m", 0.0)),
        "entry_change_15m": sig.get("change_15m") or 0.0,
        "conviction":   conviction,
        "claudia_pts":  _sim_claudia_pts(sym, direction),
        "initial_sl_pct": stop_pct,  # baseline for time-decay SL compression
        "reversal_count": 0,
        "entry_sar":      round(sig.get("spread_atr_ratio") or 0.0, 4),
        "persistence_confirmed": _last_cycle_direction.get(sym) == ("bull" if direction == "long" else "bear"),
        "htf_bias":     htf_bias,  # observation-only; never gates or conviction
    }
    _live["total_trades"]    = _live.get("total_trades", 0) + 1
    _live_log(
        f"✅ LIVE POSITION OPENED: {sym} {ig_direction} @ {fill_price:.5f} "
        f"| size {ig_size} | notional ${_log_n:.2f} | deal {deal_id}"
    )
    _live_save_state()


# ── Lightstreamer Phase 1: session + dual-system position guard ───────────────

def _ls_init_session(endpoint: str, cst: str, xst: str, account_id: str) -> None:
    """Initialize or reinitialize Lightstreamer ACCOUNT+TRADE subscriptions.

    Called from authenticate_live() on login and on 6h session token refresh.
    Safe to call multiple times — disconnects old client before creating new one.
    DISCONNECTED:WILL-RETRY is handled automatically by the LS library.
    Terminal DISCONNECTED is resolved on the next authenticate_live() call.
    """
    global _ls_client, _ls_connected, _ls_account, _ls_confirms_closed, _ls_account_id
    if not endpoint or not account_id:
        print(f"[{_ts()}] [LS] skipped -- missing endpoint or account_id", flush=True)
        return
    try:
        from lightstreamer.client import LightstreamerClient, Subscription
    except ImportError:
        print(f"[{_ts()}] [LS] lightstreamer-client-lib not installed -- LS disabled", flush=True)
        return

    if _ls_client is not None:
        try:
            _ls_client.disconnect()
        except Exception:
            pass
    _ls_client     = None
    _ls_connected  = False
    _ls_account_id = account_id
    with _ls_confirms_lock:
        _ls_confirms_closed.clear()

    client = LightstreamerClient(endpoint, "DEFAULT")
    client.connectionDetails.setUser(account_id)
    client.connectionDetails.setPassword("CST-" + cst + "|XST-" + xst)

    class _ConnListener:
        def onStatusChange(self, status: str) -> None:
            global _ls_connected
            _ls_connected = status.startswith("CONNECTED")
            print(f"[{_ts()}] [LS] {status}", flush=True)
        def onPropertyChange(self, prop: str) -> None: pass
        def onServerError(self, code: int, msg: str) -> None:
            print(f"[{_ts()}] [LS] server error {code}: {msg}", flush=True)
        def onListenStart(self) -> None: pass
        def onListenEnd(self) -> None: pass

    class _AccountListener:
        _FIELDS = ("PNL", "DEPOSIT", "AVAILABLE_CASH", "MARGIN", "EQUITY")
        def onItemUpdate(self, update) -> None:
            changed = {fld: update.getValue(fld)
                       for fld in self._FIELDS if update.getValue(fld) is not None}
            with _ls_account_lock:
                _ls_account.update(changed)
            parts = " ".join(k + "=" + str(v) for k, v in changed.items())
            print(f"[{_ts()}] [LS ACCT] {parts}", flush=True)
        def onSubscription(self) -> None:
            print(f"[{_ts()}] [LS] ACCOUNT:{account_id} subscribed", flush=True)
        def onUnsubscription(self) -> None: pass
        def onSubscriptionError(self, code: int, msg: str) -> None:
            print(f"[{_ts()}] [LS] ACCOUNT sub error {code}: {msg}", flush=True)
        def onEndOfSnapshot(self, item: str, pos: int) -> None: pass
        def onListenStart(self, sub) -> None: pass
        def onListenEnd(self, sub) -> None: pass
        def onClearSnapshot(self, item: str, pos: int) -> None: pass
        def onLostUpdates(self, item: str, pos: int, count: int) -> None: pass
        def onRealMaxFrequency(self, freq) -> None: pass

    class _TradeListener:
        def onItemUpdate(self, update) -> None:
            raw = update.getValue("CONFIRMS")
            if not raw:
                return
            try:
                payload = json.loads(raw)
                for deal in payload.get("affectedDeals") or []:
                    if deal.get("status") == "FULLY_CLOSED":
                        closed_id = deal.get("dealId") or ""
                        if closed_id:
                            with _ls_confirms_lock:
                                _ls_confirms_closed.add(closed_id)
                            print(
                                f"[{_ts()}] [LS TRADE] CONFIRMS FULLY_CLOSED: {closed_id}"
                                f" epic={payload.get('epic')} profit={payload.get('profit')}",
                                flush=True,
                            )
            except (json.JSONDecodeError, AttributeError) as exc:
                print(f"[{_ts()}] [LS TRADE] CONFIRMS parse error: {exc}", flush=True)
        def onSubscription(self) -> None:
            print(f"[{_ts()}] [LS] TRADE:{account_id} subscribed", flush=True)
        def onUnsubscription(self) -> None: pass
        def onSubscriptionError(self, code: int, msg: str) -> None:
            print(f"[{_ts()}] [LS] TRADE sub error {code}: {msg}", flush=True)
        def onEndOfSnapshot(self, item: str, pos: int) -> None:
            print(f"[{_ts()}] [LS] TRADE snapshot complete", flush=True)
        def onListenStart(self, sub) -> None: pass
        def onListenEnd(self, sub) -> None: pass
        def onClearSnapshot(self, item: str, pos: int) -> None: pass
        def onLostUpdates(self, item: str, pos: int, count: int) -> None: pass
        def onRealMaxFrequency(self, freq) -> None: pass

    client.addListener(_ConnListener())

    acct_sub = Subscription("MERGE", ["ACCOUNT:" + account_id],
                            ["PNL", "DEPOSIT", "AVAILABLE_CASH", "MARGIN", "EQUITY"])
    acct_sub.addListener(_AccountListener())
    client.subscribe(acct_sub)

    trade_sub = Subscription("DISTINCT", ["TRADE:" + account_id], ["CONFIRMS", "OPU", "WOU"])
    trade_sub.addListener(_TradeListener())
    client.subscribe(trade_sub)

    client.connect()
    _ls_client = client
    print(f"[{_ts()}] [LS] initialized: ACCOUNT+TRADE for {account_id} @ {endpoint}", flush=True)


def _ls_get_margin() -> Optional[float]:
    """Real-time MARGIN from LS ACCOUNT subscription, or None if not connected/received."""
    if not _ls_connected:
        return None
    with _ls_account_lock:
        raw = _ls_account.get("MARGIN")
    if raw is None:
        return None
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None


def _ls_deal_closed(deal_id: str) -> bool:
    """True if LS TRADE stream confirmed deal_id as FULLY_CLOSED."""
    with _ls_confirms_lock:
        return deal_id in _ls_confirms_closed


def _ls_position_guard_check(sym: str, deal_id: str) -> tuple:
    """Dual-system pre-send guard: LS primary, REST fallback, disagreement detection.

    Always runs both systems when LS is live so disagreements surface immediately.
    On disagreement: sets manual_review_required and returns (True, "DISAGREEMENT").

    Return codes:
      (True/False, "LS+REST")       both agree
      (True/False, "LS-only")       LS live, REST unavailable
      (True/False, "REST-fallback") LS offline, REST answered
      (True,       "DISAGREEMENT")  systems disagree -- manual_review_required set
      (None,       "unavailable")   both systems failed
    """
    ls_margin = _ls_get_margin()

    # Always run REST too when LS is live -- needed for disagreement detection
    rest_dep: Optional[float] = None
    _acct_data = _ig_live_get("/accounts", version="1")
    if _acct_data is not None:
        for _ac in _acct_data.get("accounts", []):
            if _ac.get("preferred"):
                rest_dep = float(_ac.get("balance", {}).get("deposit", 0) or 0)
                break

    if ls_margin is not None and rest_dep is not None:
        ls_open   = ls_margin > 0
        rest_open = rest_dep  > 0
        if ls_open != rest_open:
            _live_log(
                f"POSITION GUARD DISAGREEMENT [{sym}]: "
                f"LS margin={ls_margin:.2f} ({'open' if ls_open else 'flat'}) "
                f"vs REST deposit={rest_dep:.2f} ({'open' if rest_open else 'flat'}) "
                f"-- treating as OPEN (cautious). Setting manual_review_required."
            )
            _live["manual_review_required"] = True
            _live_save_state()
            return (True, "DISAGREEMENT")
        return (ls_open, "LS+REST")

    if ls_margin is not None:
        return (ls_margin > 0, "LS-only")

    if rest_dep is not None:
        _live_log(
            f"[{sym}] [guard] LS offline -- deposit REST fallback (deposit={rest_dep:.2f})"
        )
        return (rest_dep > 0, "REST-fallback")

    return (None, "unavailable")


def _live_close_position(exit_reason: str, signals: dict) -> None:
    """Close the current live position via an opposing IG market order.

    STRUCTURALLY GATED: _live_trade_guard() is the first call. With switch OFF,
    logs WOULD-SELL and returns without placing any order.
    """
    pos = (_live.get("open_position") or {}).copy()
    if not pos:
        return

    sym       = pos["instrument"]
    dirn      = pos["direction"]
    ig_size   = pos.get("ig_size", 1)
    deal_id   = pos.get("deal_id", "")
    fill_px   = pos.get("fill_price", 0.0)
    entry_t   = pos.get("entry_time", time.time())

    # Reconstruct current price for P&L logging
    sig       = signals.get(sym, {})
    mid       = sig.get("price", fill_px)
    exit_px   = mid  # approximate — actual fill confirmed after order
    pnl_pct   = (exit_px - fill_px) / fill_px if dirn == "long" else \
                (fill_px - exit_px) / fill_px
    lot_sz_c  = _live_lot_sizes.get(sym, _LIVE_LOT_SIZE_FX)
    notional  = pos.get("notional", pos.get("ig_size", 1) * lot_sz_c * mid)
    dollar_pnl = notional * pnl_pct
    hold_min  = (time.time() - entry_t) / 60.0
    close_dir = "SELL" if dirn == "long" else "BUY"
    epic      = INSTRUMENTS.get(sym, "")

    _live_log(
        f"{'WOULD-SELL' if not _june_live_trading_enabled else 'SELL'}: "
        f"{sym} {close_dir} | P&L ~{pnl_pct*100:+.2f}% (${dollar_pnl:+.2f}) | "
        f"hold {hold_min:.0f}m | {exit_reason}"
    )

    if not _live_trade_guard():  # ← structural gate
        return

    # Pre-send guard: deposit via /accounts is the sole reliable signal on this
    # IG live account. GET /positions/otc and GET /positions/otc/{dealId} return 404
    # regardless of position state — confirmed by live diagnostic 2026-08-27.
    # manual_review_required blocks subsequent close attempts when post-close
    # verification detected an anomaly (cascade protection — see race-condition trace).
    if deal_id:
        if _live.get("manual_review_required"):
            _live_log(
                f"⚠️  {sym}: manual_review_required flag set — close blocked pending "
                f"reconciliation (cascade protection). No orders until IG flat confirmed."
            )
            return
        _guard_open, _guard_src = _ls_position_guard_check(sym, deal_id)
        if _guard_src == "DISAGREEMENT":
            return  # manual_review_required already set inside _ls_position_guard_check
        if _guard_open is None:
            _live_log(
                f"⚠️  {sym}: both LS and /accounts unavailable before close — "
                f"preserving state (cautious)."
            )
            return
        if not _guard_open:
            _live_log(
                f"⚠️  {sym}: position already gone [{_guard_src}] — "
                f"suppressed. Reconciling."
            )
            _live_reconcile_positions()
            _live_save_state()
            return
        _live_log(f"[pre-send guard: position open [{_guard_src}]] proceeding with close")
        # Position confirmed open. Proceed with close order.

    # ── Real close order ──────────────────────────────────────────────────────
    close_body = {
        "epic":        epic,
        "expiry":      "-",
        "direction":   close_dir,
        "size":        ig_size,
        "orderType":   "MARKET",
        "timeInForce": "FILL_OR_KILL",
        "forceOpen":     False,         # required by IG v1 API — absent field treated as null → HTTP 400
        "guaranteedStop": False,         # required by IG v1 API — absent field treated as null → HTTP 400
        "currencyCode":   "USD",         # required by IG v1 API — absent field treated as null → HTTP 400
        "dealId":         deal_id,
    }
    resp = _ig_live_post("/positions/otc", close_body, version="1")
    if not resp:
        _live_log(f"close_position: POST failed for {sym} — position may still be open")
        return

    deal_ref = resp.get("dealReference", "")
    confirm  = _live_confirm_deal(deal_ref) if deal_ref else None

    if confirm and confirm.get("dealStatus") == "ACCEPTED":
        real_exit  = float(confirm.get("level", exit_px))
        real_pnl_p = (real_exit - fill_px) / fill_px if dirn == "long" else \
                     (fill_px - real_exit) / fill_px
        real_dollar = notional * real_pnl_p
        commission  = _IG_EQUITY_COMMISSION_USD * 2 if sym in _live_equity_cfd else 0.0
        net_dollar  = real_dollar - commission
        _partial_pnl_c = pos.get("partial_dollar_pnl", 0.0)
        won         = (net_dollar + _partial_pnl_c) > 0
        _live_log(
            f"✅ LIVE POSITION CLOSED: {sym} @ {real_exit:.5f} | "
            f"gross {real_pnl_p*100:+.2f}% (${real_dollar:+.2f})"
            + (f" net ${net_dollar:+.2f} after ${commission:.0f} commission" if commission else "")
            + f" | {exit_reason}"
        )

        # Record trade
        trade_rec = {
            "instrument":   sym,
            "direction":    dirn,
            "entry_price":  fill_px,
            "exit_price":   real_exit,
            "ig_size":      ig_size,
            "notional":     round(notional, 2),
            "pnl_pct":      round(real_pnl_p, 6),
            "dollar_pnl":   round(net_dollar, 4),
            "commission":   round(commission, 2),
            "hold_min":     round(hold_min, 1),
            "exit_epoch":   int(time.time()),
            "exit_reason":  exit_reason,
            "conviction":   pos.get("conviction", 0),
            "claudia_pts":  pos.get("claudia_pts", 0.0),
        }
        hist = _live.setdefault("trade_history", [])
        hist.append(trade_rec)
        if len(hist) > 50:
            _live["trade_history"] = hist[-50:]

        # Update totals
        _live["total_wins"]   = _live.get("total_wins", 0)   + int(won)
        _live["total_losses"] = _live.get("total_losses", 0) + int(not won)
        if dirn == "long":
            _live["long_pnl"]    = round(_live.get("long_pnl", 0.0)  + net_dollar, 4)
            _live["long_trades"] = _live.get("long_trades", 0) + 1
            if won: _live["long_wins"] = _live.get("long_wins", 0) + 1
        else:
            _live["short_pnl"]    = round(_live.get("short_pnl", 0.0) + net_dollar, 4)
            _live["short_trades"] = _live.get("short_trades", 0) + 1
            if won: _live["short_wins"] = _live.get("short_wins", 0) + 1

        _live_update_streak(sym, dirn, won)
        _htf_b_c = pos.get("htf_bias", "unknown")
        if _htf_b_c not in ("unknown", None):
            _live_write_htf_event(sym, dirn, _htf_b_c, 0.0, fill_px)
        _live_perf_record(sym, won, sig.get("spread_atr_ratio"), pnl_dollar=net_dollar,
                          entry_sar=pos.get("entry_sar"), persistence_confirmed=pos.get("persistence_confirmed"))
        _sim_15m_record(sym, dirn, pos.get("entry_change_15m") or 0.0, won)
        # Live phase stat update — only counted when above balance gate
        if _live.get("balance", 0.0) >= _LIVE_PHASE_GATE_BAL:
            _live["live_phase_trades"]  = _live.get("live_phase_trades", 0) + 1
            _live["live_phase_wins"]    = _live.get("live_phase_wins", 0) + int(won)
            _live["live_phase_losses"]  = _live.get("live_phase_losses", 0) + int(not won)
            if won:
                _live["live_phase_consec_losses"] = 0
            else:
                _live["live_phase_consec_losses"] = _live.get("live_phase_consec_losses", 0) + 1
            if _live.get("live_phase_entry_balance") is None:
                _live["live_phase_entry_balance"] = _live.get("balance", 0.0)
            _live_check_phase()
        # After stop_loss: block the stopped direction for 10 min (same-dir cooldown)
        # and block ALL directions on this instrument for 15 min (instrument cooldown).
        # Prevents an immediate direction-flip into the same noise that stopped us out.
        if exit_reason == "stop_loss":
            _sl_exp = time.time() + 10 * 60
            _live.setdefault("pause_expiry", {})[_sim_combo_key(sym, dirn)] = _sl_exp
            _live_log(f"⏸️ [SAME-DIR COOLDOWN] {sym} {dirn} blocked for 10m. Opposing direction remains active.")
            _instr_exp = time.time() + 15 * 60
            _live.setdefault("instrument_cooldown", {})[sym] = _instr_exp
            _live_log(f"⏸️ [INSTRUMENT COOLDOWN] {sym} all-direction blocked for 15m after stop-out")
            # Track stop-outs per instrument for per-instrument defensive mode
            _so_ct = _live.setdefault("instrument_stopouts_today", {})
            _so_ct[sym] = _so_ct.get(sym, 0) + 1
            if _so_ct[sym] >= _LIVE_DEF_INSTR_STOPOUTS:
                _imd = _live.setdefault("instrument_mode", {})
                if _imd.get(sym) != "defensive":
                    _imd[sym] = "defensive"
                    _live.setdefault("instrument_mode_entered_at", {})[sym] = time.time()
                    _live_log(
                        f"⛔️ [DEFENSIVE] {sym}: NORMAL -> DEFENSIVE "
                        f"({_so_ct[sym]} stop-outs today >= {_LIVE_DEF_INSTR_STOPOUTS})"
                    )
        # Record wins on instruments in defensive mode (enables P&L recovery condition)
        if won and (_live.get("instrument_mode") or {}).get(sym) == "defensive":
            _live.setdefault("instrument_won_after_def", {})[sym] = 1
            _live_log(f"📈 [DEFENSIVE] {sym}: win recorded in defensive mode (recovery pending)")
    else:
        _live_log(f"close_position: confirm failed or rejected for {sym} — check IG manually")
        _live_log(f"⚠️  close_position: ambiguous IG state for {sym} — running mid-cycle reconciliation")
        _live_reconcile_positions()
        _live_save_state()
        return  # open_position set by reconciliation; entry guard blocks new trades if still open

    # Post-close verification: confirm IG is actually flat before clearing state.
    # /positions/otc returns 404 for BOTH "genuinely flat" AND "transitional state
    # with orphan position" — indistinguishable on 404 alone. Use account balance
    # as discriminator: deposit=0 means no margin in use → genuinely flat.
    time.sleep(2)  # initial wait for IG to settle
    _VERIFY_MAX    = 3
    _post_resolved = False  # True only when flat is confirmed
    _pv_deal_id    = deal_id  # captured for LS confirms check inside loop

    def _get_preferred_deposit() -> float:
        """Return deposit of preferred account via /accounts, or -1.0 on failure."""
        _bd = _ig_live_get("/accounts", version="1")
        if _bd is None:
            return -1.0
        for _ac in _bd.get("accounts", []):
            if _ac.get("preferred"):
                return float(_ac.get("balance", {}).get("deposit", 0) or 0)
        return -1.0

    def _post_dual_verify(_vi_n: int) -> Optional[bool]:
        """Dual-system flat check (LS + REST) for post-close 404 disambiguation.

        Mirrors _ls_position_guard_check philosophy: always run both when LS is
        live so disagreements surface immediately. Routes disagreement into the
        existing manual_review_required path rather than building new logic.

        Returns True=flat confirmed, False=blocked (disagreement), None=retry.
        """
        _ls_m  = _ls_get_margin()
        _ls_cl = _ls_deal_closed(_pv_deal_id) if _pv_deal_id else False
        _dep   = _get_preferred_deposit()
        _ls_ok = _ls_m is not None
        _rs_ok = _dep >= 0.0
        _ls_flat = _ls_ok and (_ls_m == 0.0 or _ls_cl)
        _rs_flat = _rs_ok and _dep == 0.0
        if _ls_ok and _rs_ok:
            if _ls_flat and _rs_flat:
                _live_log(
                    f"\u2705 POST-CLOSE VERIFY {_vi_n}/{_VERIFY_MAX}: "
                    f"LS+REST agree flat (margin={_ls_m:.2f}, deposit={_dep:.2f}) for {sym}"
                )
                return True
            if not _ls_flat and not _rs_flat:
                _live_log(
                    f"\u26a0\ufe0f  POST-CLOSE VERIFY {_vi_n}/{_VERIFY_MAX}: "
                    f"LS+REST both open (margin={_ls_m:.2f}, deposit={_dep:.2f}) for {sym}"
                    f" \u2014 retrying"
                )
                return None
            # Pyramid primary-closed-addon-survives promotion check:
            # If _ls_cl=True (primary CONFIRMS received) but deposit>0 (margin still
            # held by a remaining addon leg), this is NOT a genuine disagreement —
            # the primary closing is expected and the deposit belongs to the addon.
            # Promote the addon to primary when: exactly 1 addon leg, addon NOT in
            # _ls_confirms_closed (confirmed still open via LS + REST deposit>0).
            # Multi-addon case (>1) falls through to manual_review_required: ambiguous.
            _pyr_legs = _live.get("pyramid_legs", [])
            if _ls_cl and len(_pyr_legs) == 1:
                _addon_leg  = _pyr_legs[0]
                _addon_deal = _addon_leg.get("deal_id", "")
                if _addon_deal and not _ls_deal_closed(_addon_deal):
                    _agg_stop = _live.get("pyramid_agg_stop_level")
                    _a_fill   = _addon_leg.get("fill_price", 0.0)
                    _promoted = {
                        "instrument":         _addon_leg.get("instrument", sym),
                        "direction":          _addon_leg.get("direction", pos.get("direction")),
                        "deal_id":            _addon_deal,
                        "deal_ref":           _addon_leg.get("deal_ref", ""),
                        "fill_price":         _a_fill,
                        "ig_size":            _addon_leg.get("ig_size", 0.0),
                        "pos_size":           _addon_leg.get("notional", 0.0),
                        "leverage":           pos.get("leverage", 1),
                        "notional":           _addon_leg.get("notional", 0.0),
                        "stop_pct":           _addon_leg.get("stop_pct", pos.get("stop_pct", 0.005)),
                        "initial_sl_pct":     _addon_leg.get("stop_pct", pos.get("stop_pct", 0.005)),
                        "tp_pct":             _addon_leg.get("tp_pct", pos.get("tp_pct", 0.01)),
                        "stop_dist":          abs(_agg_stop - _a_fill) if _agg_stop and _a_fill else 0.0,
                        "broker_stop_level":  _agg_stop,
                        "entry_time":         _addon_leg.get("entry_time", time.time()),
                        "entry_vol":          0.0,
                        "entry_change_15m":   0.0,
                        "conviction":         pos.get("conviction", 5),
                        "claudia_pts":        pos.get("claudia_pts", 0),
                        "reversal_count":     0,
                        "entry_sar":          0.0,
                        "persistence_confirmed": False,
                        "htf_bias":           None,
                        "_pyramid_promoted":  True,
                    }
                    _live_log(
                        f"[PYRAMID PROMOTE] {sym}: primary {_pv_deal_id} FULLY_CLOSED "
                        f"| addon {_addon_deal} open (LS+REST deposit={_dep:.2f}) "
                        f"| promoting addon to primary "
                        f"| fill={_a_fill} broker_stop={_agg_stop}"
                    )
                    _live["open_position"]          = _promoted
                    _live["pyramid_legs"]           = []
                    _live["pyramid_agg_stop_level"] = None
                    _live_save_state()
                    return False  # caller returns without clearing the promoted open_position

            # Multi-addon legs, addon also FULLY_CLOSED, or no CONFIRMS — genuinely
            # ambiguous. Fall through to manual_review_required (cautious, correct).
            # Disagreement \u2014 route into existing manual_review_required path
            _live_log(
                f"\U0001f6a8 POST-CLOSE VERIFY {_vi_n}/{_VERIFY_MAX}: LS/REST DISAGREEMENT "
                f"margin={_ls_m:.2f} closed={_ls_cl} vs deposit={_dep:.2f} [{sym}]"
                f" \u2014 setting manual_review_required (cautious)."
            )
            _live["manual_review_required"] = True
            _live_save_state()
            return False
        if _ls_ok:
            if _ls_flat:
                _live_log(
                    f"\u2705 POST-CLOSE VERIFY {_vi_n}/{_VERIFY_MAX}: "
                    f"LS-only flat (margin={_ls_m:.2f}, REST unavailable) for {sym}"
                )
                return True
            return None
        if _rs_ok:
            if _rs_flat:
                _live_log(
                    f"\u2705 POST-CLOSE VERIFY {_vi_n}/{_VERIFY_MAX}: "
                    f"REST deposit=0 flat confirm for {sym}"
                )
                return True
            _live_log(
                f"\u26a0\ufe0f  POST-CLOSE VERIFY {_vi_n}/{_VERIFY_MAX}: "
                f"deposit={_dep:.2f}>0 for {sym} \u2014 retrying"
            )
            return None
        _live_log(
            f"\u26a0\ufe0f  POST-CLOSE VERIFY {_vi_n}/{_VERIFY_MAX}: "
            f"LS AND REST both failed for {sym} \u2014 retrying"
        )
        return None

    for _vi in range(_VERIFY_MAX):
        if _vi > 0:
            time.sleep(3)  # 3s between retries; worst-case extra: 6s
        _post_data = _ig_live_get("/positions/otc", version="1")
        if _post_data is None:
            # 404 / network error: dual-system disambiguation (LS primary + REST fallback)
            _pv_r = _post_dual_verify(_vi + 1)
            if _pv_r is True:
                _post_resolved = True
                break
            if _pv_r is False:
                return  # promotion or manual_review_required handled in _post_dual_verify
            continue
        _post_pos = _post_data.get("positions", [])
        if _post_pos:
            # 200 with open positions: possible orphan from broker-stop race
            _live_log(
                f"\u26a0\ufe0f  POST-CLOSE VERIFICATION: IG still shows {len(_post_pos)} open "
                f"position(s) after {sym} close confirm \u2014 possible broker-stop race. "
                f"Running reconciliation."
            )
            _live_reconcile_positions()
            _live_save_state()
            return  # open_position set by reconciliation; entry guard blocks new trades
        # 200 + empty list: IG confirmed flat
        _post_resolved = True
        break

    if not _post_resolved:
        # All retries exhausted AND balance showed deposit>0 on each attempt.
        # Fail safe: preserve open_position and block new entries.
        # manual_review_required prevents subsequent exit checks from sending new
        # close orders with a stale dealId (which would extend a cascade).
        # Flag is cleared by _live_reconcile_positions() when deposit=0 confirms flat.
        _live["manual_review_required"] = True
        _live_log(
            f"\U0001f6a8 POST-CLOSE VERIFICATION FAILED: could not confirm IG flat after "
            f"{_VERIFY_MAX} attempts for {sym} \u2014 open_position PRESERVED. "
            f"manual_review_required=True — all closes blocked until reconcile confirms flat."
        )
        _live_save_state()
        return  # intentionally NOT clearing open_position

    _live["open_position"] = None
    _live_save_state()


# ── Partial TP exit (live mirror of _sim_partial_tp_exit) ──────────────────────

def _live_partial_tp_exit(signals: dict) -> None:
    """Close 50% of live position at TP; let remaining 50% run with breakeven stop.

    Balance-gated: checks that half the current ig_size still clears the instrument's
    real minDeal (from _live_min_deal, populated at startup from IG API). If either
    half would be below minDeal, falls back to a full close at take_profit.

    Sim equivalent: _sim_partial_tp_exit(). Called from _live_check_exit when
    pnl_pct >= tp_pct and partial_exit_done is not yet set on the position.
    """
    pos = (_live.get("open_position") or {}).copy()
    if not pos:
        return

    sym     = pos["instrument"]
    dirn    = pos["direction"]
    ig_sz   = pos["ig_size"]
    fill_px = pos.get("fill_price", 0.0)
    deal_id = pos.get("deal_id", "")

    min_deal = _live_min_deal.get(sym, 1.0)
    half_sz  = round(ig_sz / 2, 4)
    # Both the close leg and the residual position must clear minDeal.
    if half_sz < min_deal or round(ig_sz - half_sz, 4) < min_deal:
        _live_log(
            f"⚠️ {sym}: partial TP not feasible "
            f"(ig_size {ig_sz:.4f} < 2×minDeal {2*min_deal:.4f}) — full close"
        )
        _live_close_position("take_profit", signals)
        return

    sig    = signals.get(sym, {})
    mid    = sig.get("price", fill_px)
    sp_pct = sig.get("spread_pct", 0.0)
    half_sp = mid * sp_pct / 200.0
    exit_px = (mid - half_sp) if dirn == "long" else (mid + half_sp)
    partial_pnl_pct = (
        (exit_px - fill_px) / fill_px if dirn == "long"
        else (fill_px - exit_px) / fill_px
    )

    lot_sz     = _live_lot_sizes.get(sym, _LIVE_LOT_SIZE_FX)
    price_unit = _live_price_unit.get(sym, 1.0)
    if sym in _live_equity_cfd:
        partial_notional = half_sz * mid * price_unit
    else:
        partial_notional = half_sz * lot_sz * mid  # native-price notional; pnl_pct uses same native prices, price_unit cancels

    close_dir = "SELL" if dirn == "long" else "BUY"
    epic      = INSTRUMENTS.get(sym, "")

    _live_log(
        f"✂️ LIVE PARTIAL TP: {sym} — closing {half_sz} of {ig_sz} lots "
        f"@ ~{exit_px:.5f} ({partial_pnl_pct*100:+.3f}%) | "
        f"remaining {round(ig_sz - half_sz, 4)} | minDeal check OK ({min_deal})"
    )

    if not _live_trade_guard():
        return

    # Pre-send guard: deposit-only (mirrors _live_close_position).
    # GET /positions/otc / per-deal GET always 404 on this account — confirmed 2026-08-27.
    if deal_id:
        if _live.get("manual_review_required"):
            _live_log(
                f"⚠️  {sym}: partial TP — manual_review_required flag set — "
                f"blocked pending reconciliation."
            )
            return
        _ptp_guard_open, _ptp_guard_src = _ls_position_guard_check(sym, deal_id)
        if _ptp_guard_src == "DISAGREEMENT":
            return  # manual_review_required already set
        if _ptp_guard_open is None:
            _live_log(
                f"⚠️  {sym}: partial TP — both LS and /accounts unavailable — "
                f"preserving state."
            )
            return
        if not _ptp_guard_open:
            _live_log(
                f"⚠️  {sym}: partial TP — position already gone [{_ptp_guard_src}] — "
                f"suppressed. Reconciling."
            )
            _live_reconcile_positions()
            _live_save_state()
            return
        _live_log(f"[partial TP guard: position open [{_ptp_guard_src}]] proceeding")
        # Position confirmed open. Proceed with partial close.

    close_body = {
        "epic":          epic,
        "expiry":        "-",
        "direction":     close_dir,
        "size":          half_sz,
        "orderType":     "MARKET",
        "timeInForce":   "FILL_OR_KILL",
        "forceOpen":     False,
        "guaranteedStop": False,
        "currencyCode":  "USD",
        "dealId":        deal_id,
    }
    resp = _ig_live_post("/positions/otc", close_body, version="1")
    if not resp:
        _live_log(f"partial_tp: POST failed for {sym} — falling back to full close")
        _live_close_position("take_profit", signals)
        return

    deal_ref = resp.get("dealReference", "")
    confirm  = _live_confirm_deal(deal_ref) if deal_ref else None

    if confirm and confirm.get("dealStatus") == "ACCEPTED":
        real_exit    = float(confirm.get("level", exit_px))
        real_pnl_p   = (
            (real_exit - fill_px) / fill_px if dirn == "long"
            else (fill_px - real_exit) / fill_px
        )
        partial_dollar_pnl = partial_notional * real_pnl_p

        # Tighten stop to breakeven (entry ± spread floor, so worst case ~flat).
        # Mirror of sim: _sim["open_position"]["stop_pct"] = _spread_flr
        _spread_flr = _sim_get_spread_floor(sym)

        # Post-confirm: verify IG shows a remaining position before updating
        # internal state. Catches broker-stop race on the partial close — same
        # class as the post-close verification in _live_close_position (ddb7de5).
        # Without this, a race leaves June managing a ghost half-position.
        time.sleep(2)
        _ptp_post = _ig_live_get("/positions/otc", version="1")
        if _ptp_post is None:
            _ptp_bd  = _ig_live_get("/accounts", version="1")
            _ptp_dep = 0.0
            if _ptp_bd is not None:
                for _ptp_ac in _ptp_bd.get("accounts", []):
                    if _ptp_ac.get("preferred"):
                        _ptp_dep = float(_ptp_ac.get("balance", {}).get("deposit", 0) or 0)
                        break
            if _ptp_dep == 0.0:
                _live_log(
                    f"⚠️  PARTIAL-TP POST-VERIFY: {sym} — IG flat after partial "
                    f"close (position fully gone). Clearing state."
                )
                _live["open_position"] = None
                _live_save_state()
                return
            _live_log(
                f"⚠️  PARTIAL-TP POST-VERIFY: {sym} — /positions/otc unavailable "
                f"(deposit={_ptp_dep:.2f}>0). Proceeding with state update."
            )
        else:
            _ptp_ig_pos = _ptp_post.get("positions", [])
            if not _ptp_ig_pos:
                _live_log(
                    f"⚠️  PARTIAL-TP POST-VERIFY: {sym} — IG flat after partial "
                    f"close (position fully gone). Clearing state."
                )
                _live["open_position"] = None
                _live_save_state()
                return
            if len(_ptp_ig_pos) > 1:
                _live_log(
                    f"⚠️  PARTIAL-TP POST-VERIFY: {sym} — {len(_ptp_ig_pos)} IG "
                    f"positions after partial close. Running reconciliation."
                )
                _live_reconcile_positions()
                _live_save_state()
                return
            _ptp_ig_deal = _ptp_ig_pos[0].get("position", {}).get("dealId", "")
            if _ptp_ig_deal and _ptp_ig_deal != deal_id:
                _live_log(
                    f"⚠️  PARTIAL-TP POST-VERIFY: {sym} — unexpected dealId "
                    f"{_ptp_ig_deal} after partial close (expected {deal_id}). "
                    f"Running reconciliation."
                )
                _live_reconcile_positions()
                _live_save_state()
                return
            # Single position with matching dealId confirmed — proceed with update

        remaining_sz = round(ig_sz - half_sz, 4)
        _live["open_position"]["ig_size"]            = remaining_sz
        _live["open_position"]["stop_pct"]           = _spread_flr
        _live["open_position"]["initial_sl_pct"]     = _spread_flr
        _live["open_position"]["partial_exit_done"]  = True
        _live["open_position"]["partial_dollar_pnl"] = partial_dollar_pnl
        _live["open_position"]["breakeven_locked"]   = True
        _live["open_position"]["dple_effective_sl"]  = -_spread_flr

        _live_log(
            f"✅ LIVE PARTIAL TP CONFIRMED: {sym} closed {half_sz} @ {real_exit:.5f} "
            f"({real_pnl_p*100:+.3f}%) partial=${partial_dollar_pnl:+.4f} | "
            f"remaining {remaining_sz} lots | stop -> breakeven ({_spread_flr*100:.3f}%)"
        )

        # Broker-side stop: tighten IG stop to match the internal breakeven level set above.
        # Protects the remaining 50% against gap-reversals regardless of whether MPD has armed.
        # Improvement check: only PUT if tighter than any stop MPD or entry has already set.
        _ptp_stop_level = (
            fill_px * (1.0 - _spread_flr) if dirn == 'long'
            else fill_px * (1.0 + _spread_flr)
        )
        _ptp_pip_l  = _live_pip_sizes.get(sym, _LIVE_FX_PIP)
        _ptp_spn_l  = half_sp * 2.0
        _ptp_cur_bk = _live['open_position'].get('defensive_stop_level') or \
                      _live['open_position'].get('broker_stop_level')
        _ptp_tighte = (
            _ptp_cur_bk is None or
            (dirn == 'long'  and _ptp_stop_level > _ptp_cur_bk) or
            (dirn == 'short' and _ptp_stop_level < _ptp_cur_bk)
        )
        if _ptp_tighte:
            _ptp_dist_l = abs(mid - _ptp_stop_level)
            _ptp_ig_min = (_live_min_stop_pts.get(sym, 4) + 1) * _ptp_pip_l
            _ptp_min_l  = max(_ptp_ig_min, (_ptp_spn_l / _ptp_pip_l + 1) * _ptp_pip_l)
            _ptp_synced = False
            if _ptp_dist_l >= _ptp_min_l:
                _ptp_put = _ig_live_put(
                    f"/positions/otc/{_live['open_position'].get('deal_id', '')}",
                    {"stopLevel": round(_ptp_stop_level, 5), "guaranteedStop": False},
                    version="2",
                )
                if _ptp_put:
                    _ptp_synced = True
                    _live['open_position']['defensive_stop_level'] = _ptp_stop_level
                    _live['open_position']['defensive_stop_active'] = True
            _live_log(
                f"🔒 [PARTIAL-TP SYNC] {sym}: broker stop -> breakeven "
                f"stopLevel={_ptp_stop_level:.5f} | broker_sync={_ptp_synced}"
                + (" (too close — polling-only)" if not _ptp_synced else "")
            )
        _live_save_state()
    else:
        _live_log(f"partial_tp: confirm failed for {sym} — falling back to full close")
        _live_close_position("take_profit", signals)


# ── Exit checks (Part 5 — mirrors _sim_check_exit) ───────────────────────────

def _live_check_exit(signals: dict, regime: str) -> None:
    """Exit checks for live position, mirroring _sim_check_exit logic.
    Reads sim's calibrated vol_history, win_moves, loss_moves via _sim_get_tp /
    _sim_get_dynamic_stop — this is intentional: sim calibration informs live.
    """
    pos = _live.get("open_position")
    if not pos:
        return
    sym       = pos["instrument"]
    hold_sec  = time.time() - pos.get("entry_time", time.time())
    dirn      = pos["direction"]

    # ── Proactive LS broker-stop detection ───────────────────────────────────
    # TRADE stream fires FULLY_CLOSED on broker stop/TP — detect it here so
    # state is reconciled in the same cycle instead of waiting for the next
    # REST-based detection pass. _ls_deal_closed() is a lock-protected set
    # lookup (no I/O) so this adds negligible overhead every cycle.
    _ls_chk_id = pos.get("deal_id", "")
    if _ls_chk_id and _ls_deal_closed(_ls_chk_id):
        _live_log(
            f"[LS] {sym}: TRADE stream confirmed FULLY_CLOSED deal={_ls_chk_id} "
            f"— reconciling state (broker stop/TP detected proactively)"
        )
        _live_reconcile_positions()
        _live_save_state()
        return

    # Max hold — always fires regardless of price availability
    if hold_sec >= _SIM_MAX_HOLD_SECS:
        _live_log(f"⏰ {sym}: max hold {hold_sec/3600:.1f}h — exiting")
        _live_close_position("max_hold", signals)
        return

    if sym not in signals:
        return

    sig      = signals[sym]
    mid      = sig.get("price", 0.0)
    if mid <= 0:
        return

    fill_px  = pos.get("fill_price", mid)
    # Use spread-adjusted exit price: bid for LONG exits, ask for SHORT exits.
    # spread_pct is IG-sourced for FX/commodities; Finnhub-synthetic for equity CFDs.
    # This matches IG actual fill direction, reducing stop-loss overshoot on FX/commodities.
    _sp_pct  = sig.get("spread_pct", 0.0)
    _half_sp = mid * _sp_pct / 200.0   # spread_pct is %; /100 for fraction, /2 for half-spread
    _exit_px = (mid - _half_sp) if dirn == "long" else (mid + _half_sp)
    pnl_pct  = (_exit_px - fill_px) / fill_px if dirn == "long" else (fill_px - _exit_px) / fill_px
    sig_dir  = sig.get("direction", "neutral")

    # [BLOCK OPEN] observational audit log — no behavior change.
    # Fires once per open position the first time a perf-block is active for this
    # instrument, logging block type and P&L state for the positions-gap evidence trail.
    if _live_perf_blocked(sym) and not pos.get("block_open_logged"):
        try:
            _r = _redis()
            _blk_wr  = bool(_r.get(f"june_perf_block_wr:{sym}"))
            _blk_sar = bool(_r.get(f"june_perf_block_sar:{sym}"))
            _blk_obs = _live_get_observer(sym)
            _blk_parts = []
            if _blk_wr:  _blk_parts.append("WR-hard")
            if _blk_sar: _blk_parts.append("SAR")
            if _blk_obs: _blk_parts.append(f"observer-{_blk_obs}")
            _blk_type = "/".join(_blk_parts) if _blk_parts else "unknown"
        except Exception:
            _blk_type = "unknown"
        _live_log(
            f"[BLOCK OPEN] {sym}: instrument perf-blocked ({_blk_type}) "
            f"while position is open | pnl {pnl_pct*100:+.3f}% | {dirn} "
            f"| hold {hold_sec/60:.0f}min — observational only, no action taken"
        )
        _live["open_position"]["block_open_logged"] = True

    # ── Time-Decay Exit Compression (live mirror) ─────────────────────────────
    age_min_l = hold_sec / 60.0
    _tdec_windows = 0
    if age_min_l >= 30.0:
        _init_sl_l  = pos.get('initial_sl_pct', pos.get('stop_pct', _sim_get_dynamic_stop(sym)))
        _tp_val_l   = pos.get('tp_pct', _sim_get_tp(sym, dirn, pos.get('conviction', 5)))
        _tp_prog_l  = pnl_pct / _tp_val_l if _tp_val_l > 0 else 0.0
        if _tp_prog_l < 0.50:
            _tdec_windows  = int((age_min_l - 30.0) / 15.0) + 1
            _compression_l = max(0.50, 1.0 - 0.15 * _tdec_windows)
            _eff_sl_l      = _init_sl_l * _compression_l
            if _eff_sl_l < pos.get('stop_pct', 0.0):
                _live['open_position']['stop_pct'] = _eff_sl_l
                pos = _live['open_position']
            _brb_td_l   = _barbie_overrides.get(sym, {}).get('reversal_confirm_secs')
            if _brb_td_l is not None:
                _cl_td_l    = max(_BARBIE_OVERRIDE_MIN_SECS, min(_BARBIE_OVERRIDE_MAX_SECS, int(_brb_td_l)))
                _base_pat_l = max(1, round(_cl_td_l / POLL_ACTIVE))
            else:
                _base_pat_l = _SIM_REV_PATIENCE_WIN if pnl_pct > 0 else _SIM_REV_PATIENCE_LOSS
            _rev_secs_l = max(1, _base_pat_l - _tdec_windows) * POLL_ACTIVE
            if _tdec_windows > pos.get('tdec_windows_logged', -1):
                _live['open_position']['tdec_windows_logged'] = _tdec_windows
                pos = _live['open_position']
                _live_log(
                    f'⏳ [TIME DECAY] {sym}: active for {int(age_min_l)}m '
                    f'without 50% TP progress. '
                    f'Compressed SL to {round(_eff_sl_l * 100, 3)}% and '
                    f'reversal exit wait to {_rev_secs_l}s.'
                )
    # ── Dynamic Profit Lock-in Engine (DPLE) -- live mirror ────────────────────
    _dple_tp_l = pos.get('tp_pct', _sim_get_tp(sym, dirn, pos.get('conviction', 5)))
    if _dple_tp_l > 0:
        if pnl_pct > 0:
            _peak_l = max(pos.get('peak_pnl_pct', 0.0), pnl_pct)
            if _peak_l > pos.get('peak_pnl_pct', 0.0):
                _live['open_position']['peak_pnl_pct'] = _peak_l
                pos = _live['open_position']
        _peak_l    = pos.get('peak_pnl_pct', 0.0)
        _dple_sl_l = pos.get('dple_effective_sl', None)
        if pnl_pct >= 0.75 * _dple_tp_l:
            _trail_sl_l = 0.5 * _peak_l
            if _dple_sl_l is None or _trail_sl_l > _dple_sl_l:
                _live['open_position']['dple_effective_sl'] = _trail_sl_l
                _live['open_position']['breakeven_locked']  = True
                pos = _live['open_position']
                _dple_sl_l = _trail_sl_l
                _live_log(
                    f'💰 [PROFIT TRAIL] {sym}: Locked in 50% of peak profit '
                    f'at {_trail_sl_l*100:.3f}%% floor'
                )
                # Sync improved DPLE M2 trail to IG broker stop (same PUT pattern as MPD).
                # Only fires when new level is tighter than the current broker-side stop;
                # if MPD's fixed level is still tighter, leave it alone.
                _dple_pip_l  = _live_pip_sizes.get(sym, _LIVE_FX_PIP)
                _dple_spn_l  = _half_sp * 2.0
                _dple_sl_abs = (
                    fill_px * (1.0 + _trail_sl_l) if dirn == 'long'
                    else fill_px * (1.0 - _trail_sl_l)
                )
                _dple_cur_bk = pos.get('defensive_stop_level') or pos.get('broker_stop_level')
                _dple_tighte = (
                    _dple_cur_bk is None or
                    (dirn == 'long'  and _dple_sl_abs > _dple_cur_bk) or
                    (dirn == 'short' and _dple_sl_abs < _dple_cur_bk)
                )
                if _dple_tighte:
                    _dple_dist_l = abs(mid - _dple_sl_abs)
                    _dple_ig_min = (_live_min_stop_pts.get(sym, 4) + 1) * _dple_pip_l
                    _dple_min_l  = max(_dple_ig_min, (_dple_spn_l / _dple_pip_l + 1) * _dple_pip_l)
                    _dple_synced = False
                    if _dple_dist_l >= _dple_min_l:
                        _dple_put = _ig_live_put(
                            f"/positions/otc/{pos.get('deal_id', '')}",
                            {"stopLevel": round(_dple_sl_abs, 5), "guaranteedStop": False},
                            version="2",
                        )
                        if _dple_put:
                            _dple_synced = True
                            _live['open_position']['defensive_stop_level'] = _dple_sl_abs
                            _live['open_position']['defensive_stop_active'] = True
                            pos = _live['open_position']
                    _live_log(
                        f"📈 [DPLE SYNC] {sym}: trail floor {_trail_sl_l*100:.3f}% "
                        f"-> stopLevel={_dple_sl_abs:.5f} | broker_sync={_dple_synced}"
                        + (" (too close — polling-only)" if not _dple_synced else "")
                    )
        elif pnl_pct >= 0.5 * _dple_tp_l and not pos.get('breakeven_locked'):
            _spread_buf_l = _sim_get_spread_floor(sym)
            _be_sl_l = -_spread_buf_l
            if _dple_sl_l is None or _be_sl_l > _dple_sl_l:
                _live['open_position']['dple_effective_sl'] = _be_sl_l
                _live['open_position']['breakeven_locked']  = True
                pos = _live['open_position']
                _dple_sl_l = _be_sl_l
            _live_log(f'🛡️ [BREAKEVEN LOCK] {sym}: 50% TP distance reached. Stop moved to entry.')
        if _dple_sl_l is not None and pnl_pct < _dple_sl_l:
            _live_close_position('dple_trail', signals); return

    # ── Micro-Profit Defense Engine (MPD) — live ─────────────────────────────────
    # Arms once spread-adjusted exit price clears total transaction friction above entry.
    # Attempts broker-side stop via REST PUT; falls back to software mirror (ratchet: one-way).
    if fill_px > 0:
        _mpd_pip_l  = _live_pip_sizes.get(sym, _LIVE_FX_PIP)
        _mpd_spn_l  = _half_sp * 2.0                          # spread in native price units
        _mpd_slp_l  = _MPD_SLIPPAGE_PIPS * _mpd_pip_l         # slippage buffer (native)
        _mpd_mgp_l  = _MPD_MIN_PROFIT_PIPS * _mpd_pip_l       # min guaranteed profit (native)
        _mpd_fric_l = _mpd_spn_l + _mpd_slp_l + _mpd_mgp_l   # total friction (native)
        _mpd_act    = (
            (dirn == "long"  and _exit_px - fill_px >= _mpd_fric_l) or
            (dirn == "short" and fill_px - _exit_px >= _mpd_fric_l)
        )
        if _mpd_act:
            _p_stop_l = (
                fill_px + _mpd_spn_l + _mpd_mgp_l if dirn == "long"
                else fill_px - _mpd_spn_l - _mpd_mgp_l
            )
            _cur_dsl_l = pos.get("defensive_stop_level", None)
            _improve_l = (
                _cur_dsl_l is None or
                (dirn == "long"  and _p_stop_l > _cur_dsl_l) or
                (dirn == "short" and _p_stop_l < _cur_dsl_l)
            )
            if _improve_l:
                _live["open_position"]["defensive_stop_active"] = True
                _live["open_position"]["defensive_stop_level"]  = _p_stop_l
                pos = _live["open_position"]
                # Try broker-side stop update; fall back to software mirror if too close
                _mpd_dist_l = abs(mid - _p_stop_l)
                _mpd_ig_min = (_live_min_stop_pts.get(sym, 4) + 1) * _mpd_pip_l
                _mpd_min_l  = max(_mpd_ig_min, (_mpd_spn_l / _mpd_pip_l + 1) * _mpd_pip_l)
                _mpd_synced = False
                if _mpd_dist_l >= _mpd_min_l:
                    _put_r = _ig_live_put(
                        f"/positions/otc/{pos.get('deal_id', '')}",
                        {"stopLevel": round(_p_stop_l, 5), "guaranteedStop": False},
                        version="2",
                    )
                    if _put_r:
                        _mpd_synced = True
                        _live["open_position"]["defensive_soft_sl"] = None
                    else:
                        _live["open_position"]["defensive_soft_sl"] = _p_stop_l
                else:
                    _live["open_position"]["defensive_soft_sl"] = _p_stop_l
                pos = _live["open_position"]
                _live_log(
                    f"\U0001f6e1\ufe0f [PROFIT DEFENSE] {sym}: Micro-profit lock at "
                    f"{_p_stop_l:.5f} | friction {_mpd_fric_l:.5f} | "
                    f"broker_sync={_mpd_synced}"
                )
                _live_save_state()
        if pos.get("defensive_stop_active"):
            _dsl_l = pos.get("defensive_soft_sl") or pos.get("defensive_stop_level", 0.0)
            if _dsl_l:
                if dirn == "long"  and _exit_px < _dsl_l:
                    _live_close_position("mpd_floor", signals)
                    return
                if dirn == "short" and _exit_px > _dsl_l:
                    _live_close_position("mpd_floor", signals)
                    return

    stop_pct = pos.get("stop_pct", _sim_get_dynamic_stop(sym))
    tp_pct   = pos.get("tp_pct",   _sim_get_tp(sym, dirn, pos.get("conviction", 5)))

    if pnl_pct <= -stop_pct:
        _live_close_position("stop_loss", signals)
        return
    if pnl_pct >= tp_pct:
        if not pos.get("partial_exit_done"):
            _live_partial_tp_exit(signals)
        else:
            _live_close_position("take_profit", signals)
        return

    # Asymmetric reversal — mirrors sim logic including Barbie reversal_confirm_secs override
    opposing = (dirn == "long"  and (regime == "bear" or sig_dir == "bear")) or \
               (dirn == "short" and (regime == "bull"  or sig_dir == "bull"))
    if opposing:
        _brb = _barbie_overrides.get(sym, {}).get("reversal_confirm_secs")
        if _brb is not None:
            _clamped = max(_BARBIE_OVERRIDE_MIN_SECS, min(_BARBIE_OVERRIDE_MAX_SECS, int(_brb)))
            patience = max(1, round(_clamped / POLL_ACTIVE))
        else:
            patience = _SIM_REV_PATIENCE_WIN if pnl_pct > 0 else _SIM_REV_PATIENCE_LOSS
        patience = max(1, patience - _tdec_windows)  # time-decay window decay
        rev_count = pos.get("reversal_count", 0) + 1
        _live["open_position"]["reversal_count"] = rev_count
        if rev_count >= patience:
            _live_close_position("reversal", signals)
            return
    elif pos.get("reversal_count", 0):
        _live["open_position"]["reversal_count"] = 0


# ── Instrument selection (Part 2 — mirrors _sim_select_instrument) ────────────

def _live_select_instrument(signals: dict, regime: str) -> Optional[str]:
    """Candidate selection using live balance and real IG margin rates.

    Uses _live_is_eligible which caps effective leverage at 1/margin_rate for
    instruments with margin_rate <= 1.0 — prevents selecting instruments
    that IG would reject with INSUFFICIENT_FUNDS.
    """
    bal        = _live.get("balance", 0.0)
    if bal <= 0:
        return None
    best_sym, best_vol = None, 0.0

    for sym in _sim_eligible:
        if not _live_is_eligible(sym):
            continue
        if sym not in signals:
            continue
        sig  = signals[sym]
        vol  = abs(sig.get("change_5m", 0.0))
        dirn = sig.get("direction", "neutral")
        direction_str = "long" if dirn == "bull" else ("short" if dirn == "bear" else "")

        thresh = _sim_get_threshold(sym, direction_str, low_tier=(_sim_vol_bucket(vol) == "low"))
        if vol < thresh:
            continue
        if sig.get("spread_alert"):
            continue
        if direction_str and _live_is_paused(_sim_combo_key(sym, direction_str)):
            continue
        if time.time() < (_live.get("instrument_cooldown") or {}).get(sym, 0.0):
            continue  # all-direction cooldown after stop-out
        if regime == "bull" and dirn != "bull": continue
        if regime == "bear" and dirn != "bear": continue
        if regime in ("volatile", "neutral") and dirn == "neutral": continue
        if direction_str:
            skip, reason = _sim_combo_wr_gate(sym, direction_str)
            if skip:
                _live_log(f"skip {sym}: {reason}")
                continue
        weight     = _sim_regime_weight(sym, direction_str)
        corr_adj   = _sim_corr_weight(sym, direction_str, signals)
        eff_vol    = vol * weight * corr_adj
        if _live_perf_blocked(sym):
            continue  # performance filter: below 30% WR or chronic wide Spread/ATR
        if sig.get("spread_atr_wide"):
            eff_vol *= 0.5   # rank penalty: spread > ATR threshold
        if eff_vol > best_vol:
            best_vol = eff_vol
            best_sym = sym

    return best_sym


# ── Entry logic (Part 5 — mirrors _sim_try_entry) ─────────────────────────────


# == Pyramid management (no-decay 2-leg cap) =================================

def _live_close_addon_leg(leg: dict, exit_reason: str, signals: dict) -> None:
    # Close one pyramid addon leg. Removes from pyramid_legs on confirmed close.
    sym       = leg.get("instrument", "")
    dirn      = leg.get("direction", "long")
    ig_size   = leg.get("ig_size", 0.0)
    deal_id   = leg.get("deal_id", "")
    fill_px   = leg.get("fill_price", 0.0)
    sig       = signals.get(sym, {})
    mid       = sig.get("price", fill_px)
    close_dir = "SELL" if dirn == "long" else "BUY"
    epic      = INSTRUMENTS.get(sym, "")
    pnl_pct   = (mid - fill_px) / fill_px if dirn == "long" else (fill_px - mid) / fill_px
    _live_log(
        f"[PYRAMID CLOSE] leg {leg.get('leg_index', 2)} {sym} {close_dir} | "
        f"P&L ~{pnl_pct*100:+.2f}% | reason={exit_reason}"
    )
    if not _live_trade_guard():
        return
    if deal_id:
        if _live.get("manual_review_required"):
            _live_log(f"[PYRAMID] {sym}: manual_review_required -- addon close blocked")
            return
        _g_open, _g_src = _ls_position_guard_check(sym, deal_id)
        if _g_src == "DISAGREEMENT":
            return
        if _g_open is None:
            _live_log(f"[PYRAMID] {sym}: both LS and /accounts unavailable -- preserving addon")
            return
        if not _g_open:
            _live_log(f"[PYRAMID] {sym}: addon already gone [{_g_src}] -- clearing tracking")
            _live["pyramid_legs"] = [l for l in _live.get("pyramid_legs", [])
                                     if l.get("deal_id") != deal_id]
            if not _live.get("pyramid_legs"):
                _live["pyramid_agg_stop_level"] = None
            _live_save_state()
            return
    close_body = {
        "epic":           epic,
        "expiry":         "-",
        "direction":      close_dir,
        "size":           ig_size,
        "orderType":      "MARKET",
        "timeInForce":    "FILL_OR_KILL",
        "forceOpen":      False,
        "guaranteedStop": False,
        "currencyCode":   "USD",
        "dealId":         deal_id,
    }
    resp = _ig_live_post("/positions/otc", close_body, version="1")
    if not resp:
        _live_log(f"[PYRAMID] {sym}: addon close POST failed -- state preserved")
        return
    deal_ref = resp.get("dealReference", "")
    confirm  = _live_confirm_deal(deal_ref) if deal_ref else None
    if confirm and confirm.get("dealStatus") == "ACCEPTED":
        real_exit  = float(confirm.get("level", mid))
        real_pnl_p = (real_exit - fill_px) / fill_px if dirn == "long" else (fill_px - real_exit) / fill_px
        _live_log(
            f"✅ PYRAMID LEG CLOSED: {sym} @ {real_exit:.5f} "
            f"({real_pnl_p*100:+.3f}%) | {exit_reason}"
        )
        _live["pyramid_legs"] = [l for l in _live.get("pyramid_legs", [])
                                  if l.get("deal_id") != deal_id]
        if not _live.get("pyramid_legs"):
            # Last addon leg closed -- one full multi-leg cycle complete; increment unlock counter
            try:
                _redis().incr(_PYRAMID_UNLOCK_KEY)
            except Exception:
                pass
            _live["pyramid_agg_stop_level"] = None
        _live_save_state()
    else:
        status = confirm.get("dealStatus", "?") if confirm else "no-confirm"
        _live_log(f"[PYRAMID] {sym}: addon close {status} -- state preserved for retry")


def _live_close_all_addon_legs(exit_reason: str, signals: dict) -> None:
    # Close every pyramid addon leg. Called on orphan protection or aggregate stop.
    for leg in list(_live.get("pyramid_legs", [])):
        _live_close_addon_leg(leg, exit_reason, signals)


def _live_check_pyramid_exits(signals: dict) -> None:
    # Aggregate stop check + per-addon-leg exit management.
    # Aggregate stop: closes ALL legs when adverse move exceeds _PYRAMID_AGG_STOP_PCT
    # from blended entry. Fires before individual stops -> prevents orphan scenario.
    # Per-addon: Addon SL -> close addon + primary. Addon TP -> close addon only.
    primary    = _live.get("open_position")
    addon_legs = _live.get("pyramid_legs", [])
    if not primary or not addon_legs:
        return
    sym  = primary["instrument"]
    dirn = primary["direction"]
    sig  = signals.get(sym, {})
    mid  = sig.get("price", 0.0)
    if mid <= 0:
        return
    _sp_pct  = sig.get("spread_pct", 0.0)
    _half_sp = mid * _sp_pct / 200.0
    _exit_px = (mid - _half_sp) if dirn == "long" else (mid + _half_sp)

    # Aggregate stop check
    agg_stop = _live.get("pyramid_agg_stop_level")
    if agg_stop is not None:
        breached = (
            (dirn == "long"  and _exit_px <= agg_stop) or
            (dirn == "short" and _exit_px >= agg_stop)
        )
        if breached:
            p_fill = primary.get("fill_price", mid)
            p_pnl  = (_exit_px - p_fill) / p_fill if dirn == "long" else (p_fill - _exit_px) / p_fill
            _live_log(
                f"🔴 PYRAMID AGGREGATE STOP: {sym} exit_px={_exit_px:.5f} "
                f"breached agg_stop={agg_stop:.5f} | primary P&L {p_pnl*100:+.3f}% "
                f"-- closing ALL legs"
            )
            _live_close_all_addon_legs("pyramid_agg_stop", signals)
            _live_close_position("pyramid_agg_stop", signals)
            return

    # Per-addon exits
    for leg in list(addon_legs):
        leg_fill = leg.get("fill_price", 0.0)
        stop_pct = leg.get("stop_pct", 0.0)
        tp_pct   = leg.get("tp_pct", 0.0)
        leg_idx  = leg.get("leg_index", 2)
        deal_id  = leg.get("deal_id", "")

        # LS broker-stop detection (proactive, same pattern as _live_check_exit)
        if deal_id and _ls_deal_closed(deal_id):
            _live_log(f"[LS] PYRAMID leg {leg_idx}: FULLY_CLOSED by broker -- clearing")
            _live["pyramid_legs"] = [l for l in _live["pyramid_legs"]
                                     if l.get("deal_id") != deal_id]
            if not _live["pyramid_legs"]:
                _live["pyramid_agg_stop_level"] = None
            _live_save_state()
            continue

        leg_pnl_pct = (
            (_exit_px - leg_fill) / leg_fill if dirn == "long"
            else (leg_fill - _exit_px) / leg_fill
        ) if leg_fill > 0 else 0.0

        if stop_pct > 0 and leg_pnl_pct <= -stop_pct:
            _live_log(
                f"🛑 PYRAMID LEG SL: leg {leg_idx} | pnl {leg_pnl_pct*100:.3f}% "
                f"<= -{stop_pct*100:.3f}% -- closing addon + primary (direction failed)"
            )
            _live_close_addon_leg(leg, "pyramid_leg_sl", signals)
            _live_close_position("pyramid_leg_sl_close_primary", signals)
            return

        if tp_pct > 0 and leg_pnl_pct >= tp_pct:
            _live_log(
                f"✅ PYRAMID LEG TP: leg {leg_idx} | pnl {leg_pnl_pct*100:.3f}% "
                f">= {tp_pct*100:.3f}% -- closing addon only, primary continues"
            )
            _live_close_addon_leg(leg, "pyramid_leg_tp", signals)
            return


def _pyramid_active_max_legs() -> int:
    """Return the live pyramid leg cap based on the Redis evidence counter.
    Fails safe to _PYRAMID_MAX_LEGS (2) on any Redis error."""
    try:
        n = int(_redis().get(_PYRAMID_UNLOCK_KEY) or 0)
        if n >= _PYRAMID_4LEG_THRESHOLD:
            return 4
        if n >= _PYRAMID_3LEG_THRESHOLD:
            return 3
        return _PYRAMID_MAX_LEGS
    except Exception:
        return _PYRAMID_MAX_LEGS


def _live_check_pyramid_entry(signals: dict, regime: str) -> None:
    # Evaluate whether to add a pyramid leg to the existing primary position.
    # All defensive gates explicitly checked -- mirrors _live_try_entry exactly.
    # Profit gate: primary must show >= _PYRAMID_PROFIT_GATE_PCT spread-adjusted P&L.
    # LS confirmation: primary deal must NOT be in _ls_confirms_closed.
    primary = _live.get("open_position")
    if not primary:
        return
    active_max = _pyramid_active_max_legs()
    if len(_live.get("pyramid_legs", [])) >= active_max - 1:
        return  # already at cap
    sym     = primary["instrument"]
    dirn    = primary["direction"]
    fill    = primary.get("fill_price", 0.0)
    deal_id = primary.get("deal_id", "")

    if deal_id and _ls_deal_closed(deal_id):
        return  # primary FULLY_CLOSED per LS -- no add-on

    sig = signals.get(sym, {})
    mid = sig.get("price", 0.0)
    if mid <= 0 or fill <= 0:
        return

    _sp_pct  = sig.get("spread_pct", 0.0)
    _half_sp = mid * _sp_pct / 200.0
    _exit_px = (mid - _half_sp) if dirn == "long" else (mid + _half_sp)
    pnl_pct  = (_exit_px - fill) / fill if dirn == "long" else (fill - _exit_px) / fill

    if pnl_pct < _PYRAMID_PROFIT_GATE_PCT:
        return  # not yet at profit gate

    # Defensive gates -- explicit, same as _live_try_entry
    if not _june_live_trading_enabled:
        return
    _gmode = _live.get("global_mode", "normal")
    if _gmode == "defensive" and regime == "neutral":
        return
    _imode = (_live.get("instrument_mode") or {}).get(sym, "normal")
    if _imode == "defensive" and regime == "neutral":
        return
    if _live_perf_blocked(sym):
        return
    if sym in _METALS_INSTRUMENTS and _is_metals_weekend_closure():
        return
    pause_until = _live.get("pause_expiry", {}).get(sym, 0)
    if time.time() < pause_until:
        return
    # SAR gate
    _atr5, _atr5_fb = _compute_atr_5m(sym)
    if _atr5 is not None and _atr5 > 0:
        _sp_raw = (sig.get("spread_pct", 0.0) or 0.0) * (sig.get("price", 0.0) or 0.0) / 100.0
        _sar5   = _sp_raw / _atr5
        _thr5   = _spread_atr_threshold(sym, _atr5_fb)
        if _sar5 > _thr5:
            return

    next_leg_idx = len(_live.get("pyramid_legs", [])) + 2
    _live_log(
        f"[PYRAMID] {sym}: gate passed -- primary at +{pnl_pct*100:.3f}% "
        f"(gate={_PYRAMID_PROFIT_GATE_PCT*100:.2f}%) | adding leg {next_leg_idx}/{active_max}"
    )
    _live_add_pyramid_leg(signals)


def _live_add_pyramid_leg(signals: dict) -> None:
    # Open the pyramid addon leg for the existing primary position.
    # Uses same ig_size formula as primary (minDeal-clamped, reuses pos_size/leverage).
    # forceOpen=True REQUIRED: opens separate deal in same instrument.
    # Sets aggregate stop on both legs after fill confirms.
    if not _live_trade_guard():
        return
    primary = _live.get("open_position")
    if not primary:
        return

    sym   = primary["instrument"]
    dirn  = primary["direction"]
    fill1 = primary.get("fill_price", 0.0)
    size1 = primary.get("ig_size", 0.0)

    sig = signals.get(sym, {})
    mid = sig.get("price", 0.0)
    if mid <= 0:
        _live_log(f"[PYRAMID] {sym}: no price -- addon aborted")
        return

    epic   = INSTRUMENTS.get(sym, "")
    ig_dir = "BUY" if dirn == "long" else "SELL"

    # Sizing: reuse primary pos_size and leverage for exact size match
    pos_sz   = primary.get("pos_size", 0.0)
    lev      = primary.get("leverage", 1)
    total    = _live.get("balance_total", 0.0)
    skimmed  = _live.get("skimmed_total", 0.0)
    bal      = max(0.0, total - skimmed)
    _pyr_base = min(10.0, bal) if bal < 100.0 else round(bal * 0.10, 2)
    notional = pos_sz * lev if pos_sz > 0 else max(2.0, _pyr_base) * lev

    # Dynamic leg index: 2 = first addon, 3 = second addon, 4 = third addon
    leg_index = len(_live.get("pyramid_legs", [])) + 2
    # Size decay for legs 3+ to reduce per-leg risk as pyramid deepens
    if leg_index == 3:
        notional = round(notional * _PYRAMID_3LEG_SIZE_DECAY, 2)
    elif leg_index >= 4:
        notional = round(notional * _PYRAMID_4LEG_SIZE_DECAY, 2)

    ig_size  = _live_compute_ig_size(sym, notional, mid)
    if ig_size <= 0:
        _live_log(f"[PYRAMID] {sym}: ig_size=0 -- addon aborted")
        return

    stop_pct = max(_sim_get_dynamic_stop(sym), _sim_get_spread_floor(sym))
    tp_pct   = _sim_get_tp(sym, dirn, primary.get("conviction", 5))

    # N-way approximate aggregate stop for entry-time stop distance (all legs including new)
    _existing_legs = _live.get("pyramid_legs", [])
    _approx_pts = [(fill1, size1)] + [(l.get("fill_price", 0.0), l.get("ig_size", 0.0))
                                       for l in _existing_legs] + [(mid, ig_size)]
    _approx_w   = sum(sz for _, sz in _approx_pts)
    approx_blended = (
        sum(fp * sz for fp, sz in _approx_pts) / _approx_w if _approx_w > 0 else mid
    )
    approx_agg_stop = (
        approx_blended * (1.0 - _PYRAMID_AGG_STOP_PCT) if dirn == "long"
        else approx_blended * (1.0 + _PYRAMID_AGG_STOP_PCT)
    )
    approx_stop_dist = _live_compute_stop_pts(sym, _PYRAMID_AGG_STOP_PCT, mid)

    _live_log(
        f"[PYRAMID] {'WOULD-BUY' if not _june_live_trading_enabled else 'BUY'} leg {leg_index}: "
        f"{sym} {ig_dir} size={ig_size} notional~${notional:.2f} | "
        f"stop {stop_pct*100:.2f}% TP {tp_pct*100:.2f}% | "
        f"approx_agg_stop={approx_agg_stop:.5f} ({approx_stop_dist}pts)"
    )

    body = {
        "epic":           epic,
        "expiry":         "-",
        "direction":      ig_dir,
        "size":           ig_size,
        "orderType":      "MARKET",
        "timeInForce":    "FILL_OR_KILL",
        "forceOpen":      True,         # REQUIRED: opens separate deal in same instrument
        "guaranteedStop": False,
        "currencyCode":   "USD",
        "stopDistance":   approx_stop_dist,
    }
    resp = _ig_live_post("/positions/otc", body, version="1")
    if not resp:
        _live_log(f"[PYRAMID] {sym}: POST failed -- addon aborted")
        return

    deal_ref = resp.get("dealReference", "")
    confirm  = _live_confirm_deal(deal_ref) if deal_ref else None
    if not confirm or confirm.get("dealStatus") != "ACCEPTED":
        status = confirm.get("dealStatus", "?") if confirm else "no-confirm"
        reason = confirm.get("reason", "?") if confirm else "?"
        _live_log(f"[PYRAMID] {sym}: deal {status}: {reason} -- addon aborted")
        return

    deal_id    = confirm.get("dealId", "")
    fill_price = float(confirm.get("level", mid))

    # N-way precise aggregate stop using actual fill (primary + all existing addons + new leg)
    _all_fill_pts = [(fill1, size1)] + [
        (l.get("fill_price", 0.0), l.get("ig_size", 0.0))
        for l in _live.get("pyramid_legs", [])
    ] + [(fill_price, ig_size)]
    _total_w = sum(sz for _, sz in _all_fill_pts)
    blended = (
        sum(fp * sz for fp, sz in _all_fill_pts) / _total_w if _total_w > 0 else fill_price
    )
    agg_stop_level = (
        blended * (1.0 - _PYRAMID_AGG_STOP_PCT) if dirn == "long"
        else blended * (1.0 + _PYRAMID_AGG_STOP_PCT)
    )

    leg = {
        "instrument": sym,
        "direction":  dirn,
        "deal_id":    deal_id,
        "deal_ref":   deal_ref,
        "fill_price": fill_price,
        "ig_size":    ig_size,
        "notional":   notional,
        "stop_pct":   stop_pct,
        "tp_pct":     tp_pct,
        "entry_time": time.time(),
        "leg_index":  leg_index,
    }
    _live.setdefault("pyramid_legs", []).append(leg)
    _live["pyramid_agg_stop_level"] = agg_stop_level

    # PUT aggregate stop to ALL open deals (primary + all addon legs including the new one)
    _pip_sz       = _live_pip_sizes.get(sym, _LIVE_FX_PIP)
    dist_to_agg   = abs(mid - agg_stop_level)
    min_stop_dist = (_live_min_stop_pts.get(sym, 4) + 1) * _pip_sz
    primary_deal  = primary.get("deal_id", "")
    _all_deal_ids = [d for d in
                     [primary_deal] +
                     [l.get("deal_id", "") for l in _live.get("pyramid_legs", [])]
                     if d]
    if dist_to_agg >= min_stop_dist:
        for _d in _all_deal_ids:
            _put_r = _ig_live_put(
                f"/positions/otc/{_d}",
                {"stopLevel": round(agg_stop_level, 5), "guaranteedStop": False},
                version="2",
            )
            if _put_r:
                _live_log(f"[PYRAMID] Agg stop PUT deal {_d}: stopLevel={agg_stop_level:.5f}")
            else:
                _live_log(f"[PYRAMID] Agg stop PUT failed deal {_d} -- software check only")
        if primary_deal:
            _live["open_position"]["defensive_stop_level"] = agg_stop_level
            _live["open_position"]["defensive_stop_active"] = True
    else:
        _live_log(
            f"[PYRAMID] Agg stop {agg_stop_level:.5f} too close to mid {mid:.5f} "
            f"({dist_to_agg:.5f} < {min_stop_dist:.5f}) -- software check only"
        )

    _live_log(
        f"✅ PYRAMID LEG {leg_index} OPENED: {sym} {ig_dir} @ {fill_price:.5f} "
        f"| size {ig_size} | deal {deal_id} "
        f"| blended {blended:.5f} | agg_stop {agg_stop_level:.5f} "
        f"({_PYRAMID_AGG_STOP_PCT*100:.2f}% from blended)"
    )
    _live_save_state()


# == End pyramid management ===================================================


def _live_phase_leverage(phase: int) -> int:
    """Leverage ceiling for the given live phase."""
    if phase == 1: return _LIVE_PHASE_CONSERVATIVE_LEV
    if phase == 2: return _LIVE_PHASE2_LEV
    return _LIVE_PHASE3_LEV


def _live_check_phase() -> None:
    """Performance-gated phase advancement and drop-back for live trading.
    Only activates above _LIVE_PHASE_GATE_BAL — below that, minDeal floor
    dominates and leverage differentiation has no practical effect.
    Mirror of _sim_check_phase() reading _live[] not _sim[]."""
    bal     = _live.get("balance", 0.0)
    if bal < _LIVE_PHASE_GATE_BAL:
        return
    phase   = _live.get("live_phase", 1)
    n       = _live.get("live_phase_trades", 0)
    wins    = _live.get("live_phase_wins", 0)
    wr      = wins / n if n else 0.0
    entry_b = _live.get("live_phase_entry_balance") or bal
    pnl_pct = (bal - entry_b) / entry_b if entry_b > 0 else 0.0
    consec  = _live.get("live_phase_consec_losses", 0)
    # Drop-back runs first — catches losing streaks before advancement check
    if phase > 1:
        if consec >= _LIVE_PHASE_DROP_LOSSES or pnl_pct <= _LIVE_PHASE_DROP_PNL:
            new_phase = phase - 1
            reason    = (f"{consec} consecutive losses"
                         if consec >= _LIVE_PHASE_DROP_LOSSES
                         else f"P&L {pnl_pct:+.1%} from phase entry")
            _live["live_phase"]               = new_phase
            _live["live_phase_entry_balance"] = bal
            _live["live_phase_consec_losses"] = 0
            _live["live_phase_trades"]        = 0
            _live["live_phase_wins"]          = 0
            _live["live_phase_losses"]        = 0
            _live_log(
                f"\U0001f4c9 LIVE: Phase {phase} \u2192 Phase {new_phase} "
                f"({_live_phase_leverage(new_phase)}:1) "
                f"\u2014 {reason} \u2014 leverage ceiling lowered"
            )
            _live_save_state()
            return
    # Advancement
    if phase == 1 and n >= _LIVE_P1_TO_P2_TRADES and wr >= _LIVE_P1_TO_P2_WR and pnl_pct > 0:
        _live["live_phase"]               = 2
        _live["live_phase_entry_balance"] = bal
        _live["live_phase_consec_losses"] = 0
        _live["live_phase_trades"]        = 0
        _live["live_phase_wins"]          = 0
        _live["live_phase_losses"]        = 0
        _live_log(
            f"\U0001f4c8 LIVE: Phase 1 \u2192 Phase 2 (5:1) "
            f"\u2014 {n} trades, {wr:.0%} WR, +${bal - entry_b:.2f} P&L \u2014 criteria met"
        )
        _live_save_state()
    elif phase == 2 and n >= _LIVE_P2_TO_P3_TRADES and wr >= _LIVE_P2_TO_P3_WR and pnl_pct >= _LIVE_P2_TO_P3_PNL:
        _live["live_phase"] = 3
        _live["live_phase_entry_balance"] = bal
        _live["live_phase_consec_losses"] = 0
        _live["live_phase_trades"]        = 0
        _live["live_phase_wins"]          = 0
        _live["live_phase_losses"]        = 0
        _live_log(
            f"\U0001f4c8 LIVE: Phase 2 \u2192 Phase 3 (10:1) "
            f"\u2014 {n} trades, {wr:.0%} WR, +${bal - entry_b:.2f} P&L \u2014 criteria met"
        )
        _live_save_state()


def _live_try_entry(signals: dict, regime: str) -> None:
    """Evaluate entry for live trading. Reuses all sim decision functions.
    Calls _live_open_position which is the only place orders are placed.
    """
    if _live.get("open_position"):
        return     # one position at a time

    total   = _live.get("balance_total", 0.0)
    skimmed = _live.get("skimmed_total", 0.0)
    bal     = max(0.0, total - skimmed)   # tradeable capital = total equity minus set-aside
    if bal <= 0:
        return

    # Extend signals with fresh equity CFD snapshots (same pattern as sim)
    _ext = dict(signals)
    _now_ext = time.time()
    for _eq_b, _eq_d in _direct_cfd_signals.items():
        if _eq_b in _ext:
            continue
        if _now_ext - _eq_d.get("ts", 0) > 20 * 60:
            continue
        _eq_mid = _eq_d.get("mid", 0.0)
        if _eq_mid <= 0.0:
            continue
        _eq_dir_raw = _eq_d.get("direction", "flat")
        _ext[_eq_b] = {
            "change_5m":    _eq_d["pct"],
            "direction":    "neutral" if _eq_dir_raw == "flat" else _eq_dir_raw,
            "spread_alert": False,
            "price":        _eq_mid,
            "spread_pct":   0.1,
            "change_15m":   None,
        }

    # Regime gate — applied only in DEFENSIVE mode.
    # In NORMAL mode, neutral-regime entries are permitted (CFD speed preserved).
    # In DEFENSIVE mode, require a directional regime before instrument selection.
    # _sim_regime_weight only maps FX instruments; SILVER/OIL always return 1.0
    # regardless of macro state, so gating on the string is the reliable path.
    _gmode = _live.get("global_mode", "normal")
    if _gmode == "defensive" and regime == "neutral":
        _live_log(f"No live candidate — global DEFENSIVE + regime=neutral")
        return
    sym = _live_select_instrument(_ext, regime)
    if not sym or sym not in _ext:
        _live_log(f"No live candidate — regime={regime} bal=${bal:.2f}")
        return

    # Per-instrument defensive mode regime check (applied after selection)
    _imode = (_live.get("instrument_mode") or {}).get(sym, "normal")
    if _imode == "defensive" and regime == "neutral":
        _live_log(f"skip {sym}: instrument DEFENSIVE + regime=neutral")
        return

    # Metals session gate: SILVER/OIL follow CME Sunday 18:00 ET reopen,
    # 2h after FX (21:00 UK). Silently skip — no cooldown, no log spam.
    if sym in _METALS_INSTRUMENTS and _is_metals_weekend_closure():
        _live_log(f"⏳ {sym}: CME metals not yet open (Sun 18:00 ET) — skipping")
        return

    # FX weekend gate: block non-continuous instruments Fri 22:00 UK → Sun 21:00 UK.
    # Continuous instruments (24/7 markets) bypass this gate entirely.
    if sym not in _CONTINUOUS_INSTRUMENTS and is_weekend_closure():
        return

    sig       = _ext[sym]
    chg       = sig.get("change_5m", 0.0)
    vol       = abs(chg)
    direction = "long" if chg > 0 else "short"
    combo     = _sim_combo_key(sym, direction)

    # [SIM→LIVE OBS]: read SIM per-combo WR for this candidate
    # OBSERVATION-ONLY: pure read of _sim dict, no write-back, zero gate or conviction effect.
    _slob_co = (_sim.get("combo_outcomes") or {}).get(combo, [])
    _slob_n  = len(_slob_co)
    if _slob_n >= 20:
        _slob_wr = sum(_slob_co) / _slob_n
        _live_log("[SIM→LIVE OBS] %s/%s: SIM WR=%.0f%% (n=%d)"
                  % (sym, direction, _slob_wr * 100, _slob_n))

    # Hybrid Spread/ATR gate — tiered threshold using 5-minute ATR baseline (fail-open)
    _atr5, _atr5_fb = _compute_atr_5m(sym)
    if _atr5 is not None and _atr5 > 0:
        _sp_raw = (sig.get("spread_pct", 0.0) or 0.0) * (sig.get("price", 0.0) or 0.0) / 100.0
        _sar5   = _sp_raw / _atr5
        _thr5   = _spread_atr_threshold(sym, _atr5_fb)
        if _sar5 > _thr5:
            _live_log(
                f"🚫 [HYBRID SPREAD GATE] {sym}: Spread/ATR(5m) ratio {_sar5:.2f} "
                f"> tier cap {_thr5:.2f} | Entry suppressed"
            )
            _now_sb = int(time.time())
            if _now_sb - _live_spread_block_cooldown.get(sym, 0) >= 300:
                _live_spread_block_cooldown[sym] = _now_sb
                _live_write_block_log(sym, direction, "spread_atr",
                                     {"sar5": round(_sar5, 3), "thr5": round(_thr5, 3),
                                      "atr5": round(_atr5, 4)})
            return

    # 1m anti-reversal gate (same as sim)
    price_1m = _price_n_minutes_ago(sym, 1)
    if price_1m and price_1m > 0:
        cur_px = sig.get("price", 0.0)
        rev_pct = abs(cur_px - price_1m) / price_1m * 100.0
        if direction == "long" and cur_px < price_1m and rev_pct >= _SIM_1M_MIN_REVERSAL:
            return
        if direction == "short" and cur_px > price_1m and rev_pct >= _SIM_1M_MIN_REVERSAL:
            return

    # 15m gate (same as sim, uses sim's reliability data — calibration shared)
    change_15m = sig.get("change_15m") or 0.0
    hist_sym   = _history.get(sym)
    has_15m    = hist_sym is not None and len(hist_sym) >= 15
    gate_mode, rel_score = _sim_15m_gate_mode(sym, direction)

    if has_15m and gate_mode != "relaxed":
        blocked = False
        if gate_mode == "strict":
            blocked = (direction == "long" and change_15m <= 0) or \
                      (direction == "short" and change_15m >= 0)
        else:
            blocked = (direction == "long" and change_15m <= -_SIM_15M_DEADZONE) or \
                      (direction == "short" and change_15m >= _SIM_15M_DEADZONE)
        if blocked:
            _live_log(f"skip {sym}: 15m gate ({gate_mode})")
            return

    # Conviction and leverage
    thresh  = _sim_get_threshold(sym, direction)
    weight  = _sim_regime_weight(sym, direction)
    conv    = _sim_conviction_gauge(sym, direction, vol, thresh, weight, combo,
                                    gate_mode, rel_score)
    # Conviction floor — applied only in DEFENSIVE mode (global or instrument-level).
    # In NORMAL mode, all conviction levels are permitted.
    _in_defensive = (_gmode == "defensive" or _imode == "defensive")
    _observer_key = _live_get_observer(sym)
    _observer_mult = {"light": 1.25, "moderate": 2.0}.get(_observer_key, 0.0)  # light=5.0 floor (allows 5/10, SILVER ceiling); moderate=8.0 (de-facto block for low-ceiling instruments)
    _obs_floor = (_LIVE_MIN_CONVICTION * _observer_mult) if _observer_mult > 0 else 0.0
    _def_floor = _LIVE_MIN_CONVICTION if _in_defensive else 0.0
    _eff_conv_floor = max(_obs_floor, _def_floor)
    # Dynamic exit: score clears raised observer floor AND last trade was a win
    if _observer_key and _obs_floor > 0 and conv >= _obs_floor:
        if _live_perf_last_won(sym):
            _live_clear_observer(sym)
            _observer_key = None
            _eff_conv_floor = _def_floor
            _live_log(f"🟢 [OBSERVER LIFTED] {sym}: score {conv}/10 cleared floor, last trade won")
    if _eff_conv_floor > 0 and conv < _eff_conv_floor:
        _floor_parts = []
        if _observer_key: _floor_parts.append(f"observer-{_observer_key}")
        if _in_defensive: _floor_parts.append(f"defensive[g={_gmode} i={_imode}]")
        _live_log(
            f"skip {sym}: conviction {conv}/10 below floor {_eff_conv_floor:.0f}/10 "
            f"[{'+'.join(_floor_parts)}]"
        )
        return
    # ── Trend-exhaustion gate ────────────────────────────────────────────────
    # Blocks entries where the directional move in the _history window has
    # already run > _EXHAUST_RATIO_BLOCK × ATR_5m. Prevents chasing aged moves
    # after the vol signal has decayed. Thresholds derived from OIL/SILVER data:
    # clean entries ≤2.5×, exhausted entries 4.5–8.1× (today's loss cluster).
    _ex_ratio = _exhaustion_ratio(sym, direction)
    # ── Exhaustion diagnostic (observational — zero gate impact) ────────────────────────
    # Log 10-tick sub-window ratio alongside 20-tick for calibration.
    # See session 2026-09-02 investigation: window vs hold-time analysis.
    # Same ATR_5m denominator as the gate; only net_move uses prices[-10:].
    _diag_hist = _history.get(sym)
    if _diag_hist and len(_diag_hist) >= 10:
        _diag_px       = [px for _, px in _diag_hist]
        _diag_atr, _   = _compute_atr_5m(sym)
        if _diag_atr:
            _net10_raw     = _diag_px[-1] - _diag_px[-10]
            _net10_signed  = _net10_raw if direction == "long" else -_net10_raw
            _ex10          = max(0.0, _net10_signed / _diag_atr)
            if _ex_ratio > 0.0 or _ex10 > 0.0:
                _live_log(
                    f"  📏 [EXHAUST DIAG] {sym}/{direction}: "
                    f"20t={_ex_ratio:.2f}× 10t={_ex10:.2f}× ATR={_diag_atr:.2f}"
                )
    # [EX_RATIO OBS] Continuous timeseries — OIL/SILVER/NATGAS, every cycle.
    # Purely observational: zero gate, conviction, or sizing effect.
    if sym in _HTF_INSTRUMENTS:
        _live_write_ex_ratio_obs(sym, direction, _ex_ratio,
                                 sig.get("spread_atr_ratio"), conv)
    if _ex_ratio >= _EXHAUST_RATIO_BLOCK:
        _live_log(
            f"skip {sym}: trend-exhausted {_ex_ratio:.1f}× ATR ≥ {_EXHAUST_RATIO_BLOCK}× "
            f"({direction}, net_move/ATR_5m over history window)"
        )
        _live_write_block_log(sym, direction, "exhaust_block",
                              {"ex_ratio": round(_ex_ratio, 3),
                               "threshold": _EXHAUST_RATIO_BLOCK, "conv": conv})
        return
    if _ex_ratio >= _EXHAUST_RATIO_REDUCE:
        conv = max(1, conv - 1)
        _live_log(
            f"  📉 trend-exhaustion {_ex_ratio:.1f}× ATR: conviction reduced 1pt → {conv}/10"
        )
        _live_write_block_log(sym, direction, "exhaust_reduce",
                              {"ex_ratio": round(_ex_ratio, 3),
                               "threshold": _EXHAUST_RATIO_REDUCE,
                               "conv_after_reduce": conv})

    _cv_lev = _sim_conviction_leverage("sprout", conv)
    # Phase system: cap conviction-based lev at current phase ceiling.
    # Dormant below _LIVE_PHASE_GATE_BAL — lev passes through unchanged.
    if bal >= _LIVE_PHASE_GATE_BAL:
        _phase_ceil = _live_phase_leverage(_live.get("live_phase", 1))
        lev = min(_cv_lev, _phase_ceil)
        _live_log(
            f"  [LIVE P{_live.get('live_phase', 1)}] conviction lev {_cv_lev}:1 "
            f"→ phase-capped {lev}:1 (bal ${bal:.2f} ≥ gate ${_LIVE_PHASE_GATE_BAL:.0f})"
        )
    else:
        lev = _cv_lev

    # Cap leverage at IG's real margin rate — prevents INSUFFICIENT_FUNDS rejection
    _mr = _live_margin.get(sym)
    if _mr and _mr > 0:
        lev = _ig_margin_to_max_lev(_mr, lev)

    # Sizing — flat $10 target below $100 balance, 10%-of-balance above.
    # Crossover is continuous: 10% of $100 = $10 exactly (no jump).
    # $2 floor and downstream minDeal clamp apply unchanged.
    pos_size = max(2.0, min(10.0, bal) if bal < 100.0 else round(bal * 0.10, 2))
    # Proportional size reduction when spread is wide relative to ATR
    _sar_live = sig.get("spread_atr_ratio")
    if sig.get("spread_atr_wide") and _sar_live:
        _scale = min(1.0, (SPREAD_ATR_THRESHOLD / _sar_live) ** 0.5)
        pos_size = max(2.0, round(pos_size * _scale, 2))
        _live_log(
            f"  📐 Spread/ATR={_sar_live:.1%}: position scaled ×{_scale:.2f} → ${pos_size:.2f}"
        )
    notional = pos_size * lev

    # Macro confluence — scale pos_size by Claudia directional alignment
    _macro_scale, _claudia_dir, _conf_note, _compress_sl = _live_macro_confluence(sym, direction)
    if _macro_scale < 1.0:
        pos_size = max(2.0, round(pos_size * _macro_scale, 2))
        notional = pos_size * lev
    _claudia_label = "Bullish" if _claudia_dir == 1 else ("Bearish" if _claudia_dir == -1 else "Neutral")
    _live_log(
        f"🤝 [CONFLUENCE] {sym}: Local={direction}, Macro={_claudia_label}"
        f" ({_conf_note}) | Sizing scaled to {_macro_scale}x"
    )

    # Observer Moderate: reduce position size 0.7x (light does not affect size)
    if _observer_key == "moderate":
        pos_size = max(2.0, round(pos_size * 0.7, 2))
        notional = pos_size * lev
        _live_log(f"  📉 Observer MODERATE: position scaled x0.70 -> ${pos_size:.2f}")


    # Check IG minimum feasibility
    if not _sim_check_min_feasible(sym, pos_size, lev):
        # Try with $10 fixed if pct_10 too small
        if not _sim_check_min_feasible(sym, 2.0, lev):
            _live_log(f"skip {sym}: notional ${notional:.2f} < IG min "
                      f"${_sim_min_notional.get(sym, 0):.2f}")
            return
        pos_size = 2.0
        notional = pos_size * lev

    _live_log(
        f"🎯 LIVE candidate: {sym} {direction.upper()} vol {vol:.3f}% "
        f"conv {conv}/10 lev {lev}:1 pos ${pos_size:.2f} notional ${notional:.2f}"
    )

    _htf_b, _htf_m, _htf_note = _compute_htf_alignment(sym, direction)
    _live_log("  📏 [HTF DIAG] %s/%s: %s" % (sym, direction, _htf_note))

    # DRY-RUN gate: when live is halted (CB fired or kill-switch off), all
    # observation logs above (ex_ratio obs, HTF diag, SIM->live WR, block log)
    # have already fired. Stop here — no order placement.
    # _live_trade_guard() inside _live_open_position() provides a second
    # structural backstop, but this gate makes the dry-run explicit in logs.
    if not _june_live_trading_enabled:
        _live_log(
            f"[DRY-RUN] would enter {sym}/{direction} conv={conv}/10 "
            f"lev={lev}:1 pos=${pos_size:.2f} — live halted, no order placed"
        )
        return
    _live_open_position(sym, direction, _ext, pos_size, lev, conv,
                       stop_mult=0.8 if _compress_sl else 1.0,
                       htf_bias=_htf_b)


# ── Top-level live step (called from poll_cycle) ──────────────────────────────

def run_live_step(signals: dict) -> None:
    """Called from poll_cycle() each cycle, after run_simulation_step().
    Exit/risk management runs unconditionally regardless of kill-switch state.
    june_live_enabled gates NEW positions only — never exit management.
    Sim state and sim logic are completely unaffected.
    """
    if not _live:
        return    # not started

    global _last_cycle_direction, _current_cycle_signals_snap
    _last_cycle_direction       = {s: g.get("direction") for s, g in _current_cycle_signals_snap.items()}
    _current_cycle_signals_snap = signals

    # Periodic balance and P&L refresh (every 5 minutes, not every cycle)
    _live_poll_balance()
    _live_poll_pnl()

    # Skim check after P&L update
    _live_check_skim()

    # Publish eligible instruments for Barbie supervision (rate-limited to 1h)
    _live_publish_eligible_instruments(signals)

    # Read macro regime once — shared by both exit and entry checks below
    regime = "neutral"
    try:
        raw = _redis().get("june_macro_regime")
        if raw:
            regime = json.loads(raw).get("regime", "neutral")
    except Exception:
        pass

    # Exit/risk management — runs BEFORE kill-switch check.
    # june_live_enabled controls new-trade authority only; any open position
    # must be managed unconditionally so live money is never left without
    # stop/TP coverage regardless of why the kill switch was disabled.
    if _live.get("open_position"):
        # ── Overnight DFB financing proximity warning (observational only) ─────
        # IG cutover: 22:00 UK local (BST = 21:00 UTC, GMT = 22:00 UTC).
        # Warn when a live position is open within 30 min of cutover.
        # Purely informational — no exit, no sizing, no action of any kind.
        _now_uk_fin = datetime.now(_UK_TZ)
        _uk_h_fin   = _now_uk_fin.hour + _now_uk_fin.minute / 60.0
        if 21.5 <= _uk_h_fin < 22.0:
            _pos_fin  = _live["open_position"]
            _notional = _pos_fin.get("notional", 0.0)
            _est_cost = round(_notional * 0.07 / 365, 4)
            _live_log(
                f"⚠️  [{_pos_fin.get('instrument','?')} {_pos_fin.get('direction','?').upper()}] "
                f"Open position within 30min of overnight DFB financing cutover (22:00 UK) — "
                f"est cost ~${_est_cost:.4f} (notional ${_notional:.2f} × 7% / 365)"
            )
        if not _sim.get("open_position"):   # sim path handles it via _sim_check_exit when sim is also open
            _sim_apply_pos_adjust()
        _live_check_exit(signals, regime)
        # Pyramid orphan protection: if primary just closed but addon legs remain,
        # close them immediately. Any primary exit (SL/TP/rotation) terminates addons.
        if not _live.get("open_position") and _live.get("pyramid_legs"):
            _live_log("⚠️ PYRAMID ORPHAN: primary leg closed -- immediately closing all addon legs")
            _live_close_all_addon_legs("orphan_primary_closed", signals)
        if _live.get("open_position"):
            # Primary still open -- pyramid exit checks then try to add leg 2
            _live_check_pyramid_exits(signals)
            if _live.get("open_position") and not _live.get("pyramid_legs"):
                _live_check_pyramid_entry(signals, regime)
            return    # still holding — skip entry logic

    # Belt-and-suspenders: stale addon legs without a primary position
    if _live.get("pyramid_legs"):
        _live_log("⚠️ STALE PYRAMID LEGS: no primary position -- clearing")
        _live["pyramid_legs"] = []
        _live["pyramid_agg_stop_level"] = None
        _live_save_state()

    # Daily drawdown circuit breaker — gates new-trade authority only.
    # Runs after exit management so a position that just closed still triggers
    # the check before the next entry is attempted this cycle.
    _live_check_circuit_breaker()
    # No early return on kill-switch off: _live_try_entry() still runs so all
    # observation logging fires during CB/halt periods. Order placement is blocked
    # by the DRY-RUN gate inside _live_try_entry() and by _live_trade_guard()
    # inside _live_open_position() as belt-and-suspenders.
    # Weekend and zero-balance gates below still apply in all states.

    # Update tiered defensive mode (safe to run during halted state).
    _live_update_defensive_mode()

    if _live.get("balance", 0.0) <= 0:
        _live_log("balance $0 or unavailable — skipping entry logic")
        return

    # Entry check (FX weekend gate is now per-instrument inside _live_try_entry)
    _live_try_entry(signals, regime)


# ── Startup ───────────────────────────────────────────────────────────────────


def _live_reconcile_positions() -> None:
    # Reconcile _live["open_position"] against IG real open positions.
    # Called at startup and mid-cycle after a failed close confirmation.
    # Catches two failure modes:
    #   Orphan: IG holds a position June does not know about (Redis state loss
    #           or manual entry). Reconstructs minimal state so exit management
    #           is immediately active from the next cycle.
    #   Stale:  June state says open but IG shows nothing. Clears it.
    data = _ig_live_get("/positions/otc", version="1")  # 404 → None → skip, not clear
    if data is None:
        # /positions/otc returned 404 or failed. Use account balance to disambiguate:
        # deposit=0 means no margin in use -> genuinely flat (treat as empty list).
        # deposit>0 means margin in use -> position visible soon (transitional), skip.
        _recon_bal = _ig_live_get("/accounts", version="1")
        if _recon_bal is not None:
            _recon_dep = 0.0
            for _recon_ac in _recon_bal.get("accounts", []):
                if _recon_ac.get("preferred"):
                    _recon_dep = float(_recon_ac.get("balance", {}).get("deposit", 0) or 0)
                    break
            if _recon_dep == 0.0:
                _live_log("Reconciliation: /positions/otc 404 but balance deposit=0 "
                          "-- treating as flat (no margin in use)")
                data = {"positions": []}
            else:
                _live_log(f"Reconciliation: /positions/otc 404 but deposit={_recon_dep:.2f}>0 "
                          f"-- possible transitional state, skipping to preserve state")
                return
        else:
            _live_log("Reconciliation: /positions/otc AND /accounts both failed -- skipping")
            return

    ig_positions = data.get("positions", [])
    june_pos     = _live.get("open_position")

    # Clean -- neither side has a position
    if not ig_positions and not june_pos:
        _live_log("Reconciliation: IG flat, June flat -- state matches")
        return

    # Both sides have a position -- verify deal count and deal_id agreement
    if ig_positions and june_pos:
        pyramid_legs   = _live.get("pyramid_legs", [])
        expected_n     = 1 + len(pyramid_legs)  # primary + active addon legs
        ig_deals       = {p.get("position", {}).get("dealId", "") for p in ig_positions}
        june_deal      = june_pos.get("deal_id", "")
        addon_deals    = {l.get("deal_id", "") for l in pyramid_legs}
        all_june_deals = {june_deal} | addon_deals
        if len(ig_positions) == expected_n and all_june_deals <= ig_deals:
            _live_log(
                f"Reconciliation: {expected_n} position(s) confirmed in IG -- state matches "
                f"({june_pos['instrument']} {june_pos['direction'].upper()})"
            )
        elif len(ig_positions) > _PYRAMID_MAX_LEGS:
            _live_log(
                f"Reconciliation: INCIDENT -- IG has {len(ig_positions)} positions "
                f"(max={_PYRAMID_MAX_LEGS}), June tracks {expected_n} -- "
                f"manual_review_required set"
            )
            _live["manual_review_required"] = True
        else:
            _live_log(
                f"Reconciliation: MISMATCH -- IG has {len(ig_positions)} position(s) "
                f"(expected {expected_n}), June deal={june_deal} -- manual check needed"
            )
        return

    # Critical: IG has position(s) but June state is None
    if ig_positions and not june_pos:
        ig_p       = ig_positions[0]
        pos_info   = ig_p.get("position", {})
        mkt_info   = ig_p.get("market", {})
        epic       = mkt_info.get("epic", "")
        sym        = {v: k for k, v in INSTRUMENTS.items()}.get(epic, epic)
        ig_dir     = pos_info.get("direction", "BUY")
        direction  = "long" if ig_dir == "BUY" else "short"
        ig_size    = float(pos_info.get("size", 0))
        fill_price = float(pos_info.get("level", 0.0))
        deal_id    = pos_info.get("dealId", "")
        deal_ref   = pos_info.get("dealReference", "")
        entry_time = time.time()
        try:
            _created = pos_info.get("createdDateUTC", "")
            if _created:
                _clean = _created.split(".")[0]
                entry_time = datetime.fromisoformat(_clean).replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            pass
        lot_sz   = _live_lot_sizes.get(sym, _LIVE_LOT_SIZE_FX)
        notional = ig_size * lot_sz * fill_price if fill_price > 0 else 0.0
        hold_min = (time.time() - entry_time) / 60.0

        _live["open_position"] = {
            "instrument":       sym,
            "direction":        direction,
            "deal_id":          deal_id,
            "deal_ref":         deal_ref,
            "fill_price":       fill_price,
            "ig_size":          ig_size,
            "pos_size":         notional,
            "leverage":         1,
            "notional":         notional,
            "entry_time":       entry_time,
            "entry_vol":        0.0,
            "entry_change_15m": 0.0,
            "conviction":       5,
            "claudia_pts":      0,
            "reversal_count":   0,
            "reconciled":       True,
        }
        _live_log("=" * 58)
        _live_log("*** ORPHAN POSITION DETECTED -- STATE RECONSTRUCTED ***")
        _live_log(f"   Instrument : {sym} {ig_dir}  size={ig_size}  fill={fill_price}")
        _live_log(f"   Deal ID    : {deal_id}")
        _live_log(f"   Hold time  : ~{hold_min:.0f} min (from IG creation timestamp)")
        _live_log(f"   Root cause : Redis state loss or manually-opened position")
        _live_log(f"   Action     : open_position populated -- exit management ACTIVE")
        _live_log(f"   stop/tp    : using sim calibration (conservative defaults)")
        _live_log(f"   *** VERIFY : Check IG app -- confirm position is intentional ***")
        _live_log("=" * 58)
        if len(ig_positions) > 1:
            _live_log(
                f"Reconciliation: {len(ig_positions)} IG positions total -- only first "
                f"(deal={deal_id}) reconstructed. Additional positions need manual management."
            )
        return

    # Stale: June state has position but IG shows nothing
    if not ig_positions and june_pos:
        sym     = june_pos.get("instrument", "?")
        deal_id = june_pos.get("deal_id", "?")
        _live_log("=" * 58)
        _live_log("** STALE STATE CLEARED **")
        _live_log(f"   June state: {sym} {june_pos.get('direction','?').upper()} deal={deal_id}")
        _live_log(f"   IG reports: no open positions")
        _live_log(f"   Action    : open_position cleared -- June is now flat")
        _live_log(f"   Likely    : position closed in IG app or before this restart")
        _live_log("=" * 58)
        _live["open_position"] = None
        _live.pop("manual_review_required", None)  # cascade fully resolved — unblock closes


def _apply_defect_quarantine() -> None:
    """Tag diagnosed, fixed code-defect trades in Redis so SAR/WR calculations skip them.

    Idempotent — safe to call on every startup. Reads _DEFECT_QUARANTINE registry;
    each entry must reference a real defect fixed in the same commit that added it.
    Never auto-fires based on loss size or streak — only explicit registry entries apply.

    Tagging flow:
      1. Read june_perf_stats:{sym} from Redis
      2. Find trades in [epoch_min, epoch_max] without excluded_defect_id already set
      3. Set excluded_defect_id = defect_id on matching records
      4. Write back; log count tagged (0 = already done or no matching trades)
    """
    import logging as _log
    log = _log.getLogger()
    for entry in _DEFECT_QUARANTINE:
        sym       = entry["sym"]
        defect_id = entry["defect_id"]
        emin      = entry["epoch_min"]
        emax      = entry["epoch_max"]
        note      = entry.get("note", "")
        try:
            r   = _redis()
            key = f"june_perf_stats:{sym}"
            raw = r.get(key)
            if not raw:
                continue
            stats  = json.loads(raw)
            trades = stats.get("trades", [])
            tagged = 0
            for t in trades:
                ep = t.get("epoch", 0)
                if emin <= ep <= emax and not t.get("excluded_defect_id"):
                    t["excluded_defect_id"] = defect_id
                    tagged += 1
            if tagged:
                stats["trades"] = trades
                r.set(key, json.dumps(stats))
                log.info(
                    f"[QUARANTINE] Tagged {tagged} {sym} trade(s) as {defect_id!r} "
                    f"(epoch {emin}–{emax}). Note: {note[:120]}"
                )
            else:
                log.debug(f"[QUARANTINE] {sym}/{defect_id}: no new trades to tag (already done or none in window)")
        except Exception as exc:
            log.warning(f"[QUARANTINE] Failed to tag {sym}/{defect_id}: {exc}")


def _live_startup() -> None:
    """Initialize live trading state. Called once from main() after sim_startup().
    Safe to call when live account is not available — degrades gracefully.
    """
    global _live
    if not all([IG_LIVE_BASE, IG_LIVE_KEY, IG_LIVE_USER, IG_LIVE_PASS]):
        print(f"[{_ts()}] ℹ️  Live trading: IG live credentials not configured — live step disabled",
              flush=True)
        return

    # Load persisted state from prior session
    loaded = _live_load_state()

    # Ensure all required keys present
    defaults = {
        "balance":              0.0,
        "balance_total":        0.0,
        "balance_margin":       0.0,
        "balance_fetched_at":   0,
        "cumulative_earned_pnl": 0.0,
        "pnl_seen_refs":        [],
        "pnl_fetched_at":       0,
        "skim_phase":           "pre300",
        "next_skim_threshold":  _LIVE_SKIM_PHASE1_TRIGGER,
        "skim_pending":         0.0,
        "skimmed_total":        0.0,
        "last_half_skim_time":  0.0,
        "balance_day_start":      0.0,
        "balance_day_start_date": "",
        "open_position":        None,
        "total_trades":         0,
        "total_wins":           0,
        "total_losses":         0,
        "long_pnl":             0.0,
        "short_pnl":            0.0,
        "long_trades":          0,
        "short_trades":         0,
        "long_wins":            0,
        "short_wins":           0,
        "boost_expiry":         {},
        "pause_expiry":         {},
        "streak_state":         {},
        "trade_history":        [],
        # Tiered defensive mode state
        "global_mode":               "normal",
        "global_mode_bal_entry":     0.0,
        "global_mode_entered_at":    0.0,
        "instrument_mode":           {},
        "instrument_mode_entered_at": {},
        "instrument_stopouts_today": {},
        "instrument_won_after_def":  {},
        "pyramid_legs":              [],   # addon legs for no-decay pyramid
        "pyramid_agg_stop_level":    None, # aggregate stop level when pyramid active
        # Live performance-gated phase system (separate from sim's phase tracking)
        "live_phase":                1,    # 1=3:1  2=5:1  3=10:1
        "live_phase_trades":         0,
        "live_phase_wins":           0,
        "live_phase_losses":         0,
        "live_phase_consec_losses":  0,
        "live_phase_entry_balance":  None, # set on first activation or phase change
    }
    for k, v in defaults.items():
        _live.setdefault(k, v)

    # Post-load: correct balance_day_start from the per-day Redis key when it exists.
    # The per-day key (june_balance_day_start:YYYY-MM-DD) is written on the first
    # genuine seed of each UTC day in _live_poll_balance(). If a prior session seeded
    # from a wrong IG balance (e.g., stale reading with open margin), this override
    # corrects the value on every restart before the CB can fire against it.
    try:
        _pld_today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        _pld_val   = _redis().get(f"june_balance_day_start:{_pld_today}")
        if _pld_val:
            _pld_saved = _live.get("balance_day_start", 0.0)
            _live["balance_day_start"]      = float(_pld_val)
            _live["balance_day_start_date"] = _pld_today
            if abs(_pld_saved - float(_pld_val)) > 0.01:
                _live_log(f"Day-start balance RESTORED: ${float(_pld_val):.2f} (state had ${_pld_saved:.2f})")
    except Exception:
        pass

    # Fetch initial balance (immediate, not deferred)
    global _live_balance_polled_at
    _live_balance_polled_at = 0.0   # force immediate fetch
    _live_poll_balance()

    # Fetch per-instrument lot sizes from LIVE IG API so sizing is correct
    _live_log(f"Fetching live market data for {len(INSTRUMENTS)} instruments...")
    _mkt_failed: list = []
    for _lfd_sym, _lfd_epic in list(INSTRUMENTS.items()):
        if not _live_fetch_market_data(_lfd_sym, _lfd_epic):
            _mkt_failed.append((_lfd_sym, _lfd_epic))
        time.sleep(0.3)
    if _mkt_failed:
        _loaded = len(INSTRUMENTS) - len(_mkt_failed)
        _live_log(
            f"Market data: {_loaded}/{len(INSTRUMENTS)} loaded — "
            f"retrying {len(_mkt_failed)} failed instrument(s) after 5s: "
            f"{[s for s,_ in _mkt_failed]}"
        )
        time.sleep(5.0)
        _still_failed: list = []
        for _lfd_sym, _lfd_epic in _mkt_failed:
            if not _live_fetch_market_data(_lfd_sym, _lfd_epic):
                _still_failed.append(_lfd_sym)
            time.sleep(0.3)
        if _still_failed:
            _live_log(
                f"Market data: {len(_still_failed)} instrument(s) still missing after retry "
                f"(margin gate will block them — fail-closed): {_still_failed}"
            )
        else:
            _live_log(f"Market data retry succeeded — all {len(INSTRUMENTS)} instruments loaded")
    else:
        _live_log(f"Market data: all {len(INSTRUMENTS)} instruments loaded")

    # Startup reconciliation: compare June state against IG real open positions
    _live_reconcile_positions()
    _live_migrate_perf_blocks()

    _live_save_state()

    _refresh_live_kill_switch()  # read Redis state before printing banner

    # Warn if circuit breaker fired in a prior session (48h alert window)
    try:
        import json as _json3
        _cb_alert_raw = _redis().get("june_circuit_breaker_alert")
        if _cb_alert_raw:
            _cb = _json3.loads(_cb_alert_raw)
            _ks = "ON" if _june_live_trading_enabled else "OFF"
            _parts = [
                "[" + _ts() + "] *** PRIOR CIRCUIT BREAKER ALERT (within 48h)",
                "   Fired at   : " + str(_cb.get("fired_at", "?")) + " UTC",
                "   Day-start  : $" + str(round(_cb.get("day_start", 0), 2)),
                "   At breach  : $" + str(round(_cb.get("current", 0), 2)),
                "   Drawdown   : " + str(round(_cb.get("drawdown_pct", 0), 2)) + "%",
                "   Kill switch: " + _ks,
            ]
            print(chr(10).join(_parts), flush=True)
    except Exception:
        pass
    status = "LOADED from Redis" if loaded else "FRESH state"
    kswitch = "ON 🟢" if _june_live_trading_enabled else "OFF 🔒"
    tw = _live.get("total_wins", 0)
    tl = _live.get("total_losses", 0)
    print(
        f"[{_ts()}] 🟢 Live trading initialized ({status}) | "
        f"kill switch {kswitch} | "
        f"balance ${_live.get('balance', 0.0):.2f} | "
        f"earned P&L ${_live.get('cumulative_earned_pnl', 0.0):.2f} | "
        f"trades {tw}W/{tl}L",
        flush=True,
    )

    if _live.get("open_position"):
        pos = _live["open_position"]
        hold_min = (time.time() - pos.get("entry_time", time.time())) / 60.0
        _live_log(
            f"⚠️  Resuming open live position: {pos['instrument']} "
            f"{pos['direction'].upper()} deal={pos.get('deal_id','?')} "
            f"held {hold_min:.0f}min"
        )


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
    print("  Live trading: active when june_live_enabled=true in Redis", flush=True)
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

    # Start live trading (gated behind kill switch; safe to call with switch off)
    _live_startup()

    # Restore rolling price history so grind detector doesn't cold-start after short restarts
    _history_load()

    # Tag diagnosed+fixed defect trades in Redis so SAR/WR calcs skip them
    _apply_defect_quarantine()

    if not INSTRUMENTS:
        print(f"[{_ts()}] ❌ No valid instruments after verification — exiting", flush=True)
        sys.exit(1)

    print(f"[{_ts()}] 🚀 Main loop starting — {POLL_ACTIVE}s weekday / paused weekends", flush=True)

    consec_errors = 0
    while True:
        try:
            # Weekend closure — pause entirely unless 24/7 instruments are active.
            # _CONTINUOUS_INSTRUMENTS is empty until crypto epics are confirmed;
            # current behaviour is identical to before: sleep 30 min and skip poll.
            if is_weekend_closure() and not _CONTINUOUS_INSTRUMENTS:
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
            import traceback; traceback.print_exc()
            if consec_errors >= 5:
                print(f"[{_ts()}] 💀 5 consecutive errors — sleeping 5 min before retry", flush=True)
                time.sleep(300)
                consec_errors = 0
            else:
                time.sleep(30)


if __name__ == "__main__":
    main()
