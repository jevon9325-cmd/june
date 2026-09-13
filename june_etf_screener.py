#!/usr/bin/env python3
"""
june_etf_screener.py  -  ETF Discovery Automation for June

Pulls the IG ETF screener, applies the proven mechanical filters, and stores a
dated candidate shortlist in Redis (june_etf_shortlist) for human review.

What this does:
  - Filters 13,534 ETFs -> LSE + UCITS + non-leveraged + non-inverse + USD + ISA
  - Deduplicates share-class pairs (accumulating/distributing) by ISIN
  - Sorts by AUM (liquidity proxy), applies minimum AUM and expense ratio gates
  - Stores top-N shortlist in Redis with screener-derived metadata

What this does NOT do:
  - Check actual bid/offer spread on IG (IG demo API search does not find LSE ETF
    epics -- manual verification on IG live platform required for shortlisted candidates)
  - Modify June's INSTRUMENTS dict -- all adds are deliberate and manual
  - Rate-limit June's live trading cycle (zero interaction)

Run manually:
  cd /opt/bots/june && source /opt/bots/june.env && python3 june_etf_screener.py

Cron (weekly, Sunday 08:00 UTC):
  0 8 * * 0 source /opt/bots/june.env && python3 /opt/bots/june/june_etf_screener.py
"""

import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

SCREENER_URL       = "https://etfscreener.ig.com/assets/js/tableData.json"
REDIS_OUTPUT_KEY   = "june_etf_shortlist"
REDIS_TTL          = 8 * 24 * 3600   # 8 days
TOP_N              = 25              # candidates to store after AUM sort
MIN_AUM_M          = 250             # skip funds < $250M AUM (illiquidity risk)
MAX_EXPENSE_RATIO  = 0.50            # skip funds > 0.50% OCF (cost drag)

# Already in June's INSTRUMENTS -- skip even if screener surfaces them
ALREADY_IN_JUNE = frozenset({
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "EURGBP",
    "NZDUSD", "USDCHF", "SILVER", "GOLD", "OIL", "NATGAS",
    "SPX500", "NAS100", "GER40", "BTC", "ETH",
    "NVDA", "AAPL", "MSFT", "AVGO", "AMD", "INTC", "MU", "SPCX",
})


def _redis():
    import redis as _r
    return _r.Redis(
        host=os.getenv("REDIS_HOST", ""),
        port=int(os.getenv("REDIS_PORT", 15074)),
        password=os.getenv("REDIS_PASSWORD", ""),
        ssl=False,
        decode_responses=True,
        socket_timeout=10,
    )


def fetch_screener() -> list:
    print(f"Fetching IG ETF screener ({SCREENER_URL})...")
    req = urllib.request.Request(SCREENER_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        rows = json.load(resp)
    print(f"  {len(rows)} total ETFs in screener")
    return rows


def apply_filters(rows: list) -> list:
    """Apply the proven mechanical filters. Returns filtered list."""
    filtered = []
    for r in rows:
        if r.get("LeveragedFund") != "No":
            continue
        if r.get("InverseFund") != "No":
            continue
        if r.get("UCITS") != "Yes":
            continue
        if r.get("PrimaryExchange") not in ("LSE", "London Stock Exchange"):
            continue
        if r.get("Currency") != "USD":
            continue
        if r.get("ISA") != "Yes":
            continue
        # Skip below AUM floor
        aum = r.get("NetAssets") or 0
        if aum < MIN_AUM_M * 1_000_000:
            continue
        # Skip high-cost funds (field may be str or float depending on screener version)
        _ocf_raw = r.get("ActualManagementFee") or r.get("ExpenseRatio")
        try:
            ocf = float(_ocf_raw) if _ocf_raw is not None else None
        except (TypeError, ValueError):
            ocf = None
        if ocf is not None and ocf > MAX_EXPENSE_RATIO:
            continue
        # Skip already-in-June symbols
        sym = (r.get("PrimaryExchangeSymbol") or "").upper()
        if sym and sym in ALREADY_IN_JUNE:
            continue
        filtered.append(r)

    print(f"  {len(filtered)} ETFs pass mechanical filters")
    return filtered


def deduplicate_by_isin(rows: list) -> list:
    """Keep one row per ISIN (accumulating/distributing pairs share the same fund).
    Prefer accumulating share class (ticker ends in 'a'). Fall back to highest AUM."""
    by_isin: dict = {}
    for r in rows:
        isin = r.get("ISIN") or ""
        if not isin:
            key = r.get("PrimaryExchangeSymbol") or r.get("Name") or str(id(r))
            by_isin[str(key)] = r
            continue
        if isin not in by_isin:
            by_isin[isin] = r
        else:
            existing = by_isin[isin]
            sym_new = (r.get("PrimaryExchangeSymbol") or "").lower()
            sym_old = (existing.get("PrimaryExchangeSymbol") or "").lower()
            if sym_new.endswith("a") and not sym_old.endswith("a"):
                by_isin[isin] = r
            elif (r.get("NetAssets") or 0) > (existing.get("NetAssets") or 0):
                by_isin[isin] = r

    result = list(by_isin.values())
    print(f"  {len(result)} unique funds after ISIN deduplication")
    return result


def build_shortlist(rows: list, top_n: int = TOP_N) -> list:
    """Sort by AUM, take top_n, format for Redis storage."""
    sorted_rows = sorted(rows, key=lambda r: r.get("NetAssets") or 0, reverse=True)
    candidates = sorted_rows[:top_n]

    shortlist = []
    for rank, r in enumerate(candidates, 1):
        aum_b = (r.get("NetAssets") or 0) / 1e9
        _ocf = r.get("ActualManagementFee") or r.get("ExpenseRatio")
        try:
            _ocf = float(_ocf) if _ocf is not None else None
        except (TypeError, ValueError):
            _ocf = None
        shortlist.append({
            "rank":           rank,
            "symbol":         r.get("PrimaryExchangeSymbol", "?"),
            "name":           r.get("Name", "?"),
            "isin":           r.get("ISIN", ""),
            "issuer":         r.get("Issuer", "?"),
            "aum_bn":         round(aum_b, 2),
            "expense_ratio":  _ocf,
            "asset_class":    r.get("AssetClass", "?"),
            "global_sector":  r.get("GlobalSector", "?"),
            "return_1y":      r.get("ReturnM12"),
            "return_3m":      r.get("ReturnM3"),
            "std_dev_36m":    r.get("StandardDeviation36M"),
            "distribution":   r.get("DistributionType", "?"),
            "uk_reporting":   r.get("UKReporting"),
            # Spread economics: NOT automatable via IG demo API
            # Manual check: open IG live platform, search symbol, note bid/offer
            # spread as % of mid. Target: spread/ATR < 1.0 (same gate as June)
            "spread_check":   "MANUAL_REQUIRED",
            "min_deal_check": "MANUAL_REQUIRED",
        })
    return shortlist


def store_shortlist(shortlist: list) -> None:
    now_utc = datetime.now(timezone.utc).isoformat()
    payload = {
        "generated_at":    now_utc,
        "screener_url":    SCREENER_URL,
        "filter_summary":  "LSE + UCITS + non-leveraged + non-inverse + USD + ISA + AUM>$250M + OCF<0.50%",
        "dedup_note":      "One entry per ISIN (accumulating class preferred)",
        "spread_note":     (
            "spread_check and min_deal_check require manual verification on IG live platform. "
            "IG demo API search does not index LSE ETF epics -- automated spread check not possible."
        ),
        "activation_note": (
            "Adding any candidate to June's INSTRUMENTS requires deliberate manual addition "
            "to BOTH the INSTRUMENTS dict AND the appropriate frozenset in june.py. "
            "Same deliberate process as every instrument added so far."
        ),
        "candidates":      shortlist,
    }
    r = _redis()
    r.set(REDIS_OUTPUT_KEY, json.dumps(payload, indent=2), ex=REDIS_TTL)
    print(f"\nShortlist stored -> Redis key '{REDIS_OUTPUT_KEY}' (TTL {REDIS_TTL // 86400}d)")


def print_shortlist(shortlist: list) -> None:
    print(f"\n{'='*70}")
    print(f"  ETF DISCOVERY SHORTLIST -- {len(shortlist)} candidates")
    print(f"  Filters: LSE + UCITS + non-leveraged + USD + ISA + AUM>$250M + OCF<0.50%")
    print(f"  Deduped by ISIN (accumulating class preferred)")
    print(f"{'='*70}")
    print(f"  {'#':>2}  {'Symbol':>8}  {'AUM':>7}  {'1Y%':>6}  {'OCF%':>5}  {'Class':>10}  Name")
    print(f"  {'--':>2}  {'------':>8}  {'---':>7}  {'---':>6}  {'----':>5}  {'-----':>10}  {'----':>35}")
    for c in shortlist:
        sym  = c["symbol"]
        aum  = f"${c['aum_bn']:.1f}B"
        ret1 = f"{c['return_1y']:.1f}%" if c["return_1y"] is not None else "  N/A"
        ocf  = f"{c['expense_ratio']:.2f}%" if c["expense_ratio"] is not None else "  N/A"
        cls  = c["asset_class"][:10]
        name = c["name"][:35]
        print(f"  {c['rank']:>2}  {sym:>8}  {aum:>7}  {ret1:>6}  {ocf:>5}  {cls:>10}  {name}")
    print()
    print(f"  NOTE: spread_check + min_deal_check: MANUAL verification required")
    print(f"        IG demo API search cannot find LSE ETF epics -- automated check not possible")
    print(f"        Manual process: IG live platform -> search symbol -> check bid/offer spread as % of mid")
    print(f"        Gate: spread/ATR < 1.0 (same threshold as June's existing spread_atr gate)")


def main():
    print(f"\n{'='*60}")
    print(f"  june_etf_screener.py  --  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    try:
        rows = fetch_screener()
    except Exception as e:
        print(f"ERROR: screener fetch failed -- {e}")
        sys.exit(1)

    filtered  = apply_filters(rows)
    deduped   = deduplicate_by_isin(filtered)
    shortlist = build_shortlist(deduped)

    print_shortlist(shortlist)

    try:
        store_shortlist(shortlist)
    except Exception as e:
        print(f"\nWARNING: Redis store failed -- {e}")
        print("(Shortlist printed above -- copy manually if needed)")

    print("\nDone.")


if __name__ == "__main__":
    main()
