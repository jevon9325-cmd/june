"""Build 4C-A3 broker-truth settlement reconciliation — focused tests.

Extracts _live_reconcile_provisional_settlements from june.py and drives it with
a fake Redis (durable list) and a fake _ig_live_get returning controlled
/history/activity and /history/transactions payloads. Verifies: exact confirm,
UNKNOWN preserved, ambiguity, partial deferral, cross-instrument non-match,
idempotency/exactly-once perf feed, backoff, and no-deal-id skip.
"""
import json
from types import SimpleNamespace
from unittest.mock import Mock
from test_broker_identity import execute, function


class FakeList:
    """In-memory stand-in for a Redis list (june_live_trade_history_full)."""
    def __init__(self, items): self.items = [json.dumps(x) for x in items]
    def lrange(self, key, a, b):
        return list(self.items) if b == -1 else self.items[a:b+1]
    def lindex(self, key, i):
        return self.items[i] if 0 <= i < len(self.items) else None
    def lset(self, key, i, v): self.items[i] = v
    def parsed(self): return [json.loads(x) for x in self.items]


def _harness(records, activity_by_deal, transactions, *, now=1_000_000.0):
    fake = FakeList(records)
    perf_calls = []
    observe = []

    def _igget(path, params=None, version="1", not_found_default=None):
        if path == "/history/transactions":
            return {"transactions": transactions}
        if path == "/history/activity":
            did = (params or {}).get("dealId")
            return {"activities": activity_by_deal.get(did, [])}
        return None

    ns = dict(
        json=json, time=SimpleNamespace(time=lambda: now),
        _redis=Mock(return_value=fake),
        _LIVE_TRADE_HIST_KEY="hist",
        _RECON_MAX_PER_CYCLE=5, _RECON_MIN_AGE_SECS=120,
        _RECON_BASE_BACKOFF_SECS=300, _RECON_MAX_BACKOFF_SECS=6*3600,
        _RECON_GIVEUP_SECS=14*24*3600, _RECON_SCHEMA_VERSION=1,
        _ig_live_get=_igget,
        _live_perf_record=lambda sym, won, sar, pnl_dollar=0.0, **k: perf_calls.append((sym, won, pnl_dollar)),
        _live_observe=lambda ev, *a, **k: observe.append(ev),
        _live_log=Mock(),
    )
    execute([function("_live_reconcile_provisional_settlements")], ns)
    ns["_live_reconcile_provisional_settlements"]()
    return fake.parsed(), perf_calls, observe


def _prov(deal_id="D1", sym="SILVER", epic="SILVER", entry=6073.3, exit_epoch=900_000):
    return {"instrument": sym, "direction": "long", "deal_id": deal_id,
            "entry_price": entry, "exit_price": None, "dollar_pnl": None,
            "settlement_state": "PROVISIONAL",
            "settlement_source": "close_guard_absent:REST-deal",
            "exit_epoch": exit_epoch}


def _closed_activity(deal_id, epic="SILVER", level=6076.0, size=0.09, partial=False):
    return [{"dealId": deal_id, "epic": epic, "date": "2026-09-29T05:57:00",
             "details": {"level": level, "size": size, "direction": "BUY",
                         "actions": [{"actionType": "POSITION_PARTIALLY_CLOSED" if partial
                                      else "POSITION_CLOSED", "affectedDealId": deal_id}]}}]


def _tx(name="Silver", close=6076.0, pnl="$0.35", size="+0.09"):
    return {"transactionType": "DEAL", "instrumentName": name, "closeLevel": close,
            "openLevel": 6073.3, "profitAndLoss": pnl, "size": size,
            "date": "29-Sep-26", "reference": "R1"}


# ─────────────────────────── happy path ─────────────────────────────────────

def test_exact_confirm_promotes_and_feeds_once():
    recs, perf, obs = _harness(
        [_prov("D1")],
        {"D1": _closed_activity("D1", level=6076.0)},
        [_tx(close=6076.0, pnl="$0.35")],
    )
    r = recs[0]
    assert r["settlement_state"] == "CONFIRMED"
    assert r["dollar_pnl"] == 0.35
    assert r["exit_price"] == 6076.0
    assert r["pnl_source"] == "broker_confirmed_transaction_reconciled"
    assert r["reconciled"] is True
    assert perf == [("SILVER", True, 0.35)]        # fed exactly once, won=True
    assert "settlement_confirmed" in obs


def test_losing_confirm_sign_preserved():
    recs, perf, obs = _harness([_prov("D1")],
                               {"D1": _closed_activity("D1", level=6050.0)},
                               [_tx(close=6050.0, pnl="$-1.20")])
    assert recs[0]["dollar_pnl"] == -1.20
    assert perf == [("SILVER", False, -1.20)]


# ─────────────────────────── UNKNOWN preserved ──────────────────────────────

def test_no_close_activity_stays_provisional_unknown():
    recs, perf, obs = _harness([_prov("D1")], {"D1": []}, [_tx()])
    assert recs[0]["settlement_state"] == "PROVISIONAL"
    assert recs[0]["dollar_pnl"] is None          # UNKNOWN, never 0
    assert perf == []
    assert "settlement_reconcile_deferred" in obs


def test_no_matching_transaction_stays_unknown():
    recs, perf, obs = _harness([_prov("D1")],
                               {"D1": _closed_activity("D1", level=6076.0)},
                               [_tx(close=9999.0)])   # level mismatch -> 0 candidates
    assert recs[0]["dollar_pnl"] is None
    assert recs[0]["settlement_state"] == "PROVISIONAL"
    assert "settlement_match_ambiguous" in obs


def test_ambiguous_two_candidates_stays_unknown():
    recs, perf, obs = _harness([_prov("D1")],
                               {"D1": _closed_activity("D1", level=6076.0)},
                               [_tx(close=6076.0, pnl="$0.35"),
                                _tx(close=6076.0, pnl="$0.40")])
    assert recs[0]["dollar_pnl"] is None          # 2 candidates -> ambiguous -> no confirm
    assert "settlement_match_ambiguous" in obs
    assert perf == []


# ─────────────────────────── partial close ──────────────────────────────────

def test_partial_close_not_finalized():
    recs, perf, obs = _harness([_prov("D1")],
                               {"D1": _closed_activity("D1", partial=True)},
                               [_tx()])
    assert recs[0]["settlement_state"] == "PROVISIONAL"
    assert perf == []
    assert "settlement_reconcile_deferred" in obs


# ─────────────────────────── cross-instrument safety ────────────────────────

def test_cross_instrument_transaction_does_not_match():
    # activity confirms SILVER close at 6076; only a GOLD tx exists at same level.
    recs, perf, obs = _harness([_prov("D1", sym="SILVER")],
                               {"D1": _closed_activity("D1", epic="SILVER", level=6076.0)},
                               [_tx(name="Spot Gold", close=6076.0, pnl="$0.35")])
    assert recs[0]["dollar_pnl"] is None          # instrument name mismatch -> no match
    assert recs[0]["settlement_state"] == "PROVISIONAL"


# ─────────────────────────── identity / eligibility ─────────────────────────

def test_no_deal_id_skipped():
    rec = _prov("")
    rec["deal_id"] = ""
    recs, perf, obs = _harness([rec], {}, [_tx()])
    assert recs[0]["settlement_state"] == "PROVISIONAL"
    assert perf == []

def test_too_fresh_skipped():
    # exit 30s ago (< _RECON_MIN_AGE_SECS) -> not attempted
    recs, perf, obs = _harness([_prov("D1", exit_epoch=999_970)],
                               {"D1": _closed_activity("D1")}, [_tx()])
    assert recs[0]["settlement_state"] == "PROVISIONAL"
    assert "settlement_reconcile_attempt" not in obs

def test_legacy_record_untouched():
    legacy = {"instrument": "HO", "dollar_pnl": 2.18, "settlement_state": None}
    recs, perf, obs = _harness([legacy], {}, [_tx()])
    assert recs[0] == legacy                       # legacy left byte-identical
    assert perf == []

def test_already_confirmed_not_refed():
    done = _prov("D1"); done["settlement_state"] = "CONFIRMED"; done["dollar_pnl"] = 0.35
    recs, perf, obs = _harness([done], {"D1": _closed_activity("D1")}, [_tx()])
    assert perf == []                              # never re-fed
    assert recs[0]["dollar_pnl"] == 0.35


# ─────────────────────────── idempotency / backoff ──────────────────────────

def test_backoff_prevents_immediate_reattempt():
    rec = _prov("D1")
    rec["recon_attempts"] = 1
    rec["recon_last_attempt"] = 1_000_000 - 100   # 100s ago < base backoff 300s
    recs, perf, obs = _harness([rec], {"D1": []}, [_tx()])
    assert "settlement_reconcile_attempt" not in obs   # backoff not elapsed

def test_attempt_recorded_durably_even_on_defer():
    recs, perf, obs = _harness([_prov("D1")], {"D1": []}, [_tx()])
    assert recs[0]["recon_attempts"] == 1
    assert recs[0]["recon_last_attempt"] == 1_000_000
    assert "settlement_reconcile_attempt" in obs


# ─────────────────────────── patch shape / invariants ───────────────────────

def test_no_risk_constant_touched():
    from pathlib import Path
    s = Path("june.py").read_text(encoding="utf-8")
    assert "_LIVE_CIRCUIT_BREAKER_PCT = -0.05" in s
    assert "_mpd_mfe_gate" in s
    assert "_PYRAMID_PROFIT_GATE_PCT = 0.0015" in s
    assert "_ROLLING_MAX_GENERATIONS       = 2" in s
    # reconciler wired after poll_pnl
    assert "_live_reconcile_provisional_settlements()  # B4CA3" in s
