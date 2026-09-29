"""Build 4C-A3.1 matching-completeness — focused tests.

Extracts the repaired reconciler (+ helpers) and drives it with a fake Redis
durable list and a fake _ig_live_get serving /history/activity (Match A direct +
Match B affectedDealId) and /history/transactions. Verifies fail-closed matching,
epic normalization, exactly-once, perf-replay, and the SILVER/OIL shapes.
"""
import json
from types import SimpleNamespace
from unittest.mock import Mock
from test_broker_identity import execute, function


class FakeList:
    def __init__(self, items): self.items = [json.dumps(x) for x in items]
    def lrange(self, k, a, b): return list(self.items) if b == -1 else self.items[a:b+1]
    def lindex(self, k, i): return self.items[i] if 0 <= i < len(self.items) else None
    def lset(self, k, i, v): self.items[i] = v
    def parsed(self): return [json.loads(x) for x in self.items]


def _run(records, activities, transactions, *, now=1_000_000.0, activity_err=False):
    fake = FakeList(records); perf = []; obs = []

    def _igget(path, params=None, version="1", not_found_default=None):
        if path == "/history/transactions":
            return {"transactions": transactions}
        if path == "/history/activity":
            if activity_err:
                return None                      # simulate 403/timeout -> defer
            # ensure caller passed from/to (regression: A3 passed maxSpanSeconds)
            assert params and "from" in params and "to" in params, "activity must use from/to"
            return {"activities": activities}
        return None

    ns = dict(
        json=json, time=SimpleNamespace(time=lambda: now),
        _redis=Mock(return_value=fake),
        _LIVE_TRADE_HIST_KEY="hist",
        # Mirror the production module-level token map so _recon_instr_matches
        # (which references it as a global) resolves inside the exec namespace.
        _RECON_INSTR_TOKENS={
            "SILVER": ("SILVER",), "GOLD": ("GOLD",), "OIL": ("OIL", "CRUDE"),
            "NATGAS": ("NATURAL GAS", "NAT GAS"), "WHEAT": ("WHEAT",),
            "SUGAR": ("SUGAR",), "COCOA": ("COCOA",), "HO": ("HEATING OIL",),
        },
        _RECON_MAX_PER_CYCLE=5, _RECON_MIN_AGE_SECS=120,
        _RECON_BASE_BACKOFF_SECS=300, _RECON_MAX_BACKOFF_SECS=6*3600,
        _RECON_GIVEUP_SECS=14*24*3600, _RECON_SCHEMA_VERSION=1,
        _ig_live_get=_igget,
        _live_perf_record=lambda s, w, sar, pnl_dollar=0.0, **k: perf.append((s, w, pnl_dollar)),
        _live_observe=lambda ev, *a, **k: obs.append(ev),
        _live_log=Mock(),
    )
    for fn in ("_recon_instr_matches", "_recon_iso", "_live_reconcile_provisional_settlements"):
        execute([function(fn)], ns)
    ns["_live_reconcile_provisional_settlements"]()
    return fake.parsed(), perf, obs


def _prov(deal="D1", sym="SILVER", entry=6073.3, size=0.09, exit_epoch=900_000):
    return {"instrument": sym, "direction": "long", "deal_id": deal, "entry_price": entry,
            "exit_price": None, "ig_size": size, "dollar_pnl": None,
            "settlement_state": "PROVISIONAL",
            "settlement_source": "close_guard_absent:REST-deal", "exit_epoch": exit_epoch}


def _act_direct(deal, epic="CS.D.CFDSILVER.BMU.IP", level=6076.0, size=0.09, partial=False):
    return {"dealId": deal, "epic": epic, "date": "2026-09-29T05:57:00",
            "details": {"level": level, "size": size, "direction": "BUY",
                        "actions": [{"actionType": "POSITION_PARTIALLY_CLOSED" if partial
                                     else "POSITION_CLOSED", "affectedDealId": deal}]}}


def _act_affected(close_deal, open_deal, epic="CS.D.CFDSILVER.BMU.IP", level=6076.0, partial=False):
    # broker-managed close: primary dealId differs; our open deal is affectedDealId
    return {"dealId": close_deal, "epic": epic, "date": "2026-09-29T05:57:00",
            "details": {"level": level, "size": 0.09, "direction": "SELL",
                        "actions": [{"actionType": "POSITION_PARTIALLY_CLOSED" if partial
                                     else "POSITION_CLOSED", "affectedDealId": open_deal}]}}


def _tx(name="Silver", close=6076.0, pnl="$0.35"):
    return {"transactionType": "DEAL", "instrumentName": name, "closeLevel": close,
            "openLevel": 6073.3, "profitAndLoss": pnl, "size": "+0.09",
            "date": "29-Sep-26", "reference": "R1"}


# ── 1: Match A direct still works ───────────────────────────────────────────
def test_match_a_direct_confirms():
    recs, perf, obs = _run([_prov("D1")], [_act_direct("D1")], [_tx()])
    assert recs[0]["settlement_state"] == "CONFIRMED"
    assert recs[0]["dollar_pnl"] == 0.35
    assert "recon:A_direct" in recs[0]["settlement_source"]
    assert perf == [("SILVER", True, 0.35)]

# ── 2: Match B affectedDealId fallback ──────────────────────────────────────
def test_match_b_affected_confirms():
    recs, perf, obs = _run([_prov("OPEN1")],
                           [_act_affected("CLOSE9", "OPEN1")], [_tx()])
    assert recs[0]["settlement_state"] == "CONFIRMED"
    assert recs[0]["dollar_pnl"] == 0.35
    assert "recon:B_affected" in recs[0]["settlement_source"]

# ── 3: affectedDealId partial -> not final ──────────────────────────────────
def test_match_b_partial_defers():
    recs, perf, obs = _run([_prov("OPEN1")],
                           [_act_affected("CLOSE9", "OPEN1", partial=True)], [_tx()])
    assert recs[0]["settlement_state"] == "PROVISIONAL"
    assert perf == []
    assert "settlement_reconcile_deferred" in obs

# ── 4: multiple affected close candidates -> defer ──────────────────────────
def test_match_b_multiple_affected_defers():
    recs, perf, obs = _run([_prov("OPEN1")],
                           [_act_affected("C1", "OPEN1"), _act_affected("C2", "OPEN1")], [_tx()])
    assert recs[0]["settlement_state"] == "PROVISIONAL"
    assert "settlement_match_ambiguous" in obs

# ── 5: unrelated activity ignored ───────────────────────────────────────────
def test_unrelated_activity_ignored():
    other = _act_affected("CX", "SOMEONE_ELSE")
    recs, perf, obs = _run([_prov("OPEN1")], [other], [_tx()])
    assert recs[0]["settlement_state"] == "PROVISIONAL"

# ── 6/7/8: cross-match protections at the transaction join ──────────────────
def test_level_mismatch_no_confirm():
    recs, perf, obs = _run([_prov("D1")], [_act_direct("D1", level=6076.0)], [_tx(close=9999.0)])
    assert recs[0]["dollar_pnl"] is None

def test_two_tx_candidates_defer():
    recs, perf, obs = _run([_prov("D1")], [_act_direct("D1", level=6076.0)],
                           [_tx(close=6076.0, pnl="$0.35"), _tx(close=6076.0, pnl="$0.40")])
    assert recs[0]["dollar_pnl"] is None
    assert "settlement_match_ambiguous" in obs

# ── 10: instrument mismatch -> no match ─────────────────────────────────────
def test_instrument_mismatch_no_match():
    recs, perf, obs = _run([_prov("D1", sym="SILVER")], [_act_direct("D1", level=6076.0)],
                           [_tx(name="Spot Gold", close=6076.0)])
    assert recs[0]["dollar_pnl"] is None

# ── 11: epic:null with authoritative symbol normalization -> valid ──────────
def test_symbol_normalization_valid_match():
    # record has no epic; SILVER normalizes to token "SILVER" in "Spot Silver"
    recs, perf, obs = _run([_prov("D1", sym="SILVER")], [_act_direct("D1", level=6076.0)],
                           [_tx(name="Spot Silver", close=6076.0, pnl="$0.35")])
    assert recs[0]["dollar_pnl"] == 0.35

# ── 12: ambiguous/unknown symbol normalization -> defer (fail closed) ───────
def test_unknown_symbol_fails_closed():
    recs, perf, obs = _run([_prov("D1", sym="ZZZ")], [_act_direct("D1", level=6076.0)],
                           [_tx(name="ZZZ Thing", close=6076.0)])
    assert recs[0]["dollar_pnl"] is None          # no token map for ZZZ -> refuse

# ── 13/14: SILVER + OIL shaped fixtures ─────────────────────────────────────
def test_silver_shape():
    recs, perf, obs = _run([_prov("DIAAAAR8VWPHYAR", sym="SILVER")],
                           [_act_affected("CLOSEX", "DIAAAAR8VWPHYAR", level=6076.31)],
                           [_tx(name="Silver", close=6076.31, pnl="$0.35")])
    assert recs[0]["settlement_state"] == "CONFIRMED"
    assert recs[0]["dollar_pnl"] == 0.35

def test_oil_shape_loss():
    recs, perf, obs = _run([_prov("DIAAAAR8WUU9ABB", sym="OIL", entry=9775.7)],
                           [_act_affected("CLOSEY", "DIAAAAR8WUU9ABB", epic="CC.D.LCO.BMU.IP", level=9781.0)],
                           [_tx(name="Oil - US Crude", close=9781.0, pnl="$-1.05")])
    assert recs[0]["settlement_state"] == "CONFIRMED"
    assert recs[0]["dollar_pnl"] == -1.05
    assert perf == [("OIL", False, -1.05)]

# ── 18/19: API error -> defer, no confirm, no storm ─────────────────────────
def test_activity_error_defers():
    recs, perf, obs = _run([_prov("D1")], [_act_direct("D1")], [_tx()], activity_err=True)
    assert recs[0]["settlement_state"] == "PROVISIONAL"
    assert perf == []

# ── 24/25: confirmed never changes / UNKNOWN never zero ─────────────────────
def test_already_confirmed_untouched():
    done = _prov("D1"); done.update(settlement_state="CONFIRMED", dollar_pnl=0.35,
                                    reconciled=True, perf_fed=True)
    recs, perf, obs = _run([done], [_act_direct("D1")], [_tx()])
    assert recs[0]["dollar_pnl"] == 0.35
    assert perf == []

def test_no_evidence_keeps_unknown_not_zero():
    recs, perf, obs = _run([_prov("D1")], [], [_tx()])
    assert recs[0]["dollar_pnl"] is None          # not 0

# ── 22/23: perf-replay for CONFIRMED-but-not-fed (crash window) ─────────────
def test_perf_replay_for_unfed_confirmed():
    unfed = _prov("D1"); unfed.update(settlement_state="CONFIRMED", dollar_pnl=0.35,
                                      reconciled=True, perf_fed=False)
    recs, perf, obs = _run([unfed], [], [_tx()])
    assert perf == [("SILVER", True, 0.35)]       # replayed once
    assert recs[0]["perf_fed"] is True

def test_perf_replay_not_double_fed():
    fed = _prov("D1"); fed.update(settlement_state="CONFIRMED", dollar_pnl=0.35,
                                  reconciled=True, perf_fed=True)
    recs, perf, obs = _run([fed], [], [_tx()])
    assert perf == []                             # already fed -> no replay

# ── 27: financing/deposit cannot match ──────────────────────────────────────
def test_non_deal_transaction_ignored():
    recs, perf, obs = _run([_prov("D1")], [_act_direct("D1", level=6076.0)],
                           [{"transactionType": "DEPOSIT", "instrumentName": "Silver",
                             "closeLevel": 6076.0, "profitAndLoss": "$50.00"}])
    assert recs[0]["dollar_pnl"] is None          # DEPOSIT skipped -> 0 DEAL candidates

# ── from/to regression guard (Match must pass from/to, not maxSpanSeconds) ──
def test_activity_uses_from_to_params():
    # _igget asserts params has from/to; if the code regressed to maxSpanSeconds,
    # the AssertionError would surface here.
    recs, perf, obs = _run([_prov("D1")], [_act_direct("D1")], [_tx()])
    assert recs[0]["settlement_state"] == "CONFIRMED"

# ── source-level invariants ─────────────────────────────────────────────────
def test_no_risk_constant_touched():
    from pathlib import Path
    s = Path("june.py").read_text(encoding="utf-8")
    assert "_LIVE_CIRCUIT_BREAKER_PCT = -0.05" in s
    assert "_mpd_mfe_gate" in s
    assert "_PYRAMID_PROFIT_GATE_PCT = 0.0015" in s
    assert "_ROLLING_MAX_GENERATIONS       = 2" in s
    assert '"from": _from, "to": _to' in s        # activity uses from/to
