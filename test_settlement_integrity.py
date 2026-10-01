"""Settlement-integrity characterization + regression tests (repair/settlement-
integrity-4e22d32).

Covers the four real PROVISIONAL campaign shapes from the 4e22d32 forensic report
plus the full enumerated matrix: partial+final aggregation, late/unordered
evidence, missing-activity transaction-only certification, duplicate delivery,
duplicate reconciliation, restart boundaries, PROVISIONAL->CONFIRMED upgrade,
and exactly-once trade_history / performance delivery.

Two layers:
  * Pure-logic unit tests against settlement_reconcile (no I/O).
  * End-to-end reconciler tests that extract _live_reconcile_provisional_
    settlements from june.py (via the shared AST harness) and drive it with a
    fake Redis durable list + a fake _ig_live_get, asserting durable promotion,
    exactly-once perf feed, and the in-memory snapshot refresh.
"""
import json
from types import SimpleNamespace
from unittest.mock import Mock

import settlement_reconcile as sr
from test_broker_identity import execute, function


# ─────────────────────────── instrument matcher ─────────────────────────────
_TOKENS = {
    "SILVER": ("SILVER",), "GOLD": ("GOLD",), "OIL": ("OIL", "CRUDE"),
    "NATGAS": ("NATURAL GAS", "NAT GAS"), "WHEAT": ("WHEAT",),
    "SUGAR": ("SUGAR",), "COCOA": ("COCOA",), "HO": ("HEATING OIL",),
}
def instr_matcher(sym, name):
    name = str(name or "").upper()
    toks = _TOKENS.get(str(sym).upper())
    return bool(name) and bool(toks) and any(t in name for t in toks)


# ─────────────────────────── fixture builders ───────────────────────────────
def rec(deal="D1", sym="GOLD", dirn="short", entry=4152.13, qty=0.06,
        partial_pnl=0.0, exit_epoch=900_000):
    return {"instrument": sym, "direction": dirn, "deal_id": deal,
            "entry_price": entry, "exit_price": None, "ig_size": qty,
            "dollar_pnl": None, "partial_dollar_pnl": partial_pnl,
            "settlement_state": "PROVISIONAL",
            "settlement_source": "close_guard_absent:REST-deal",
            "exit_reason": "dple_trail", "exit_epoch": exit_epoch,
            "settlement_identity": f"deal:{deal}"}

def act(deal, level, *, affected=None, partial=False, epic="CS.D.CFDGOLD.BMU.IP"):
    """One IG activity. If `affected` set, it is a broker-managed close whose
    dealId differs and references our deal via affectedDealId."""
    close_deal = affected if affected else deal
    target = deal
    return {"dealId": close_deal, "epic": epic, "date": "2026-09-30T05:21:22",
            "details": {"level": level, "size": 0.03, "direction": "SELL",
                        "actions": [{"actionType": "POSITION_PARTIALLY_CLOSED" if partial
                                     else "POSITION_CLOSED", "affectedDealId": target}]}}

def tx(ref, name, open_lvl, close_lvl, size, pnl):
    return {"transactionType": "DEAL", "reference": ref, "instrumentName": name,
            "openLevel": open_lvl, "closeLevel": close_lvl, "size": size,
            "profitAndLoss": pnl, "date": "30-Sep-26"}


# ═══════════════════════════ 1. PURE-LOGIC LAYER ════════════════════════════
# ── The four real failure shapes ────────────────────────────────────────────
def test_campaign_8_gold_short_partial_plus_final():
    v = sr.reconcile_settlement(
        rec("DIAAAAR84SLLNAZ", "GOLD", "short", 4176.06, 0.06, partial_pnl=0.15),
        [act("DIAAAAR84SLLNAZ", 4171.06, affected="P84", partial=True),
         act("DIAAAAR84SLLNAZ", 4170.98, affected="F84", partial=False)],
        [tx("84S874AU", "Spot Gold", 4176.06, 4171.06, "-0.03", "$0.15"),
         tx("84SVFWAH", "Spot Gold", 4176.06, 4170.98, "-0.03", "$0.15")],
        instr_matcher)
    assert v["verdict"] == sr.CONFIRM
    assert v["dollar_pnl"] == 0.30
    assert v["evidence_path"] == sr.ACTIVITY_ANCHORED
    assert v["n_transactions"] == 2

def test_campaign_15_gold_long_partial_plus_final():
    v = sr.reconcile_settlement(
        rec("DIAAAAR87C47HAC", "GOLD", "long", 4210.62, 0.12, partial_pnl=0.3096),
        [act("DIAAAAR87C47HAC", 4215.78, affected="P87", partial=True),
         act("DIAAAAR87C47HAC", 4213.35, affected="F87", partial=False)],
        [tx("87D9NBAF", "Spot Gold", 4210.62, 4215.78, "+0.06", "$0.31"),
         tx("87C2F2AH", "Spot Gold", 4210.62, 4213.35, "+0.06", "$0.16")],
        instr_matcher)
    assert v["verdict"] == sr.CONFIRM
    assert v["dollar_pnl"] == 0.47
    assert v["n_transactions"] == 2

def test_campaign_18_oil_long_partial_plus_final():
    v = sr.reconcile_settlement(
        rec("DIAAAAR88WC78AC", "OIL", "long", 9863.9, 0.04, partial_pnl=0.364),
        [act("DIAAAAR88WC78AC", 9882.1, affected="P88", partial=True, epic="CC.D.LCO.BMU.IP"),
         act("DIAAAAR88WC78AC", 9877.4, affected="F88", partial=False, epic="CC.D.LCO.BMU.IP")],
        [tx("88ZJZTAG", "Oil - US Crude", 9863.9, 9882.1, "+0.02", "$0.36"),
         tx("88VTY9AH", "Oil - US Crude", 9863.9, 9877.4, "+0.02", "$0.27")],
        instr_matcher)
    assert v["verdict"] == sr.CONFIRM
    assert v["dollar_pnl"] == 0.63
    assert v["n_transactions"] == 2

def test_campaign_21_gold_short_transaction_only():
    # No close activity returned at all; booked transactions reconcile to opening.
    v = sr.reconcile_settlement(
        rec("DIAAAAR9A68Y8AE", "GOLD", "short", 4152.13, 0.06, partial_pnl=0.1656),
        [],
        [tx("9A9N6DAY", "Spot Gold", 4152.13, 4146.61, "-0.03", "$0.17"),
         tx("9A76D5AH", "Spot Gold", 4152.13, 4147.01, "-0.03", "$0.15")],
        instr_matcher)
    assert v["verdict"] == sr.CONFIRM
    assert v["dollar_pnl"] == 0.32
    assert v["evidence_path"] == sr.TRANSACTION_ONLY
    assert v["n_transactions"] == 2


# ── ordinary / managed single-exit shapes ───────────────────────────────────
def test_ordinary_full_broker_close():
    v = sr.reconcile_settlement(
        rec("D1", "SILVER", "long", 6073.3, 0.09),
        [act("D1", 6077.2, affected=None, epic="CS.D.CFDSILVER.BMU.IP")],
        [tx("R1", "Silver", 6073.3, 6077.2, "+0.09", "$0.35")], instr_matcher)
    assert v["verdict"] == sr.CONFIRM and v["dollar_pnl"] == 0.35
    assert v["match"] == "A_direct"

def test_broker_stop_managed_close_affected():
    v = sr.reconcile_settlement(
        rec("OPEN1", "OIL", "long", 9775.7, 0.03),
        [act("OPEN1", 9812.9, affected="STOP9", epic="CC.D.LCO.BMU.IP")],
        [tx("R1", "Oil - US Crude", 9775.7, 9812.9, "-0.03", "$-1.12")], instr_matcher)
    assert v["verdict"] == sr.CONFIRM and v["dollar_pnl"] == -1.12
    assert v["match"] == "B_affected"

def test_dple_managed_close_single():
    v = sr.reconcile_settlement(
        rec("D1", "HO", "short", 43872.0, 0.02),
        [act("D1", 44061.0, affected="DPLE1", epic="CC.D.HO.BMU.IP")],
        [tx("R1", "Heating Oil", 43872.0, 44061.0, "-0.02", "$-3.78")], instr_matcher)
    assert v["verdict"] == sr.CONFIRM and v["dollar_pnl"] == -3.78

def test_reversal_close_single():
    v = sr.reconcile_settlement(
        rec("D1", "COCOA", "long", 4252.0, 0.13),
        [act("D1", 4233.5, affected="REV1", epic="CC.D.LCC.BMU.IP")],
        [tx("R1", "Cocoa", 4252.0, 4233.5, "+0.13", "$-2.41")], instr_matcher)
    assert v["verdict"] == sr.CONFIRM and v["dollar_pnl"] == -2.41


# ── partial + final permutations ─────────────────────────────────────────────
def test_partial_then_final_confirms_once():
    v = sr.reconcile_settlement(
        rec("D1", "GOLD", "short", 4176.06, 0.06, partial_pnl=0.15),
        [act("D1", 4170.98, affected="F", partial=False)],
        [tx("P", "Spot Gold", 4176.06, 4171.06, "-0.03", "$0.15"),
         tx("F", "Spot Gold", 4176.06, 4170.98, "-0.03", "$0.15")], instr_matcher)
    assert v["verdict"] == sr.CONFIRM and v["dollar_pnl"] == 0.30

def test_multiple_partials_plus_final():
    # three fills 0.02+0.02+0.02 = 0.06, final close level 4170.0
    v = sr.reconcile_settlement(
        rec("D1", "GOLD", "short", 4176.06, 0.06, partial_pnl=0.2),
        [act("D1", 4170.0, affected="F", partial=False)],
        [tx("P1", "Spot Gold", 4176.06, 4172.0, "-0.02", "$0.10"),
         tx("P2", "Spot Gold", 4176.06, 4171.0, "-0.02", "$0.12"),
         tx("F",  "Spot Gold", 4176.06, 4170.0, "-0.02", "$0.14")], instr_matcher)
    assert v["verdict"] == sr.CONFIRM
    assert abs(v["dollar_pnl"] - 0.36) < 1e-9
    assert v["n_transactions"] == 3

def test_partial_quantity_plus_residual_equals_campaign():
    # residual (final) + partial sizes must equal opening quantity
    v = sr.reconcile_settlement(
        rec("D1", "GOLD", "long", 4210.62, 0.12, partial_pnl=0.31),
        [act("D1", 4213.35, affected="F", partial=False)],
        [tx("P", "Spot Gold", 4210.62, 4215.78, "+0.06", "$0.31"),
         tx("F", "Spot Gold", 4210.62, 4213.35, "+0.06", "$0.16")], instr_matcher)
    assert v["verdict"] == sr.CONFIRM
    assert v["aggregated_abs_size"] == 0.12


# ── unordered evidence convergence ───────────────────────────────────────────
def test_evidence_order_independent():
    acts = [act("D1", 4170.98, affected="F", partial=False),
            act("D1", 4171.06, affected="P", partial=True)]
    txs_ab = [tx("P", "Spot Gold", 4176.06, 4171.06, "-0.03", "$0.15"),
              tx("F", "Spot Gold", 4176.06, 4170.98, "-0.03", "$0.15")]
    txs_ba = list(reversed(txs_ab))
    v1 = sr.reconcile_settlement(rec("D1", "GOLD", "short", 4176.06, 0.06, 0.15), acts, txs_ab, instr_matcher)
    v2 = sr.reconcile_settlement(rec("D1", "GOLD", "short", 4176.06, 0.06, 0.15), list(reversed(acts)), txs_ba, instr_matcher)
    assert v1["verdict"] == v2["verdict"] == sr.CONFIRM
    assert v1["dollar_pnl"] == v2["dollar_pnl"] == 0.30


# ── duplicate delivery / idempotency ─────────────────────────────────────────
def test_duplicate_transaction_delivery_counts_once():
    dup = [tx("P", "Spot Gold", 4152.13, 4146.61, "-0.03", "$0.17"),
           tx("F", "Spot Gold", 4152.13, 4147.01, "-0.03", "$0.15"),
           tx("P", "Spot Gold", 4152.13, 4146.61, "-0.03", "$0.17"),   # dup page
           tx("F", "Spot Gold", 4152.13, 4147.01, "-0.03", "$0.15")]
    v = sr.reconcile_settlement(rec("D1", "GOLD", "short", 4152.13, 0.06, 0.1656), [], dup, instr_matcher)
    assert v["verdict"] == sr.CONFIRM
    assert v["dollar_pnl"] == 0.32            # not 0.64
    assert v["n_transactions"] == 2


# ── fail-closed: ambiguity / incomplete / mismatch stays provisional ─────────
def test_quantity_mismatch_defers():
    v = sr.reconcile_settlement(
        rec("D1", "GOLD", "short", 4152.13, 0.06, 0.1656), [],
        [tx("P", "Spot Gold", 4152.13, 4146.61, "-0.03", "$0.17")], instr_matcher)
    assert v["verdict"] == sr.DEFER
    assert v["reason"] in ("quantity_coverage_incomplete", "insufficient_tx_for_tx_only")

def test_mismatched_deal_identity_instrument_defers():
    v = sr.reconcile_settlement(
        rec("D1", "GOLD", "short", 4152.13, 0.06),
        [act("D1", 4146.61, affected="F")],
        [tx("F", "Spot Silver", 4152.13, 4146.61, "-0.06", "$0.30")], instr_matcher)
    assert v["verdict"] == sr.DEFER        # instrument name is SILVER, record GOLD

def test_no_transaction_at_activity_close_level_defers():
    # Activity says closed at 4146.61 but no booked DEAL row reports that close
    # level -> activity/transactions disagree -> defer (never fabricate).
    v = sr.reconcile_settlement(
        rec("D1", "GOLD", "short", 4152.13, 0.06),
        [act("D1", 4146.61, affected="F")],
        [tx("F", "Spot Gold", 4152.13, 9999.0, "-0.06", "$0.30")], instr_matcher)
    assert v["verdict"] == sr.DEFER
    assert v["reason"] == "no_transaction_at_close_level"

def test_partial_only_activity_never_finalizes():
    v = sr.reconcile_settlement(
        rec("D1", "GOLD", "short", 4152.13, 0.06, partial_pnl=0.1656),
        [act("D1", 4146.61, affected="P", partial=True)],
        [tx("P", "Spot Gold", 4152.13, 4146.61, "-0.06", "$0.30")], instr_matcher)
    assert v["verdict"] == sr.DEFER
    assert v["reason"] == "partial_close_not_final"

def test_broker_disappearance_without_economics_defers():
    v = sr.reconcile_settlement(rec("D1", "GOLD", "short", 4152.13, 0.06), [], [], instr_matcher)
    assert v["verdict"] == sr.DEFER
    assert v["reason"] == "no_close_activity_yet"

def test_tx_only_without_observed_partial_defers():
    # Two covering txns but NO observed-partial marker on the record -> stays
    # provisional (single-exit campaigns must not confirm from stray txns).
    v = sr.reconcile_settlement(
        rec("D1", "GOLD", "short", 4152.13, 0.06, partial_pnl=0.0), [],
        [tx("P", "Spot Gold", 4152.13, 4146.61, "-0.03", "$0.17"),
         tx("F", "Spot Gold", 4152.13, 4147.01, "-0.03", "$0.15")], instr_matcher)
    assert v["verdict"] == sr.DEFER

def test_two_distinct_tx_at_close_level_ambiguous():
    v = sr.reconcile_settlement(
        rec("D1", "GOLD", "short", 4152.13, 0.06),
        [act("D1", 4146.61, affected="F")],
        [tx("A", "Spot Gold", 4152.13, 4146.61, "-0.06", "$0.30"),
         tx("B", "Spot Gold", 4152.13, 4146.61, "-0.06", "$0.40")], instr_matcher)
    assert v["verdict"] == sr.AMBIGUOUS

def test_multiple_affected_closes_ambiguous():
    v = sr.reconcile_settlement(
        rec("D1", "GOLD", "short", 4152.13, 0.06),
        [act("D1", 4146.0, affected="C1"), act("D1", 4147.0, affected="C2")],
        [], instr_matcher)
    assert v["verdict"] == sr.AMBIGUOUS

def test_cent_rounding_preserved():
    v = sr.reconcile_settlement(
        rec("D1", "GOLD", "long", 4210.62, 0.12, partial_pnl=0.31),
        [act("D1", 4213.35, affected="F")],
        [tx("P", "Spot Gold", 4210.62, 4215.78, "+0.06", "$0.31"),
         tx("F", "Spot Gold", 4210.62, 4213.35, "+0.06", "$0.16")], instr_matcher)
    assert v["dollar_pnl"] == 0.47           # booked cents, exact


# ═══════════════════════ 2. END-TO-END RECONCILER LAYER ═════════════════════
class FakeList:
    def __init__(self, items): self.items = [json.dumps(x) for x in items]
    def lrange(self, k, a, b): return list(self.items) if b == -1 else self.items[a:b+1]
    def lindex(self, k, i): return self.items[i] if 0 <= i < len(self.items) else None
    def lset(self, k, i, v): self.items[i] = v
    def parsed(self): return [json.loads(x) for x in self.items]


def _drive(records, activities, transactions, *, now=1_000_000.0, live=None,
           perf=None, saves=None):
    """Extract and run the real _live_reconcile_provisional_settlements +
    _live_refresh_trade_history_snapshot against fakes. Returns (durable, perf)."""
    fake = FakeList(records)
    perf = perf if perf is not None else []
    saves = saves if saves is not None else []
    _live = live if live is not None else {}

    def _igget(path, params=None, version="1", not_found_default=None):
        if path == "/history/transactions":
            return {"transactions": transactions}
        if path == "/history/activity":
            assert params and "from" in params and "to" in params
            return {"activities": activities}
        return None

    def _perf(sym, won, sar, pnl_dollar=0.0, **k):
        # Mirror the real observation_id dedup so exactly-once is testable.
        oid = k.get("settlement_identity") or (f"deal:{k.get('deal_id')}" if k.get("deal_id") else None)
        if any(p[-1] == oid for p in perf):
            return
        perf.append((sym, won, pnl_dollar, oid))

    ns = dict(
        json=json, time=SimpleNamespace(time=lambda: now),
        _redis=Mock(return_value=fake), _LIVE_TRADE_HIST_KEY="hist",
        _live=_live,
        _RECON_INSTR_TOKENS=_TOKENS,
        _RECON_MAX_PER_CYCLE=5, _RECON_MIN_AGE_SECS=120,
        _RECON_BASE_BACKOFF_SECS=300, _RECON_MAX_BACKOFF_SECS=6*3600,
        _RECON_GIVEUP_SECS=14*24*3600, _RECON_SCHEMA_VERSION=1,
        _ig_live_get=_igget, _live_perf_record=_perf,
        _live_observe=lambda ev, *a, **k: None,
        _live_log=Mock(),
        _live_save_state=lambda: saves.append(True),
    )
    execute([function("_recon_instr_matches"), function("_recon_iso"),
             function("_live_reconcile_provisional_settlements"),
             function("_live_refresh_trade_history_snapshot")], ns)
    ns["_live_reconcile_provisional_settlements"]()
    return fake.parsed(), perf, _live, saves


def test_e2e_partial_campaign_promotes_and_feeds_once():
    r = rec("DIAAAAR84SLLNAZ", "GOLD", "short", 4176.06, 0.06, partial_pnl=0.15)
    durable, perf, _live, _ = _drive(
        [r],
        [act("DIAAAAR84SLLNAZ", 4170.98, affected="F84", partial=False),
         act("DIAAAAR84SLLNAZ", 4171.06, affected="P84", partial=True)],
        [tx("84S874AU", "Spot Gold", 4176.06, 4171.06, "-0.03", "$0.15"),
         tx("84SVFWAH", "Spot Gold", 4176.06, 4170.98, "-0.03", "$0.15")])
    assert durable[0]["settlement_state"] == "CONFIRMED"
    assert durable[0]["dollar_pnl"] == 0.30
    assert durable[0]["perf_fed"] is True
    assert durable[0]["recon_tx_count"] == 2
    assert durable[0]["evidence_class"] == "BROKER_CONFIRMED_LIVE"
    assert len(perf) == 1 and perf[0][:3] == ("GOLD", True, 0.30)


def test_e2e_transaction_only_campaign_21():
    r = rec("DIAAAAR9A68Y8AE", "GOLD", "short", 4152.13, 0.06, partial_pnl=0.1656)
    durable, perf, _, _ = _drive(
        [r], [],
        [tx("9A9N6DAY", "Spot Gold", 4152.13, 4146.61, "-0.03", "$0.17"),
         tx("9A76D5AH", "Spot Gold", 4152.13, 4147.01, "-0.03", "$0.15")])
    assert durable[0]["settlement_state"] == "CONFIRMED"
    assert durable[0]["dollar_pnl"] == 0.32
    assert durable[0]["recon_evidence_path"] == "transaction_only"
    assert len(perf) == 1


def test_e2e_duplicate_reconciliation_call_no_double_feed():
    r = rec("D1", "GOLD", "short", 4176.06, 0.06, partial_pnl=0.15)
    acts = [act("D1", 4170.98, affected="F", partial=False)]
    txs = [tx("P", "Spot Gold", 4176.06, 4171.06, "-0.03", "$0.15"),
           tx("F", "Spot Gold", 4176.06, 4170.98, "-0.03", "$0.15")]
    # First pass promotes and feeds.
    durable, perf, _live, _ = _drive([r], acts, txs)
    assert durable[0]["settlement_state"] == "CONFIRMED"
    assert len(perf) == 1
    # Second pass over the SAME durable row: already CONFIRMED -> no re-feed.
    durable2, perf2, _, _ = _drive(durable, acts, txs, perf=perf)
    assert durable2[0]["dollar_pnl"] == 0.30
    assert len(perf2) == 1                     # still once (idempotent)


def test_e2e_restart_between_partial_and_final_stays_provisional_until_final():
    # Only the partial activity is visible (final not yet returned); a restart in
    # between must not finalize. partial_only -> defer.
    r = rec("D1", "GOLD", "short", 4176.06, 0.06, partial_pnl=0.15)
    durable, perf, _, _ = _drive(
        [r], [act("D1", 4171.06, affected="P", partial=True)],
        [tx("P", "Spot Gold", 4176.06, 4171.06, "-0.03", "$0.15")])
    assert durable[0]["settlement_state"] == "PROVISIONAL"
    assert durable[0]["dollar_pnl"] is None
    assert perf == []


def test_e2e_late_evidence_upgrades_provisional_to_confirmed():
    r = rec("D1", "GOLD", "short", 4176.06, 0.06, partial_pnl=0.15)
    # Pass 1: no transactions yet -> stays provisional.
    durable, perf, _live, _ = _drive([r], [], [])
    assert durable[0]["settlement_state"] == "PROVISIONAL"
    # Pass 2: transactions + final activity arrive late -> upgrades.
    durable[0].setdefault("recon_last_attempt", 0)
    durable[0]["recon_last_attempt"] = 0       # clear backoff
    durable2, perf2, _, _ = _drive(
        durable,
        [act("D1", 4170.98, affected="F", partial=False)],
        [tx("P", "Spot Gold", 4176.06, 4171.06, "-0.03", "$0.15"),
         tx("F", "Spot Gold", 4176.06, 4170.98, "-0.03", "$0.15")],
        perf=perf)
    assert durable2[0]["settlement_state"] == "CONFIRMED"
    assert durable2[0]["dollar_pnl"] == 0.30
    assert len(perf2) == 1


def test_e2e_unresolved_evidence_stays_provisional():
    r = rec("D1", "GOLD", "short", 4152.13, 0.06, partial_pnl=0.1656)
    durable, perf, _, _ = _drive(
        [r], [],
        [tx("P", "Spot Gold", 4152.13, 4146.61, "-0.03", "$0.17")])  # half coverage
    assert durable[0]["settlement_state"] == "PROVISIONAL"
    assert perf == []


def test_e2e_snapshot_refresh_syncs_without_refeeding():
    # Durable already CONFIRMED+perf_fed; in-memory snapshot still stale PROVISIONAL.
    confirmed = rec("D1", "GOLD", "short", 4176.06, 0.06)
    confirmed.update(settlement_state="CONFIRMED", dollar_pnl=0.30, reconciled=True,
                     perf_fed=True, evidence_class="BROKER_CONFIRMED_LIVE",
                     settlement_identity="deal:D1")
    stale_snapshot = rec("D1", "GOLD", "short", 4176.06, 0.06)   # PROVISIONAL copy
    _live = {"trade_history": [stale_snapshot]}
    durable, perf, live_out, saves = _drive([confirmed], [], [], live=_live)
    row = live_out["trade_history"][0]
    assert row["settlement_state"] == "CONFIRMED"
    assert row["dollar_pnl"] == 0.30
    assert row["snapshot_refreshed_from_durable"] is True
    assert perf == []                          # snapshot refresh NEVER feeds perf
    assert saves                               # state saved after change


def test_e2e_campaign_pnl_equals_authoritative_broker_sum():
    cases = [
        ("DIAAAAR84SLLNAZ", "GOLD", 4176.06, 0.06, 0.15,
         [("P", 4171.06, "-0.03", "$0.15"), ("F", 4170.98, "-0.03", "$0.15")], 0.30),
        ("DIAAAAR87C47HAC", "GOLD", 4210.62, 0.12, 0.31,
         [("P", 4215.78, "+0.06", "$0.31"), ("F", 4213.35, "+0.06", "$0.16")], 0.47),
        ("DIAAAAR88WC78AC", "OIL", 9863.9, 0.04, 0.36,
         [("P", 9882.1, "+0.02", "$0.36"), ("F", 9877.4, "+0.02", "$0.27")], 0.63),
    ]
    for deal, sym, entry, qty, ppnl, fills, expect in cases:
        epic = "CC.D.LCO.BMU.IP" if sym == "OIL" else "CS.D.CFDGOLD.BMU.IP"
        name = "Oil - US Crude" if sym == "OIL" else "Spot Gold"
        final_lvl = fills[-1][1]
        acts = [act(deal, final_lvl, affected="F", partial=False, epic=epic)]
        txs = [tx(r, name, entry, lvl, sz, p) for (r, lvl, sz, p) in fills]
        durable, perf, _, _ = _drive([rec(deal, sym, "long", entry, qty, ppnl)], acts, txs)
        assert durable[0]["dollar_pnl"] == expect, (deal, durable[0]["dollar_pnl"])
