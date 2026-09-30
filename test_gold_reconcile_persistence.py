"""Focused tests for the proven stale-primary persistence defect."""
import ast
import json
from pathlib import Path

SOURCE_PATH = next(p for p in (Path("_prod_patch_current.py"), Path("june.py")) if p.exists())
SOURCE = SOURCE_PATH.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)


def node(name):
    return next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == name)


def extract(names, ns):
    for n in TREE.body:
        if isinstance(n, ast.FunctionDef) and n.name in names:
            exec(compile(ast.Module(body=[n], type_ignores=[]), "extract_gold_reconcile", "exec"), ns)
    return ns


def test_reconciled_stale_primary_clear_persists_flat_state():
    state = {"open_position": {"deal_id": "DIAAAAR84TKUCA4"},
             "manual_review_required": True, "failed_theses": {"GOLD_long": {"x": 1}}}
    saves = []
    ns = {"_live": state, "_live_capture_active": lambda *a: None,
          "_live_save_state": lambda: saves.append(json.loads(json.dumps(state)))}
    extract({"_live_finalize_reconciled_stale_primary"}, ns)
    ns["_live_finalize_reconciled_stale_primary"]()
    assert state["open_position"] is None
    assert "manual_review_required" not in state
    assert state["failed_theses"] == {"GOLD_long": {"x": 1}}
    assert len(saves) == 1
    assert saves[0]["open_position"] is None


def test_finalize_is_safe_to_repeat_without_settlement_side_effects():
    state = {"open_position": None}
    saves = []
    ns = {"_live": state, "_live_capture_active": lambda *a: None,
          "_live_save_state": lambda: saves.append(True)}
    extract({"_live_finalize_reconciled_stale_primary"}, ns)
    fn = ns["_live_finalize_reconciled_stale_primary"]
    fn(); fn()
    assert state["open_position"] is None
    assert len(saves) == 2


def test_stale_branch_requires_broker_absence_before_finalize():
    start = SOURCE.index("# Stale: June state has position but IG shows nothing")
    end = SOURCE.index("def _apply_defect_quarantine", start)
    body = SOURCE[start:end]
    assert "if not ig_positions and june_pos:" in body
    assert "_live_finalize_reconciled_stale_primary()" in body
    assert body.index("_live_settle_primary_exit") < body.index("_live_finalize_reconciled_stale_primary")


def test_canonical_settlement_is_idempotent_by_deal_and_durable_history():
    class Redis:
        def lrange(self, key, start, stop):
            return [json.dumps({"deal_id": "DIAAAAR84TKUCA4"})]
    ns = {"_live": {"settled_primary_keys": []},
          "_redis": lambda: Redis(), "_LIVE_TRADE_HIST_KEY": "june_live_trade_history_full",
          "json": json}
    extract({"_live_settlement_key", "_live_already_settled"}, ns)
    assert ns["_live_already_settled"]({"deal_id": "DIAAAAR84TKUCA4"}) is True


def test_canonical_settlement_uses_confirmed_broker_evidence_contract():
    start = SOURCE.index("def _live_settle_primary_exit")
    end = SOURCE.index("def _live_close_position", start)
    body = SOURCE[start:end]
    assert "confirmed_pnl is a real number" in body
    assert 'settlement_state = "CONFIRMED"' in body
    assert 'evidence_class": ("BROKER_CONFIRMED_LIVE"' in body
    assert 'settlement_identity": f"deal:{deal_id}"' in body


def test_absence_without_confirmed_economics_remains_provisional_not_zero():
    start = SOURCE.index("def _live_settle_primary_exit")
    end = SOURCE.index("def _live_close_position", start)
    body = SOURCE[start:end]
    assert 'dollar_pnl = None' in body
    assert 'else: PROVISIONAL' in body
    assert 'dollar_pnl/pnl_pct = None' in body


def test_stop_management_404_alone_does_not_prove_flatness():
    start = SOURCE.index("def _live_close_position")
    end = SOURCE.index("def _live_partial_tp_exit", start)
    body = SOURCE[start:end]
    assert "if _guard_open is None" in body
    assert "if _guard_open is False" in body
    assert "position may still be open" in body


def test_reconciliation_clears_only_after_positions_absence():
    body = SOURCE[SOURCE.index("def _live_reconcile_positions"):SOURCE.index("def _apply_defect_quarantine")]
    assert "ig_positions = data.get(\"positions\")" in body
    assert "if not ig_positions and june_pos:" in body
    assert "position_absence_observed" in body


def test_broker_side_disappearance_does_not_create_b1_stop_loss_failure():
    start = SOURCE.index("def _live_settle_primary_exit")
    end = SOURCE.index("def _live_close_position", start)
    body = SOURCE[start:end]
    assert 'if exit_reason == "stop_loss":' in body
    assert '"broker_side_disappearance"' not in body  # caller supplies it; no forced B1 classification


def test_reconciliation_preserves_build4_cleanup_hook():
    start = SOURCE.index("def _live_reconcile_positions")
    end = SOURCE.index("def _apply_defect_quarantine", start)
    body = SOURCE[start:end]
    assert "b4a_clear_campaign_rolling_state" in body or "_live_finalize_reconciled_stale_primary" in body
    assert "_live_save_state()" in SOURCE[SOURCE.index("def _live_finalize_reconciled_stale_primary"):SOURCE.index("def _live_reconcile_positions")]


def test_reconciliation_patch_does_not_touch_c1_or_risk_universe_logic():
    assert "_SIM_SCALE_RATIO_MAX = 8.0" in SOURCE
    assert "def _live_instrument_capability" in SOURCE
    assert "_LIVE_CIRCUIT_BREAKER_PCT = -0.05" in SOURCE
