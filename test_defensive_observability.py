import json
import sqlite3
from contextlib import closing
from unittest.mock import Mock, patch
import ast
import pytest
from campaign_telemetry import Store
from test_broker_identity import execute, function, TREE
from test_dynamic_defensive import step_harness
from test_defensive_scaling import defensive_harness, evaluate, SIGNALS, evidence, position


def test_account_transitions_flat_and_held_share_bounded_store(tmp_path):
    store = Store(tmp_path / "telemetry.sqlite3", event_cap=2)
    for i in range(4):
        state = {} if i % 2 == 0 else {"open_position": position()}
        store.observe(state, {}, account="fixture", now=1000+i, unit=lambda s, p: p,
                      event="global_mode_transition", details=dict(prior="normal", new="defensive",
                      account_loss=10, reason="account_loss_reached_activation", observed_at=1000+i))
    with closing(sqlite3.connect(store.path)) as db:
        rows = db.execute("SELECT campaign,kind,payload FROM events").fetchall()
        assert len(rows) == 2
        assert all(r[0] is None and r[1] == "global_mode_transition" for r in rows)
        assert json.loads(rows[-1][2])["account_loss"] == 10
        assert db.execute("SELECT count(*) FROM campaigns").fetchone()[0] == 0


def test_each_eligible_decision_retained_with_economics(tmp_path):
    ns = defensive_harness()
    ns["_live_perf_blocked"].return_value = True
    store = Store(tmp_path / "telemetry.sqlite3", event_cap=20)
    for i in range(3):
        decision = evaluate(ns)
        decision["attempt"] += i
        store.observe(ns["_live"], SIGNALS, account="fixture", now=1000+i,
                      unit=lambda s, p: p, event="pyramid_decision", details=decision)
    with closing(sqlite3.connect(store.path)) as db:
        rows = db.execute("SELECT payload FROM events WHERE kind='pyramid_decision'").fetchall()
    assert len(rows) == 3
    for row in rows:
        d = json.loads(row[0])["event_details"]
        assert d["global_mode"] == "defensive" and d["macro"] == "neutral"
        assert d["reason"] == "independent_session_or_performance_block"
        assert d["protection_state"] == "profit_protected"
        assert d["liquidation_before"] > 0 and d["remaining_capacity"] > 0
        assert d["protection"][0]["acknowledged"] == 101.


@pytest.mark.parametrize("error", [OSError("disk full"), sqlite3.OperationalError("database is locked")])
def test_real_telemetry_failure_cannot_suppress_exit(error):
    ns = step_harness()
    ns.update(_live_sess={"account_id": "fixture"}, _live_campaign_unit=lambda *a: 1.,
              _PYRAMID_PROFIT_GATE_PCT=.0015)
    execute([function(n) for n in ("_live_observe", "run_live_step")], ns)
    with patch("campaign_telemetry.default_store", side_effect=error):
        ns["run_live_step"]({})
    ns["_live_check_exit"].assert_called_once()
    ns["_live_retry_stop_sync"].assert_called_once()
    assert ns["_live"]["global_mode"] == "defensive"


def test_real_state_roundtrip_preserves_defensive_reference_and_pending_risk():
    ns = defensive_harness()
    ns["_live"].update(global_mode_reference=162.27, global_mode_entered_at=1.)
    ns["_ig_live_post"].return_value = None
    evaluate(ns)
    saved = json.loads(ns["_redis"]().set.call_args.args[1])
    ns["_redis"]().get.return_value = json.dumps(saved)
    ns["_live"] = {}
    ns["_live_capture_active"] = Mock()
    execute([function("_live_load_state")], ns)
    assert ns["_live_load_state"]()
    assert ns["_live"]["global_mode_reference"] == 162.27
    rd = ns["_live"]["pyramid_entry_pending"]["risk_decision"]
    assert rd["f50_protected_before"] > 0  # B2: liquidation_after computed post-confirm only
    evaluate(ns)
    ns["_ig_live_post"].assert_called_once()


def test_partial_loss_is_debited_and_profit_is_not_spent():
    baseline = evidence()["liquidation_before"]
    for realized in (-1., 1.):
        p = position()
        p.update(partial_exit_done=True, partial_dollar_pnl=realized)
        assert evidence(p)["liquidation_before"] == pytest.approx(baseline + min(0, realized))


def test_normal_claimed_protected_floor_obeys_same_economics():
    ns = defensive_harness(mode="normal")
    assert evaluate(ns)["reason"] == "f50_protected_profit_finances_addon"
    ns["_ig_live_post"].assert_called_once()


def test_mindeal_blocked_does_not_corrupt_position_state():
    # Build 2: MINDEAL-blocked F50 rejection leaves position unchanged (no prefinancing side effects).
    ns = defensive_harness()
    p_stop_before = ns["_live"]["open_position"]["broker_stop_level"]
    ns["_live_min_deal"]["GOLD"] = 200.  # forces MINDEAL-blocked rejection
    d = evaluate(ns)
    assert d["decision"] == "reject"
    assert "f50_mindeal_blocked" in d["reason"]
    # No broker calls made: neither stop-tightening PUT nor addon POST.
    ns["_ig_live_put"].assert_not_called()
    ns["_ig_live_post"].assert_not_called()
    # Position is unmodified — floor still held by original acknowledged stop.
    assert ns["_live"]["open_position"]["broker_stop_level"] == p_stop_before


def test_crossed_quotes_do_not_qualify_as_execution_evidence():
    ns = defensive_harness()
    ns["_live_check_pyramid_entry"]({"GOLD": dict(SIGNALS["GOLD"], bid=106., offer=104.)}, "neutral")
    ns["_ig_live_post"].assert_not_called()
    assert ns["_live_observe"].call_args.args[3]["reason"] == "crossed_execution_quotes"


@pytest.mark.parametrize("day,equity,halt", [(100., 89., False), (100., 88., True),
                                          (20., 15., False), (20., 14., True),
                                          (1., .6, True), (162.27, 151.55, False),
                                          (162.27, 140., True)])
def test_actual_circuit_breaker_remains_independent(day, equity, halt):
    ns = dict(_live={"balance_day_start": day, "balance_total": equity, "global_mode": "defensive"},
              _june_live_trading_enabled=True, _redis=Mock(return_value=Mock()), _live_log=Mock())
    for node in TREE.body:
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name.startswith("_LIVE_CB_") or name == "_LIVE_CIRCUIT_BREAKER_PCT":
                ns[name] = ast.literal_eval(node.value)
    execute([function("_live_check_circuit_breaker")], ns)
    ns["_live_check_circuit_breaker"]()
    assert ns["_june_live_trading_enabled"] is not halt
    if halt:
        ns["_redis"]().set.assert_any_call("june_live_enabled", "false")
