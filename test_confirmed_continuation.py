"""Offline regression cases for confirmed allocation and protected costs."""
import ast
import json
from copy import deepcopy
from pathlib import Path
import subprocess
from functools import lru_cache
from unittest.mock import Mock

import pytest

from continuation_economics import spread_economics, verify_inventory
from defensive_scaling import f50_capacity, snapshot
from winner_accounting import campaign_allocation, confirm_allocation_reduction
from test_defensive_scaling import defensive_harness, evaluate, SIGNALS
from test_live_accounting import close_harness


def oil():
    return dict(deal_id="oil", instrument="OIL", direction="long", ig_size=.04,
                fill_price=9863.9, leverage=4, pos_size=106.14)


def certify(p, size=.02):
    row = dict(position=dict(dealId=p["deal_id"], direction="BUY", size=size))
    p["allocation_confirmation"] = confirm_allocation_reduction(
        p, row, dict(dealStatus="ACCEPTED", dealReference="close"), "close", p["ig_size"])
    p["original_ig_size"] = p["ig_size"]
    p["ig_size"] = size
    return p


def allocation(p, **kw):
    return campaign_allocation(p, [], lambda price: price, 106.712, **kw)


def test_oil_confirmed_release_exact():
    p = certify(oil())
    a = allocation(p)
    assert a["actual_notional"] == pytest.approx(197.278)
    assert a["consumed_allocation"] == pytest.approx(53.07)
    assert a["remaining_allocation"] == pytest.approx(53.642)
    assert a["reason"] == "allocation_released_confirmed_partial"
    assert p["pos_size"] == 106.14


@pytest.mark.parametrize("status", ["request_pending", "unknown", "ACCEPTED", "REJECTED", "ambiguous"])
def test_unresolved_partial_cannot_release_even_if_local_quantity_reduced(status):
    p = certify(oil())
    p["partial_exit_pending"] = dict(original_size=.04, status=status)
    assert allocation(p)["consumed_allocation"] == pytest.approx(106.14)
    assert allocation(p)["actual_notional"] == pytest.approx(394.556)
    assert allocation(p)["reason"] == "allocation_retained_unresolved_partial"


@pytest.mark.parametrize("extra", [{}, {"partial_exit_done": True}, {"original_ig_size": .04},
                                 {"partial_dollar_pnl": 100., "partial_exit_done": True}])
def test_local_flags_or_profit_without_certificate_never_release(extra):
    p = dict(oil(), ig_size=.02, **extra)
    assert allocation(p)["consumed_allocation"] == pytest.approx(106.14)


@pytest.mark.parametrize("status", ["submitting", "unknown", "ACCEPTED", "reconciling"])
def test_pending_addon_consumes_remaining_capacity(status):
    a = allocation(certify(oil()), pending={"status": status, "order": {"size": .01}})
    assert a["remaining_allocation"] == 0
    assert a["pending_reservation"] == pytest.approx(53.642)
    assert a["reason"] == "allocation_retained_pending_exposure"


def test_pending_order_above_budget_is_not_under_reserved():
    a = allocation(certify(oil()), pending={"status": "unknown", "sized_notional": 1000.})
    assert a["pending_reservation"] == 250.
    assert a["actual_notional"] == pytest.approx(1197.278)
    assert a["remaining_allocation"] == 0.


@pytest.mark.parametrize("observed", [.04, .06])
def test_larger_broker_exposure_wins(observed):
    p = certify(oil()); p["allocation_observed_quantity"] = observed
    a = allocation(p)
    assert a["quantity"] == observed
    assert a["consumed_allocation"] >= 106.14


def test_larger_local_exposure_wins():
    p = certify(oil()); p["ig_size"] = .04; p["allocation_observed_quantity"] = .02
    assert allocation(p)["consumed_allocation"] == pytest.approx(106.14)


@pytest.mark.parametrize("pending", [False, True])
def test_restart_and_repeated_accounting_are_idempotent(pending):
    p = certify(oil())
    if pending:
        p["partial_exit_pending"] = dict(original_size=.04, status="unknown")
    before = allocation(p)
    recovered = json.loads(json.dumps(p))
    for _ in range(5):
        assert allocation(recovered) == before
    assert recovered == p


@pytest.mark.parametrize("changes", [{"dealStatus": "REJECTED"}, {"dealReference": "wrong"}])
def test_invalid_close_confirmation_cannot_certify(changes):
    c = dict(dealStatus="ACCEPTED", dealReference="close"); c.update(changes)
    with pytest.raises(ValueError):
        confirm_allocation_reduction(oil(), {"position": dict(dealId="oil", direction="BUY", size=.02)}, c, "close", .04)


@pytest.mark.parametrize("changes", [{"dealId": "wrong"}, {"direction": "SELL"},
                                     {"size": .04}, {"size": float("nan")}, {"size": 0}])
def test_invalid_residual_cannot_certify(changes):
    row = dict(dealId="oil", direction="BUY", size=.02); row.update(changes)
    with pytest.raises(ValueError):
        confirm_allocation_reduction(oil(), {"position": row}, {"dealStatus": "ACCEPTED"}, "close", .04)


def test_actual_partial_path_emits_and_persists_certificate():
    ns = close_harness()
    ns["_ig_live_get"].return_value["positions"][0]["position"]["direction"] = "BUY"
    ns["_live_partial_tp_exit"]({"GOLD": {"price": 102}})
    p = ns["_live"]["open_position"]
    assert p["allocation_confirmation"]["confirmed_quantity"] == 5
    assert p["partial_exit_done"] and p["partial_dollar_pnl"] == 10
    assert any(c.args[0] == "allocation_released_confirmed_partial" for c in ns["_live_observe"].call_args_list)
    assert json.loads(json.dumps(p))["allocation_confirmation"] == p["allocation_confirmation"]
    ns["_live_save_state"].assert_called()


def test_actual_partial_missing_identity_retains_reservation():
    ns = close_harness()
    ns["_live_partial_tp_exit"]({"GOLD": {"price": 102}})
    p = ns["_live"]["open_position"]
    assert "allocation_confirmation" not in p
    assert p["ig_size"] == 5  # TP policy remains unchanged.


def sugar_evidence():
    p = dict(deal_id="sugar", instrument="SUGAR", direction="short", fill_price=517.2,
             ig_size=.33, broker_stop_level=513.74994)
    return snapshot([p], aggregate=None, exit_price=510.3, multiplier=1, slippage=2, commission=0)


def economics(**kw):
    args = dict(spread=.6, spread_pct=.6/510*100, history=[.6/510*100]*60,
                min_readings=5, anomaly_factor=3., stop_distance=2., quantity=.03, min_deal=.01)
    ev = kw.pop("evidence", sugar_evidence()); args.update(kw)
    return spread_economics(ev, **args)


def test_sugar_early_f50_candidate_includes_all_costs():
    ev = sugar_evidence(); qty, reason = f50_capacity(ev, 2, .01)
    assert qty == .03 and reason == "ok"
    e = economics(quantity=qty)
    assert e["allowed"]
    assert e["incremental_reserve"] == pytest.approx(.198)
    assert e["f50_budget"] == pytest.approx(.2392599)
    assert e["protected_floor_after"] >= .5 * ev["liquidation_before"]


def test_full_candidate_rejected_without_silent_joint_resizing():
    ev = sugar_evidence(); ev["liquidation_before"] = .9735198
    qty, _ = f50_capacity(ev, 2, .01)
    assert qty == .08
    e = economics(evidence=ev, quantity=qty)
    assert not e["allowed"] and e["reason"] == "continuation_absolute_cost_veto"
    assert e["quantity"] == .08


@pytest.mark.parametrize("protected,expected_size,allowed", [
    (.4785198, .03, True), (.726, .06, False), (.9735198, .08, False)])
def test_three_retained_sugar_funding_groups(protected, expected_size, allowed):
    ev = sugar_evidence(); ev["liquidation_before"] = protected
    qty, _ = f50_capacity(ev, 2, .01)
    assert qty == expected_size
    assert economics(evidence=ev, quantity=qty)["allowed"] is allowed
    assert economics(evidence=ev, quantity=.01)["allowed"]


def test_unknown_primary_close_invalidates_continuation_without_losing_exposure():
    ns = close_harness()
    ns["_live"]["open_position"]["scaling_history_complete"] = True
    ns["_ig_live_post"].return_value = None
    ns["_live_close_position"]("take_profit", {"GOLD": {"price": 102}})
    assert ns["_live"]["open_position"]["ig_size"] == 10
    assert ns["_live"]["open_position"]["scaling_history_complete"] is False
    ns["_live_save_state"].assert_called()


def test_crash_at_preclose_save_has_not_submitted_a_close():
    class Crash(BaseException):
        pass
    ns = close_harness()
    ns["_live"]["open_position"]["scaling_history_complete"] = True
    ns["_live_save_state"] = Mock(side_effect=Crash())
    with pytest.raises(Crash):
        ns["_live_close_position"]("take_profit", {"GOLD": {"price": 102}})
    ns["_ig_live_post"].assert_not_called()
    assert ns["_live"]["open_position"]["scaling_history_complete"] is False


def test_full_confirmed_close_removes_campaign_without_minting_allocation_credit():
    ns = close_harness()
    ns["_ig_live_get"].return_value = {"positions": []}
    ns["_live_close_position"]("take_profit", {"GOLD": {"price": 102}})
    assert ns["_live"]["open_position"] is None
    assert "allocation_credit" not in ns["_live"]


def test_pathological_actual_spread_rejected_with_abundant_funding():
    ev = sugar_evidence(); ev["liquidation_before"] = 100
    e = economics(evidence=ev, spread=3., spread_pct=3/510*100)
    assert e["reason"] == "continuation_absolute_spread_veto"


@pytest.mark.parametrize("kw", [{"history": []}, {"history": [.1]*4}, {"spread": 0},
                               {"spread": float("nan")}, {"spread_pct": None},
                               {"quantity": .005}, {"quantity": .015}])
def test_missing_or_illegal_economics_fail_closed(kw):
    with pytest.raises((ValueError, TypeError)):
        economics(**kw)


@pytest.mark.parametrize("field,value", [("slippage", -1), ("commission", float("nan")),
                                        ("multiplier", 0), ("liquidation_before", 0),
                                        ("current_campaign_pnl", 0)])
def test_invalid_cost_inputs_fail_closed(field, value):
    ev = sugar_evidence(); ev[field] = value
    with pytest.raises(ValueError):
        economics(evidence=ev)


def test_actual_low_atr_continuation_can_submit_once():
    ns = defensive_harness()
    ns["_compute_atr_5m"] = lambda _: (.00001, False)
    d = evaluate(ns)
    assert d["decision"] == "approve", d
    assert d["continuation_spread_reason"] == "low_atr_continuation_admission"
    assert d["spread_atr"] > d["spread_atr_threshold"]
    assert d["broker_inventory_verified"]
    assert d["continuation_economics"]["allowed"]
    evaluate(ns)
    ns["_ig_live_post"].assert_called_once()


@pytest.mark.parametrize("case", ["missing_history", "widened", "missing_inventory", "quantity",
                                 "stop", "direction", "epic", "duplicate", "pending", "manual_review"])
def test_actual_protected_admission_fails_closed(case):
    ns = defensive_harness(); ns["_compute_atr_5m"] = lambda _: (.00001, False)
    if case == "missing_history": ns["_spread_hist"] = {}
    elif case == "widened": ns["_spread_hist"] = {"GOLD": [.00001]*60}
    elif case == "pending": ns["_live"]["pyramid_entry_pending"] = {"status": "ACCEPTED"}
    elif case == "manual_review": ns["_live"]["manual_review_required"] = True
    else:
        response = ns["_ig_live_get"]()
        row = response["positions"][0]
        if case == "missing_inventory": response = None
        if case == "quantity": row["position"]["size"] *= 2
        if case == "stop": row["position"]["stopLevel"] = 99
        if case == "direction": row["position"]["direction"] = "SELL"
        if case == "epic": row["market"]["epic"] = "wrong"
        if case == "duplicate": response["positions"].append(deepcopy(row))
        ns["_ig_live_get"] = Mock(return_value=response)
    d = evaluate(ns)
    assert d["decision"] == "reject", d
    ns["_ig_live_post"].assert_not_called()


def test_partial_realized_profit_not_added_to_f50():
    p = dict(deal_id="sugar", instrument="SUGAR", direction="short", fill_price=517.2,
             ig_size=.33, broker_stop_level=513.74994, partial_exit_done=True, partial_dollar_pnl=100.)
    ev = snapshot([p], aggregate=None, exit_price=510.3, multiplier=1, slippage=2, commission=0)
    assert ev["liquidation_before"] == sugar_evidence()["liquidation_before"]


@pytest.mark.parametrize("case", ["allocation", "margin", "MINDEAL", "kill", "ack", "sync"])
def test_existing_binding_constraints_still_block(case):
    ns = defensive_harness()
    if case == "allocation": ns["_live"]["balance_total"] = 25.
    if case == "margin": ns["_live_margin"]["GOLD"] = 1000.
    if case == "MINDEAL": ns["_live_min_deal"]["GOLD"] = 1000.
    if case == "kill": ns["_june_live_trading_enabled"] = False
    if case == "ack": ns["_live"]["open_position"]["broker_stop_level"] = None
    if case == "sync": ns["_live"]["open_position"]["stop_sync"] = {"status": "pending"}
    assert evaluate(ns)["decision"] == "reject"
    ns["_ig_live_post"].assert_not_called()


def test_nonprotected_ratio_gate_unchanged():
    ns = defensive_harness(stop=99., mode="normal")
    ns["_compute_atr_5m"] = lambda _: (.00001, False)
    assert evaluate(ns)["reason"] == "spread_atr_gate"
    ns["_ig_live_post"].assert_not_called()


# Cache parsing only; every invariant compares actual candidate and baseline ASTs.
@lru_cache(maxsize=1)
def policy_nodes():
    baseline = subprocess.check_output(["git", "show", "76951a7121c44ae6b35b7698c6bee2f62f5d581b:june.py"], encoding="utf-8")
    current = Path(__file__).with_name("june.py").read_text(encoding="utf-8")
    return [{n.name: ast.dump(n) for n in ast.parse(text).body if isinstance(n, ast.FunctionDef)}
            for text in (baseline, current)]


# Structural comparison locks policy functions to the supplied production baseline.
@pytest.mark.parametrize("name", ["_live_try_entry", "_live_check_exit", "_live_check_pyramid_exits",
    "_live_check_circuit_breaker", "_live_trade_guard", "_live_compute_stop_pts",
    "_live_tier_risk_pct", "_spread_atr_threshold", "_sim_get_tp", "_sim_get_dynamic_stop"])
def test_excluded_policy_functions_unchanged(name):
    baseline, current = policy_nodes()
    assert current[name] == baseline[name]
