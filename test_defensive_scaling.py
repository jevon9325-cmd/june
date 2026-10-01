import json
from datetime import datetime, timezone
from unittest.mock import Mock
import pytest
from defensive_scaling import snapshot, plan, confirm_funding
from test_broker_identity import execute, function
from test_winner_accounting import harness


def position(direction="long", stop=101):
    return dict(instrument="GOLD", direction=direction, deal_id="primary",
                fill_price=100., ig_size=.4, broker_stop_level=stop, stop_pct=.01)


def evidence(p=None, exit_price=105):
    return snapshot([p or position()], aggregate=None, exit_price=exit_price,
                    multiplier=1, slippage=.02, commission=0)


@pytest.mark.parametrize("stop,classification", [(99., "unprotected"), (100.02, "breakeven_protected"), (101., "profit_protected")])
def test_economic_classification(stop, classification):
    assert evidence(position(stop=stop))["protection_state"] == classification


def test_breakeven_can_scale_only_after_financing_ack():
    old = evidence(position(stop=100.02))
    proposal = plan(old, entry_price=105.1, exit_price=105, stop_distance=.2, min_distance=.05, quantity=.4)
    with pytest.raises(ValueError, match="do_not_finance"):
        confirm_funding(old, old, proposal)
    funded = evidence(position(stop=proposal["prefinance_targets"][0]))
    assert confirm_funding(old, funded, proposal) >= -1e-9


@pytest.mark.parametrize("direction,stop,exit_price", [("long", 101., 105.), ("short", 99., 95.)])
def test_profit_floor_preserved_in_both_directions(direction, stop, exit_price):
    old = evidence(position(direction, stop), exit_price)
    proposal = plan(old, entry_price=exit_price, exit_price=exit_price, stop_distance=.2, min_distance=.05, quantity=.4)
    funded = evidence(position(direction, proposal["prefinance_targets"][0]), exit_price)
    assert confirm_funding(old, funded, proposal) == pytest.approx(old["liquidation_before"])


@pytest.mark.parametrize("changes", [dict(broker_stop_level=None, intended_stop_level=101.),
                                    dict(intended_stop_level=102.),
                                    dict(stop_sync={"status": "pending"}),
                                    dict(stop_sync={"status": "rejected"}),
                                    dict(stop_sync={"status": "acknowledged", "deal_id": "other"})])
def test_claimed_protection_is_not_evidence(changes):
    p = position()
    p.update(changes)
    with pytest.raises(ValueError):
        evidence(p)


def test_economically_destructive_addon_refused():
    with pytest.raises(ValueError, match="cannot_preserve_floor"):
        plan(evidence(), entry_price=105, exit_price=105, stop_distance=10, min_distance=.05, quantity=10)


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 9, 24, 15, tzinfo=timezone.utc).astimezone(tz)


def defensive_harness(stop=101., mode="defensive"):
    ns = harness()
    ns["_live"].update(global_mode=mode, balance_total=10000.)  # large enough for F50 allocation
    p = ns["_live"]["open_position"]
    p.update(broker_stop_level=stop, scaling_history_complete=True, conviction=5,
             entry_time=FixedDateTime.now(timezone.utc).timestamp()-60)
    ns.update(datetime=FixedDateTime, _UK_TZ=timezone.utc,
              _MPD_SLIPPAGE_PIPS=2, _IG_EQUITY_COMMISSION_USD=9,
              _ls_deal_closed=Mock(return_value=False), _PYRAMID_PROFIT_GATE_PCT=.0015,
              _live_perf_blocked=Mock(return_value=False), _METALS_INSTRUMENTS=set(),
              _compute_atr_5m=lambda _: (1., False), _spread_atr_threshold=lambda *a: 1.)
    ns.update(_spread_hist={"GOLD": [.0002] * 60}, SPREAD_MIN_READINGS=5,
              SPREAD_ALERT_FACTOR=3.)
    ns["_ig_live_get"] = Mock(side_effect=lambda *a, **k: {"positions": [
        {"position": {"dealId": leg["deal_id"], "size": leg["ig_size"],
                      "direction": "BUY" if leg["direction"] == "long" else "SELL",
                      "stopLevel": leg.get("broker_stop_level")},
         "market": {"epic": ns["INSTRUMENTS"][leg["instrument"]]}}
        for leg in [ns["_live"]["open_position"], *ns["_live"].get("pyramid_legs", [])]]})
    ns["_ig_live_put"] = Mock(return_value={"dealReference": "stop"})
    def confirm(ref):
        if ref == "stop":
            path, body = ns["_ig_live_put"].call_args.args
            return dict(dealStatus="ACCEPTED", dealReference=ref,
                        dealId=path.rsplit("/", 1)[-1], stopLevel=body["stopLevel"])
        return dict(dealStatus="ACCEPTED", dealReference=ref, dealId="addon", level=105., stopLevel=104.95)
    ns["_live_confirm_deal"] = Mock(side_effect=confirm)
    execute([function(n) for n in ("_live_defensive_scaling_evidence", "_live_check_pyramid_entry")], ns)
    return ns


# Leave room for the newly explicit spread reserve in this approval fixture.
SIGNALS = {"GOLD": {"price": 105., "spread_pct": .0002}}


def evaluate(ns, regime="neutral"):
    ns["_live_check_pyramid_entry"](SIGNALS, regime)
    return ns["_live_observe"].call_args.args[3]


def test_breakeven_protected_rejected_by_f50():
    # Build 2: F50 requires profit_protected; breakeven_protected campaigns are rejected.
    ns = defensive_harness(100.02)
    d = evaluate(ns)
    assert d["decision"] == "reject"
    assert "f50_requires_profit_protected" in d["reason"]
    ns["_ig_live_post"].assert_not_called()


def test_profit_protected_approved_by_f50():
    # Build 2: profit_protected campaigns use F50 sizing (no prefinancing stop tightening).
    ns = defensive_harness(101.)
    d = evaluate(ns)
    assert d["decision"] == "approve", d
    ns["_ig_live_post"].assert_called_once()
    # F50 invariant: estimated floor after addon >= 50% of protected_before.
    assert d["estimated_floor_after"] >= d["f50_required_floor"] - 1e-9
    # broker_stop_level update depends on distance gate (floating point); use software floor.
    assert ns["_live"]["open_position"].get("intended_stop_level", 0.) > 101.
    assert ns["_live"].get("pyramid_agg_stop_level", 0.) > 101.


@pytest.mark.parametrize("regime", ["neutral", "bull", "bear"])
def test_unprotected_cannot_bypass_defensive_by_macro(regime):
    ns = defensive_harness(99.)
    d = evaluate(ns, regime)
    assert d["reason"] == "f50_requires_profit_protected: unprotected"
    ns["_ig_live_post"].assert_not_called()


def test_f50_skips_prefinancing_and_approves():
    # Build 2: F50 does NOT call plan()/PUT for stop tightening before the addon POST.
    # Setting _ig_live_put to None has no effect — F50 path bypasses prefinancing entirely.
    ns = defensive_harness()
    ns["_ig_live_put"].return_value = None  # would break old plan() path — irrelevant in B2
    d = evaluate(ns)
    # F50 approves directly without prefinancing.
    assert d["decision"] == "approve"
    assert d["reason"] == "f50_protected_profit_finances_addon"
    # F50 invariant verified.
    assert d["f50_protected_before"] > 0
    assert d["estimated_floor_after"] >= d["f50_required_floor"] - 1e-9
    ns["_ig_live_post"].assert_called_once()


def test_duplicate_quote_and_restart_no_duplicate_order():
    ns = defensive_harness()
    assert evaluate(ns)["decision"] == "approve"
    ns["_live"] = json.loads(json.dumps(ns["_live"]))
    d = evaluate(ns)
    assert d["reason"] == "duplicate_accepted_quote_evaluation"
    ns["_ig_live_post"].assert_called_once()


def test_session_performance_still_independent():
    ns = defensive_harness()
    ns["_live_perf_blocked"].return_value = True
    assert evaluate(ns)["reason"] == "independent_session_or_performance_block"
    ns["_ig_live_post"].assert_not_called()


def test_instrument_defensive_uses_same_economic_rule():
    ns = defensive_harness(mode="normal")
    ns["_live"]["instrument_mode"] = {"GOLD": "defensive"}
    assert evaluate(ns)["decision"] == "approve"


@pytest.mark.parametrize("changes", [dict(scaling_history_complete=False),
                                    dict(broker_stop_level=None),
                                    dict(stop_sync={"status": "pending"}),
                                    dict(entry_time=1.)])
def test_ambiguous_evidence_refuses_only_addon(changes):
    ns = defensive_harness()
    ns["_live"]["open_position"].update(changes)
    assert evaluate(ns)["decision"] == "reject"
    ns["_ig_live_post"].assert_not_called()
    assert ns["_live"]["open_position"]


def test_capacity_before_prefinancing_even_with_MINDEAL():
    ns = defensive_harness()
    ns["_live_min_deal"]["GOLD"] = 10.  # B2: ig_raw≈2.18; MINDEAL=10 forces F50 mindeal_blocked
    assert evaluate(ns)["reason"] == "f50_mindeal_blocked: mindeal_blocked"
    ns["_ig_live_put"].assert_not_called()
    ns["_ig_live_post"].assert_not_called()


def test_fresh_defensive_neutral_now_reaches_selection():
    # Build 4C-A: DEFENSIVE + neutral no longer hard-returns before instrument
    # selection. It proceeds into the downstream funnel (and is gated there by the
    # stronger defensive conviction floor). With an empty candidate list, selection
    # is reached and returns no candidate -> no order. Proves reachability, not admission.
    ns = defensive_harness()
    ns["_live"]["open_position"] = None
    ns.update(_direct_cfd_signals={}, _live_select_instrument=Mock(return_value=[]))
    execute([function("_live_try_entry")], ns)
    ns["_live_try_entry"]({}, "neutral")
    ns["_live_select_instrument"].assert_called_once()
