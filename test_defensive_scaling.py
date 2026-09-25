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
    ns["_live"].update(global_mode=mode)
    p = ns["_live"]["open_position"]
    p.update(broker_stop_level=stop, scaling_history_complete=True, conviction=5,
             entry_time=FixedDateTime.now(timezone.utc).timestamp()-60)
    ns.update(datetime=FixedDateTime, _UK_TZ=timezone.utc,
              _MPD_SLIPPAGE_PIPS=2, _IG_EQUITY_COMMISSION_USD=9,
              _ls_deal_closed=Mock(return_value=False), _PYRAMID_PROFIT_GATE_PCT=.0015,
              _live_perf_blocked=Mock(return_value=False), _METALS_INSTRUMENTS=set(),
              _compute_atr_5m=lambda _: (1., False), _spread_atr_threshold=lambda *a: 1.)
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


SIGNALS = {"GOLD": {"price": 105., "spread_pct": .02}}


def evaluate(ns, regime="neutral"):
    ns["_live_check_pyramid_entry"](SIGNALS, regime)
    return ns["_live_observe"].call_args.args[3]


@pytest.mark.parametrize("stop", [100.02, 101.])
def test_actual_defensive_neutral_protected_order(stop):
    ns = defensive_harness(stop)
    d = evaluate(ns)
    assert d["decision"] == "approve", d
    ns["_ig_live_post"].assert_called_once()
    assert d["liquidation_after"] >= d["liquidation_before"] - 1e-9
    assert ns["_live"]["open_position"]["broker_stop_level"] > stop
    assert ns["_live"]["pyramid_agg_stop_level"] >= ns["_live"]["open_position"]["broker_stop_level"]


@pytest.mark.parametrize("regime", ["neutral", "bull", "bear"])
def test_unprotected_cannot_bypass_defensive_by_macro(regime):
    ns = defensive_harness(99.)
    d = evaluate(ns, regime)
    assert d["reason"] == "defensive_unprotected_campaign"
    ns["_ig_live_post"].assert_not_called()


def test_unacknowledged_prefinance_no_addon_and_floor_retained():
    ns = defensive_harness()
    ns["_ig_live_put"].return_value = None
    d = evaluate(ns)
    assert d["reason"] == "prefinancing_stop_not_acknowledged"
    ns["_ig_live_post"].assert_not_called()
    p = ns["_live"]["open_position"]
    assert p["broker_stop_level"] == 101.
    assert p["intended_stop_level"] > 101.


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
    ns["_live_min_deal"]["GOLD"] = 2
    assert evaluate(ns)["reason"] == "rounded_capacity_or_MINDEAL_rejected"
    ns["_ig_live_put"].assert_not_called()
    ns["_ig_live_post"].assert_not_called()


def test_fresh_defensive_neutral_still_blocked():
    ns = defensive_harness()
    ns["_live"]["open_position"] = None
    ns.update(_direct_cfd_signals={}, _live_select_instrument=Mock())
    execute([function("_live_try_entry")], ns)
    ns["_live_try_entry"]({}, "neutral")
    ns["_live_select_instrument"].assert_not_called()
