"""Actual production addon function; all broker/storage effects are mocked."""
import json
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from test_broker_identity import execute, function
from winner_accounting import campaign_allocation, unit_exposure, validate_addon


def primary():
    return dict(deal_id="primary", instrument="GOLD", direction="long",
                ig_size=.4, fill_price=100., leverage=2, pos_size=20., stop_pct=.01)


def harness():
    ns = dict(_live={"open_position": primary(), "pyramid_legs": [], "balance_total": 100.},
              json=json, time=SimpleNamespace(time=lambda: 1000.),
              _live_equity_cfd=set(), _live_fx_instruments=set(),
              _live_lot_sizes={"GOLD": 1.}, _live_min_deal={"GOLD": .01},
              _live_price_unit={"GOLD": 1.}, _live_fx_base={"GOLD": 1.},
              _live_pip_sizes={"GOLD": .01}, _live_min_stop_pts={"GOLD": 4},
              _live_margin={"GOLD": .01}, _LIVE_LOT_SIZE_FX=1., _LIVE_FX_PIP=.01,
              _live_trade_guard=Mock(return_value=True), _pyramid_active_max_legs=lambda: 4,
              _live_tier_risk_pct=lambda _: .8, _live_tier_name=lambda _: "fixture",
              _real_margin_fraction=lambda s, m: m, _sim_get_dynamic_stop=lambda _: .01,
              _sim_get_spread_floor=lambda _: .001, _sim_get_tp=lambda *a: .02,
              _live_compute_stop_pts=lambda *a: 5, _PYRAMID_3LEG_SIZE_DECAY=.7,
              _PYRAMID_4LEG_SIZE_DECAY=.5, _PYRAMID_AGG_STOP_PCT=.002,
              _MINDEAL_OVERSIZE_MAX=3.5, _june_live_trading_enabled=True,
              INSTRUMENTS={"GOLD": "FIXTURE.GOLD"}, _LIVE_REDIS_KEY="fixture",
              _LIVE_REDIS_TTL=1000, _redis=Mock(return_value=Mock()),
              _ig_live_post=Mock(return_value={"dealReference": "r1"}),
              _live_confirm_deal=Mock(return_value={"dealStatus": "ACCEPTED", "dealId": "a1", "level": 101.}),
              _ig_live_put=Mock(return_value=None), _live_entry_evidence=Mock(return_value={}))
    for name in ("_live_observe", "_live_log", "_live_save_state", "_live_capture_evidence"):
        ns[name] = Mock()
    execute([function(n) for n in ("_live_protect_stop", "_live_campaign_unit", "_live_compute_ig_size",
                                   "_live_add_pyramid_leg")], ns)
    return ns


def add(ns):
    ns["_live_add_pyramid_leg"]({"GOLD": {"price": 100.}})


class AccountingTests(unittest.TestCase):
    def test_no_addon(self):
        result = campaign_allocation(primary(), [], lambda p: p, 80)
        self.assertEqual((result["quantity"], result["actual_notional"], result["remaining_allocation"]), (.4, 40., 60.))

    def test_prior_addons_consume_capacity(self):
        addon = dict(primary(), deal_id="addon", ig_size=.3)
        result = campaign_allocation(primary(), [addon], lambda p: p, 80)
        self.assertEqual(result["remaining_allocation"], 45.)

    def test_closed_addon_releases_capacity(self):
        addon = dict(primary(), deal_id="addon", ig_size=.3)
        with_addon = campaign_allocation(primary(), [addon], lambda p: p, 80)
        closed = campaign_allocation(primary(), [], lambda p: p, 80)
        self.assertEqual(closed["remaining_allocation"] - with_addon["remaining_allocation"], 15.)

    def test_partial_primary_does_not_release_reservation(self):
        p = dict(primary(), ig_size=.2)
        result = campaign_allocation(p, [], lambda p: p, 80)
        self.assertEqual(result["actual_notional"], 20.)
        self.assertEqual(result["consumed_allocation"], 20.)
        self.assertEqual(p["pos_size"], 20.)

    def test_rounded_primary_above_reservation_consumes_capacity(self):
        result = campaign_allocation(dict(primary(), ig_size=1.), [], lambda p: p, 80)
        self.assertEqual(result["remaining_allocation"], 30.)

    def test_units(self):
        self.assertEqual(unit_exposure(9500, kind="commodity", lot=1, price_unit=.01), 9500)
        self.assertEqual(unit_exposure(100, kind="equity", price_unit=.01, fx=.8), 1.25)
        self.assertEqual(unit_exposure(1.2, kind="fx", lot=10, pip=.0001), 100000)

    def test_malformed_metadata(self):
        for value in (None, 0, -1, "bad", float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                unit_exposure(100, kind="commodity", lot=value)

    def test_duplicate_inventory_refused(self):
        with self.assertRaises(ValueError):
            campaign_allocation(primary(), [primary()], lambda p: p, 80)

    def test_oversize_and_affordability(self):
        for args in [(40, 10, 2, 60, .01, 100, 40), (40, 40, 2, 60, 1., 70, 40)]:
            with self.assertRaises(ValueError):
                validate_addon(*args)

    def test_round_up_within_capacity(self):
        ns = harness()
        ns["_live_min_deal"]["GOLD"] = .5
        add(ns)
        leg = ns["_live"]["pyramid_legs"][0]
        self.assertEqual((leg["ig_size"], leg["intended_notional"], leg["actual_notional"]), (.5, 40, 50.5))
        self.assertEqual(leg["notional"], 50.5)

    def test_round_up_exceeds_capacity_no_post(self):
        ns = harness()
        ns["_live_min_deal"]["GOLD"] = 1.3
        add(ns)
        ns["_ig_live_post"].assert_not_called()

    def test_previous_addons_prevent_reuse(self):
        ns = harness()
        ns["_live"]["pyramid_legs"] = [dict(primary(), deal_id="old", ig_size=1.18)]
        add(ns)
        ns["_ig_live_post"].assert_not_called()

    def test_unknown_post_and_restart_do_not_retry(self):
        ns = harness()
        ns["_ig_live_post"].return_value = None
        add(ns)
        snapshot = json.loads(ns["_redis"]().set.call_args.args[1])
        ns["_live"] = snapshot
        add(ns)
        ns["_ig_live_post"].assert_called_once()

    def test_unknown_confirmation_no_retry(self):
        ns = harness()
        ns["_live_confirm_deal"].return_value = None
        add(ns)
        add(ns)
        ns["_ig_live_post"].assert_called_once()

    def test_rejected_attempt_allows_replacement(self):
        ns = harness()
        ns["_live_confirm_deal"].return_value = {"dealStatus": "REJECTED"}
        add(ns)
        self.assertNotIn("pyramid_entry_pending", ns["_live"])
        add(ns)
        self.assertEqual(ns["_ig_live_post"].call_count, 2)

    def test_pending_persistence_failure_no_post(self):
        ns = harness()
        ns["_redis"]().set.side_effect = OSError("offline")
        add(ns)
        ns["_ig_live_post"].assert_not_called()

    def test_missing_margin_or_lot_no_post(self):
        for name in ("_live_lot_sizes", "_live_margin", "_live_min_deal"):
            ns = harness()
            ns[name] = {}
            add(ns)
            ns["_ig_live_post"].assert_not_called()

    def test_duplicate_confirmation_preserves_pending(self):
        ns = harness()
        ns["_live_confirm_deal"].return_value["dealId"] = "primary"
        add(ns)
        self.assertEqual(ns["_live"]["pyramid_legs"], [])
        self.assertIn("pyramid_entry_pending", ns["_live"])

    def test_incomplete_accepted_confirmation_preserves_durable_evidence(self):
        ns = harness()
        reply = {"dealStatus": "ACCEPTED", "dealId": "a1"}
        ns["_live_confirm_deal"].return_value = reply
        add(ns)
        call = ns["_live_capture_evidence"].call_args
        self.assertEqual(call.args[1], "addon_opening_outcome_unresolved")
        self.assertEqual(call.kwargs["confirmation"], reply)
        self.assertIn("pyramid_entry_pending", ns["_live"])
        self.assertEqual(ns["_live"]["pyramid_legs"], [])

    def test_accepted_round_trip(self):
        ns = harness()
        add(ns)
        state = ns["_live"]
        self.assertEqual(json.loads(json.dumps(state)), state)
        self.assertAlmostEqual(state["pyramid_legs"][0]["actual_notional"], 40.4)

    def test_cap_applies_even_to_direct_call(self):
        ns = harness()
        ns["_pyramid_active_max_legs"] = lambda: 2
        add(ns)
        add(ns)
        ns["_ig_live_post"].assert_called_once()

    def test_malformed_price_unit_fails_closed(self):
        ns = harness()
        ns["_live_price_unit"]["GOLD"] = "malformed"
        add(ns)
        ns["_ig_live_post"].assert_not_called()

    def test_three_and_four_legs_use_existing_decay_and_cap(self):
        ns = harness()
        for i in range(3):
            ns["_live_confirm_deal"].return_value = {"dealStatus": "ACCEPTED", "dealId": f"a{i}", "level": 100.}
            add(ns)
        self.assertEqual([leg["intended_notional"] for leg in ns["_live"]["pyramid_legs"]], [40., 28., 20.])
        add(ns)
        self.assertEqual(ns["_ig_live_post"].call_count, 3)


if __name__ == "__main__":
    unittest.main()
