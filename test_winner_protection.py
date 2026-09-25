import json
import unittest
from unittest.mock import Mock
from winner_protection import campaign_stop, effective, protect
from test_live_accounting import close_harness
from test_winner_accounting import harness, add
from test_broker_identity import execute, function


def position(direction="long"):
    return dict(deal_id="deal", direction=direction, fill_price=100., ig_size=1.,
                stop_pct=.01, broker_stop_level=99. if direction == "long" else 101.)


def sync(pos, reply=None, put=None, now=1):
    put = put or Mock(return_value={"dealReference": "stop-ref"})
    confirm = Mock(return_value=reply)
    result = protect(pos, 102. if pos["direction"] == "long" else 98.,
                     put=put, confirm=confirm, save=Mock(), now=now, log=Mock())
    return result, put, confirm


class ProtectionTests(unittest.TestCase):
    def test_success_long_and_short(self):
        for direction, target in (("long", 102.), ("short", 98.)):
            pos = position(direction)
            ok, _, _ = sync(pos, dict(dealStatus="ACCEPTED", dealReference="stop-ref", dealId="deal", stopLevel=target))
            self.assertTrue(ok)
            self.assertEqual(pos["broker_stop_level"], target)
            self.assertEqual(pos["defensive_soft_sl"], target)

    def test_rejected(self):
        pos = position()
        ok, _, _ = sync(pos, dict(dealStatus="REJECTED", dealReference="stop-ref", dealId="deal"))
        self.assertFalse(ok)
        self.assertEqual(pos["stop_sync"]["status"], "rejected")
        self.assertEqual(pos["broker_stop_level"], 99.)
        self.assertEqual(effective(pos), 102.)

    def test_lost_response(self):
        pos = position()
        ok, _, _ = sync(pos, put=Mock(return_value=None))
        self.assertFalse(ok)
        self.assertEqual(pos["broker_stop_level"], 99.)
        self.assertEqual(effective(pos), 102.)

    def test_timeout(self):
        pos = position()
        ok, _, _ = sync(pos, put=Mock(side_effect=TimeoutError("timeout")))
        self.assertFalse(ok)
        self.assertEqual(pos["stop_sync"]["status"], "pending")
        self.assertEqual(pos["broker_stop_level"], 99.)

    def test_stale_or_incomplete_ack(self):
        for changes in ({"dealReference": "old"}, {"dealId": "other"}, {"stopLevel": 101.}, {"stopLevel": None}):
            reply = dict(dealStatus="ACCEPTED", dealReference="stop-ref", dealId="deal", stopLevel=102.)
            reply.update(changes)
            pos = position()
            self.assertFalse(sync(pos, reply)[0])
            self.assertEqual(pos["broker_stop_level"], 99.)

    def test_duplicate_ack_no_second_put(self):
        pos = position()
        reply = dict(dealStatus="ACCEPTED", dealReference="stop-ref", dealId="deal", stopLevel=102.)
        sync(pos, reply)
        ok, put, confirm = sync(pos, reply, now=2)
        self.assertTrue(ok)
        put.assert_not_called()
        confirm.assert_not_called()

    def test_restart_with_pending_reference(self):
        pos = position()
        sync(pos)
        pos = json.loads(json.dumps(pos))
        reply = dict(dealStatus="ACCEPTED", dealReference="stop-ref", dealId="deal", stopLevel=102.)
        ok, put, _ = sync(pos, reply, now=2)
        self.assertTrue(ok)
        put.assert_not_called()

    def test_weaker_proposal_cannot_replace_software_or_ack(self):
        for direction, target in (("long", 105.), ("short", 95.)):
            pos = position(direction)
            pos["defensive_soft_sl"] = target
            sync(pos)
            self.assertEqual(effective(pos), target)

    def test_partial_tp_preserves_positive_dple(self):
        for direction in ("long", "short"):
            ns = close_harness(direction=direction)
            pos = ns["_live"]["open_position"]
            pos["dple_effective_sl"] = .005
            ns["_live_partial_tp_exit"]({"GOLD": {"price": 102. if direction == "long" else 98.}})
            self.assertEqual(pos["dple_effective_sl"], .005)
            self.assertEqual(pos["ig_size"], 5.)

    def test_partial_before_dple_retains_spread_floor(self):
        ns = close_harness()
        ns["_live_partial_tp_exit"]({"GOLD": {"price": 102.}})
        self.assertEqual(ns["_live"]["open_position"]["dple_effective_sl"], -.001)

    def test_partial_cannot_loosen_compressed_stop(self):
        ns = close_harness()
        pos = ns["_live"]["open_position"]
        pos.update(stop_pct=.0005, initial_sl_pct=.0005)
        ns["_live_partial_tp_exit"]({"GOLD": {"price": 102.}})
        self.assertEqual(pos["stop_pct"], .0005)

    def test_campaign_floor_preserves_dollars_and_price(self):
        for direction in ("long", "short"):
            p = position(direction)
            sign = 1 if direction == "long" else -1
            p["defensive_soft_sl"] = 100 + sign * 2
            new = dict(p, fill_price=100 + sign * 4, ig_size=.5)
            target = campaign_stop([p], new, 100 - sign, None)
            pnl = sign * ((target - 100) + .5 * (target - new["fill_price"]))
            self.assertGreaterEqual(pnl + 1e-9, 2.)
            self.assertGreaterEqual(sign * (target - p["defensive_soft_sl"]), 0)

    def test_oil_historical_scenario(self):
        old = dict(position(), fill_price=9520.4, ig_size=.04, broker_stop_level=9525.)
        new = dict(old, fill_price=9578.2, ig_size=.03)
        target = campaign_stop([old], new, 9497.44557)
        self.assertGreaterEqual(.476 + .04 * (target - 9520.4) + .03 * (target - 9578.2), .476 + .04 * (9525 - 9520.4) - 1e-8)

    def test_mpd_then_addon_failed_put_no_fake_ack(self):
        ns = harness()
        p = ns["_live"]["open_position"]
        p.update(defensive_soft_sl=100.5, broker_stop_level=99.)
        add(ns)
        # Claimed profit protection with unresolved acknowledgement cannot fund
        # an addon, including in normal mode. Existing floor is retained.
        ns["_ig_live_post"].assert_not_called()
        self.assertEqual(p["defensive_soft_sl"], 100.5)
        self.assertEqual(p["broker_stop_level"], 99.)

    def test_addon_then_dple_or_mpd(self):
        ns = harness()
        add(ns)
        p = ns["_live"]["open_position"]
        old = effective(p)
        p["dple_effective_sl"] = .03
        sync(p)
        self.assertGreaterEqual(effective(p), old)
        self.assertGreaterEqual(effective(p), 103.)

    def test_software_only_no_put(self):
        pos = position()
        put = Mock()
        protect(pos, 102., put=put, confirm=Mock(), save=Mock(), now=1, log=Mock(), can_send=False)
        put.assert_not_called()
        self.assertEqual(effective(pos), 102.)

    def test_retry_runs_after_exit_in_live_loop(self):
        import ast
        node = function("_run_live_step_observed")
        calls = [(n.lineno, n.func.id) for n in ast.walk(node) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)]
        lines = dict((name, line) for line, name in calls)
        self.assertGreater(lines["_live_retry_stop_sync"], lines["_live_check_exit"])

    def test_actual_exit_after_failed_mpd_sync(self):
        for direction, price in (("long", 100.4), ("short", 99.6)):
            ns = close_harness(direction=direction)
            pos = ns["_live"]["open_position"]
            pos.update(defensive_soft_sl=100.5 if direction == "long" else 99.5,
                       defensive_stop_active=True, broker_stop_level=99. if direction == "long" else 101.,
                       stop_sync={"status": "pending"}, tp_pct=.1)
            ns.update(_ls_deal_closed=lambda _: False, _SIM_MAX_HOLD_SECS=100000,
                      _live_perf_blocked=lambda _: False, _sim_get_tp=lambda *a: .1,
                      _sim_get_dynamic_stop=lambda _: .01, _MPD_SLIPPAGE_PIPS=1,
                      _MPD_MIN_PROFIT_PIPS=1, _live_close_position=Mock())
            execute([function("_live_check_exit")], ns)
            ns["_live_check_exit"]({"GOLD": {"price": price}}, "neutral")
            ns["_live_close_position"].assert_called_once_with("mpd_floor", {"GOLD": {"price": price}})

    def test_actual_dple_after_partial_and_partial_after_dple(self):
        ns = close_harness()
        ns.update(_ls_deal_closed=lambda _: False, _SIM_MAX_HOLD_SECS=100000,
                  _live_perf_blocked=lambda _: False, _sim_get_tp=lambda *a: .01,
                  _sim_get_dynamic_stop=lambda _: .01, _MPD_SLIPPAGE_PIPS=10000,
                  _MPD_MIN_PROFIT_PIPS=1)
        execute([function("_live_check_exit")], ns)
        ns["_live_check_exit"]({"GOLD": {"price": 102.}}, "neutral")
        pos = ns["_live"]["open_position"]
        self.assertTrue(pos["partial_exit_done"])
        self.assertAlmostEqual(pos["dple_effective_sl"], .01)
        ns["_live_check_exit"]({"GOLD": {"price": 104.}}, "neutral")
        self.assertAlmostEqual(pos["dple_effective_sl"], .02)


if __name__ == "__main__":
    unittest.main()
