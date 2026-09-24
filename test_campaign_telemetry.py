import json
import sqlite3
import tempfile
import unittest
from copy import deepcopy
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch
from campaign_telemetry import Store, executable
from test_live_accounting import close_harness
from test_broker_identity import execute, function
from test_winner_accounting import harness, add


def state(direction="long"):
    return {"open_position": dict(deal_id="p1", instrument="GOLD", direction=direction,
                                  fill_price=100., ig_size=1., pos_size=20., leverage=5,
                                  notional=100., stop_pct=.01, tp_pct=.02, entry_time=1.),
            "pyramid_legs": []}


class TelemetryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "telemetry.sqlite3"
        self.store = Store(self.path)

    def tearDown(self):
        self.temp.cleanup()

    def observe(self, s=None, price=101., now=100., **kwargs):
        self.store.observe(s if s is not None else state(), {"GOLD": {"price": price, "spread_pct": .2}},
                           account="acct", now=now, unit=lambda s, p: p, cadence=60, **kwargs)

    def rows(self, table):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute(f"SELECT * FROM {table}").fetchall()

    def data(self):
        return json.loads(self.rows("campaigns")[0][4])

    def kinds(self):
        return [row[3] for row in self.rows("events")]

    def test_executable_long_short(self):
        self.assertEqual(executable({"bid": 99., "offer": 101.}, "long"), (99., "bid_observed"))
        self.assertEqual(executable({"bid": 99., "offer": 101.}, "short"), (101., "offer_observed"))
        self.assertEqual(executable({"price": 100., "spread_pct": 2.}, "short")[0], 101.)

    def test_missing_spread_is_gap_not_zero_cost(self):
        self.assertIsNone(executable({"price": 100.}, "long")[0])

    def test_extrema_and_timestamps(self):
        self.observe(price=99., now=100.)
        self.observe(price=103., now=160.)
        data = self.data()
        self.assertAlmostEqual(data["extrema"]["mae_dollars"]["value"], -1.099)
        self.assertEqual(data["extrema"]["mae_dollars"]["at"], 100.)
        self.assertEqual(data["extrema"]["mfe_dollars"]["at"], 160.)
        self.assertAlmostEqual(data["extrema"]["mfe_return"]["value"], .02897)
        self.assertEqual(data["last_observation"]["elapsed_since_sample"], 60.)

    def test_short_economics(self):
        self.observe(state("short"), price=98.)
        self.assertAlmostEqual(self.data()["last_observation"]["estimated_campaign_pnl"], 1.902)

    def test_restart_continuation(self):
        self.observe(price=103.)
        original = self.data()["extrema"]["mfe_dollars"]
        self.store = Store(self.path)
        self.observe(price=102., now=200.)
        self.assertEqual(self.data()["extrema"]["mfe_dollars"], original)
        self.assertEqual(len(self.rows("campaigns")), 1)

    def test_duplicate_event_suppression(self):
        self.observe(event="entry")
        self.observe(event="entry", now=200.)
        self.assertEqual(self.kinds().count("entry"), 1)

    def test_threshold_once(self):
        self.observe(details={"pyramid_trigger": .0015}, price=103.)
        self.observe(details={"pyramid_trigger": .0015}, price=104., now=200.)
        self.assertEqual(self.kinds().count("pyramid_threshold_first_reached"), 1)
        self.assertEqual(self.kinds().count("tp_reached"), 1)

    def test_partial_realized_and_open(self):
        s = state()
        self.observe(s)
        s["open_position"].update(ig_size=.5, notional=50., partial_dollar_pnl=1., partial_exit_done=True)
        self.observe(s, price=102., event="partial_tp_confirmed")
        payload = self.data()["last_observation"]
        self.assertAlmostEqual(payload["estimated_campaign_pnl"], 1.949)
        self.assertEqual(payload["realized_pnl_known_to_june"], 1.)
        self.assertEqual(payload["quantity"], .5)

    def test_addon_lifecycle_and_promotion_identity(self):
        s = state()
        self.observe(s)
        addon = dict(s["open_position"], deal_id="a1", leg_index=2, fill_price=101., ig_size=.5)
        s["pyramid_legs"] = [addon]
        self.observe(s, event="addon_opened", position=addon)
        cid = self.data()["campaign_id"]
        self.observe(s, event="leg_closed", position=s["open_position"], details={"realized_pnl": 1.})
        self.observe({"open_position": addon, "pyramid_legs": []}, now=200.)
        self.assertEqual(self.data()["campaign_id"], cid)
        self.assertEqual(self.data()["primary_deal_id"], "p1")
        self.assertEqual(self.data()["last_observation"]["quantity"], .5)

    def test_protection_economics(self):
        s = state()
        s["open_position"].update(dple_effective_sl=.005, intended_stop_level=100.5, broker_stop_level=99.)
        self.observe(s)
        payload = self.data()["last_observation"]
        self.assertAlmostEqual(payload["estimated_pnl_at_protection"], .5)
        self.assertEqual(payload["costs_status"], "PROVISIONAL")
        self.assertEqual(payload["legs"][0]["broker_stop_level"], 99.)

    def test_missing_price_persisted_gap(self):
        self.observe(price=None)
        self.assertIn("observation_gap", self.kinds())
        self.assertEqual(self.data()["extrema"], {})
        self.assertIsNone(self.data()["last_observation"]["estimated_campaign_pnl"])

    def test_final_close_local_not_broker_finality(self):
        self.observe()
        self.observe({}, now=200., event="after_evaluation")
        self.assertIn("final_campaign_close", self.kinds())
        self.assertTrue(self.data()["realized_incomplete"])
        self.assertEqual(self.rows("campaigns")[0][3], 1)

    def test_unknown_addon_intent_prevents_final_close_claim(self):
        self.observe()
        self.observe({"pyramid_entry_pending": {"status": "submitting"}}, event="after_evaluation")
        self.assertNotIn("final_campaign_close", self.kinds())
        self.assertEqual(self.rows("campaigns")[0][3], 0)

    def test_close_then_reentry_without_flat_callback_finalizes_old_campaign(self):
        self.observe()
        new = state()
        new["open_position"]["deal_id"] = "p2"
        self.observe(new, now=200., event="entry")
        self.observe(new, now=201., event="after_evaluation")
        self.assertEqual(sum(row[3] for row in self.rows("campaigns")), 1)
        self.assertIn("final_campaign_close", self.kinds())

    def test_known_close_not_double_counted(self):
        s = state()
        self.observe(s)
        self.observe(s, event="leg_closed", position=s["open_position"], details={"realized_pnl": 2.})
        self.assertEqual(self.data()["last_observation"]["estimated_campaign_pnl"], 2.)
        self.observe({}, event="after_evaluation")
        self.assertFalse(self.data().get("realized_incomplete", False))

    def test_count_retention(self):
        self.store = Store(self.path, sample_cap=3, event_cap=4)
        for i in range(8):
            self.observe(now=100+i, event="sample", price=100+i)
        self.assertEqual(len(self.rows("samples")), 3)
        self.assertLessEqual(len(self.rows("events")), 4)
        self.assertAlmostEqual(self.data()["extrema"]["mae_dollars"]["value"], -.1)

    def test_age_retention(self):
        self.store = Store(self.path, retention=10)
        self.observe(now=1.)
        self.observe(now=20.)
        self.assertEqual(len(self.rows("samples")), 1)
        self.assertEqual(self.data()["started_observing_at"], 1.)

    def test_input_state_unchanged(self):
        s = state()
        original = deepcopy(s)
        self.observe(s)
        self.assertEqual(s, original)

    def test_account_uses_retained_opening_evidence(self):
        s = state()
        s["open_position"]["broker_entry_evidence"] = {"account_id": "original-account"}
        self.observe(s)
        self.assertEqual(self.data()["account"], "original-account")
        self.assertEqual(self.data()["account_basis"], "retained_opening_account_evidence")

    def wire(self, ns):
        ns.update(_live_sess={"account_id": "acct"}, _current_cycle_signals_snap={},
                  _live_campaign_unit=lambda s, p: p, POLL_ACTIVE=60)
        execute([function("_live_observe")], ns)

    def test_storage_failure_cannot_veto_protective_close(self):
        ns = close_harness()
        self.wire(ns)
        ns["_ig_live_get"].return_value = {"positions": []}
        with patch("campaign_telemetry.default_store", side_effect=OSError("disk full")):
            ns["_live_close_position"]("stop_loss", {"GOLD": {"price": 99.}})
        ns["_ig_live_post"].assert_called_once()
        self.assertIsNone(ns["_live"]["open_position"])

    def test_enabled_and_failed_telemetry_same_close_decision_and_payload(self):
        outputs = []
        for failing in (False, True):
            ns = close_harness()
            self.wire(ns)
            ns["_ig_live_get"].return_value = {"positions": []}
            with patch("campaign_telemetry.default_store", side_effect=OSError() if failing else None, return_value=self.store):
                ns["_live_close_position"]("stop_loss", {"GOLD": {"price": 99., "spread_pct": .1}})
            outputs.append((ns["_ig_live_post"].call_args, ns["_live"]))
        self.assertEqual(outputs[0], outputs[1])

    def test_partial_and_mindeal_hooks(self):
        for min_deal, expected in ((.01, "partial_tp_confirmed"), (6., "mindeal_full_close_fallback")):
            ns = close_harness()
            self.wire(ns)
            ns["_live_min_deal"]["GOLD"] = min_deal
            ns["_live_close_position"] = Mock()
            with patch("campaign_telemetry.default_store", return_value=self.store):
                ns["_live_partial_tp_exit"]({"GOLD": {"price": 102., "spread_pct": .1}})
            self.assertIn(expected, self.kinds())

    def test_addon_hooks(self):
        ns = harness()
        self.wire(ns)
        with patch("campaign_telemetry.default_store", return_value=self.store):
            add(ns)
        for kind in ("addon_proposed", "addon_accepted", "addon_opened"):
            self.assertIn(kind, self.kinds())

    def test_exit_reason_events(self):
        for reason, kind in (("stop_loss", "stop_exit"), ("max_hold", "max_hold_exit"), ("reversal", "reversal_exit")):
            self.observe(event="exit_requested", details={"reason": reason})
            self.assertIn(kind, self.kinds())

    def test_normal_loop_failure_still_calls_trading(self):
        ns = dict(_run_live_step_observed=Mock(), _PYRAMID_PROFIT_GATE_PCT=.0015, _live={})
        self.wire(ns)
        execute([function("run_live_step")], ns)
        with patch("campaign_telemetry.default_store", side_effect=sqlite3.OperationalError("locked")):
            ns["run_live_step"]({})
        ns["_run_live_step_observed"].assert_called_once_with({})

    def test_unknown_addon_disappearance_not_zero_realized(self):
        s = state()
        addon = dict(s["open_position"], deal_id="addon", leg_index=2)
        s["pyramid_legs"] = [addon]
        self.observe(s)
        self.observe(s, event="leg_closed", position=addon)
        self.assertTrue(self.data()["realized_incomplete"])
        self.assertEqual(self.data()["last_observation"]["costs_status"], "UNKNOWN")
        self.assertIn("addon_closed", self.kinds())

    def test_missing_metadata_keeps_quantity_but_not_fabricated_exposure(self):
        self.store.observe(state(), {"GOLD": {"price": 101., "spread_pct": .1}},
                           account="acct", now=1., unit=Mock(side_effect=ValueError("missing lot")))
        payload = self.data()["last_observation"]
        self.assertEqual(payload["quantity"], 1.)
        self.assertIsNone(payload["entry_exposure"])

    def test_campaign_count_retention_and_no_broker_database_touch(self):
        broker = self.path.with_name(".broker-evidence.sqlite3")
        broker.write_bytes(b"unresolved broker evidence fixture")
        self.store = Store(self.path, campaign_cap=2)
        for i in range(5):
            s = state()
            s["open_position"]["deal_id"] = f"p{i}"
            self.observe(s, now=100+i*2)
            self.observe({}, now=101+i*2, event="after_evaluation")
        self.assertEqual(len(self.rows("campaigns")), 2)
        self.assertEqual(broker.read_bytes(), b"unresolved broker evidence fixture")

    def test_real_sqlite_lock_does_not_veto_close(self):
        self.observe()
        blocker = sqlite3.connect(self.path)
        try:
            blocker.execute("BEGIN EXCLUSIVE")
            ns = close_harness()
            self.wire(ns)
            ns["_ig_live_get"].return_value = {"positions": []}
            with patch("campaign_telemetry.default_store", return_value=self.store):
                ns["_live_close_position"]("stop_loss", {"GOLD": {"price": 99.}})
            self.assertIsNone(ns["_live"]["open_position"])
        finally:
            blocker.rollback()
            blocker.close()


if __name__ == "__main__":
    unittest.main()
