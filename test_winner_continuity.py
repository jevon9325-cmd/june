"""Winner-continuity + joint-sizing OBSERVATION-ONLY diagnostics tests
(feature/winner-continuity-joint-sizing-6a4b27e).

These assert the pure diagnostic logic AND that the june.py wiring is observation
only: the unsplittable full-close fallback still executes and the F50 addon size
is unchanged. No trading-decision behavior is modified by this feature.
"""
import ast
import unittest
from pathlib import Path

from winner_continuity import (unsplittable_readiness, joint_sizing_diagnostic,
                               legal_quantity_floor, READY, NOT_READY)


# ─────────────────────── unsplittable readiness ─────────────────────────────
class ReadinessTests(unittest.TestCase):
    def _short(self, **over):
        base = dict(position={"direction": "short", "fill_price": 47459.0},
                    ig_size=0.01, min_deal=0.01, acknowledged_stop_level=47400.0,
                    stop_sync_status="acknowledged", exit_price=47350.0,
                    multiplier=1.0, cost_reserve=0.02)
        base.update(over)
        return unsplittable_readiness(base.pop("position"), **base)

    def test_ready_broker_protected_unsplittable(self):
        v = self._short()
        self.assertEqual(v["verdict"], READY)
        self.assertEqual(v["reason"], "broker_protected_unsplittable_winner")
        self.assertFalse(v["legal_split"])
        self.assertGreater(v["protected_net"], 0)

    def test_software_only_protection_not_ready(self):
        v = self._short(stop_sync_status="pending")
        self.assertEqual(v["verdict"], NOT_READY)
        self.assertEqual(v["reason"], "protection_not_broker_acknowledged")

    def test_no_acknowledged_level_not_ready(self):
        v = self._short(acknowledged_stop_level=None)
        self.assertEqual(v["verdict"], NOT_READY)
        self.assertEqual(v["reason"], "protection_not_broker_acknowledged")

    def test_stop_not_protective_not_ready(self):
        # short with stop ABOVE fill => locks a loss, not protective (HO#2 shape)
        v = unsplittable_readiness({"direction": "short", "fill_price": 47459.0},
                                   ig_size=0.01, min_deal=0.01,
                                   acknowledged_stop_level=47496.496,
                                   stop_sync_status="acknowledged",
                                   exit_price=47533.0, multiplier=1.0, cost_reserve=0.02)
        self.assertEqual(v["verdict"], NOT_READY)
        self.assertEqual(v["reason"], "acknowledged_stop_not_protective")

    def test_protected_below_minimum_not_ready(self):
        v = self._short(cost_reserve=0.02, min_protected_profit=10.0)
        self.assertEqual(v["verdict"], NOT_READY)
        self.assertEqual(v["reason"], "protected_profit_below_minimum")

    def test_splittable_not_applicable(self):
        v = unsplittable_readiness({"direction": "short", "fill_price": 100.0},
                                   ig_size=0.08, min_deal=0.01,
                                   acknowledged_stop_level=99.0,
                                   stop_sync_status="acknowledged",
                                   exit_price=98.0, multiplier=1.0)
        self.assertEqual(v["verdict"], NOT_READY)
        self.assertEqual(v["reason"], "splittable_not_applicable")
        self.assertTrue(v["legal_split"])

    def test_manual_review_ambiguous(self):
        self.assertEqual(self._short(manual_review=True)["reason"], "campaign_state_ambiguous")

    def test_orphan_ambiguous(self):
        self.assertEqual(self._short(orphan=True)["reason"], "campaign_state_ambiguous")

    def test_pending_partial_ambiguous(self):
        self.assertEqual(self._short(partial_exit_pending=True)["reason"], "campaign_state_ambiguous")

    def test_missing_metadata_fail_closed(self):
        v = unsplittable_readiness({"direction": "short"}, ig_size=None, min_deal=0.01,
                                   acknowledged_stop_level=47400.0, stop_sync_status="acknowledged",
                                   exit_price=47350.0, multiplier=1.0)
        self.assertEqual(v["verdict"], NOT_READY)
        self.assertEqual(v["reason"], "insufficient_position_metadata")

    def test_long_ready(self):
        v = unsplittable_readiness({"direction": "long", "fill_price": 100.0},
                                   ig_size=0.01, min_deal=0.01, acknowledged_stop_level=101.0,
                                   stop_sync_status="acknowledged", exit_price=102.0,
                                   multiplier=1.0, cost_reserve=0.0)
        self.assertEqual(v["verdict"], READY)
        self.assertAlmostEqual(v["protected_gross"], 0.01)  # (101-100)*0.01*1
        self.assertAlmostEqual(v["giveback_budget"], 0.01)  # (102-100)*0.01 - 0.01


# ─────────────────────── joint sizing diagnostic ────────────────────────────
class JointSizingTests(unittest.TestCase):
    def test_f50_max_fits_allocation_no_rescue(self):
        # SUGAR 11:15 shape: F50 max already fits allocation -> joint == f50.
        d = joint_sizing_diagnostic(q_f50=0.08, min_deal=0.01, mid=512.0, leverage=4.0,
                                    remaining_allocation=24.268, margin_fraction=0.025,
                                    equity=False, residual_notional=512.0*0.33,
                                    per_lot_cost=6.0, expendable=0.4867599, commission=0.0)
        self.assertEqual(d["q_joint"], 0.08)
        self.assertFalse(d["joint_lt_f50"])

    def test_allocation_binds_below_f50(self):
        d = joint_sizing_diagnostic(q_f50=0.08, min_deal=0.01, mid=512.0, leverage=4.0,
                                    remaining_allocation=3.0, margin_fraction=0.025,
                                    equity=False, residual_notional=512.0*0.33,
                                    per_lot_cost=6.0, expendable=0.4867599, commission=0.0)
        self.assertEqual(d["q_joint"], 0.02)
        self.assertTrue(d["joint_lt_f50"])
        self.assertEqual(d["binding_constraint"], "allocation")

    def test_f50_cost_binds(self):
        d = joint_sizing_diagnostic(q_f50=0.08, min_deal=0.01, mid=512.0, leverage=4.0,
                                    remaining_allocation=100.0, margin_fraction=0.025,
                                    equity=False, residual_notional=0.0,
                                    per_lot_cost=6.0, expendable=0.18, commission=0.0)
        # cost ceiling = floor(0.18/6/0.01)*0.01 = 0.03
        self.assertEqual(d["q_cost"], 0.03)
        self.assertEqual(d["q_joint"], 0.03)
        self.assertEqual(d["binding_constraint"], "f50_cost")

    def test_no_mindeal_fits_blocked(self):
        d = joint_sizing_diagnostic(q_f50=0.0, min_deal=0.01, mid=512.0, leverage=4.0,
                                    remaining_allocation=100.0, margin_fraction=0.025,
                                    equity=False, residual_notional=0.0,
                                    per_lot_cost=6.0, expendable=0.5, commission=0.0)
        self.assertEqual(d["q_joint"], 0.0)
        self.assertEqual(d["binding_constraint"], "mindeal_blocked")

    def test_mindeal_boundary(self):
        d = joint_sizing_diagnostic(q_f50=0.01, min_deal=0.01, mid=100.0, leverage=4.0,
                                    remaining_allocation=100.0, margin_fraction=0.025,
                                    equity=False, residual_notional=0.0,
                                    per_lot_cost=1.0, expendable=0.5, commission=0.0)
        self.assertEqual(d["q_joint"], 0.01)

    def test_insufficient_metadata(self):
        d = joint_sizing_diagnostic(q_f50=0.08, min_deal=0.0, mid=512.0, leverage=4.0,
                                    remaining_allocation=24.0, margin_fraction=0.025,
                                    equity=False, residual_notional=0.0,
                                    per_lot_cost=6.0, expendable=0.5)
        self.assertEqual(d["binding_constraint"], "insufficient_metadata")

    def test_legal_quantity_floor(self):
        self.assertEqual(legal_quantity_floor(0.079, 0.01), 0.07)
        self.assertEqual(legal_quantity_floor(0.005, 0.01), 0.0)
        self.assertAlmostEqual(legal_quantity_floor(0.08, 0.01), 0.08)
        self.assertEqual(legal_quantity_floor(None, 0.01), 0.0)


# ─────────────── june.py wiring: observation-only guarantees ─────────────────
class WiringObservationOnlyTests(unittest.TestCase):
    def setUp(self):
        self.src = Path("june.py").read_text(encoding="utf-8")
        self.tree = ast.parse(self.src)

    def test_readiness_observe_before_full_close(self):
        fn = next(n for n in ast.walk(self.tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "_live_partial_tp_exit")
        # Find the fallback branch: _wc_readiness_observe called, then
        # _live_close_position('take_profit') still called after it.
        calls = [(n.lineno, n.func.attr if isinstance(n.func, ast.Attribute) else
                  getattr(n.func, "id", None))
                 for n in ast.walk(fn) if isinstance(n, ast.Call)]
        names = [c[1] for c in calls]
        self.assertIn("_wc_readiness_observe", names)
        self.assertIn("_live_close_position", names)
        # readiness observe must appear before the full close in source order
        ro = min(l for l, nm in calls if nm == "_wc_readiness_observe")
        fc = min(l for l, nm in calls if nm == "_live_close_position")
        self.assertLess(ro, fc, "readiness observe must precede full-close fallback")

    def test_full_close_fallback_unchanged(self):
        # The fallback still calls _live_close_position with take_profit.
        self.assertIn('_live_close_position("take_profit", signals)', self.src)

    def test_joint_sizing_diagnostic_does_not_resize(self):
        fn = next(n for n in ast.walk(self.tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "_live_add_pyramid_leg")
        src = ast.get_source_segment(self.src, fn)
        self.assertIn("joint_addon_sizing_diagnostic", src)
        # The selected size assignment must remain _f50_ig (observation does not change it).
        self.assertIn("ig_size = positive(_f50_ig)", src)
        # The diagnostic must be observation-only labeled.
        self.assertIn("no_decision_change", src)

    def test_readiness_is_telemetry_event(self):
        self.assertIn('"unsplittable_winner_readiness"', self.src)
        self.assertIn('"joint_addon_sizing_diagnostic"', self.src)

    def test_no_f50_fraction_change(self):
        # F50 floor fraction default unchanged (0.50) in defensive_scaling.
        ds = Path("defensive_scaling.py").read_text(encoding="utf-8")
        self.assertIn("floor_fraction=0.50", ds)


if __name__ == "__main__":
    unittest.main()
