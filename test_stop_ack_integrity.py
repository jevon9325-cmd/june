"""Broker stop-protection acknowledgement integrity tests
(repair/stop-ack-integrity-f629553).

Covers the four live failed-acknowledgement shapes, the full idempotency/race
matrix, the broker position-snapshot reconciliation path, and the F50 integration
cases A-D. Fail-closed broker truth is asserted throughout: a software/intended
floor is never certified as acknowledged.
"""
import json
import unittest
from unittest.mock import Mock

from winner_protection import (protect, effective, strongest, reply_targets_deal,
                               reconcile_broker_stop, covers)
from defensive_scaling import snapshot


# ───────────────────────────── helpers ──────────────────────────────────────
def position(direction="long", **over):
    p = dict(deal_id="POS1", direction=direction, fill_price=100., ig_size=1.,
             stop_pct=.01, broker_stop_level=99. if direction == "long" else 101.)
    p.update(over)
    return p


def accepted_reply(ref="stop-ref", *, deal_id=None, affected=None, stop=102., status="ACCEPTED"):
    """An IG /confirms reply. By default the top-level dealId is the AMENDMENT's
    own id (not the position), and the position appears in affectedDeals — the
    real shape that broke the old matcher."""
    r = dict(dealStatus=status, dealReference=ref, stopLevel=stop)
    r["dealId"] = deal_id if deal_id is not None else "AMEND-XYZ"
    if affected is not None:
        r["affectedDeals"] = [{"dealId": d, "status": "AMENDED"} for d in affected]
    return r


def run_protect(pos, reply, *, put=None, now=1, target=None):
    put = put or Mock(return_value={"dealReference": "stop-ref"})
    confirm = Mock(return_value=reply)
    tgt = target if target is not None else (102. if pos["direction"] == "long" else 98.)
    ok = protect(pos, tgt, put=put, confirm=confirm, save=Mock(), now=now, log=Mock())
    return ok, put, confirm


# ═══════════════ PART A: confirm identity via affectedDeals ═════════════════
class ConfirmIdentityTests(unittest.TestCase):
    def test_reply_targets_deal_top_level(self):
        self.assertTrue(reply_targets_deal({"dealId": "POS1"}, "POS1"))

    def test_reply_targets_deal_via_affected(self):
        self.assertTrue(reply_targets_deal(
            {"dealId": "AMEND-XYZ", "affectedDeals": [{"dealId": "POS1"}]}, "POS1"))

    def test_reply_targets_deal_rejects_unrelated(self):
        self.assertFalse(reply_targets_deal(
            {"dealId": "AMEND-XYZ", "affectedDeals": [{"dealId": "OTHER"}]}, "POS1"))
        self.assertFalse(reply_targets_deal({}, "POS1"))
        self.assertFalse(reply_targets_deal({"dealId": "POS1"}, ""))

    def test_amendment_confirm_acknowledges_via_affected_deals(self):
        # THE live bug: amendment dealId != position id; position in affectedDeals.
        pos = position("short")
        ok, _, _ = run_protect(pos, accepted_reply(
            deal_id="DIAAAAR9B5SK7AF", affected=["POS1"], stop=98.))
        self.assertTrue(ok)
        self.assertEqual(pos["stop_sync"]["status"], "acknowledged")
        self.assertEqual(pos["acknowledged_stop_level"], 98.)
        self.assertEqual(pos["broker_stop_level"], 98.)

    def test_top_level_dealid_still_acknowledges(self):
        # Backward compatible: a reply echoing the position id at top level works.
        pos = position("long")
        ok, _, _ = run_protect(pos, accepted_reply(deal_id="POS1", stop=102.))
        self.assertTrue(ok)
        self.assertEqual(pos["acknowledged_stop_level"], 102.)

    def test_affected_other_deal_does_not_acknowledge(self):
        pos = position("long")
        ok, _, _ = run_protect(pos, accepted_reply(
            deal_id="AMEND-XYZ", affected=["SOMEONE_ELSE"], stop=102.))
        self.assertFalse(ok)
        self.assertEqual(pos["broker_stop_level"], 99.)
        self.assertEqual(pos["stop_sync"]["status"], "pending")

    def test_wrong_reference_does_not_acknowledge(self):
        pos = position("long")
        ok, _, _ = run_protect(pos, accepted_reply(
            ref="OLD-REF", deal_id="AMEND-XYZ", affected=["POS1"], stop=102.))
        self.assertFalse(ok)
        self.assertEqual(pos["broker_stop_level"], 99.)

    def test_rejected_amendment_marks_rejected(self):
        pos = position("long")
        ok, _, _ = run_protect(pos, accepted_reply(
            deal_id="AMEND-XYZ", affected=["POS1"], status="REJECTED"))
        self.assertFalse(ok)
        self.assertEqual(pos["stop_sync"]["status"], "rejected")
        self.assertEqual(pos["broker_stop_level"], 99.)

    def test_wrong_stop_level_does_not_acknowledge(self):
        pos = position("long")
        ok, _, _ = run_protect(pos, accepted_reply(
            deal_id="AMEND-XYZ", affected=["POS1"], stop=101.0))  # != target 102
        self.assertFalse(ok)
        self.assertEqual(pos["broker_stop_level"], 99.)


# ═══════════════ The four live failed-ack campaign shapes ═══════════════════
class LiveFailedAckShapes(unittest.TestCase):
    def _amend(self, pos, target, amend_id, pos_id, now):
        return run_protect(pos, accepted_reply(deal_id=amend_id, affected=[pos_id], stop=target),
                           target=target, now=now)

    def test_R9B49TWBB_oil_short_sequence(self):
        pos = position("short", deal_id="R9B49TWBB", fill_price=9710.3,
                       broker_stop_level=9710.3, stop_pct=None)
        # five accepted amendments ratcheting down (stronger for a short)
        for i, (tgt, amend) in enumerate([(9686.19928, "A1"), (9682.59914, "A2"),
                                          (9678.45052, "A3"), (9677.50029, "A4"),
                                          (9674.14945, "A5")], start=1):
            ok, _, _ = self._amend(pos, tgt, amend, "R9B49TWBB", now=i)
            self.assertTrue(ok, f"amendment {amend} should acknowledge")
        self.assertEqual(pos["acknowledged_stop_level"], 9674.14945)
        self.assertEqual(pos["stop_sync"]["status"], "acknowledged")

    def test_R9D8A47AQ_cocoa_short_sequence(self):
        pos = position("short", deal_id="R9D8A47AQ", fill_price=3990.3,
                       broker_stop_level=3990.3, stop_pct=None)
        for i, (tgt, amend) in enumerate([(3969.35046, "C1"), (3959.59995, "C2")], start=1):
            ok, _, _ = self._amend(pos, tgt, amend, "R9D8A47AQ", now=i)
            self.assertTrue(ok)
        self.assertEqual(pos["acknowledged_stop_level"], 3959.59995)


# ═══════════════ PART B: broker position-snapshot reconciliation ════════════
class SnapshotReconcileTests(unittest.TestCase):
    def pending(self, direction="short", target=98., deal_id="POS1"):
        p = position(direction, deal_id=deal_id,
                     broker_stop_level=101. if direction == "short" else 99.)
        p["intended_stop_level"] = target
        p["stop_sync"] = {"status": "pending", "target": target, "deal_id": deal_id,
                          "deal_ref": "ref"}
        return p

    def test_broker_snapshot_exact_match_acknowledges(self):
        p = self.pending()
        promoted = reconcile_broker_stop(p, 98., "POS1", now=5, log=Mock())
        self.assertTrue(promoted)
        self.assertEqual(p["stop_sync"]["status"], "acknowledged")
        self.assertEqual(p["acknowledged_stop_level"], 98.)
        self.assertEqual(p["stop_sync"]["acknowledged_via"], "broker_position_snapshot")

    def test_broker_snapshot_stronger_acknowledges_broker_level(self):
        # broker holds a STRONGER stop than requested (contract C): acknowledge
        # the broker-supported (stronger) level, which satisfies the request.
        p = self.pending(target=98.)
        promoted = reconcile_broker_stop(p, 97., "POS1", now=5)  # 97 stronger for short
        self.assertTrue(promoted)
        self.assertEqual(p["acknowledged_stop_level"], 97.)

    def test_broker_snapshot_weaker_does_not_acknowledge(self):
        # broker stop WEAKER than requested -> cannot certify the request.
        p = self.pending(target=98.)
        promoted = reconcile_broker_stop(p, 99.5, "POS1", now=5)  # weaker for short
        self.assertFalse(promoted)
        self.assertEqual(p["stop_sync"]["status"], "pending")

    def test_software_advanced_beyond_broker_only_broker_credited(self):
        # Contract D: software/intended floor advanced beyond the broker stop.
        # Only the broker-supported portion is acknowledged.
        p = self.pending(target=98.)
        p["intended_stop_level"] = 96.  # software has advanced further
        p["defensive_soft_sl"] = 96.
        p["stop_sync"]["target"] = 98.  # the actually-requested broker amendment
        promoted = reconcile_broker_stop(p, 98., "POS1", now=5)
        self.assertTrue(promoted)
        self.assertEqual(p["acknowledged_stop_level"], 98.)   # NOT 96 (software)
        self.assertEqual(effective(p), 96.)  # intended still carried as software floor

    def test_wrong_deal_id_no_reconcile(self):
        p = self.pending(deal_id="POS1")
        self.assertFalse(reconcile_broker_stop(p, 98., "OTHERDEAL", now=5))
        self.assertEqual(p["stop_sync"]["status"], "pending")

    def test_no_broker_stop_no_reconcile(self):
        p = self.pending()
        self.assertFalse(reconcile_broker_stop(p, None, "POS1", now=5))

    def test_no_pending_request_no_reconcile(self):
        p = position("short", deal_id="POS1")
        p.pop("stop_sync", None)
        p.pop("intended_stop_level", None)
        self.assertFalse(reconcile_broker_stop(p, 98., "POS1", now=5))

    def test_idempotent_second_call_noop(self):
        p = self.pending()
        reconcile_broker_stop(p, 98., "POS1", now=5)
        ack_before = p["acknowledged_stop_level"]
        # second identical call: already acknowledged -> no change / stable level
        again = reconcile_broker_stop(p, 98., "POS1", now=6)
        self.assertTrue(again)  # still covers; stays acknowledged
        self.assertEqual(p["acknowledged_stop_level"], ack_before)

    def test_restart_pending_then_snapshot(self):
        p = self.pending()
        p = json.loads(json.dumps(p))  # simulate restart round-trip
        self.assertEqual(p["stop_sync"]["status"], "pending")
        promoted = reconcile_broker_stop(p, 98., "POS1", now=10)
        self.assertTrue(promoted)
        self.assertEqual(p["stop_sync"]["status"], "acknowledged")


# ═══════════════ covers() normalization ═════════════════════════════════════
class CoversTests(unittest.TestCase):
    def test_covers_equal_within_tol(self):
        self.assertTrue(covers("long", 100.0000001, 100.0))

    def test_covers_stronger(self):
        self.assertTrue(covers("long", 101., 100.))   # higher = stronger for long
        self.assertTrue(covers("short", 99., 100.))   # lower = stronger for short

    def test_covers_weaker(self):
        self.assertFalse(covers("long", 99., 100.))
        self.assertFalse(covers("short", 101., 100.))


# ═══════════════ PART C: F50 integration (defensive_scaling.snapshot) ════════
def leg_for_f50(direction="short", *, ack, intended, sync_status="acknowledged",
                sync_deal="POS1", deal_id="POS1", fill=9710.3, qty=0.03):
    leg = dict(deal_id=deal_id, direction=direction, instrument="OIL",
               fill_price=fill, ig_size=qty, stop_pct=None,
               broker_stop_level=ack, acknowledged_stop_level=ack,
               intended_stop_level=intended)
    if sync_status is not None:
        leg["stop_sync"] = {"status": sync_status, "deal_id": sync_deal,
                            "target": intended}
    return leg


# exit_price must be beyond (more favorable than) the acknowledged stop so the
# position is not treated as already-breached. Short entered 9710.3: current 9670
# is below the profit-locking stop (9674) and the weaker-floor case (9690).
F50_KW = dict(aggregate=None, exit_price=9670.0, multiplier=1.0,
              slippage=2.0, commission=0.0)


class F50IntegrationTests(unittest.TestCase):
    def test_case_A_software_only_blocked(self):
        # Software/intended protection exists but NO broker acknowledgement.
        leg = leg_for_f50(ack=9710.3, intended=9674.0, sync_status="pending")
        # ack present (entry stop) but sync unresolved -> blocked.
        with self.assertRaises(ValueError) as ctx:
            snapshot([leg], **F50_KW)
        self.assertIn("protection synchronization unresolved", str(ctx.exception))

    def test_case_A_no_ack_level_blocked(self):
        leg = leg_for_f50(ack=9710.3, intended=9674.0, sync_status="acknowledged")
        leg["broker_stop_level"] = None
        leg["acknowledged_stop_level"] = None
        with self.assertRaises(ValueError) as ctx:
            snapshot([leg], **F50_KW)
        self.assertIn("missing acknowledged protection", str(ctx.exception))

    def test_case_B_broker_confirmed_sync_clears(self):
        # Broker authoritatively confirms matching protection -> snapshot succeeds.
        leg = leg_for_f50(ack=9674.0, intended=9674.0, sync_status="acknowledged")
        ev = snapshot([leg], **F50_KW)
        self.assertEqual(ev["protection_state"] in
                         ("profit_protected", "breakeven_protected", "unprotected"), True)
        self.assertEqual(len(ev["protection"]), 1)
        self.assertEqual(ev["protection"][0]["acknowledged"], 9674.0)

    def test_case_C_broker_weaker_than_software_rejected(self):
        # Broker confirms only an older/weaker floor while software advanced.
        # snapshot must refuse to credit the stronger unsupported floor.
        leg = leg_for_f50(ack=9690.0, intended=9674.0, sync_status="acknowledged")
        # short: intended 9674 is STRONGER than ack 9690 -> intended exceeds ack.
        with self.assertRaises(ValueError) as ctx:
            snapshot([leg], **F50_KW)
        self.assertIn("intended floor exceeds acknowledged protection", str(ctx.exception))

    def test_case_C_credits_only_broker_level(self):
        # When intended == ack (software not beyond broker), economics use the
        # broker-acknowledged level exactly.
        leg = leg_for_f50(ack=9690.0, intended=9690.0, sync_status="acknowledged")
        ev = snapshot([leg], **F50_KW)
        self.assertEqual(ev["protection"][0]["acknowledged"], 9690.0)
        # liquidation uses ack (9690), not a stronger software floor
        sign = -1  # short
        expected = sign * (9690.0 - 9710.3) * 0.03 - (0.03 * 2.0)
        self.assertAlmostEqual(ev["liquidation_before"], expected, places=6)

    def test_case_D_requested_or_stronger_proceeds(self):
        # Broker confirms requested-or-stronger protection and it is profit-
        # protected -> snapshot returns a usable evidence object (next gate).
        leg = leg_for_f50(ack=9674.0, intended=9674.0, sync_status="acknowledged",
                          fill=9710.3, qty=0.03)
        ev = snapshot([leg], **F50_KW)
        # short entered 9710.3, stop 9674.0 => locked-in gross before reserve
        self.assertGreater(ev["liquidation_before"], 0.0)
        self.assertEqual(ev["protection_state"], "profit_protected")

    def test_sync_deal_identity_mismatch_blocked(self):
        leg = leg_for_f50(ack=9674.0, intended=9674.0, sync_status="acknowledged",
                          sync_deal="DIFFERENT")
        with self.assertRaises(ValueError) as ctx:
            snapshot([leg], **F50_KW)
        self.assertIn("protection synchronization unresolved", str(ctx.exception))


# ═══════════════ Idempotency / race safety (enumerated) ═════════════════════
class IdempotencyRaceTests(unittest.TestCase):
    def test_accept_then_stronger_request(self):
        # (3)(4): accept A, then software requests stronger B; A stays acknowledged
        # as broker floor, B becomes intended/pending.
        pos = position("short", stop_pct=None, broker_stop_level=101.)
        ok, _, _ = run_protect(pos, accepted_reply(deal_id="A1", affected=["POS1"], stop=98.),
                               target=98.)
        self.assertTrue(ok)
        self.assertEqual(pos["acknowledged_stop_level"], 98.)
        # stronger request B=97 whose confirm is lost (put ok, confirm None)
        ok2, _, _ = run_protect(pos, None, target=97., now=2)
        self.assertFalse(ok2)
        self.assertEqual(pos["acknowledged_stop_level"], 98.)  # A retained
        self.assertEqual(pos["intended_stop_level"], 97.)      # B intended
        self.assertEqual(effective(pos), 97.)

    def test_duplicate_confirmation_delivery(self):
        # (6): second identical accepted confirm -> still acknowledged, no second PUT.
        pos = position("long", stop_pct=None)
        reply = accepted_reply(deal_id="A1", affected=["POS1"], stop=102.)
        run_protect(pos, reply)
        ok, put, confirm = run_protect(pos, reply, now=2)
        self.assertTrue(ok)
        put.assert_not_called()
        confirm.assert_not_called()

    def test_retry_after_accepted_no_duplicate_amendment(self):
        # (8): once acknowledged, protect() early-returns without PUT/confirm.
        pos = position("short", stop_pct=None, broker_stop_level=101.)
        run_protect(pos, accepted_reply(deal_id="A1", affected=["POS1"], stop=98.), target=98.)
        put = Mock(); confirm = Mock()
        ok = protect(pos, 98., put=put, confirm=confirm, save=Mock(), now=9, log=Mock())
        self.assertTrue(ok)
        put.assert_not_called()
        confirm.assert_not_called()

    def test_amendment_rejected_then_snapshot_shows_broker_has_it(self):
        # (2)(18->reconcile): confirm lost, broker snapshot proves the stop.
        pos = position("short", stop_pct=None, broker_stop_level=101.)
        run_protect(pos, None, target=98., now=1)  # confirm lost -> pending
        self.assertEqual(pos["stop_sync"]["status"], "pending")
        promoted = reconcile_broker_stop(pos, 98., "POS1", now=2)
        self.assertTrue(promoted)
        self.assertEqual(pos["acknowledged_stop_level"], 98.)

    def test_broker_gone_not_manufactured(self):
        # (12): reconcile only runs on an open-deal snapshot; absence => caller
        # does not call reconcile (no stop level), so no acknowledgement arises.
        pos = position("short", stop_pct=None)
        pos["intended_stop_level"] = 98.
        pos["stop_sync"] = {"status": "pending", "target": 98., "deal_id": "POS1"}
        # No broker stop level available (position gone) -> no promotion.
        self.assertFalse(reconcile_broker_stop(pos, None, "POS1", now=2))

    def test_float_normalized_equivalent_stop(self):
        # (16): broker reports a float-equivalent stop within tolerance.
        pos = position("long", stop_pct=None)
        pos["intended_stop_level"] = 102.0
        pos["stop_sync"] = {"status": "pending", "target": 102.0, "deal_id": "POS1"}
        self.assertTrue(reconcile_broker_stop(pos, 102.0000004, "POS1", now=2))


# ═══════════════ Source wiring: Part B is reachable in june.py ══════════════
class WiringTests(unittest.TestCase):
    def test_reconcile_positions_invokes_broker_stop_reconcile(self):
        # Prove _live_reconcile_positions wires the broker-position-snapshot
        # acknowledgement path (reconcile_broker_stop) using the broker row's
        # stopLevel, so the fix is actually reachable at runtime (startup +
        # post-failed-close), not only unit-tested in isolation.
        from pathlib import Path
        src = Path("june.py").read_text(encoding="utf-8")
        self.assertIn("reconcile_broker_stop", src)
        self.assertIn('matches[0].get("stopLevel")', src)
        # It must live inside the reconcile-positions routine: the function must
        # import reconcile_broker_stop and invoke the resulting binding there.
        import ast
        tree = ast.parse(src)
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef) and n.name == "_live_reconcile_positions")
        imported = {}
        for imp in ast.walk(fn):
            if isinstance(imp, ast.ImportFrom):
                for a in imp.names:
                    if a.name == "reconcile_broker_stop":
                        imported[a.name] = a.asname or a.name
        self.assertTrue(imported, "reconcile_broker_stop not imported inside _live_reconcile_positions")
        bound = next(iter(imported.values()))
        called = {n.func.id for n in ast.walk(fn)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        self.assertIn(bound, called, f"{bound} imported but not called")

    def test_protect_matching_uses_reply_targets_deal(self):
        from pathlib import Path
        src = Path("winner_protection.py").read_text(encoding="utf-8")
        self.assertIn("reply_targets_deal(reply, record[\"deal_id\"])", src)
        # The old brittle direct-equality predicate must be gone from protect().
        self.assertNotIn('reply.get("dealId") == record["deal_id"]', src)


if __name__ == "__main__":
    unittest.main()
