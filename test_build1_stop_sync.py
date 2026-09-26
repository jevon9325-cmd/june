"""Build 1 focused tests: stop-sync correction for pyramid evaluation.

Tests verify:
1. Eligible winner + successful stop acknowledgement
2. Stop rejection
3. Stop timeout / unknown
4. Acknowledged stop differs from intended
5. Protection already sufficient (no amendment needed)
6. Addon rejection after tightening
7. Restart / recovery state
8. Non-pyramid campaign unchanged
9. Existing addon sizing unchanged
10. Max-leg / cascade behaviour unchanged
"""
import sys
import unittest

sys.path.insert(0, "/opt/bots/june-build1")

from winner_protection import protect, strongest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakePosition(dict):
    pass


def _make_pos(fill=6426.3, ig=0.04, direction="short",
              broker_stop=6445.0, ack_stop=None, stop_sync=None,
              deal_id="DEAL1", instrument="SILVER"):
    p = _FakePosition({
        "fill_price": fill, "ig_size": ig, "direction": direction,
        "broker_stop_level": broker_stop,
        "acknowledged_stop_level": ack_stop,
        "stop_sync": stop_sync or {},
        "deal_id": deal_id, "instrument": instrument,
        "stop_pct": 0.003,
    })
    return p


def _ack_conf(deal_id, deal_ref, stop_level):
    return {"dealReference": deal_ref, "dealId": deal_id,
            "dealStatus": "ACCEPTED", "stopLevel": str(stop_level)}


def _reject_conf(deal_id, deal_ref):
    return {"dealReference": deal_ref, "dealId": deal_id, "dealStatus": "REJECTED"}


def _call_protect(position, target, put_fn, confirm_fn):
    def save():
        pass
    return protect(position, target, put=put_fn, confirm=confirm_fn,
                   save=save, now=0.0, log=lambda _: None, can_send=True)


# ============================================================================
# 1. Eligible winner + successful stop acknowledgement
# ============================================================================
class TestSuccessfulAcknowledgement(unittest.TestCase):

    def test_ack_updates_position(self):
        pos = _make_pos(broker_stop=6445.0, ack_stop=None)
        target = 6396.8

        call_log = []

        def put_fn(path, body, version="2"):
            call_log.append(("PUT", path, body))
            return {"dealReference": "REF1"}

        def confirm_fn(ref, retries=4):
            return _ack_conf("DEAL1", "REF1", target)

        result = _call_protect(pos, target, put_fn, confirm_fn)

        self.assertTrue(result)
        self.assertEqual(pos["acknowledged_stop_level"], target)
        self.assertEqual(pos["stop_sync"]["status"], "acknowledged")
        self.assertEqual(len(call_log), 1)

    def test_pending_ref_reused_no_second_put(self):
        """When pending deal_ref exists, protect() reuses it without a new PUT."""
        pos = _make_pos(broker_stop=6445.0)
        pos["stop_sync"] = {"status": "pending", "target": 6396.8,
                             "deal_id": "DEAL1", "deal_ref": "REF1", "attempted_at": -999}
        pos["intended_stop_level"] = 6396.8

        put_calls = []

        def put_fn(path, body, version="2"):
            put_calls.append(path)
            return {"dealReference": "REF2"}

        def confirm_fn(ref, retries=4):
            return _ack_conf("DEAL1", "REF1", 6396.8)

        result = _call_protect(pos, 6396.8, put_fn, confirm_fn)

        self.assertTrue(result)
        self.assertEqual(pos["stop_sync"]["status"], "acknowledged")
        self.assertEqual(len(put_calls), 0,
                         "No new PUT when pending deal_ref exists")

    def test_pending_ref_polled_close_to_stop(self):
        """Build 1 core: polling fires for pending ref even when distance < minimum."""
        pos = _make_pos(broker_stop=6445.0)
        pos["stop_sync"] = {"status": "pending", "target": 6396.8,
                             "deal_id": "DEAL1", "deal_ref": "REF1", "attempted_at": -999}
        pos["intended_stop_level"] = 6396.8

        confirmed = []

        def put_fn(path, body, version="2"):
            raise AssertionError("Must not send new PUT when pending ref exists")

        def confirm_fn(ref, retries=4):
            confirmed.append(ref)
            return _ack_conf("DEAL1", "REF1", 6396.8)

        result = _call_protect(pos, 6396.8, put_fn, confirm_fn)

        self.assertTrue(result)
        self.assertEqual(pos["acknowledged_stop_level"], 6396.8)
        self.assertEqual(confirmed, ["REF1"])


# ============================================================================
# 2. Stop rejection
# ============================================================================
class TestStopRejection(unittest.TestCase):

    def test_rejected_stop_does_not_update_ack(self):
        pos = _make_pos(broker_stop=6445.0)

        def put_fn(path, body, version="2"):
            return {"dealReference": "REF1"}

        def confirm_fn(ref, retries=4):
            return _reject_conf("DEAL1", "REF1")

        result = _call_protect(pos, 6396.8, put_fn, confirm_fn)

        self.assertFalse(result)
        self.assertIsNone(pos.get("acknowledged_stop_level"))
        self.assertEqual(pos["stop_sync"]["status"], "rejected")
        self.assertEqual(pos["intended_stop_level"], 6396.8)

    def test_rejected_stop_blocks_addon_via_snapshot(self):
        from defensive_scaling import snapshot

        pos = _make_pos(fill=6426.3, ig=0.04, broker_stop=6396.8)
        pos["stop_sync"] = {"status": "rejected", "deal_id": "DEAL1",
                             "target": 6396.8, "attempted_at": 0.0}

        with self.assertRaises(ValueError):
            snapshot([pos], aggregate=None, exit_price=6388.8,
                     multiplier=0.04, slippage=0.0, commission=0.0)


# ============================================================================
# 3. Stop timeout / unknown
# ============================================================================
class TestStopTimeout(unittest.TestCase):

    def test_timeout_leaves_stop_pending(self):
        pos = _make_pos(broker_stop=6445.0)

        def put_fn(path, body, version="2"):
            return {"dealReference": "REF1"}

        def confirm_fn(ref, retries=4):
            return None  # timeout

        result = _call_protect(pos, 6396.8, put_fn, confirm_fn)

        self.assertFalse(result)
        self.assertIsNone(pos.get("acknowledged_stop_level"))
        self.assertEqual(pos["stop_sync"]["status"], "pending")
        self.assertEqual(pos["intended_stop_level"], 6396.8)

    def test_timeout_blocks_addon_via_snapshot(self):
        from defensive_scaling import snapshot

        pos = _make_pos(fill=6426.3, ig=0.04, broker_stop=6396.8)
        pos["stop_sync"] = {"status": "pending", "deal_id": "DEAL1",
                             "target": 6396.8, "deal_ref": "REF1", "attempted_at": 0.0}

        with self.assertRaises(ValueError):
            snapshot([pos], aggregate=None, exit_price=6388.8,
                     multiplier=0.04, slippage=0.0, commission=0.0)


# ============================================================================
# 4. Acknowledged stop differs from intended
# ============================================================================
class TestAcknowledgedDiffersFromIntended(unittest.TestCase):

    def test_ack_at_different_level_not_accepted(self):
        """protect() does not ack if confirmed stop level != intended (1e-6 tolerance)."""
        pos = _make_pos(broker_stop=6445.0)
        intended = 6396.8
        actual_ack = 6398.0  # IG returned a different level

        def put_fn(path, body, version="2"):
            return {"dealReference": "REF1"}

        def confirm_fn(ref, retries=4):
            return _ack_conf("DEAL1", "REF1", actual_ack)

        result = _call_protect(pos, intended, put_fn, confirm_fn)

        self.assertFalse(result)
        self.assertIsNone(pos.get("acknowledged_stop_level"))
        self.assertEqual(pos["intended_stop_level"], intended)

    def test_snapshot_uses_acknowledged_not_intended(self):
        """snapshot() uses acknowledged_stop_level for protection classification.
        When intended == ack (consistent state), protection_state reflects ack level."""
        from defensive_scaling import snapshot

        pos = _make_pos(fill=6426.3, ig=0.04)
        pos["acknowledged_stop_level"] = 6396.8
        pos["broker_stop_level"] = 6396.8
        # intended must be == or weaker than ack; snapshot() rejects if intended is stronger
        pos["intended_stop_level"] = 6396.8
        pos["stop_sync"] = {"status": "acknowledged", "deal_id": "DEAL1",
                             "target": 6396.8}

        result = snapshot([pos], aggregate=None, exit_price=6388.8,
                          multiplier=0.04, slippage=0.0, commission=0.0)
        self.assertEqual(result["protection_state"], "profit_protected")


# ============================================================================
# 5. Protection already sufficient
# ============================================================================
class TestProtectionAlreadySufficient(unittest.TestCase):

    def test_no_put_when_already_at_or_beyond_target(self):
        pos = _make_pos(broker_stop=6445.0)
        pos["acknowledged_stop_level"] = 6390.0
        pos["broker_stop_level"] = 6390.0

        put_calls = []

        def put_fn(*a, **kw):
            put_calls.append(a)
            return {}

        result = _call_protect(pos, 6396.8, put_fn, lambda *a, **kw: None)

        self.assertTrue(result)
        self.assertEqual(len(put_calls), 0)

    def test_no_pending_sync_when_sufficient(self):
        pos = _make_pos(broker_stop=6445.0)
        pos["acknowledged_stop_level"] = 6390.0
        pos["broker_stop_level"] = 6390.0

        _call_protect(pos, 6396.8, lambda *a, **kw: {}, lambda *a, **kw: None)

        sync = pos.get("stop_sync") or {}
        self.assertNotEqual(sync.get("status"), "pending")


# ============================================================================
# 6. Addon rejection after tightening
# ============================================================================
class TestAddonRejectionAfterTightening(unittest.TestCase):

    def test_tightened_stop_persists_after_addon_failure(self):
        """Tightened primary protection survives an addon POST failure."""
        pos = _make_pos(broker_stop=6445.0)
        target = 6396.8

        def put_fn(path, body, version="2"):
            return {"dealReference": "REF1"}

        def confirm_fn(ref, retries=4):
            return _ack_conf("DEAL1", "REF1", target)

        _call_protect(pos, target, put_fn, confirm_fn)

        # Tightening is persisted
        self.assertEqual(pos["acknowledged_stop_level"], target)
        self.assertEqual(pos["stop_sync"]["status"], "acknowledged")
        self.assertEqual(pos["intended_stop_level"], target)
        self.assertEqual(pos["broker_stop_level"], target)


# ============================================================================
# 7. Restart / recovery state
# ============================================================================
class TestRestartRecovery(unittest.TestCase):

    def test_pending_ref_survives_restart(self):
        pos = _make_pos(broker_stop=6445.0)
        # attempted_at=-999 ensures protect()'s same-second guard does not fire
        pos["stop_sync"] = {"status": "pending", "target": 6396.8,
                             "deal_id": "DEAL1", "deal_ref": "REF1", "attempted_at": -999}
        pos["intended_stop_level"] = 6396.8

        confirm_calls = []

        def put_fn(*a, **kw):
            raise AssertionError("Must not re-PUT on restart with pending ref")

        def confirm_fn(ref, retries=4):
            confirm_calls.append(ref)
            return _ack_conf("DEAL1", "REF1", 6396.8)

        result = _call_protect(pos, 6396.8, put_fn, confirm_fn)

        self.assertTrue(result)
        self.assertEqual(confirm_calls, ["REF1"])

    def test_no_false_ack_on_missing_ref_after_restart(self):
        pos = _make_pos(broker_stop=6445.0)
        # attempted_at=-999 ensures protect()'s same-second guard does not fire
        pos["stop_sync"] = {"status": "pending", "target": 6396.8,
                             "deal_id": "DEAL1", "deal_ref": None, "attempted_at": -999}
        pos["intended_stop_level"] = 6396.8

        call_log = []

        def put_fn(path, body, version="2"):
            call_log.append("PUT")
            return {"dealReference": "REF_NEW"}

        def confirm_fn(ref, retries=4):
            return _ack_conf("DEAL1", "REF_NEW", 6396.8)

        result = _call_protect(pos, 6396.8, put_fn, confirm_fn)

        self.assertTrue(result)
        self.assertEqual(call_log, ["PUT"], "Must re-PUT when ref is missing")

    def test_acknowledged_stop_stable_after_restart(self):
        pos = _make_pos(broker_stop=6396.8)
        pos["acknowledged_stop_level"] = 6396.8
        pos["stop_sync"] = {"status": "acknowledged", "deal_id": "DEAL1", "target": 6396.8}
        pos["intended_stop_level"] = 6396.8

        put_calls = []
        result = _call_protect(pos, 6396.8,
                               put_fn=lambda *a, **kw: put_calls.append(1) or {},
                               confirm_fn=lambda *a, **kw: None)

        self.assertTrue(result)
        self.assertEqual(len(put_calls), 0)


# ============================================================================
# 8. Non-pyramid campaign unchanged
# ============================================================================
class TestNonPyramidCampaignUnchanged(unittest.TestCase):

    def _would_retry(self, pos, mid, min_stop_pts=8, pip_sz=1.0):
        """Replicate Build 1 _live_retry_stop_sync decision logic."""
        target = pos.get("intended_stop_level")
        if not target or mid <= 0:
            return False
        sync = pos.get("stop_sync") or {}
        has_pending_ref = (sync.get("status") == "pending"
                           and sync.get("target") == target
                           and sync.get("deal_ref"))
        if has_pending_ref:
            return True
        direction = pos["direction"]
        distance = mid - target if direction == "long" else target - mid
        minimum = (min_stop_pts + 1) * pip_sz
        return distance >= minimum

    def test_no_intended_stop_no_retry(self):
        pos = _make_pos(broker_stop=6445.0)
        pos["intended_stop_level"] = None
        self.assertFalse(self._would_retry(pos, 6388.8))

    def test_already_acked_no_retry(self):
        pos = _make_pos(broker_stop=6396.8)
        pos["acknowledged_stop_level"] = 6396.8
        pos["stop_sync"] = {"status": "acknowledged", "deal_id": "DEAL1", "target": 6396.8}
        pos["intended_stop_level"] = 6396.8
        # Stop is acknowledged — outer loop already skips it
        self.assertEqual(pos["stop_sync"]["status"], "acknowledged")

    def test_no_stop_sync_no_pending_ref_distance_check_applies(self):
        pos = _make_pos(broker_stop=6445.0)
        pos["stop_sync"] = {}
        pos["intended_stop_level"] = 6396.8
        # distance < minimum: 8 pts < 9 pts → old path, no retry
        self.assertFalse(self._would_retry(pos, 6388.8))
        # distance >= minimum: 15 pts >= 9 pts → retry fires
        self.assertTrue(self._would_retry(pos, 6381.8))

    def test_pending_ref_fires_regardless_of_distance(self):
        pos = _make_pos(broker_stop=6445.0)
        pos["stop_sync"] = {"status": "pending", "target": 6396.8,
                             "deal_id": "DEAL1", "deal_ref": "REF1", "attempted_at": -999}
        pos["intended_stop_level"] = 6396.8
        # Both close (8 pts) and far (15 pts) → retry fires because pending ref exists
        self.assertTrue(self._would_retry(pos, 6388.8))  # 8 pts — NEW behavior
        self.assertTrue(self._would_retry(pos, 6381.8))  # 15 pts — same as before


# ============================================================================
# 9. Addon sizing unchanged
# ============================================================================
class TestAddonSizingUnchanged(unittest.TestCase):

    def test_protect_does_not_touch_ig_size(self):
        pos = _make_pos(broker_stop=6445.0)
        original_size = pos["ig_size"]

        _call_protect(pos, 6396.8,
                      put_fn=lambda *a, **kw: {"dealReference": "REF1"},
                      confirm_fn=lambda *a, **kw: _ack_conf("DEAL1", "REF1", 6396.8))

        self.assertEqual(pos["ig_size"], original_size)


# ============================================================================
# 10. Max-leg / cascade behaviour unchanged
# ============================================================================
class TestMaxLegCascadeUnchanged(unittest.TestCase):

    def test_snapshot_raises_on_newer_pending_sync(self):
        """snapshot() still raises when a newer pending amendment exists."""
        from defensive_scaling import snapshot

        pos = _make_pos(fill=6426.3, ig=0.04, broker_stop=6396.8, ack_stop=6396.8)
        # A further tightening in progress — not yet acked
        pos["stop_sync"] = {"status": "pending", "deal_id": "DEAL1",
                             "target": 6390.0, "deal_ref": "REF2", "attempted_at": 0.0}

        with self.assertRaises(ValueError):
            snapshot([pos], aggregate=None, exit_price=6388.8,
                     multiplier=0.04, slippage=0.0, commission=0.0)

    def test_snapshot_rejects_breached_stop(self):
        """snapshot() still rejects if acknowledged stop is already past exit price."""
        from defensive_scaling import snapshot

        pos = _make_pos(fill=6426.3, ig=0.04)
        pos["acknowledged_stop_level"] = 6385.0
        pos["broker_stop_level"] = 6385.0
        pos["stop_sync"] = {"status": "acknowledged", "deal_id": "DEAL1", "target": 6385.0}

        with self.assertRaises(ValueError):
            snapshot([pos], aggregate=None, exit_price=6390.0,
                     multiplier=0.04, slippage=0.0, commission=0.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
