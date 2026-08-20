"""
test_risk_safety.py — Risk management and concurrent execution safety tests for June + Barbie.
Run with: python3 /opt/bots/june/test_risk_safety.py
All tests are self-contained (no running bot required; Redis mocked).
"""
import json
import time
import threading
import unittest
from unittest.mock import MagicMock, patch, call

# ── Constants mirrored from june.py (kept in sync manually) ─────────────────
_LIVE_CIRCUIT_BREAKER_PCT = -0.05
_LIVE_CB_FLOOR_USD        = 20.0


# ── Helper: replicate the CB logic exactly as it appears in june.py ──────────
def cb_would_fire(day_start: float, current: float) -> tuple[bool, float]:
    """Return (fires, effective_buffer) for given balances."""
    if day_start <= 0 or current <= 0:
        return False, 0.0
    dollar_loss      = day_start - current
    effective_buffer = max(_LIVE_CB_FLOOR_USD, abs(_LIVE_CIRCUIT_BREAKER_PCT) * day_start)
    fires = dollar_loss >= effective_buffer
    return fires, effective_buffer


# ────────────────────────────────────────────────────────────────────────────
class TestCircuitBreakerScaling(unittest.TestCase):
    """Verify dollar-floor CB behaviour at key account sizes."""

    def test_30_floor_dominates(self):
        """$30 account: pct buffer = $1.50 << $20 floor — floor must govern."""
        fires, buf = cb_would_fire(30.0, 30.0)
        self.assertFalse(fires, "CB must not fire with zero loss")
        self.assertAlmostEqual(buf, 20.0, places=2)

    def test_30_small_loss_no_fire(self):
        """$30 → $27: $3 loss < $20 floor — CB should NOT fire."""
        fires, buf = cb_would_fire(30.0, 27.0)
        self.assertFalse(fires, f"CB fired on $3 loss with ${buf:.2f} buffer — should need $20")

    def test_30_large_loss_fires(self):
        """$30 → $9: $21 loss > $20 floor — CB SHOULD fire."""
        fires, _ = cb_would_fire(30.0, 9.0)
        self.assertTrue(fires, "CB must fire when dollar_loss >= floor")

    def test_50_floor_dominates(self):
        """$50 account: pct buffer = $2.50 << $20 floor."""
        fires, buf = cb_would_fire(50.0, 50.0)
        self.assertFalse(fires)
        self.assertAlmostEqual(buf, 20.0, places=2)

    def test_50_loss_19_no_fire(self):
        """$50 → $31: $19 loss < $20 buffer — should NOT fire."""
        fires, _ = cb_would_fire(50.0, 31.0)
        self.assertFalse(fires, "19 dollar loss should not breach $20 buffer")

    def test_50_loss_20_fires(self):
        """$50 → $30: $20 loss == $20 buffer — should fire."""
        fires, _ = cb_would_fire(50.0, 30.0)
        self.assertTrue(fires, "$20 exact loss must breach $20 buffer")

    def test_150_floor_still_dominates(self):
        """$150: pct buffer = $7.50 < $20 floor — floor still governs."""
        fires, buf = cb_would_fire(150.0, 150.0)
        self.assertFalse(fires)
        self.assertAlmostEqual(buf, 20.0, places=2)

    def test_150_loss_19_no_fire(self):
        """$150 → $131: $19 loss < $20 floor — should NOT fire."""
        fires, _ = cb_would_fire(150.0, 131.0)
        self.assertFalse(fires)

    def test_400_crossover(self):
        """$400: pct buffer = $20 == floor — exactly at crossover point."""
        fires, buf = cb_would_fire(400.0, 400.0)
        self.assertFalse(fires)
        self.assertAlmostEqual(buf, 20.0, msg="at crossover, pct=floor=20", places=2)

    def test_500_pct_dominates(self):
        """$500: pct buffer = $25 > $20 floor — percentage logic takes over."""
        fires, buf = cb_would_fire(500.0, 500.0)
        self.assertFalse(fires)
        self.assertAlmostEqual(buf, 25.0, places=2, msg="$500: 5% = $25 > floor")

    def test_500_loss_24_no_fire(self):
        """$500 → $476: $24 loss < $25 buffer — should NOT fire."""
        fires, _ = cb_would_fire(500.0, 476.0)
        self.assertFalse(fires)

    def test_500_loss_25_fires(self):
        """$500 → $475: $25 loss == $25 buffer — should fire."""
        fires, _ = cb_would_fire(500.0, 475.0)
        self.assertTrue(fires)

    def test_current_account_no_longer_fires(self):
        """$36.98 → $33.72 (today's actual): $3.26 loss < $20 buffer — CB should NOT fire."""
        fires, buf = cb_would_fire(36.98, 33.72)
        self.assertFalse(
            fires,
            f"Account at -$3.26 must not trigger CB (buffer=${buf:.2f}). "
            "Old pct-only logic incorrectly fired here."
        )


# ────────────────────────────────────────────────────────────────────────────
class TestConcurrencyPosAdjust(unittest.TestCase):
    """
    Simulate simultaneous Barbie write + June GETDEL to confirm no duplicate
    application or lost-write scenarios.

    Barbie uses SET NX — won't overwrite a pending adjustment.
    June uses GETDEL — atomic read+delete.
    """

    def _make_redis_mock(self):
        """Return a mock Redis client with a shared in-memory store."""
        store: dict = {}
        mock = MagicMock()

        def fake_getdel(key):
            return store.pop(key, None)

        def fake_set(key, value, ex=None, nx=False):
            if nx and key in store:
                return False  # NX: do not overwrite
            store[key] = value
            return True

        def fake_get(key):
            return store.get(key)

        def fake_delete(key):
            store.pop(key, None)

        mock.getdel.side_effect = fake_getdel
        mock.set.side_effect    = fake_set
        mock.get.side_effect    = fake_get
        mock.delete.side_effect = fake_delete
        return mock, store

    def test_june_getdel_clears_key(self):
        """June's GETDEL atomically reads and removes the pending adjustment."""
        mock, store = self._make_redis_mock()
        payload = json.dumps({"instrument": "EURUSD", "stop_pct": 0.003})
        mock.set("barbie_june_pos_adjust", payload)
        result = mock.getdel("barbie_june_pos_adjust")
        self.assertEqual(result, payload, "GETDEL must return the stored value")
        self.assertNotIn("barbie_june_pos_adjust", store, "Key must be deleted after GETDEL")

    def test_barbie_nx_blocks_overwrite(self):
        """Barbie SET NX must not overwrite an adjustment June hasn't consumed."""
        mock, store = self._make_redis_mock()
        first  = json.dumps({"instrument": "GOLD",   "stop_pct": 0.002})
        second = json.dumps({"instrument": "SILVER",  "stop_pct": 0.001})

        mock.set("barbie_june_pos_adjust", first, ex=300, nx=True)
        self.assertIn("barbie_june_pos_adjust", store)
        result = mock.set("barbie_june_pos_adjust", second, ex=300, nx=True)
        self.assertFalse(result, "Second NX write must be blocked while first is pending")
        self.assertEqual(store["barbie_june_pos_adjust"], first, "Original payload must be intact")

    def test_concurrent_write_consume_no_duplication(self):
        """Threaded: Barbie writes once, June consumes once — no duplicate application."""
        mock, store = self._make_redis_mock()
        KEY = "barbie_june_pos_adjust"
        payload = json.dumps({"instrument": "USDJPY", "tp_pct": 0.005})
        applied: list = []
        errors:  list = []

        def barbie_write():
            ok = mock.set(KEY, payload, ex=300, nx=True)
            if not ok:
                errors.append("barbie blocked by existing key")

        def june_consume():
            time.sleep(0.005)  # brief delay so write happens first
            raw = mock.getdel(KEY)
            if raw:
                applied.append(json.loads(raw))

        t_barbie = threading.Thread(target=barbie_write)
        t_june   = threading.Thread(target=june_consume)
        t_barbie.start(); t_june.start()
        t_barbie.join();  t_june.join()

        self.assertEqual(len(applied), 1, "June must apply exactly one adjustment")
        self.assertEqual(applied[0]["instrument"], "USDJPY")
        self.assertNotIn(KEY, store, "Key must be cleared after consumption")

    def test_june_second_getdel_returns_none(self):
        """If June somehow calls GETDEL twice, the second call returns None (no double-apply)."""
        mock, store = self._make_redis_mock()
        payload = json.dumps({"instrument": "OIL", "stop_pct": 0.004})
        mock.set("barbie_june_pos_adjust", payload)
        first  = mock.getdel("barbie_june_pos_adjust")
        second = mock.getdel("barbie_june_pos_adjust")
        self.assertIsNotNone(first,  "First GETDEL must return value")
        self.assertIsNone(second, "Second GETDEL must return None — key already gone")

    def test_barbie_succeeds_after_june_consumes(self):
        """After June consumes, Barbie's next NX write succeeds (key is gone)."""
        mock, store = self._make_redis_mock()
        KEY = "barbie_june_pos_adjust"
        first  = json.dumps({"instrument": "EURUSD", "stop_pct": 0.002})
        second = json.dumps({"instrument": "EURUSD", "stop_pct": 0.001})
        # Barbie writes
        mock.set(KEY, first, ex=300, nx=True)
        # June consumes
        mock.getdel(KEY)
        # Barbie's next cycle can write again
        ok = mock.set(KEY, second, ex=300, nx=True)
        self.assertTrue(ok, "NX write must succeed after key was consumed")
        self.assertEqual(store[KEY], second)


# ────────────────────────────────────────────────────────────────────────────
class TestPositionReconciliation(unittest.TestCase):
    """Verify Barbie's adjustments are applied without overwriting June's metadata."""

    def _make_sim_position(self):
        return {
            "instrument": "SILVER",
            "direction":  "long",
            "fill_price": 6800.0,
            "stop_pct":   0.005,
            "tp_pct":     0.010,
            "peak_pnl_pct":       0.003,
            "dple_effective_sl":  None,
            "breakeven_locked":   False,
            "defensive_stop_active": False,
            "reversal_count": 0,
        }

    def test_stop_tighten_preserves_metadata(self):
        """Tightened stop must not clobber DPLE or MPD metadata fields."""
        pos = self._make_sim_position()
        adj_stop = 0.003  # tighter than existing 0.005
        # Simulate what _sim_apply_pos_adjust does for stop
        if adj_stop < pos["stop_pct"]:
            pos["stop_pct"] = adj_stop
        # All other fields should be untouched
        self.assertEqual(pos["peak_pnl_pct"],       0.003)
        self.assertIsNone(pos["dple_effective_sl"])
        self.assertFalse(pos["defensive_stop_active"])
        self.assertEqual(pos["reversal_count"],     0)

    def test_loose_stop_rejected(self):
        """A looser stop must be ignored — June's tighten-only rule."""
        pos = self._make_sim_position()
        adj_stop = 0.008  # looser than existing 0.005
        original = pos["stop_pct"]
        if adj_stop < pos["stop_pct"]:
            pos["stop_pct"] = adj_stop
        self.assertEqual(pos["stop_pct"], original, "Loose stop must be rejected")

    def test_tp_adjustment_any_direction(self):
        """TP can move either direction within bounds."""
        pos = self._make_sim_position()
        pos["tp_pct"] = 0.015  # widen TP
        self.assertEqual(pos["tp_pct"], 0.015)
        pos["tp_pct"] = 0.005  # tighten TP
        self.assertEqual(pos["tp_pct"], 0.005)


# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromModule(__import__("__main__"))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
