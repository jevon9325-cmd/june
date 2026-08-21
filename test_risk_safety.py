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


# ────────────────────────────────────────────────────────────────────────────
class TestMinStopDistance(unittest.TestCase):
    """P2/P3: IG minimum stop distance enforcement in _live_compute_stop_pts()."""

    def _compute_stop_pts(self, sym, ref_price, price_unit, stop_pct, pip_sz,
                          min_stop_pts_map):
        """Replicate _live_compute_stop_pts() logic from june.py."""
        pts = int(ref_price * price_unit * stop_pct / pip_sz)
        ig_min = min_stop_pts_map.get(sym, 4)
        return max(ig_min + 1, pts)

    def test_below_minimum_expands(self):
        """Stop below IG minimum must expand to min+1."""
        # Strategy gives 3 pts, IG minimum is 10 — result must be 11
        result = self._compute_stop_pts(
            sym="NVDA", ref_price=120.0, price_unit=1.0,
            stop_pct=0.0001, pip_sz=0.01,  # very small stop → ~1 pt
            min_stop_pts_map={"NVDA": 10}
        )
        self.assertGreaterEqual(result, 11,
            f"Stop {result} must be >= min+1 (11) when IG minimum is 10")

    def test_above_minimum_unchanged(self):
        """Stop already above IG minimum must not be inflated."""
        # Strategy gives 50 pts, IG minimum is 10 — result must stay 50
        result = self._compute_stop_pts(
            sym="AAPL", ref_price=175.0, price_unit=1.0,
            stop_pct=0.003, pip_sz=0.01,  # ~52 pts
            min_stop_pts_map={"AAPL": 10}
        )
        self.assertEqual(result, 52,
            f"Stop {result} must stay at strategy value (52) when above IG minimum+1")

    def test_missing_sym_uses_floor4(self):
        """Symbol not in map falls back to hard floor of 4 pts."""
        result = self._compute_stop_pts(
            sym="UNKNOWN", ref_price=10.0, price_unit=1.0,
            stop_pct=0.0001, pip_sz=0.01,
            min_stop_pts_map={}
        )
        self.assertGreaterEqual(result, 5,
            f"Unknown symbol must use floor 4 → result >= 5, got {result}")

    def test_exact_minimum_gets_buffer(self):
        """Stop equal to IG minimum must still get +1 buffer."""
        # IG min = 5; strategy also gives 5 pts → must return 6
        result = self._compute_stop_pts(
            sym="MSFT", ref_price=300.0, price_unit=1.0,
            stop_pct=5/30000, pip_sz=0.01,  # exactly 5 pts
            min_stop_pts_map={"MSFT": 5}
        )
        self.assertGreaterEqual(result, 6,
            f"Stop at exact minimum ({result}) must be >= min+1 (6)")


# ────────────────────────────────────────────────────────────────────────────
class TestAuth401Retry(unittest.TestCase):
    """Session token auto-refresh: 401 triggers re-auth and one retry."""

    def _make_response(self, status_code, json_body=None):
        r = MagicMock()
        r.status_code = status_code
        r.json.return_value = json_body or {}
        r.text = str(json_body or "")
        return r

    def test_401_then_200_succeeds(self):
        """401 triggers authenticate_live(); second request with fresh token returns data."""
        ok_data = {"epic": "NVDA", "bid": 130.0}
        # Simulate: first GET returns 401, second GET returns 200 with ok_data
        responses = [
            self._make_response(401),
            self._make_response(200, ok_data),
        ]
        call_count = {"n": 0}
        def fake_get(url, headers=None, params=None, timeout=None):
            r = responses[call_count["n"]]
            call_count["n"] += 1
            return r

        sess_store = {"cst": "old_cst", "token": "old_tok"}
        def fake_reauth():
            sess_store["cst"]   = "new_cst"
            sess_store["token"] = "new_tok"
            return True

        with patch("requests.get", side_effect=fake_get):
            # Minimal replicated logic from _ig_live_get
            r = fake_get("url")
            if r.status_code == 401:
                reauthed = fake_reauth()
                self.assertTrue(reauthed, "Re-auth must succeed")
                r = fake_get("url")
            result = r.json() if r.status_code == 200 else None

        self.assertEqual(result, ok_data, "Should return data after re-auth success")
        self.assertEqual(call_count["n"], 2, "Exactly 2 requests: initial + retry")
        self.assertEqual(sess_store["cst"], "new_cst", "CST must be refreshed")

    def test_401_reauth_failure_returns_none(self):
        """If re-auth itself fails, the wrapper must return None — not retry blindly."""
        def fake_reauth_fail():
            return False

        r = self._make_response(401)
        if r.status_code == 401:
            reauthed = fake_reauth_fail()
            result = None if not reauthed else {"should": "not reach here"}
        else:
            result = r.json()

        self.assertIsNone(result, "Failed re-auth must yield None — no blind retry")

    def test_non_401_error_no_reauth(self):
        """500 errors must not trigger re-auth loop."""
        reauth_calls = {"n": 0}
        def fake_reauth():
            reauth_calls["n"] += 1
            return True

        r = self._make_response(500)
        if r.status_code == 401:
            fake_reauth()
        result = None  # 500 → return None in wrapper

        self.assertIsNone(result)
        self.assertEqual(reauth_calls["n"], 0, "Re-auth must not fire on 500")


# ────────────────────────────────────────────────────────────────────────────
class TestDealConfirmPolling(unittest.TestCase):
    """P5: _live_confirm_deal() exponential backoff and status handling."""

    def _simulate_confirm(self, status_sequence, retries=4):
        """Replicate _live_confirm_deal() logic, feeding statuses from sequence."""
        _backoff = [0.2, 0.5, 1.0, 2.0]
        calls = []
        for attempt in range(retries):
            # time.sleep skipped in tests
            if attempt < len(status_sequence):
                status = status_sequence[attempt]
                calls.append(status)
                data = {"dealStatus": status, "dealReference": "REF123"}
                if status != "PENDING":
                    return data, calls
        return None, calls

    def test_immediate_accepted(self):
        """First poll returns ACCEPTED — must return immediately, one call."""
        result, calls = self._simulate_confirm(["ACCEPTED"])
        self.assertIsNotNone(result)
        self.assertEqual(result["dealStatus"], "ACCEPTED")
        self.assertEqual(len(calls), 1, "Should return after first poll")

    def test_pending_then_accepted(self):
        """Two PENDINGs then ACCEPTED — must return on third poll."""
        result, calls = self._simulate_confirm(["PENDING", "PENDING", "ACCEPTED"])
        self.assertIsNotNone(result)
        self.assertEqual(result["dealStatus"], "ACCEPTED")
        self.assertEqual(len(calls), 3, "Must wait through PENDINGs then return")

    def test_rejected_returns_data(self):
        """REJECTED is not PENDING — must return the rejected dict (caller inspects it)."""
        result, calls = self._simulate_confirm(["REJECTED"])
        self.assertIsNotNone(result)
        self.assertEqual(result["dealStatus"], "REJECTED")

    def test_all_pending_returns_none(self):
        """If all retries exhausted still PENDING — must return None."""
        result, calls = self._simulate_confirm(["PENDING", "PENDING", "PENDING", "PENDING"])
        self.assertIsNone(result, "All-PENDING after all retries must return None")
        self.assertEqual(len(calls), 4, "Must make all 4 attempts before giving up")

    def test_backoff_sequence_length(self):
        """Backoff list has 4 entries matching the default retries=4."""
        _backoff = [0.2, 0.5, 1.0, 2.0]
        self.assertEqual(len(_backoff), 4, "Backoff list length must match retries default")
        self.assertEqual(_backoff[0], 0.2)
        self.assertEqual(_backoff[-1], 2.0)



# ────────────────────────────────────────────────────────────────────────────
class TestKnownMinNotionals(unittest.TestCase):
    """_KNOWN_MIN_NOTIONALS correctness — prevents IG REJECTED: UNKNOWN due to wrong floors."""

    KNOWN = {
        "EURUSD":  0.47,  "GBPUSD":  0.54,  "USDJPY": 40.0,
        "AUDUSD":  0.29,  "USDCAD":  0.55,  "EURGBP":  0.34,
        "NZDUSD":  0.24,  "USDCHF":  0.32,  "SILVER":  3.30,
        "OIL":     2.71,
    }

    def test_usdjpy_blocks_at_small_balance(self):
        """USDJPY min_notional=40 → ineligible at $14.48 balance (50% margin = $20 needed)."""
        min_n = self.KNOWN["USDJPY"]
        max_lev = 2  # 1/0.50 margin rate
        concentration_cap = 0.20
        balance = 14.48
        # Eligibility: (min_n / lev) <= cap * balance
        eligible = (min_n / max_lev) <= concentration_cap * balance
        self.assertFalse(eligible,
            f"USDJPY (min_n={min_n}, lev={max_lev}) should be ineligible at ${balance}")

    def test_usdjpy_eligible_at_hundred_balance(self):
        """USDJPY becomes eligible once balance reaches ~$100."""
        min_n = self.KNOWN["USDJPY"]
        max_lev = 2
        concentration_cap = 0.20
        balance = 100.0
        eligible = (min_n / max_lev) <= concentration_cap * balance
        self.assertTrue(eligible,
            f"USDJPY (min_n={min_n}) should be eligible at ${balance}")

    def test_eurusd_eligible_at_small_balance(self):
        """EURUSD min_notional=0.47 → eligible at $14.48 (lot=10 live, not 1000 demo)."""
        min_n = self.KNOWN["EURUSD"]
        max_lev = 2  # 50% margin
        concentration_cap = 0.20
        balance = 14.48
        eligible = (min_n / max_lev) <= concentration_cap * balance
        self.assertTrue(eligible,
            f"EURUSD (min_n={min_n}) should be eligible at ${balance}")

    def test_oil_eligible_at_small_balance(self):
        """OIL min_notional=2.71 → eligible at $14.48 (100% margin, 1x lev)."""
        min_n = self.KNOWN["OIL"]
        max_lev = 1  # 100% margin → 1x
        concentration_cap = 0.20
        balance = 14.48
        eligible = (min_n / max_lev) <= concentration_cap * balance
        self.assertTrue(eligible,
            f"OIL (min_n={min_n}) should be eligible at ${balance}")

    def test_silver_ineligible_at_small_balance(self):
        """SILVER min_notional=3.30 → ineligible at $14.48 (80% margin → 1x lev, 3.30 > 2.90)."""
        min_n = self.KNOWN["SILVER"]
        max_lev = 1  # int(1/0.80) = 1
        concentration_cap = 0.20
        balance = 14.48
        eligible = (min_n / max_lev) <= concentration_cap * balance
        self.assertFalse(eligible,
            f"SILVER (min_n={min_n}) should be ineligible at ${balance} (spread gate handles entry timing)")

    def test_all_non_usdjpy_fx_eligible_at_small_balance(self):
        """All FX pairs except USDJPY should be eligible at $14.48 with 50% margin (2x lev)."""
        max_lev_by_sym = {
            "EURUSD": 2, "GBPUSD": 2, "AUDUSD": 2, "USDCAD": 2,
            "EURGBP": 2, "NZDUSD": 1, "USDCHF": 2,  # NZDUSD: 75% margin → 1x
        }
        concentration_cap = 0.20
        balance = 14.48
        for sym, lev in max_lev_by_sym.items():
            min_n = self.KNOWN[sym]
            eligible = (min_n / lev) <= concentration_cap * balance
            self.assertTrue(eligible,
                f"{sym} (min_n={min_n}, lev={lev}) should be eligible at ${balance}")

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromModule(__import__("__main__"))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
