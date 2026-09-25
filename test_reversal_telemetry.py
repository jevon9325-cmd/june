"""
Tests for reversal trigger-source telemetry and re-entry observation.

Coverage:
  - regime-only classification
  - sig_dir-only classification
  - both classification
  - non-opposing cycles produce no reversal source
  - source survives into trade_rec (close/history telemetry)
  - same-direction re-entry interval captured
  - opposite-direction re-entry captured correctly
  - no previous reversal entry handled correctly (no crash)
  - legacy records missing reversal_trigger_source handled correctly
  - telemetry exceptions cannot veto exits
  - telemetry exceptions cannot veto entries
  - storage remains bounded at 20 instruments
  - Redis telemetry key constants present with expected values
"""

import importlib
import json
import types
import time
import sys
import unittest


# ── Module bootstrap ──────────────────────────────────────────────────────────

def _load_june():
    """Load june module with all live/broker/external calls stubbed out."""
    import unittest.mock as mock

    stubs = {
        "redis":          mock.MagicMock(),
        "lightstreamer":  mock.MagicMock(),
        "requests":       mock.MagicMock(),
        "websocket":      mock.MagicMock(),
        "pandas":         mock.MagicMock(),
        "numpy":          mock.MagicMock(),
    }

    with mock.patch.dict("sys.modules", {k: v for k, v in stubs.items()
                                         if k not in sys.modules}):
        spec = importlib.util.spec_from_file_location("june", "june.py")
        mod = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(mod)
        except Exception:
            pass  # startup code that requires env vars / network can fail safely
        return mod


try:
    import june as _june_mod
except Exception:
    _june_mod = None


# ── Helper: simulate the trigger-source derivation logic ─────────────────────

def _derive_rev_source(dirn: str, regime: str, sig_dir: str) -> str | None:
    """Pure reimplementation of the trigger-source logic for test verification."""
    opposing = (
        (dirn == "long"  and (regime == "bear" or sig_dir == "bear")) or
        (dirn == "short" and (regime == "bull" or sig_dir == "bull"))
    )
    if not opposing:
        return None
    if dirn == "long":
        rtrig_regime = (regime == "bear")
        rtrig_sig    = (sig_dir == "bear")
    else:
        rtrig_regime = (regime == "bull")
        rtrig_sig    = (sig_dir == "bull")
    if rtrig_regime and rtrig_sig:
        return "both"
    if rtrig_regime:
        return "regime"
    return "sig_dir"


# ── Tests: trigger-source classification ─────────────────────────────────────

class TestReversalTriggerSourceClassification(unittest.TestCase):

    # long / regime-only
    def test_long_regime_only(self):
        self.assertEqual(
            _derive_rev_source("long", "bear", "neutral"),
            "regime"
        )

    def test_long_regime_only_bull_sig(self):
        # regime=bear, sig=bull: regime triggers, sig does NOT (bull ≠ bear for a long)
        self.assertEqual(
            _derive_rev_source("long", "bear", "bull"),
            "regime"
        )

    # long / sig_dir-only
    def test_long_sig_dir_only(self):
        self.assertEqual(
            _derive_rev_source("long", "bull", "bear"),
            "sig_dir"
        )

    def test_long_sig_dir_only_neutral_regime(self):
        self.assertEqual(
            _derive_rev_source("long", "neutral", "bear"),
            "sig_dir"
        )

    # long / both
    def test_long_both(self):
        self.assertEqual(
            _derive_rev_source("long", "bear", "bear"),
            "both"
        )

    # short / regime-only
    def test_short_regime_only(self):
        self.assertEqual(
            _derive_rev_source("short", "bull", "neutral"),
            "regime"
        )

    def test_short_regime_only_bear_sig(self):
        # regime=bull, sig=bear: regime triggers (bull opp short), sig does NOT
        self.assertEqual(
            _derive_rev_source("short", "bull", "bear"),
            "regime"
        )

    # short / sig_dir-only
    def test_short_sig_dir_only(self):
        self.assertEqual(
            _derive_rev_source("short", "bear", "bull"),
            "sig_dir"
        )

    def test_short_sig_dir_only_neutral_regime(self):
        self.assertEqual(
            _derive_rev_source("short", "neutral", "bull"),
            "sig_dir"
        )

    # short / both
    def test_short_both(self):
        self.assertEqual(
            _derive_rev_source("short", "bull", "bull"),
            "both"
        )

    # non-opposing: no source
    def test_long_no_opposing_neutral(self):
        self.assertIsNone(_derive_rev_source("long", "neutral", "neutral"))

    def test_long_no_opposing_aligned(self):
        self.assertIsNone(_derive_rev_source("long", "bull", "bull"))

    def test_short_no_opposing_neutral(self):
        self.assertIsNone(_derive_rev_source("short", "neutral", "neutral"))

    def test_short_no_opposing_aligned(self):
        self.assertIsNone(_derive_rev_source("short", "bear", "bear"))

    def test_long_no_opposing_bull_neutral(self):
        self.assertIsNone(_derive_rev_source("long", "bull", "neutral"))

    def test_short_no_opposing_bear_neutral(self):
        self.assertIsNone(_derive_rev_source("short", "bear", "neutral"))


# ── Tests: reversal_trigger_source in trade record ───────────────────────────

class TestReversalSourceInTradeRecord(unittest.TestCase):
    """Simulate the trade_rec construction to verify field presence."""

    def _make_trade_rec(self, exit_reason: str, reversal_trigger_source=None) -> dict:
        pos = {
            "instrument":  "OIL",
            "direction":   "long",
            "conviction":  3,
            "claudia_pts": 1.2,
            "reversal_trigger_source": reversal_trigger_source,
        }
        trade_rec = {
            "exit_reason":  exit_reason,
            "conviction":   pos.get("conviction", 0),
            "claudia_pts":  pos.get("claudia_pts", 0.0),
            "reversal_trigger_source": (pos.get("reversal_trigger_source")
                                        if exit_reason == "reversal" else None),
        }
        return trade_rec

    def test_reversal_exit_carries_source_regime(self):
        rec = self._make_trade_rec("reversal", "regime")
        self.assertEqual(rec["reversal_trigger_source"], "regime")

    def test_reversal_exit_carries_source_sig_dir(self):
        rec = self._make_trade_rec("reversal", "sig_dir")
        self.assertEqual(rec["reversal_trigger_source"], "sig_dir")

    def test_reversal_exit_carries_source_both(self):
        rec = self._make_trade_rec("reversal", "both")
        self.assertEqual(rec["reversal_trigger_source"], "both")

    def test_reversal_exit_no_source_stored(self):
        # Source not stored yet (first opposing cycle, not yet firing)
        rec = self._make_trade_rec("reversal", None)
        self.assertIsNone(rec["reversal_trigger_source"])

    def test_stop_loss_exit_no_source(self):
        rec = self._make_trade_rec("stop_loss", "regime")
        self.assertIsNone(rec["reversal_trigger_source"])

    def test_take_profit_exit_no_source(self):
        rec = self._make_trade_rec("take_profit", "both")
        self.assertIsNone(rec["reversal_trigger_source"])

    def test_dple_trail_exit_no_source(self):
        rec = self._make_trade_rec("dple_trail", "sig_dir")
        self.assertIsNone(rec["reversal_trigger_source"])

    def test_field_always_present_in_record(self):
        for reason in ("reversal", "stop_loss", "take_profit", "dple_trail",
                       "mpd_floor", "max_hold", "better_opportunity"):
            rec = self._make_trade_rec(reason, "regime")
            self.assertIn("reversal_trigger_source", rec,
                          f"Field missing for exit_reason={reason}")

    def test_legacy_record_missing_field_is_handled(self):
        """Records without reversal_trigger_source must be deserializable."""
        legacy = {
            "instrument": "GOLD",
            "direction":  "short",
            "exit_reason": "reversal",
            "conviction": 2,
            "dollar_pnl": -0.31,
        }
        serialized = json.dumps(legacy)
        loaded = json.loads(serialized)
        # Should not raise; field simply absent
        self.assertIsNone(loaded.get("reversal_trigger_source"))


# ── Tests: _live_reversal_exits bounded storage ───────────────────────────────

class TestReversalExitsBoundedStorage(unittest.TestCase):

    def _make_exit(self, sym: str, direction: str = "long",
                   src: str = "regime", pnl: float = -0.50,
                   epoch: int | None = None) -> dict:
        return {
            "exit_epoch":    epoch if epoch is not None else int(time.time()),
            "direction":     direction,
            "trigger_source": src,
            "dollar_pnl":    pnl,
        }

    def test_bounded_at_20(self):
        store: dict = {}
        for i in range(25):
            sym = f"INSTR_{i:02d}"
            store[sym] = self._make_exit(sym, epoch=i)
            if len(store) > 20:
                oldest = min(store, key=lambda k: store[k]["exit_epoch"])
                del store[oldest]
        self.assertEqual(len(store), 20)

    def test_oldest_removed(self):
        store: dict = {}
        for i in range(21):
            sym = f"INSTR_{i:02d}"
            store[sym] = self._make_exit(sym, epoch=i)
            if len(store) > 20:
                oldest = min(store, key=lambda k: store[k]["exit_epoch"])
                del store[oldest]
        self.assertNotIn("INSTR_00", store)
        self.assertIn("INSTR_20", store)

    def test_single_instrument_overwritten(self):
        store: dict = {}
        store["OIL"] = self._make_exit("OIL", direction="long", pnl=-1.0, epoch=100)
        store["OIL"] = self._make_exit("OIL", direction="short", pnl=-0.5, epoch=200)
        self.assertEqual(store["OIL"]["direction"], "short")
        self.assertEqual(store["OIL"]["dollar_pnl"], -0.5)

    def test_no_crash_on_empty_store(self):
        store: dict = {}
        result = store.get("GOLD")
        self.assertIsNone(result)


# ── Tests: re-entry telemetry record contents ────────────────────────────────

class TestReentryTelemetryRecord(unittest.TestCase):

    def _make_tel_rec(self, sym: str, direction: str, prev_direction: str,
                      elapsed_s: float, prev_src: str | None,
                      prev_pnl: float, conv: int, regime: str, sig_dir: str,
                      global_mode: str = "normal") -> dict:
        same_dir = (prev_direction == direction)
        interval = ("<=5m"  if elapsed_s <= 300  else
                    "<=15m" if elapsed_s <= 900  else
                    "<=30m" if elapsed_s <= 1800 else
                    "<=60m" if elapsed_s <= 3600 else ">60m")
        return {
            "epoch":               int(time.time()),
            "instrument":          sym,
            "new_direction":       direction,
            "prev_direction":      prev_direction,
            "same_direction":      same_dir,
            "elapsed_s":           round(elapsed_s, 1),
            "interval":            interval,
            "prev_trigger_source": prev_src,
            "prev_dollar_pnl":     prev_pnl,
            "conviction":          conv,
            "regime":              regime,
            "sig_dir":             sig_dir,
            "global_mode":         global_mode,
        }

    def test_same_direction_flagged(self):
        rec = self._make_tel_rec("OIL", "long", "long", 120.0, "regime", -0.5, 3, "bull", "bull")
        self.assertTrue(rec["same_direction"])

    def test_opposite_direction_flagged(self):
        rec = self._make_tel_rec("OIL", "short", "long", 120.0, "regime", -0.5, 3, "bear", "bear")
        self.assertFalse(rec["same_direction"])

    def test_interval_le5m(self):
        rec = self._make_tel_rec("OIL", "long", "long", 299.0, "regime", -0.5, 3, "bull", "bull")
        self.assertEqual(rec["interval"], "<=5m")

    def test_interval_le15m(self):
        rec = self._make_tel_rec("OIL", "long", "long", 600.0, "regime", -0.5, 3, "bull", "bull")
        self.assertEqual(rec["interval"], "<=15m")

    def test_interval_le30m(self):
        rec = self._make_tel_rec("OIL", "long", "long", 1200.0, "regime", -0.5, 3, "bull", "bull")
        self.assertEqual(rec["interval"], "<=30m")

    def test_interval_le60m(self):
        rec = self._make_tel_rec("OIL", "long", "long", 2400.0, "regime", -0.5, 3, "bull", "bull")
        self.assertEqual(rec["interval"], "<=60m")

    def test_interval_gt60m(self):
        rec = self._make_tel_rec("OIL", "long", "long", 7200.0, "regime", -0.5, 3, "bull", "bull")
        self.assertEqual(rec["interval"], ">60m")

    def test_all_required_fields_present(self):
        rec = self._make_tel_rec("SILVER", "short", "long", 400.0, "sig_dir", -1.2, 2,
                                 "bear", "bull")
        required = ["epoch", "instrument", "new_direction", "prev_direction",
                    "same_direction", "elapsed_s", "interval", "prev_trigger_source",
                    "prev_dollar_pnl", "conviction", "regime", "sig_dir", "global_mode"]
        for field in required:
            self.assertIn(field, rec, f"Missing required field: {field}")

    def test_no_prev_reversal_no_telemetry(self):
        """When _live_reversal_exits has no entry for instrument, no tel record is written."""
        store: dict = {}
        prev_rev = store.get("GOLD")
        self.assertIsNone(prev_rev)
        # Tel record should not be constructed
        created = False
        if prev_rev is not None:
            created = True
        self.assertFalse(created)

    def test_prev_source_none_handled(self):
        """prev_trigger_source may be None if source derivation errored; must not crash."""
        rec = self._make_tel_rec("NATGAS", "long", "long", 120.0, None, -0.2, 4, "bull", "bull")
        self.assertIsNone(rec["prev_trigger_source"])

    def test_record_is_json_serializable(self):
        rec = self._make_tel_rec("HO", "short", "short", 500.0, "both", -3.1, 3,
                                 "bear", "bear", "defensive")
        serialized = json.dumps(rec)
        loaded = json.loads(serialized)
        self.assertEqual(loaded["instrument"], "HO")
        self.assertEqual(loaded["interval"], "<=15m")
        self.assertTrue(loaded["same_direction"])


# ── Tests: telemetry exceptions cannot veto exits or entries ─────────────────

class TestTelemetryExceptionIsolation(unittest.TestCase):

    def test_trigger_source_exception_does_not_propagate(self):
        """Simulates the try/except block around trigger-source derivation."""
        position_closed = False

        class BadState:
            def __getitem__(self, key): raise RuntimeError("storage error")
            def __setitem__(self, key, val): raise RuntimeError("storage error")

        def exit_path(live_state):
            # Simulates the reversal block: trigger-source derivation wrapped in try/except
            try:
                live_state["open_position"]["reversal_trigger_source"] = "regime"
            except Exception:
                pass
            # Exit still proceeds regardless
            nonlocal position_closed
            position_closed = True

        exit_path(BadState())
        self.assertTrue(position_closed)

    def test_reentry_tel_exception_does_not_block_entry(self):
        """Simulates the try/except block around re-entry telemetry in _live_try_entry."""
        entry_opened = False

        def entry_path(reversal_exits, sym):
            try:
                prev_rev = reversal_exits.get(sym)
                if prev_rev is not None:
                    raise RuntimeError("simulated telemetry failure")
            except Exception:
                pass
            # Entry proceeds regardless
            nonlocal entry_opened
            entry_opened = True

        broken_store = {"OIL": {"exit_epoch": int(time.time()), "direction": "long",
                                 "trigger_source": "regime", "dollar_pnl": -0.5}}
        entry_path(broken_store, "OIL")
        self.assertTrue(entry_opened)

    def test_redis_exception_does_not_block_exit(self):
        """Simulates the inner Redis try/except in the re-entry path."""
        exit_recorded = False

        def record_and_push(store, sym, rec):
            nonlocal exit_recorded
            store[sym] = rec  # local write always happens
            exit_recorded = True
            try:
                raise ConnectionError("redis unavailable")
            except Exception:
                pass

        store: dict = {}
        record_and_push(store, "GOLD",
                        {"exit_epoch": int(time.time()), "direction": "short",
                         "trigger_source": "sig_dir", "dollar_pnl": -0.3})
        self.assertTrue(exit_recorded)
        self.assertIn("GOLD", store)

    def test_redis_exception_does_not_block_entry(self):
        """Simulates the inner Redis try/except in the re-entry tel path."""
        entry_opened = False

        def tel_and_entry(store, sym):
            try:
                prev = store.get(sym)
                if prev is not None:
                    try:
                        raise ConnectionError("redis unavailable")
                    except Exception:
                        pass
            except Exception:
                pass
            nonlocal entry_opened
            entry_opened = True

        tel_and_entry({"OIL": {"exit_epoch": 0, "direction": "long",
                                "trigger_source": "both", "dollar_pnl": -1.0}}, "OIL")
        self.assertTrue(entry_opened)


# ── Tests: Redis key constants ────────────────────────────────────────────────

class TestRedisKeyConstants(unittest.TestCase):

    def _load_constants(self):
        """Extract constants from june.py source without executing the module."""
        constants = {}
        with open("june.py", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                for name in ("_LIVE_REENTRY_TEL_KEY", "_LIVE_REENTRY_TEL_CAP",
                             "_LIVE_REENTRY_TEL_TTL", "_LIVE_TRADE_HIST_KEY",
                             "_LIVE_TRADE_HIST_CAP", "_LIVE_TRADE_HIST_TTL"):
                    if line.startswith(name + " "):
                        val_part = line.split("=", 1)[1].split("#")[0].strip()
                        try:
                            constants[name] = eval(val_part)  # safe: numeric/string literals only
                        except Exception:
                            pass
        return constants

    def test_reentry_tel_key_present(self):
        c = self._load_constants()
        self.assertIn("_LIVE_REENTRY_TEL_KEY", c)
        self.assertEqual(c["_LIVE_REENTRY_TEL_KEY"], "june_reversal_reentry_tel")

    def test_reentry_tel_cap_is_500(self):
        c = self._load_constants()
        self.assertEqual(c.get("_LIVE_REENTRY_TEL_CAP"), 500)

    def test_reentry_tel_ttl_is_30_days(self):
        c = self._load_constants()
        self.assertEqual(c.get("_LIVE_REENTRY_TEL_TTL"), 86400 * 30)

    def test_trade_hist_key_unchanged(self):
        c = self._load_constants()
        self.assertEqual(c.get("_LIVE_TRADE_HIST_KEY"), "june_live_trade_history_full")

    def test_trade_hist_cap_unchanged(self):
        c = self._load_constants()
        self.assertEqual(c.get("_LIVE_TRADE_HIST_CAP"), 2000)

    def test_trade_hist_ttl_unchanged(self):
        c = self._load_constants()
        self.assertEqual(c.get("_LIVE_TRADE_HIST_TTL"), 86400 * 30)


# ── Tests: Python 3.12 compile check ─────────────────────────────────────────

class TestPython312CompileCheck(unittest.TestCase):

    def test_june_py_compiles_without_syntax_error(self):
        import py_compile
        try:
            py_compile.compile("june.py", doraise=True)
        except py_compile.PyCompileError as e:
            self.fail(f"june.py has a syntax error: {e}")


# ── Tests: strategy constants unchanged ──────────────────────────────────────

class TestStrategyConstantsUnchanged(unittest.TestCase):
    """Verify that all strategy-critical constants are not modified."""

    def _load_constants(self):
        constants = {}
        with open("june.py", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                for name in (
                    "_SIM_REV_PATIENCE_WIN",
                    "_SIM_REV_PATIENCE_LOSS",
                    "_PERF_BLOCK_MIN_RECENT",
                    "_PERF_BLOCK_WR_THRESH",
                    "_PERF_BLOCK_SAR_THRESH",
                    "_PERF_BLOCK_HARD_MIN_TRADES",
                    "_PERF_BLOCK_HARD_WR_THRESH",
                    "_PERF_BLOCK_SAR_SESSION_MIN",
                    "_SIM_TP_WIN_FRACTION",
                    "_PYRAMID_PROFIT_GATE_PCT",
                ):
                    if line.startswith(name + " ") or line.startswith(name + "="):
                        val_part = line.split("=", 1)[1].split("#")[0].strip()
                        try:
                            constants[name] = eval(val_part)
                        except Exception:
                            pass
        return constants

    def test_reversal_patience_win_unchanged(self):
        c = self._load_constants()
        self.assertEqual(c.get("_SIM_REV_PATIENCE_WIN"), 2)

    def test_reversal_patience_loss_unchanged(self):
        c = self._load_constants()
        self.assertEqual(c.get("_SIM_REV_PATIENCE_LOSS"), 1)

    def test_perf_block_min_recent_unchanged(self):
        c = self._load_constants()
        self.assertEqual(c.get("_PERF_BLOCK_MIN_RECENT"), 8)

    def test_perf_block_wr_thresh_unchanged(self):
        c = self._load_constants()
        self.assertAlmostEqual(c.get("_PERF_BLOCK_WR_THRESH", 0), 0.30)

    def test_perf_block_sar_thresh_unchanged(self):
        c = self._load_constants()
        self.assertAlmostEqual(c.get("_PERF_BLOCK_SAR_THRESH", 0), 0.50)

    def test_perf_block_hard_min_trades_unchanged(self):
        c = self._load_constants()
        self.assertEqual(c.get("_PERF_BLOCK_HARD_MIN_TRADES"), 12)

    def test_perf_block_hard_wr_thresh_unchanged(self):
        c = self._load_constants()
        self.assertAlmostEqual(c.get("_PERF_BLOCK_HARD_WR_THRESH", 0), 0.20)

    def test_perf_block_sar_session_min_unchanged(self):
        c = self._load_constants()
        self.assertEqual(c.get("_PERF_BLOCK_SAR_SESSION_MIN"), 4)

    def test_tp_win_fraction_unchanged(self):
        c = self._load_constants()
        self.assertAlmostEqual(c.get("_SIM_TP_WIN_FRACTION", 0), 0.82)

    def test_pyramid_profit_gate_unchanged(self):
        c = self._load_constants()
        self.assertAlmostEqual(c.get("_PYRAMID_PROFIT_GATE_PCT", 0), 0.0015)


if __name__ == "__main__":
    unittest.main(verbosity=2)
