"""Build 4C-C1: simulation quote-scale / epic-remap continuity guard tests.

Adversarial coverage for the generic price-scale continuity invariant that
prevents an epic remap (a symbol's underlying IG contract changing quote scale
by an order of magnitude) from manufacturing a fictitious simulated P&L that
poisons win_moves / loss_moves / vol_history / TP / stop calibration.

The invariant is DISCONTINUITY-based, not magnitude-based: large-but-valid moves
(AG 6.60%, SMCI 5.97%, SQQQ 4.44%, OXY 4.30%, NATGAS 3.58%) must survive; only
an order-of-magnitude scale break (e.g. 6303.8 -> 63.1) is rejected.

Functions are loaded via AST single-function extraction with module-level
constants injected into the namespace (same pattern as test_build4cc_provenance).
"""
import ast
import math
from pathlib import Path
from types import SimpleNamespace

SOURCE_PATH = Path("_cc1_june.py") if Path("_cc1_june.py").exists() else Path("june.py")
SOURCE = SOURCE_PATH.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)

PURE_HELPERS = {"_sim_scale_ratio", "_sim_scale_continuity_ok"}
CLOSE_FUNCS = {
    "_sim_scale_ratio", "_sim_scale_continuity_ok",
    "_sim_compute_pnl_pct", "_sim_invalidate_scale_break", "_sim_close_position",
}


def _extract(names, ns):
    for node in TREE.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            exec(compile(ast.Module(body=[node], type_ignores=[]),
                         "extracted_cc1", "exec"), ns)
    return ns


# ─────────────────────────── pure-helper tests ────────────────────────────────
def _load_pure():
    ns = {}
    return _extract(PURE_HELPERS, ns)


def test_scale_ratio_symmetric_and_ge_one():
    ns = _load_pure()
    assert ns["_sim_scale_ratio"](100.0, 100.0) == 1.0
    # symmetric: ratio(a,b) == ratio(b,a)
    assert ns["_sim_scale_ratio"](6303.8, 63.1) == ns["_sim_scale_ratio"](63.1, 6303.8)
    assert ns["_sim_scale_ratio"](6303.8, 63.1) > 99.0


def test_scale_ratio_undefined_on_nonpositive_or_bad():
    ns = _load_pure()
    assert ns["_sim_scale_ratio"](0.0, 63.1) is None
    assert ns["_sim_scale_ratio"](63.1, 0.0) is None
    assert ns["_sim_scale_ratio"](-5.0, 63.1) is None
    assert ns["_sim_scale_ratio"](None, 63.1) is None
    assert ns["_sim_scale_ratio"]("x", 63.1) is None


def test_same_scale_normal_move_is_ok():
    ns = _load_pure()
    ok = ns["_sim_scale_continuity_ok"]
    # tiny move
    assert ok(6303.8, 6303.9) is True
    # exactly equal
    assert ok(63.1, 63.1) is True


def test_large_but_valid_moves_are_not_rejected():
    """Discontinuity test must NOT reject large legitimate single-position moves."""
    ns = _load_pure()
    ok = ns["_sim_scale_continuity_ok"]
    # entry vs exit for each cited large-but-valid move (both directions)
    cases = {
        "AG":     (100.0, 106.60),  # +6.60%
        "SMCI":   (100.0, 105.97),
        "SQQQ":   (100.0, 104.44),
        "OXY":    (100.0, 104.30),
        "NATGAS": (100.0, 103.58),
    }
    for name, (entry, exit_) in cases.items():
        assert ok(entry, exit_) is True, f"{name} up move wrongly rejected"
        assert ok(exit_, entry) is True, f"{name} down move wrongly rejected"
    # even a large 50% drawdown/spike stays a valid (in-scale) observation
    assert ok(100.0, 150.0) is True
    assert ok(100.0, 60.0) is True


def test_epic_remap_both_directions_detected():
    ns = _load_pure()
    ok = ns["_sim_scale_continuity_ok"]
    # SILVER BMU->CFM: 6303.8 -> 63.1  (~100x down)
    assert ok(6303.8010614, 63.095995092) is False
    # reverse remap CFM->BMU: 63.1 -> 6303.8  (~100x up)
    assert ok(63.095995092, 6303.8010614) is False


def test_ratio_ceiling_boundary():
    ns = _load_pure()
    ok = ns["_sim_scale_continuity_ok"]
    # exactly at 8x -> compatible (<=); just above -> incompatible
    assert ok(10.0, 80.0, 8.0) is True
    assert ok(10.0, 80.01, 8.0) is False


def test_undefined_ratio_fails_open():
    """Missing / zero price must NOT be treated as a scale break (warmup/data gap)."""
    ns = _load_pure()
    ok = ns["_sim_scale_continuity_ok"]
    assert ok(0.0, 63.1) is True
    assert ok(63.1, 0.0) is True
    assert ok(63.1, None) is True


# ───────────────────── close-path (integration) fixtures ─────────────────────
class _FakeRedisList:
    def __init__(self):
        self.lists = {}
        self.kv = {}
    def rpush(self, k, v):
        self.lists.setdefault(k, []).append(v)
    def lpush(self, k, v):
        self.lists.setdefault(k, []).insert(0, v)
    def ltrim(self, *a, **k):
        pass
    def set(self, k, v, **kw):
        self.kv[k] = v
    def get(self, k):
        return self.kv.get(k)


def _load_close_ns(sim_state, history_deques):
    saved_trades = []
    r = _FakeRedisList()

    def _sim_save_trade(rec):
        saved_trades.append(rec)

    ns = {
        "time": SimpleNamespace(time=lambda: 2_000_000.0),
        "_sim": sim_state,
        "_history": history_deques,
        "_redis": lambda: r,
        "_sim_save_trade": _sim_save_trade,
        "_sim_save_state": lambda: None,
        "_sim_log": lambda *a, **k: None,
        "_sim_update_streak": lambda *a, **k: None,
        "_sim_15m_record": lambda *a, **k: None,
        "_live_write_htf_event": lambda *a, **k: None,
        "_SIM_SCALE_RATIO_MAX": 8.0,
        "_SIM_TP_WIN_WINDOW": 10,
        "_SIM_COMBO_WINDOW": 20,
        "_LEV_FUND_INSTRUMENTS": set(),
        "_LEV_FUND_EVIDENCE_KEY": "june_lev_fund_sim_evidence",
        "_saved_trades": saved_trades,
    }
    _extract(CLOSE_FUNCS, ns)
    ns["_saved_trades"] = saved_trades
    return ns


class _Deque(list):
    """Minimal deque stand-in exposing .clear() (list already has it)."""
    pass


def _base_sim_state(open_pos):
    return {
        "balance": 100.0,
        "stage": "sprout", "phase": 1,
        "open_position": open_pos,
        "win_moves": {}, "loss_moves": {}, "combo_outcomes": {},
        "vol_history": {}, "trade_history": [],
        "stage_trades": 0, "stage_wins": 0, "stage_losses": 0,
        "phase_trades": 0, "phase_wins": 0, "phase_losses": 0,
        "phase_consec_losses": 0, "total_wins": 0, "total_losses": 0,
        "long_pnl": 0.0, "long_trades": 0, "long_wins": 0,
        "short_pnl": 0.0, "short_trades": 0, "short_wins": 0,
        "approach_stats": {}, "vol_stats": {},
    }


def _silver_short_pos():
    return {
        "instrument": "SILVER", "direction": "short",
        "fill_price": 6303.8010614, "size": 2.5, "leverage": 5,
        "approach": "fixed_5", "vol_bucket": "mid", "entry_time": 1_999_000.0,
        "entry_vol": 0.2468, "conviction": 3, "claudia_pts": 0.0,
        "stop_price": 6350.0, "tp_price": 6250.0,
    }


def test_scale_break_close_invalidates_without_training():
    """The reproduced defect: closing a 6303.8-entry SILVER short at a 63.1
    observation must NOT compute P&L, must NOT append to win_moves/loss_moves,
    must NOT change balance, and must record SIM_INVALID_SCALE."""
    hist = {"SILVER": _Deque([1, 2, 3])}
    sim = _base_sim_state(_silver_short_pos())
    ns = _load_close_ns(sim, hist)
    # observed mid on the NEW scale
    prices = {"bid": 63.05, "ask": 63.14, "mid": 63.095995092}
    ns["_sim_close_position"](prices, "reversal")

    # position closed
    assert sim["open_position"] is None
    # NO training whatsoever
    assert sim["win_moves"] == {}
    assert sim["loss_moves"] == {}
    assert sim["combo_outcomes"] == {}
    # balance UNCHANGED (no fabricated profit or loss)
    assert sim["balance"] == 100.0
    # win/loss counters untouched
    assert sim["total_wins"] == 0 and sim["total_losses"] == 0
    assert sim["short_trades"] == 0
    # audit record present, flagged, pnl None, zero dollar
    rec = sim["trade_history"][-1]
    assert rec["evidence_class"] == "SIM_INVALID_SCALE"
    assert rec["pnl_pct"] is None
    assert rec["dollar_pnl"] == 0.0
    assert rec["scale_ratio"] > 99.0
    # rolling history for the symbol cleared (dimensionally incompatible)
    assert list(hist["SILVER"]) == []
    # audit trade persisted to redis-backed list too
    assert ns["_saved_trades"][-1]["evidence_class"] == "SIM_INVALID_SCALE"


def test_invalid_observation_never_enters_winmoves_even_when_profitable_looking():
    """Cross-scale short 'move' looks like a +99% win; it must not become a win_move."""
    hist = {"SILVER": _Deque()}
    sim = _base_sim_state(_silver_short_pos())
    ns = _load_close_ns(sim, hist)
    ns["_sim_close_position"]({"bid": 63.05, "ask": 63.14, "mid": 63.1}, "take_profit")
    assert "SILVER_short" not in sim["win_moves"]
    assert "SILVER_short" not in sim["loss_moves"]


def test_normal_in_scale_close_still_trains():
    """Regression guard: an ordinary in-scale close must still train calibration."""
    hist = {"SILVER": _Deque()}
    pos = _silver_short_pos()
    sim = _base_sim_state(pos)
    ns = _load_close_ns(sim, hist)
    # small favorable in-scale move for a short: ask below entry
    prices = {"bid": 6300.0, "ask": 6300.5, "mid": 6300.25}
    ns["_sim_close_position"](prices, "reversal")
    assert sim["open_position"] is None
    # trained: SILVER_short recorded a win move (short profited: entry-ask>0)
    assert "SILVER_short" in sim["win_moves"]
    assert len(sim["win_moves"]["SILVER_short"]) == 1
    # balance changed by real P&L
    assert sim["balance"] != 100.0
    # no invalid record
    assert all(r.get("evidence_class") != "SIM_INVALID_SCALE"
               for r in sim["trade_history"])


def test_reverse_remap_close_invalidates():
    """A position entered on the NEW scale then priced on the OLD scale is also caught."""
    hist = {"SILVER": _Deque([9])}
    pos = _silver_short_pos()
    pos["fill_price"] = 63.1  # entered on new scale
    sim = _base_sim_state(pos)
    ns = _load_close_ns(sim, hist)
    prices = {"bid": 6300.0, "ask": 6307.0, "mid": 6303.8}  # old scale observation
    ns["_sim_close_position"](prices, "stop_loss")
    assert sim["open_position"] is None
    assert sim["win_moves"] == {} and sim["loss_moves"] == {}
    assert sim["balance"] == 100.0
    assert sim["trade_history"][-1]["evidence_class"] == "SIM_INVALID_SCALE"


def test_no_position_close_is_noop():
    sim = _base_sim_state(None)
    ns = _load_close_ns(sim, {})
    ns["_sim_close_position"]({"bid": 1.0, "ask": 1.0, "mid": 1.0}, "reversal")
    assert sim["open_position"] is None
    assert sim["trade_history"] == []


def test_mid_absent_uses_bid_ask_midpoint_for_detection():
    hist = {"SILVER": _Deque()}
    sim = _base_sim_state(_silver_short_pos())
    ns = _load_close_ns(sim, hist)
    # no 'mid' key; bid/ask imply new scale -> must still detect break
    ns["_sim_close_position"]({"bid": 63.05, "ask": 63.14}, "reversal")
    assert sim["trade_history"][-1]["evidence_class"] == "SIM_INVALID_SCALE"
    assert sim["balance"] == 100.0
