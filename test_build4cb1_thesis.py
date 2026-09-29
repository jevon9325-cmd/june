"""Build 4C-B1 focused tests for thesis-aware post-loss re-entry.

These tests extract only the pure/durable B1 helpers from june.py so they do not
import the live broker client or perform Redis writes.
"""
import ast
import json
from pathlib import Path
from types import SimpleNamespace


_SOURCE_PATH = Path("_b1_june.py") if Path("_b1_june.py").exists() else Path("june.py")
_SOURCE = _SOURCE_PATH.read_text(encoding="utf-8")
_TREE = ast.parse(_SOURCE)
_NAMES = {
    "_live_b1_macro_class", "_live_b1_thesis_key", "_live_b1_material_change",
    "_live_b1_record_failure", "_live_b1_reentry_allowed", "_live_b1_update_excursion",
}
_NS = {
    "json": json,
    "time": SimpleNamespace(time=lambda: 1_000_000.0),
    "_EXHAUST_RATIO_REDUCE": 2.5,
    "_EXHAUST_RATIO_BLOCK": 3.5,
    "_B1_THESIS_SCHEMA_VERSION": 1,
    "_B1_FAILURE_MAX_AGE_SECS": 14 * 24 * 3600,
    "_B1_MIN_CONVICTION_DELTA": 2,
    "_B1_MIN_EXHAUSTION_RESET": 1.0,
    "_B1_PRICE_RESET_ATR": 1.0,
    "_B1_FAILURES_KEY": "failed_theses",
    "_B1_FAILURE_HISTORY_KEY": "failed_thesis_history",
    "_B1_REENTRY_LOGGED": set(),
    "_sim_combo_key": lambda sym, direction: f"{sym}_{direction}",
    "_live": {},
    "_live_save_state": lambda: None,
    "_live_log": lambda msg: None,
}
for node in _TREE.body:
    if isinstance(node, ast.FunctionDef) and node.name in _NAMES:
        exec(compile(ast.Module(body=[node], type_ignores=[]), "extracted_b1", "exec"), _NS)

macro_class = _NS["_live_b1_macro_class"]
key = _NS["_live_b1_thesis_key"]
material = _NS["_live_b1_material_change"]
record = _NS["_live_b1_record_failure"]
gate = _NS["_live_b1_reentry_allowed"]
excursion = _NS["_live_b1_update_excursion"]


def snapshot(**overrides):
    value = {
        "schema": 1, "instrument": "NATGAS", "direction": "short",
        "entry_price": 3135.0, "conviction": 5, "regime": "neutral",
        "session": "day", "sub_session": "pre_nyse", "htf_bias": "unknown",
        "htf_move": 0.0, "exhaustion": 1.2, "macro_scale": 0.8,
        "claudia_dir": 0, "macro_note": "neutral", "atr5": 10.0,
        "change_5m": -0.5, "change_15m": -0.4, "spread_atr_ratio": 0.6,
        "persistence_confirmed": True, "gate_mode": "relaxed", "rel_score": None,
    }
    value.update(overrides)
    value["thesis_key"] = key(value)
    return value


def make_failure(**overrides):
    s = snapshot()
    return {
        "schema": 1, "failure_id": "D1", "instrument": "NATGAS", "direction": "short",
        "failure_epoch": 999_000.0, "entry_price": s["entry_price"],
        "entry_conviction": s["conviction"], "entry_htf_bias": s["htf_bias"],
        "entry_exhaustion": s["exhaustion"], "entry_macro_scale": s["macro_scale"],
        "entry_atr": s["atr5"], "entry_thesis_key": s["thesis_key"],
        "failure_thesis_key": s["thesis_key"],
    } | overrides


def reset_live():
    _NS["_live"] = {}
    _NS["_B1_REENTRY_LOGGED"].clear()


def test_same_evidence_blocks_after_cooldown():
    reset_live(); _NS["_live"]["failed_theses"] = {"NATGAS_short": make_failure()}
    assert gate("NATGAS", "short", snapshot()) is False


def test_trivial_conviction_change_blocks():
    reset_live(); _NS["_live"]["failed_theses"] = {"NATGAS_short": make_failure()}
    assert gate("NATGAS", "short", snapshot(conviction=6)) is False


def test_material_conviction_change_releases():
    reset_live(); _NS["_live"]["failed_theses"] = {"NATGAS_short": make_failure()}
    assert gate("NATGAS", "short", snapshot(conviction=7)) is True
    assert "NATGAS_short" not in _NS["_live"]["failed_theses"]
    assert len(_NS["_live"]["failed_thesis_history"]) == 1


def test_htf_opposition_to_alignment_releases():
    reset_live(); _NS["_live"]["failed_theses"] = {"NATGAS_short": make_failure(entry_htf_bias="bull")}
    assert gate("NATGAS", "short", snapshot(htf_bias="bear")) is True


def test_exhaustion_reset_releases():
    reset_live(); _NS["_live"]["failed_theses"] = {"NATGAS_short": make_failure(entry_exhaustion=3.6)}
    assert gate("NATGAS", "short", snapshot(exhaustion=1.2)) is True


def test_macro_conflict_to_alignment_releases():
    reset_live(); _NS["_live"]["failed_theses"] = {"NATGAS_short": make_failure(entry_macro_scale=0.5)}
    assert gate("NATGAS", "short", snapshot(macro_scale=1.0)) is True


def test_natgas_price_reset_allows_second_short():
    reset_live(); _NS["_live"]["failed_theses"] = {"NATGAS_short": make_failure()}
    # 3135 -> 3105 is a 30-point directional reset versus ATR=10.
    assert gate("NATGAS", "short", snapshot(entry_price=3105.0, conviction=5)) is True


def test_missing_price_reset_evidence_does_not_release():
    reset_live(); _NS["_live"]["failed_theses"] = {"NATGAS_short": make_failure(entry_atr=None)}
    assert gate("NATGAS", "short", snapshot(entry_price=3105.0, atr5=None)) is False


def test_old_failure_requires_two_dimensions_not_time_alone():
    reset_live(); _NS["time"] = SimpleNamespace(time=lambda: 999_000.0 + 15 * 86400)
    _NS["_live"]["failed_theses"] = {"NATGAS_short": make_failure()}
    assert gate("NATGAS", "short", snapshot(conviction=7)) is False
    assert gate("NATGAS", "short", snapshot(conviction=7, htf_bias="bear")) is True
    _NS["time"] = SimpleNamespace(time=lambda: 1_000_000.0)


def test_opposite_direction_is_unaffected():
    reset_live(); _NS["_live"]["failed_theses"] = {"NATGAS_short": make_failure()}
    assert gate("NATGAS", "long", snapshot(direction="long", entry_price=3135.0)) is True


def test_different_instrument_is_unaffected():
    reset_live(); _NS["_live"]["failed_theses"] = {"NATGAS_short": make_failure()}
    assert gate("OIL", "short", snapshot(instrument="OIL")) is True


def test_profitable_stop_exit_is_not_recorded():
    reset_live(); saves = []
    _NS["_live_save_state"] = lambda: saves.append(1)
    record({"instrument": "OIL", "direction": "short", "deal_id": "WIN",
            "fill_price": 100.0, "thesis_snapshot": snapshot(instrument="OIL")},
           {"exit_reason": "stop_loss", "dollar_pnl": 0.5, "exit_epoch": 10}, "test")
    assert _NS["_live"].get("failed_theses", {}) == {}
    assert saves == []


def test_unknown_stop_failure_is_recorded_and_duplicate_is_idempotent():
    reset_live(); saves = []
    _NS["_live_save_state"] = lambda: saves.append(1)
    pos = {"instrument": "OIL", "direction": "short", "deal_id": "D1",
           "fill_price": 100.0, "entry_time": 1, "conviction": 5,
           "entry_sar": 0.5, "thesis_snapshot": snapshot(instrument="OIL")}
    rec = {"exit_reason": "stop_loss", "dollar_pnl": None, "settlement_state": "PROVISIONAL", "exit_epoch": 20}
    record(pos, rec, "close_guard_absent:REST-deal")
    record(pos, rec, "close_guard_absent:REST-deal")
    assert len(_NS["_live"]["failed_theses"]) == 1
    assert len(saves) == 1


def test_restart_preserves_failure_gate():
    reset_live(); persisted = {"failed_theses": {"NATGAS_short": make_failure()}}
    _NS["_live"] = json.loads(json.dumps(persisted))
    assert gate("NATGAS", "short", snapshot()) is False


def test_excursion_telemetry_is_nonnegative_and_r_normalized():
    reset_live(); saves = []
    _NS["_live_save_state"] = lambda: saves.append(1)
    pos = {"initial_sl_pct": 0.25, "max_favorable_excursion_pct": 0.0,
           "max_adverse_excursion_pct": 0.0}
    _NS["_live"]["open_position"] = pos
    excursion(pos, 0.5)
    excursion(pos, -0.25)
    assert pos["max_favorable_excursion_pct"] == 0.5
    assert pos["max_favorable_excursion_r"] == 2.0
    assert pos["max_adverse_excursion_pct"] == 0.25
    assert pos["max_adverse_excursion_r"] == 1.0


def test_source_wires_b1_without_touching_policy_mechanics():
    assert "_b1_gate = globals().get(\"_live_b1_reentry_allowed\")" in _SOURCE
    assert "thesis_snapshot=_b1_snapshot" in _SOURCE
    assert "_b1_record = globals().get(\"_live_b1_record_failure\")" in _SOURCE
    assert "_PYRAMID_PROFIT_GATE_PCT = 0.0015" in _SOURCE
    assert "_LIVE_CIRCUIT_BREAKER_PCT = -0.05" in _SOURCE
    assert "_MPD_MIN_PROFIT_PIPS = 1" in _SOURCE
    assert "_SIM_TP_WIN_FRACTION = 0.82" in _SOURCE
