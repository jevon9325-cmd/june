"""Build 4C-C evidence-class, confirmed-only learning, and economic invariance tests."""
import ast
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

SOURCE_PATH = Path("_cc_june.py") if Path("_cc_june.py").exists() else Path("june.py")
SOURCE = SOURCE_PATH.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)
HELPERS = {
    "_evidence_class", "_record_evidence_class", "_confirmed_outcome_record",
    "_performance_learning_rows", "_outcome_observation_id", "_live_perf_record",
}

class FakeRedis:
    def __init__(self): self.data = {}
    def get(self, key): return self.data.get(key)
    def set(self, key, value, **kwargs): self.data[key] = value
    def setex(self, key, ttl, value): self.data[key] = value


def load_helpers():
    r = FakeRedis()
    ns = {
        "json": json, "time": SimpleNamespace(time=lambda: 1_000_000.0),
        "_EVIDENCE_SIM_PRIOR": "SIM_PRIOR",
        "_EVIDENCE_BROKER_CONFIRMED": "BROKER_CONFIRMED_LIVE",
        "_EVIDENCE_LEGACY_LIVE": "LEGACY_LIVE",
        "_EVIDENCE_PROVISIONAL": "PROVISIONAL",
        "_EVIDENCE_UNKNOWN": "UNKNOWN",
        "_EVIDENCE_CLASSES": frozenset({"SIM_PRIOR", "BROKER_CONFIRMED_LIVE", "LEGACY_LIVE", "PROVISIONAL", "UNKNOWN"}),
        "_redis": lambda: r, "_live": {"balance": 100.0},
        "_live_log": lambda *_a, **_k: None,
        "_live_clear_observer": lambda *_a, **_k: None,
        "_live_set_observer": lambda *_a, **_k: None,
        "_perf_block_cache": {},
        "_PERF_BLOCK_WINDOW": 8, "_PERF_BLOCK_HARD_MIN_TRADES": 12,
        "_PERF_BLOCK_RECENCY_DAYS": 14, "_PERF_BLOCK_WR_THRESH": .30,
        "_PERF_BLOCK_HARD_WR_THRESH": .20, "_PERF_BLOCK_HARD_LOSS_PCT": .07,
        "_PERF_BLOCK_MIN_RECENT": 8, "_PERF_BLOCK_OBS_LIGHT_PCT": .03,
        "_PERF_BLOCK_SAR_SESSION_MIN": 4, "_PERF_BLOCK_SAR_THRESH": .50,
        "_PERF_BLOCK_HARD_TTL": 43200, "_PERF_BLOCK_TTL": 86400,
        "_PERF_SAR_BLOCK_EPOCH_CUTOFF": 0,
        "_SESSION_SCHEMA_VERSION": 2,
        "is_overnight": lambda: False,
        "_current_sub_session": lambda _sym: "day",
        "_perf_block_sar_ttl": lambda *_a, **_k: 3600,
    }
    for node in TREE.body:
        if isinstance(node, ast.FunctionDef) and node.name in HELPERS:
            exec(compile(ast.Module(body=[node], type_ignores=[]), "extracted_cc", "exec"), ns)
    return ns, r


def test_evidence_classes_are_explicit_and_legacy_safe():
    ns, _ = load_helpers()
    assert ns["_evidence_class"]("SIM_PRIOR") == "SIM_PRIOR"
    assert ns["_evidence_class"]("BROKER_CONFIRMED_LIVE") == "BROKER_CONFIRMED_LIVE"
    assert ns["_record_evidence_class"]({"settlement_state": "CONFIRMED"}) == "LEGACY_LIVE"
    assert ns["_record_evidence_class"]({"settlement_state": "PROVISIONAL"}) == "PROVISIONAL"
    assert ns["_record_evidence_class"]({"settlement_state": "UNKNOWN"}) == "UNKNOWN"
    assert ns["_record_evidence_class"]({"source": "simulation"}) == "SIM_PRIOR"


def test_only_explicit_confirmed_non_null_outcomes_are_confirmed_plane():
    ns, _ = load_helpers()
    eligible = {"evidence_class": "BROKER_CONFIRMED_LIVE", "dollar_pnl": 1.2}
    assert ns["_confirmed_outcome_record"](eligible)
    assert not ns["_confirmed_outcome_record"]({"evidence_class": "BROKER_CONFIRMED_LIVE", "dollar_pnl": None})
    assert not ns["_confirmed_outcome_record"]({"evidence_class": "PROVISIONAL", "dollar_pnl": -1.2})
    assert not ns["_confirmed_outcome_record"]({"evidence_class": "UNKNOWN", "dollar_pnl": 0.0})
    assert not ns["_confirmed_outcome_record"]({"evidence_class": "SIM_PRIOR", "dollar_pnl": 1.2})


def test_legacy_rows_remain_backward_readable_but_unresolved_rows_are_excluded():
    ns, _ = load_helpers()
    rows = [
        {"dollar_pnl": 1.0},
        {"evidence_class": "LEGACY_LIVE", "dollar_pnl": -1.0},
        {"evidence_class": "PROVISIONAL", "dollar_pnl": -2.0},
        {"evidence_class": "UNKNOWN", "dollar_pnl": None},
        {"evidence_class": "SIM_PRIOR", "dollar_pnl": 3.0},
    ]
    learned = ns["_performance_learning_rows"](rows)
    assert len(learned) == 2
    assert all(ns["_record_evidence_class"](x) in ("LEGACY_LIVE", "BROKER_CONFIRMED_LIVE") for x in learned)


def test_perf_record_preserves_provenance_and_deduplicates():
    ns, r = load_helpers()
    perf = ns["_live_perf_record"]
    perf("OIL", False, .4, pnl_dollar=-1.2, campaign_id="C1", deal_id="D1",
         settlement_identity="deal:D1", settlement_state="CONFIRMED",
         evidence_class="BROKER_CONFIRMED_LIVE", pnl_source="broker_tx",
         exit_reason="stop_loss")
    perf("OIL", False, .4, pnl_dollar=-1.2, campaign_id="C1", deal_id="D1",
         settlement_identity="deal:D1", settlement_state="CONFIRMED",
         evidence_class="BROKER_CONFIRMED_LIVE", pnl_source="broker_tx",
         exit_reason="stop_loss")
    rows = json.loads(r.get("june_perf_stats:OIL"))["trades"]
    assert len(rows) == 1
    assert rows[0]["evidence_class"] == "BROKER_CONFIRMED_LIVE"
    assert rows[0]["deal_id"] == "D1"
    assert rows[0]["settlement_identity"] == "deal:D1"


def test_perf_record_skips_provisional_unknown_and_sim():
    ns, r = load_helpers()
    perf = ns["_live_perf_record"]
    for cls, state, pnl in (("PROVISIONAL", "PROVISIONAL", -1.0),
                            ("UNKNOWN", "UNKNOWN", 0.0),
                            ("SIM_PRIOR", "CONFIRMED", 1.0)):
        perf("SILVER", pnl > 0, .2, pnl_dollar=pnl,
             settlement_state=state, evidence_class=cls,
             settlement_identity=f"{cls}:1")
    assert r.get("june_perf_stats:SILVER") is None


def test_sim_prior_trade_metadata_is_written_and_htf_requires_confirmed():
    assert '"evidence_class": "SIM_PRIOR"' in SOURCE
    assert 'if _record_evidence_class(ev) != _EVIDENCE_BROKER_CONFIRMED:' in SOURCE
    assert 'if match is None or match.get("dollar_pnl") is None:' in SOURCE
    assert 'won = float(match["dollar_pnl"]) > 0' in SOURCE


def test_unknown_is_not_coerced_to_zero_loss_in_source():
    assert 'match.get("dollar_pnl", 0.0) > 0' not in SOURCE
    assert "_performance_learning_rows" in SOURCE
    assert "_EVIDENCE_PROVISIONAL" in SOURCE
    assert "_EVIDENCE_UNKNOWN" in SOURCE


def test_live_confirmed_calls_carry_provenance_and_identity():
    assert 'evidence_class="BROKER_CONFIRMED_LIVE"' in SOURCE
    assert 'settlement_identity=f"deal:{deal_id}"' in SOURCE
    assert '"campaign_id": pos.get("campaign_id") or pos.get("rolling_campaign_id")' in SOURCE


def test_b1_records_provenance_dimensions_without_retuning_thresholds():
    assert '"evidence_classes": {' in SOURCE
    assert '"conviction": "MIXED_SIM_PRIOR_LIVE_STATS"' in SOURCE
    assert '"atr_price_reset": "CURRENT_MARKET"' in SOURCE
    assert '_B1_MIN_CONVICTION_DELTA  = 2' in SOURCE


def _extract(source, names):
    tree = ast.parse(source)
    ns = {"math": __import__("math"), "json": json}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            exec(compile(ast.Module(body=[node], type_ignores=[]), "formula", "exec"), ns)
    return ns


def _formula_env():
    return {
        "_sim": {"win_moves": {}, "loss_moves": {}, "vol_history": {}},
        "_barbie_overrides": {}, "_EVIDENCE_SIM_PRIOR": "SIM_PRIOR",
        "_SIM_TP_WIN_FRACTION": .82, "_SIM_TP_VOL_FRACTION": .30,
        "_SIM_TP_CAP": .010,
        "_BARBIE_TP_FRAC_MIN": .50, "_BARBIE_TP_FRAC_MAX": 1.50,
        "_SIM_TP_MIN_SAMPLES": 3, "_SIM_TP_FLOOR": .0002,
        "_SIM_STOP_COLD": .002, "_SIM_STOP_FLOOR": .0008,
        "_SIM_STOP_CAP": .005, "_SIM_STOP_VOL_MULT": 2.0,
        "_LEV_FUND_INSTRUMENTS": set(), "_EQUITY_CFD_INSTRUMENTS": set(),
        "_LEV_FUND_STOP_CAP": .04, "_EQUITY_CFD_STOP_CAP": .015,
    }


def test_tp_and_stop_formulas_are_economically_invariant():
    pre = subprocess.check_output(["git", "show", "HEAD:june.py"], text=True, encoding="utf-8")
    post = SOURCE
    names = {"_sim_get_tp", "_sim_get_dynamic_stop"}
    pre_ns = _extract(pre, names); post_ns = _extract(post, names)
    for ns in (pre_ns, post_ns):
        ns.update(_formula_env())
    for sym in ("NATGAS", "COCOA", "HO", "OIL", "SILVER", "WHEAT"):
        for direction in ("long", "short"):
            hist = [0.02, .04, .08, .12, .20, .16, .10, .06, .04, .08]
            for ns in (pre_ns, post_ns):
                ns["_sim"]["vol_history"][sym] = hist
                ns["_sim"]["win_moves"][f"{sym}_{direction}"] = [.001, .002, .003]
                ns["_sim"]["loss_moves"][f"{sym}_{direction}"] = [.0015, .002, .0025]
            assert pre_ns["_sim_get_tp"](sym, direction, 5) == post_ns["_sim_get_tp"](sym, direction, 5)
        for ns in (pre_ns, post_ns):
            ns["_sim"]["vol_history"][sym] = hist
        assert pre_ns["_sim_get_dynamic_stop"](sym) == post_ns["_sim_get_dynamic_stop"](sym)


def test_mpd_dple_build4_and_settlement_wiring_remain_present():
    assert "_mpd_mfe_gate" in SOURCE
    assert "0.5 * _dple_tp_l" in SOURCE
    assert "_PYRAMID_AGG_STOP_PCT" in SOURCE
    assert "_live_settle_primary_exit" in SOURCE
    assert "_live_reconcile_provisional_settlements" in SOURCE
