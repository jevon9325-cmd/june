"""Build 4C-D instrument-universe capability and fallback hygiene tests.

No broker/Redis/service access. Tests extract only pure/candidate helpers from the
current deployed source and use broker-audit-shaped metadata fixtures.
"""
import ast
from pathlib import Path
from types import SimpleNamespace

SOURCE_PATH = next(p for p in (Path("_prod_universe_june.py"), Path("june.py")) if p.exists())
SOURCE = SOURCE_PATH.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)


def extract(names, ns):
    for node in TREE.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            exec(compile(ast.Module(body=[node], type_ignores=[]), "extract_4cd_universe", "exec"), ns)
    return ns


def load_selection(eligible, statuses, margins):
    logs = []
    ns = {
        "_live_market_status": dict(statuses),
        "_live_capability_skip_logged": set(),
        "_live_margin": dict(margins),
        "_live_fx_instruments": set(),
        "_live": {"balance": 135.83, "balance_total": 135.83, "skimmed_total": 0.0,
                  "instrument_cooldown": {}, "pause_expiry": {}},
        "_sim_eligible": set(eligible),
        "_SIM_LEV_RANGES": {"sprout": (3, 10)},
        "_sim_is_eligible": lambda *a, **k: True,
        "_ig_margin_to_max_lev": lambda rate, ceiling: ceiling,
        "_live_log": logs.append,
        "_sim_get_threshold": lambda *a, **k: 0.05,
        "_sim_vol_bucket": lambda v: "high" if v > 0.5 else "mid",
        "time": SimpleNamespace(time=lambda: 1_790_743_000.0),
        "_live_is_paused": lambda combo: False,
        "_sim_combo_key": lambda s, d: f"{s}_{d}",
        "_sim_combo_wr_gate": lambda s, d: (False, 0, ""),
        "_sim_regime_weight": lambda s, d: 1.0,
        "_sim_corr_weight": lambda s, d, sig: 1.0,
        "_live_perf_blocked": lambda s: False,
    }
    extract({"_live_instrument_capability", "_live_is_eligible",
             "_live_select_instrument"}, ns)
    return ns, logs


def _signals(*symbols):
    return {s: {"change_5m": 5.0 if s == "ARM" else 1.0,
                 "direction": "bull", "spread_alert": False,
                 "spread_atr_wide": False} for s in symbols}


# ───────────────────────────── capability classification ──────────────────────
def test_edits_only_is_structurally_unexecutable():
    ns, logs = load_selection({"PBI"}, {"PBI": "EDITS_ONLY"}, {"PBI": 0.20})
    assert ns["_live_instrument_capability"]("PBI") == (False, "market_edits_only")
    assert ns["_live_is_eligible"]("PBI") is False
    assert any("symbol=PBI reason=market_edits_only" in x for x in logs)


def test_missing_margin_is_rejected_before_ranking():
    ns, logs = load_selection({"ARM"}, {}, {})
    assert ns["_live_instrument_capability"]("ARM") == (False, "missing_margin")
    assert ns["_live_is_eligible"]("ARM") is False
    assert any("symbol=ARM reason=missing_margin" in x for x in logs)


def test_valid_tradeable_margin_capability_remains_eligible():
    ns, _ = load_selection({"GOLD"}, {"GOLD": "TRADEABLE"}, {"GOLD": 0.005})
    assert ns["_live_instrument_capability"]("GOLD") == (True, None)
    assert ns["_live_is_eligible"]("GOLD") is True


def test_current_closed_market_is_skipped_with_temporary_reason():
    ns, _ = load_selection({"SLB"}, {"SLB": "CLOSED"}, {"SLB": 0.20})
    assert ns["_live_instrument_capability"]("SLB") == (False, "market_temporarily_closed")


def test_temporary_closed_symbol_reopens_without_blacklist_state():
    ns, _ = load_selection({"SLB"}, {"SLB": "CLOSED"}, {"SLB": 0.20})
    assert ns["_live_is_eligible"]("SLB") is False
    ns["_live_market_status"]["SLB"] = "TRADEABLE"
    assert ns["_live_is_eligible"]("SLB") is True


def test_fx_capability_metadata_does_not_override_existing_fx_live_exclusion():
    ns, _ = load_selection({"EURUSD"}, {"EURUSD": "TRADEABLE"}, {"EURUSD": 0.02})
    ns["_live_fx_instruments"].add("EURUSD")
    assert ns["_live_instrument_capability"]("EURUSD") == (True, None)
    # Existing _live_is_eligible structural FX policy remains separately enforced.
    ns["_sim_is_eligible"] = lambda *a, **k: True
    assert ns["_live_is_eligible"]("EURUSD") is False


def test_capability_reason_logging_is_bounded_per_symbol_reason():
    ns, logs = load_selection({"ARM"}, {}, {})
    assert ns["_live_is_eligible"]("ARM") is False
    assert ns["_live_is_eligible"]("ARM") is False
    assert len([x for x in logs if "symbol=ARM" in x]) == 1

# ───────────────────────────── ranking / fallback ────────────────────────────
def test_dead_rank_one_falls_through_to_valid_rank_two():
    ns, _ = load_selection({"ARM", "GOLD"}, {"GOLD": "TRADEABLE"}, {"GOLD": 0.005})
    ranked = ns["_live_select_instrument"](_signals("ARM", "GOLD"), "bull")
    assert ranked == ["GOLD"]


def test_edits_only_rank_one_falls_through_to_valid_rank_two():
    ns, _ = load_selection({"PBI", "GOLD"}, {"PBI": "EDITS_ONLY", "GOLD": "TRADEABLE"},
                           {"PBI": 0.20, "GOLD": 0.005})
    ranked = ns["_live_select_instrument"](_signals("PBI", "GOLD"), "bull")
    assert ranked == ["GOLD"]


def test_valid_candidate_is_not_lost_when_other_symbol_has_missing_margin():
    ns, _ = load_selection({"SMCI", "GOLD"}, {"GOLD": "TRADEABLE"}, {"GOLD": 0.005})
    ranked = ns["_live_select_instrument"](_signals("SMCI", "GOLD"), "bull")
    assert ranked == ["GOLD"]


def test_selection_does_not_return_structurally_dead_candidate():
    ns, _ = load_selection({"SPXU"}, {"SPXU": "EDITS_ONLY"}, {"SPXU": 0.20})
    assert ns["_live_select_instrument"](_signals("SPXU"), "neutral") == []

# ───────────────────────────── source contracts ──────────────────────────────
def test_static_universe_remains_36_symbols_and_no_additions_were_guessed():
    node = next(n for n in TREE.body if isinstance(n, (ast.Assign, ast.AnnAssign))
                and ((isinstance(n, ast.Assign)
                     and any(isinstance(t, ast.Name) and t.id == "INSTRUMENTS" for t in n.targets))
                     or (isinstance(n, ast.AnnAssign)
                         and isinstance(n.target, ast.Name) and n.target.id == "INSTRUMENTS")))
    value = ast.literal_eval(node.value)
    assert len(value) == 36
    assert "SOXL" not in value and "SOXS" not in value and "DFEN" not in value


def test_direct_map_discovery_is_bounded_and_not_an_unbounded_loop():
    source = SOURCE
    start = source.index("def _maybe_discover_cfd")
    end = source.index("def _startup_discovery_pass")
    body = source[start:end]
    assert "DIRECT_CFD_SEARCH_INTERVAL" in body
    assert "DIRECT_CFD_CONFIRMED_MISS_TTL" in body
    assert "return  # one search per invocation" in body


def test_newly_discovered_direct_cfd_is_queued_for_existing_backfill():
    source = SOURCE
    start = source.index("def _maybe_discover_cfd")
    end = source.index("def _startup_discovery_pass")
    body = source[start:end]
    assert "_notional_pending.append((base, epic))" in body
    assert "discovered_queued_for_backfill" in body


def test_market_status_is_refreshed_from_static_and_direct_metadata():
    source = SOURCE
    assert "_live_market_status[_price_sym] = _market_status" in source
    assert "_live_market_status[base] = _direct_status" in source
    assert "_live_market_status[sym] = _market_status" in source


def test_structural_capability_gate_precedes_live_concentration_math():
    source = SOURCE
    start = source.index("def _live_is_eligible")
    end = source.index("def _live_is_paused")
    body = source[start:end]
    assert body.index("_live_instrument_capability") < body.index("_sim_is_eligible")
    assert "reason={_cap_reason}" in body


def test_candidate_ranking_uses_capability_filtered_eligibility():
    source = SOURCE
    start = source.index("def _live_select_instrument")
    end = source.index("# ── Entry logic", start)
    body = source[start:end]
    assert "if not _live_is_eligible(sym):" in body
    assert body.index("_live_is_eligible") < body.index("candidates.append")


def test_live_fallback_attempts_remain_bounded():
    source = SOURCE
    start = source.index("def _live_try_entry")
    end = source.index("def _live_shadow_obs_blocked", start)
    body = source[start:end]
    assert body.count("len(_skip) <= 3") >= 3


def test_temporary_status_is_not_persisted_as_permanent_blacklist():
    source = SOURCE
    assert "_live_market_status" in source
    assert "june_untradeable" not in source
    assert "permanent_blacklist" not in source


def test_c1_quote_scale_guard_remains_present_and_unchanged():
    assert "_SIM_SCALE_RATIO_MAX = 8.0" in SOURCE
    assert 'SIM_INVALID_SCALE' in SOURCE
    assert "def _sim_scale_continuity_ok" in SOURCE


def test_4cd_daily_lifecycle_remains_present_and_unchanged():
    assert "def _live_expire_prior_epoch_global_defensive" in SOURCE
    assert "balance_day_start_date" in SOURCE
    assert "global_mode_reference" in SOURCE


def test_strategy_risk_constants_remain_unchanged():
    assert "_SIM_TP_WIN_WINDOW   = 10" in SOURCE
    assert "_SIM_STOP_VOL_MULT   = 2.0" in SOURCE
    assert "_LIVE_CIRCUIT_BREAKER_PCT = -0.05" in SOURCE
    assert "_LIVE_DEF_PCT              = 0.025" in SOURCE


def test_sessions_remain_broad_and_no_four_symbol_limit_returned():
    assert "def _current_sub_session" in SOURCE
    assert "WHOLE tradable universe" in SOURCE
    assert "_SESSION_BOUNDARY_OVERRIDES" in SOURCE


def test_b1_c_build4_state_paths_remain_in_source():
    for marker in ("_live_b1_reentry_allowed", "_evidence_class",
                    "_sim_invalidate_scale_break", "rolling_capacity_slot"):
        assert marker in SOURCE


def test_no_new_universe_addition_requires_unverified_metadata():
    # This pass deliberately makes no additions; all direct additions remain
    # broker-discovered/validated rather than guessed in static source.
    assert '"SOXL":' not in SOURCE
    assert '"XRP":' not in SOURCE
