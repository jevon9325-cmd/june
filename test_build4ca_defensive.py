"""Build 4C-A focused + characterization tests: defensive opportunity gate repair.

Offline AST extraction of the production _live_try_entry (no bot import, no I/O).
Proves:
  - BEFORE-state characterization (A-H)  [some become NEW-behavior after patch]
  - T1-T18 defensive repair behavior
The harness drives _live_try_entry with a permissive downstream funnel and
uses _live_open_position as the terminal admission sentinel, so we can isolate
the effect of each gate.
"""
import json
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from test_broker_identity import execute, function


# Sentinel raised by the mocked _live_open_position so we can detect admission
class _Admitted(Exception):
    pass


def _base_ns(*, global_mode="normal", instrument_mode=None, regime="neutral",
             conv=9, candidate="GOLD", chg=0.5,
             spread_ok=True, sar_blocked=False, observer=None,
             exhausted=False, htf_block=False, gate15_block=False,
             live_enabled=True, kill=False, balance=137.13):
    """Build a namespace where every downstream gate defaults to PASS, so each
    test can flip exactly one gate. _live_open_position raises _Admitted."""
    live = dict(
        balance=balance, balance_total=balance, skimmed_total=0.0,
        global_mode=global_mode,
        instrument_mode=(instrument_mode or {}),
        open_position=None, pyramid_legs=[], pyramid_entry_pending=None,
        orphan_suspected=None, manual_review_required=None,
        pause_expiry={}, instrument_cooldown={},
    )
    sig = {"change_5m": chg, "direction": "bull" if chg > 0 else "bear",
           "price": 4150.0, "spread_pct": 0.02, "change_15m": (chg),
           "spread_alert": False, "spread_atr_wide": False,
           "spread_atr_ratio": 0.1}
    signals = {candidate: sig}

    def _open(*a, **k):
        raise _Admitted()

    # Production _live_select_instrument enforces _live_perf_blocked internally;
    # mirror that so SAR/perf blocks are exercised through selection.
    def _select(_signals, _regime):
        return [] if sar_blocked else [candidate]

    ns = dict(
        _live=live, json=json,
        time=SimpleNamespace(time=lambda: 1000.0),
        _LIVE_MIN_CONVICTION=4,
        _LIVE_DEF_CONVICTION_FLOOR=5,   # present post-patch; harmless pre-patch (unused)
        _METALS_INSTRUMENTS=set(), _CONTINUOUS_INSTRUMENTS={candidate},
        _HTF_INSTRUMENTS=set(), _HTF_OPPOSITION_FLOOR={},
        _direct_cfd_signals={},
        _live_spread_block_cooldown={},
        # history entries are (ts, px) tuples (production shape)
        _history={candidate: [(0, 4150.0)] * 20},
        _SIM_1M_MIN_REVERSAL=99.0,      # effectively disables 1m anti-reversal
        _SIM_15M_DEADZONE=0.0,
        _EXHAUST_RATIO_REDUCE=2.5, _EXHAUST_RATIO_BLOCK=3.5,
        _LIVE_PHASE_GATE_BAL=200.0,     # bal 137 < gate -> phase lev pass-through
        SPREAD_ATR_THRESHOLD=1.0,
        _live_margin={}, _sim_min_notional={candidate: 1.0},
        _fallback_epics=set(),
        # Treat candidate as FX so the commodity margin/minDeal prechecks
        # short-circuit and we reach the terminal _live_open_position cleanly.
        _live_fx_instruments={candidate}, _live_equity_cfd=set(),
        _live_lot_sizes={}, _LIVE_LOT_SIZE_FX=1.0, _live_min_deal={candidate: 1.0},
        _june_live_trading_enabled=live_enabled and not kill,
        # --- downstream helpers, default PASS ---
        _live_select_instrument=Mock(side_effect=_select),
        _live_is_eligible=Mock(return_value=True),
        _is_metals_weekend_closure=Mock(return_value=False),
        is_weekend_closure=Mock(return_value=False),
        _compute_atr_5m=Mock(return_value=(1.0, False)),
        _spread_atr_threshold=Mock(return_value=(0.01 if not spread_ok else 100.0)),
        _price_n_minutes_ago=Mock(return_value=None),
        _sim_15m_gate_mode=Mock(return_value=("relaxed", 1.0)),
        _sim_get_threshold=Mock(return_value=0.0),
        _sim_regime_weight=Mock(return_value=1.0),
        _sim_combo_key=lambda s, d: f"{s}_{d}",
        _sim_combo_wr_gate=Mock(return_value=(False, 0, "")),
        _sim_conviction_gauge=Mock(return_value=conv),
        _compute_htf_alignment=Mock(return_value=("neutral", 0, "")),
        _htf_combo_gate=Mock(return_value=("", 0, "")),
        _htf_opposed_floor_gate=Mock(return_value=(bool(htf_block), "htf")),
        _exhaustion_ratio=Mock(return_value=(9.9 if exhausted else 0.1)),
        _live_write_ex_ratio_obs=Mock(),
        _live_get_observer=Mock(return_value=observer),
        _live_perf_last_won=Mock(return_value=False),
        _live_clear_observer=Mock(),
        _live_perf_blocked=Mock(return_value=bool(sar_blocked)),
        _live_write_block_log=Mock(),
        _live_shadow_obs_blocked=Mock(),
        _sim_conviction_leverage=Mock(return_value=3),
        _live_phase_leverage=Mock(return_value=3),
        _ig_margin_to_max_lev=Mock(return_value=3),
        _live_tier_risk_pct=Mock(return_value=0.8),
        _live_tier_name=Mock(return_value="Seedling"),
        _live_macro_confluence=Mock(return_value=(1.0, 0, "", False)),
        _sim_check_min_feasible=Mock(return_value=True),
        _live_log=Mock(), _live_observe=Mock(),
        _live_open_position=Mock(side_effect=_open),
    )
    return ns, signals


def _run(ns, signals, regime):
    execute([function("_live_try_entry")], ns)
    admitted = False
    try:
        ns["_live_try_entry"](signals, regime)
    except _Admitted:
        admitted = True
    return admitted


# We patch the extracted function to stop right after selection for reachability
# probes by checking whether _live_select_instrument was called.
def _select_called(ns):
    return ns["_live_select_instrument"].called


# ─────────────────────────── Characterization (Phase 1) ────────────────────────
# NOTE: These assert the POST-PATCH intended behavior. Pre-patch, B/T-series fail
# by design (proving the tests have teeth); this file is committed WITH the patch.

def test_A_normal_neutral_reaches_selection():
    ns, sig = _base_ns(global_mode="normal", regime="neutral", conv=9)
    _run(ns, sig, "neutral")
    assert _select_called(ns), "NORMAL+neutral must reach instrument selection"


def test_C_defensive_directional_reaches_selection():
    ns, sig = _base_ns(global_mode="defensive", regime="bull", conv=9, chg=0.5)
    _run(ns, sig, "bull")
    assert _select_called(ns), "DEFENSIVE+directional must reach selection"


# ─────────────────────────── Focused (Phase 5) ─────────────────────────────────

def test_B4CA_defensive_neutral_now_reaches_selection():
    """Core repair: DEFENSIVE+neutral no longer returns before selection."""
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=9)
    _run(ns, sig, "neutral")
    assert _select_called(ns), "DEFENSIVE+neutral must now reach selection (4C-A)"


def test_T1_defensive_weak_candidate_rejected():
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=2)
    assert _run(ns, sig, "neutral") is False


def test_T2_defensive_below_defensive_floor_above_normal_rejected():
    # conv=4 == normal min, but below defensive floor 5 -> rejected in defensive
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=4)
    assert _run(ns, sig, "neutral") is False


def test_T3_defensive_strong_candidate_admitted():
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=6)
    assert _run(ns, sig, "neutral") is True


def test_T4_spread_atr_still_blocks_in_defensive():
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=9,
                       spread_ok=False)
    assert _run(ns, sig, "neutral") is False


def test_T5_sar_perf_block_still_blocks():
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=9,
                       sar_blocked=True)
    assert _run(ns, sig, "neutral") is False


def test_T6_observer_restriction_still_blocks():
    # observer 'moderate' -> floor = 4*2.0 = 8; conv 6 < 8 -> blocked even though
    # conv clears the defensive floor of 5.
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=6,
                       observer="moderate")
    assert _run(ns, sig, "neutral") is False


def test_T7_exhaustion_still_blocks():
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=9,
                       exhausted=True)
    assert _run(ns, sig, "neutral") is False


def test_T8_htf_confirmation_failure_still_blocks():
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=9,
                       htf_block=True)
    ns["_HTF_INSTRUMENTS"] = {"GOLD"}
    ns["_HTF_OPPOSITION_FLOOR"] = {"GOLD": 5}
    assert _run(ns, sig, "neutral") is False


def test_T11_normal_mode_admission_unchanged():
    # NORMAL: no defensive floor; conv 4 (>=1) should be admitted (all gates pass)
    ns, sig = _base_ns(global_mode="normal", regime="neutral", conv=4)
    assert _run(ns, sig, "neutral") is True


def test_T12_defensive_directional_not_looser():
    # DEFENSIVE + directional still enforces the stronger floor (conv 4 < 5 -> reject)
    ns, sig = _base_ns(global_mode="defensive", regime="bull", conv=4, chg=0.5)
    assert _run(ns, sig, "bull") is False


def test_T16_no_bypass_of_select_instrument():
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=9)
    _run(ns, sig, "neutral")
    assert ns["_live_select_instrument"].called


def test_T17_telemetry_records_defensive_reject():
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=4)
    _run(ns, sig, "neutral")
    events = [c.args[0] for c in ns["_live_observe"].call_args_list if c.args]
    assert "defensive_conviction_reject" in events


def test_T17b_telemetry_records_proceed():
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=9)
    _run(ns, sig, "neutral")
    events = [c.args[0] for c in ns["_live_observe"].call_args_list if c.args]
    assert "defensive_eval_proceed" in events


def test_T9_T10_T18_killswitch_dryrun_blocks_admission():
    # _june_live_trading_enabled False (CB fired OR kill switch) -> DRY-RUN gate
    # stops before _live_open_position even for a strong defensive candidate.
    ns, sig = _base_ns(global_mode="defensive", regime="neutral", conv=9,
                       live_enabled=False)
    assert _run(ns, sig, "neutral") is False
    assert not ns["_live_open_position"].called


def test_instrument_defensive_neutral_reaches_and_floors():
    # instrument-mode defensive on the selected candidate; conv 4 < 5 -> reject,
    # but it must have PROCEEDED past selection (repair) not hard-returned.
    ns, sig = _base_ns(global_mode="normal",
                       instrument_mode={"GOLD": "defensive"},
                       regime="neutral", conv=4)
    assert _run(ns, sig, "neutral") is False
    assert ns["_live_select_instrument"].called
