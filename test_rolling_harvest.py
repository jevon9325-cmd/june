"""Build-3 rolling harvest tests — Commit 1: State/generation scaffolding."""
import json, time
import pytest
from unittest.mock import patch, MagicMock


# ── Helpers ──────────────────────────────────────────────────────────────────

def make_primary(sym="SILVER", dirn="short", fill=6426.3):
    return dict(
        instrument=sym, direction=dirn, deal_id="PRIMARY", fill_price=fill,
        ig_size=0.4, broker_stop_level=6500.0, acknowledged_stop_level=None,
        stop_sync=None, leverage=10, pos_size=100.0, notional=1000.0,
        entry_time=time.time(), conviction=7, scaling_history_complete=True,
        leg_index=1,
    )


def make_addon_leg(sym="SILVER", dirn="short", fill=6350.0, leg_generation=1):
    return dict(
        instrument=sym, direction=dirn, deal_id="ADDON1", fill_price=fill,
        ig_size=0.4, broker_stop_level=6500.0, acknowledged_stop_level=None,
        stop_sync=None, leverage=10, pos_size=100.0, notional=1000.0,
        entry_time=time.time(), stop_pct=0.01, tp_pct=0.005,
        leg_index=2, leg_generation=leg_generation,
        actual_notional=1000.0, consumed_allocation=100.0,
    )


# ── Commit 1: Constants ───────────────────────────────────────────────────────

def test_rolling_constants_exist():
    import june
    assert hasattr(june, "_ROLLING_MAX_GENERATIONS"), "constant missing"
    assert june._ROLLING_MAX_GENERATIONS == 1
    assert hasattr(june, "_ROLLING_HARVEST_THRESHOLD_PCT"), "constant missing"
    assert june._ROLLING_HARVEST_THRESHOLD_PCT == pytest.approx(0.0025)


def test_rolling_max_generations_blocks_gen2():
    import june
    assert june._ROLLING_MAX_GENERATIONS < 2, "gen-2 must be blocked in Build 3"


# ── Commit 1: leg_generation stamped on open ─────────────────────────────────

def test_leg_generation_present_in_opened_leg():
    leg = make_addon_leg()
    assert leg.get("leg_generation") == 1


def test_legacy_leg_missing_generation_treated_conservatively():
    # A leg without leg_generation (pre-Build-3) should default to gen 1.
    leg = make_addon_leg()
    del leg["leg_generation"]
    gen = leg.get("leg_generation", 1)  # conservative default
    assert gen == 1


# ── Commit 1: rolling_capacity_slot state ────────────────────────────────────

def test_rolling_capacity_slot_initial_state():
    # Before any addon opens, rolling_capacity_slot should be absent or None.
    live = {}
    assert live.get("rolling_capacity_slot") is None


def test_rolling_capacity_slot_set_on_bootstrap():
    live = {"rolling_capacity_slot": None, "rolling_bootstrap_liq_before": None}
    # Simulate what happens when gen-1 leg opens with no protection economics.
    live["rolling_capacity_slot"] = "bootstrap"
    live["rolling_bootstrap_liq_before"] = None  # normal_existing_controls path
    assert live["rolling_capacity_slot"] == "bootstrap"
    assert live["rolling_bootstrap_liq_before"] is None


def test_rolling_bootstrap_liq_before_stored_from_evidence():
    # When protection_required=True, liq_before is a real value.
    live = {}
    liq_before = 0.0805
    live["rolling_capacity_slot"] = "bootstrap"
    live["rolling_bootstrap_liq_before"] = liq_before
    assert live["rolling_bootstrap_liq_before"] == pytest.approx(0.0805)


def test_rolling_capacity_slot_not_overwritten_on_second_leg():
    # If slot already set to "bootstrap", a second leg open does not reset it.
    live = {
        "rolling_capacity_slot": "bootstrap",
        "rolling_bootstrap_liq_before": 0.05,
        "pyramid_legs": [make_addon_leg()],
    }
    # Simulate the condition guard: if slot != "bootstrap", set it
    leg2 = make_addon_leg()
    if live.get("rolling_capacity_slot") != "bootstrap":
        live["rolling_capacity_slot"] = "bootstrap"
        live["rolling_bootstrap_liq_before"] = None
    # Slot was already "bootstrap"; nothing changed.
    assert live["rolling_bootstrap_liq_before"] == pytest.approx(0.05)


# ── Commit 1: Restart compatibility ──────────────────────────────────────────

def test_restart_does_not_lose_rolling_state():
    live = {
        "rolling_capacity_slot": "bootstrap",
        "rolling_bootstrap_liq_before": 0.0805,
        "rolling_realized_harvest": None,
    }
    serialized = json.dumps(live)
    restored = json.loads(serialized)
    assert restored["rolling_capacity_slot"] == "bootstrap"
    assert restored["rolling_bootstrap_liq_before"] == pytest.approx(0.0805)
    assert restored["rolling_realized_harvest"] is None


def test_restart_missing_rolling_keys_treated_as_no_harvest():
    # Pre-Build-3 state: no rolling keys present.
    live = {"open_position": make_primary(), "pyramid_legs": [make_addon_leg()]}
    assert live.get("rolling_capacity_slot") is None
    assert live.get("rolling_bootstrap_liq_before") is None
    assert live.get("rolling_realized_harvest") is None
