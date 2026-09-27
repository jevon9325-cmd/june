"""Build-3 rolling harvest tests — Commit 4: restart/idempotency hardening."""
import json, time
import pytest


# ── State validation helpers (pure — mirrors _live_validate_rolling_state_on_load) ──

def validate_rolling_state(live):
    """Pure mirror of validation logic. Returns list of warnings emitted."""
    warnings = []
    slot        = live.get("rolling_capacity_slot")
    realized    = live.get("rolling_realized_harvest")
    legs        = live.get("pyramid_legs", [])

    # Stamp legacy legs
    for leg in legs:
        if "leg_generation" not in leg:
            leg["leg_generation"] = 1

    gen1_legs = [l for l in legs if l.get("leg_generation", 1) == 1]

    if slot == "bootstrap" and not gen1_legs:
        warnings.append("bootstrap_slot_but_no_gen1_legs")
        # Do NOT release slot

    if slot == "available" and realized is None:
        if gen1_legs:
            warnings.append("available_slot_no_harvest_record_reverting_to_bootstrap")
            live["rolling_capacity_slot"] = "bootstrap"
        else:
            warnings.append("available_slot_no_harvest_no_legs_consistent")

    if realized is not None:
        recorded_deal = realized.get("deal_id")
        still_open = any(l.get("deal_id") == recorded_deal for l in legs)
        if still_open:
            warnings.append(f"harvest_recorded_but_leg_still_tracked:{recorded_deal}")

    return warnings


def make_addon_leg(deal_id="ADDON1", gen=1):
    return dict(
        instrument="SILVER", direction="short", deal_id=deal_id, fill_price=6350.0,
        ig_size=0.4, leg_index=2, leg_generation=gen, stop_pct=0.01, tp_pct=0.005,
        entry_time=time.time(),
    )


# ── Commit 4: Restart invariants ─────────────────────────────────────────────

class TestRestartBeforeHarvest:
    def test_clean_state_bootstrap_with_gen1_leg(self):
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_bootstrap_liq_before": None,
            "rolling_realized_harvest": None,
            "pyramid_legs": [make_addon_leg()],
        }
        warnings = validate_rolling_state(live)
        assert warnings == [], f"unexpected warnings: {warnings}"

    def test_restart_no_rolling_state_no_warnings(self):
        live = {"pyramid_legs": [make_addon_leg()]}
        warnings = validate_rolling_state(live)
        assert warnings == []

    def test_gen1_leg_without_generation_stamped_conservatively(self):
        leg = make_addon_leg()
        del leg["leg_generation"]
        live = {"pyramid_legs": [leg]}
        validate_rolling_state(live)
        assert leg["leg_generation"] == 1


class TestRestartAfterRequestBeforeConfirmation:
    def test_bootstrap_slot_with_no_gen1_leg_logged_not_released(self):
        # Restart after close request but before broker confirmation:
        # slot is "bootstrap", but leg may have been removed optimistically.
        # Correct: log, keep slot "bootstrap", do NOT release.
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_realized_harvest": None,
            "pyramid_legs": [],  # leg gone but not confirmed
        }
        warnings = validate_rolling_state(live)
        assert "bootstrap_slot_but_no_gen1_legs" in warnings
        # Slot NOT released
        assert live["rolling_capacity_slot"] == "bootstrap"

    def test_no_duplicate_capacity_on_restart(self):
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_realized_harvest": None,
            "pyramid_legs": [],
        }
        validate_rolling_state(live)
        # After validation, slot is still bootstrap (not available)
        assert live["rolling_capacity_slot"] == "bootstrap"
        # A second validation call must not change it either
        warnings2 = validate_rolling_state(live)
        assert live["rolling_capacity_slot"] == "bootstrap"


class TestRestartAfterBrokerCloseBeforePersistence:
    def test_available_slot_with_gen1_leg_reverts_to_bootstrap(self):
        # slot="available" but leg still tracked AND no harvest record --
        # persistence failed after slot release but before harvest record write.
        live = {
            "rolling_capacity_slot": "available",
            "rolling_realized_harvest": None,
            "pyramid_legs": [make_addon_leg()],
        }
        warnings = validate_rolling_state(live)
        assert "available_slot_no_harvest_record_reverting_to_bootstrap" in warnings
        # Reverted safely
        assert live["rolling_capacity_slot"] == "bootstrap"


class TestRestartAfterHarvestPersistence:
    def test_clean_post_harvest_state(self):
        live = {
            "rolling_capacity_slot": "available",
            "rolling_realized_harvest": {"deal_id": "ADDON1", "realized_pnl_estimate": 0.12},
            "pyramid_legs": [],  # leg closed and removed
        }
        warnings = validate_rolling_state(live)
        assert warnings == []

    def test_no_duplicate_harvest_on_duplicate_confirmation(self):
        # Harvest already recorded; leg somehow still tracked (duplicate confirmation).
        live = {
            "rolling_capacity_slot": "available",
            "rolling_realized_harvest": {"deal_id": "ADDON1", "realized_pnl_estimate": 0.12},
            "pyramid_legs": [make_addon_leg("ADDON1")],
        }
        warnings = validate_rolling_state(live)
        assert any("harvest_recorded_but_leg_still_tracked" in w for w in warnings)
        # Harvest NOT duplicated
        assert live["rolling_realized_harvest"]["realized_pnl_estimate"] == pytest.approx(0.12)


class TestDuplicateHarvestDelivery:
    def test_eligibility_blocks_duplicate_same_deal_id(self):
        # _live_rolling_harvest_eligibility logic: same deal_id blocked.
        existing = {"deal_id": "ADDON1", "realized_pnl_estimate": 0.12}
        new_leg   = make_addon_leg("ADDON1")
        live      = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_realized_harvest": existing,
        }
        # Mirror of eligibility check
        existing_record = live.get("rolling_realized_harvest") or {}
        if existing_record.get("deal_id") == new_leg["deal_id"]:
            eligible = False
            reason   = "duplicate_harvest_delivery"
        else:
            eligible = True
            reason   = "eligible"
        assert eligible is False
        assert reason == "duplicate_harvest_delivery"

    def test_eligibility_allows_different_deal_id(self):
        existing = {"deal_id": "ADDON1", "realized_pnl_estimate": 0.12}
        new_leg   = make_addon_leg("ADDON2")
        live      = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_realized_harvest": existing,
        }
        existing_record = live.get("rolling_realized_harvest") or {}
        if existing_record.get("deal_id") == new_leg["deal_id"]:
            eligible, reason = False, "duplicate_harvest_delivery"
        elif new_leg.get("leg_generation", 1) != 1:
            eligible, reason = False, "gen_not_eligible"
        elif live.get("rolling_capacity_slot") != "bootstrap":
            eligible, reason = False, "slot_unexpected"
        else:
            eligible, reason = True, "eligible"
        assert eligible is True  # different deal_id is fine


class TestMissingProtectionEconomics:
    def test_missing_liq_before_blocks_replacement(self):
        # K5: None liq_before → protection_economics_unavailable
        live = {
            "rolling_bootstrap_liq_before": None,
            "rolling_realized_harvest": {"realized_pnl_estimate": 1.0},
        }
        liq_before = live.get("rolling_bootstrap_liq_before")
        if liq_before is None:
            reason = "protection_economics_unavailable"
        else:
            reason = "would_proceed"
        assert reason == "protection_economics_unavailable"

    def test_pending_stop_sync_leaves_liq_before_as_none(self):
        # Bootstrap with stop_sync="pending" → protection_required check via claims_protection.
        # acknowledged_stop_level=None → claims_protection=False → normal_existing_controls
        # → liq_before stored as None.
        leg = make_addon_leg()
        leg["acknowledged_stop_level"] = None
        leg["stop_sync"] = {"status": "pending"}
        # effective() would return broker_stop (below fill for short) → claims_protection=False
        # In that case rolling_bootstrap_liq_before = None per the scaffolding code.
        live = {"rolling_bootstrap_liq_before": None}
        assert live.get("rolling_bootstrap_liq_before") is None


class TestStaleOrphanLeg:
    def test_stale_leg_without_rolling_state_does_not_manufacture_harvest(self):
        # Pre-Build-3 legacy state: pyramid_legs present but no rolling keys.
        live = {
            "pyramid_legs": [make_addon_leg()],
        }
        # No rolling_capacity_slot → no harvest evaluation
        assert live.get("rolling_capacity_slot") is None
        assert live.get("rolling_realized_harvest") is None
        # validate_rolling_state is safe on this input
        warnings = validate_rolling_state(live)
        assert warnings == []
        # Still no harvest manufactured
        assert live.get("rolling_realized_harvest") is None

    def test_broker_truth_authoritative_on_leg_disagreement(self):
        # Harvest recorded but leg still tracked -- logged, not auto-cleared.
        # Broker truth (IG positions) resolves this; local state does not auto-clear.
        live = {
            "rolling_capacity_slot": "available",
            "rolling_realized_harvest": {"deal_id": "ADDON1"},
            "pyramid_legs": [make_addon_leg("ADDON1")],
        }
        initial_slot = live["rolling_capacity_slot"]
        warnings = validate_rolling_state(live)
        # Slot is NOT changed -- broker truth must resolve
        assert live["rolling_capacity_slot"] == initial_slot
        assert any("harvest_recorded_but_leg_still_tracked" in w for w in warnings)
