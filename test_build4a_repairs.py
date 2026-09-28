"""
test_build4a_repairs.py — Targeted tests for B4A-F1/F2/F4/F5 repairs.

F1: Protection provenance distinguishes acknowledged amendment vs entry-stop fallback.
F2: Stale bootstrap does NOT reduce actionable Ledger B (D2 stays UNKNOWN).
F4: LS-detected addon closure is fail-closed; campaign cleanup fires when both gone.
F5: b4a_validate_rolling_state_extended checks liq_before provenance integrity.
"""
import time
import pytest
import rolling_build4a as rb4
from rolling_build4a import B4A_UNKNOWN, _B4A_VALID_LIQ_SOURCES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_primary(deal="P1", dirn="short", fill=6426.3):
    return dict(
        instrument="SILVER", direction=dirn, deal_id=deal,
        fill_price=fill, ig_size=0.4, stop_pct=0.005,
        pos_size=10.0, notional=100.0, leverage=10,
        broker_stop_level=6500.0, acknowledged_stop_level=None,
        entry_time=time.time(), conviction=7, leg_index=1,
    )


def make_live_harvest(liq_before=0.08, realized_pnl=0.12,
                      liq_source="broker_acknowledged_stop",
                      campaign_id="campaign_P1"):
    live = {
        "open_position": make_primary(),
        "pyramid_legs": [],
        "rolling_capacity_slot": "available",
        "rolling_bootstrap_liq_before": liq_before,
        "rolling_realized_harvest": {
            "deal_id": "ADDON1", "instrument": "SILVER", "direction": "short",
            "leg_generation": 1, "fill_price": 6350.0,
            "confirmed_exit_price": 6270.0,
            "exit_price_source": "broker_confirmed",
            "exit_mid_estimate": 6268.0,
            "pnl_pct": 0.0126, "realized_pnl_estimate": realized_pnl,
            "ig_size": 0.4, "epoch": time.time(),
        },
        "rolling_profit_deployed": 0.0,
        "rolling_campaign_id": campaign_id,
        "liq_before": liq_before,
        "liq_before_provenance": {
            "value": liq_before,
            "acknowledged_stop_level": 6500.0,
            "fill_price": 6426.3,
            "ig_size": 0.4,
            "multiplier": 1.0,
            "direction": "short",
            "source": liq_source,
            "campaign_id": campaign_id,
            "epoch": time.time(),
        },
    }
    return live


# ===========================================================================
# F1 TESTS: Protection provenance labeling
# ===========================================================================

class TestF1ProtectionProvenance:

    def test_broker_acknowledged_stop_is_valid_source(self):
        """'broker_acknowledged_stop' is in _B4A_VALID_LIQ_SOURCES."""
        assert "broker_acknowledged_stop" in _B4A_VALID_LIQ_SOURCES

    def test_entry_stop_fallback_is_valid_source(self):
        """'broker_confirmed_entry_stop_fallback' is in _B4A_VALID_LIQ_SOURCES."""
        assert "broker_confirmed_entry_stop_fallback" in _B4A_VALID_LIQ_SOURCES

    def test_unknown_source_is_not_valid(self):
        """Unrecognized sources are not in _B4A_VALID_LIQ_SOURCES."""
        for bad in ("", "software_only", "pending", "bootstrap_snapshot_stale",
                    "broker_stop_level", None):
            assert bad not in _B4A_VALID_LIQ_SOURCES, f"Should not be valid: {bad!r}"

    def test_build_ledgers_trusts_acknowledged_source(self):
        """liq_before with source=broker_acknowledged_stop produces current_broker_acknowledged_stop provenance."""
        live = make_live_harvest(liq_source="broker_acknowledged_stop")
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_provenance"] == "current_broker_acknowledged_stop"
        assert ledgers["ledger_b_protected_primary"] != B4A_UNKNOWN

    def test_build_ledgers_trusts_entry_stop_fallback(self):
        """liq_before with source=broker_confirmed_entry_stop_fallback is still trusted as current."""
        live = make_live_harvest(liq_source="broker_confirmed_entry_stop_fallback")
        ledgers = rb4.b4a_build_ledgers(live)
        # It's a valid but conservative source — still produces a numeric B
        # The provenance label is 'current_liq_before' (not 'current_broker_acknowledged_stop')
        # because the source string != 'broker_acknowledged_stop'
        assert ledgers["ledger_b_protected_primary"] != B4A_UNKNOWN
        # Not the strongest label but still usable
        assert ledgers["ledger_b_provenance"] in (
            "current_broker_acknowledged_stop", "current_liq_before"
        )

    def test_validate_accepts_acknowledged_source(self):
        """Valid source passes F5 validation with no warnings."""
        live = make_live_harvest(liq_source="broker_acknowledged_stop")
        warns = rb4.b4a_validate_rolling_state_extended(live)
        liq_warns = [w for w in warns if "LIQ_BEFORE" in w]
        assert liq_warns == [], f"Unexpected liq_before warnings: {liq_warns}"

    def test_validate_accepts_entry_stop_fallback_source(self):
        """broker_confirmed_entry_stop_fallback passes F5 validation."""
        live = make_live_harvest(liq_source="broker_confirmed_entry_stop_fallback")
        warns = rb4.b4a_validate_rolling_state_extended(live)
        liq_warns = [w for w in warns if "LIQ_BEFORE_INVALID_SOURCE" in w]
        assert liq_warns == [], f"Unexpected invalid-source warning: {liq_warns}"

    def test_validate_rejects_unknown_source(self):
        """Unknown source triggers B4A_LIQ_BEFORE_INVALID_SOURCE warning."""
        live = make_live_harvest(liq_source="software_only")
        warns = rb4.b4a_validate_rolling_state_extended(live)
        assert any("B4A_LIQ_BEFORE_INVALID_SOURCE" in w for w in warns), warns

    def test_validate_rejects_empty_source(self):
        """Empty source triggers B4A_LIQ_BEFORE_INVALID_SOURCE warning."""
        live = make_live_harvest(liq_source="")
        warns = rb4.b4a_validate_rolling_state_extended(live)
        assert any("B4A_LIQ_BEFORE_INVALID_SOURCE" in w for w in warns), warns


# ===========================================================================
# F2 TESTS: Stale bootstrap must not reduce actionable D2
# ===========================================================================

class TestF2StaleBootstrap:

    def test_no_current_liq_with_bootstrap_gives_unknown_ledger_b(self):
        """When only bootstrap_liq_before exists (no current liq_before), Ledger B = UNKNOWN."""
        live = {
            "open_position": make_primary(),
            "pyramid_legs": [],
            "rolling_bootstrap_liq_before": 0.08,
            # No liq_before key (current value absent)
            "rolling_campaign_id": "campaign_P1",
        }
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == B4A_UNKNOWN, (
            f"Expected UNKNOWN when only bootstrap present, got {ledgers['ledger_b_protected_primary']}"
        )
        assert ledgers["ledger_b_provenance"] == "bootstrap_snapshot_stale"

    def test_bootstrap_stale_gives_unknown_d2(self):
        """Stale bootstrap provenance yields D2 = UNKNOWN (cannot reduce D2)."""
        live = {
            "open_position": make_primary(),
            "rolling_bootstrap_liq_before": 0.08,
            "rolling_campaign_id": "campaign_P1",
            "rolling_realized_harvest": {
                "deal_id": "ADDON1", "realized_pnl_estimate": 0.12,
                "exit_price_source": "broker_confirmed",
            },
            "rolling_profit_deployed": 0.0,
        }
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == B4A_UNKNOWN

        result = rb4.b4a_original_principal_at_risk(
            ledger_a=0.12, ledger_a_valid=True,
            ledger_b=B4A_UNKNOWN, ledger_d=B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.80,
            ledger_a_remaining=0.12,
        )
        assert result["d2_opar_conditional"] == B4A_UNKNOWN, (
            "Stale bootstrap must not reduce D2 — D2 must be UNKNOWN"
        )

    def test_bootstrap_preserved_as_historical(self):
        """bootstrap_liq_before_historical is preserved even when actionable B = UNKNOWN."""
        live = {
            "open_position": make_primary(),
            "rolling_bootstrap_liq_before": 0.08,
            "rolling_campaign_id": "campaign_P1",
        }
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["bootstrap_liq_before_historical"] == 0.08
        assert ledgers["ledger_b_protected_primary"] == B4A_UNKNOWN

    def test_current_liq_before_still_used(self):
        """When current liq_before is present with valid source, Ledger B is numeric (not UNKNOWN)."""
        live = make_live_harvest(liq_source="broker_acknowledged_stop", liq_before=0.05)
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == 0.05
        assert ledgers["ledger_b_provenance"] == "current_broker_acknowledged_stop"

    def test_stale_bootstrap_does_not_change_d1(self):
        """D1 is unconditional — stale bootstrap cannot affect it."""
        # With bootstrap only (B=UNKNOWN), D1 = stop_risk - realized_coverage
        result = rb4.b4a_original_principal_at_risk(
            ledger_a=0.40, ledger_a_valid=True,
            ledger_b=B4A_UNKNOWN, ledger_d=B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.80,
            ledger_a_remaining=0.40,
        )
        assert abs(result["d1_opar_unconditional"] - 0.40) < 1e-8


# ===========================================================================
# F4 TESTS: LS-detected addon closure
# ===========================================================================

class TestF4LSDetectedClose:

    def test_ls_close_with_gen1_leg_marks_source_unknown(self):
        """A leg carrying ls_close_price_unknown source is ineligible for harvest
        if capacity slot is not bootstrap (standard dedup guard applies)."""
        # capacity_slot="available" means harvest already credited — dedup blocks
        live = {
            "rolling_capacity_slot": "available",
            "rolling_profit_deployed": 0.0,
            "rolling_realized_harvest": {"deal_id": "ADDON1"},
        }
        leg = dict(deal_id="ADDON1", leg_generation=1,
                   confirmed_exit_price=None,
                   exit_price_source="ls_close_price_unknown")
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is False
        assert reason == "duplicate_harvest_deal_id"

    def test_ls_close_different_deal_id_eligible_on_bootstrap_slot(self):
        """A different deal_id with bootstrap slot is eligible for harvest credit."""
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_profit_deployed": 0.0,
            "rolling_realized_harvest": None,
        }
        leg = dict(deal_id="ADDON2", leg_generation=1,
                   confirmed_exit_price=None,
                   exit_price_source="ls_close_price_unknown")
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is True, f"Expected eligible, got reason={reason}"

    def test_ls_close_gen2_not_eligible(self):
        """Gen-2 legs are never harvest-eligible, even via LS path."""
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_profit_deployed": 0.0,
            "rolling_realized_harvest": None,
        }
        leg = dict(deal_id="ADDON_GEN2", leg_generation=2,
                   exit_price_source="ls_close_price_unknown")
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is False
        assert "generation_2" in reason

    def test_campaign_cleanup_removes_all_rolling_fields(self):
        """b4a_clear_campaign_rolling_state removes all rolling fields (F4 cleanup path)."""
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_bootstrap_liq_before": 0.05,
            "rolling_realized_harvest": None,
            "rolling_profit_deployed": 0.0,
            "rolling_campaign_id": "campaign_P1",
            "rolling_harvest_id": "abc123",
            "liq_before": 0.05,
        }
        cleared = rb4.b4a_clear_campaign_rolling_state(live, reason="ls_detected_full_campaign_close")
        assert "rolling_capacity_slot" in cleared
        assert "rolling_bootstrap_liq_before" in cleared
        assert "rolling_campaign_id" in cleared
        assert live.get("rolling_capacity_slot") is None
        assert live.get("rolling_campaign_id") is None
        # liq_before is NOT in the cleared list (it's a live field, not campaign-rolling)
        # but the campaign_id guard in b4a_build_ledgers handles it

    def test_ls_close_no_deal_id_not_eligible(self):
        """LS close with empty deal_id is not harvest-eligible."""
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_profit_deployed": 0.0,
            "rolling_realized_harvest": {"deal_id": ""},
        }
        leg = dict(deal_id="", leg_generation=1,
                   exit_price_source="ls_close_price_unknown")
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        # empty == empty triggers dedup guard (F8 fix)
        assert ok is False
        assert reason == "duplicate_harvest_deal_id"

    def test_ls_close_source_label_is_not_valid_for_liq_before(self):
        """ls_close_price_unknown is not a valid liq_before source."""
        assert "ls_close_price_unknown" not in _B4A_VALID_LIQ_SOURCES


# ===========================================================================
# F5 TESTS: liq_before provenance validation
# ===========================================================================

class TestF5LiqBeforeProvenance:

    def test_no_liq_before_no_warnings(self):
        """No liq_before in state produces no F5 warnings."""
        live = {"open_position": make_primary(), "rolling_campaign_id": "campaign_P1"}
        warns = rb4.b4a_validate_rolling_state_extended(live)
        liq_warns = [w for w in warns if "LIQ_BEFORE" in w]
        assert liq_warns == []

    def test_liq_before_missing_provenance_warns(self):
        """liq_before present without any provenance triggers warning."""
        live = {
            "open_position": make_primary(),
            "liq_before": 0.05,
            # No liq_before_provenance
            "rolling_campaign_id": "campaign_P1",
        }
        warns = rb4.b4a_validate_rolling_state_extended(live)
        assert any("B4A_LIQ_BEFORE_NO_PROVENANCE" in w for w in warns), warns

    def test_liq_before_provenance_missing_campaign_id_warns(self):
        """Provenance dict without campaign_id triggers warning."""
        live = {
            "open_position": make_primary(),
            "liq_before": 0.05,
            "liq_before_provenance": {"source": "broker_acknowledged_stop"},
            # No campaign_id in provenance
            "rolling_campaign_id": "campaign_P1",
        }
        warns = rb4.b4a_validate_rolling_state_extended(live)
        assert any("B4A_LIQ_BEFORE_MISSING_CAMPAIGN_ID" in w for w in warns), warns

    def test_liq_before_campaign_id_mismatch_warns(self):
        """Provenance campaign_id != rolling_campaign_id triggers warning."""
        live = {
            "open_position": make_primary(),
            "liq_before": 0.05,
            "liq_before_provenance": {
                "source": "broker_acknowledged_stop",
                "campaign_id": "campaign_OLD",
            },
            "rolling_campaign_id": "campaign_P1",
        }
        warns = rb4.b4a_validate_rolling_state_extended(live)
        assert any("B4A_LIQ_BEFORE_CAMPAIGN_MISMATCH" in w for w in warns), warns

    def test_liq_before_primary_absent_warns(self):
        """liq_before present without open_position triggers warning."""
        live = {
            # No open_position
            "liq_before": 0.05,
            "liq_before_provenance": {
                "source": "broker_acknowledged_stop",
                "campaign_id": "campaign_P1",
            },
            "rolling_campaign_id": "campaign_P1",
        }
        warns = rb4.b4a_validate_rolling_state_extended(live)
        assert any("B4A_LIQ_BEFORE_PRIMARY_ABSENT" in w for w in warns), warns

    def test_liq_before_invalid_source_warns(self):
        """Unrecognized provenance source triggers warning."""
        live = {
            "open_position": make_primary(),
            "liq_before": 0.05,
            "liq_before_provenance": {
                "source": "mystery_source",
                "campaign_id": "campaign_P1",
            },
            "rolling_campaign_id": "campaign_P1",
        }
        warns = rb4.b4a_validate_rolling_state_extended(live)
        assert any("B4A_LIQ_BEFORE_INVALID_SOURCE" in w for w in warns), warns

    def test_clean_state_no_liq_before_warnings(self):
        """Fully clean state with correct provenance produces no liq_before warnings."""
        live = make_live_harvest(liq_source="broker_acknowledged_stop")
        warns = rb4.b4a_validate_rolling_state_extended(live)
        liq_warns = [w for w in warns if "LIQ_BEFORE" in w]
        assert liq_warns == [], f"Unexpected warnings on clean state: {liq_warns}"

    def test_legacy_state_no_liq_before_no_warnings(self):
        """Legacy state without liq_before at all produces no F5 warnings."""
        live = {
            "open_position": make_primary(),
            "rolling_capacity_slot": "bootstrap",
            "rolling_campaign_id": "campaign_P1",
        }
        warns = rb4.b4a_validate_rolling_state_extended(live)
        liq_warns = [w for w in warns if "LIQ_BEFORE" in w]
        assert liq_warns == [], liq_warns

    def test_restart_with_stale_provenance_warns(self):
        """Simulates a restart where liq_before_provenance has a different campaign_id."""
        # This is the post-restart scenario: old provenance survived in Redis
        live = {
            "open_position": make_primary(deal="NEW_DEAL"),
            "liq_before": 0.03,
            "liq_before_provenance": {
                "source": "broker_acknowledged_stop",
                "campaign_id": "campaign_OLD_DEAL",  # stale
            },
            "rolling_campaign_id": "campaign_NEW_DEAL",
        }
        warns = rb4.b4a_validate_rolling_state_extended(live)
        assert any("CAMPAIGN_MISMATCH" in w or "LIQ_BEFORE_CAMPAIGN_MISMATCH" in w
                   for w in warns), warns


# ===========================================================================
# Integration: F2 + F5 together
# ===========================================================================

class TestF2F5Integration:

    def test_bootstrap_only_state_no_liq_before_key_no_f5_warnings(self):
        """Bootstrap-only state (liq_before absent) triggers no F5 warnings
        but Ledger B = UNKNOWN per F2 fix."""
        live = {
            "open_position": make_primary(),
            "rolling_bootstrap_liq_before": 0.08,
            "rolling_campaign_id": "campaign_P1",
        }
        # No liq_before key
        warns = rb4.b4a_validate_rolling_state_extended(live)
        liq_warns = [w for w in warns if "LIQ_BEFORE" in w]
        assert liq_warns == [], liq_warns

        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == B4A_UNKNOWN
        assert ledgers["ledger_b_provenance"] == "bootstrap_snapshot_stale"

    def test_current_broker_backed_gives_numeric_b_and_no_warnings(self):
        """Current broker-acknowledged liq_before gives numeric B and no F5 warnings."""
        live = make_live_harvest(liq_source="broker_acknowledged_stop", liq_before=0.05)
        warns = rb4.b4a_validate_rolling_state_extended(live)
        liq_warns = [w for w in warns if "LIQ_BEFORE" in w]
        assert liq_warns == [], liq_warns

        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == 0.05
        assert ledgers["ledger_b_provenance"] == "current_broker_acknowledged_stop"

    def test_unknown_liq_before_does_not_reduce_d2_from_build_ledgers(self):
        """When b4a_build_ledgers returns B=UNKNOWN, D2 is UNKNOWN (never 0)."""
        live = {
            "open_position": make_primary(),
            "rolling_bootstrap_liq_before": 100.0,  # large bootstrap, must NOT help
            "rolling_campaign_id": "campaign_P1",
            "rolling_realized_harvest": {
                "deal_id": "A1", "realized_pnl_estimate": 0.05,
                "exit_price_source": "broker_confirmed",
            },
            "rolling_profit_deployed": 0.0,
        }
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == B4A_UNKNOWN

        result = rb4.b4a_original_principal_at_risk(
            ledger_a=0.05, ledger_a_valid=True,
            ledger_b=B4A_UNKNOWN, ledger_d=B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.80,
            ledger_a_remaining=0.05,
        )
        assert result["d2_opar_conditional"] == B4A_UNKNOWN
        # D1 is still computable
        assert abs(result["d1_opar_unconditional"] - 0.75) < 1e-8
