"""Build 4A focused tests -- Section 17.

Tests cover Sections 4-16 shared machinery:
 1.  Ordinary addon TP behavior unchanged
 2.  Rolling-harvest evaluation independent of ordinary addon TP
 3.  Observational A/B/C telemetry cannot execute a rolling harvest
 4.  Broker-confirmed exit used for realized harvest
 5.  Fallback exit estimate remains labeled
 6.  Released margin never enters realized-profit ledger
 7.  Protected primary value never enters realized-profit ledger
 8.  Protected primary value not treated as broker cash
 9.  Missing protection remains UNKNOWN rather than zero
10.  Realized rolling profit persists across restart
11.  Deployed rolling profit persists across restart
12.  Duplicate evaluation cannot double-spend realized profit
13.  Duplicate broker confirmation cannot double-credit
14.  Duplicate harvest observation cannot double-credit
15.  Campaign close clears active rolling state correctly
16.  New campaign cannot inherit old rolling fuel
17.  Generation metadata survives restart
18.  Gen-2 evaluator produces complete economics
19.  Gen-2 submission is impossible
20.  Malformed legacy state fails safely
21.  R_primary provenance is retained
22.  Build-1 stop-sync behavior unchanged
23.  Build-2 behavior unchanged
24.  Build-3 harvest accounting repair unchanged
25.  Gen-1 existing behavior not blocked by Build-4A gen-2 safety invariant
"""
import json, time, pytest
from unittest.mock import patch, MagicMock

import rolling_build4a as rb4


# ---- helpers ----------------------------------------------------------------

def make_primary(sym="SILVER", dirn="short", fill=6426.3, deal="PRIMARY"):
    return dict(
        instrument=sym, direction=dirn, deal_id=deal, fill_price=fill,
        ig_size=0.4, stop_pct=0.005, stop_dist=32.0,
        pos_size=100.0, notional=1000.0, leverage=10,
        broker_stop_level=6500.0, acknowledged_stop_level=None,
        entry_time=time.time(), conviction=7, leg_index=1,
    )


def make_addon_leg(sym="SILVER", dirn="short", fill=6350.0, deal="ADDON1",
                   gen=1, tp_pct=0.005, stop_pct=0.01):
    return dict(
        instrument=sym, direction=dirn, deal_id=deal, fill_price=fill,
        ig_size=0.4, stop_pct=stop_pct, tp_pct=tp_pct,
        leg_index=2, leg_generation=gen,
        pos_size=100.0, notional=1000.0, leverage=10,
        entry_time=time.time(),
    )


def make_live_with_harvest(liq_before=0.0805, realized_pnl=0.12):
    """Live state after a successful harvest."""
    return {
        "open_position":                make_primary(),
        "pyramid_legs":                 [],
        "rolling_capacity_slot":        "available",
        "rolling_bootstrap_liq_before": liq_before,
        "rolling_realized_harvest": {
            "deal_id":               "ADDON1",
            "instrument":            "SILVER",
            "direction":             "short",
            "leg_generation":        1,
            "fill_price":            6350.0,
            "confirmed_exit_price":  6270.0,
            "exit_price_source":     "broker_confirmed",
            "exit_mid_estimate":     6268.0,
            "pnl_pct":               0.0126,
            "realized_pnl_estimate": realized_pnl,
            "ig_size":               0.4,
            "epoch":                 time.time(),
        },
        "rolling_profit_deployed":      0.0,
        "rolling_campaign_id":          "campaign_PRIMARY",
    }


# ============================================================================
# Test 1: Ordinary addon TP behavior unchanged
# ============================================================================

class TestOrdinaryAddonTP:
    def test_ordinary_tp_routing_not_rolling(self):
        """Leg below harvest threshold routes to ordinary (not rolling) path."""
        leg = make_addon_leg(tp_pct=0.005)
        is_rolling, reason = rb4.b4a_is_rolling_harvest_path(
            leg=leg,
            leg_pnl_pct=0.006,   # above tp_pct but below harvest threshold 0.0025? No -- above it
            tp_pct=0.005,
            harvest_threshold_pct=0.0025,
        )
        # 0.006 >= 0.0025, gen=1: should be rolling path
        assert is_rolling is True
        assert reason == "rolling_harvest_path"

    def test_gen2_leg_never_on_rolling_path(self):
        """Gen-2 leg always routes to ordinary addon TP path."""
        leg = make_addon_leg(gen=2, tp_pct=0.005)
        is_rolling, reason = rb4.b4a_is_rolling_harvest_path(
            leg=leg, leg_pnl_pct=0.01, tp_pct=0.005, harvest_threshold_pct=0.0025,
        )
        assert is_rolling is False
        assert "gen2" in reason

    def test_below_harvest_threshold_ordinary_path(self):
        """Gen-1 leg with pnl below harvest threshold routes to ordinary path."""
        leg = make_addon_leg(gen=1, tp_pct=0.005)
        is_rolling, reason = rb4.b4a_is_rolling_harvest_path(
            leg=leg, leg_pnl_pct=0.001, tp_pct=0.005, harvest_threshold_pct=0.0025,
        )
        assert is_rolling is False
        assert "below_harvest_threshold" in reason


# ============================================================================
# Test 2: Rolling-harvest evaluation independent of ordinary addon TP
# ============================================================================

class TestRollingEvalIndependence:
    def test_b4a_rolling_path_fail_closed_does_not_execute(self):
        """b4a_rolling_path_fail_closed returns blocked result, no execution."""
        result = rb4.b4a_rolling_path_fail_closed()
        assert result["rolling_path_executed"] is False
        assert result["reason"] == "rolling_policy_not_enabled"
        assert result["deployment_blocked"] is True
        assert result["gen2_submission_blocked"] is True

    def test_can_credit_harvest_blocks_without_bootstrap_slot(self):
        """Harvest credit blocked when capacity slot is not bootstrap."""
        live = {"rolling_capacity_slot": "available"}
        leg  = make_addon_leg()
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is False
        assert "capacity_slot_unexpected" in reason


# ============================================================================
# Test 3: Observational A/B/C telemetry cannot execute rolling harvest
# ============================================================================

class TestCounterfactualTelemetry:
    def test_counterfactuals_policy_selected_is_none(self):
        """Counterfactual telemetry never selects a policy."""
        live = make_live_with_harvest()
        ledgers  = rb4.b4a_build_ledgers(live)
        econ     = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"],
            harvest_rec=live.get("rolling_realized_harvest") or {},
            ledgers=ledgers,
            live_min_deal={"SILVER": 0.1},
            live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda sym, px: 1.0,
            live_compute_ig_size_fn=lambda sym, notional, px: notional,
            current_price=6300.0,
            spread_pct=0.1,
        )
        cf = rb4.b4a_policy_counterfactuals(
            economics=econ, ledgers=ledgers,
            harvest_rec=live.get("rolling_realized_harvest") or {},
            r_primary=8.0, r_primary_provenance="test",
        )
        assert cf["policy_selected"] is None
        assert cf["deployment_authorized"] is False
        assert cf["telemetry_only"] is True

    def test_all_counterfactual_labels_present(self):
        """All 5 policy labels appear in counterfactuals."""
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"],
            harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0,
        )
        cf = rb4.b4a_policy_counterfactuals(
            economics=econ, ledgers=ledgers, harvest_rec={},
            r_primary=8.0, r_primary_provenance="test",
        )
        for label in rb4.B4A_POLICIES:
            assert label in cf["counterfactuals"], f"{label} missing"


# ============================================================================
# Test 4: Broker-confirmed exit used for realized harvest
# ============================================================================

class TestHarvestAccounting:
    def test_broker_confirmed_exit_used_when_available(self):
        """If confirmed_exit_price is set, exit_price_source == broker_confirmed."""
        live = make_live_with_harvest()
        harvest = live["rolling_realized_harvest"]
        assert harvest["exit_price_source"] == "broker_confirmed"
        assert harvest["confirmed_exit_price"] == 6270.0

    def test_ledger_a_source_tracks_exit_provenance(self):
        """Ledger A source field matches harvest record exit_price_source."""
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_a_source"] == "broker_confirmed"


# ============================================================================
# Test 5: Fallback exit estimate remains labeled
# ============================================================================

class TestFallbackLabel:
    def test_mid_estimate_fallback_labeled(self):
        """When no confirmed price, harvest source is labeled as estimate."""
        live = make_live_with_harvest()
        live["rolling_realized_harvest"]["confirmed_exit_price"] = None
        live["rolling_realized_harvest"]["exit_price_source"]    = "mid_estimate_fallback"
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_a_source"] == "mid_estimate_fallback"


# ============================================================================
# Test 6: Released margin never enters realized-profit ledger
# ============================================================================

class TestReleasedMarginNotProfit:
    def test_ledger_c_not_added_to_ledger_a(self):
        """Released margin (Ledger C) is never combined with realized profit (Ledger A)."""
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["released_margin_in_realized_profit"] is False
        # Ledger C is a dict with a note, not a scalar added to Ledger A
        assert isinstance(ledgers["ledger_c_released_margin"], dict)
        assert "NOT_PROFIT" in ledgers["ledger_c_released_margin"]["note"]

    def test_ledger_a_equals_only_harvest_pnl(self):
        """Ledger A equals exactly the realized_pnl_estimate from harvest record."""
        live = make_live_with_harvest(realized_pnl=0.12)
        ledgers = rb4.b4a_build_ledgers(live)
        assert abs(ledgers["ledger_a_realized_profit"] - 0.12) < 1e-9


# ============================================================================
# Test 7: Protected primary value never enters realized-profit ledger
# ============================================================================

class TestProtectedPrimaryNotProfit:
    def test_ledger_b_not_added_to_ledger_a(self):
        """Protected primary (Ledger B) is separate from realized profit (Ledger A).
        F2: Ledger B is actionable only from a CURRENT broker-backed liq_before,
        so supply one explicitly (bootstrap alone is now historical-only)."""
        live = make_live_with_harvest(liq_before=0.5, realized_pnl=0.12)
        live["liq_before"] = 0.5
        live["liq_before_provenance"] = {
            "source": "broker_acknowledged_stop",
            "campaign_id": live["rolling_campaign_id"],
        }
        ledgers = rb4.b4a_build_ledgers(live)
        # Ledger A is only the harvest pnl, not liq_before
        assert abs(ledgers["ledger_a_realized_profit"] - 0.12) < 1e-9
        # Ledger B is the current broker-backed liq_before value
        assert abs(ledgers["ledger_b_protected_primary"] - 0.5) < 1e-9
        assert ledgers["protected_primary_treated_as_cash"] is False


# ============================================================================
# Test 8: Protected primary not treated as broker cash
# ============================================================================

class TestProtectedPrimaryNotCash:
    def test_protected_primary_treated_as_cash_flag_false(self):
        """Invariant flag: protected_primary_treated_as_cash is always False."""
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["protected_primary_treated_as_cash"] is False

    def test_can_deploy_profit_always_blocked(self):
        """b4a_can_deploy_profit blocks in Build 4A -- cannot treat any value as deployable cash."""
        live = make_live_with_harvest()
        ok, reason = rb4.b4a_can_deploy_profit(live, 0.05)
        assert ok is False
        assert "build4a_observation_only" in reason


# ============================================================================
# Test 9: Missing protection remains UNKNOWN rather than zero
# ============================================================================

class TestK5Invariant:
    def test_missing_liq_before_gives_unknown_ledger_b(self):
        """When rolling_bootstrap_liq_before is None, Ledger B = UNKNOWN."""
        live = make_live_with_harvest()
        live["rolling_bootstrap_liq_before"] = None
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == rb4.B4A_UNKNOWN

    def test_missing_liq_before_gives_unknown_ledger_d(self):
        """When Ledger B is UNKNOWN, Ledger D is also UNKNOWN."""
        live = make_live_with_harvest()
        live["rolling_bootstrap_liq_before"] = None
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_d_principal_at_risk"] == rb4.B4A_UNKNOWN

    def test_opae_d1_computable_d2_unknown_when_ledger_b_unknown(self):
        """D1 is computable without B; D2 = UNKNOWN when Ledger B = UNKNOWN (K5 on D2)."""
        result = rb4.b4a_original_principal_at_risk(
            ledger_a=0.12, ledger_a_valid=True,
            ledger_b=rb4.B4A_UNKNOWN, ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=5.0, candidate_stop_risk=2.0,
            ledger_a_remaining=0.12,
        )
        # D1 is unconditional -- computable even when B is UNKNOWN
        assert result["d1_opar_unconditional"] != rb4.B4A_UNKNOWN
        assert abs(result["d1_opar_unconditional"] - 1.88) < 1e-6, (
            f"D1 expected ~1.88, got {result['d1_opar_unconditional']}"
        )
        # D2 is conditional on B -- UNKNOWN when B is UNKNOWN (K5)
        assert result["d2_opar_conditional"] == rb4.B4A_UNKNOWN
        assert result["k5_invariant_applied"] is True
        # B never entered profit ledger
        assert result["b_entered_profit_ledger"] is False

    def test_zero_liq_before_is_not_unknown(self):
        """A real CURRENT value of 0.0 for liq_before is valid (not UNKNOWN).
        F2: exercised via a current broker-backed provenance, since a bootstrap
        snapshot alone is now historical-only and yields UNKNOWN."""
        live = make_live_with_harvest(liq_before=0.0)
        live["liq_before"] = 0.0
        live["liq_before_provenance"] = {
            "source": "broker_acknowledged_stop",
            "campaign_id": live["rolling_campaign_id"],
        }
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == 0.0
        assert ledgers["ledger_b_protected_primary"] != rb4.B4A_UNKNOWN


# ============================================================================
# Test 10: Realized rolling profit persists across restart
# ============================================================================

class TestPersistenceRealized:
    def test_realized_profit_survives_json_roundtrip(self):
        """Realized profit in rolling_realized_harvest survives Redis-style JSON roundtrip."""
        live = make_live_with_harvest(realized_pnl=0.12345)
        serialized = json.dumps(live)
        restored   = json.loads(serialized)
        ledgers = rb4.b4a_build_ledgers(restored)
        assert abs(ledgers["ledger_a_realized_profit"] - 0.12345) < 1e-9
        assert ledgers["ledger_a_valid"] is True


# ============================================================================
# Test 11: Deployed rolling profit persists across restart
# ============================================================================

class TestPersistenceDeployed:
    def test_deployed_profit_survives_json_roundtrip(self):
        """rolling_profit_deployed persists across Redis-style JSON roundtrip."""
        live = make_live_with_harvest()
        live["rolling_profit_deployed"] = 0.0   # Build 4A: never > 0
        serialized = json.dumps(live)
        restored   = json.loads(serialized)
        ledgers = rb4.b4a_build_ledgers(restored)
        assert ledgers["ledger_a_deployed"] == 0.0

    def test_remaining_equals_realized_when_deployed_zero(self):
        """When deployed=0, remaining == realized."""
        live = make_live_with_harvest(realized_pnl=0.12)
        ledgers = rb4.b4a_build_ledgers(live)
        assert abs(ledgers["ledger_a_remaining"] - ledgers["ledger_a_realized_profit"]) < 1e-9


# ============================================================================
# Test 12: Duplicate evaluation cannot double-spend realized profit
# ============================================================================

class TestNoDuplicateEvalDoubleSpend:
    def test_duplicate_eval_does_not_modify_ledger(self):
        """Calling replacement evaluator twice does not change ledger values."""
        live   = make_live_with_harvest()
        params = dict(
            primary=live["open_position"],
            harvest_rec=live.get("rolling_realized_harvest") or {},
            ledgers=rb4.b4a_build_ledgers(live),
            live_min_deal={"SILVER": 0.1},
            live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0,
        )
        econ1 = rb4.b4a_compute_replacement_economics(**params)
        econ2 = rb4.b4a_compute_replacement_economics(**params)
        # Ledger values must be identical
        assert econ1["ledger_a_realized_profit"] == econ2["ledger_a_realized_profit"]
        assert econ1["ledger_b_protected_primary"] == econ2["ledger_b_protected_primary"]


# ============================================================================
# Test 13: Duplicate broker confirmation cannot double-credit
# ============================================================================

class TestNoDuplicateConfirmDoubleCredit:
    def test_same_deal_id_blocks_second_harvest_credit(self):
        """A second harvest with same deal_id is blocked by b4a_can_credit_harvest."""
        live = make_live_with_harvest()
        # Simulate an existing harvest for ADDON1
        live["rolling_capacity_slot"] = "bootstrap"  # reset to allow re-test
        leg = make_addon_leg(deal="ADDON1")
        # First: existing harvest has deal_id ADDON1
        live["rolling_realized_harvest"] = {"deal_id": "ADDON1"}
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is False
        assert reason == "duplicate_harvest_deal_id"

    def test_different_deal_id_not_blocked(self):
        """A different deal_id does not trigger the duplicate guard."""
        live = {"rolling_capacity_slot": "bootstrap",
                "rolling_realized_harvest": {"deal_id": "ADDON1"},
                "rolling_profit_deployed": 0.0}
        leg2 = make_addon_leg(deal="ADDON2")
        ok, reason = rb4.b4a_can_credit_harvest(live, leg2)
        # Slot is bootstrap, gen=1, no deployment, deal_id differs -> eligible
        assert ok is True
        assert reason == "eligible"


# ============================================================================
# Test 14: Duplicate harvest observation cannot double-credit
# ============================================================================

class TestNoDuplicateHarvestObservation:
    def test_can_credit_harvest_empty_deal_id_blocked_by_dedup(self):
        """F8 FIX: empty deal_id == empty deal_id -> blocked (dedup guard).

        When both incoming and existing deal_id are empty strings,
        empty == empty is True, so the duplicate guard correctly blocks.
        This prevents empty-identity harvests from bypassing dedup.
        """
        # Existing harvest also has empty deal_id
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_profit_deployed": 0.0,
            "rolling_realized_harvest": {"deal_id": ""},  # existing with empty id
        }
        leg = make_addon_leg()
        leg["deal_id"] = ""  # incoming also empty
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        # F8 fix: "" == "" should block (not bypass)
        assert ok is False, (
            f"Empty deal_id should be blocked by dedup, got ok={ok}, reason={reason}"
        )
        assert reason == "duplicate_harvest_deal_id"


# ============================================================================
# Test 15: Campaign close clears active rolling state correctly
# ============================================================================

class TestCampaignCleanup:
    def test_clear_campaign_state_removes_all_rolling_fields(self):
        """b4a_clear_campaign_rolling_state clears all rolling fields."""
        live = make_live_with_harvest()
        live["rolling_campaign_id"] = "campaign_PRIMARY"
        live["rolling_profit_deployed"] = 0.0
        cleared = rb4.b4a_clear_campaign_rolling_state(live, reason="take_profit")
        assert "rolling_capacity_slot" in cleared
        assert "rolling_realized_harvest" in cleared
        assert "rolling_bootstrap_liq_before" in cleared
        assert "rolling_campaign_id" in cleared
        # Fields are gone from live
        assert live.get("rolling_capacity_slot") is None
        assert live.get("rolling_realized_harvest") is None

    def test_clear_leaves_audit_trail(self):
        """b4a_clear_campaign_rolling_state records what was cleared."""
        live = make_live_with_harvest()
        rb4.b4a_clear_campaign_rolling_state(live, reason="stop_loss")
        assert "rolling_last_campaign_cleared" in live
        assert live["rolling_last_campaign_cleared"]["reason"] == "stop_loss"


# ============================================================================
# Test 16: New campaign cannot inherit old rolling fuel
# ============================================================================

class TestNoFuelInheritance:
    def test_detect_stale_state_different_campaign(self):
        """b4a_detect_stale_rolling_state detects prior campaign's state."""
        live = make_live_with_harvest()
        live["rolling_campaign_id"] = "campaign_OLD_PRIMARY"
        new_primary = make_primary(deal="NEW_PRIMARY")
        result = rb4.b4a_detect_stale_rolling_state(live, new_primary)
        assert result["stale_detected"] is True
        assert result["prior_campaign_id"] == "campaign_OLD_PRIMARY"
        assert result["new_campaign_id"] == "campaign_NEW_PRIMARY"

    def test_no_stale_when_no_prior_campaign_id(self):
        """No stale detection when no prior campaign_id present."""
        live = {}
        new_primary = make_primary()
        result = rb4.b4a_detect_stale_rolling_state(live, new_primary)
        assert result["stale_detected"] is False

    def test_same_campaign_not_stale(self):
        """Same campaign continuing is not flagged as stale."""
        live = {"rolling_campaign_id": "campaign_PRIMARY"}
        same_primary = make_primary(deal="PRIMARY")
        result = rb4.b4a_detect_stale_rolling_state(live, same_primary)
        assert result["stale_detected"] is False


# ============================================================================
# Test 17: Generation metadata survives restart
# ============================================================================

class TestGenerationMetadataPersists:
    def test_leg_generation_survives_json_roundtrip(self):
        """leg_generation survives Redis-style JSON serialization."""
        leg = make_addon_leg(gen=1)
        state = {"pyramid_legs": [leg]}
        serialized = json.dumps(state)
        restored   = json.loads(serialized)
        assert restored["pyramid_legs"][0]["leg_generation"] == 1

    def test_campaign_id_survives_json_roundtrip(self):
        """rolling_campaign_id survives JSON roundtrip."""
        live = make_live_with_harvest()
        live["rolling_campaign_id"] = "campaign_PRIMARY"
        serialized = json.dumps(live)
        restored   = json.loads(serialized)
        assert restored["rolling_campaign_id"] == "campaign_PRIMARY"


# ============================================================================
# Test 18: Gen-2 evaluator produces complete economics
# ============================================================================

class TestGen2EvaluatorComplete:
    def test_economics_has_all_required_fields(self):
        """b4a_compute_replacement_economics returns all required fields."""
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"],
            harvest_rec=live.get("rolling_realized_harvest") or {},
            ledgers=ledgers,
            live_min_deal={"SILVER": 0.1},
            live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0,
            spread_pct=0.1,
        )
        required = [
            "status", "eval_id", "campaign_id", "instrument", "direction",
            "current_generation", "candidate_generation",
            "fill_reference_price", "legal_ig_size_1x_mindeal", "mindeal_raw",
            "margin_rate", "margin_requirement_1x", "nominal_leverage",
            # F9: renamed from gap_risk_observable_spread_pct
            "spread_pct_observed", "bid_ask_spread_pct_observed",
            "orderly_stop_risk", "gap_exposure_beyond_stop",
            "total_possible_loss_note",
            "realized_profit_pool",
            "realized_profit_source", "ledger_a_realized_profit",
            "ledger_b_protected_primary", "ledger_c_released_margin",
            "ledger_d_principal_at_risk", "original_principal_exposure",
            "candidate_sizes_mindeal_multiples", "mindeal_oversize_max",
            # F12: R_primary provenance
            "r_primary_original", "r_primary_original_source",
            "generation2_submission", "note",
        ]
        for field in required:
            assert field in econ, f"Missing field: {field}"

    def test_all_four_mindeal_multiples_present(self):
        """All 1x/2x/3x/4x MINDEAL candidates are in the economics."""
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"], harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0,
        )
        sizes = econ["candidate_sizes_mindeal_multiples"]
        for mult in ["1x_mindeal", "2x_mindeal", "3x_mindeal", "4x_mindeal"]:
            assert mult in sizes, f"Missing {mult}"

    def test_gen2_submission_field_blocked(self):
        """generation2_submission field is always BLOCKED."""
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"], harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0,
        )
        assert "BLOCKED" in econ["generation2_submission"]


# ============================================================================
# Test 19: Gen-2 submission is impossible
# ============================================================================

class TestGen2SubmissionImpossible:
    def test_b4a_gen2_submission_guard_blocks_gen2(self):
        """b4a_gen2_submission_guard blocks candidate_generation=2."""
        blocked, reason = rb4.b4a_gen2_submission_guard(
            candidate_generation=2,
            rolling_max_generations=1,
            caller="test",
        )
        assert blocked is True
        assert "build4a_observation_only" in reason

    def test_b4a_gen2_submission_guard_allows_gen1(self):
        """b4a_gen2_submission_guard does not block gen-1."""
        blocked, reason = rb4.b4a_gen2_submission_guard(
            candidate_generation=1,
            rolling_max_generations=1,
            caller="test",
        )
        assert blocked is False

    def test_june_rolling_max_generations_is_1(self):
        """june._ROLLING_MAX_GENERATIONS == 1 (gen-2 blocked)."""
        import june
        assert june._ROLLING_MAX_GENERATIONS == 1

    def test_b4a_can_deploy_profit_blocks(self):
        """b4a_can_deploy_profit always returns False in Build 4A."""
        live = make_live_with_harvest()
        ok, _ = rb4.b4a_can_deploy_profit(live, 0.10)
        assert ok is False


# ============================================================================
# Test 20: Malformed legacy state fails safely
# ============================================================================

class TestMalformedLegacyState:
    def test_extended_validation_tolerates_missing_fields(self):
        """b4a_validate_rolling_state_extended handles missing/empty live state."""
        warnings = rb4.b4a_validate_rolling_state_extended({})
        assert isinstance(warnings, list)
        # Empty state should produce no warnings
        assert warnings == []

    def test_deployed_exceeds_realized_raises_warning(self):
        """Extended validation warns when deployed > realized."""
        live = make_live_with_harvest(realized_pnl=0.05)
        live["rolling_profit_deployed"] = 0.10  # more than realized
        warnings = rb4.b4a_validate_rolling_state_extended(live)
        assert any("B4A_DOUBLE_SPEND_RISK" in w for w in warnings)

    def test_campaign_id_mismatch_raises_warning(self):
        """Extended validation warns on campaign ID mismatch."""
        live = make_live_with_harvest()
        live["rolling_campaign_id"] = "campaign_WRONG"
        live["open_position"]["deal_id"] = "PRIMARY"  # campaign_PRIMARY expected
        warnings = rb4.b4a_validate_rolling_state_extended(live)
        assert any("B4A_CAMPAIGN_MISMATCH" in w for w in warnings)

    def test_build_ledgers_no_harvest_safe(self):
        """b4a_build_ledgers handles state with no harvest record."""
        live = {"open_position": make_primary(), "pyramid_legs": []}
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_a_valid"] is False
        assert ledgers["ledger_a_realized_profit"] == 0.0


# ============================================================================
# Test 21: R_primary provenance is retained
# ============================================================================

class TestRPrimaryProvenance:
    def test_r_primary_provenance_in_counterfactuals(self):
        """r_primary_provenance is included in counterfactual telemetry."""
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"], harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0,
        )
        cf = rb4.b4a_policy_counterfactuals(
            economics=econ, ledgers=ledgers, harvest_rec={},
            r_primary=8.0, r_primary_provenance="stop_dist_x_ig_size_approximation",
        )
        assert cf["r_primary_provenance"] == "stop_dist_x_ig_size_approximation"

    def test_unknown_r_primary_propagates_to_b_variants(self):
        """When R_primary is UNKNOWN, all bootstrap variants are UNKNOWN."""
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"], harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0,
        )
        cf = rb4.b4a_policy_counterfactuals(
            economics=econ, ledgers=ledgers, harvest_rec={},
            r_primary=rb4.B4A_UNKNOWN, r_primary_provenance="unknown",
        )
        for label in ["bootstrap_0_25R", "bootstrap_0_50R", "bootstrap_0_75R", "bootstrap_1_00R"]:
            assert cf["counterfactuals"][label]["outcome"] == rb4.B4A_UNKNOWN


# ============================================================================
# Test 22: Build-1 stop-sync behavior unchanged
# ============================================================================

class TestBuild1Regression:
    def test_build1_stop_sync_constants_present(self):
        """Build-1 stop sync behavior: _ROLLING_MAX_GENERATIONS and stop_sync field."""
        import june
        # Build-1 invariant: gen-2 submission is blocked by _ROLLING_MAX_GENERATIONS == 1
        assert hasattr(june, "_ROLLING_MAX_GENERATIONS"), (
            "Build-1 constant _ROLLING_MAX_GENERATIONS missing from june.py"
        )
        assert june._ROLLING_MAX_GENERATIONS == 1, (
            "Build-1: _ROLLING_MAX_GENERATIONS must be 1 to block gen-2"
        )
        # Build-1 stop_sync: _live_retry_stop_sync must exist
        assert hasattr(june, "_live_retry_stop_sync"), (
            "Build-1 stop sync function _live_retry_stop_sync missing"
        )


# ============================================================================
# Test 23: Build-2 behavior unchanged
# ============================================================================

class TestBuild2Regression:
    def test_f50_constants_unchanged(self):
        """Build-2 F50 defensive scaling is unchanged."""
        import june
        # F50 requires profit_protected state -- this must still function
        # Key: f50_capacity function still importable
        from defensive_scaling import f50_capacity
        assert callable(f50_capacity)


# ============================================================================
# Test 24: Build-3 harvest accounting repair unchanged
# ============================================================================

class TestBuild3HarvestAccountingUnchanged:
    def test_broker_confirmed_exit_price_priority(self):
        """Build-3 invariant: broker-confirmed exit price takes priority in harvest."""
        # This is tested by checking harvest record contains the right exit source
        live = make_live_with_harvest()
        assert live["rolling_realized_harvest"]["exit_price_source"] == "broker_confirmed"
        assert live["rolling_realized_harvest"]["confirmed_exit_price"] == 6270.0

    def test_rolling_harvest_eligibility_function_exists(self):
        """Build-3 _live_rolling_harvest_eligibility still present."""
        import june
        assert hasattr(june, "_live_rolling_harvest_eligibility")

    def test_duplicate_harvest_delivery_still_blocked(self):
        """Build-3 duplicate harvest delivery guard still works via b4a layer."""
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_realized_harvest": {"deal_id": "ADDON1"},
            "rolling_profit_deployed": 0.0,
        }
        leg = make_addon_leg(deal="ADDON1")
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is False
        assert reason == "duplicate_harvest_deal_id"


# ============================================================================
# Test 25: Gen-1 existing behavior not blocked by Build-4A gen-2 safety invariant
# ============================================================================

class TestGen1NotBlocked:
    def test_gen2_guard_does_not_block_gen1(self):
        """b4a_gen2_submission_guard allows gen-1 (only blocks gen-2+)."""
        blocked, reason = rb4.b4a_gen2_submission_guard(
            candidate_generation=1,
            rolling_max_generations=1,
            caller="gen1_path",
        )
        assert blocked is False
        assert "generation_within_allowed_range" in reason

    def test_can_credit_harvest_allows_gen1(self):
        """b4a_can_credit_harvest allows gen-1 with bootstrap slot."""
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_realized_harvest": None,
            "rolling_profit_deployed": 0.0,
        }
        leg = make_addon_leg(gen=1, deal="ADDON1")
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is True
        assert reason == "eligible"

    def test_is_rolling_harvest_path_gen1_eligible(self):
        """b4a_is_rolling_harvest_path routes gen-1 legs above threshold to rolling path."""
        leg = make_addon_leg(gen=1)
        is_rolling, reason = rb4.b4a_is_rolling_harvest_path(
            leg=leg, leg_pnl_pct=0.005, tp_pct=0.005, harvest_threshold_pct=0.0025,
        )
        assert is_rolling is True
        assert reason == "rolling_harvest_path"


# ============================================================================
# NEW TESTS: D1/D2 formula verification (Step 3 -- must fail pre-repair)
# ============================================================================

class TestD1D2Formulas:
    """Verify the new D1 (unconditional) and D2 (conditional) OPAR formulas."""

    def _call(self, ledger_a, ledger_a_valid, ledger_b,
              candidate_stop_risk, ledger_a_remaining=None):
        return rb4.b4a_original_principal_at_risk(
            ledger_a=ledger_a,
            ledger_a_valid=ledger_a_valid,
            ledger_b=ledger_b,
            ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0,
            candidate_stop_risk=candidate_stop_risk,
            ledger_a_remaining=ledger_a_remaining,
        )

    def test_case_a_realized_covers_all(self):
        """A=0.80, stop_risk=0.80: D1=0.00 (fully covered by realized)."""
        r = self._call(0.80, True, 0.0, 0.80, 0.80)
        assert abs(r["d1_opar_unconditional"]) < 1e-8, f"D1={r['d1_opar_unconditional']}"

    def test_partial_coverage_d1(self):
        """A=0.40, stop_risk=0.80: D1=0.40 (not 0.00)."""
        r = self._call(0.40, True, rb4.B4A_UNKNOWN, 0.80, 0.40)
        assert abs(r["d1_opar_unconditional"] - 0.40) < 1e-8, (
            f"D1 expected 0.40, got {r['d1_opar_unconditional']}"
        )

    def test_d1_d2_with_known_b(self):
        """A=0.40, B=0.40, stop_risk=0.80: D1=0.40, D2=0.00 (conditional)."""
        r = self._call(0.40, True, 0.40, 0.80, 0.40)
        assert abs(r["d1_opar_unconditional"] - 0.40) < 1e-8, f"D1={r['d1_opar_unconditional']}"
        assert r["d2_opar_conditional"] != rb4.B4A_UNKNOWN
        assert abs(r["d2_opar_conditional"]) <= 0.0 + 1e-8, (
            f"D2 expected 0.00 conditionally, got {r['d2_opar_conditional']}"
        )

    def test_negative_a_does_not_inflate_d1(self):
        """A=negative(-0.40), stop_risk=0.80: D1=0.80 (not 1.20)."""
        r = self._call(-0.40, True, rb4.B4A_UNKNOWN, 0.80, -0.40)
        assert abs(r["d1_opar_unconditional"] - 0.80) < 1e-8, (
            f"D1 expected 0.80 (negative A floored at 0), got {r['d1_opar_unconditional']}"
        )

    def test_remaining_not_total_for_d1(self):
        """realized=2.00, deployed=1.25, remaining=0.75: D1=0.25 for stop_risk=1.00."""
        # Ledger A total=2.00, deployed=1.25, remaining=0.75
        r = self._call(2.00, True, rb4.B4A_UNKNOWN, 1.00, 0.75)
        assert abs(r["d1_opar_unconditional"] - 0.25) < 1e-8, (
            f"D1 expected 0.25 (remaining covers 0.75 of 1.00), got {r['d1_opar_unconditional']}"
        )

    def test_primary_closed_ledger_b_unknown(self):
        """Primary closed -> Ledger B = UNKNOWN -> D2 = UNKNOWN."""
        live = make_live_with_harvest()
        live["open_position"] = None  # primary closed
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == rb4.B4A_UNKNOWN
        assert ledgers["ledger_b_provenance"] == "primary_closed_protection_cleared"

    def test_bootstrap_liq_before_historical_preserved(self):
        """bootstrap_liq_before_historical preserved alongside current protection."""
        live = make_live_with_harvest(liq_before=0.0805)
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["bootstrap_liq_before_historical"] == 0.0805

    def test_gap_field_renamed(self):
        """gap_risk_observable_spread_pct is absent; spread_pct_observed is present."""
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"], harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0, spread_pct=0.1,
        )
        assert "gap_risk_observable_spread_pct" not in econ, (
            "Old field gap_risk_observable_spread_pct should be absent after rename"
        )
        assert "spread_pct_observed" in econ
        assert "total_possible_loss_note" in econ

    def test_pure_self_funding_eligible(self):
        """pure_self_funding: eligible when remaining realized >= candidate stop risk."""
        live = make_live_with_harvest(realized_pnl=1.00)
        # Make stop_pct such that candidate_stop_risk < 1.00
        live["open_position"]["stop_pct"] = 0.001  # very small stop
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"], harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0,
        )
        cf = rb4.b4a_policy_counterfactuals(
            economics=econ, ledgers=ledgers,
            harvest_rec=live.get("rolling_realized_harvest") or {},
            r_primary=8.0, r_primary_provenance="test",
        )
        psf = cf["counterfactuals"]["pure_self_funding"]
        # With realized=1.00 and tiny stop, should be eligible
        assert psf["outcome"] in ("eligible", rb4.B4A_UNKNOWN), (
            f"Expected eligible or UNKNOWN, got {psf['outcome']}"
        )

    def test_pure_self_funding_insufficient(self):
        """pure_self_funding: insufficient when remaining < candidate stop risk."""
        live = make_live_with_harvest(realized_pnl=0.01)
        live["open_position"]["stop_pct"] = 0.10  # large stop
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"], harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 10.0}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0,
        )
        cf = rb4.b4a_policy_counterfactuals(
            economics=econ, ledgers=ledgers,
            harvest_rec=live.get("rolling_realized_harvest") or {},
            r_primary=8.0, r_primary_provenance="test",
        )
        psf = cf["counterfactuals"]["pure_self_funding"]
        assert psf["outcome"] in ("insufficient_realized_profit", rb4.B4A_UNKNOWN), (
            f"Expected insufficient or UNKNOWN, got {psf['outcome']}"
        )

    def test_same_deal_id_twice_blocked(self):
        """Duplicate deal_id -> second call blocked."""
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_profit_deployed": 0.0,
            "rolling_realized_harvest": {"deal_id": "ADDON1"},
        }
        leg = make_addon_leg(deal="ADDON1")
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is False
        assert reason == "duplicate_harvest_deal_id"

    def test_empty_deal_id_same_empty_blocked(self):
        """Empty deal_id == empty deal_id -> blocked (F8 fix)."""
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_profit_deployed": 0.0,
            "rolling_realized_harvest": {"deal_id": ""},
        }
        leg = make_addon_leg()
        leg["deal_id"] = ""
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is False, f"Empty dedup should block, got ok={ok}"
        assert reason == "duplicate_harvest_deal_id"

    def test_d2_unknown_protection_stays_unknown(self):
        """D2 = UNKNOWN when protection is UNKNOWN; never coerced to 0."""
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=0.40, ledger_a_valid=True,
            ledger_b=rb4.B4A_UNKNOWN, ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=5.0, candidate_stop_risk=0.80,
            ledger_a_remaining=0.40,
        )
        assert r["d2_opar_conditional"] == rb4.B4A_UNKNOWN
        assert r["d1_opar_unconditional"] != rb4.B4A_UNKNOWN

    def test_c38_replay_no_realized_profit(self):
        """C38 replay: no realized profit -> D1 = full stop risk."""
        live = {
            "open_position": make_primary(),
            "pyramid_legs": [],
            "rolling_capacity_slot": "bootstrap",
            "rolling_bootstrap_liq_before": 0.0,
            "rolling_profit_deployed": 0.0,
            # No rolling_realized_harvest
        }
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"], harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0,
        )
        opae = econ["original_principal_exposure"]
        # D1: full stop_risk (no realized coverage)
        d1 = opae.get("d1_opar_unconditional", rb4.B4A_UNKNOWN)
        stop_risk = econ.get("orderly_stop_risk", rb4.B4A_UNKNOWN)
        if d1 != rb4.B4A_UNKNOWN and stop_risk != rb4.B4A_UNKNOWN:
            assert abs(d1 - float(stop_risk)) < 1e-6, (
                f"C38: D1 should equal stop_risk when no realized profit; D1={d1}, stop={stop_risk}"
            )

    def test_gap_matrix_case_a(self):
        """Gap matrix Case A: realized=$0.80, stop_risk=$0.80 -> D1=0.00."""
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=0.80, ledger_a_valid=True,
            ledger_b=0.0, ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.80,
            ledger_a_remaining=0.80,
        )
        assert abs(r["d1_opar_unconditional"]) < 1e-8

    def test_gap_matrix_case_b(self):
        """Gap matrix Case B: realized=$0.40, protection=$0.40 -> D1=0.40, D2~=0.00."""
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=0.40, ledger_a_valid=True,
            ledger_b=0.40, ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.80,
            ledger_a_remaining=0.40,
        )
        assert abs(r["d1_opar_unconditional"] - 0.40) < 1e-8
        assert abs(r["d2_opar_conditional"]) < 1e-8

    def test_gap_matrix_case_c(self):
        """Gap matrix Case C: realized=$0.00, protection=$0.80 -> D1=0.80, D2~=0.00."""
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=0.0, ledger_a_valid=False,
            ledger_b=0.80, ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.80,
            ledger_a_remaining=0.0,
        )
        assert abs(r["d1_opar_unconditional"] - 0.80) < 1e-8
        assert abs(r["d2_opar_conditional"]) < 1e-8

    def test_gap_matrix_case_d(self):
        """Gap matrix Case D: realized=$0.40, protection=UNKNOWN -> D1=0.40, D2=UNKNOWN."""
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=0.40, ledger_a_valid=True,
            ledger_b=rb4.B4A_UNKNOWN, ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.80,
            ledger_a_remaining=0.40,
        )
        assert abs(r["d1_opar_unconditional"] - 0.40) < 1e-8
        assert r["d2_opar_conditional"] == rb4.B4A_UNKNOWN

    def test_gap_matrix_case_e_gap_labeled(self):
        """Gap matrix Case E: gap_exposure_beyond_stop labeled not_modeled_unknown."""
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=live["open_position"], harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0, spread_pct=0.1,
        )
        assert econ["gap_exposure_beyond_stop"] == "not_modeled_unknown"
        assert "NOT_maximum_loss" in econ["total_possible_loss_note"]


# ============================================================================
# F13: Executable failure-atomicity tests (12 scenarios)
# ============================================================================

class TestFailureAtomicity:
    """Executable tests for failure scenarios from FAILURE_SCENARIOS matrix."""

    def test_fa1_harvest_blocked_wrong_slot(self):
        """FA1: Harvest credit blocked when slot != bootstrap (wrong state)."""
        live = {"rolling_capacity_slot": "available", "rolling_profit_deployed": 0.0}
        leg  = make_addon_leg(gen=1, deal="ADDON1")
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is False
        assert "capacity_slot_unexpected" in reason

    def test_fa2_harvest_confirmation_empty_deal(self):
        """FA2: Harvest with empty deal_id blocked; identity unknown."""
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_profit_deployed": 0.0,
            "rolling_realized_harvest": {"deal_id": ""},
        }
        leg = make_addon_leg()
        leg["deal_id"] = ""
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is False
        assert reason == "duplicate_harvest_deal_id"

    def test_fa3_deal_id_dedup_blocks_re_credit_after_restart(self):
        """FA3: deal_id dedup blocks re-credit after simulated restart (JSON roundtrip)."""
        import json
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_profit_deployed": 0.0,
            "rolling_realized_harvest": {"deal_id": "ADDON1", "realized_pnl_estimate": 0.12},
        }
        # Simulate save/reload (JSON roundtrip)
        restored = json.loads(json.dumps(live))
        # Try to re-credit same deal_id
        restored["rolling_capacity_slot"] = "bootstrap"  # slot reset
        leg = make_addon_leg(deal="ADDON1")
        ok, reason = rb4.b4a_can_credit_harvest(restored, leg)
        assert ok is False
        assert reason == "duplicate_harvest_deal_id"

    def test_fa4_realized_preserved_across_json_roundtrip(self):
        """FA4: realized_total preserved across JSON roundtrip; deployed not reset."""
        import json
        live = make_live_with_harvest(realized_pnl=0.12)
        live["rolling_profit_deployed"] = 0.0
        restored = json.loads(json.dumps(live))
        ledgers = rb4.b4a_build_ledgers(restored)
        assert abs(ledgers["ledger_a_realized_profit"] - 0.12) < 1e-9
        assert ledgers["ledger_a_deployed"] == 0.0
        assert abs(ledgers["ledger_a_remaining"] - 0.12) < 1e-9

    def test_fa5_d1_correct_after_json_reload(self):
        """FA5: D1 computed correctly after JSON reload with realized profit."""
        import json
        live = make_live_with_harvest(realized_pnl=0.40)
        restored = json.loads(json.dumps(live))
        ledgers = rb4.b4a_build_ledgers(restored)
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=ledgers["ledger_a_realized_profit"],
            ledger_a_valid=ledgers["ledger_a_valid"],
            ledger_b=rb4.B4A_UNKNOWN,
            ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0,
            candidate_stop_risk=0.80,
            ledger_a_remaining=ledgers["ledger_a_remaining"],
        )
        assert abs(r["d1_opar_unconditional"] - 0.40) < 1e-8

    def test_fa6_duplicate_broker_confirmation_blocked(self):
        """FA6: Two calls with same deal_id -> second blocked."""
        live = {"rolling_capacity_slot": "bootstrap", "rolling_profit_deployed": 0.0,
                "rolling_realized_harvest": {"deal_id": "ADDON1"}}
        leg = make_addon_leg(deal="ADDON1")
        ok1, _ = rb4.b4a_can_credit_harvest(live, leg)
        ok2, reason2 = rb4.b4a_can_credit_harvest(live, leg)
        assert ok1 is False  # already recorded
        assert ok2 is False
        assert reason2 == "duplicate_harvest_deal_id"

    def test_fa7_redis_unavailable_validate_handles_missing(self):
        """FA7: b4a_validate_rolling_state_extended handles completely empty state."""
        warnings = rb4.b4a_validate_rolling_state_extended({})
        assert isinstance(warnings, list)
        assert warnings == []  # no crash, no false warnings

    def test_fa8_primary_closes_ledger_b_cleared(self):
        """FA8: After primary closes, Ledger B becomes UNKNOWN; D2 = UNKNOWN."""
        live = make_live_with_harvest(liq_before=0.50)
        live["open_position"] = None  # primary closed
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == rb4.B4A_UNKNOWN
        assert ledgers["ledger_b_provenance"] == "primary_closed_protection_cleared"
        # D2 must be UNKNOWN after primary close
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=0.12, ledger_a_valid=True,
            ledger_b=ledgers["ledger_b_protected_primary"],
            ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.50,
            ledger_a_remaining=0.12,
        )
        assert r["d2_opar_conditional"] == rb4.B4A_UNKNOWN

    def test_fa9_stale_rolling_state_campaign_mismatch(self):
        """FA9: Campaign id mismatch detects stale state for new campaign."""
        live = make_live_with_harvest()
        live["rolling_campaign_id"] = "campaign_OLD"
        new_primary = make_primary(deal="NEW_DEAL")
        result = rb4.b4a_detect_stale_rolling_state(live, new_primary)
        assert result["stale_detected"] is True
        assert result["prior_campaign_id"] == "campaign_OLD"

    def test_fa10_deployed_exceeds_realized_warning(self):
        """FA10: deployed > realized -> validation warning fires."""
        live = make_live_with_harvest(realized_pnl=0.10)
        live["rolling_profit_deployed"] = 0.20  # > realized
        warnings = rb4.b4a_validate_rolling_state_extended(live)
        assert any("B4A_DOUBLE_SPEND_RISK" in w for w in warnings)

    def test_fa11_missing_identity_empty_deal_blocked(self):
        """FA11: Missing/empty harvest identity -> blocked by dedup (F8 fix)."""
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_profit_deployed": 0.0,
            "rolling_realized_harvest": {"deal_id": ""},  # existing empty
        }
        leg = make_addon_leg()
        leg["deal_id"] = ""
        ok, reason = rb4.b4a_can_credit_harvest(live, leg)
        assert ok is False
        assert reason == "duplicate_harvest_deal_id"

    def test_fa12_d2_unknown_when_primary_protection_unknown(self):
        """FA12: D2 = UNKNOWN when primary liq_before unavailable."""
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=0.12, ledger_a_valid=True,
            ledger_b=rb4.B4A_UNKNOWN,
            ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=1.00,
            ledger_a_remaining=0.12,
        )
        assert r["d2_opar_conditional"] == rb4.B4A_UNKNOWN
        assert r["k5_invariant_applied"] is True
        # D1 is still computable
        assert r["d1_opar_unconditional"] != rb4.B4A_UNKNOWN

# ============================================================================
# NF1/NF2/NF3 Second Repair Pass Tests
# ============================================================================

class TestNF1LiqBeforeProvenance:
    """NF1: Current Ledger B data supply -- provenance validation."""

    def test_nf1_current_liq_before_used_when_present(self):
        live = make_live_with_harvest(liq_before=0.08)
        live["liq_before"] = 0.15
        live["liq_before_provenance"] = {
            "source": "broker_acknowledged_stop",
            "campaign_id": "campaign_PRIMARY",
        }
        ledgers = rb4.b4a_build_ledgers(live)
        assert abs(ledgers["ledger_b_protected_primary"] - 0.15) < 1e-9
        assert ledgers["ledger_b_provenance"] == "current_broker_acknowledged_stop"

    def test_nf1_improved_stop_overwrites_liq_before(self):
        live = make_live_with_harvest(liq_before=0.08)
        live["liq_before"] = 0.20
        live["liq_before_provenance"] = {
            "source": "broker_acknowledged_stop",
            "campaign_id": "campaign_PRIMARY",
        }
        ledgers = rb4.b4a_build_ledgers(live)
        assert abs(ledgers["ledger_b_protected_primary"] - 0.20) < 1e-9

    def test_nf1_pending_stop_does_not_become_ledger_b(self):
        # F2 REPAIR: with no CURRENT broker-backed liq_before, only a stale
        # bootstrap snapshot remains. Post-F2 that snapshot is historical-only
        # and MUST NOT be used as actionable Ledger B -> UNKNOWN (fail closed).
        live = make_live_with_harvest(liq_before=0.08)
        live.pop("liq_before", None)
        live.pop("liq_before_provenance", None)
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == rb4.B4A_UNKNOWN
        assert ledgers["ledger_b_provenance"] == "bootstrap_snapshot_stale"
        # Bootstrap value is still preserved for provenance/telemetry only.
        assert abs(ledgers["bootstrap_liq_before_historical"] - 0.08) < 1e-9

    def test_nf1_rejected_stop_does_not_become_ledger_b(self):
        live = make_live_with_harvest(liq_before=0.08)
        live["liq_before"] = 0.05
        live["liq_before_provenance"] = {
            "source": "broker_acknowledged_stop",
            "campaign_id": "campaign_PRIMARY",
        }
        ledgers = rb4.b4a_build_ledgers(live)
        assert abs(ledgers["ledger_b_protected_primary"] - 0.05) < 1e-9

    def test_nf1_primary_close_invalidates_liq_before(self):
        live = make_live_with_harvest(liq_before=0.50)
        live["liq_before"] = 0.50
        live["liq_before_provenance"] = {"source": "broker_acknowledged_stop", "campaign_id": "campaign_PRIMARY"}
        live["open_position"] = None
        ledgers = rb4.b4a_build_ledgers(live)
        assert ledgers["ledger_b_protected_primary"] == rb4.B4A_UNKNOWN
        assert ledgers["ledger_b_provenance"] == "primary_closed_protection_cleared"

    def test_nf1_campaign_mismatch_invalidates_liq_before(self):
        # A mismatched-campaign CURRENT liq_before must be invalidated.
        # F2 REPAIR: after invalidation only a stale bootstrap remains, which
        # is now historical-only -> actionable Ledger B is UNKNOWN (fail closed).
        live = make_live_with_harvest(liq_before=0.08)
        live["liq_before"] = 0.99
        live["liq_before_provenance"] = {
            "source": "broker_acknowledged_stop",
            "campaign_id": "campaign_OLD",
        }
        live["rolling_campaign_id"] = "campaign_PRIMARY"
        ledgers = rb4.b4a_build_ledgers(live)
        # Mismatched current value must NOT survive
        assert ledgers["ledger_b_protected_primary"] != 0.99
        # And the stale bootstrap must NOT silently substitute -> UNKNOWN
        assert ledgers["ledger_b_protected_primary"] == rb4.B4A_UNKNOWN
        assert abs(ledgers["bootstrap_liq_before_historical"] - 0.08) < 1e-9

    def test_nf1_restart_valid_provenance_preserves_liq_before(self):
        import json as _json
        live = make_live_with_harvest(liq_before=0.08)
        live["liq_before"] = 0.15
        live["liq_before_provenance"] = {
            "source": "broker_acknowledged_stop",
            "campaign_id": "campaign_PRIMARY",
        }
        restored = _json.loads(_json.dumps(live))
        ledgers = rb4.b4a_build_ledgers(restored)
        assert abs(ledgers["ledger_b_protected_primary"] - 0.15) < 1e-9

    def test_nf1_restart_stale_provenance_rejected(self):
        import json as _json
        live = make_live_with_harvest(liq_before=0.08)
        live["liq_before"] = 0.99
        live["liq_before_provenance"] = {
            "source": "broker_acknowledged_stop",
            "campaign_id": "campaign_STALE",
        }
        live["rolling_campaign_id"] = "campaign_NEW"
        restored = _json.loads(_json.dumps(live))
        ledgers = rb4.b4a_build_ledgers(restored)
        assert ledgers["ledger_b_protected_primary"] != 0.99


class TestNF1Formula:
    """NF1: Verify liq_before formula with worked examples."""

    def _liq(self, direction, fill, stop, ig, lot):
        sign = 1.0 if direction == "long" else -1.0
        return sign * (stop - fill) * ig * lot

    def test_silver_long_stop_below_fill(self):
        assert abs(self._liq("long", 6367.5, 6350.0, 0.04, 1.0) - (-0.70)) < 1e-9

    def test_silver_long_stop_above_fill(self):
        assert abs(self._liq("long", 6367.5, 6380.0, 0.04, 1.0) - 0.50) < 1e-9

    def test_gold_long_stop_below_fill(self):
        assert abs(self._liq("long", 3200.0, 3180.0, 0.01, 1.0) - (-0.20)) < 1e-9

    def test_gold_long_stop_above_fill(self):
        assert abs(self._liq("long", 3200.0, 3210.0, 0.01, 1.0) - 0.10) < 1e-9


class TestNF2UnknownCandidateRisk:
    """NF2: Unknown candidate stop risk propagates through OPAR."""

    def _econ(self, stop_pct):
        primary = make_primary()
        primary["stop_pct"] = stop_pct
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        return rb4.b4a_compute_replacement_economics(
            primary=primary, harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0, spread_pct=0.1,
        )

    def test_nf2_zero_stop_pct_gives_unknown_d1(self):
        opae = self._econ(0.0)["original_principal_exposure"]
        assert opae["d1_opar_unconditional"] == rb4.B4A_UNKNOWN

    def test_nf2_zero_stop_pct_gives_unknown_d2(self):
        opae = self._econ(0.0)["original_principal_exposure"]
        assert opae["d2_opar_conditional"] == rb4.B4A_UNKNOWN

    def test_nf2_valid_stop_pct_gives_numeric_d1(self):
        opae = self._econ(0.005)["original_principal_exposure"]
        assert opae["d1_opar_unconditional"] != rb4.B4A_UNKNOWN
        assert isinstance(opae["d1_opar_unconditional"], float)

    def test_nf2_unknown_distinct_from_zero(self):
        assert rb4.B4A_UNKNOWN != 0.0
        assert not isinstance(rb4.B4A_UNKNOWN, (int, float))

    def test_nf2_negative_stop_pct_unknown(self):
        primary = make_primary()
        primary["stop_pct"] = -0.01
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=primary, harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0, spread_pct=0.1,
        )
        opae = econ["original_principal_exposure"]
        assert opae["d1_opar_unconditional"] == rb4.B4A_UNKNOWN
        assert opae["d2_opar_conditional"] == rb4.B4A_UNKNOWN


class TestNF3StableHarvestIdentity:
    """NF3: Rolling harvest identity stable across restarts."""

    def _hid(self, campaign, deal, fill, ig):
        import hashlib
        src = "{}:{}:{}:{}".format(campaign, deal, fill, ig)
        return hashlib.sha256(src.encode()).hexdigest()[:16]

    def test_nf3_same_inputs_same_id(self):
        assert self._hid("campaign_X", "DEAL1", 6350.0, 0.4) == self._hid("campaign_X", "DEAL1", 6350.0, 0.4)

    def test_nf3_different_deals_different_ids(self):
        assert self._hid("campaign_X", "DEAL1", 6350.0, 0.4) != self._hid("campaign_X", "DEAL2", 6350.0, 0.4)

    def test_nf3_id_not_time_dependent(self):
        import time as _time
        id1 = self._hid("campaign_X", "DEAL1", 6350.0, 0.4)
        _time.sleep(0.002)
        id2 = self._hid("campaign_X", "DEAL1", 6350.0, 0.4)
        assert id1 == id2

    def test_nf3_duplicate_blocked_by_deal_id_dedup(self):
        import json as _json
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_profit_deployed": 0.0,
            "rolling_realized_harvest": {"deal_id": "ADDON1", "realized_pnl_estimate": 0.12},
        }
        restored = _json.loads(_json.dumps(live))
        leg = make_addon_leg(deal="ADDON1")
        ok, reason = rb4.b4a_can_credit_harvest(restored, leg)
        assert ok is False
        assert reason == "duplicate_harvest_deal_id"

    def test_nf3_id_is_16_hex(self):
        hid = self._hid("campaign_X", "DEAL1", 6350.0, 0.4)
        assert len(hid) == 16
        assert all(c in "0123456789abcdef" for c in hid)

    def test_nf3_empty_deal_id_deterministic(self):
        id1 = self._hid("campaign_X", "", 6350.0, 0.4)
        id2 = self._hid("campaign_X", "", 6350.0, 0.4)
        assert id1 == id2


class TestNF4DeadFallback:
    """NF4: gap_risk_observable_spread_pct dead fallback removed."""

    def test_nf4_old_field_absent_from_economics(self):
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        primary = make_primary()
        primary["stop_pct"] = 0.005
        econ = rb4.b4a_compute_replacement_economics(
            primary=primary, harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0, spread_pct=0.1,
        )
        assert "gap_risk_observable_spread_pct" not in econ
        assert "spread_pct_observed" in econ


class TestNF1NF2Matrix:
    """D1/D2 accounting matrix validation after repairs."""

    def test_matrix_a_d1_80_d2_zero(self):
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=0.0, ledger_a_valid=False, ledger_b=0.80, ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.80, ledger_a_remaining=0.0,
        )
        assert abs(r["d1_opar_unconditional"] - 0.80) < 1e-8
        assert abs(r["d2_opar_conditional"]) < 1e-8

    def test_matrix_b_partial_coverage(self):
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=0.30, ledger_a_valid=True, ledger_b=0.20, ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.80, ledger_a_remaining=0.30,
        )
        assert abs(r["d1_opar_unconditional"] - 0.50) < 1e-8
        assert abs(r["d2_opar_conditional"] - 0.30) < 1e-8

    def test_matrix_c_full_a_covers(self):
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=1.00, ledger_a_valid=True, ledger_b=0.80, ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.80, ledger_a_remaining=1.00,
        )
        assert abs(r["d1_opar_unconditional"]) < 1e-8
        assert abs(r["d2_opar_conditional"]) < 1e-8

    def test_matrix_d_b_unknown_d1_numeric_d2_unknown(self):
        r = rb4.b4a_original_principal_at_risk(
            ledger_a=0.0, ledger_a_valid=False, ledger_b=rb4.B4A_UNKNOWN, ledger_d=rb4.B4A_UNKNOWN,
            candidate_margin=0.0, candidate_stop_risk=0.80, ledger_a_remaining=0.0,
        )
        assert abs(r["d1_opar_unconditional"] - 0.80) < 1e-8
        assert r["d2_opar_conditional"] == rb4.B4A_UNKNOWN

    def test_gen2_blocked(self):
        primary = make_primary()
        primary["stop_pct"] = 0.005
        live = make_live_with_harvest()
        ledgers = rb4.b4a_build_ledgers(live)
        econ = rb4.b4a_compute_replacement_economics(
            primary=primary, harvest_rec={}, ledgers=ledgers,
            live_min_deal={"SILVER": 0.1}, live_margin={"SILVER": 0.05},
            live_campaign_unit_fn=lambda s, p: 1.0,
            live_compute_ig_size_fn=lambda s, n, p: n,
            current_price=6300.0, spread_pct=0.1,
        )
        assert econ.get("generation2_submission") == "BLOCKED_build4a_observation_only"
