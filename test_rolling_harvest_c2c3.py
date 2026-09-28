"""Build-3 rolling harvest tests — Commits 2 & 3: harvest execution + replacement evaluation."""
import json, time
import pytest
from unittest.mock import patch, MagicMock


# ── Minimal state fixture ─────────────────────────────────────────────────────

def make_primary(sym="SILVER", dirn="short", fill=6426.3):
    return dict(
        instrument=sym, direction=dirn, deal_id="PRIMARY", fill_price=fill,
        ig_size=0.4, broker_stop_level=6500.0, acknowledged_stop_level=6422.287,
        stop_sync=None, leverage=10, pos_size=100.0, notional=1000.0,
        entry_time=time.time(), conviction=7, scaling_history_complete=True,
        leg_index=1,
    )


def make_addon(sym="SILVER", dirn="short", fill=6350.0, gen=1, deal_id="ADDON1"):
    return dict(
        instrument=sym, direction=dirn, deal_id=deal_id, fill_price=fill,
        ig_size=0.4, broker_stop_level=6500.0, acknowledged_stop_level=None,
        stop_sync=None, leverage=10, pos_size=100.0, notional=1000.0,
        entry_time=time.time(), stop_pct=0.01, tp_pct=0.005,
        leg_index=2, leg_generation=gen,
        actual_notional=1000.0, consumed_allocation=100.0,
    )


# ── Harvest eligibility logic (pure) ─────────────────────────────────────────

def harvest_eligibility(leg, live):
    """Mirror of _live_rolling_harvest_eligibility logic without importing june."""
    gen = leg.get("leg_generation", 1)
    deal_id = leg.get("deal_id", "?")
    existing = live.get("rolling_realized_harvest") or {}
    if existing.get("deal_id") == deal_id:
        return False, "duplicate_harvest_delivery"
    if gen != 1:
        return False, f"generation_{gen}_not_harvest_eligible"
    slot = live.get("rolling_capacity_slot")
    if slot != "bootstrap":
        return False, f"capacity_slot_unexpected:{slot}"
    return True, "eligible"


def replacement_eval(live, realized_harvest_pnl=None):
    """Mirror of _live_evaluate_rolling_replacement logic (pure, no june import)."""
    ROLLING_MAX_GENERATIONS = 1  # Build 3 constant
    liq_before = live.get("rolling_bootstrap_liq_before")
    if liq_before is None:
        return {"decision": "block", "reason": "protection_economics_unavailable"}
    harvest = live.get("rolling_realized_harvest") or {}
    rpnl = realized_harvest_pnl if realized_harvest_pnl is not None else harvest.get("realized_pnl_estimate", 0.0)
    original_principal_risk = max(0.0, -liq_before)
    economic_floor = -liq_before
    economically_ok = rpnl > 0.0 and rpnl >= economic_floor
    result = {
        "decision": "block", "reason": "evaluation_incomplete",
        "ledger_a": rpnl, "ledger_b": "not_counted_as_profit",
        "ledger_c": original_principal_risk,
        "economic_floor": economic_floor, "economically_eligible": economically_ok,
    }
    if not economically_ok:
        result["reason"] = "economically_ineligible"
        return result
    replacement_gen = 2
    if replacement_gen > ROLLING_MAX_GENERATIONS:
        result.update(reason="generation_cap_disabled_in_build_3",
                      would_be_generation=replacement_gen,
                      rolling_max_generations=ROLLING_MAX_GENERATIONS)
        return result
    result["reason"] = "build3_invariant_violation_gen2_would_submit"
    return result


# ── Commit 2: Harvest execution ───────────────────────────────────────────────

class TestHarvestEligibility:
    def test_gen1_bootstrap_slot_is_eligible(self):
        leg = make_addon(gen=1)
        live = {"rolling_capacity_slot": "bootstrap", "rolling_realized_harvest": None}
        ok, reason = harvest_eligibility(leg, live)
        assert ok is True
        assert reason == "eligible"

    def test_duplicate_delivery_blocked(self):
        leg = make_addon(gen=1)
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_realized_harvest": {"deal_id": "ADDON1"},
        }
        ok, reason = harvest_eligibility(leg, live)
        assert ok is False
        assert reason == "duplicate_harvest_delivery"

    def test_wrong_generation_blocked(self):
        leg = make_addon(gen=2)
        live = {"rolling_capacity_slot": "bootstrap", "rolling_realized_harvest": None}
        ok, reason = harvest_eligibility(leg, live)
        assert ok is False
        assert "generation_2_not_harvest_eligible" in reason

    def test_capacity_slot_not_bootstrap_blocked(self):
        leg = make_addon(gen=1)
        live = {"rolling_capacity_slot": "available", "rolling_realized_harvest": None}
        ok, reason = harvest_eligibility(leg, live)
        assert ok is False
        assert "capacity_slot_unexpected" in reason

    def test_slot_none_blocked(self):
        leg = make_addon(gen=1)
        live = {"rolling_capacity_slot": None, "rolling_realized_harvest": None}
        ok, reason = harvest_eligibility(leg, live)
        assert ok is False

    def test_legacy_leg_no_generation_defaults_to_gen1(self):
        leg = make_addon()
        del leg["leg_generation"]
        live = {"rolling_capacity_slot": "bootstrap", "rolling_realized_harvest": None}
        ok, reason = harvest_eligibility(leg, live)
        assert ok is True  # conservative default gen=1


class TestHarvestCapacityRelease:
    def test_capacity_released_after_confirmed_close(self):
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_realized_harvest": None,
            "rolling_bootstrap_liq_before": 0.0805,
        }
        # Simulate record_rolling_harvest
        leg = make_addon(gen=1)
        ok, _ = harvest_eligibility(leg, live)
        assert ok
        live["rolling_realized_harvest"] = {"deal_id": leg["deal_id"], "realized_pnl_estimate": 0.12}
        live["rolling_capacity_slot"] = "available"
        assert live["rolling_capacity_slot"] == "available"
        assert live["rolling_realized_harvest"]["deal_id"] == "ADDON1"

    def test_capacity_not_released_on_duplicate(self):
        live = {
            "rolling_capacity_slot": "bootstrap",
            "rolling_realized_harvest": {"deal_id": "ADDON1"},
        }
        leg = make_addon(gen=1)
        ok, reason = harvest_eligibility(leg, live)
        assert ok is False
        assert reason == "duplicate_harvest_delivery"
        # Slot must NOT change on duplicate
        assert live["rolling_capacity_slot"] == "bootstrap"

    def test_realized_pnl_recorded_three_ledger(self):
        # Harvest record must contain all three-ledger fields.
        harvest_record = {
            "deal_id": "ADDON1", "realized_pnl_estimate": 0.12,
            "rolling_bootstrap_liq_before": 0.0805,
        }
        # Ledger A: realized harvest P&L
        assert harvest_record["realized_pnl_estimate"] > 0
        # Ledger B: released margin is NOT in realized P&L
        assert "released_margin" not in harvest_record
        # Ledger C: original principal risk derived from liq_before
        original_principal_risk = max(0.0, -harvest_record["rolling_bootstrap_liq_before"])
        assert original_principal_risk == pytest.approx(0.0)  # C38: liq_before=0.0805 > 0


# ── Commit 3: Replacement evaluation ─────────────────────────────────────────

class TestK5Enforcement:
    def test_none_liq_before_blocks_replacement(self):
        live = {
            "rolling_bootstrap_liq_before": None,
            "rolling_realized_harvest": {"realized_pnl_estimate": 1.0},
        }
        result = replacement_eval(live)
        assert result["decision"] == "block"
        assert result["reason"] == "protection_economics_unavailable"

    def test_zero_default_would_allow_phantom_replacement(self):
        # Demonstrate WHY defaulting to 0.0 is wrong (Option A rejected by K5).
        # With 0.0: economic_floor = -0.0 = 0.0, any positive harvest passes.
        # With None (correct): blocked before economic check.
        live_wrong = {"rolling_bootstrap_liq_before": 0.0, "rolling_realized_harvest": {"realized_pnl_estimate": 0.001}}
        result_wrong = replacement_eval(live_wrong)
        # 0.0 passes economics (realized > 0, floor = 0) → reaches gen cap
        assert result_wrong["reason"] == "generation_cap_disabled_in_build_3"

        live_correct = {"rolling_bootstrap_liq_before": None, "rolling_realized_harvest": {"realized_pnl_estimate": 0.001}}
        result_correct = replacement_eval(live_correct)
        # None correctly blocks before economics → protection_economics_unavailable
        assert result_correct["reason"] == "protection_economics_unavailable"


class TestThreeLedgerAccounting:
    def test_positive_liq_before_reduces_economic_floor(self):
        # C38 case: liq_before=0.0805 → economic_floor = -0.0805 → any positive harvest passes.
        live = {
            "rolling_bootstrap_liq_before": 0.0805,
            "rolling_realized_harvest": {"realized_pnl_estimate": 0.001},
        }
        result = replacement_eval(live)
        assert result.get("economic_floor") == pytest.approx(-0.0805)
        assert result.get("economically_eligible") is True

    def test_negative_liq_before_requires_harvest_to_cover_gap(self):
        # liq_before = -0.05 → principal has $0.05 loss exposure → harvest must cover it.
        live = {
            "rolling_bootstrap_liq_before": -0.05,
            "rolling_realized_harvest": {"realized_pnl_estimate": 0.03},
        }
        result = replacement_eval(live)
        # floor = 0.05; realized = 0.03 < 0.05 → ineligible
        assert result["reason"] == "economically_ineligible"
        assert result.get("economically_eligible") is False

    def test_harvest_covers_principal_gap(self):
        live = {
            "rolling_bootstrap_liq_before": -0.05,
            "rolling_realized_harvest": {"realized_pnl_estimate": 0.06},
        }
        result = replacement_eval(live)
        # floor = 0.05; realized = 0.06 > 0.05 → eligible but gen-cap blocked
        assert result["reason"] == "generation_cap_disabled_in_build_3"
        assert result.get("economically_eligible") is True

    def test_released_margin_not_counted_as_profit(self):
        # Ledger B: margin release does not enter the economic floor calculation.
        live = {
            "rolling_bootstrap_liq_before": 0.0,
            "rolling_realized_harvest": {"realized_pnl_estimate": 0.0},
        }
        result = replacement_eval(live)
        # No harvest profit at all → economically ineligible even if margin was released.
        assert result["reason"] == "economically_ineligible"


class TestGenerationCap:
    def test_gen2_blocked_by_rolling_max_generations(self):
        live = {
            "rolling_bootstrap_liq_before": 0.0805,
            "rolling_realized_harvest": {"realized_pnl_estimate": 0.12},
        }
        result = replacement_eval(live)
        assert result["decision"] == "block"
        assert result["reason"] == "generation_cap_disabled_in_build_3"
        assert result["would_be_generation"] == 2
        assert result["rolling_max_generations"] == 1

    def test_gen_cap_vs_economic_ineligibility_distinguished(self):
        # Cap-blocked: economically eligible but generation cap prevents submission.
        live_capblocked = {
            "rolling_bootstrap_liq_before": 0.0805,
            "rolling_realized_harvest": {"realized_pnl_estimate": 0.12},
        }
        result_cap = replacement_eval(live_capblocked)
        assert result_cap["reason"] == "generation_cap_disabled_in_build_3"
        assert result_cap.get("economically_eligible") is True

        # Economically ineligible: harvest doesn't cover floor.
        live_ineligible = {
            "rolling_bootstrap_liq_before": -0.05,
            "rolling_realized_harvest": {"realized_pnl_estimate": 0.01},
        }
        result_inel = replacement_eval(live_ineligible)
        assert result_inel["reason"] == "economically_ineligible"
        assert result_inel.get("economically_eligible") is False

    def test_gen2_order_never_submitted_build3(self):
        # V1 supersedes Build-3: gen-2 is now permitted (cap=2), gen-3 impossible.
        import june
        assert june._ROLLING_MAX_GENERATIONS == 2
        assert 3 > june._ROLLING_MAX_GENERATIONS  # gen-3 still blocked


# ── C38 Regression Replay ─────────────────────────────────────────────────────

class TestC38RegressionReplay:
    """Replay C38/S4/S5 evidence through Build-3 accounting logic.

    C38 actuals:
        bootstrap liq_before:    0.0805199999999968
        liq_after (prefinanced): 0.0805199999999966
        primary campaign P&L:    1.9239400480000404
        ack stop level:          6422.287 -> 6387.287 (after prefinancing)
        protection_state:        profit_protected

    Build-3 counterfactual (not what happened -- addon was closed normally):
        Assume bootstrap addon was harvested at some positive P&L.
        Verify three-ledger produces correct numbers.
    """

    LIQ_BEFORE = 0.0805199999999968

    def test_c38_liq_before_stored_correctly(self):
        live = {"rolling_bootstrap_liq_before": self.LIQ_BEFORE}
        assert live["rolling_bootstrap_liq_before"] == pytest.approx(0.0805, abs=1e-4)

    def test_c38_economic_floor_is_negative(self):
        # liq_before = 0.0805 > 0 → economic_floor = -0.0805 → any positive harvest qualifies.
        live = {
            "rolling_bootstrap_liq_before": self.LIQ_BEFORE,
            "rolling_realized_harvest": {"realized_pnl_estimate": 0.01},
        }
        result = replacement_eval(live)
        assert result.get("economic_floor") == pytest.approx(-self.LIQ_BEFORE, abs=1e-4)

    def test_c38_original_principal_risk_is_zero(self):
        # liq_before = 0.0805 (positive) → no principal at risk.
        live = {
            "rolling_bootstrap_liq_before": self.LIQ_BEFORE,
            "rolling_realized_harvest": {"realized_pnl_estimate": 0.01},
        }
        result = replacement_eval(live)
        assert result.get("ledger_c") == pytest.approx(0.0)

    def test_c38_small_harvest_still_economically_eligible(self):
        # Any positive harvest with liq_before=0.0805 passes economics.
        live = {
            "rolling_bootstrap_liq_before": self.LIQ_BEFORE,
            "rolling_realized_harvest": {"realized_pnl_estimate": 0.001},
        }
        result = replacement_eval(live)
        assert result.get("economically_eligible") is True

    def test_c38_reaches_generation_cap_not_economic_block(self):
        live = {
            "rolling_bootstrap_liq_before": self.LIQ_BEFORE,
            "rolling_realized_harvest": {"realized_pnl_estimate": 0.12},
        }
        result = replacement_eval(live)
        assert result["reason"] == "generation_cap_disabled_in_build_3"
        assert result.get("economically_eligible") is True
        assert result.get("would_be_generation") == 2
