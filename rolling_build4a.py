"""
rolling_build4a.py - Build 4A: Shared Rolling Machinery

Implements SHARED infrastructure across rolling architecture candidates.
NO POLICY DECISIONS. NO GEN-2 BROKER SUBMISSION. TELEMETRY ONLY.

FOUR-LEDGER SEPARATION:
  LEDGER A: realized rolling profit (broker-confirmed harvested addon P&L)
  LEDGER B: acknowledged protected primary floor (NOT cash, NOT profit)
  LEDGER C: released margin when addon closed (NOT profit)
  LEDGER D: original principal still at risk

K5 INVARIANT: missing protection = UNKNOWN, never coerced to 0.0.
Released margin NEVER enters Ledger A.
Protected primary NEVER treated as broker cash.
Build 4A does NOT select any policy.
Build 4A does NOT deploy realized profit.
Build 4A does NOT enable gen-2 broker orders.
"""
from __future__ import annotations
import time, uuid, logging
from typing import Optional

logger = logging.getLogger(__name__)

# Sentinel: missing value != 0 (K5 invariant)
B4A_UNKNOWN = "UNKNOWN"

# Counterfactual policy labels for telemetry
B4A_POLICIES = [
    "pure_self_funding",
    "bootstrap_0_25R",
    "bootstrap_0_50R",
    "bootstrap_0_75R",
    "bootstrap_1_00R",
]


# ---- Campaign / harvest / eval identifiers ----------------------------------

def b4a_campaign_id(primary: dict) -> str:
    """Derive stable campaign id from primary deal_id (broker-anchored)."""
    deal_id = primary.get("deal_id", "")
    return f"campaign_{deal_id}" if deal_id else f"campaign_unknown_{int(time.time())}"


def b4a_harvest_id(leg: dict) -> str:
    """Stable harvest deduplication key from addon deal_id."""
    deal_id = leg.get("deal_id", "")
    return f"harvest_{deal_id}" if deal_id else f"harvest_unknown_{int(time.time())}"


def b4a_eval_id() -> str:
    """Unique id for one replacement evaluation pass."""
    return f"eval_{int(time.time())}_{uuid.uuid4().hex[:8]}"


# ---- Four-ledger accounting --------------------------------------------------

def b4a_build_ledgers(live: dict) -> dict:
    """Build four-ledger snapshot from _live state.

    K5: missing protection = UNKNOWN, never 0.0.
    Released margin NEVER enters Ledger A.
    Protected primary NEVER treated as cash.
    F3 FIX: prefer current liq_before over bootstrap snapshot; label staleness.
    Primary closed => Ledger B cleared to UNKNOWN (protection no longer applies).
    """
    harvest_rec = live.get("rolling_realized_harvest") or {}

    # LEDGER A: realized rolling profit (confirmed harvested addon P&L only)
    ledger_a_raw    = harvest_rec.get("realized_pnl_estimate")
    ledger_a_source = harvest_rec.get("exit_price_source", "no_harvest")
    if ledger_a_raw is None:
        ledger_a, ledger_a_valid = 0.0, False
    else:
        ledger_a, ledger_a_valid = float(ledger_a_raw), True

    ledger_a_deployed  = float(live.get("rolling_profit_deployed", 0.0))
    ledger_a_remaining = round(ledger_a - ledger_a_deployed, 8) if ledger_a_valid else 0.0

    # LEDGER B: acknowledged protected primary floor (NOT cash, NOT profit)
    # F3 FIX: prefer live current liq_before over stale bootstrap snapshot.
    # NF1 FIX: validate liq_before_provenance campaign_id before trusting current value.
    # When primary is closed, protection no longer applies -> UNKNOWN.
    primary_open  = live.get("open_position") is not None
    current_liq   = live.get("liq_before")                      # fresh each cycle if available
    bootstrap_liq = live.get("rolling_bootstrap_liq_before")    # snapshot at addon open

    # NF1: campaign provenance check -- previous campaign's liq_before must not carry forward
    liq_prov       = live.get("liq_before_provenance") or {}
    current_campaign = live.get("rolling_campaign_id")
    _prov_campaign  = liq_prov.get("campaign_id")
    _campaign_valid = (
        current_liq is None  # no current value -- nothing to invalidate
        or _prov_campaign is None  # no provenance (legacy / bootstrap-only state)
        or _prov_campaign == current_campaign  # same campaign -- valid
    )
    if not _campaign_valid:
        # Campaign mismatch: previous campaign's protection value must not survive
        current_liq = None

    if not primary_open:
        liq_before          = None
        ledger_b_provenance = "primary_closed_protection_cleared"
    elif current_liq is not None:
        liq_before          = current_liq
        ledger_b_provenance = (
            "current_broker_acknowledged_stop"
            if liq_prov.get("source") == "broker_acknowledged_stop"
            else "current_liq_before"
        )
    elif bootstrap_liq is not None:
        liq_before          = bootstrap_liq
        ledger_b_provenance = "bootstrap_snapshot_stale"
    else:
        liq_before          = None
        ledger_b_provenance = "not_recorded_normal_controls_path"

    if liq_before is None:
        ledger_b = B4A_UNKNOWN
    else:
        ledger_b = float(liq_before)

    # LEDGER C: released margin when addon closed (informational, NOT profit)
    addon_ig   = harvest_rec.get("ig_size", 0.0)
    addon_exit = harvest_rec.get("confirmed_exit_price") or harvest_rec.get("exit_mid_estimate", 0.0)
    ledger_c = {
        "approx_notional_released": addon_ig * addon_exit if (addon_ig and addon_exit) else 0.0,
        "note": "NOT_PROFIT_released_margin_only",
        "source": "addon_exit_notional_estimate",
    }

    # LEDGER D: original principal at risk = max(0, -liq_before)
    if ledger_b == B4A_UNKNOWN:
        ledger_d         = B4A_UNKNOWN
        ledger_d_formula = "UNKNOWN_protection_economics_unavailable"
    else:
        raw_d    = max(0.0, -float(ledger_b))
        ledger_d = round(raw_d, 8)
        ledger_d_formula = (
            f"max(0, -liq_before) = max(0, -{float(ledger_b):.6f}) = {ledger_d:.6f}"
        )

    return {
        "ledger_a_realized_profit":           round(ledger_a, 8) if ledger_a_valid else 0.0,
        "ledger_a_valid":                     ledger_a_valid,
        "ledger_a_source":                    ledger_a_source,
        "ledger_a_deployed":                  round(ledger_a_deployed, 8),
        "ledger_a_remaining":                 round(ledger_a_remaining, 8),
        "ledger_b_protected_primary":         ledger_b,
        "ledger_b_provenance":                ledger_b_provenance,
        "bootstrap_liq_before_historical":    bootstrap_liq,   # preserved for provenance (F3)
        "ledger_c_released_margin":           ledger_c,
        "ledger_d_principal_at_risk":         ledger_d,
        "ledger_d_formula":                   ledger_d_formula,
        "released_margin_in_realized_profit": False,    # invariant assertion
        "protected_primary_treated_as_cash":  False,    # invariant assertion
    }


# ---- Original principal exposure --------------------------------------------

def b4a_original_principal_at_risk(
    ledger_a: float,
    ledger_a_valid: bool,
    ledger_b,
    ledger_d,
    candidate_margin: float,
    candidate_stop_risk: float,
    ledger_a_remaining: float = None,
) -> dict:
    """D1/D2 split: unconditional and conditional original principal exposure.

    F2 FIX: B (protection) is NOT subtracted in D1. Only A (realized remaining) covers D1.
    F5 FIX: uses ledger_a_remaining (not total) for coverage.
    F6 FIX: floors realized_remaining at 0; negative A never inflates D1 beyond stop_risk.

    D1 UNCONDITIONAL OPAR:
      realized_available = max(0, ledger_a_remaining)   [floor negative at 0]
      realized_coverage  = min(realized_available, candidate_stop_risk)
      D1 = max(0, candidate_stop_risk - realized_coverage)

    D2 CONDITIONAL CAMPAIGN EXPOSURE:
      IF ledger_b = UNKNOWN -> D2 = UNKNOWN  (K5 invariant on D2 only)
      ELSE protection_coverage = max(0, liq_before)
           D2 = max(0, D1 - protection_coverage)
      D2 is CONDITIONAL: requires primary to realize at or above protection floor.

    B NEVER enters the realized-profit ledger.
    C NEVER enters the realized-profit ledger.
    """
    # F5: use remaining (not total) for D1 coverage
    a_rem = ledger_a_remaining if ledger_a_remaining is not None else ledger_a

    # F6: floor negative realized at 0 so negative A cannot inflate D1
    realized_available = max(0.0, a_rem) if ledger_a_valid else 0.0
    realized_coverage  = min(realized_available, max(0.0, candidate_stop_risk))
    d1 = round(max(0.0, candidate_stop_risk - realized_coverage), 8)

    # D2: conditional on known primary protection (K5 applies here, not to D1)
    if ledger_b == B4A_UNKNOWN:
        d2             = B4A_UNKNOWN
        prot_coverage  = B4A_UNKNOWN
        k5_applied     = True
    else:
        liq_b         = float(ledger_b)
        prot_coverage = round(max(0.0, liq_b), 8)
        d2            = round(max(0.0, d1 - prot_coverage), 8)
        k5_applied    = False

    return {
        # D1: unconditional (primary accounting value)
        "original_principal_at_risk":         d1,   # = D1 (backward-compat key)
        "d1_opar_unconditional":              d1,
        # D2: conditional on primary protection being realized
        "d2_opar_conditional":                d2,
        "d2_condition":                       "requires_primary_to_realize_at_or_above_protection_floor",
        # Coverage detail
        "realized_profit_coverage":           round(realized_coverage, 8),
        "protection_coverage":                prot_coverage,
        "principal_residual":                 d1,   # = D1
        "candidate_stop_risk":                round(candidate_stop_risk, 8),
        "formula_d1": (
            f"max(0, {candidate_stop_risk:.6f}"
            f" - min(max(0,{a_rem:.6f}),{candidate_stop_risk:.6f}))"
            f" = {d1:.6f}"
        ),
        "k5_invariant_applied":               k5_applied,
        "b_entered_profit_ledger":            False,
        "c_entered_profit_ledger":            False,
    }


# ---- Double-spend accounting ------------------------------------------------

def b4a_can_credit_harvest(live: dict, leg: dict) -> tuple:
    """Check whether harvest can be credited -- prevents double-spend.

    Returns (can_credit: bool, reason: str). Fail-closed.
    """
    deal_id  = leg.get("deal_id", "")
    gen      = leg.get("leg_generation", 1)
    slot     = live.get("rolling_capacity_slot")
    existing = live.get("rolling_realized_harvest") or {}

    if existing.get("deal_id") == deal_id:
        return False, "duplicate_harvest_deal_id"
    if gen != 1:
        return False, f"generation_{gen}_not_harvest_eligible"
    if slot != "bootstrap":
        return False, f"capacity_slot_unexpected:{slot}"
    deployed = live.get("rolling_profit_deployed", 0.0)
    if deployed > 0:
        return False, "realized_profit_already_deployed"
    return True, "eligible"


def b4a_can_deploy_profit(live: dict, amount: float) -> tuple:
    """Check whether realized profit can be deployed. BUILD 4A: always blocked."""
    return False, "build4a_observation_only_no_deployment"


# ---- Replacement economics evaluator ----------------------------------------

def b4a_compute_replacement_economics(
    primary: dict,
    harvest_rec: dict,
    ledgers: dict,
    live_min_deal: dict,
    live_margin: dict,
    live_campaign_unit_fn,
    live_compute_ig_size_fn,
    mindeal_oversize_max: float = 3.5,
    current_price: float = 0.0,
    spread_pct: float = 0.0,
) -> dict:
    """Compute candidate gen-2 replacement economics -- NO ORDER SUBMISSION.

    OBSERVATIONAL ONLY. No IG API calls. Does not modify _live state.
    Unknown values labeled B4A_UNKNOWN (never 0.0 for missing data).
    """
    sym  = primary.get("instrument", "")
    dirn = primary.get("direction", "long")

    if current_price <= 0:
        return {"status": "no_price", "reason": "candidate_economics_unavailable"}

    min_deal    = live_min_deal.get(sym, 1.0)
    margin_rate = live_margin.get(sym, 0.0)
    campaign_id = b4a_campaign_id(primary)
    eval_id_val = b4a_eval_id()

    half_sp  = current_price * spread_pct / 200.0
    fill_ref = (current_price - half_sp) if dirn == "long" else (current_price + half_sp)

    # 1x/2x/3x/4x MINDEAL candidate economics (observational only)
    candidate_sizes = {}
    for mult in [1, 2, 3, 4]:
        cand_ig = min_deal * mult
        try:
            cand_unit     = live_campaign_unit_fn(sym, current_price)
            cand_notional = cand_ig * cand_unit
            cand_margin   = cand_notional * margin_rate if margin_rate > 0 else B4A_UNKNOWN
        except Exception:
            cand_notional = cand_margin = B4A_UNKNOWN
        candidate_sizes[f"{mult}x_mindeal"] = {
            "ig_size":            cand_ig,
            "notional":           cand_notional,
            "margin_requirement": cand_margin,
            "orderly_stop_risk":  B4A_UNKNOWN,
            "note":               "observational_only",
        }

    # 1x MINDEAL reference
    legal_ig = min_deal
    try:
        unit       = live_campaign_unit_fn(sym, current_price)
        notional   = legal_ig * unit
        margin_req = notional * margin_rate if margin_rate > 0 else B4A_UNKNOWN
    except Exception:
        unit = notional = margin_req = B4A_UNKNOWN

    ledger_a   = ledgers.get("ledger_a_realized_profit", 0.0)
    ledger_a_r = ledgers.get("ledger_a_remaining", 0.0)
    ledger_b   = ledgers.get("ledger_b_protected_primary", B4A_UNKNOWN)
    ledger_c   = ledgers.get("ledger_c_released_margin", {})
    ledger_d   = ledgers.get("ledger_d_principal_at_risk", B4A_UNKNOWN)

    realized_remaining = (
        max(0.0, float(ledger_a_r))
        if isinstance(ledger_a_r, (int, float)) else 0.0
    )

    # F1 FIX: derive candidate stop risk from primary stop geometry (not hardcoded 0.0)
    primary_stop_pct = primary.get("stop_pct", 0.0)
    try:
        _unit = live_campaign_unit_fn(sym, current_price)
    except Exception:
        _unit = None
    if primary_stop_pct > 0 and current_price > 0 and legal_ig > 0 and _unit is not None:
        candidate_stop_risk      = round(legal_ig * _unit * current_price * primary_stop_pct, 8)
        candidate_stop_provenance = "derived_from_primary_stop_pct_same_instrument"
    elif primary_stop_pct <= 0:
        candidate_stop_risk       = B4A_UNKNOWN
        candidate_stop_provenance = "stop_pct_unavailable"
    else:
        candidate_stop_risk       = B4A_UNKNOWN
        candidate_stop_provenance = "price_or_ig_size_unavailable"

    # OPAR: pass remaining (not total) for D1; candidate_stop_risk may be UNKNOWN.
    # NF2 FIX: UNKNOWN candidate risk must propagate -- never coerce to 0.0.
    if candidate_stop_risk is B4A_UNKNOWN or not isinstance(candidate_stop_risk, (int, float)):
        opae_result = {
            "original_principal_at_risk":   B4A_UNKNOWN,
            "d1_opar_unconditional":        B4A_UNKNOWN,
            "d2_opar_conditional":          B4A_UNKNOWN,
            "d2_condition":                 "requires_primary_to_realize_at_or_above_protection_floor",
            "realized_profit_coverage":     B4A_UNKNOWN,
            "protection_coverage":          B4A_UNKNOWN,
            "principal_residual":           B4A_UNKNOWN,
            "candidate_stop_risk":          B4A_UNKNOWN,
            "formula_d1":                   "unknown_candidate_stop_risk_propagated",
            "k5_invariant_applied":         False,
            "b_entered_profit_ledger":      False,
            "c_entered_profit_ledger":      False,
            "d1_provenance":                "candidate_stop_risk_unknown_propagated",
            "d2_provenance":                "d1_unknown_propagated",
        }
    else:
        _stop_for_opar = float(candidate_stop_risk) if candidate_stop_risk > 0 else 0.0
        opae_result = b4a_original_principal_at_risk(
            ledger_a=ledger_a,
            ledger_a_valid=ledgers.get("ledger_a_valid", False),
            ledger_b=ledger_b,
            ledger_d=ledger_d,
            candidate_margin=margin_req if isinstance(margin_req, float) else 0.0,
            candidate_stop_risk=_stop_for_opar,
            ledger_a_remaining=float(ledger_a_r) if isinstance(ledger_a_r, (int, float)) else 0.0,
        )

    # F9 FIX: rename misleading field; add explicit loss-note telemetry
    spread_pct_obs = spread_pct / 100.0 if spread_pct > 0 else B4A_UNKNOWN
    nom_leverage   = primary.get("leverage", B4A_UNKNOWN)

    # F12: R_primary provenance -- expose original and note DPLE may have moved stop
    _stop_dist  = primary.get("stop_dist", 0.0)
    _ig_sz      = primary.get("ig_size", 0.0)
    r_prim_orig = round(_stop_dist * _ig_sz, 8) if (_stop_dist and _ig_sz) else B4A_UNKNOWN
    r_prim_orig_src = (
        "entry_stop_dist_stored_at_open"
        if r_prim_orig != B4A_UNKNOWN else "stop_dist_or_ig_size_unavailable"
    )
    # Current R derived from protection floor if known
    if ledger_b != B4A_UNKNOWN and float(ledger_b) < 0:
        r_prim_current = round(abs(float(ledger_b)), 8)
        r_prim_curr_src = "current_liq_before_implicit"
    else:
        r_prim_current = B4A_UNKNOWN
        r_prim_curr_src = "not_derivable_from_available_state_dple_may_have_moved_stop"

    return {
        "status":                             "evaluation_complete",
        "eval_id":                            eval_id_val,
        "campaign_id":                        campaign_id,
        "instrument":                         sym,
        "direction":                          dirn,
        "current_generation":                 1,
        "candidate_generation":               2,
        "fill_reference_price":               fill_ref,
        "legal_ig_size_1x_mindeal":           legal_ig,
        "mindeal_raw":                        min_deal,
        "margin_rate":                        margin_rate,
        "margin_requirement_1x":              margin_req,
        "nominal_leverage":                   nom_leverage,
        # F9: renamed from gap_risk_observable_spread_pct
        "spread_pct_observed":                spread_pct_obs,
        "bid_ask_spread_pct_observed":        spread_pct_obs,
        "orderly_stop_risk":                  candidate_stop_risk,
        "orderly_stop_risk_provenance":       candidate_stop_provenance,
        "gap_exposure_beyond_stop":           "not_modeled_unknown",
        "total_possible_loss_note":           "orderly_stop_risk_is_NOT_maximum_loss_gap_not_estimated",
        # F5: realized pool uses remaining
        "realized_profit_pool":               realized_remaining,
        "realized_profit_source":             ledgers.get("ledger_a_source", "no_harvest"),
        "ledger_a_realized_profit":           ledger_a,
        "ledger_b_protected_primary":         ledger_b,
        "ledger_c_released_margin":           ledger_c,
        "ledger_d_principal_at_risk":         ledger_d,
        "original_principal_exposure":        opae_result,
        "candidate_sizes_mindeal_multiples":  candidate_sizes,
        "mindeal_oversize_max":               mindeal_oversize_max,
        # F12: R_primary provenance
        "r_primary_original":                 r_prim_orig,
        "r_primary_original_source":          r_prim_orig_src,
        "r_primary_current_from_dple":        r_prim_current,
        "r_primary_current_source":           r_prim_curr_src,
        "generation2_submission":             "BLOCKED_build4a_observation_only",
        "note": (
            "All economics observational. No IG API call made. "
            "Gen-2 broker submission is impossible from this evaluator."
        ),
    }


# ---- Policy counterfactuals -------------------------------------------------

def b4a_policy_counterfactuals(
    economics: dict,
    ledgers: dict,
    harvest_rec: dict,
    r_primary,
    r_primary_provenance: str,
) -> dict:
    """Compute what each policy would decide -- TELEMETRY ONLY.

    Does not alter entry/exit/harvest/replacement/sizing/broker orders.
    policy_selected = None. deployment_authorized = False.
    """
    ledger_a   = ledgers.get("ledger_a_realized_profit", 0.0)
    ledger_a_v = ledgers.get("ledger_a_valid", False)
    opae       = economics.get("original_principal_exposure", {})
    orig_at_risk = opae.get("original_principal_at_risk", B4A_UNKNOWN)
    r_p        = r_primary

    counterfactuals = {}

    # Architecture A: pure self-funding (F10 FIX: evaluate mechanically when data available)
    _remaining_a    = ledgers.get("ledger_a_remaining", 0.0)
    _cand_stop      = economics.get("orderly_stop_risk", B4A_UNKNOWN)
    _remaining_real = float(_remaining_a) if isinstance(_remaining_a, (int, float)) else B4A_UNKNOWN
    if _remaining_real == B4A_UNKNOWN or _cand_stop == B4A_UNKNOWN:
        psf_outcome = B4A_UNKNOWN
        psf_reason  = "unknown_inputs"
    elif not ledger_a_v:
        psf_outcome = B4A_UNKNOWN
        psf_reason  = "no_realized_profit_recorded"
    elif not isinstance(_cand_stop, (int, float)) or _cand_stop <= 0:
        psf_outcome = B4A_UNKNOWN
        psf_reason  = "candidate_stop_risk_not_computed"
    else:
        _eligible   = (_remaining_real >= float(_cand_stop))
        psf_outcome = "eligible" if _eligible else "insufficient_realized_profit"
        psf_reason  = (
            f"remaining={_remaining_real:.6f}_vs_stop={float(_cand_stop):.6f}"
        )
    counterfactuals["pure_self_funding"] = {
        "policy":      "pure_self_funding",
        "condition":   "remaining_realized >= candidate_stop_risk",
        "H_realized":  round(float(_remaining_a), 8) if isinstance(_remaining_a, (int, float)) and ledger_a_v else B4A_UNKNOWN,
        "R_candidate": _cand_stop,
        "outcome":     psf_outcome,
        "reason":      psf_reason,
        "note":        "evaluated_mechanically_when_data_available",
    }

    # Architecture B variants
    for label, bound in [
        ("bootstrap_0_25R", 0.25),
        ("bootstrap_0_50R", 0.50),
        ("bootstrap_0_75R", 0.75),
        ("bootstrap_1_00R", 1.00),
    ]:
        if r_p == B4A_UNKNOWN or orig_at_risk == B4A_UNKNOWN:
            cf = {
                "policy": label, "condition": f"O <= {bound} x R_primary",
                "O": orig_at_risk, "R_primary": r_p, "bound": bound,
                "outcome": B4A_UNKNOWN, "note": "r_primary_or_O_unknown",
            }
        else:
            o_val     = float(orig_at_risk)
            r_val     = float(r_p)
            threshold = bound * r_val
            eligible  = (r_val > 0 and o_val <= threshold)
            cf = {
                "policy": label, "condition": f"O <= {bound} x R_primary",
                "O": round(o_val, 8), "R_primary": round(r_val, 8),
                "bound": bound, "threshold": round(threshold, 8),
                "outcome": "eligible" if eligible else "ineligible",
                "note": f"O={o_val:.6f} vs {bound}xR={threshold:.6f}",
            }
        counterfactuals[label] = cf

    return {
        "telemetry_only":        True,
        "epoch":                 time.time(),
        "r_primary":             r_p,
        "r_primary_provenance":  r_primary_provenance,
        "counterfactuals":       counterfactuals,
        "policy_selected":       None,
        "deployment_authorized": False,
    }


# ---- Leverage observability -------------------------------------------------

def b4a_leverage_observability(
    primary: dict,
    harvest_rec: dict,
    ledgers: dict,
    economics: dict,
    r_primary,
) -> dict:
    """Leverage observability breakdown for telemetry."""
    notional = primary.get("notional", 0.0)
    stop_pct = primary.get("stop_pct", 0.0)
    leverage = primary.get("leverage", 1)
    pos_size = primary.get("pos_size", 0.0)

    nom_lev  = (notional / pos_size) if pos_size > 0 else leverage
    stop_exp = notional * stop_pct if notional and stop_pct else B4A_UNKNOWN
    gap_obs  = economics.get("spread_pct_observed", B4A_UNKNOWN)  # NF4: removed stale gap_risk_observable_spread_pct fallback (field never produced)

    ledger_a   = ledgers.get("ledger_a_realized_profit", 0.0)
    ledger_a_v = ledgers.get("ledger_a_valid", False)
    ledger_b   = ledgers.get("ledger_b_protected_primary", B4A_UNKNOWN)
    opae       = economics.get("original_principal_exposure", {})
    orig_at_risk = opae.get("original_principal_at_risk", B4A_UNKNOWN)

    if isinstance(stop_exp, float) and stop_exp > 0:
        profit_backed = (
            min(1.0, ledger_a / stop_exp) if ledger_a_v and ledger_a > 0 else 0.0
        )
        if ledger_b == B4A_UNKNOWN:
            prot_backed = B4A_UNKNOWN
        else:
            liq_b       = float(ledger_b)
            prot_backed = min(1.0, max(0.0, liq_b) / stop_exp) if liq_b > 0 else 0.0
    else:
        profit_backed = prot_backed = B4A_UNKNOWN

    return {
        "nominal_leverage":                   round(nom_lev, 4),
        "orderly_stop_exposure_usd":          stop_exp,
        "gap_risk_spread_pct":                gap_obs,
        "realized_profit_backed_fraction":    profit_backed,
        "protected_primary_backed_fraction":  prot_backed,
        "original_principal_exposure_usd":    orig_at_risk,
        "position_notional":                  round(notional, 4),
        "position_stop_pct":                  stop_pct,
    }


# ---- Failure atomicity scenario matrix -------------------------------------

FAILURE_SCENARIOS = {
    "A_harvest_order_rejected": {
        "broker_state":   "addon still open, TP not triggered",
        "local_state":    "pyramid_legs unchanged; harvest not recorded",
        "ledger_state":   "no change; slot stays bootstrap",
        "double_count":   "impossible; no credit issued",
        "gen2_possible":  "NO; replacement eval requires harvest record",
        "recovery":       "next cycle re-evaluates addon exit naturally",
    },
    "B_harvest_confirmation_unknown": {
        "broker_state":   "close may or may not have executed",
        "local_state":    "_live_close_addon_leg preserves leg if confirm fails",
        "ledger_state":   "no harvest credit; slot stays bootstrap",
        "double_count":   "impossible; credit requires ACCEPTED confirm",
        "gen2_possible":  "NO",
        "recovery":       "next cycle: leg present -> re-evaluate",
    },
    "C_broker_closes_addon_local_save_fails": {
        "broker_state":   "addon closed by IG",
        "local_state":    "leg still in pyramid_legs; slot bootstrap",
        "ledger_state":   "harvest NOT credited (save failed)",
        "double_count":   "possible on next observed close; deduplicated by deal_id",
        "gen2_possible":  "NO; slot not available",
        "recovery":       "restart: validation sees bootstrap+no gen-1 leg; reconciles",
    },
    "D_local_state_saves_before_process_dies": {
        "broker_state":   "addon may be open or closed",
        "local_state":    "partial state in Redis; slot bootstrap",
        "ledger_state":   "harvest NOT credited if process died before credit",
        "double_count":   "deal_id dedup prevents double-credit on replay",
        "gen2_possible":  "NO",
        "recovery":       "restart: load_state + validate_rolling_state_on_load",
    },
    "E_restart_before_realized_profit_ledger_credit": {
        "broker_state":   "addon closed",
        "local_state":    "slot=bootstrap; no harvest record",
        "ledger_state":   "Ledger A = 0",
        "double_count":   "deal_id guard prevents double-credit when observed later",
        "gen2_possible":  "NO; no harvest record blocks replacement",
        "recovery":       "next cycle: addon absent -> harvest eligibility checked",
    },
    "F_restart_after_realized_profit_ledger_credit": {
        "broker_state":   "addon closed",
        "local_state":    "slot=available; harvest record present in Redis",
        "ledger_state":   "Ledger A credited; persists across restart",
        "double_count":   "deal_id guard prevents re-credit",
        "gen2_possible":  "NO; _ROLLING_MAX_GENERATIONS=1 + submission boundary guard",
        "recovery":       "restart: slot=available+no gen-1 leg = consistent post-harvest",
    },
    "G_duplicate_broker_confirmation": {
        "broker_state":   "one close event delivered twice",
        "local_state":    "second: existing.deal_id == incoming -> skip",
        "ledger_state":   "Ledger A unchanged; no double-credit",
        "double_count":   "IMPOSSIBLE; deal_id dedup in b4a_can_credit_harvest",
        "gen2_possible":  "NO",
        "recovery":       "skipped with reason=duplicate_harvest_deal_id",
    },
    "H_duplicate_harvest_event": {
        "broker_state":   "same",
        "local_state":    "_live_rolling_harvest_eligibility also checks deal_id",
        "ledger_state":   "no double-credit; two independent deal_id checks",
        "double_count":   "IMPOSSIBLE",
        "gen2_possible":  "NO",
        "recovery":       "logged as duplicate_harvest_delivery",
    },
    "I_duplicate_replacement_evaluation": {
        "broker_state":   "no change; evaluation never submits",
        "local_state":    "eval_id changes each call; last result overwrites",
        "ledger_state":   "no change; evaluator is read-only",
        "double_count":   "IMPOSSIBLE; evaluator never credits",
        "gen2_possible":  "NO; blocked at submission boundary",
        "recovery":       "idempotent; last eval result is authoritative",
    },
    "J_redis_unavailable": {
        "broker_state":   "no change",
        "local_state":    "_live_save_state logs warning; in-memory state intact",
        "ledger_state":   "in-memory ledger intact; persistence delayed",
        "double_count":   "possible if crash before Redis recovers; deal_id mitigates",
        "gen2_possible":  "NO",
        "recovery":       "Redis recovery -> next save_state persists in-memory",
    },
    "K_sqlite_telemetry_unavailable": {
        "broker_state":   "no change; trading continues",
        "local_state":    "no change; telemetry is best-effort",
        "ledger_state":   "no change; ledgers in Redis not SQLite",
        "double_count":   "IMPOSSIBLE; telemetry is write-only observability",
        "gen2_possible":  "NO",
        "recovery":       "telemetry gap logged; no trading impact",
    },
    "L_primary_exits_while_addon_harvest_pending": {
        "broker_state":   "primary closed; addon may be open",
        "local_state":    "open_position cleared; addon tracking remains",
        "ledger_state":   "harvest not yet credited",
        "double_count":   "possible if addon later closes; deal_id guard protects",
        "gen2_possible":  "NO; no primary = replacement blocked",
        "recovery":       "_live_close_all_addon_legs; campaign cleanup clears rolling",
    },
    "M_primary_exits_after_harvest_before_replacement_eval": {
        "broker_state":   "primary closed; harvest recorded; no replacement submitted",
        "local_state":    "slot=available; open_position=None",
        "ledger_state":   "Ledger A credited; others unchanged",
        "double_count":   "IMPOSSIBLE; no replacement path without primary",
        "gen2_possible":  "NO; evaluator: no_primary_position",
        "recovery":       "campaign cleanup clears rolling state on next open",
    },
    "N_broker_position_disappears": {
        "broker_state":   "position gone; reason unknown",
        "local_state":    "orphan_suspected=True; manual_review_required=True",
        "ledger_state":   "no change; no credit without confirmed close",
        "double_count":   "IMPOSSIBLE",
        "gen2_possible":  "NO",
        "recovery":       "operator reviews IG app; reconcile_positions called",
    },
    "O_campaign_closes_while_rolling_state_exists": {
        "broker_state":   "primary closed normally",
        "local_state":    "b4a_clear_campaign_rolling_state called on confirmed close",
        "ledger_state":   "all rolling fields cleared; new campaign starts clean",
        "double_count":   "IMPOSSIBLE after clear",
        "gen2_possible":  "NO",
        "recovery":       "campaign_id mismatch detects stale state if clear failed",
    },
}


# ---- Campaign cleanup -------------------------------------------------------

def b4a_clear_campaign_rolling_state(live: dict, reason: str = "campaign_close") -> dict:
    """Clear all rolling state for a completed campaign.

    Called when primary closes (broker confirmed).
    New campaign must NEVER inherit old rolling fuel.
    Returns record of what was cleared.
    """
    cleared = {}
    for field in [
        "rolling_capacity_slot",
        "rolling_bootstrap_liq_before",
        "rolling_realized_harvest",
        "rolling_replacement_eval",
        "rolling_profit_deployed",
        "rolling_campaign_id",
        "rolling_harvest_id",
        "rolling_primary_stop_risk",
        "rolling_generation_state",
        "rolling_b4a_telemetry",
    ]:
        if field in live:
            cleared[field] = live.pop(field)

    live["rolling_last_campaign_cleared"] = {
        "epoch":          time.time(),
        "reason":         reason,
        "fields_cleared": list(cleared.keys()),
    }
    logger.info("B4A CAMPAIGN CLEANUP: cleared %d rolling fields on %s",
                len(cleared), reason)
    return cleared


def b4a_detect_stale_rolling_state(live: dict, new_primary: dict) -> dict:
    """Detect stale rolling state from a previous campaign.

    Does NOT clear -- logs anomaly, returns details for caller.
    """
    existing_id = live.get("rolling_campaign_id")
    new_id      = b4a_campaign_id(new_primary)
    if existing_id is None:
        return {"stale_detected": False, "reason": "no_prior_campaign_id"}
    if existing_id == new_id:
        return {"stale_detected": False, "reason": "same_campaign_continuing"}
    return {
        "stale_detected":    True,
        "prior_campaign_id": existing_id,
        "new_campaign_id":   new_id,
        "stale_slot":        live.get("rolling_capacity_slot"),
        "stale_realized":    live.get("rolling_realized_harvest") is not None,
        "action_required":   "clear_rolling_state_before_new_campaign",
    }


# ---- Restart/recovery validation (B4A extension) ----------------------------

def b4a_validate_rolling_state_extended(live: dict) -> list:
    """Extended rolling state validation -- Build 4A supplement.

    Returns list of warning strings. Empty = clean.
    Never grants capacity. Never manufactures profit.
    """
    warnings    = []
    harvest     = live.get("rolling_realized_harvest") or {}
    realized    = harvest.get("realized_pnl_estimate")
    deployed    = live.get("rolling_profit_deployed", 0.0)
    campaign_id = live.get("rolling_campaign_id")
    primary     = live.get("open_position")

    if realized is not None and deployed > 0 and deployed > realized:
        warnings.append(
            f"B4A_DOUBLE_SPEND_RISK: deployed={deployed:.6f} > realized={realized:.6f}"
        )

    if campaign_id is not None and primary is not None:
        expected = b4a_campaign_id(primary)
        if campaign_id != expected:
            warnings.append(
                f"B4A_CAMPAIGN_MISMATCH: stored={campaign_id} expected={expected}"
            )

    if deployed > 0:
        warnings.append(
            f"B4A_UNEXPECTED_DEPLOYMENT: deployed={deployed:.6f} "
            f"(Build 4A must never deploy profit)"
        )
    return warnings


# ---- Harvest trigger control flow -------------------------------------------

def b4a_is_rolling_harvest_path(
    leg: dict,
    leg_pnl_pct: float,
    tp_pct: float,
    harvest_threshold_pct: float,
) -> tuple:
    """Route decision: ordinary addon TP vs rolling-harvest evaluation path.

    Called only AFTER ordinary TP outer gate fires.
    Returns (is_rolling_path: bool, reason: str).

    Rolling path in Build 4A: observe + emit telemetry only.
    NEVER executes new harvest -- returns rolling_policy_not_enabled.
    """
    gen = leg.get("leg_generation", 1)
    if gen != 1:
        return False, f"ordinary_addon_tp_gen{gen}"
    if leg_pnl_pct < harvest_threshold_pct:
        return False, "ordinary_addon_tp_below_harvest_threshold"
    return True, "rolling_harvest_path"


def b4a_rolling_path_fail_closed() -> dict:
    """Explicit fail-closed result for Build 4A rolling path.

    Path may observe and emit telemetry. NEVER executes harvest/replacement.
    """
    return {
        "rolling_path_executed":   False,
        "reason":                  "rolling_policy_not_enabled",
        "build4a_status":          "observation_and_telemetry_only",
        "deployment_blocked":      True,
        "gen2_submission_blocked": True,
    }


# ---- Gen-2 submission boundary guard (second invariant) ---------------------

def b4a_gen2_submission_guard(
    candidate_generation: int,
    rolling_max_generations: int,
    caller: str = "unknown",
) -> tuple:
    """Second-layer gen-2 submission guard at broker submission boundary.

    This is the SECOND invariant (first is _ROLLING_MAX_GENERATIONS check).
    Returns (blocked: bool, reason: str).

    Gen-2 MAY: be constructed, have economics calculated, emit telemetry.
    Gen-2 MUST NOT: reach broker submission.
    """
    if candidate_generation > rolling_max_generations:
        return True, (
            f"build4a_observation_only: gen-{candidate_generation} submission blocked; "
            f"max_generations={rolling_max_generations}; caller={caller}"
        )
    return False, "generation_within_allowed_range"
