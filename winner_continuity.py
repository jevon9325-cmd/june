"""Observational readiness + joint-sizing diagnostics (pure logic, no I/O).

OBSERVATION-ONLY. Nothing in this module changes a trading decision, loosens any
protection, resizes any order, or alters any gate. It computes diagnostic verdicts
that june.py attaches to existing best-effort telemetry so the two roadmap
questions can be evaluated from live data WITHOUT taking the unproven behavioral
changes:

  1. UNSPLITTABLE WINNER CONTINUATION — whether a 1xMINDEAL winner reaching TP
     (which today must full-close because a legal partial is impossible) is in a
     state where a broker-protected continuation WOULD be admissible under a safe
     contract. This only records readiness; the existing full-close fallback
     still executes unchanged.

  2. JOINT ADDON SIZING — whether the current F50 "compute max then validate
     against allocation/margin" ordering would ever reject a quantity that a
     smaller broker-legal MINDEAL multiple could have satisfied. This only
     records what a jointly-constrained solver WOULD choose; the live F50 size is
     unchanged.

Forensic baseline (reports 2026-09-30/10-01): automatic protected hold is
PLAUSIBLE BUT UNPROVEN, and F50_OVERSIZED_PROPOSAL_REJECTION had 0 actual events.
This module therefore exists to accumulate the evidence that is currently
missing, not to act on it.

Fail-closed: any missing/ambiguous input yields a not-ready / no-diagnostic
verdict with an explicit reason. Readiness NEVER certifies a software-only floor.
"""

# Readiness verdicts
READY = "ready"
NOT_READY = "not_ready"

# Minimum broker-protected net profit (USD) for continuation to be considered
# economically meaningful. This is a DIAGNOSTIC threshold for observation only;
# it does not gate any live action. Chosen conservatively; recorded on the
# verdict so analysis can re-derive with any threshold.
DEFAULT_MIN_PROTECTED_PROFIT = 0.0


def _f(value):
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v != v or v in (float("inf"), float("-inf")):
        return None
    return v


def unsplittable_readiness(position, *, ig_size, min_deal, acknowledged_stop_level,
                           stop_sync_status, exit_price, multiplier,
                           cost_reserve=0.0, min_protected_profit=DEFAULT_MIN_PROTECTED_PROFIT,
                           manual_review=False, orphan=False,
                           partial_exit_pending=False):
    """Decide whether an unsplittable 1xMINDEAL winner at TP is in a state where a
    broker-protected continuation WOULD be admissible under a safe contract.

    OBSERVATION ONLY. The caller still executes its existing full-close fallback
    regardless of this verdict.

    Fail-closed readiness requires ALL of:
      * quantity is genuinely unsplittable at MINDEAL (a legal partial leaving a
        legal residual is impossible) — otherwise ordinary partial TP applies and
        continuation is not the relevant question;
      * broker protection is ACKNOWLEDGED (stop_sync_status == 'acknowledged' and
        an acknowledged broker stop level exists) — a software/intended floor is
        never sufficient;
      * no manual-review / orphan / pending-partial ambiguity;
      * the acknowledged stop is on the protective side of the fill (locks a
        non-negative gross) and establishes net protected profit, after expected
        costs, at least `min_protected_profit`.

    Returns a verdict dict:
      {verdict, reason, legal_split, protected_gross, protected_net, current_value,
       giveback_budget, acknowledged_stop_level, min_protected_profit}
    protected_net is the broker-protected profit if the acknowledged stop were hit
    now; giveback_budget is current executable value minus protected_net (how much
    of the open gain a continuation would be risking down to the protected floor).
    """
    direction = (position or {}).get("direction")
    fill = _f((position or {}).get("fill_price"))
    q = _f(ig_size)
    md = _f(min_deal)
    ack = _f(acknowledged_stop_level)
    exit_px = _f(exit_price)
    mult = _f(multiplier)

    base = {"verdict": NOT_READY, "reason": None, "legal_split": None,
            "protected_gross": None, "protected_net": None, "current_value": None,
            "giveback_budget": None, "acknowledged_stop_level": ack,
            "min_protected_profit": min_protected_profit}

    if direction not in ("long", "short") or fill is None or q is None or md is None or md <= 0:
        base["reason"] = "insufficient_position_metadata"
        return base

    # Is the position genuinely unsplittable? A legal partial needs BOTH the close
    # leg and the residual to clear MINDEAL. (Mirror of june's half-size test.)
    half = round(q / 2.0, 4)
    legal_split = (half >= md and round(q - half, 4) >= md)
    base["legal_split"] = legal_split
    if legal_split:
        # Splittable: ordinary partial TP is the applicable path, not continuation.
        base["reason"] = "splittable_not_applicable"
        return base

    if manual_review or orphan or partial_exit_pending:
        base["reason"] = "campaign_state_ambiguous"
        return base

    if stop_sync_status != "acknowledged" or ack is None:
        base["reason"] = "protection_not_broker_acknowledged"
        return base

    sign = 1.0 if direction == "long" else -1.0
    # Acknowledged stop must lock a non-negative gross (protective side of fill).
    protected_gross = sign * (ack - fill) * q * (mult if mult is not None else 1.0)
    base["protected_gross"] = round(protected_gross, 6)
    if protected_gross < 0:
        base["reason"] = "acknowledged_stop_not_protective"
        return base

    cost = _f(cost_reserve) or 0.0
    protected_net = protected_gross - cost
    base["protected_net"] = round(protected_net, 6)
    if protected_net < min_protected_profit:
        base["reason"] = "protected_profit_below_minimum"
        return base

    if exit_px is not None and mult is not None:
        current_value = sign * (exit_px - fill) * q * mult
        base["current_value"] = round(current_value, 6)
        base["giveback_budget"] = round(current_value - protected_net, 6)

    base["verdict"] = READY
    base["reason"] = "broker_protected_unsplittable_winner"
    return base


def legal_quantity_floor(value, min_deal):
    """Largest MINDEAL multiple <= value (0.0 if below one MINDEAL)."""
    v, md = _f(value), _f(min_deal)
    if v is None or md is None or md <= 0 or v < md:
        return 0.0
    import math
    return math.floor(v / md + 1e-9) * md


def joint_sizing_diagnostic(*, q_f50, min_deal, mid, leverage, remaining_allocation,
                            margin_fraction, equity, residual_notional,
                            per_lot_cost, expendable, commission=0.0):
    """Diagnostic ONLY: what quantity a jointly-constrained solver would admit,
    and which constraint binds. Does NOT change the live F50 size.

    q_legal = largest MINDEAL multiple q satisfying ALL of:
      1. q <= q_f50                                  (never exceed F50 maximum)
      2. q*mid/leverage <= remaining_allocation      (campaign allocation)
      3. (residual_notional + q*mid)*margin_fraction <= allowed  [see note]
      4. q*per_lot_cost + commission <= expendable   (F50 expendable profit)
      5. q >= min_deal

    For margin (3) we report the per-lot allocation/margin ceilings so analysis
    can see whether allocation or F50 funding is the binding constraint. We do not
    re-derive the account equity ceiling here (june's validate_addon owns that);
    instead we expose q_margin as the allocation-equivalent and flag when
    allocation is the binding constraint vs F50.

    Returns {q_f50, q_allocation, q_cost, q_joint, binding_constraint,
             joint_lt_f50} — all MINDEAL-quantized. joint_lt_f50 True means a
    smaller legal quantity than the F50 maximum satisfies every constraint, i.e.
    the max-first ordering could matter.
    """
    qf = _f(q_f50) or 0.0
    md = _f(min_deal)
    m = _f(mid)
    lev = _f(leverage)
    ra = _f(remaining_allocation)
    plc = _f(per_lot_cost)
    exp = _f(expendable)
    comm = _f(commission) or 0.0

    out = {"q_f50": qf, "q_allocation": None, "q_cost": None, "q_joint": None,
           "binding_constraint": None, "joint_lt_f50": None}
    if md is None or md <= 0 or m is None or m <= 0 or lev is None or lev <= 0:
        out["binding_constraint"] = "insufficient_metadata"
        return out

    # Allocation ceiling: q*mid/lev <= remaining_allocation
    q_alloc = legal_quantity_floor((ra * lev / m) if ra is not None else 0.0, md)
    out["q_allocation"] = q_alloc
    # F50 expendable-cost ceiling: q*per_lot_cost + commission <= expendable
    if plc is not None and plc > 0 and exp is not None:
        q_cost = legal_quantity_floor((exp - comm) / plc, md)
    else:
        q_cost = 0.0
    out["q_cost"] = q_cost

    # Joint = min of all ceilings, MINDEAL-quantized.
    q_joint = legal_quantity_floor(min(qf, q_alloc, q_cost), md)
    out["q_joint"] = q_joint

    # Which constraint binds the joint result?
    ceilings = {"f50_max": qf, "allocation": q_alloc, "f50_cost": q_cost}
    binding = min(ceilings, key=lambda k: ceilings[k])
    out["binding_constraint"] = binding if q_joint > 0 else "mindeal_blocked"
    out["joint_lt_f50"] = (q_joint < qf) if qf > 0 else False
    return out
