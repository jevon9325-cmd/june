"""Auditable defensive scaling economics; no broker or storage operations.

Dollar estimates reserve the existing MPD slippage buffer and known commission.
No realized-profit credit. Unknown campaign history cannot finance new exposure.
"""
import math
from winner_accounting import positive
from winner_protection import effective, strongest


def finite(value):
    value = float(value)
    if not math.isfinite(value):
        raise ValueError("nonfinite economic input")
    return value


def snapshot(legs, *, aggregate, exit_price, multiplier, slippage, commission):
    multiplier, exit_price = positive(multiplier), positive(exit_price)
    slippage, commission = finite(slippage), finite(commission)
    if slippage < 0 or commission < 0:
        raise ValueError("negative cost reserve")
    direction = legs[0]["direction"]
    sign = 1 if direction == "long" else -1
    seen, rows = set(), []
    current, before, quantity = 0., 0., 0.
    for leg in legs:
        identity = leg.get("deal_id")
        if not identity or identity in seen or leg["direction"] != direction or leg["instrument"] != legs[0]["instrument"]:
            raise ValueError("ambiguous campaign identity")
        seen.add(identity)
        fill, qty = positive(leg["fill_price"]), positive(leg["ig_size"])
        ack = strongest(direction, leg.get("broker_stop_level"), leg.get("acknowledged_stop_level"))
        intended = effective(leg, aggregate)
        sync = leg.get("stop_sync") or {}
        if ack is None:
            raise ValueError("missing acknowledged protection")
        if sync and (sync.get("status") != "acknowledged" or sync.get("deal_id") != identity):
            raise ValueError("protection synchronization unresolved")
        if intended is None or sign * (ack - intended) < -1e-9:
            raise ValueError("intended floor exceeds acknowledged protection")
        if sign * (exit_price - ack) <= 0:
            raise ValueError("acknowledged stop already breached")
        # Charge the whole round trip; never spend unconfirmed realized profits.
        if leg.get("partial_exit_done") and "partial_dollar_pnl" not in leg:
            raise ValueError("partial_exit_economics_unavailable")
        realized_debit = min(0., finite(leg.get("partial_dollar_pnl", 0.)))
        reserve = qty * multiplier * slippage + commission * (2 if leg.get("partial_exit_done") else 1)
        pnl = sign * (ack - fill) * qty * multiplier - reserve + realized_debit
        before += pnl
        current += sign * (exit_price - fill) * qty * multiplier - reserve + realized_debit
        quantity += qty
        rows.append(dict(deal_id=identity, quantity=qty, fill=fill, intended=intended,
                         acknowledged=ack, cost_reserve=reserve, realized_debit=realized_debit,
                         liquidation_pnl=pnl))
    classification = "profit_protected" if before > 1e-9 else "breakeven_protected" if before >= -1e-9 else "unprotected"
    return dict(protection_state=classification, liquidation_before=before,
                current_campaign_pnl=current, quantity=quantity, protection=rows,
                direction=direction, multiplier=multiplier, slippage=slippage,
                commission=commission, economics_basis="open_legs_cost_reserved_no_realized_credit")


def plan(evidence, *, entry_price, exit_price, stop_distance, min_distance, quantity):
    """Finance the initial addon stop BEFORE POST by tightening existing stops.

    Preserve the ORIGINAL floor even during post-fill synchronization. Reject
    an infeasible plan instead of assuming a later stop amendment will succeed.
    """
    if evidence["protection_state"] == "unprotected":
        raise ValueError("defensive_unprotected_campaign")
    if evidence["current_campaign_pnl"] <= 0:
        raise ValueError("campaign_not_a_current_winner")
    sign = 1 if evidence["direction"] == "long" else -1
    entry_price, exit_price = positive(entry_price), positive(exit_price)
    stop_distance, min_distance, quantity = map(positive, (stop_distance, min_distance, quantity))
    multiplier, slip = evidence["multiplier"], evidence["slippage"]
    # Initial stop is attached by distance to the fill. Include entry AND exit
    # slippage reserves, rather than assuming the planned fill is guaranteed.
    addon_loss = quantity * multiplier * (stop_distance + 2 * slip) + evidence["commission"]
    increase = addon_loss / (evidence["quantity"] * multiplier)
    targets = [row["acknowledged"] + sign * increase for row in evidence["protection"]]
    if any(sign * (exit_price - target) < min_distance for target in targets):
        raise ValueError("addon_cannot_preserve_floor_with_feasible_broker_stops")
    return dict(prefinance_targets=targets, addon_initial_loss_reserve=addon_loss,
                liquidation_after=evidence["liquidation_before"],
                addon_initial_stop=entry_price - sign * stop_distance,
                addon_entry_estimate=entry_price, proposed_quantity=quantity)


def confirm_funding(original, funded, plan):
    after = funded["liquidation_before"] - plan["addon_initial_loss_reserve"]
    if after < max(0., original["liquidation_before"]) - 1e-9:
        raise ValueError("acknowledged_stops_do_not_finance_addon_floor")
    return after


def f50_capacity(evidence, stop_distance_native, min_deal, floor_fraction=0.50):
    """Maximum MINDEAL-quantized addon ig satisfying the F50 nominal-stop invariant.

    F50: estimated campaign liquidation at acknowledged stops after opening the addon
    must be >= floor_fraction * protected_before.

    stop_distance_native: addon stop distance in native price (pts x native_point).
    Returns (ig_quantized, reason_string). ig_quantized=0.0 means no capacity.

    Callers must confirm protection_state=="profit_protected" before calling.
    Returns 0.0 naturally for non-positive protected_before.
    """
    protected_before = evidence["liquidation_before"]
    if protected_before <= 1e-9:
        return 0.0, "protected_profit_nonpositive"
    required_floor = floor_fraction * protected_before
    expendable = protected_before - required_floor          # = 0.5 x protected_before
    multiplier = float(evidence["multiplier"])
    slippage   = float(evidence["slippage"])
    commission = float(evidence["commission"])
    per_lot_cost = multiplier * (float(stop_distance_native) + 2.0 * max(0.0, slippage))
    if per_lot_cost <= 0:
        return 0.0, "per_lot_cost_nonpositive"
    net_expendable = expendable - commission
    if net_expendable <= 0:
        return 0.0, "expendable_consumed_by_commission"
    ig_raw = net_expendable / per_lot_cost
    min_deal_f = float(min_deal)
    if min_deal_f <= 0:
        return 0.0, "invalid_min_deal"
    ig_quantized = math.floor(ig_raw / min_deal_f) * min_deal_f
    if ig_quantized <= 0:
        return 0.0, "mindeal_blocked"
    return ig_quantized, "ok"

