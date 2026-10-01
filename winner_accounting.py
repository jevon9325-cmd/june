"""Pure campaign accounting. Allocation dollars are not broker margin dollars."""
import math


def positive(value):
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("missing/nonpositive/nonfinite campaign metadata")
    return value


def unit_exposure(price, *, kind, lot=None, price_unit=None, fx=None, pip=None):
    """USD sizing basis per broker quantity, inverse of June's sizing formula.

    FX retains June's base-unit allocation convention (not mark-to-market USD).
    Commodity native-price notional must not apply price_unit a second time.
    """
    price = positive(price)
    if kind == "equity":
        return price * positive(price_unit) / positive(fx)
    if kind == "fx":
        return positive(lot) / positive(pip)
    if kind == "commodity":
        return price * positive(lot)
    raise ValueError("unknown instrument kind")


def confirm_allocation_reduction(leg, broker_row, confirmation, reference, before):
    """Return durable evidence only for a matched, accepted residual reduction.

    No local flag, request, or accepted receipt alone releases allocation.
    Repeated confirmations replace the residual certificate, never subtract twice.
    """
    position = broker_row["position"]
    direction = "BUY" if leg["direction"] == "long" else "SELL"
    if (not reference or confirmation.get("dealStatus") != "ACCEPTED"
            or confirmation.get("dealReference", reference) != reference
            or position.get("dealId") != leg.get("deal_id")
            or position.get("direction") != direction):
        raise ValueError("allocation_reduction_identity_unconfirmed")
    before, residual = positive(before), positive(position["size"])
    if residual >= before:
        raise ValueError("allocation_reduction_not_confirmed")
    original = positive(leg.get("original_ig_size", before))
    if original < before:
        raise ValueError("allocation_original_quantity_invalid")
    return dict(deal_id=leg["deal_id"], direction=leg["direction"],
                original_quantity=original, confirmed_quantity=residual,
                original_reservation=positive(leg["pos_size"]),
                confirmation_reference=reference, status="confirmed_residual")


def campaign_allocation(primary, addons, unit_at_price, tier_budget, *, pending=None):
    """Release only certified reductions, retaining larger plausible exposure.

    Rounded primary exposure above its reservation also consumes capacity.
    Legacy addons inherit primary leverage, as the original opening path did.
    Unresolved orders consume all remaining capacity until reconciled. No profit
    or F50 credit is created by releasing an allocation reservation.
    """
    lev = positive(primary.get("leverage"))
    reserved = positive(primary.get("pos_size"))
    rows, seen = [], set()
    consumed = 0.0
    for index, leg in enumerate([primary, *addons]):
        identity = leg.get("deal_id")
        if not identity or identity in seen:
            raise ValueError("missing/duplicate active deal identity")
        seen.add(identity)
        if (leg.get("instrument") != primary.get("instrument") or
                leg.get("direction") != primary.get("direction")):
            raise ValueError("incompatible campaign leg")
        quantity = positive(leg.get("ig_size"))
        quantity = max(quantity, positive(leg.get("allocation_observed_quantity", quantity)))
        unresolved = leg.get("partial_exit_pending") or {}
        if unresolved:
            quantity = max(quantity, positive(unresolved.get("original_size", quantity)))
        actual = quantity * positive(unit_at_price(positive(leg.get("fill_price"))))
        allocation = actual / positive(leg.get("leverage", lev))
        if index == 0:
            reason = "allocation_original_reservation_retained"
            certificate = leg.get("allocation_confirmation") or {}
            if unresolved:
                reason = "allocation_retained_unresolved_partial"
            elif certificate:
                if (certificate.get("status") != "confirmed_residual"
                        or certificate.get("deal_id") != identity
                        or certificate.get("direction") != leg["direction"]
                        or not certificate.get("confirmation_reference")):
                    raise ValueError("invalid allocation certificate")
                original = positive(certificate["original_quantity"])
                confirmed = positive(certificate["confirmed_quantity"])
                if confirmed > original or positive(certificate["original_reservation"]) != reserved:
                    raise ValueError("invalid allocation certificate basis")
                quantity = max(quantity, confirmed)
                actual = quantity * positive(unit_at_price(positive(leg["fill_price"])))
                allocation = actual / positive(leg.get("leverage", lev))
                reserved *= min(1., quantity / original)
                reason = ("allocation_released_confirmed_partial" if quantity < original
                          else "allocation_retained_larger_exposure")
            allocation = max(reserved, allocation)
        else:
            reason = "allocation_active_addon"
        consumed += allocation
        rows.append(dict(deal_id=identity, quantity=quantity,
                         actual_notional=actual, consumed_allocation=allocation, reason=reason))
    budget = positive(tier_budget)
    pending_notional, pending_quantity = 0., 0.
    pending_reserve = max(0., budget - consumed) if pending else 0.
    if pending:
        order = pending.get("order") or {}
        if order.get("size") is not None:
            pending_quantity = positive(order["size"])
            pending_notional = pending_quantity * positive(unit_at_price(positive(primary["fill_price"])))
        for key in ("sized_notional", "intended_notional"):
            if pending.get(key) is not None:
                pending_notional = max(pending_notional, positive(pending[key]))
        pending_reserve = max(pending_reserve, pending_notional / lev)
    consumed += pending_reserve
    return dict(legs=rows, quantity=sum(row["quantity"] for row in rows) + pending_quantity,
                actual_notional=sum(row["actual_notional"] for row in rows) + pending_notional,
                consumed_allocation=consumed, remaining_allocation=max(0., budget - consumed),
                pending_reservation=pending_reserve,
                reason="allocation_retained_pending_exposure" if pending else rows[0]["reason"])


def validate_addon(actual, intended, leverage, remaining, margin_fraction,
                   available, campaign_notional, *, equity=False, oversize_max=3.5):
    """Validate FINAL rounded quantity in allocation, exposure and margin units."""
    actual, intended, leverage = map(positive, (actual, intended, leverage))
    remaining, available = float(remaining), float(available)
    margin_fraction = positive(margin_fraction)
    if not all(math.isfinite(v) for v in (remaining, available, campaign_notional)):
        raise ValueError("nonfinite capacity")
    if actual / leverage > remaining + 1e-9:
        raise ValueError("rounded addon exceeds remaining allocation")
    if equity:
        if actual / (intended / leverage) > leverage + .5:
            raise ValueError("equity leverage gate")
    elif actual > intended * positive(oversize_max) + 1e-9:
        raise ValueError("commodity oversize gate")
    if (campaign_notional + actual) * margin_fraction > available + 1e-9:
        raise ValueError("campaign margin exceeds available equity")
    return actual / leverage
