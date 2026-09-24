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


def campaign_allocation(primary, addons, unit_at_price, tier_budget):
    """Preserve original primary reservation; never reclaim it on a partial TP.

    Rounded primary exposure above its reservation also consumes capacity.
    Legacy addons inherit primary leverage, as the original opening path did.
    Closed addons are absent, so their allocation is available for replacement.
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
        actual = quantity * positive(unit_at_price(positive(leg.get("fill_price"))))
        allocation = actual / positive(leg.get("leverage", lev))
        if index == 0:
            allocation = max(reserved, allocation)
        consumed += allocation
        rows.append(dict(deal_id=identity, quantity=quantity,
                         actual_notional=actual, consumed_allocation=allocation))
    budget = positive(tier_budget)
    return dict(legs=rows, quantity=sum(row["quantity"] for row in rows),
                actual_notional=sum(row["actual_notional"] for row in rows),
                consumed_allocation=consumed, remaining_allocation=max(0., budget - consumed))


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
