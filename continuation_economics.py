"""Protected continuation cost checks. No broker calls or future-profit credit."""
import math
from winner_accounting import positive


def spread_economics(evidence, *, spread, spread_pct, history, min_readings,
                     anomaly_factor, stop_distance, quantity, min_deal):
    """Judge the final F50 candidate; do not resize it or change the F50 fraction.

    The existing spread anomaly factor supplies the widening veto. The absolute
    dollar veto is the existing half-protected-profit budget, including spread.
    Adding spread to the attached-stop reserve is intentionally conservative:
    it reserves immediate liquidation cost as well as the nominal fill-to-stop
    loss. This is not an assertion that spread is paid twice at the stop.
    """
    if evidence["protection_state"] != "profit_protected":
        raise ValueError("continuation_profitable_ack_required")
    protected = positive(evidence["liquidation_before"])
    positive(evidence["current_campaign_pnl"])
    spread, spread_pct = positive(spread), positive(spread_pct)
    multiplier = positive(evidence["multiplier"])
    stop_distance, quantity, min_deal = map(positive, (stop_distance, quantity, min_deal))
    slip, commission = float(evidence["slippage"]), float(evidence["commission"])
    if not all(math.isfinite(x) and x >= 0 for x in (slip, commission)):
        raise ValueError("continuation_cost_evidence_unavailable")
    if quantity < min_deal or not math.isclose(quantity / min_deal, round(quantity / min_deal), abs_tol=1e-8):
        raise ValueError("continuation_MINDEAL_illegal")
    values = [positive(value) for value in history]
    if len(values) < min_readings:
        raise ValueError("continuation_spread_history_unavailable")
    baseline = sum(values) / len(values)
    stop_risk = quantity * multiplier * stop_distance
    spread_cost = quantity * multiplier * spread
    slippage_cost = quantity * multiplier * 2 * slip
    total = stop_risk + spread_cost + slippage_cost + commission
    budget = protected * .5
    reason = ("continuation_absolute_spread_veto" if spread_pct > baseline * positive(anomaly_factor)
              else "continuation_absolute_cost_veto" if total > budget + 1e-9
              else "continuation_economics_pass")
    return dict(allowed=reason == "continuation_economics_pass", reason=reason,
                spread_native=spread, spread_pct=spread_pct, baseline_spread_pct=baseline,
                spread_history_readings=len(values),
                widening_limit_pct=baseline * anomaly_factor, stop_risk=stop_risk,
                spread_cost=spread_cost, slippage_reserve=slippage_cost,
                commission_reserve=commission, incremental_reserve=total,
                protected_profit=protected, f50_budget=budget,
                protected_floor_after=protected-total, quantity=quantity, min_deal=min_deal)


def verify_inventory(legs, response, epics):
    """A fresh inventory must match every campaign leg, including quantities.

    Unknown additional inventory also blocks admission; accepted pending orders
    are blocked separately by the durable existing submission state.
    """
    rows = response.get("positions") if isinstance(response, dict) else None
    if not isinstance(rows, list) or len(rows) != len(legs):
        raise ValueError("continuation_broker_inventory_unresolved")
    seen = set()
    for leg in legs:
        matches = [r for r in rows if isinstance(r, dict)
                   and isinstance(r.get("position"), dict)
                   and r["position"].get("dealId") == leg.get("deal_id")]
        if len(matches) != 1 or not leg.get("deal_id") or leg["deal_id"] in seen:
            raise ValueError("continuation_broker_identity_unresolved")
        seen.add(leg["deal_id"])
        row = matches[0]
        pos = row["position"]
        broker_quantity = positive(pos["size"])
        local_quantity = positive(leg["ig_size"])
        if (pos.get("direction") != ("BUY" if leg["direction"] == "long" else "SELL")
                or (row.get("market") or {}).get("epic") != epics[leg["instrument"]]
                or not math.isclose(broker_quantity, local_quantity, rel_tol=0., abs_tol=1e-8)):
            raise ValueError("continuation_broker_local_disagreement")
        from winner_protection import strongest
        acknowledged = strongest(leg["direction"], leg.get("broker_stop_level"),
                                 leg.get("acknowledged_stop_level"))
        broker_stop = positive(pos.get("stopLevel"))
        sign = 1 if leg["direction"] == "long" else -1
        if acknowledged is None or sign * (broker_stop - acknowledged) < -1e-9:
            raise ValueError("continuation_broker_protection_disagreement")
    return True
