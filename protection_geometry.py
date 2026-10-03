"""Pure amendment geometry in the SAME native price units as IG levels.

No order, price target, economic parameter or acknowledgement is created here.
Broker points convert using pip / price_unit, the inverse of order stop sizing.
"""
import math
from winner_protection import normalized_target


def positive(value):
    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("invalid protection geometry")
    return value


def _geometry(position, proposed, *, mid, pip, price_unit, min_points,
                       spread_native=0., min_fraction=0., include_spread=True):
    mid, pip, price_unit = map(positive, (mid, pip, price_unit))
    minimum = positive(min_points)
    spread, fraction = float(spread_native), float(min_fraction or 0.)
    if not all(math.isfinite(v) and v >= 0 for v in (spread, fraction)):
        raise ValueError("invalid spread/minimum fraction")
    target = normalized_target(position, proposed)
    if target is None:
        raise ValueError("missing normalized target")
    point = pip / price_unit
    # Match _live_compute_stop_pts(..., stop_pct=0) exactly, including its
    # existing one-broker-point buffer and dynamic percentage minimum.
    buffered_points = max(int(minimum) + 1, int(mid * fraction / point) + 1)
    broker_minimum_native = max(minimum * point, mid * fraction)
    buffered_native = buffered_points * point
    # Keep existing economic spread buffer (one pip), in native units.
    required = max(buffered_native, spread + pip) if include_spread else buffered_native
    sign = 1 if position["direction"] == "long" else -1
    distance = sign * (mid - target)
    return dict(proposed_stop=proposed, normalized_requested_stop=target,
                mid_native=mid, pip=pip, price_unit=price_unit,
                native_price_per_broker_point=point, minimum_broker_points=minimum,
                minimum_fraction=fraction, broker_minimum_native=broker_minimum_native,
                buffered_minimum_native=buffered_native, spread_native=spread,
                required_distance_native=required, signed_distance_native=distance,
                can_send=distance >= required,
                protection_status="REQUEST_ELIGIBLE" if distance >= required else "LOCALLY_TOO_CLOSE",
                geometry_basis="cached_IG_rule_and_retained_mid_not_broker_acceptance")


def amendment_geometry(position, proposed, **values):
    try:
        return _geometry(position, proposed, **values)
    except (ValueError, TypeError, KeyError, OverflowError, ZeroDivisionError):
        return dict(can_send=False, proposed_stop=proposed, protection_status="UNKNOWN_GEOMETRY",
                    reason="invalid_or_missing_geometry", geometry_basis="unavailable")


def status(position, *, locally_blocked=False, geometry=None):
    if locally_blocked:
        return (geometry or {}).get("protection_status", "LOCALLY_TOO_CLOSE")
    record = position.get("stop_sync") or {}
    if record.get("status") == "acknowledged":
        return "BROKER_SNAPSHOT_CONFIRMED" if record.get("acknowledged_via") == "broker_position_snapshot" else "BROKER_ACKNOWLEDGED"
    if record.get("status") == "rejected":
        return "BROKER_REJECTED"
    if record.get("status") == "pending":
        return "BROKER_REQUEST_PENDING" if record.get("deal_ref") else "UNKNOWN_OUTCOME"
    return "NOT_REQUESTED"
