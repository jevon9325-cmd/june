"""Monotonic software protection and explicitly acknowledged broker stops."""
import math


def level(value):
    try:
        value = float(value)
        return value if math.isfinite(value) and value > 0 else None
    except (ValueError, TypeError):
        return None


def strongest(direction, *values):
    values = [v for v in map(level, values) if v is not None]
    if direction not in ("long", "short"):
        raise ValueError("unknown protection direction")
    return (max(values) if direction == "long" else min(values)) if values else None


def effective(position, aggregate=None):
    direction = position["direction"]
    fill = level(position.get("fill_price"))
    dple = position.get("dple_effective_sl")
    dple_price = fill * (1 + dple if direction == "long" else 1 - dple) if fill and dple is not None else None
    pct = position.get("stop_pct")
    initial = fill * (1 - pct if direction == "long" else 1 + pct) if fill and pct is not None else None
    return strongest(direction, aggregate, initial, dple_price,
                     position.get("broker_stop_level"), position.get("acknowledged_stop_level"),
                     position.get("defensive_stop_level"), position.get("defensive_soft_sl"),
                     position.get("intended_stop_level"))


def campaign_stop(old_legs, new_leg, proposed, aggregate=None):
    """Preserve old estimated liquidation P&L without selecting a profit target.

    Same instrument/direction: quantity-weighted price points have a common
    positive dollar multiplier, which cancels. Existing realized P&L cancels too.
    Costs/slippage are unknown; this is a planned gross floor, not a guarantee.
    """
    direction = new_leg["direction"]
    signed = 1 if direction == "long" else -1
    old_pnl = 0.
    floors = []
    for leg in old_legs:
        stop = effective(leg, aggregate)
        if stop is None:
            raise ValueError("cannot certify existing campaign protection")
        floors.append(stop)
        old_pnl += signed * (stop - leg["fill_price"]) * leg["ig_size"]
    legs = [*old_legs, new_leg]
    total = sum(leg["ig_size"] for leg in legs)
    preserve = (sum(leg["fill_price"] * leg["ig_size"] for leg in legs) + signed * old_pnl) / total
    return strongest(direction, proposed, preserve, effective(new_leg), *floors)


def protect(position, proposed, *, put, confirm, save, now, log, can_send=True):
    """Keep software floor even on rejection, timeout, stale or unknown reply.

    HTTP success/dealReference alone is NOT a stop acknowledgement. Only a
    matching accepted confirmation echoing deal and stop can update broker fields.
    Legacy defensive_stop_level is a software floor; it is never promoted to ack.
    """
    direction = position["direction"]
    target = strongest(direction, proposed, effective(position))
    if target is None:
        return False
    # Round towards protection, never away from it.
    target = (math.ceil(target * 100000) if direction == "long" else math.floor(target * 100000)) / 100000
    position["intended_stop_level"] = target
    position["defensive_soft_sl"] = target
    position["defensive_stop_level"] = target
    position["defensive_stop_active"] = True
    ack = strongest(direction, position.get("acknowledged_stop_level"), position.get("broker_stop_level"))
    if ack is not None and strongest(direction, ack, target) == ack:
        return True
    pending = position.get("stop_sync") or {}
    if pending.get("attempted_at") == now:
        return False
    record = dict(target=target, deal_id=position.get("deal_id"), status="software_only", attempted_at=now)
    position["stop_sync"] = record
    try:
        if not can_send or not record["deal_id"]:
            save()
            return False
        record["status"] = "pending"
        save()
        # Resume a pending matching reference after restart; otherwise a PUT is
        # an idempotent request for this same or stronger absolute stop.
        ref = pending.get("deal_ref") if pending.get("target") == target and pending.get("status") == "pending" else None
        if not ref:
            response = put(f"/positions/otc/{record['deal_id']}",
                           {"stopLevel": target, "guaranteedStop": False}, version="2")
            ref = response.get("dealReference") if isinstance(response, dict) else None
        record["deal_ref"] = ref
        save()
        reply = confirm(ref) if ref else None
        matching = (isinstance(reply, dict) and reply.get("dealReference") == ref
                    and reply.get("dealId") == record["deal_id"])
        if matching and reply.get("dealStatus") == "REJECTED":
            record["status"] = "rejected"
        elif (matching and reply.get("dealStatus") == "ACCEPTED"
              and level(reply.get("stopLevel")) is not None
              and abs(float(reply["stopLevel"]) - target) < .000001
              and position.get("intended_stop_level") == target):
            position["acknowledged_stop_level"] = target
            position["broker_stop_level"] = target
            record["status"] = "acknowledged"
        if record["status"] != "acknowledged":
            log(f"Stop sync {record['status']}: deal={record['deal_id']} software={target}")
        save()
    except Exception as exc:
        record["status"] = "pending"
        log(f"Stop sync uncertain; software protection retained: {exc}")
        try:
            save()
        except Exception:
            pass
    return record["status"] == "acknowledged"
