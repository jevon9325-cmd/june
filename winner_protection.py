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


def reply_targets_deal(reply, deal_id):
    """True iff an IG confirm reply authoritatively concerns `deal_id`.

    IG returns a confirmation whose top-level `dealId` is the AMENDMENT's own
    deal id, not the amended position's id; the amended position appears in
    `affectedDeals[].dealId`. A stop amendment therefore matches when the
    position id is the top-level dealId OR appears among affectedDeals. This is
    identity evidence only — it does not by itself certify ACCEPTED or the stop
    level; callers still check dealStatus and the normalized stop.
    """
    if not isinstance(reply, dict) or not deal_id:
        return False
    if reply.get("dealId") == deal_id:
        return True
    for affected in (reply.get("affectedDeals") or []):
        if isinstance(affected, dict) and affected.get("dealId") == deal_id:
            return True
    return False


def covers(direction, candidate, target):
    """True iff `candidate` stop is at least as protective as `target`
    (equal within normalization tolerance or strictly stronger)."""
    c, t = level(candidate), level(target)
    if c is None or t is None:
        return False
    if abs(c - t) < 1e-6:
        return True
    return strongest(direction, c, t) == c


def reconcile_broker_stop(position, broker_stop_level, broker_deal_id, *, now, log=None):
    """Acknowledge a pending stop request from an authoritative broker POSITION
    snapshot when the broker already carries the requested-or-stronger stop on
    the SAME open deal. Fail-closed broker truth: this consumes evidence the
    broker itself reports, never a local/software intention.

    Returns True iff it promoted the pending request to acknowledged this call.

    Rules (authority contract B/C/D):
      * The snapshot must be for this position's deal_id.
      * There must be a pending/software request (stop_sync) to reconcile.
      * The broker stop must COVER the pending target (requested-or-stronger).
      * The acknowledged floor is set to the strongest of the existing
        acknowledged floor and the broker-reported stop — never the newer
        software/intended floor. If software protection has since advanced
        beyond the broker stop, only the broker-supported portion is
        acknowledged (the stronger software floor stays merely intended).
    """
    direction = position.get("direction")
    if direction not in ("long", "short"):
        return False
    deal_id = position.get("deal_id")
    if not deal_id or broker_deal_id != deal_id:
        return False
    broker = level(broker_stop_level)
    if broker is None:
        return False
    pending = position.get("stop_sync") or {}
    target = level(pending.get("target")) or level(position.get("intended_stop_level"))
    if target is None:
        return False
    # The broker must actually hold the requested-or-stronger protection.
    if not covers(direction, broker, target):
        return False
    # Acknowledged floor = strongest broker-supported evidence (existing ack or
    # the broker-reported stop). Never certify the software/intended floor.
    new_ack = strongest(direction, position.get("acknowledged_stop_level"),
                        position.get("broker_stop_level"), broker)
    position["acknowledged_stop_level"] = new_ack
    position["broker_stop_level"] = new_ack
    record = dict(pending)
    record["status"] = "acknowledged"
    record["deal_id"] = deal_id
    record["acknowledged_via"] = "broker_position_snapshot"
    record["acknowledged_stop_level"] = new_ack
    record["acknowledged_at"] = now
    position["stop_sync"] = record
    if log is not None:
        log(f"Stop sync acknowledged via broker position snapshot: "
            f"deal={deal_id} broker_stop={new_ack}")
    return True


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
        # Identity: the confirm reply must echo our deal reference AND concern
        # our position deal. IG returns the amendment's own dealId at top level
        # and the amended position in affectedDeals[].dealId, so accept either.
        matching = (isinstance(reply, dict) and reply.get("dealReference") == ref
                    and reply_targets_deal(reply, record["deal_id"]))
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
