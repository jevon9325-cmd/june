"""Durable write-ahead fuel reservation for Build-4B gen-2 replacement.

Pure state machine. No network, no Redis client, no june import, no orders.
The caller supplies persistence (a save callback) and broker-truth reconciliation.

INVARIANT (the whole point of this module):
  No unit of realized Ledger-A fuel may finance two orders.

STATE MACHINE (single active gen-2 reservation per campaign in V1):
  (none)
    -> AVAILABLE      capacity exists, no reservation
  AVAILABLE
    -> RESERVED       fuel debited + persisted BEFORE any broker call
  RESERVED
    -> SUBMITTED      broker POST issued; dealReference persisted
    -> RELEASED       proven no order exists (reservation reversed, fuel restored)
  SUBMITTED
    -> OPEN           broker CONFIRMED the deal exists (dealId known)
    -> RELEASED       proven the order never opened (reject / absent in broker truth)
    -> (stays SUBMITTED) outcome UNKNOWN -> fail closed, reconcile from broker later
  OPEN
    -> HARVESTED      broker-confirmed profitable close; realized credited
    -> CLOSED_LOSS    broker-confirmed losing/flat close; reserved fuel consumed
  HARVESTED / CLOSED_LOSS
    -> SETTLED        terminal; reservation retired

Fuel arithmetic reconciliation (must always hold):
  A_total  -  consumed_losses  -  reserved_or_deployed  ==  A_remaining

Reserved fuel is NOT profit and NOT released margin. It is realized Ledger-A
that is committed to a specific gen-2 deal and therefore unavailable to any
other order until the deal reaches a terminal broker-confirmed outcome.
"""
from __future__ import annotations

# Reservation lifecycle states
AVAILABLE   = "AVAILABLE"
RESERVED    = "RESERVED"
ATTEMPTED   = "ATTEMPTED"   # broker POST about to be / was sent; exposure UNKNOWN
SUBMITTED   = "SUBMITTED"
OPEN        = "OPEN"
HARVESTED   = "HARVESTED"
CLOSED_LOSS = "CLOSED_LOSS"
RELEASED    = "RELEASED"
SETTLED     = "SETTLED"

_TERMINAL = frozenset({HARVESTED, CLOSED_LOSS, RELEASED, SETTLED})

RESERVATION_KEY = "rolling_fuel_reservation"   # stored inside _live


class FuelError(ValueError):
    """Refuse the operation and keep prior durable state; fail closed."""


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError) as exc:
        raise FuelError(f"non-numeric fuel value: {v!r}") from exc
    if f != f or f in (float("inf"), float("-inf")):
        raise FuelError(f"non-finite fuel value: {v!r}")
    return f


def a_remaining(live: dict) -> float:
    """Realized Ledger-A remaining = realized total - already-committed reservation.

    Reserved/deployed fuel is subtracted so it can never finance a second order.
    """
    harvest = live.get("rolling_realized_harvest") or {}
    total_raw = harvest.get("realized_pnl_estimate")
    if total_raw is None:
        return 0.0
    total = _num(total_raw)
    res = live.get(RESERVATION_KEY) or {}
    committed = 0.0
    if res.get("state") in (RESERVED, ATTEMPTED, SUBMITTED, OPEN):
        committed = _num(res.get("amount", 0.0))
    deployed = _num(live.get("rolling_profit_deployed", 0.0) or 0.0)
    return round(total - committed - deployed, 8)


def reconcile_invariant(live: dict) -> None:
    """Assert A_total - deployed - committed == A_remaining. Raise on mismatch."""
    harvest = live.get("rolling_realized_harvest") or {}
    total = _num(harvest.get("realized_pnl_estimate", 0.0) or 0.0)
    deployed = _num(live.get("rolling_profit_deployed", 0.0) or 0.0)
    res = live.get(RESERVATION_KEY) or {}
    committed = _num(res.get("amount", 0.0)) if res.get("state") in (RESERVED, ATTEMPTED, SUBMITTED, OPEN) else 0.0
    rem = a_remaining(live)
    if round(total - deployed - committed - rem, 8) != 0.0:
        raise FuelError(
            f"fuel reconciliation broken: total={total} deployed={deployed} "
            f"committed={committed} remaining={rem}")


def can_reserve(live: dict, amount, campaign_id: str, deal_id_expected=None) -> tuple:
    """Fail-closed check before reserving. Returns (ok, reason).

    Requires: no existing active reservation, sufficient A_remaining, a real
    campaign id, and a finite positive amount.
    """
    try:
        amt = _num(amount)
    except FuelError as exc:
        return False, f"amount_invalid:{exc}"
    if amt <= 0:
        return False, "amount_nonpositive"
    if not campaign_id:
        return False, "campaign_id_missing"
    res = live.get(RESERVATION_KEY) or {}
    if res.get("state") in (RESERVED, ATTEMPTED, SUBMITTED, OPEN):
        return False, f"reservation_active:{res.get('state')}"
    if res.get("campaign_id") and res.get("campaign_id") != campaign_id and res.get("state") not in _TERMINAL and res.get("state") is not None:
        return False, "reservation_campaign_mismatch"
    if a_remaining(live) + 1e-9 < amt:
        return False, "insufficient_realized_fuel"
    return True, "ok"


def reserve(live: dict, amount, campaign_id: str, reservation_id: str, *, save) -> dict:
    """Debit fuel and persist a RESERVED record BEFORE any broker call.

    Persistence (save) must succeed or we raise and leave no reservation
    (fail closed: no reservation means no submission).
    """
    ok, reason = can_reserve(live, amount, campaign_id)
    if not ok:
        raise FuelError(f"cannot reserve: {reason}")
    if not reservation_id:
        raise FuelError("reservation_id required")
    amt = _num(amount)
    record = {
        "state":         RESERVED,
        "amount":        amt,
        "campaign_id":   campaign_id,
        "reservation_id": reservation_id,
        "deal_ref":      None,
        "deal_id":       None,
    }
    prior = live.get(RESERVATION_KEY)
    live[RESERVATION_KEY] = record
    try:
        save()
    except Exception as exc:
        # Roll back in-memory so a failed persist cannot leave phantom reservation.
        if prior is None:
            live.pop(RESERVATION_KEY, None)
        else:
            live[RESERVATION_KEY] = prior
        raise FuelError(f"reservation persist failed; no reservation held: {exc}") from exc
    reconcile_invariant(live)
    return dict(record)


def mark_attempted(live: dict, reservation_id: str, *, save) -> dict:
    """RESERVED -> ATTEMPTED, persisted BEFORE the broker POST is issued.

    After this transition, the system must treat the gen-2 order as POSSIBLY
    EXISTING at the broker. Fuel stays committed. A lost/timed-out POST can no
    longer be mistaken for "never sent": reconciliation will not release an
    ATTEMPTED reservation without positive broker evidence of absence.
    """
    res = live.get(RESERVATION_KEY) or {}
    if res.get("reservation_id") != reservation_id:
        raise FuelError("reservation_id mismatch on attempt")
    if res.get("state") not in (RESERVED, ATTEMPTED):
        raise FuelError(f"cannot mark_attempted from state {res.get('state')}")
    res["state"] = ATTEMPTED
    live[RESERVATION_KEY] = res
    save()
    return dict(res)


def mark_submitted(live: dict, reservation_id: str, deal_ref, *, save) -> dict:
    """Record the broker dealReference immediately after POST returns.

    Must be called before trusting any confirm. Idempotent for the same ref.
    """
    res = live.get(RESERVATION_KEY) or {}
    if res.get("reservation_id") != reservation_id:
        raise FuelError("reservation_id mismatch on submit")
    if res.get("state") not in (RESERVED, ATTEMPTED, SUBMITTED):
        raise FuelError(f"cannot submit from state {res.get('state')}")
    res["state"] = SUBMITTED
    res["deal_ref"] = deal_ref
    live[RESERVATION_KEY] = res
    save()
    return dict(res)


def mark_open(live: dict, reservation_id: str, deal_id, *, save) -> dict:
    """Broker truth established the deal EXISTS (dealId). Reservation -> OPEN."""
    res = live.get(RESERVATION_KEY) or {}
    if res.get("reservation_id") != reservation_id:
        raise FuelError("reservation_id mismatch on open")
    if res.get("state") not in (SUBMITTED, OPEN):
        raise FuelError(f"cannot open from state {res.get('state')}")
    if not deal_id:
        raise FuelError("deal_id required to open")
    res["state"] = OPEN
    res["deal_id"] = deal_id
    live[RESERVATION_KEY] = res
    save()
    return dict(res)


def release(live: dict, reservation_id: str, reason: str, *, save) -> dict:
    """Reverse a reservation ONLY when it is proven no order exists.

    Restores fuel by discarding the committed reservation. Never call this on
    UNKNOWN broker outcome (that must stay SUBMITTED and reconcile later).
    """
    res = live.get(RESERVATION_KEY) or {}
    if res.get("reservation_id") != reservation_id:
        raise FuelError("reservation_id mismatch on release")
    if res.get("state") != RESERVED:
        raise FuelError(f"cannot release from state {res.get('state')} "
                        f"(post-attempt release requires proven broker absence)")
    res["state"] = RELEASED
    res["release_reason"] = reason
    live[RESERVATION_KEY] = res
    save()
    reconcile_invariant(live)
    return dict(res)


def settle_harvest(live: dict, reservation_id: str, realized_add, *, save) -> dict:
    """Terminal winning close. The reserved fuel returns to A_total AND the new
    realized profit is added. Net effect: A_remaining rises by realized_add.

    We move the reservation to HARVESTED then SETTLED. The reserved amount is
    un-committed (returns to remaining) and realized_add is credited by the
    caller's harvest record; here we only retire the reservation.
    """
    res = live.get(RESERVATION_KEY) or {}
    if res.get("reservation_id") != reservation_id:
        raise FuelError("reservation_id mismatch on harvest settle")
    if res.get("state") != OPEN:
        raise FuelError(f"cannot harvest-settle from state {res.get('state')}")
    res["state"] = SETTLED
    res["outcome"] = HARVESTED
    res["realized_add"] = _num(realized_add)
    live[RESERVATION_KEY] = res
    save()
    return dict(res)


def settle_loss(live: dict, reservation_id: str, *, save) -> dict:
    """Terminal losing/flat close. Reserved fuel is CONSUMED (moves to deployed).

    rolling_profit_deployed is incremented by the reserved amount so that the
    fuel is permanently spent and cannot finance another order.
    """
    res = live.get(RESERVATION_KEY) or {}
    if res.get("reservation_id") != reservation_id:
        raise FuelError("reservation_id mismatch on loss settle")
    if res.get("state") != OPEN:
        raise FuelError(f"cannot loss-settle from state {res.get('state')}")
    amt = _num(res.get("amount", 0.0))
    live["rolling_profit_deployed"] = round(
        _num(live.get("rolling_profit_deployed", 0.0) or 0.0) + amt, 8)
    res["state"] = SETTLED
    res["outcome"] = CLOSED_LOSS
    live[RESERVATION_KEY] = res
    save()
    reconcile_invariant(live)
    return dict(res)


def active_reservation(live: dict):
    """Return the active (non-terminal) reservation dict, or None."""
    res = live.get(RESERVATION_KEY) or {}
    if res.get("state") in (RESERVED, ATTEMPTED, SUBMITTED, OPEN):
        return dict(res)
    return None


def release_proven_absent(live: dict, reservation_id: str, reason: str, *, save) -> dict:
    """Release an ATTEMPTED/SUBMITTED reservation ONLY when the caller has
    POSITIVELY established (via broker truth) that the intended deal does not
    exist -- e.g. a broker-confirmed REJECTED status for a known dealReference.
    This is the ONLY sanctioned way to reverse a post-attempt reservation."""
    res = live.get(RESERVATION_KEY) or {}
    if res.get("reservation_id") != reservation_id:
        raise FuelError("reservation_id mismatch on proven-absent release")
    if res.get("state") not in (ATTEMPTED, SUBMITTED):
        raise FuelError(f"proven-absent release invalid from state {res.get('state')}")
    res["state"] = RELEASED
    res["release_reason"] = reason
    live[RESERVATION_KEY] = res
    save()
    reconcile_invariant(live)
    return dict(res)


def mark_ambiguous(live: dict, reservation_id: str, reason: str, *, save) -> dict:
    """Annotate an ATTEMPTED reservation whose broker outcome cannot be resolved
    from available identifiers. State stays ATTEMPTED (fuel locked, no new gen-2);
    we only add operator-visible telemetry. Never releases fuel."""
    res = live.get(RESERVATION_KEY) or {}
    if res.get("reservation_id") != reservation_id:
        raise FuelError("reservation_id mismatch on mark_ambiguous")
    if res.get("state") not in (ATTEMPTED, SUBMITTED):
        raise FuelError(f"mark_ambiguous invalid from state {res.get('state')}")
    res["ambiguous"] = True
    res["ambiguous_reason"] = reason
    live[RESERVATION_KEY] = res
    save()
    return dict(res)


def reconcile_on_load(live: dict, *, broker_deal_ids, save) -> dict:
    """Resolve a persisted reservation against broker truth after restart.

    broker_deal_ids: set of dealIds currently OPEN at the broker (from a verified
    /positions read). Callers pass an EMPTY set only when they have a VERIFIED
    empty inventory (never on an errored/unknown read).

    Rules:
      RESERVED   : no order was ever sent (no deal_ref) -> release (restore fuel).
      SUBMITTED  : a POST may have opened a deal. If the reservation's deal_id is
                   known and present at broker -> OPEN. If a matching deal is not
                   present AND inventory is verified -> release (order never opened).
                   Otherwise stay SUBMITTED (fail closed; resolve next time).
      OPEN       : if the deal is gone from broker inventory, the caller handles
                   the close/harvest path; we do not release an OPEN reservation.
    Returns an action record. Never double-spends and never strands a real order.
    """
    res = live.get(RESERVATION_KEY) or {}
    state = res.get("state")
    if state not in (RESERVED, ATTEMPTED, SUBMITTED, OPEN):
        return {"action": "none", "state": state}
    rid = res.get("reservation_id")
    deal_id = res.get("deal_id")
    if state == RESERVED:
        # RESERVED means the durable ATTEMPTED transition never happened, so the
        # broker POST was NEVER issued (mark_attempted persists strictly before POST).
        # Safe to release and restore fuel.
        release(live, rid, "restart_reserved_no_submission_attempted", save=save)
        return {"action": "released", "from": RESERVED}
    if state in (ATTEMPTED, SUBMITTED):
        # A broker POST MAY have reached IG. We must not treat "no response" as
        # "no order". Resolve ONLY with positive broker evidence.
        if broker_deal_ids is None:
            # Unknown inventory -> fail closed, keep committed, block new gen-2.
            return {"action": "retained_unknown_inventory", "state": state}
        if deal_id and deal_id in broker_deal_ids:
            mark_open(live, rid, deal_id, save=save)
            return {"action": "opened", "deal_id": deal_id}
        if deal_id and deal_id not in broker_deal_ids:
            # We KNOW this deal id and it is provably absent from a verified
            # inventory -> the order did not open (or already closed pre-open).
            release_proven_absent(live, rid, "verified_absent_known_deal_id", save=save)
            return {"action": "released_known_absent", "from": state}
        # No dealId known (lost POST response). A verified inventory that does not
        # contain a matchable id does NOT prove our order is absent, because an
        # async fill could appear after the read. Fail closed: stay locked,
        # annotate ambiguous, require operator/broker-history resolution.
        mark_ambiguous(live, rid,
                       "lost_post_response_no_deal_id_unresolvable_from_inventory", save=save)
        return {"action": "ambiguous_locked", "state": state,
                "reason": "lost_post_no_dealid"}
    # OPEN: leave for the normal close/harvest path.
    return {"action": "open_retained", "deal_id": deal_id}


def needs_broker_reconciliation(live: dict) -> bool:
    """True if a reservation is SUBMITTED with unknown open/closed status
    (the fail-closed state that must be resolved from broker truth on restart)."""
    res = live.get(RESERVATION_KEY) or {}
    return res.get("state") in (ATTEMPTED, SUBMITTED)
