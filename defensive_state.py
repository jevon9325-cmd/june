"""Account hysteresis, independent of instrument/session/campaign decisions."""
import math


def update(state, *, now, max_age, micro_balance, micro_floor, micro_pct, floor, pct):
    day = float(state["balance_day_start"])
    equity = float(state["balance_total"])
    fetched = float(state["balance_fetched_at"])
    if not all(math.isfinite(v) for v in (day, equity, fetched, now)) or day <= 0:
        raise ValueError("invalid account risk inputs")
    if not 0 <= now - fetched <= max_age:
        raise ValueError("account equity unavailable or stale")
    prior = state.get("global_mode", "normal")
    if prior not in ("normal", "defensive"):
        raise ValueError("unknown global mode")
    # Keep the episode's reference through UTC rollover. A new CB baseline is
    # not evidence that the account recovered. Legacy state adopts today's basis.
    reference = float(state.get("global_mode_reference", day)) if prior == "defensive" else day
    if not math.isfinite(reference) or reference <= 0:
        raise ValueError("invalid defensive reference")
    threshold = max(micro_floor, micro_pct * reference) if reference < micro_balance else max(floor, pct * reference)
    loss = reference - equity
    new, reason = prior, None
    if prior == "normal" and loss >= threshold:
        new, reason = "defensive", "account_loss_reached_activation"
        state["global_mode_bal_entry"] = equity
        state["global_mode_entered_at"] = now
    elif prior == "defensive" and loss < threshold * .5:
        # Also respect today's activation threshold when references differ.
        today_threshold = max(micro_floor, micro_pct * day) if day < micro_balance else max(floor, pct * day)
        if day - equity < today_threshold * .5:
            new, reason = "normal", "account_recovered_below_existing_half_threshold"
    state["global_mode"] = new
    if new == "defensive":
        state["global_mode_reference"] = reference
    else:
        state.pop("global_mode_reference", None)
    if new != prior:
        return dict(prior=prior, new=new, reason=reason, account_loss=loss,
                    equity=equity, reference=reference, day_start=day,
                    activation=threshold, recovery=threshold * .5, observed_at=now)
    return None
