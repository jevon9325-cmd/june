"""Opening evidence only: no I/O, sizing changes or inferred broker timestamps."""

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation


def response_account_evidence(session, headers):
    """Attribute a response only while its request tokens match the known session.

    Tokens are compared in memory and NEVER included in returned evidence.
    A changed/unknown session is explicitly unverified rather than guessed.
    """
    matched = (bool(headers.get("CST")) and bool(headers.get("X-SECURITY-TOKEN"))
               and headers["CST"] == session.get("cst")
               and headers["X-SECURITY-TOKEN"] == session.get("token"))
    account = session.get("account_id") if matched else None
    if not isinstance(account, str) or not account.strip() or account == "?":
        account = None
    return {"account_id": account, "currency": session.get("account_currency") if account else None,
            "source": "IG.session.currentAccountId" if account else "unverified_session",
            "verified": account is not None}


def opening_evidence(instrument, role, order, response, confirmation, market=None, local=None):
    """Retain accepted receipt separately from submitted/local estimates.

    Confirmation `date` is a transaction timestamp, not documented as UTC or as
    exact position.createdDateUTC. Even an offset-bearing value is retained as
    confirmation_utc only. A later account-pinned position/activity response must
    supply exact opening identity before registration in the canonical ledger.
    """
    if role not in ("primary", "add_on"):
        raise ValueError("Unknown opening role")
    order, response, confirmation = deepcopy(order), deepcopy(response), deepcopy(confirmation)
    post_account = response.pop("_june_account_evidence", {})
    confirm_account = confirmation.pop("_june_account_evidence", {})
    issues = []
    verified = (post_account.get("verified") is True and confirm_account.get("verified") is True
                and bool(post_account.get("account_id"))
                and post_account.get("account_id") == confirm_account.get("account_id"))
    if not verified:
        issues.append("unverified_or_changed_account")
    if confirmation.get("dealStatus") != "ACCEPTED":
        issues.append("confirmation_not_accepted")
    if not confirmation.get("dealId"):
        issues.append("missing_deal_id")
    if not response.get("dealReference"):
        issues.append("missing_opening_reference")
    elif confirmation.get("dealReference") != response["dealReference"]:
        issues.append("missing_or_conflicting_confirmation_reference")
    for key in ("epic", "direction"):
        if not confirmation.get(key) or confirmation[key] != order.get(key):
            issues.append("missing_or_conflicting_" + key)
    for key in ("level", "size"):
        try:
            number = Decimal(str(confirmation.get(key)))
            if not number.is_finite() or number <= 0:
                raise ValueError()
        except (InvalidOperation, ValueError):
            issues.append("missing_or_invalid_" + key)
    if confirmation.get("status") not in ("OPEN", "OPENED"):
        issues.append("opening_status_not_confirmed")
    confirmation_utc = None
    try:
        stamp = datetime.fromisoformat(str(confirmation.get("date")).replace("Z", "+00:00"))
        if stamp.tzinfo is not None:
            confirmation_utc = stamp.astimezone(timezone.utc).isoformat()
    except ValueError:
        pass
    market = deepcopy(market) if market else None
    if market and market.get("epic") != order.get("epic"):
        issues.append("market_epic_mismatch")
        market = None
    return {
        "schema_version": 1, "source": "june.accepted_entry_path",
        "identity_status": "unverified_entry" if issues else "pending_broker_opening",
        "identity_issues": issues,
        "account_id": post_account.get("account_id") if verified else None,
        "account_evidence": {"order": post_account, "confirmation": confirm_account},
        "instrument": instrument, "role": role,
        "deal_id": confirmation.get("dealId"), "deal_reference": response.get("dealReference"),
        "broker_direction": confirmation.get("direction"),
        "broker_quantity": confirmation.get("size"), "broker_level": confirmation.get("level"),
        "broker_opened_utc": None, "confirmation_utc": confirmation_utc,
        "missing_for_reconciliation": ["exact_broker_opening_utc", "verified_transaction_instrument_name",
                                       "verified_position_currency_and_unit_semantics"],
        "submitted_order": order, "opening_response": response,
        "accepted_confirmation": confirmation, "broker_market_snapshot": market,
        "local_context": deepcopy(local or {}),
    }
