"""Normalization of raw IG /history/transactions rows.

No bot import, network, Redis, or orders.
UNKNOWN is the explicit sentinel for fields that are absent or unparseable —
never a default, guess, or synthetic value.  Raw source evidence is always
preserved verbatim for provenance audit.
"""

from copy import deepcopy
from decimal import Decimal, InvalidOperation

from broker_ledger import EvidenceError, _cash_number, _utc


UNKNOWN = "UNKNOWN"  # Field unavailable in source row: absent, empty, or unparseable

_TYPE_MAP = {
    "DEAL":     "deal",
    "COMM":     "commission",
    "DEPO":     "deposit",
    "SWAP":     "financing",
    "WITH":     "withdrawal",
    "INTEREST": "interest",
}


def _parse_decimal(value):
    if value is None or (isinstance(value, str) and not value.strip()):
        return UNKNOWN
    try:
        d = Decimal(str(value).replace(",", "").strip())
        if not d.is_finite():
            return UNKNOWN
        return str(d)
    except (InvalidOperation, ValueError, TypeError):
        return UNKNOWN


def parse_utc(value):
    """Normalize an ISO-8601 timestamp to UTC ISO-8601 with explicit offset.

    Returns UNKNOWN when absent, empty, or unparseable.
    """
    try:
        return _utc(value).isoformat()
    except (EvidenceError, ValueError, TypeError):
        return UNKNOWN


def _parse_cash(raw):
    """Parse IG profitAndLoss field.

    Handles all observed IG formats: '$-0.16', '-$9', '$0.17', '1.23', etc.
    Returns a plain Decimal string or UNKNOWN.
    """
    try:
        return str(_cash_number(raw))
    except EvidenceError:
        return UNKNOWN


def _parse_currency(raw):
    if raw in ("$", "USD"):
        return "USD"
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return UNKNOWN


def _parse_size(raw_size):
    """Return (direction, close_quantity) from the IG size field.

    IG sign convention (confirmed by broker_ledger reconciliation):
      negative size → original position was SHORT (opened by SELL)
      positive size → original position was LONG  (opened by BUY)
    """
    if raw_size is None:
        return UNKNOWN, UNKNOWN
    try:
        d = Decimal(str(raw_size))
        if not d.is_finite() or d == 0:
            return UNKNOWN, UNKNOWN
        return ("long" if d > 0 else "short"), str(abs(d))
    except (InvalidOperation, ValueError, TypeError):
        return UNKNOWN, UNKNOWN


def normalize_transaction(row, account_id):
    """Normalize one raw IG /history/transactions row.

    All fields that are absent, empty, or unparseable are set to UNKNOWN.
    The raw source row is preserved verbatim under 'raw' for provenance audit.
    No field is ever invented, defaulted, or guessed.

    Normalized fields:
      schema_version        always 1
      account_id            from the enclosing batch (caller-supplied)
      transaction_type      deal | commission | deposit | financing |
                            withdrawal | interest | other; UNKNOWN if absent
      raw_transaction_type  original transactionType string (audit)
      reference             broker reference; UNKNOWN if absent/empty
      instrument_name       from instrumentName; UNKNOWN if absent/empty
      direction             long | short derived from size sign; UNKNOWN if unparseable
      close_quantity        abs(size) as Decimal string; UNKNOWN if unparseable
      open_price            from openLevel; UNKNOWN if absent/unparseable
      close_price           from closeLevel; UNKNOWN if absent/unparseable
      open_utc              from openDateUtc, UTC ISO-8601; UNKNOWN if absent/invalid
      close_utc             from dateUtc, UTC ISO-8601; UNKNOWN if absent/invalid
      cash_amount           from profitAndLoss (multiple formats); UNKNOWN if unparseable
      currency              USD for '$'/'USD'; raw string otherwise; UNKNOWN if absent

    Permanently UNKNOWN at C2c-A (IG transaction rows do not expose these):
      deal_id               original deal ID not in transaction history rows
      opening_deal_id       not in transaction history rows
    """
    if not isinstance(row, dict):
        raise EvidenceError("Transaction row must be a dict")
    if not isinstance(account_id, str) or not account_id.strip():
        raise EvidenceError("Account identity required for normalization")

    raw_type = row.get("transactionType")
    tx_type = _TYPE_MAP.get(raw_type, "other") if isinstance(raw_type, str) else UNKNOWN

    direction, quantity = _parse_size(row.get("size"))

    return {
        "schema_version": 1,
        "account_id": account_id,
        "transaction_type": tx_type,
        "raw_transaction_type": raw_type,
        "reference": row.get("reference") or UNKNOWN,
        "instrument_name": row.get("instrumentName") or UNKNOWN,
        "direction": direction,
        "close_quantity": quantity,
        "open_price": _parse_decimal(row.get("openLevel")),
        "close_price": _parse_decimal(row.get("closeLevel")),
        "open_utc": parse_utc(row.get("openDateUtc")),
        "close_utc": parse_utc(row.get("dateUtc")),
        "cash_amount": _parse_cash(row.get("profitAndLoss")),
        "currency": _parse_currency(row.get("currency")),
        "deal_id": UNKNOWN,
        "opening_deal_id": UNKNOWN,
        "source": "IG.history.transactions.v2",
        "raw": deepcopy(row),
    }


def normalize_batch(batch):
    """Normalize all rows from a complete fetch_transaction_history result.

    The batch must carry a verified account identity and history_complete=True.
    All transaction types are preserved (DEAL + COMM + DEPO + SWAP etc.) for
    downstream cost attribution.  Order is preserved from the fetched window.
    """
    if not isinstance(batch, dict) or batch.get("history_complete") is not True:
        raise EvidenceError("Complete history batch required for normalization")
    account_id = batch.get("account_id")
    if not isinstance(account_id, str) or not account_id.strip():
        raise EvidenceError("Batch must carry verified account identity")
    return [normalize_transaction(row, account_id)
            for row in batch.get("transactions", [])]
