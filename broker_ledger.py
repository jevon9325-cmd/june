"""Pure broker-history reconciliation. No bot imports, network, Redis, or orders.

The caller must supply broker-confirmed opening identity, complete paginated
history, and explicit cost attribution. Absence from /positions is not P&L
evidence. Pending records must never be delivered to learning consumers.
"""

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re
from broker_source import source_fields, IDENTITY_REASON


class EvidenceError(ValueError):
    """Conflicting or invalid evidence; keep the position pending for review."""


def _number(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise EvidenceError("Invalid numeric evidence") from exc
    if not result.is_finite():
        raise EvidenceError("Non-finite numeric evidence")
    return result


def _utc(value):
    try:
        text = str(value).strip()
        if not re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", text):
            raise ValueError("Broker timestamp must include seconds")
        stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceError("Invalid broker UTC timestamp") from exc
    # IG openDateUtc/dateUtc are explicitly UTC even when the offset is absent.
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc)


def _text(value):
    return format(value.normalize(), "f") if value else "0"


def _key(parts):
    return sha256(json.dumps(parts, separators=(",", ":"), sort_keys=True).encode()).hexdigest()


def _cash_number(value):
    """Strict decimal cash syntax; accept a single sign before or after USD $."""
    text = str(value).strip()
    if text.startswith(("-$", "+$")):
        text = text[0] + text[2:]
    elif text.startswith("$"):
        text = text[1:]
    if not re.fullmatch(r"[+-]?(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)(?:\.[0-9]+)?", text):
        raise EvidenceError("Invalid broker cash syntax")
    return _number(text.replace(",", ""))


def _money(row, currency):
    if currency != "USD" or row.get("currency") not in ("$", "USD"):
        raise EvidenceError("USD evidence required; do not guess FX conversion")
    return _cash_number(row.get("profitAndLoss"))


def reconcile_completed_trade(position, transactions, costs=(), *,
                              history_complete=False, costs_complete=False):
    """Build one JSON-safe record from explicit broker evidence, without I/O.

    position: account_id, deal_id, instrument, broker_instrument, direction,
      opened_utc (exact broker time), entry_price, original_quantity, currency;
      optionally asset_class, role, exit_reason, strategy_context, and
      notional_per_quantity (already converted using verified broker units).
    transactions: unmodified IG /history/transactions rows, from one account.
    costs: explicitly attributed signed USD amounts with account_id, deal_id,
      cost_id, amount, kind ('commission' or 'other'), source, broker_reference.
      Never allocate a fee by instrument name alone. Unknown costs stay pending.

    Exact opening identity deliberately avoids time/price tolerance joins that
    can assign adjacent entries or an add-on to the wrong position. Callers
    lacking that identity must obtain it; do not substitute local clock times.
    Quantities use Decimal; full realization coverage is required for completion.
    Transaction identity includes the opening tuple, not just closing reference.
    """
    required = ("account_id", "deal_id", "instrument", "broker_instrument",
                "direction", "opened_utc", "entry_price", "original_quantity", "currency")
    if any(position.get(k) in (None, "") for k in required):
        raise EvidenceError("Missing broker opening identity")
    if position["direction"] not in ("long", "short"):
        raise EvidenceError("Unknown position direction")
    if position["currency"] != "USD":
        raise EvidenceError("Only verified USD positions are currently supported")
    opened = _utc(position["opened_utc"])
    price = _number(position["entry_price"])
    quantity = _number(position["original_quantity"])
    if price <= 0 or quantity <= 0:
        raise EvidenceError("Opening price and quantity must be positive")
    sign = 1 if position["direction"] == "long" else -1
    realizations = {}
    for row in transactions:
        if row.get("transactionType") != "DEAL":
            continue
        if row.get("instrumentName") != position["broker_instrument"]:
            continue
        if _utc(row.get("openDateUtc")) != opened:
            continue
        if _number(row.get("openLevel")) != price:
            continue
        signed_size = _number(row.get("size"))
        if signed_size == 0:
            raise EvidenceError("Zero broker realization quantity")
        if (1 if signed_size > 0 else -1) != sign:
            continue
        closed = _utc(row.get("dateUtc"))
        if closed < opened:
            raise EvidenceError("Realization predates opening")
        reference = row.get("reference")
        if not reference:
            raise EvidenceError("Missing broker realization reference")
        exit_price = _number(row.get("closeLevel"))
        if exit_price <= 0:
            raise EvidenceError("Invalid closing price")
        amount = _money(row, position["currency"])
        identity = ['IG.realization.v3', position["account_id"],
                    row['instrumentName'], reference, opened.isoformat(),
                    _text(price), _text(signed_size), closed.isoformat(), _text(exit_price),
                    source_fields(row)]
        realization_id = _key(identity)
        realization = {
            "realization_id": realization_id, "broker_reference": reference,
            "exit_utc": closed.isoformat(), "exit_price": _text(exit_price),
            "quantity": _text(abs(signed_size)), "gross_pnl": _text(amount),
            "source": "IG.history.transactions.DEAL",
        }
        prior = realizations.get(realization_id)
        if prior is not None and prior != realization:
            raise EvidenceError("Conflicting versions of a broker realization")
        realizations[realization_id] = realization

    parts = sorted(realizations.values(), key=lambda x: (x["exit_utc"], x["realization_id"]))
    realized_quantity = sum((_number(x["quantity"]) for x in parts), Decimal(0))
    if realized_quantity > quantity:
        raise EvidenceError("Realized quantity exceeds the confirmed opening quantity")
    remaining = quantity - realized_quantity
    gross = sum((_number(x["gross_pnl"]) for x in parts), Decimal(0))
    attributable = {}
    for cost in costs:
        if (cost.get("account_id"), cost.get("deal_id")) != (position["account_id"], position["deal_id"]):
            continue
        if any(not cost.get(k) for k in ("cost_id", "source", "broker_reference")):
            raise EvidenceError("Cost attribution requires identity and provenance")
        if cost.get("currency") != position["currency"] or cost.get("kind") not in ("commission", "other"):
            raise EvidenceError("Unsupported attributed cost")
        normalized = {k: cost[k] for k in ("cost_id", "source", "broker_reference", "currency", "kind")}
        normalized["amount"] = _text(_number(cost.get("amount")))
        prior = attributable.get(cost["cost_id"])
        if prior is not None and prior != normalized:
            raise EvidenceError("Conflicting versions of an attributed cost")
        attributable[cost["cost_id"]] = normalized
    fees = sorted(attributable.values(), key=lambda x: x["cost_id"])
    commissions = sum((_number(x["amount"]) for x in fees if x["kind"] == "commission"), Decimal(0))
    other = sum((_number(x["amount"]) for x in fees if x["kind"] == "other"), Decimal(0))
    identified_net = gross + commissions + other
    complete = bool(history_complete and costs_complete and parts and remaining == 0)
    status = ("provisional" if complete else "pending_realizations"
              if not history_complete or remaining or not parts else "pending_costs")
    unit_notional = position.get("notional_per_quantity")
    exposure = None
    if unit_notional is not None:
        unit_notional = _number(unit_notional)
        if unit_notional <= 0:
            raise EvidenceError("Invalid verified unit notional")
        exposure = quantity * unit_notional
    closed = _utc(parts[-1]["exit_utc"]) if parts else None
    return {
        "schema_version": 1,
        "trade_id": _key([position["account_id"], position["deal_id"]]),
        "account_id": position["account_id"], "broker_deal_id": position["deal_id"],
        "instrument": position["instrument"], "broker_instrument": position["broker_instrument"],
        "asset_class": position.get("asset_class"), "direction": position["direction"],
        "currency": position["currency"], "status": status,
        "entry_utc": opened.isoformat(), "entry_price": _text(price),
        "exit_utc": closed.isoformat() if complete else None,
        "hold_seconds": (closed - opened).total_seconds() if complete else None,
        "original_quantity": _text(quantity), "remaining_quantity": _text(remaining),
        "original_notional": _text(exposure) if exposure is not None else None,
        "remaining_notional": _text(remaining * unit_notional) if exposure is not None else None,
        "gross_realized_pnl": _text(gross), "commissions": _text(commissions),
        "other_costs": _text(other), "net_identified_pnl": _text(identified_net),
        "net_realized_pnl": None,
        "won": None,
        "economic_state": "PROVISIONAL" if parts else "UNRESOLVED",
        "identity_state": "UNRESOLVED",
        "uncertainty_reasons": [IDENTITY_REASON],
        "realizations": parts, "costs": fees,
        "partial_close_count": len(parts) if remaining else max(0, len(parts) - 1),
        "role": position.get("role", "unknown"), "exit_reason": position.get("exit_reason", "unknown"),
        "strategy_context": deepcopy(position.get("strategy_context", {})),
        "provenance": {"pnl": "broker_realizations", "context": "matched_strategy_metadata",
                       "realization_identity_version": 3,
                       "identity_evidence_complete": False,
                       "history_complete": bool(history_complete), "costs_complete": bool(costs_complete)},
    }


def completed_history_view(record):
    """Compatibility view: one whole-position outcome, never a residual outcome.

    Pending evidence is rejected. The complete canonical record remains the
    authority; this projection is not a separately estimated outcome.
    """
    if record.get("status") != "complete":
        raise EvidenceError("Pending evidence cannot enter completed-trade history")
    gross = _number(record["gross_realized_pnl"])
    notional = record["original_notional"]
    context = record["strategy_context"]
    quantity = _number(record["original_quantity"])
    weighted_exit = sum((_number(p["quantity"]) * _number(p["exit_price"])
                         for p in record["realizations"]), Decimal(0)) / quantity
    return {
        "trade_id": record["trade_id"], "source": "broker_ledger_v1",
        "instrument": record["instrument"], "direction": record["direction"],
        "entry_price": float(record["entry_price"]), "exit_price": float(weighted_exit),
        "ig_size": float(quantity), "notional": float(notional) if notional is not None else None,
        "pnl_pct": float(gross / _number(notional)) if notional is not None else None,
        "dollar_pnl": float(record["net_realized_pnl"]), "won": record["won"],
        "commission": -float(record["commissions"]), "hold_min": record["hold_seconds"] / 60,
        "exit_epoch": _utc(record["exit_utc"]).timestamp(), "exit_reason": record["exit_reason"],
        "conviction": context.get("conviction"), "claudia_pts": context.get("claudia_pts"),
    }
