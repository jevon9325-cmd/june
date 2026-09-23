"""Cost attribution for IG trading history transactions.

Classifies non-DEAL rows into cost scope categories and produces cost records
for reconcile_completed_trade().  No bot import, network, Redis, or orders.

Attribution scopes
------------------
POSITION    — uniquely attributable to one specific position; the COMM reference
              matches a confirmed deal reference (opening or closing leg).
INSTRUMENT  — identifiable by instrument but not a single position; typical for
              SWAP financing rows where multiple concurrent positions are possible.
ACCOUNT     — account-wide only; no position or instrument context.
NON_TRADING — account capital movement, not a trading cost.  Known example:
              DEPO "Trade Based Concession - Jul26" ($150.00) is real account capital,
              not trade alpha, not performance, not commission.  It must never be
              attached to any trade record merely because it appears nearby in time.

IG commission model (equity CFD)
---------------------------------
Opening commission: COMM row, reference = opening deal reference.
  Source: the opening confirmation (broker_entry_evidence).
Closing commission: COMM row, reference = closing deal reference.
  Source: the closing DEAL row's reference field.
Round-trip commission: typically $9 open + $9 close = $18/trade.
Historical finding: 5 equity CFD trades, gross P&L ≈ +$3.68, commissions = −$90.
  Gross P&L alone is not sufficient for performance evaluation.

To attribute both commission legs, call attribute_costs() with:
  deal_references: references from all matching closing DEAL rows
                   (from collect_lifecycle_realizations()["deal_references"])
  entry_reference: reference from the opening confirmation
                   (from broker_entry_evidence or the accepted-entry response)

Financing (SWAP) limitation
-----------------------------
Known example: "Daily Financing Adjustment - FX Interest for 1 day Spot Gold ($1)".
SWAP rows carry an instrument_name but no position-specific deal reference.
When multiple positions are open simultaneously in the same instrument, SWAP rows
cannot be attributed to a single position.  They are always INSTRUMENT scope at best.
cost_complete is conservatively set to False whenever any SWAP row is present for
the position's instrument.  Callers must NOT silently treat this as zero cost.

cost_complete semantics
-----------------------
cost_complete=True means all trading costs for this position have been identified
and attributed.  This requires:
  1. deal_references supplied (closing-leg commissions confirmed)
  2. entry_reference supplied (opening-leg commission confirmed)
  3. No unresolved COMM rows (all referenced COMM rows in known deal refs)
  4. No SWAP rows for this instrument (financing attribution impossible)
cost_complete=False: pass costs_complete=False to reconcile_completed_trade and
leave net_realized_pnl as None.  Do not treat missing costs as zero.
"""

from broker_ledger import EvidenceError, _key, _number, _text
from broker_transaction import UNKNOWN

POSITION    = "position"      # uniquely attributable to one trade
INSTRUMENT  = "instrument"    # instrument-level only; multiple positions possible
ACCOUNT     = "account"       # account-wide; no instrument or position context
NON_TRADING = "non_trading"   # account capital movement; excluded from all trade costs

_NON_TRADING_TYPES = frozenset({"deposit", "withdrawal"})


def classify_cost(norm_tx):
    """Classify one normalized non-DEAL transaction for cost attribution.

    Returns:
    {
      "scope":       POSITION | INSTRUMENT | ACCOUNT | NON_TRADING,
      "reason":      human-readable classification rationale,
      "reference":   broker reference (or UNKNOWN),
      "instrument":  instrument_name (or UNKNOWN),
      "amount":      cash_amount — negative = charge, positive = credit (or UNKNOWN),
      "tx_type":     normalized transaction_type,
    }

    POSITION scope is a candidate only — the caller must still confirm the reference
    matches a known deal reference before attributing.  Never attribute by reference
    alone when multiple positions share the same closing reference (shared-reference
    case from broker_ledger).
    """
    tx_type = norm_tx.get("transaction_type", UNKNOWN)
    ref     = norm_tx.get("reference", UNKNOWN)
    inst    = norm_tx.get("instrument_name", UNKNOWN)
    amount  = norm_tx.get("cash_amount", UNKNOWN)

    base = {"reference": ref, "instrument": inst, "amount": amount, "tx_type": tx_type}

    if tx_type in _NON_TRADING_TYPES:
        return {**base, "scope": NON_TRADING,
                "reason": f"account capital movement ({tx_type}); excluded from all trade costs"}

    if tx_type == "commission":
        if ref != UNKNOWN:
            return {**base, "scope": POSITION,
                    "reason": "commission with broker reference; requires deal confirmation"}
        return {**base, "scope": ACCOUNT, "reason": "commission without broker reference"}

    if tx_type == "financing":
        if inst != UNKNOWN:
            return {**base, "scope": INSTRUMENT,
                    "reason": "instrument-linked financing; multiple positions possible — "
                              "INSTRUMENT scope only, never POSITION"}
        return {**base, "scope": ACCOUNT,
                "reason": "financing without instrument name; account-level only"}

    # interest, other, unknown type
    return {**base, "scope": ACCOUNT, "reason": f"unclassified non-trade: {tx_type}"}


def build_cost_record(norm_tx, account_id, deal_id):
    """Convert a confirmed POSITION-scope COMM or SWAP row to a cost record dict.

    The result is directly usable as an element of the costs= argument to
    reconcile_completed_trade().  The deal_id MUST come from the matched position
    identity — it is never present in the raw IG transaction row.

    Returns None when required fields (reference or cash_amount) are UNKNOWN —
    meaning the row cannot be attributed without additional evidence.

    Raises EvidenceError for unsupported transaction types or non-USD currency.
    Cost records with missing reference or amount are returned as None, not raised,
    so callers can route them to unattributed_costs rather than halting.
    """
    if not isinstance(account_id, str) or not account_id.strip():
        raise EvidenceError("Account identity required to build cost record")
    if not isinstance(deal_id, str) or not deal_id.strip():
        raise EvidenceError("Deal identity required to build cost record")

    tx_type = norm_tx.get("transaction_type")
    if tx_type not in ("commission", "financing"):
        raise EvidenceError(
            f"Only commission and financing rows become cost records; got: {tx_type!r}")

    currency = norm_tx.get("currency")
    if currency != "USD":
        raise EvidenceError("USD evidence required; do not guess FX conversion")

    ref    = norm_tx.get("reference")
    amount = norm_tx.get("cash_amount")
    if ref == UNKNOWN or amount == UNKNOWN:
        return None

    kind    = "commission" if tx_type == "commission" else "other"
    raw_t   = norm_tx.get("raw_transaction_type") or tx_type.upper()
    if norm_tx.get('account_id') != account_id:
        raise EvidenceError('Cost source belongs to another account')
    # A source-row fingerprint, never a claimant-dependent identity. Preserve
    # every normalized economic discriminator available in the history row.
    # This does not prove that two economically distinct, identical source rows
    # are one event; certification needs stronger broker evidence in that case.
    identity = {k: norm_tx.get(k, UNKNOWN) for k in (
        'source', 'raw_transaction_type', 'reference', 'instrument_name',
        'open_utc', 'close_utc', 'direction', 'currency')}
    for key in ('cash_amount', 'open_price', 'close_price', 'close_quantity'):
        value = norm_tx.get(key, UNKNOWN)
        identity[key] = UNKNOWN if value == UNKNOWN else _text(_number(value))
    cost_id = 'cost-v2:' + _key([account_id, identity])

    return {
        "account_id":       account_id,
        "deal_id":          deal_id,
        "cost_id":          cost_id,
        "source":           f"IG.history.transactions.{raw_t}",
        "broker_reference": str(ref),
        "currency":         "USD",
        "kind":             kind,
        "amount":           str(amount),
    }


def attribute_costs(position_evidence, normalized_transactions,
                    deal_references=None, entry_reference=None):
    """Attribute non-DEAL rows from a normalized batch to a specific position.

    position_evidence: dict with at least account_id, deal_id, broker_instrument.
    normalized_transactions: complete normalized batch from normalize_batch().
    deal_references: set of reference strings from matched closing DEAL transactions.
                     Obtain from collect_lifecycle_realizations()["deal_references"].
    entry_reference: reference from the opening confirmation (broker_entry_evidence),
                     needed to attribute the opening-leg commission.

    Attribution rules:
      COMM with reference ∈ {deal_references ∪ {entry_reference}} → position_costs
      COMM with reference NOT in known refs                        → unattributed_costs
      COMM without reference                                       → account_costs
      SWAP with instrument == position's broker_instrument         → instrument_costs
      SWAP with different or UNKNOWN instrument                    → account_costs
      DEPO / WITH                                                  → non_trading
      Other                                                        → account_costs

    cost_complete is conservatively False whenever:
      - deal_references or entry_reference is not supplied
      - any SWAP rows are present for this instrument (financing not resolvable)
      - any unattributed COMM rows remain
    Only when all of the above are resolved is cost_complete True.

    Returns:
    {
      "position_costs":       list[dict]  — for reconcile_completed_trade costs=
      "instrument_costs":     list[dict]  — SWAP rows for this instrument
      "account_costs":        list[dict]  — account-wide non-trading-cost rows
      "non_trading":          list[dict]  — capital movements; excluded from costs
      "unattributed_costs":   list[dict]  — COMM rows not matched to a deal ref
      "cost_complete":        bool
      "cost_complete_reason": str
    }
    """
    if not isinstance(position_evidence, dict):
        raise EvidenceError("Position evidence dict required")
    if not isinstance(normalized_transactions, list):
        raise EvidenceError("Normalized transaction list required")

    account_id  = position_evidence.get("account_id", "")
    deal_id     = position_evidence.get("deal_id", "")
    broker_inst = position_evidence.get("broker_instrument", "")

    known_refs = set()
    if deal_references:
        known_refs.update(str(r) for r in deal_references if r not in (None, UNKNOWN))
    if entry_reference and entry_reference not in (None, UNKNOWN):
        known_refs.add(str(entry_reference))

    position_costs   = []
    instrument_costs = []
    account_costs    = []
    non_trading      = []
    unattributed     = []
    financing_present = False

    # A shared closing reference is not a position identity. Inspect every
    # opening in the supplied batch, including rows belonging to other trades.
    reference_openings = {}
    for tx in normalized_transactions:
        if tx.get('account_id') == account_id and tx.get('transaction_type') == 'deal':
            signature = tuple(tx.get(k, UNKNOWN) for k in (
                'instrument_name', 'open_utc', 'open_price', 'direction'))
            reference_openings.setdefault(tx.get('reference'), set()).add(signature)

    for norm_tx in normalized_transactions:
        if norm_tx.get("transaction_type") == "deal":
            continue
        if norm_tx.get("account_id") != account_id:
            continue

        cl = classify_cost(norm_tx)
        scope = cl["scope"]

        if scope == NON_TRADING:
            non_trading.append(cl)
            continue

        if scope == ACCOUNT:
            account_costs.append(cl)
            continue

        if scope == INSTRUMENT:
            tx_inst = norm_tx.get("instrument_name")
            if broker_inst and tx_inst == broker_inst:
                financing_present = True
                instrument_costs.append(cl)
            else:
                account_costs.append(cl)
            continue

        # scope == POSITION (commission with reference)
        ref = norm_tx.get("reference")
        if known_refs and ref in known_refs and len(reference_openings.get(ref, ())) <= 1:
            rec = build_cost_record(norm_tx, account_id, deal_id)
            if rec is not None:
                position_costs.append(rec)
            else:
                unattributed.append(cl)
        else:
            unattributed.append(cl)

    if deal_references is None:
        complete = False
        reason = "deal_references not supplied; closing-leg commission attribution unconfirmed"
    elif entry_reference is None:
        complete = False
        reason = "entry_reference not supplied; opening-leg commission attribution unconfirmed"
    elif financing_present:
        complete = False
        reason = (
            "instrument-level financing present for this position's instrument; "
            "SWAP attribution at position level requires deal ID in SWAP rows "
            "(inherent IG history API limitation) — cost_complete cannot be asserted")
    elif unattributed:
        complete = False
        reason = (f"{len(unattributed)} commission row(s) with reference not in "
                  "confirmed deal set; manual attribution required")
    else:
        complete = True
        reason = ("all COMM rows for confirmed deal references attributed; "
                  "no unresolved instrument-level financing")

    return {
        "position_costs":       position_costs,
        "instrument_costs":     instrument_costs,
        "account_costs":        account_costs,
        "non_trading":          non_trading,
        "unattributed_costs":   unattributed,
        "cost_complete":        complete,
        "cost_complete_reason": reason,
    }
