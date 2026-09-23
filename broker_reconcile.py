"""Full C2c evidence pipeline: normalize → lifecycle → attribute → store.

Connects C2c-A (broker_transaction), C2c-B (broker_match, broker_cost) and
the C2c-1/C2b PendingCloseStore into one reconciliation call.

No bot import, live Redis construction, network, or broker orders.

Delayed evidence model
----------------------
Broker history arrives asynchronously after June observes a close.  The
progression is always forward — callers simply re-call reconcile_position
as more evidence arrives; the store advances automatically.

  T0  history window empty             → pending_realizations
  T1  DEAL row present, COMM absent    → pending_costs
  T2  DEAL + COMM + cost_evidence      → complete

cost_evidence is an explicit external attestation ({source, reference,
covered_through}) that the caller supplies when they can confirm all costs
have been accounted for.  Finding no cost rows in the batch is NOT
sufficient — the store requires the attestation to finalize.

Idempotency
-----------
Calling reconcile_position multiple times with the same evidence produces
the same record each time.  The store refuses to overwrite a completed
record with a different outcome (raises EvidenceError).  The realization
claim mechanism prevents the same realization from being attributed to two
different positions.

excess_close is never resolved by guessing — it raises immediately.  The
caller must supply opened_utc in position_evidence to disambiguate.
"""

from broker_ledger import EvidenceError
from broker_transaction import normalize_batch
from broker_match import collect_lifecycle_realizations
from broker_cost import attribute_costs


def reconcile_position(store, deal_id, raw_batch, position_evidence,
                       entry_reference=None, cost_evidence=None):
    """Normalize raw history, run lifecycle + cost attribution, update the store.

    Parameters
    ----------
    store             : PendingCloseStore — the durable pending store.
    deal_id           : str — broker opening deal ID.
    raw_batch         : dict — output of fetch_transaction_history(); must carry
                        account_id, history_complete=True, from, to, transactions.
    position_evidence : dict — position identity: account_id, deal_id,
                        broker_instrument, direction, entry_price,
                        original_quantity.  opened_utc sharpens same-price matching.
    entry_reference   : str or None — opening COMM reference from the entry
                        confirmation.  Needed to attribute the opening-leg commission.
    cost_evidence     : dict or None — {source, reference, covered_through}.
                        Only passed to the store when attribution.cost_complete is
                        True — unattributed costs must not be silently ignored.
                        When None, the record stays pending_costs even if all costs
                        in the current batch were attributed.

    Returns
    -------
    (record, attribution) — record is the broker_ledger outcome dict;
    attribution is the attribute_costs() result for audit and delayed-evidence
    tracking.

    Raises
    ------
    EvidenceError
        excess_close: ambiguous opening identity — supply opened_utc to resolve.
        conflict: direction contradiction in matched DEAL rows.
        Any store.reconcile() invariant violation (collision, evidence regression).
    """
    if not isinstance(raw_batch, dict):
        raise EvidenceError("Raw history batch dict required")
    if not isinstance(position_evidence, dict):
        raise EvidenceError("Position evidence dict required")

    # C2c-A: normalize all rows in the batch (DEAL, COMM, DEPO, SWAP, …)
    normalized = normalize_batch(raw_batch)

    # C2c-B: lifecycle — accumulate all matching DEAL rows
    lc = collect_lifecycle_realizations(position_evidence, normalized)

    if lc["lifecycle_state"] == "conflict":
        raise EvidenceError(f"Realization conflict for deal {deal_id}: {lc['notes']}")
    if lc["lifecycle_state"] == "excess_close":
        raise EvidenceError(
            f"excess_close for deal {deal_id}: multiple positions share this opening "
            "identity — supply opened_utc in position_evidence to disambiguate. "
            + lc["notes"]
        )

    # C2c-B: cost attribution — route COMM/SWAP/DEPO to correct buckets
    attribution = attribute_costs(
        position_evidence, normalized,
        deal_references=lc.get("deal_references"),
        entry_reference=entry_reference,
    )

    # Only pass cost_evidence to the store when attribution confirms all costs in
    # this batch were accounted for.  If unattributed COMM rows remain, we must
    # NOT silently treat unknown costs as zero.
    effective_cost_evidence = cost_evidence if attribution["cost_complete"] else None

    record = store.reconcile(
        deal_id,
        raw_batch,
        costs=tuple(attribution["position_costs"]),
        cost_evidence=effective_cost_evidence,
    )
    return record, attribution
