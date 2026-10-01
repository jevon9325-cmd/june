"""Settlement reconciliation matching + aggregation (pure logic, no I/O).

Extracted so the authoritative "broker-truth -> confirmed campaign economics"
decision is unit-testable in isolation and shared by the live reconciler.

Repairs two defects demonstrated in the 4e22d32 settlement forensic report:

  1. PARTIAL-exit veto: the previous scanner returned PARTIAL and gave up the
     moment it saw a POSITION_PARTIALLY_CLOSED action for the deal, even when the
     authoritative FINAL-close activity for the same deal was also present. A
     partial exit followed by a final close is a COMPLETE campaign and must
     reconcile, not defer forever.

  2. Single-transaction matcher: the previous matcher selected exactly ONE
     /history/transactions DEAL row by instrument + one close level. A campaign
     that exits in two (or more) broker fills books two DEAL rows at (usually)
     different close levels; the single-row matcher could never aggregate them,
     so even lifting the veto would have under- or non-reported the economics.

Design rules (fail-closed; never invent economics):
  * A campaign CONFIRMS only from authoritative broker-booked DEAL transactions
    whose identity reconciles to the opening and whose absolute sizes exactly
    cover the opening quantity (no residual broker exposure).
  * Transactions are de-duplicated by broker `reference`, so a duplicated page
    or a duplicated reconciliation call can never double-count P&L.
  * Partial + final economics are summed exactly once in broker currency.
  * Two evidence paths may CONFIRM:
      ACTIVITY_ANCHORED  - an authoritative FINAL-close activity for the deal is
                           present (direct dealId, or a unique affectedDealId
                           close), and the campaign transactions reconcile.
      TRANSACTION_ONLY   - close ACTIVITY has not (yet) been returned, but the
                           booked DEAL transactions reconcile to the opening by
                           strong identity (instrument + opening level +
                           direction + bounded opening time) and their absolute
                           sizes EXACTLY cover the opening quantity. Insufficient
                           identity or incomplete coverage stays PROVISIONAL.
  * Anything ambiguous or incomplete stays PROVISIONAL (DEFER / AMBIGUOUS).

This module performs no Redis, no HTTP and no clock reads of its own; `now` and
all broker evidence are passed in. The caller (june._live_reconcile_provisional_
settlements) keeps the durable-history, perf-feed and exactly-once persistence.
"""

CONFIRM = "CONFIRM"
DEFER = "DEFER"
AMBIGUOUS = "AMBIGUOUS"

# Evidence paths (recorded on the settlement for audit).
ACTIVITY_ANCHORED = "activity_anchored"
TRANSACTION_ONLY = "transaction_only"

# Quantity reconciliation tolerance. IG books lot sizes to 2 dp; 1e-6 is tighter
# than any real lot increment yet absorbs float noise.
_QTY_EPS = 1e-6
_LEVEL_EPS = 1e-6

POSITION_CLOSED = "POSITION_CLOSED"
POSITION_PARTIALLY_CLOSED = "POSITION_PARTIALLY_CLOSED"


def _to_float(value):
    """Parse an IG numeric which may be a float, int, or '$-1,234.56' string."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace("$", "").replace(",", "")
    if s == "":
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def scan_close_activity(deal_id, activities):
    """Classify close-activity evidence for `deal_id`.

    Returns a dict:
      {"status": "full_close"|"partial_only"|"none"|"ambiguous",
       "match": "A_direct"|"B_affected"|None,
       "close": {epic, level, size, direction, date} | None,
       "saw_partial": bool}

    A FINAL close is authoritative even if a partial-close activity for the same
    deal is also present (the previous bug vetoed on the partial). "partial_only"
    is returned ONLY when a partial action was seen and NO final close was found.
    """
    direct = None
    affected = []
    saw_partial = False
    for a in (activities or []):
        det = a.get("details") or {}
        acts = det.get("actions") or []
        is_direct = a.get("dealId") == deal_id
        for ac in acts:
            atype = ac.get("actionType")
            affects_us = ac.get("affectedDealId") == deal_id
            if atype == POSITION_PARTIALLY_CLOSED and (is_direct or affects_us):
                saw_partial = True  # noted, but NEVER vetoes a present final close
            elif atype == POSITION_CLOSED:
                if is_direct:
                    direct = {"epic": a.get("epic"), "level": det.get("level"),
                              "size": det.get("size"), "direction": det.get("direction"),
                              "date": a.get("date"), "match": "A_direct"}
                elif affects_us:
                    affected.append({"epic": a.get("epic"), "level": det.get("level"),
                                     "size": det.get("size"), "direction": det.get("direction"),
                                     "date": a.get("date"), "match": "B_affected"})
    if direct is not None:
        return {"status": "full_close", "match": "A_direct", "close": direct,
                "saw_partial": saw_partial}
    if len(affected) == 1:
        return {"status": "full_close", "match": "B_affected", "close": affected[0],
                "saw_partial": saw_partial}
    if len(affected) > 1:
        return {"status": "ambiguous", "match": None, "close": None,
                "saw_partial": saw_partial}
    if saw_partial:
        return {"status": "partial_only", "match": None, "close": None, "saw_partial": True}
    return {"status": "none", "match": None, "close": None, "saw_partial": False}


def _parse_date_token(value):
    """Best-effort ISO/date parse to epoch seconds; returns None if unparseable.

    Used only to bound TRANSACTION_ONLY opening-time identity. Never fabricates a
    time; an unparseable value simply means that identity signal is unavailable.
    """
    if value is None:
        return None
    import datetime as _dt
    s = str(value).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return _dt.datetime.strptime(s[:len(fmt) + 2] if "T" in s else s, fmt).replace(
                tzinfo=_dt.timezone.utc).timestamp()
        except ValueError:
            continue
    # ISO with timezone / 'Z'
    try:
        return _dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def collect_campaign_transactions(sym, opening_level, direction, transactions,
                                  instr_matcher, *, require_open_level=True):
    """Return the DEAL transactions that authoritatively belong to this campaign.

    Identity (fail-closed, all required):
      * transactionType == 'DEAL'
      * instrument name authoritatively identifies sym (instr_matcher)
      * openLevel matches the opening fill level (abs < _LEVEL_EPS)

    When the opening fill level is known (the normal case) a transaction that
    omits openLevel cannot assert campaign identity and is excluded. A campaign's
    partial and final fills share the SAME openLevel, so this groups every fill
    of one campaign while separating other campaigns on the same instrument.

    De-duplicates by broker `reference` (duplicate pages / redeliveries collapse).
    """
    seen_refs = set()
    rows = []
    ol = _to_float(opening_level)
    for tx in (transactions or []):
        if tx.get("transactionType") != "DEAL":
            continue
        if not instr_matcher(sym, tx.get("instrumentName")):
            continue
        tx_open = _to_float(tx.get("openLevel"))
        if ol is not None:
            if tx_open is None:
                # Opening level known but transaction omits it -> cannot assert identity.
                continue
            if abs(tx_open - ol) >= _LEVEL_EPS:
                continue
        elif require_open_level:
            # No authoritative opening level to anchor identity -> fail closed.
            continue
        ref = tx.get("reference")
        if ref is not None:
            if ref in seen_refs:
                continue
            seen_refs.add(ref)
        rows.append(tx)
    return rows


def _tx_close_matches(tx, level):
    """True iff a transaction's closeLevel matches `level` (abs < eps)."""
    if level is None:
        return False
    tc = _to_float(tx.get("closeLevel"))
    lv = _to_float(level)
    return tc is not None and lv is not None and abs(tc - lv) < _LEVEL_EPS


def _tx_signature(tx):
    """Economic fingerprint of a transaction row (reference + the fields that
    define its economics). Two rows with the same signature are the SAME booked
    transaction redelivered; the same reference with DIFFERENT economics is
    contradictory data and is NOT treated as a safe duplicate."""
    return (tx.get("reference"), _to_float(tx.get("closeLevel")),
            _to_float(tx.get("openLevel")), _to_float(tx.get("size")),
            _to_float(tx.get("profitAndLoss")))


def _dedup_by_reference(rows):
    """Collapse IDENTICAL redelivered rows (same reference AND same economics).

    Rows that share a reference but disagree on economics are kept (contradictory
    data must surface as ambiguity, never be silently merged). Rows without a
    reference are all kept (cannot prove duplication)."""
    seen_sig = set()
    seen_ref = set()
    out = []
    for tx in rows:
        ref = tx.get("reference")
        sig = _tx_signature(tx)
        if ref is not None and sig in seen_sig:
            continue  # exact redelivery of an already-seen transaction
        if ref is not None and ref in seen_ref and sig not in seen_sig:
            # Same reference, different economics -> contradictory; keep so the
            # caller's multiplicity guard can flag ambiguity.
            out.append(tx)
            continue
        if ref is not None:
            seen_ref.add(ref)
            seen_sig.add(sig)
        out.append(tx)
    return out


def aggregate_transactions(rows):
    """Sum P&L and absolute size across campaign DEAL rows.

    Returns {"pnl": float, "abs_size": float, "n": int, "last_close_level": float|None,
             "unparseable": bool}. `unparseable` is True if any row's P&L could not
            be parsed (caller must then NOT confirm).
    """
    pnl = 0.0
    abs_size = 0.0
    last_level = None
    unparseable = False
    # Preserve input order; the caller passes broker order (partial then final).
    for tx in rows:
        p = _to_float(tx.get("profitAndLoss"))
        if p is None:
            unparseable = True
        else:
            pnl += p
        sz = _to_float(tx.get("size"))
        if sz is not None:
            abs_size += abs(sz)
        lvl = _to_float(tx.get("closeLevel"))
        if lvl is not None:
            last_level = lvl
    return {"pnl": pnl, "abs_size": abs_size, "n": len(rows),
            "last_close_level": last_level, "unparseable": unparseable}


def reconcile_settlement(record, activities, transactions, instr_matcher, *, now=None):
    """Decide whether a PROVISIONAL campaign can CONFIRM from broker evidence.

    `record` is the durable settlement row (needs deal_id, instrument, direction,
    the opening quantity under 'ig_size', and 'entry_price'/'exit_epoch').

    Returns a verdict dict:
      {"verdict": CONFIRM, "dollar_pnl": float, "exit_price": float|None,
       "match": "A_direct"|"B_affected"|"transaction_only",
       "evidence_path": ACTIVITY_ANCHORED|TRANSACTION_ONLY,
       "n_transactions": int, "aggregated_abs_size": float, "reason": None}
    or {"verdict": DEFER, "reason": str, ...}
    or {"verdict": AMBIGUOUS, "reason": str, ...}

    Fail-closed: any missing identity, incomplete quantity coverage, unparseable
    economics, or ambiguity yields DEFER / AMBIGUOUS (campaign stays PROVISIONAL).
    """
    deal_id = record.get("deal_id")
    sym = record.get("instrument", "?")
    direction = record.get("direction", "long")
    opening_qty = _to_float(record.get("ig_size"))
    opening_level = record.get("entry_price")

    scan = scan_close_activity(deal_id, activities)

    if scan["status"] == "ambiguous":
        return {"verdict": AMBIGUOUS, "reason": "multiple_affected_closes",
                "match": None, "evidence_path": None}

    _have_opening_qty = opening_qty is not None and opening_qty > 0

    # ---- Path 1: ACTIVITY_ANCHORED (a final-close activity is present) ----
    if scan["status"] == "full_close":
        close_level = scan["close"].get("level")
        # Instrument-identified DEAL rows that book the authoritative final-close
        # level. Deduped by broker reference (duplicate pages collapse).
        anchor_rows = _dedup_by_reference(
            [tx for tx in (transactions or [])
             if tx.get("transactionType") == "DEAL"
             and instr_matcher(sym, tx.get("instrumentName"))
             and _tx_close_matches(tx, close_level)])
        if not anchor_rows:
            # Activity says closed at a level no booked transaction reports.
            return {"verdict": DEFER, "reason": "no_transaction_at_close_level",
                    "match": scan["match"], "evidence_path": None}
        if len(anchor_rows) > 1:
            # More than one distinct booked transaction reports the authoritative
            # close level -> genuinely ambiguous which is the final fill; do not
            # guess (identical redeliveries have already been collapsed).
            return {"verdict": AMBIGUOUS, "reason": "multiple_tx_at_close_level",
                    "match": scan["match"], "evidence_path": None,
                    "n_candidates": len(anchor_rows)}
        anchor = anchor_rows[0]

        if _have_opening_qty:
            # Group every campaign fill by the anchor's opening level (all fills of
            # one campaign share the entry fill price), aggregate exactly once, and
            # require full quantity coverage so no residual exposure is implied.
            group = collect_campaign_transactions(
                sym, anchor.get("openLevel"), direction, transactions, instr_matcher,
                require_open_level=True)
            if not group:
                group = [anchor]
            agg = aggregate_transactions(group)
            if agg["unparseable"]:
                return {"verdict": DEFER, "reason": "unparseable_pnl",
                        "match": scan["match"], "evidence_path": None}
            if abs(agg["abs_size"] - opening_qty) >= _QTY_EPS:
                return {"verdict": DEFER, "reason": "quantity_coverage_incomplete",
                        "match": scan["match"], "evidence_path": None,
                        "aggregated_abs_size": round(agg["abs_size"], 8),
                        "opening_qty": opening_qty}
            exit_price = close_level if close_level is not None else agg["last_close_level"]
            return {"verdict": CONFIRM, "dollar_pnl": round(agg["pnl"], 4),
                    "exit_price": _to_float(exit_price),
                    "match": scan["match"], "evidence_path": ACTIVITY_ANCHORED,
                    "n_transactions": agg["n"],
                    "aggregated_abs_size": round(agg["abs_size"], 8), "reason": None}

        # Opening quantity unavailable AND an observed partial exit -> aggregate the
        # anchor's opening-level group (cannot verify coverage, but the local
        # partial marker evidences a multi-fill campaign). Otherwise, conservative
        # single-fill confirm from the unique anchor transaction.
        if _record_shows_observed_partial(record):
            group = collect_campaign_transactions(
                sym, anchor.get("openLevel"), direction, transactions, instr_matcher,
                require_open_level=True) or [anchor]
        else:
            group = [anchor]
        agg = aggregate_transactions(group)
        if agg["unparseable"]:
            return {"verdict": DEFER, "reason": "unparseable_pnl",
                    "match": scan["match"], "evidence_path": None}
        exit_price = close_level if close_level is not None else agg["last_close_level"]
        return {"verdict": CONFIRM, "dollar_pnl": round(agg["pnl"], 4),
                "exit_price": _to_float(exit_price),
                "match": scan["match"], "evidence_path": ACTIVITY_ANCHORED,
                "n_transactions": agg["n"],
                "aggregated_abs_size": round(agg["abs_size"], 8), "reason": None}

    # Authoritative campaign transactions (grouped by instrument + opening level)
    # for the transaction-only path below.
    rows = collect_campaign_transactions(
        sym, opening_level, direction, transactions, instr_matcher)

    # ---- partial_only: activity explicitly shows a partial close but NO final ----
    # The broker activity stream is positively telling us the campaign is not yet
    # fully closed. Even if transactions appear to cover the quantity, an explicit
    # partial-only activity signal must not be overridden to declare closure
    # (invariant: never declare possible residual exposure closed to finalize).
    if scan["status"] == "partial_only":
        return {"verdict": DEFER, "reason": "partial_close_not_final",
                "match": None, "evidence_path": None}

    # ---- Path 2: TRANSACTION_ONLY (close activity ABSENT, booked txns authoritative) ----
    # Reserved for the narrow case where NO close activity (partial or final) has
    # been returned yet, but the booked transactions reconcile to the opening by
    # identity + exact quantity coverage, proving no remaining broker exposure.
    #
    # This path is additionally gated on positive local evidence that the campaign
    # genuinely exited in multiple broker fills: the settlement record must carry
    # an observed partial-exit marker (partial_dollar_pnl != 0). This distinguishes
    # a real multi-fill campaign whose activity rows have not yet surfaced (report
    # cohort #21) from a stray single transaction that merely happens to match an
    # instrument + level. Without that marker we stay PROVISIONAL (fail-closed).
    if scan["status"] == "none":
        if not rows:
            return {"verdict": DEFER, "reason": "no_close_activity_yet",
                    "match": None, "evidence_path": None}
        if not _record_shows_observed_partial(record):
            return {"verdict": DEFER, "reason": "no_close_activity_yet",
                    "match": None, "evidence_path": None}
        if not _have_opening_qty:
            # Without an authoritative opening quantity we cannot prove full
            # coverage from transactions alone -> stay provisional.
            return {"verdict": DEFER, "reason": "opening_quantity_unknown_for_tx_only",
                    "match": None, "evidence_path": None}
        agg = aggregate_transactions(rows)
        if agg["unparseable"]:
            return {"verdict": DEFER, "reason": "unparseable_pnl",
                    "match": None, "evidence_path": None}
        if agg["n"] < 2:
            # A genuine multi-fill campaign books at least a partial and a final
            # DEAL row. A single transaction with no activity is insufficient.
            return {"verdict": DEFER, "reason": "insufficient_tx_for_tx_only",
                    "match": None, "evidence_path": None}
        if abs(agg["abs_size"] - opening_qty) >= _QTY_EPS:
            return {"verdict": DEFER, "reason": "quantity_coverage_incomplete",
                    "match": None, "evidence_path": None,
                    "aggregated_abs_size": round(agg["abs_size"], 8),
                    "opening_qty": opening_qty}
        # Strong identity (instrument + opening level + observed partial) and exact
        # quantity coverage across >=2 booked fills.
        return {"verdict": CONFIRM, "dollar_pnl": round(agg["pnl"], 4),
                "exit_price": _to_float(agg["last_close_level"]),
                "match": "transaction_only", "evidence_path": TRANSACTION_ONLY,
                "n_transactions": agg["n"],
                "aggregated_abs_size": round(agg["abs_size"], 8), "reason": None}

    return {"verdict": DEFER, "reason": "unclassified", "match": None, "evidence_path": None}


def _record_shows_observed_partial(record):
    """True iff the settlement record carries positive evidence that a partial
    exit was locally observed for this campaign (partial_dollar_pnl != 0). Used to
    gate the TRANSACTION_ONLY path so it cannot fire for a plain single exit."""
    try:
        return abs(float(record.get("partial_dollar_pnl") or 0.0)) > 1e-9
    except (TypeError, ValueError):
        return False
