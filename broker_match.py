"""Open/close matching between pending June positions and normalized transactions.

No bot import, network, Redis, or orders.  Pure functions only.

Match confidence states
-----------------------
EXACT           All key identity fields present and matching; single candidate.
HIGH_CONFIDENCE Core fields match (account + instrument + open_price + direction);
                at least one confirming field (quantity or open_utc) absent.
AMBIGUOUS       Multiple transactions reach HIGH_CONFIDENCE or above for one position.
NO_MATCH        No transaction reaches the minimum identity threshold.
CONFLICT        A transaction matches account + instrument + open_price but
                contradicts direction — evidence is internally inconsistent.

Reference alone is explicitly forbidden as a matching key.  Real IG history
contains cases where two distinct realizations share the same closing reference
but have different opening tuples (instrument + openDateUtc + openLevel) that
identify different positions.  The matcher never uses reference for identity.

Identity hierarchy (strongest first)
-------------------------------------
1. account_id              [required — 1 pt]
2. instrument_name         [1 pt; required for >= HIGH_CONFIDENCE]
3. open_price              [1 pt; strong discriminator]
4. direction               [1 pt; required for >= HIGH_CONFIDENCE;
                            CONFLICT if present and contradicts when score >= 3]
5. close_quantity          [1 pt; confirming]
6. open_utc                [1 pt bonus; confirming; promotes to EXACT without quantity]

EXACT  requires: account + instrument + open_price + direction
              + at least one confirming field (close_quantity or open_utc).
HIGH_CONFIDENCE: account + instrument + open_price + direction only.
"""

from decimal import Decimal, InvalidOperation

from broker_ledger import EvidenceError
from broker_transaction import UNKNOWN, parse_utc

EXACT           = "EXACT"
HIGH_CONFIDENCE = "HIGH_CONFIDENCE"
AMBIGUOUS       = "AMBIGUOUS"
NO_MATCH        = "NO_MATCH"
CONFLICT        = "CONFLICT"

_REQUIRED_FOR_HIGH = frozenset({"account_id", "instrument_name", "open_price", "direction"})
_CONFIRMING       = frozenset({"close_quantity", "open_utc"})


def _decimal_eq(a, b):
    """Exact Decimal equality; False if either value is absent or unparseable."""
    try:
        da = Decimal(str(a))
        db = Decimal(str(b))
        return da.is_finite() and db.is_finite() and da == db
    except (InvalidOperation, ValueError, TypeError):
        return False


def _score_transaction(norm_tx, position):
    """Score one normalized transaction against position identity evidence.

    Returns (score, basis, conflict) where:
      score     int 0–6 (higher = stronger match)
      basis     list[str] of matched field names
      conflict  True when account + instrument + open_price all match but direction
                contradicts; this is evidence inconsistency, not a weak match

    Only DEAL rows close positions; other types score 0.
    Reference is never included in basis — forbidden by spec.
    """
    if norm_tx.get("transaction_type") != "deal":
        return 0, [], False
    if norm_tx.get("account_id") != position.get("account_id"):
        return 0, [], False

    basis = ["account_id"]
    score = 1

    # Instrument — required for HIGH_CONFIDENCE; instrument mismatch is non-match, not conflict
    tx_inst = norm_tx.get("instrument_name")
    pos_inst = position.get("broker_instrument")
    if tx_inst == UNKNOWN or not pos_inst:
        pass  # can't confirm or deny
    elif tx_inst == pos_inst:
        basis.append("instrument_name")
        score += 1
    else:
        return score, basis, False  # instrument contradicts — unrelated trade

    if score < 2:
        return score, basis, False

    # Open price — strong discriminator
    op = norm_tx.get("open_price")
    ep = position.get("entry_price")
    if op != UNKNOWN and ep is not None and _decimal_eq(op, ep):
        basis.append("open_price")
        score += 1

    # Direction — if both present and contradicting when score >= 3: CONFLICT
    tx_dir = norm_tx.get("direction")
    pos_dir = position.get("direction")
    if tx_dir != UNKNOWN and pos_dir:
        if tx_dir == pos_dir:
            basis.append("direction")
            score += 1
        elif score >= 3:
            # account + instrument + open_price all match but direction wrong — CONFLICT
            return score, basis, True
        # score < 3: direction contradicts but evidence too weak to call conflict

    # Close quantity — confirming field
    cq = norm_tx.get("close_quantity")
    oq = position.get("original_quantity")
    if cq != UNKNOWN and oq is not None and _decimal_eq(cq, oq):
        basis.append("close_quantity")
        score += 1

    # Open UTC — bonus confirming field; normalized before comparison
    ou = norm_tx.get("open_utc")
    pu_raw = position.get("opened_utc")
    if ou != UNKNOWN and pu_raw:
        pu = parse_utc(pu_raw)
        if pu != UNKNOWN and ou == pu:
            basis.append("open_utc")
            score += 1

    return score, basis, False


def _is_exact(basis):
    return _REQUIRED_FOR_HIGH.issubset(basis) and bool(_CONFIRMING & set(basis))


def _is_high_confidence(basis):
    return _REQUIRED_FOR_HIGH.issubset(basis)


def collect_lifecycle_realizations(position, normalized_transactions):
    """Collect ALL DEAL transactions attributable to this position's full lifecycle.

    Unlike match_realizations() which signals AMBIGUOUS when multiple transactions
    reach HIGH_CONFIDENCE, this function ACCUMULATES all matching DEAL rows as the
    position's complete realization set.  Correctly handles partial + residual
    close sequences without requiring the caller to resolve AMBIGUOUS manually.

    Matching criterion: all four core fields must match — account, instrument,
    open_price, direction.  Quantity is NOT required for collection; individual
    partial closes may be smaller than original_quantity.  When opened_utc is
    known on the position it acts as an additional filter via _score_transaction,
    preventing same-price positions from mixing.

    Lifecycle states:
      full_close:    sum(close_quantity) == original_quantity  [Stage 1 AMBIGUOUS resolved]
      partial_close: sum < original_quantity (residual close still pending)
      excess_close:  sum > original_quantity — AMBIGUOUS; two or more positions
                     likely share this opening identity.  Supply opened_utc to resolve.
                     Do NOT pick one transaction arbitrarily from an excess_close result.
      no_match:      no DEAL row matches the minimum four identity fields
      conflict:      a matched row contradicts direction

    Limitation: without opened_utc, same-price same-direction positions cannot be
    distinguished.  excess_close is the signal; the Stage 1 AMBIGUOUS limitation is
    not fully resolved in this case — see broker_match module docstring.

    Returns:
    {
      "lifecycle_state":       str,
      "realizations":          list of matched norm_tx dicts (ALL partials + residual)
      "total_closed_quantity": Decimal string or UNKNOWN
      "quantity_accounted":    bool — True only when sum == original_quantity
      "deal_references":       set[str] — references from all matched DEAL rows
      "raw_rows":              list[dict] — norm_tx["raw"] for reconcile_completed_trade
      "notes":                 str
    }
    """
    if not isinstance(position, dict):
        raise EvidenceError("Position evidence dict required")
    if not isinstance(normalized_transactions, list):
        raise EvidenceError("Normalized transaction list required")

    realizations = []
    conflicts = []

    for norm_tx in normalized_transactions:
        score, basis, is_conflict = _score_transaction(norm_tx, position)
        if is_conflict:
            conflicts.append(norm_tx)
        elif _REQUIRED_FOR_HIGH.issubset(set(basis)):
            # All 4 core identity fields confirmed — include regardless of quantity match
            realizations.append(norm_tx)

    if conflicts and not realizations:
        return {
            "lifecycle_state": "conflict",
            "realizations": [],
            "total_closed_quantity": UNKNOWN,
            "quantity_accounted": False,
            "deal_references": set(),
            "raw_rows": [],
            "notes": f"{len(conflicts)} direction conflict(s); no valid realizations",
        }

    if not realizations:
        return {
            "lifecycle_state": "no_match",
            "realizations": [],
            "total_closed_quantity": UNKNOWN,
            "quantity_accounted": False,
            "deal_references": set(),
            "raw_rows": [],
            "notes": "No DEAL transactions match the 4 core identity fields",
        }

    total = Decimal("0")
    total_known = True
    for r in realizations:
        cq = r.get("close_quantity")
        if cq == UNKNOWN:
            total_known = False
        else:
            try:
                total += Decimal(str(cq))
            except (InvalidOperation, ValueError):
                total_known = False

    oq = position.get("original_quantity")
    if total_known and oq is not None:
        try:
            original = Decimal(str(oq))
            if total == original:
                state, accounted = "full_close", True
            elif total < original:
                state, accounted = "partial_close", False
            else:
                state, accounted = "excess_close", False
        except (InvalidOperation, ValueError):
            state, accounted = "partial_close", False
    else:
        state, accounted = "partial_close", False

    deal_refs = {r.get("reference") for r in realizations
                 if r.get("reference") not in (UNKNOWN, None)}

    notes_parts = [f"{len(realizations)} realization(s)"]
    if total_known:
        notes_parts.append(f"total closed {total}")
    if state == "excess_close":
        notes_parts.append(
            "excess quantity — multiple positions likely share this opening identity; "
            "provide opened_utc to resolve")
    if conflicts:
        notes_parts.append(f"{len(conflicts)} direction conflict(s) present")

    return {
        "lifecycle_state": state,
        "realizations": realizations,
        "total_closed_quantity": str(total) if total_known else UNKNOWN,
        "quantity_accounted": accounted,
        "deal_references": deal_refs,
        "raw_rows": [r.get("raw", {}) for r in realizations],
        "notes": "; ".join(notes_parts),
    }


def match_realizations(position, normalized_transactions):
    """Match a pending position to normalized DEAL transactions.

    position: dict with at minimum:
      account_id         verified account identity
      broker_instrument  instrument name as used by the IG broker
      direction          'long' or 'short'
      entry_price        broker fill level (Decimal-parseable string)
      original_quantity  broker fill size (Decimal-parseable string), or None
      opened_utc         exact broker opening UTC (may be None at C2c-A); if
                         provided it is normalized before comparison so
                         'YYYY-MM-DDTHH:MM:SS' and 'YYYY-MM-DDTHH:MM:SS+00:00'
                         match the same normalized transaction timestamp

    Returns:
    {
      "confidence": EXACT | HIGH_CONFIDENCE | AMBIGUOUS | NO_MATCH | CONFLICT,
      "matches":    list of {"transaction": norm_tx, "score": int, "basis": list},
      "notes":      human-readable explanation,
    }

    Reference is never used as a matching key — see module docstring.
    """
    if not isinstance(position, dict):
        raise EvidenceError("Position evidence dict required")
    if not isinstance(normalized_transactions, list):
        raise EvidenceError("Normalized transaction list required")

    candidates = []
    conflicts = []

    for norm_tx in normalized_transactions:
        score, basis, is_conflict = _score_transaction(norm_tx, position)
        entry = {"transaction": norm_tx, "score": score, "basis": basis}
        if is_conflict:
            entry["basis"] = basis + ["direction_conflict"]
            entry["score"] = -1
            conflicts.append(entry)
        elif _is_exact(basis) or _is_high_confidence(basis):
            candidates.append(entry)

    if conflicts and not candidates:
        return {
            "confidence": CONFLICT,
            "matches": conflicts,
            "notes": (f"{len(conflicts)} transaction(s) match account + instrument + "
                      f"open_price but direction contradicts the position"),
        }

    if not candidates:
        note = "No transaction reaches HIGH_CONFIDENCE threshold"
        if conflicts:
            note += f"; {len(conflicts)} direction conflict(s) also present"
        return {"confidence": NO_MATCH, "matches": [], "notes": note}

    if len(candidates) > 1:
        return {
            "confidence": AMBIGUOUS,
            "matches": candidates,
            "notes": (f"{len(candidates)} transactions reach HIGH_CONFIDENCE or above; "
                      f"reference not used for disambiguation"),
        }

    match = candidates[0]
    confidence = EXACT if _is_exact(match["basis"]) else HIGH_CONFIDENCE
    return {
        "confidence": confidence,
        "matches": [match],
        "notes": "Matched on: " + ", ".join(match["basis"]),
    }
