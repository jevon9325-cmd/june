"""Read-only IG transaction pagination; caller supplies an account-pinned GET.

No authentication, requests, bot import or order endpoints. A fetched window is
not proof that commissions/carry are final. Failed/unstable pagination raises;
the caller must retain pending work and retry the entire fixed window.
Reference: https://labs.ig.com/reference/history-transactions.html (GET v2).
"""

from copy import deepcopy

from broker_ledger import EvidenceError, _key, _utc


def fetch_transaction_history(get, account_id, start, end, *, page_size=500, max_pages=100):
    """Fetch ALL transaction types, including fees, without reference-only dedup.

    `get` must stay on the supplied verified broker account for the entire call.
    No incomplete result is returned. Completeness applies only to this window
    and the broker's current response, not transactions that have yet to arrive.
    """
    if not isinstance(account_id, str) or not account_id.strip():
        raise EvidenceError("Verified account identity required")
    start, end = _utc(start), _utc(end)
    if start >= end or start.microsecond or end.microsecond:
        raise EvidenceError("History requires increasing whole-second UTC bounds")
    if any(type(v) is not int or v <= 0 for v in (page_size, max_pages)):
        raise EvidenceError("Positive pagination limits required")
    params = {"type": "ALL", "from": start.strftime("%Y-%m-%dT%H:%M:%S"),
              "to": end.strftime("%Y-%m-%dT%H:%M:%S"), "pageSize": page_size}
    rows, seen, expected = [], set(), None
    for page in range(1, max_pages + 1):
        response = get("/history/transactions", params={**params, "pageNumber": page}, version="2")
        if not isinstance(response, dict) or response.get("errorCode"):
            raise EvidenceError("History page unavailable; retry fixed window")
        metadata = response.get("metadata", response.get("metaData"))
        if "metadata" in response and "metaData" in response and response["metadata"] != response["metaData"]:
            raise EvidenceError("Conflicting pagination metadata")
        if not isinstance(metadata, dict) or not isinstance(metadata.get("pageData"), dict):
            raise EvidenceError("History lacks pagination evidence")
        info, chunk = metadata["pageData"], response.get("transactions")
        numbers = [info.get("pageNumber"), info.get("pageSize"), info.get("totalPages"), metadata.get("size")]
        if any(type(v) is not int for v in numbers) or not isinstance(chunk, list):
            raise EvidenceError("Malformed history page")
        number, size, pages, count = numbers
        if number != page or size != page_size or pages < 0 or count < 0 or pages > max_pages:
            raise EvidenceError("Unexpected history pagination")
        if pages != (count + page_size - 1) // page_size and not (count == 0 and pages == 1):
            raise EvidenceError("Inconsistent history page count")
        shape = (pages, count)
        if expected is not None and shape != expected:
            raise EvidenceError("History changed during pagination; retry")
        expected = shape
        expected_size = min(page_size, max(0, count - (page - 1) * page_size))
        if len(chunk) != expected_size or any(not isinstance(row, dict) for row in chunk):
            raise EvidenceError("History page is truncated or malformed")
        for row in chunk:
            fingerprint = _key(row)
            if fingerprint in seen:
                raise EvidenceError("Repeated history row across pages; retry")
            seen.add(fingerprint)
            rows.append(deepcopy(row))
        if page >= pages:
            return {"account_id": account_id, "from": start.isoformat(), "to": end.isoformat(),
                    "transactions": rows, "history_complete": True,
                    "source": "IG.history.transactions.v2", "page_count": page}
    raise EvidenceError("History exceeds page budget; split window without discarding pending work")
