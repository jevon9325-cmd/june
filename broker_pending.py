"""Durable pending evidence and atomic projections, using an injected Redis client.

June forwards pending capture only; history/projections remain unwired.
No network client is constructed here. All journal
fields share one account-scoped hash, without TTL. WATCH + one HSET commits
evidence, realization ownership and delivery state together. Redis persistence
itself is an operational prerequisite; this protects application restarts and
ambiguous acknowledgements, not loss of the Redis database.
"""

from copy import deepcopy
import json

from redis.exceptions import WatchError

from broker_ledger import EvidenceError, _key, _number, _text, _utc, reconcile_completed_trade


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class _TargetedFields:
    """Lazy reads inside WATCH; only touched fields participate in serialization."""
    def __init__(self, client, key):
        self.client, self.key = client, key
        self.before, self.values = {}, {}

    def __contains__(self, field):
        if field not in self.before:
            raw = self.client.hget(self.key, field)
            self.before[field] = raw.decode() if isinstance(raw, bytes) else raw
            if raw is not None:
                self.values[field] = json.loads(raw)
        return field in self.values

    def __getitem__(self, field):
        if field not in self:
            raise KeyError(field)
        return self.values[field]

    def get(self, field, default=None):
        return self[field] if field in self else default

    def __setitem__(self, field, value):
        field in self  # retain the original value before mutation
        self.values[field] = value

    def setdefault(self, field, default):
        if field not in self:
            self.values[field] = default
        return self.values[field]

    def updates(self):
        changed = {}
        for field, value in self.values.items():
            encoded = _json(value)
            if self.before[field] != encoded:
                changed[field] = encoded
        return changed


class PendingCloseStore:
    def __init__(self, client, account_id):
        if not isinstance(account_id, str) or not account_id.strip():
            raise EvidenceError("Verified account identity required")
        self.client, self.account_id = client, account_id
        self.key = "june_broker_ledger_v1:" + _key(account_id)

    def _field(self, deal_id):
        if not isinstance(deal_id, str) or not deal_id.strip():
            raise EvidenceError("Broker deal identity required")
        return "trade:" + _key([self.account_id, deal_id])

    def _update(self, change):
        # No callback may perform external side effects: WATCH can retry it.
        for _ in range(8):
            with self.client.pipeline() as pipe:
                try:
                    pipe.watch(self.key)
                    data = _TargetedFields(pipe, self.key)
                    result = change(data)
                    updates = data.updates()
                    if updates:
                        pipe.multi()
                        pipe.hset(self.key, mapping=updates)
                        pipe.execute()
                    return deepcopy(result)
                except WatchError:
                    continue
        raise EvidenceError("Journal contention; retain pending work and retry")

    def capture(self, deal_id, source, evidence):
        """Keep an immutable raw snapshot BEFORE a caller clears/removes a leg.

        Capture is not proof of June ownership or of a closed trade. Unknown
        identity/costs remain pending. Failure propagates; callers must not
        silently clear their only remaining evidence after a failed capture.
        """
        field = self._field(deal_id)
        if not isinstance(source, str) or not source or not isinstance(evidence, dict):
            raise EvidenceError("Evidence source and object required")
        event = {"source": source, "evidence": deepcopy(evidence)}
        event_id = _key(json.loads(_json(event)))

        def change(data):
            entry = data.setdefault(field, {"account_id": self.account_id, "deal_id": deal_id,
                                           "events": {}, "opening": None, "record": None})
            entry["events"].setdefault(event_id, event)
            return entry
        return self._update(change)

    def register_opening(self, position, ownership_evidence):
        """Attach exact broker identity and explicit evidence of June entry.

        Recovered/manual positions must not be registered merely because June
        manages them. The caller must retain the actual accepted entry receipt.
        Local timestamps, inferred quantities and instrument-name fee joins are
        not acceptable substitutes. All known openings must be registered before
        reconciliation so colliding opening tuples are held for review.
        """
        pos = deepcopy(position)
        reconcile_completed_trade(pos, [])  # validate canonical identity/units
        if pos["account_id"] != self.account_id:
            raise EvidenceError("Opening belongs to another account")
        if (not isinstance(ownership_evidence, dict)
                or ownership_evidence.get("source") != "june.accepted_entry_confirm"
                or ownership_evidence.get("dealId") != pos["deal_id"]
                or ownership_evidence.get("dealStatus") != "ACCEPTED"
                or not ownership_evidence.get("dealReference")):
            raise EvidenceError("Explicit June entry confirmation required")
        field = self._field(pos["deal_id"])
        signature = _key([pos["broker_instrument"], _utc(pos["opened_utc"]).isoformat(),
                          _text(_number(pos["entry_price"])), pos["direction"]])

        def change(data):
            entry = data.get(field)
            if entry is None:
                raise EvidenceError("Capture opening evidence before registration")
            owner_key = "opening:" + signature
            owners = data.setdefault(owner_key, [])
            if field not in owners:
                owners.append(field)
            # Retain collisions rather than silently choosing either position.
            if entry["opening"] is not None and entry["opening"] != pos:
                raise EvidenceError("Conflicting broker opening identity")
            entry["opening"] = pos
            entry["opening_key"] = owner_key
            entry["ownership_evidence"] = deepcopy(ownership_evidence)
            return entry
        return self._update(change)

    def reconcile(self, deal_id, batch, costs=(), *, cost_evidence=None):
        """Persist pending or completed result; completion never guesses zero costs.

        `cost_evidence` must explicitly attest attributable costs through the
        closing date, with source/reference/covered_through. Merely observing no
        fee rows is insufficient. Delayed/corrected evidence cannot silently
        overwrite a previously completed outcome.
        """
        field = self._field(deal_id)
        if batch.get("account_id") != self.account_id or batch.get("history_complete") is not True:
            raise EvidenceError("Complete account-pinned history required")

        def change(data):
            entry = data.get(field)
            if not entry or entry["opening"] is None:
                raise EvidenceError("Exact broker opening still missing")
            if (entry.get('record') and entry['record'].get('costs')
                    and entry.get('cost_claim_version') != 2):
                raise EvidenceError('Legacy cost records require explicit evidence review')
            if data[entry["opening_key"]] != [field]:
                raise EvidenceError("Ambiguous opening tuple; manual attribution required")
            pos = entry["opening"]
            if _utc(batch["from"]) > _utc(pos["opened_utc"]) or _utc(batch["to"]) <= _utc(pos["opened_utc"]):
                raise EvidenceError("History window does not cover opening")
            if cost_evidence is not None:
                if any(not cost_evidence.get(k) for k in ("source", "reference", "covered_through")):
                    raise EvidenceError("Explicit cost coverage evidence required")
            record = reconcile_completed_trade(pos, batch["transactions"], costs,
                                               history_complete=True, costs_complete=cost_evidence is not None)
            for part in record["realizations"]:
                if _utc(part["exit_utc"]) > _utc(batch["to"]):
                    raise EvidenceError("Realization outside fetched history window")
                claim = "realization:" + part["realization_id"]
                if data.get(claim, field) != field:
                    raise EvidenceError("Realization already attributed to another position")
                data[claim] = field
            for cost in record['costs']:
                claim = 'cost:' + cost['cost_id']
                if data.get(claim, field) != field:
                    raise EvidenceError('Cost already attributed to another position')
                data[claim] = field
            if record["status"] == "complete":
                if _utc(cost_evidence["covered_through"]) < _utc(record["exit_utc"]):
                    raise EvidenceError("Cost evidence ends before final realization")
                record["provenance"]["cost_coverage"] = deepcopy(cost_evidence)
            previous = entry["record"]
            if previous:
                for collection, identity in (("realizations", "realization_id"), ("costs", "cost_id")):
                    current = {item[identity]: item for item in record[collection]}
                    if any(current.get(item[identity]) != item for item in previous[collection]):
                        raise EvidenceError("Previously observed evidence missing or changed; retain pending record")
            if previous and previous["status"] == "complete" and previous != record:
                raise EvidenceError("Completed evidence changed; review rather than redeliver")
            entry["record"] = record
            entry['cost_claim_version'] = 2
            return record
        return self._update(change)

    def project_once(self, deal_id, consumer, project):
        """Atomically update a JOURNAL-OWNED projection and its delivery marker.

        `project(state, record)` must return JSON-safe state and have NO external
        side effects. This is NOT an acknowledgement API for existing June/Redis
        learning functions: those are not transactionally integrated yet.
        """
        field = self._field(deal_id)
        if not isinstance(consumer, str) or not consumer:
            raise EvidenceError("Consumer identity required")
        state_key, delivery_key = "consumer:" + consumer, "delivery:" + _key([field, consumer])

        def change(data):
            entry = data.get(field)
            record = entry.get("record") if entry else None
            if not record or record["status"] != "complete":
                raise EvidenceError("Pending outcome cannot be delivered")
            if record.get('costs') and entry.get('cost_claim_version') != 2:
                raise EvidenceError('Legacy cost records cannot be delivered')
            if data[entry["opening_key"]] != [field]:
                raise EvidenceError("Ambiguous opening tuple cannot be delivered")
            if delivery_key not in data:
                data[state_key] = project(deepcopy(data.get(state_key, {})), deepcopy(record))
                data[delivery_key] = record["trade_id"]
            return data[state_key]
        return self._update(change)

    def entries(self):
        """Compatibility snapshot; use iter_entries for incremental recovery."""
        return list(self.iter_entries())

    def get_entry(self, deal_id):
        """Read one position without enumerating account history."""
        raw = self.client.hget(self.key, self._field(deal_id))
        return json.loads(raw) if raw is not None else None

    def iter_entries(self, count=100):
        """Incremental HSCAN, not a consistent snapshot; retry/dedup by deal ID.

        Redis may repeat fields during a scan. Recovery consumers must therefore
        be idempotent. No unresolved or completed evidence expires here.
        """
        if type(count) is not int or count <= 0:
            raise ValueError("Positive scan count required")
        for _, raw in self.client.hscan_iter(self.key, match="trade:*", count=count):
            yield json.loads(raw)
