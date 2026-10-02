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
from broker_finality import cost_statement_consistent


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _quarantine_record(record, reason):
    record = deepcopy(record)
    record.update(status='unresolved', economic_state='UNRESOLVED',
                  identity_state='AMBIGUOUS', cost_state='UNRESOLVED', net_realized_pnl=None, won=None)
    record.setdefault('provenance', {}).update(economic_evidence_complete=False,
                                             identity_evidence_complete=False,
                                             costs_complete=False, broker_posting_finalized=False)
    record['uncertainty_reasons'] = sorted(set(record.get('uncertainty_reasons', []) + [reason]))
    return record


def _entry_view(entry):
    """Old completed records cannot regain certification merely by being read."""
    entry = deepcopy(entry)
    if entry and (entry.get('record') or {}).get('status') == 'complete':
        entry.setdefault('record_before_quarantine', deepcopy(entry['record']))
        entry['record'] = _quarantine_record(entry['record'], 'Legacy completion is not certified by C2c')
    return entry


def _projection_view(value, reason):
    if isinstance(value, dict) and value.get('quarantine_version') == 1:
        result = deepcopy(value)
        result['reasons'] = sorted(set(result['reasons'] + [reason]))
        return result
    return {'quarantine_version': 1, 'status': 'quarantined', 'economic_state': 'UNRESOLVED',
            'value': None, 'prior_projection': deepcopy(value), 'reasons': [reason],
            'stage_e_recovery_required': True}


def _quarantine_collision(data, owners, reason):
    """One WATCH/HSET invalidates records and opaque historical aggregates.

    Old reducers have no inverse/dependency ledger. Quarantine every account
    projection rather than subtracting an invented correction from an aggregate.
    HSCAN is used only on this exceptional collision path, never normal capture.
    """
    for owner in owners:
        entry = data.get(owner)
        if entry is None:
            continue
        entry['identity_quarantine'] = sorted(set(entry.get('identity_quarantine', []) + [reason]))
        if entry.get('record'):
            entry.setdefault('record_before_quarantine', deepcopy(entry['record']))
            entry['record'] = _quarantine_record(entry['record'], reason)
    for key, _ in data.client.hscan_iter(data.key, match='consumer:*', count=100):
        key = key.decode() if isinstance(key, bytes) else key
        data[key] = _projection_view(data[key], reason)


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
        return _entry_view(self._update(change))

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
            if (entry.get('ownership_evidence') is not None
                    and entry['ownership_evidence'] != ownership_evidence):
                raise EvidenceError('Conflicting accepted entry receipt')
            entry["opening"] = pos
            entry["opening_key"] = owner_key
            entry["ownership_evidence"] = deepcopy(ownership_evidence)
            reference_key = 'entry_reference:' + _key(ownership_evidence['dealReference'])
            reference_owners = data.setdefault(reference_key, [])
            if field not in reference_owners:
                reference_owners.append(field)
            if len(owners) > 1:
                _quarantine_collision(data, owners, 'Opening identity collision: ' + owner_key)
            if len(reference_owners) > 1:
                affected = [owner for owner in reference_owners
                            if any(cost['broker_reference'] == ownership_evidence['dealReference']
                                   for cost in (data.get(owner, {}).get('record') or {}).get('costs', []))]
                if affected:
                    _quarantine_collision(data, affected, 'Commission reference collision: ' + reference_key)
            return entry
        return _entry_view(self._update(change))

    def reconcile(self, deal_id, batch, costs=(), *, cost_evidence=None):
        """Persist provisional or unresolved observations; never certify net P&L.

        Version-2 cost_evidence is retained as an UNVERIFIED assertion. Its
        consistency is checked independently of certification. No supported
        broker identity/posting-finality adapter exists, so even consistent
        evidence remains provisional with unresolved costs and no certified net.
        """
        field = self._field(deal_id)
        costs = tuple(costs)
        if batch.get("account_id") != self.account_id or batch.get("history_complete") is not True:
            raise EvidenceError("Complete account-pinned history required")

        def change(data):
            entry = data.get(field)
            if not entry or entry["opening"] is None:
                raise EvidenceError("Exact broker opening still missing")
            if entry.get('identity_quarantine'):
                raise EvidenceError('Ambiguous identity is quarantined; no automatic resurrection')
            if (entry.get('record') and entry['record'].get('costs')
                    and entry.get('cost_claim_version') != 3):
                raise EvidenceError('Legacy cost records require explicit evidence review')
            if (entry.get('record') and entry['record'].get('realizations')
                    and entry['record'].get('provenance', {}).get('realization_identity_version') != 3):
                raise EvidenceError('Legacy realization records require explicit evidence review')
            if data[entry["opening_key"]] != [field]:
                raise EvidenceError("Ambiguous opening tuple; manual attribution required")
            pos = entry["opening"]
            if _utc(batch["from"]) > _utc(pos["opened_utc"]) or _utc(batch["to"]) <= _utc(pos["opened_utc"]):
                raise EvidenceError("History window does not cover opening")
            # Retain unresolved economics too. Like realizations, cost history
            # must be cumulative; a retry cannot silently forget a prior SWAP
            # or reference-less fee merely because it could not be attributed.
            observed_cost_rows = {_key(row): deepcopy(row) for row in batch['transactions']
                                  if row.get('transactionType') not in ('DEAL', 'DEPO', 'WITH')}
            if any(key not in observed_cost_rows for key in entry.get('observed_cost_rows', {})):
                raise EvidenceError('Previously observed cost rows missing; retain pending record')
            entry['observed_cost_rows'] = observed_cost_rows
            record = reconcile_completed_trade(pos, batch["transactions"], costs,
                                               history_complete=True, costs_complete=False)
            statement_consistent = cost_statement_consistent(pos, record, batch, cost_evidence)
            if statement_consistent:
                # Direct store callers must not bypass unresolved raw costs.
                from broker_cost import build_cost_record
                from broker_transaction import normalize_batch
                identified = {cost['cost_id'] for cost in record['costs']}
                for tx in normalize_batch(batch):
                    if tx['transaction_type'] in ('deal', 'deposit', 'withdrawal'):
                        continue
                    if tx['transaction_type'] != 'commission':
                        statement_consistent = False
                        break
                    if tx['close_utc'] != 'UNKNOWN' and not (
                            _utc(batch['from']) <= _utc(tx['close_utc']) <= _utc(batch['to'])):
                        raise EvidenceError('Cost posting outside fetched history window')
                    candidate = build_cost_record(tx, self.account_id, deal_id)
                    if candidate is None or candidate['cost_id'] not in identified:
                        statement_consistent = False
                        break
            if statement_consistent:
                record = reconcile_completed_trade(pos, batch['transactions'], costs,
                                                   history_complete=True, costs_complete=True)
            record['provenance']['history_request_complete'] = True
            record['provenance']['history_window_covered'] = True
            record['provenance']['history_window'] = {k: batch[k] for k in ('from', 'to')}
            record['provenance']['broker_posting_finalized'] = False
            record['provenance']['history_fetch_complete'] = True
            if cost_evidence is not None:
                entry.setdefault('unverified_cost_statements', {})[_key(cost_evidence)] = deepcopy(cost_evidence)
            record['provenance']['cost_statement_consistent'] = statement_consistent
            record['provenance']['economic_evidence_complete'] = False
            for part in record["realizations"]:
                if _utc(part["exit_utc"]) > _utc(batch["to"]):
                    raise EvidenceError("Realization outside fetched history window")
                claim = "realization:" + part["realization_id"]
                if data.get(claim, field) != field:
                    raise EvidenceError("Realization already attributed to another position")
                data[claim] = field
            for cost in record['costs']:
                owners = data.get('entry_reference:' + _key(cost['broker_reference']), [])
                if owners and owners != [field]:
                    raise EvidenceError('Ambiguous commission reference ownership')
                claim = 'cost:' + cost['cost_id']
                if data.get(claim, field) != field:
                    raise EvidenceError('Cost already attributed to another position')
                data[claim] = field
            previous = entry["record"]
            if previous:
                for collection, identity in (("realizations", "realization_id"), ("costs", "cost_id")):
                    current = {item[identity]: item for item in record[collection]}
                    if any(current.get(item[identity]) != item for item in previous[collection]):
                        raise EvidenceError("Previously observed evidence missing or changed; retain pending record")
            if previous and previous["status"] == "complete" and previous != record:
                raise EvidenceError("Completed evidence changed; review rather than redeliver")
            entry["record"] = record
            entry['cost_claim_version'] = 3
            return record
        return self._update(change)

    def project_once(self, deal_id, consumer, project):
        """Reserved compatibility boundary: C2c currently refuses every delivery.

        There is no supported finality adapter. Never execute the reducer, even
        for a legacy record claiming complete. Historical projections are read
        only through get_projection's quarantined audit view. External reversal
        or rebuilding is a Stage E responsibility, not implemented here.
        """
        field = self._field(deal_id)
        if not isinstance(consumer, str) or not consumer:
            raise EvidenceError("Consumer identity required")
        def change(data):
            entry = data.get(field)
            record = entry.get("record") if entry else None
            if not record or record["status"] != "complete":
                raise EvidenceError("Pending outcome cannot be delivered")
            raise EvidenceError('Legacy identity/posting finality has no supported certification adapter')
        return self._update(change)

    def entries(self):
        """Compatibility snapshot; use iter_entries for incremental recovery."""
        return list(self.iter_entries())

    def get_entry(self, deal_id):
        """Read one position without enumerating account history."""
        raw = self.client.hget(self.key, self._field(deal_id))
        return _entry_view(json.loads(raw)) if raw is not None else None

    def prune_settled(self, deal_id, durable_archive_present):
        """Fail-closed removal of ONE redundant trade:<deal> recovery field.

        Redis retention repair (repair/redis-retention-0dfab7b). The ledger is a
        HOT degraded-mode recovery mirror of the authoritative local SQLite
        evidence journal. Once a deal's lifecycle evidence is durably archived in
        SQLite (the caller asserts this via `durable_archive_present`, having
        confirmed the row(s) exist with forwarded=1 AND the deal is terminally
        settled), the per-deal Redis field is pure redundant history and may be
        released to bound unbounded growth.

        Safety invariants (all fail-closed):
          * Removes ONLY the single `trade:<deal>` field — never claim fields
            (opening:/entry_reference:/realization:/cost:/consumer:) which carry
            exactly-once identity, never another deal, never the whole key.
          * NEVER removes anything unless `durable_archive_present` is exactly
            True (archive-before-removal; a missing/unknown/false archive keeps
            the field — partial or failed archival cannot cause deletion).
          * NEVER removes a field still carrying unresolved recovery state: an
            opening without a completed/settled record, a pending partial, or an
            identity quarantine are all retained.
          * Idempotent: absent field -> no-op, returns False.
          * HDEL is targeted; it cannot manufacture or alter settlement, P&L,
            performance delivery, position or protection evidence (it only drops
            a field already proven redundant with durable SQLite).

        Returns True iff a redundant field was released, else False.
        """
        if durable_archive_present is not True:
            return False
        field = self._field(deal_id)
        raw = self.client.hget(self.key, field)
        if raw is None:
            return False
        try:
            entry = json.loads(raw)
        except (ValueError, TypeError):
            return False  # unparseable -> never delete, keep for review
        # Retain anything still operationally unresolved. A terminally settled
        # deal has an opening and no pending-partial / identity quarantine.
        if not isinstance(entry, dict):
            return False
        if entry.get('identity_quarantine'):
            return False
        if (entry.get('partial_exit_pending')):
            return False
        return bool(self.client.hdel(self.key, field))

    def get_projection(self, consumer):
        """Audit-only view. Never return an old aggregate as certified value.

        Reversing external learning effects or rebuilding aggregates is Stage E.
        Raw consumer:* fields are internal archival storage, not a truth API.
        """
        if not isinstance(consumer, str) or not consumer:
            raise EvidenceError('Consumer identity required')
        raw = self.client.hget(self.key, 'consumer:' + consumer)
        if raw is None:
            return None
        value = json.loads(raw)
        if isinstance(value, dict) and value.get('quarantine_version') == 1:
            return value
        return _projection_view(value, 'Legacy projection has no C2c certification')

    def iter_entries(self, count=100):
        """Incremental HSCAN, not a consistent snapshot; retry/dedup by deal ID.

        Redis may repeat fields during a scan. Recovery consumers must therefore
        be idempotent. No unresolved or completed evidence expires here.
        """
        if type(count) is not int or count <= 0:
            raise ValueError("Positive scan count required")
        for _, raw in self.client.hscan_iter(self.key, match="trade:*", count=count):
            yield _entry_view(json.loads(raw))
