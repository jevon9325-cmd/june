"""Exit decision provenance, independent of economic settlement and account risk.

IG v3 action/channel semantics: https://labs.ig.com/reference/history-activity.html
No broker requests or persistence here. Callers persist this contract with outcomes.
Missing historical contracts preserve legacy populations; new exits always get one.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math

JUNE_STRATEGY = 'JUNE_STRATEGY'
BROKER_PROTECTION = 'BROKER_PROTECTION'
EXTERNAL_OPERATOR = 'EXTERNAL_OPERATOR'
UNKNOWN = 'UNKNOWN'
ELIGIBLE = {JUNE_STRATEGY, BROKER_PROTECTION}
FIELDS = ('exit_authority', 'exit_authority_schema', 'exit_authority_evidence',
          'exit_authority_conflict', 'operator_intervention',
          'strategy_learning_eligible', 'accounting_eligible', 'close_intents',
          'exit_authority_legs', 'exit_authority_identity', 'exit_authority_epic',
          'entry_time', 'broker_entry_evidence', 'stop_sync',
          'acknowledged_stop_level', 'broker_stop_level')


def identity(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def learning_eligible(record):
    # Deliberate compatibility, not inferred historical reclassification/backfill.
    if not record or ('exit_authority' not in record and 'exit_authority_schema' not in record):
        return True
    return (record.get('exit_authority') in ELIGIBLE
            and record.get('strategy_learning_eligible') is True
            and not record.get('exit_authority_conflict'))


def contract(record):
    return {k: deepcopy(record[k]) for k in FIELDS if k in record}


class PerformanceWriteRejected(RuntimeError):
    """Definitive no-write response; durable delivery may be retried safely."""


def commit_performance(redis, key, previous, stats, observation_id):
    """Atomic stats + permanent delivery identity; bounded windows aren't a ledger.

    CAS avoids overwriting another outcome. A failed commit must leave the durable
    settlement pending for replay. No expiry: an old restart cannot train it again.
    """
    if not observation_id:
        redis.set(key, json.dumps(stats))  # unchanged explicit legacy delivery
        return True
    marker = 'june_perf_delivery:' + identity([key, observation_id])
    from redis.exceptions import OutOfMemoryError
    try:
        result = redis.eval('''
        if redis.call('GET', KEYS[2]) then return 0 end
        if (redis.call('GET', KEYS[1]) or '') ~= ARGV[1] then return -1 end
        local written = redis.pcall('MSET', KEYS[1], ARGV[2], KEYS[2], ARGV[3])
        if type(written) == 'table' and written.err then
            if string.find(written.err, 'OOM', 1, true) then return -2 end
            return redis.error_reply(written.err)
        end
        return 1
    ''', 2, key, marker, previous or '', json.dumps(stats), observation_id)
    except OutOfMemoryError as exc:
        # Either EVAL was refused or the sole atomic MSET was refused. No
        # preceding mutation exists. Transport/lost-ack errors remain uncertain.
        raise PerformanceWriteRejected('Performance write rejected by Redis OOM; replay required') from exc
    if result == -2:
        raise PerformanceWriteRejected('Performance write rejected by Redis OOM; replay required')
    if result == -1:
        raise RuntimeError('Performance history changed concurrently; replay required')
    return result == 1


def capture(position, event, details):
    """Retain exact June order/response/confirm linkage before tracking is cleared."""
    if event == 'close_intent':
        order = details.get('order') or {}
        if order.get('dealId') != position.get('deal_id'):
            return
        intent = {'id': identity([position.get('deal_id'), order, details.get('expected_exit_reason'),
                                  details.get('local_request_time')]),
                  'deal_id': position.get('deal_id'), 'order': deepcopy(order),
                  'reason': details.get('expected_exit_reason'),
                  'at': details.get('local_request_time')}
        intents = position.setdefault('close_intents', [])
        if not any(i['id'] == intent['id'] for i in intents):
            intents.append(intent)
    elif event in ('close_response', 'close_confirmation_observed'):
        intents = position.get('close_intents') or []
        if not intents:
            return
        ref = (details.get('response') or {}).get('dealReference')
        intent = intents[-1]
        if ref:
            if intent.get('deal_reference') not in (None, ref):
                intent['conflict'] = True
                return
            intent['deal_reference'] = ref
        if event == 'close_confirmation_observed' and ref:
            confirm = details.get('confirmation') or {}
            # A different returned reference must never certify this submission.
            if confirm.get('dealReference') not in (None, ref):
                intent['conflict'] = True
                return
            intent['confirmation'] = deepcopy(confirm)
    elif event == 'authoritative_broker_close' and details.get('activity'):
        classify(position, [details['activity']])


def _accepted_intent(intent, deal, partial=False):
    confirm = intent.get('confirmation') or {}
    statuses = {'PARTIALLY_CLOSED'} if partial else {'FULLY_CLOSED'}
    return (intent.get('deal_id') == deal and intent.get('deal_reference')
            and not intent.get('conflict') and confirm.get('dealStatus') == 'ACCEPTED'
            and any(a.get('dealId') == deal and a.get('status') in statuses
                    for a in confirm.get('affectedDeals') or [] if isinstance(a, dict)))


def _valid_activity(record, activity):
    deal = record.get('deal_id')
    if activity.get('status') != 'ACCEPTED' or not deal:
        return []
    epic = record.get('exit_authority_epic')
    if not epic or activity.get('epic') != epic:
        return []
    opening = record.get('broker_entry_evidence') or {}
    account = opening.get('account_id')
    proofs = opening.get('account_evidence') or {}
    if (opening.get('deal_id') != deal or not account
            or not all((proofs.get(k) or {}).get('verified') is True
                       and (proofs.get(k) or {}).get('account_id') == account
                       for k in ('order', 'confirmation'))):
        return []
    try:
        at = datetime.fromisoformat(activity['date'].replace('Z', '+00:00'))
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        start = float(record['entry_time'])
        if not math.isfinite(start) or at.timestamp() < start - 60:
            return []
        # Economic reconciliation can run long after exit, but cannot use future events.
        if at.timestamp() > datetime.now(timezone.utc).timestamp():
            return []
    except (KeyError, TypeError, ValueError, AttributeError):
        return []
    return [a.get('actionType') for a in (activity.get('details') or {}).get('actions') or []
            if isinstance(a, dict) and a.get('affectedDealId') == deal]


def classify(record, activities=(), *, partial=False):
    """Monotonic evidence merge. Conflicting attribution is sticky UNKNOWN.

    SYSTEM alone and price proximity never establish protection. Require explicit
    STOP_ORDER_FILLED against the exact deal plus previously acknowledged stop.
    MOBILE/WEB/DEALER close actions establish external intervention; API alone does not.
    """
    deal = record.get('deal_id')
    observations = {}
    intents = record.get('close_intents') or []
    for intent in intents:
        if _accepted_intent(intent, deal, partial):
            observations['ref:' + intent['deal_reference']] = {
                'authority': JUNE_STRATEGY, 'source': 'June.order+accepted.affectedDeals',
                'broker_reference': intent['deal_reference'], 'intent_id': intent['id']}
    for activity in activities or []:
        if not isinstance(activity, dict):
            continue
        actions = _valid_activity(record, activity)
        closing = 'POSITION_PARTIALLY_CLOSED' if partial else 'POSITION_CLOSED'
        if closing not in actions:
            continue
        ref = (activity.get('details') or {}).get('dealReference')
        key = 'ref:' + ref if ref else 'activity:' + identity(activity)
        matched = next((i for i in intents if ref and i.get('deal_reference') == ref
                        and i.get('deal_id') == deal and not i.get('conflict')), None)
        channel = activity.get('channel')
        authority = UNKNOWN
        if channel in ('MOBILE', 'WEB', 'DEALER'):
            authority = EXTERNAL_OPERATOR
        elif matched and channel == 'PUBLIC_WEB_API':
            authority = JUNE_STRATEGY
        elif channel == 'SYSTEM' and 'STOP_ORDER_FILLED' in actions:
            sync = record.get('stop_sync') or {}
            stop = record.get('acknowledged_stop_level')
            applied = (activity.get('details') or {}).get('stopLevel')
            opening = record.get('broker_entry_evidence') or {}
            accepted = opening.get('accepted_confirmation') or {}
            submitted = opening.get('submitted_order') or {}
            acknowledged = (sync.get('status') == 'acknowledged' and sync.get('deal_id') == deal)
            initial = (accepted.get('dealStatus') == 'ACCEPTED' and accepted.get('dealId') == deal
                       and submitted.get('stopLevel') is not None
                       and accepted.get('stopLevel') == submitted.get('stopLevel'))
            if initial and stop is None:
                stop = accepted.get('stopLevel')
            try:
                protection_match = (stop is not None and applied is not None
                                    and math.isfinite(float(stop)) and float(stop) == float(applied))
            except (ValueError, TypeError):
                protection_match = False
            if (acknowledged or initial) and protection_match:
                authority = BROKER_PROTECTION
        evidence = {'authority': authority, 'source': 'IG.activity.' + str(channel),
                    'broker_reference': ref, 'broker_close_id': activity.get('dealId'),
                    'activity_identity': identity(activity),
                    'intent_id': matched['id'] if matched else None}
        if matched and (matched.get('confirmation') or {}).get('dealStatus') == 'REJECTED':
            evidence['conflict'] = True
        previous = observations.get(key)
        if previous and previous['authority'] != authority and authority != UNKNOWN:
            evidence['conflict'] = True
        observations[key] = evidence
    prior = {e['identity']: e for e in record.get('exit_authority_evidence') or []}
    for key, evidence in observations.items():
        evidence['identity'] = key
        old = prior.get(key)
        if old and old['authority'] != UNKNOWN:
            if evidence['authority'] == UNKNOWN:
                continue  # missing/weak later evidence never erases affirmative proof
            if old['authority'] != evidence['authority']:
                evidence['conflict'] = True
                evidence['prior_authority'] = old['authority']
        prior[key] = evidence
    evidence = [prior[k] for k in sorted(prior)]
    authorities = {e['authority'] for e in evidence if e['authority'] != UNKNOWN}
    # Different partial and final decisions are retained, but a mixed campaign
    # cannot answer how June alone performed. Unknown partials also veto training.
    legs = record.setdefault('exit_authority_legs', [])
    if not partial and not legs and (record.get('partial_exit_done') or record.get('partial_dollar_pnl')):
        legs.append({'identity': identity([deal, 'legacy_partial_authority_unretained']),
                     'authority': UNKNOWN, 'source': 'partial_authority_unretained'})
    if not partial:
        for activity in activities or []:
            if isinstance(activity, dict) and 'POSITION_PARTIALLY_CLOSED' in _valid_activity(record, activity):
                leg = deepcopy(record)
                leg.pop('exit_authority_evidence', None)
                leg.pop('exit_authority_conflict', None)
                leg.pop('exit_authority_legs', None)
                classify(leg, [activity], partial=True)
                key = identity(['activity_partial', (activity.get('details') or {}).get('dealReference')
                                or identity(activity)])
                if not any(l['identity'] == key for l in legs):
                    legs.append({'identity': key, 'authority': leg['exit_authority'],
                                 'evidence': leg['exit_authority_evidence']})
    conflict = bool(record.get('exit_authority_conflict') or any(e.get('conflict') for e in evidence)
                    or any(i.get('conflict') for i in intents)
                    or len(authorities) > 1
                    or (authorities and any(e['authority'] == UNKNOWN for e in evidence)))
    authority = next(iter(authorities)) if len(authorities) == 1 and not conflict else UNKNOWN
    eligible = authority in ELIGIBLE and all(l.get('authority') in ELIGIBLE for l in legs)
    record.update(exit_authority_schema=1, exit_authority=authority,
                  exit_authority_evidence=evidence, exit_authority_conflict=conflict,
                  exit_authority_identity='deal:' + str(deal),
                  operator_intervention=(record.get('operator_intervention') is True
                                         or EXTERNAL_OPERATOR in authorities
                                         or any(l.get('authority') == EXTERNAL_OPERATOR for l in legs)),
                  strategy_learning_eligible=eligible, accounting_eligible=True)
    return record


def record_partial(position):
    leg = deepcopy(position)
    leg.pop('exit_authority_evidence', None)
    leg.pop('exit_authority_legs', None)
    leg.pop('exit_authority_conflict', None)
    classify(leg, partial=True)
    evidence = leg['exit_authority_evidence']
    key = identity(evidence) if evidence else identity(position.get('partial_exit_pending') or {})
    legs = position.setdefault('exit_authority_legs', [])
    if not any(l['identity'] == key for l in legs):
        legs.append({'identity': key, 'authority': leg['exit_authority'], 'evidence': evidence})


def refresh_confirmed_authorities(raw_records, redis, history_key, get, *, account, now, observe):
    """Bounded provenance-only retry for settled LS/transaction-only exits.

    Economic confirmation does not imply decision authority. Resolve new-contract
    UNKNOWN records independently, never reclassify legacy rows or rewrite P&L.
    A later settlement replay supplies eligible performance; this function doesn't.
    """
    attempted = 0
    for idx, raw in enumerate(list(raw_records)):
        rec = {}
        try:
            rec = json.loads(raw)
            if (rec.get('exit_authority_schema') != 1 or rec.get('exit_authority') != UNKNOWN
                    or rec.get('exit_authority_conflict') or rec.get('settlement_state') != 'CONFIRMED'
                    or rec.get('perf_fed') or not account
                    or (rec.get('broker_entry_evidence') or {}).get('account_id') != account):
                continue
            if now - rec.get('authority_last_attempt', 0) < 300:
                continue
            exit_at = float(rec['exit_epoch'])
            start = max(float(rec['entry_time']) - 24 * 3600, exit_at - 48 * 3600)
            end = min(now + 24 * 3600, exit_at + 24 * 3600)
            if end <= start or attempted >= 2:
                continue
            attempted += 1
            rec['authority_last_attempt'] = now
            redis.lset(history_key, idx, json.dumps(rec))
            def iso(value):
                return datetime.fromtimestamp(value, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
            response = get('/history/activity', params={'from': iso(start), 'to': iso(end),
                           'detailed': 'true', 'pageSize': 500}, version='3')
            activities = response.get('activities') if isinstance(response, dict) else None
            if not isinstance(activities, list) or (((response.get('metadata') or {}).get('paging') or {}).get('next')):
                raw_records[idx] = json.dumps(rec)
                continue
            before = contract(rec)
            classify(rec, activities)
            # Validate the durable row identity before replacing the same index.
            latest = json.loads(redis.lindex(history_key, idx))
            if latest.get('deal_id') != rec.get('deal_id'):
                continue
            latest.update(contract(rec))
            latest['authority_last_attempt'] = now
            raw_records[idx] = json.dumps(latest)
            redis.lset(history_key, idx, raw_records[idx])
            if contract(latest) != before:
                observe('exit_authority_resolved', None, latest, contract(latest))
        except Exception as exc:
            observe('exit_authority_reconcile_deferred', None, rec,
                    {'reason': type(exc).__name__})
            continue  # unresolved, no certification or economic modification
