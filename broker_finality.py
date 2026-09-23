"""Validate externally verified broker cost statements; never infer finality.

This is an evidence-input contract, not a statement fetcher or authenticator.
An adapter must retain and verify the referenced broker documents before supplying
this object. No current runtime adapter produces it. History timestamps, elapsed
time and empty fee lists are not substitutes for broker posting finality.

covered_from/covered_through describe the economic accrual interval. A separate
posting_finality attestation states that all postings for that interval are final,
including delayed commissions/financing. posted_through is the posting horizon
that must be included in the fetched history. Component totals are signed USD;
explicit final zero totals are necessary to establish a zero-cost position.
"""
from broker_ledger import EvidenceError, _number, _utc


def economic_evidence_complete(position, record, batch, evidence):
    if evidence is None:
        return False
    if not isinstance(evidence, dict) or any(
            not evidence.get(k) for k in ('source', 'reference', 'covered_through')):
        raise EvidenceError('Explicit cost coverage evidence required')
    # Legacy three-field coverage is fetch coverage, not economic finality.
    if evidence.get('schema_version') != 2:
        return False
    if any(evidence.get(k) != position[k] for k in ('account_id', 'deal_id', 'currency')):
        raise EvidenceError('Cost statement belongs to another position')
    if not record['realizations'] or _number(record['remaining_quantity']) != 0:
        return False
    start = _utc(evidence.get('covered_from'))
    end = _utc(evidence['covered_through'])
    last_exit = max(_utc(part['exit_utc']) for part in record['realizations'])
    if start > _utc(position['opened_utc']) or end < last_exit or start > end:
        raise EvidenceError('Cost coverage does not span the full position lifecycle')
    finality = evidence.get('posting_finality')
    if not isinstance(finality, dict) or finality.get('final') is not True:
        return False
    if any(not finality.get(k) for k in ('source', 'reference', 'posted_through')):
        return False
    posting_end = _utc(finality['posted_through'])
    if posting_end < end or _utc(batch['to']) < posting_end or _utc(batch['from']) > start:
        return False
    components = evidence.get('components', {})
    if not isinstance(components, dict):
        return False
    totals = {}
    for kind in ('commission', 'financing', 'other'):
        component = components.get(kind)
        if (not isinstance(component, dict) or component.get('final') is not True
                or any(not component.get(k) for k in ('source', 'reference'))):
            return False
        totals[kind] = _number(component.get('total'))
    # There is no verified position-level financing adapter in C2c. Never let
    # a financing charge cancel an unrelated credit and masquerade as zero.
    return (totals['commission'] == _number(record['commissions'])
            and totals['financing'] == 0
            and totals['other'] == _number(record['other_costs']))
