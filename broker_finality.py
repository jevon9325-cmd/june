"""Validate cost assertions for consistency, never authenticate posting finality.

Reviewed IG transaction/activity/confirmation schemas establish observations and
execution facts, not an authoritative all-postings-final watermark. The current
capture path adds no such proof. C2c therefore has NO supported certification
adapter. A caller's final=True, statement reference, zero totals or elapsed
waiting period cannot enable economic completion.

The legacy version-2 object remains an audit/consistency input. Its covered_from
and covered_through describe claimed accrual coverage; posted_through describes
claimed posting coverage. Validating these claims does not authenticate them.
"""
from broker_ledger import EvidenceError, _number, _utc


def cost_statement_consistent(position, record, batch, evidence):
    """Check supplied scope/totals only; True is NOT broker finality."""
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


def economic_evidence_complete(position, record, batch, evidence):
    """Fail closed until an independently specified evidence adapter is reviewed.

    No configuration flag or caller dictionary may change this capability.
    Retain validation errors for malformed/mismatched assertions.
    """
    cost_statement_consistent(position, record, batch, evidence)
    return False
