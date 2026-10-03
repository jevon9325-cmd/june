"""Decision/evidence views only; never infer unreached gates or broker truth."""
import json
import math
import time

_QUOTES = {}  # private observation-only cache; never a signal/Redis contract


def remember_quote(epic, quote, source, metadata=None):
    if not isinstance(quote, dict):
        return
    if epic not in _QUOTES and len(_QUOTES) >= 64:
        _QUOTES.pop(next(iter(_QUOTES)))
    _QUOTES[epic] = dict(epic=epic, received_at=time.time(), source=source,
                        values={k:quote.get(k) for k in ('bid','offer','mid','spread')},
                        broker_metadata={k:(metadata or {}).get(k) for k in
                            ('updateTime','updateTimeUTC','scalingFactor','decimalPlacesFactor','marketStatus')})


def quote_view(epic):
    return json.loads(json.dumps(_QUOTES.get(epic)))


def entry_receipt(position):
    value=(position.get('broker_entry_evidence') or {}).get('accepted_confirmation') or {}
    return {k:value.get(k) for k in ('date','dealStatus','dealReference','dealId','affectedDeals',
                                   'epic','direction','level','size','stopLevel','limitLevel','profitCurrency')}


CRITICAL = {'entry','pyramid_threshold_first_reached','pyramid_decision','addon_proposed',
            'addon_submission','addon_submission_response','addon_confirmation','addon_accepted','addon_opened',
            'addon_rejected','addon_outcome_unknown','addon_closed','leg_closed','primary_closed',
            'primary_settled','settlement_confirmed','exit_authority_resolved','exit_requested',
            'final_campaign_close','partial_tp_requested','partial_tp_confirmed',
            'allocation_released_confirmed_partial','position_absence_observed',
            'observation_gap','unpriced_leg_disappearance','tracking_empty_outcome_unresolved'}


def critical(kind):
    return kind in CRITICAL or kind.startswith(('protection_','rolling_','v1_gen2'))


GATE_SEQUENCE = ("unresolved_position_or_submission", "campaign_leg_cap", "primary_closed",
                 "new_trade_authority", "session_performance", "market_pause", "spread_atr",
                 "trade_guard", "duplicate_quote", "allocation", "protection", "f50",
                 "mindeal_rounding_margin", "spread_cost_inventory", "equity_commission",
                 "established_protection", "pending_intent_persistence", "submission")


def decision_view(details):
    reason = details.get("reason")
    result = details.get("decision")
    gates = {name: "UNOBSERVED" for name in GATE_SEQUENCE}
    if "protection_state" in details:
        gates["protection"] = details["protection_state"]
    if "allocation" in details:
        gates["allocation"] = {k: details["allocation"].get(k) for k in
                               ("consumed_allocation", "remaining_allocation", "pending_reservation")}
    if "f50_legal_ig" in details:
        gates["f50"] = {k: details.get(k) for k in
                        ("f50_protected_before", "f50_expendable", "f50_legal_ig", "f50_reason")}
    if "continuation_economics" in details:
        gates["spread_cost_inventory"] = details["continuation_economics"]
    return dict(gate_sequence=list(GATE_SEQUENCE), first_binding_gate=reason if result != "approve" else None,
                known_gate_evidence=gates, decision=result, reason=reason,
                unobserved_gates_are_not_passes=True)


def protection_values(legs, unit):
    values=[]
    for leg in legs:
        try:
            fill, q = float(leg["fill_price"]), float(leg["ig_size"])
            sign = 1 if leg["direction"] == "long" else -1
            # Software/aggregate floors are NEVER used to estimate broker protection.
            ack = leg.get("acknowledged_stop_level") or leg.get("broker_stop_level")
            gross = sign * (float(ack)-fill) / fill * q * unit(leg["instrument"], fill) if ack else None
            if gross is not None and not math.isfinite(gross):gross=None
            values.append(dict(deal_id=leg.get("deal_id"), broker_stop=ack,
                               broker_gross_at_stop=gross, source="retained_broker_stop_fields",
                               costs_and_slippage_certified=False))
        except (ValueError,TypeError,KeyError,ZeroDivisionError):
            values.append(dict(deal_id=leg.get("deal_id"), broker_gross_at_stop=None, source="missing"))
    return values


def reconstruct(db, campaign, *, coverage=None):
    """Replay retained decision evidence, not hypothetical ticks or executions.

    JSON events are sufficient; no journal parsing. Missing evidence is explicit.
    A certificate describes this decision chain, not continuous-path completeness.
    """
    rows=db.execute("SELECT id,at,kind,payload FROM compounding_events WHERE campaign=? ORDER BY at,rowid",(campaign,)).fetchall()
    events=[dict(event_id=i,at=at,kind=k,payload=json.loads(p)) for i,at,k,p in rows]
    kinds={e['kind'] for e in events}
    required={'entry','pyramid_threshold_first_reached','protection_geometry','protection_request',
              'pyramid_decision','addon_proposed','addon_submission','addon_opened','final_campaign_close'}
    missing=sorted(required-kinds)
    evidence=[e['payload'].get('event_details',e['payload']) for e in events]
    from winner_protection import level
    if not any(p.get('protection_status') in {'BROKER_ACKNOWLEDGED','BROKER_SNAPSHOT_CONFIRMED'}
               and level(p.get('acknowledged_stop') or p.get('broker_stop_level')) is not None
               and (p.get('broker_deal_id') or p.get('stop_sync',{}).get('deal_id')) for p in evidence):
        missing.append('positive_broker_acknowledgement_evidence')
    decisions=[e for e in events if e['kind']=='pyramid_decision']
    approved=[e['payload'].get('event_details',{}) for e in decisions
              if e['payload'].get('event_details',{}).get('decision')=='approve']
    if not any(p.get('f50_protected_before') is not None and p.get('allocation',{}).get('remaining_allocation') is not None
               and p.get('f50_legal_ig') is not None and p.get('continuation_economics',{}).get('allowed') is True
               and p.get('broker_inventory_verified') is True for p in approved):
        missing.append('funding_allocation_mindeal_cost_inventory_evidence')
    confirmations=[e['payload'].get('event_details',{}).get('confirmation')
                   for e in events if e['kind']=='addon_confirmation']
    accepted=[p for p in confirmations if isinstance(p,dict) and p.get('dealStatus')=='ACCEPTED'
              and p.get('dealId') and p.get('dealReference') and level(p.get('level')) and level(p.get('size'))]
    opened_ids={l.get('deal_id') for e in events if e['kind'] in {'addon_opened','v1_gen2_opened'}
                for l in e['payload'].get('legs',[]) if l.get('leg_generation',0)>0}
    if not accepted or not opened_ids or not opened_ids.issubset({p['dealId'] for p in accepted}):
        missing.append('accepted_addon_confirmation')
    if not kinds.intersection({'primary_settled','settlement_confirmed'}):missing.append('primary_settlement')
    if not kinds.intersection({'addon_closed','leg_closed'}):missing.append('addon_close')
    if kinds.intersection({'observation_gap','unpriced_leg_disappearance'}):missing.append('observed_evidence_gap')
    finals=[e['payload'] for e in events if e['kind']=='final_campaign_close']
    if any(p.get('realized_incomplete') for p in finals):missing.append('incomplete_realized_outcomes')
    result=dict(campaign_id=campaign, events=events, missing=missing,
                evidence_scope="recorded_runtime_decision_chain_not_continuous_market_path",
                decision_chain_complete=not missing and coverage is not None and coverage.get('gap_count') == 0,
                recording_coverage=coverage,
                qualification=next((e['at'] for e in events if e['kind']=='pyramid_threshold_first_reached'),None),
                decisions=[dict(at=e['at'],**decision_view(e['payload'].get('event_details',e['payload']))) for e in decisions],
                generations=sorted({l.get('leg_generation',0) for e in events for l in e['payload'].get('legs',[])}))
    return result


if __name__ == '__main__':
    import argparse
    import pathlib
    import sqlite3
    parser=argparse.ArgumentParser(description='Read a retained compounding decision chain; never modify SQLite.')
    parser.add_argument('database')
    parser.add_argument('campaign')
    args=parser.parse_args()
    path=pathlib.Path(args.database).resolve()
    marker=pathlib.Path(str(path)+'.coverage.json')
    coverage=json.loads(marker.read_text(encoding='utf-8')) if marker.exists() else None
    with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True) as db:
        print(json.dumps(reconstruct(db,args.campaign,coverage=coverage),indent=2,sort_keys=True))
