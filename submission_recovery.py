"""Truthful recovery of a pre-broker persistence abort. Never sends orders.

Unknown/accepted submissions remain gated for broker reconciliation. Absence
from inventory/history alone is never proof of a submission failure.
"""
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import math
from datetime import datetime, timezone, timedelta

def identity(intent):
    return hashlib.sha256(json.dumps(intent,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()

def record_abort(root, intent, error):
    """Only called inside the persistence exception BEFORE the broker call."""
    proof={'source':'pre_broker_persistence_exception','intent':intent,'error_type':type(error).__name__,'at':time.time()}
    with sqlite3.connect(Path(root)/'.submission-recovery.sqlite3') as db:
        db.execute('PRAGMA synchronous=FULL')
        db.execute('CREATE TABLE IF NOT EXISTS aborts (identity TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        db.execute('INSERT OR IGNORE INTO aborts VALUES(?,?)',(identity(intent),json.dumps(proof,sort_keys=True,allow_nan=False)))

def abort_proof(root,intent,account):
    path=Path(root)/'.submission-recovery.sqlite3'
    if path.exists():
        with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True) as db:
            row=db.execute('SELECT payload FROM aborts WHERE identity=?',(identity(intent),)).fetchone()
        if row:
            proof=json.loads(row[0])
            if proof.get('intent')==intent:return proof
    # Legacy immutable telemetry captured by the existing exception/return path.
    path=Path(root)/'campaign_telemetry.sqlite3'
    if not path.exists():return None
    at=intent.get('created_at');risk=intent.get('risk_decision') or {}
    if not isinstance(at,(int,float)) or not isinstance(risk.get('attempt'),(int,float)):return None
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True) as db:
        rows=db.execute('SELECT id,at,kind,payload FROM compounding_events WHERE at BETWEEN ? AND ? ORDER BY at',(at,at+5)).fetchall()
    proposals=[];aborts=[]
    for eid,observed,kind,raw in rows:
        event=json.loads(raw);details=event.get('event_details') or {}
        if event.get('account')!=account or event.get('primary_deal_id')!=intent.get('primary_deal_id'):continue
        if kind.startswith('addon_') and kind!='addon_proposed':return None
        if kind=='addon_proposed' and details.get('attempt')==at:
            proposals.append((eid,observed,raw))
        if (kind=='pyramid_decision' and details.get('attempt')==risk['attempt']
                and details.get('decision')=='reject' and details.get('reason')=='pending_intent_persistence_failed'
                and details.get('instrument')==risk.get('instrument') and details.get('direction')==risk.get('direction')
                and not details.get('submission')):
            aborts.append((eid,observed,raw))
    if len(proposals)!=1 or len(aborts)!=1 or proposals[0][1]>=aborts[0][1]:return None
    return {'source':'immutable_telemetry_pre_broker_return','proposed_id':proposals[0][0],
            'abort_id':aborts[0][0],'payload_sha256':hashlib.sha256((proposals[0][2]+aborts[0][2]).encode()).hexdigest(),
            'intent_identity':identity(intent)}

def resolve(state, proof, positions, orders, history, *, account):
    """Pure assessment. Proof + complete broker contradiction check required."""
    intent=state.get('pyramid_entry_pending')
    if not intent:return state,None
    if not proof or intent.get('deal_ref') or intent.get('deal_id'):return state,None
    if proof.get('source') not in ('immutable_telemetry_pre_broker_return','pre_broker_persistence_exception'):return state,None
    if proof.get('source')=='pre_broker_persistence_exception' and proof.get('intent')!=intent:return state,None
    if proof.get('source')=='immutable_telemetry_pre_broker_return' and proof.get('intent_identity')!=identity(intent):return state,None
    from live_state_integrity import assess,inventory
    candidate=copy.deepcopy(state);candidate.pop('pyramid_entry_pending',None)
    # This repair resolves only an isolated, flat, fully known pre-broker abort.
    if assess(json.dumps(candidate)).kind!='KNOWN_FLAT' or inventory(positions)!=[] or orders!=[]:return state,None
    if not isinstance(history,dict) or not isinstance(history.get('activities'),list):return state,None
    paging=history.get('metadata',{}).get('paging')
    if not isinstance(paging,dict) or paging.get('next') or paging.get('size')!=len(history['activities']):return state,None
    request=intent['order'];at=intent['created_at']
    for event in history['activities']:
        details=event.get('details') or {}
        if (event.get('epic')==request.get('epic') and event.get('type')=='POSITION'
                and details.get('direction')==request.get('direction') and details.get('size')==request.get('size')):
            return state,None # any matching broker result requires explicit identity reconciliation
    receipt={'intent_identity':identity(intent),'primary_deal_id':intent['primary_deal_id'],
             'created_at':at,'disposition':'NEVER_SUBMITTED','source':proof['source'],'proof':proof,
             'account':account,'resolved_at':time.time()}
    candidate['last_submission_recovery']=receipt
    return candidate,receipt

def recovery_checkpoint(state, checkpoint):
    """Only the newer polling clock may differ after a failed Redis write."""
    if checkpoint == state:return state,None
    if not isinstance(checkpoint,dict):return None,None
    if {k:v for k,v in state.items() if k!='pnl_fetched_at'}!={k:v for k,v in checkpoint.items() if k!='pnl_fetched_at'}:return None,None
    old=state.get('pnl_fetched_at');new=checkpoint.get('pnl_fetched_at')
    if any(isinstance(x,bool) or not isinstance(x,(int,float)) or not math.isfinite(x) for x in (old,new)):return None,None
    if not 0<=old<new<=time.time()+5:return None,None
    return checkpoint,{'field':'pnl_fetched_at','redis':old,'checkpoint':new}

def recover_before_startup(client,root,broker_get,recovery_prepare=None):
    """Normal durable writer, no manual state edit or stale checkpoint replay."""
    from live_state_integrity import assess
    from live_state_durability import read_checkpoint,persist_state
    try:raw=client.get('june_live_state')
    except Exception:return # existing startup guard reports ERROR/fails closed
    local=assess(raw)
    if not local.state or not local.state.get('pyramid_entry_pending'):return
    state,clock=recovery_checkpoint(local.state,read_checkpoint(root))
    if state is None:return
    intent=state['pyramid_entry_pending']
    account=(intent.get('risk_decision') or {}).get('account_id')
    # Original primary opening account is retained in immutable telemetry.
    path=Path(root)/'campaign_telemetry.sqlite3'
    if not path.exists():return
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True) as db:
        accounts=db.execute('SELECT account FROM links WHERE deal=?',(intent['primary_deal_id'],)).fetchall()
    if len(accounts)!=1:return
    account=accounts[0][0];proof=abort_proof(root,intent,account)
    if not proof:return
    current=broker_get('/accounts',version='1')
    if not any(x.get('accountId')==account and x.get('preferred') is True for x in (current or {}).get('accounts',[])):return
    at=datetime.fromtimestamp(intent['created_at'],timezone.utc)
    query={'from':(at-timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%S'),
           'to':(at+timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%S'),'detailed':'true','pageSize':500}
    # Existing helper does not support params; query is a literal bounded URL.
    from urllib.parse import urlencode
    history=broker_get('/history/activity?'+urlencode(query),version='3')
    positions=broker_get('/positions',version='2');orders=broker_get('/workingorders',version='2')
    candidate,receipt=resolve(state,proof,(positions or {}).get('positions'),(orders or {}).get('workingOrders'),history,account=account)
    if not receipt:return
    if clock:receipt['checkpoint_polling_clock_reconciliation']=clock
    # Only normal, enabled settlement retention runs here, after all proof.
    # Its FULL SQLite archive precedes any release of settled Redis evidence.
    if recovery_prepare is not None:recovery_prepare()
    if client.get('june_live_state')!=raw:raise RuntimeError('Submission recovery state changed')
    # Receipt is in the full synchronous checkpoint before Redis acknowledgement.
    persist_state(client,root,candidate)
    import logging
    logging.warning('SUBMISSION RECOVERED: %s NEVER_SUBMITTED; immutable pre-broker abort proof',receipt['intent_identity'])
