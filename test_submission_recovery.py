import copy,json,sqlite3
from pathlib import Path
from unittest.mock import Mock,patch
import pytest
from submission_recovery import identity,resolve,record_abort,abort_proof,recover_before_startup,recovery_checkpoint
from live_state_durability import checkpoint,read_checkpoint,persist_state
from live_state_integrity import assess
from test_winner_accounting import harness

@pytest.fixture
def evidence():
    return json.loads(Path(__file__).with_name('fixtures').joinpath('natgas_prebroker_abort_20261005.json').read_text())

def install_telemetry(root,evidence):
    with sqlite3.connect(root/'campaign_telemetry.sqlite3') as db:
        db.execute('CREATE TABLE compounding_events(id TEXT PRIMARY KEY,at REAL,kind TEXT,payload TEXT)')
        db.execute('CREATE TABLE links(account TEXT,deal TEXT)')
        db.execute('INSERT INTO links VALUES(?,?)',('HT2Q8',evidence['state']['pyramid_entry_pending']['primary_deal_id']))
        for n,e in enumerate(evidence['telemetry']):
            db.execute('INSERT INTO compounding_events VALUES(?,?,?,?)',(str(n),e['at'],e['kind'],json.dumps(e['payload'])))

def history(events=()):return {'activities':list(events),'metadata':{'paging':{'size':len(events),'next':None}}}

def test_exact_natgas_immutable_proof_and_idempotence(tmp_path,evidence):
    install_telemetry(tmp_path,evidence);state=evidence['state'];intent=state['pyramid_entry_pending']
    proof=abort_proof(tmp_path,intent,'HT2Q8');assert proof
    new,receipt=resolve(state,proof,[],[],history(),account='HT2Q8')
    assert receipt['disposition']=='NEVER_SUBMITTED' and 'pyramid_entry_pending' not in new
    assert assess(json.dumps(new)).kind=='KNOWN_FLAT'
    assert new['trade_history']==state['trade_history'] and new['pnl_seen_refs']==state['pnl_seen_refs']
    assert resolve(new,proof,[],[],history(),account='HT2Q8')==(new,None)
    assert 'pyramid_entry_pending' in state

@pytest.mark.parametrize('disposition',['accepted_open','accepted_closed','rejected','lost_ack','flat_no_history','flat_acceptance','flat_rejection','duplicate'])
def test_never_guesses_or_forgets_broker_outcomes(tmp_path,evidence,disposition):
    state=evidence['state'];intent=state['pyramid_entry_pending'];install_telemetry(tmp_path,evidence)
    proof=abort_proof(tmp_path,intent,'HT2Q8')
    event={'epic':intent['order']['epic'],'type':'POSITION','status':'ACCEPTED','details':{'size':.01,'direction':'BUY'}}
    if disposition in ('rejected','flat_rejection'):event['status']='REJECTED'
    positions=[{'position':{'dealId':'actual-addon','size':.01,'direction':'BUY'}}] if disposition=='accepted_open' else []
    h=history([event,event] if disposition=='duplicate' else [event])
    if disposition=='flat_no_history':proof=None;h=history()
    if disposition=='lost_ack':proof=None
    result,receipt=resolve(state,proof,positions,[],h,account='HT2Q8')
    assert result is state and receipt is None and result['pyramid_entry_pending']==intent

def test_unknown_reference_not_cleared(tmp_path,evidence):
    state=evidence['state'];state['pyramid_entry_pending']['deal_ref']='actual-reference'
    install_telemetry(tmp_path,evidence);proof=abort_proof(tmp_path,state['pyramid_entry_pending'],'HT2Q8')
    assert resolve(state,proof,[],[],history(),account='HT2Q8')==(state,None)

@pytest.mark.parametrize('fault',['missing','changed','unpaged','wrong_account','changed_primary'])
def test_proof_and_complete_history_required(tmp_path,evidence,fault):
    install_telemetry(tmp_path,evidence);state=evidence['state'];proof=abort_proof(tmp_path,state['pyramid_entry_pending'],'HT2Q8');h=history()
    if fault=='missing':proof=None
    if fault=='changed':state['pyramid_entry_pending']['created_at']+=1
    if fault=='unpaged':h['metadata']['paging']['next']='next-page'
    if fault=='wrong_account':assert abort_proof(tmp_path,state['pyramid_entry_pending'],'wrong') is None;return
    if fault=='changed_primary':state['pyramid_entry_pending']['primary_deal_id']='other'
    assert resolve(state,proof,[],[],h,account='HT2Q8')==(state,None)

def test_future_prebroker_failure_durable_before_release(tmp_path,evidence):
    intent=evidence['state']['pyramid_entry_pending'];record_abort(tmp_path,intent,OSError('OOM'))
    assert abort_proof(tmp_path,intent,'HT2Q8')['source']=='pre_broker_persistence_exception'

@pytest.mark.parametrize('redis_failure',[False,True])
def test_restart_checkpoint_recovery_and_redis_fault(tmp_path,evidence,redis_failure):
    import fakeredis
    install_telemetry(tmp_path,evidence);state=evidence['state'];r=fakeredis.FakeRedis(decode_responses=True)
    persist_state(r,tmp_path,state)
    broker=Mock(side_effect=lambda path,version: history() if path.startswith('/history/') else
                {'accounts':[{'accountId':'HT2Q8','preferred':True}]} if path=='/accounts' else
                {'positions':[]} if path=='/positions' else {'workingOrders':[]})
    if redis_failure:
        with patch.object(r,'set',side_effect=OSError('OOM')):
            with pytest.raises(OSError):recover_before_startup(r,tmp_path,broker)
        assert read_checkpoint(tmp_path).get('pyramid_entry_pending') is None
        assert json.loads(r.get('june_live_state'))['pyramid_entry_pending']==state['pyramid_entry_pending']
        # Divergence blocks replay; no stale authoritative image substituted.
        recover_before_startup(r,tmp_path,broker)
        assert json.loads(r.get('june_live_state'))['pyramid_entry_pending']==state['pyramid_entry_pending']
    else:
        recover_before_startup(r,tmp_path,broker)
        new=json.loads(r.get('june_live_state'));assert new==read_checkpoint(tmp_path)
        assert new.get('pyramid_entry_pending') is None
        before=broker.call_count;recover_before_startup(r,tmp_path,broker);assert broker.call_count==before

def test_actual_prebroker_exception_never_posts_and_releases_with_receipt(tmp_path):
    ns=harness();ns['__file__']=str(tmp_path/'june.py')
    ns['_live_persist_state']=Mock(side_effect=OSError('OOM'))
    ns['_live_add_pyramid_leg']({'GOLD':{'price':100.}}, {})
    ns['_ig_live_post'].assert_not_called()
    assert not ns['_live'].get('pyramid_entry_pending')
    assert (tmp_path/'.submission-recovery.sqlite3').exists()

def test_abort_archive_failure_retains_barrier(tmp_path):
    ns=harness();ns['__file__']=str(tmp_path/'june.py');ns['_live_persist_state']=Mock(side_effect=OSError('OOM'))
    with patch('submission_recovery.record_abort',side_effect=OSError('disk failed')):
        ns['_live_add_pyramid_leg']({'GOLD':{'price':100.}}, {})
    ns['_ig_live_post'].assert_not_called();assert ns['_live']['pyramid_entry_pending']

@pytest.mark.parametrize('fault',['exposure','baseline','missing','added_null','old_clock','future','nan','boolean'])
def test_only_newer_polling_clock_can_be_reconciled(evidence,fault):
    state=evidence['state'];new=copy.deepcopy(state);new['pnl_fetched_at']=state['pnl_fetched_at']+1
    if fault=='exposure':new['open_position']={'deal_id':'unknown'}
    if fault=='baseline':new['balance_day_start']+=1
    if fault=='missing':new.pop('trade_history')
    if fault=='added_null':new['unknown_field']=None
    if fault=='old_clock':new['pnl_fetched_at']-=2
    if fault=='future':new['pnl_fetched_at']=10**12
    if fault=='nan':new['pnl_fetched_at']=float('nan')
    if fault=='boolean':new['pnl_fetched_at']=True
    assert recovery_checkpoint(state,new)==(None,None)

def test_timestamp_only_recovery_prepares_retention_before_oom_write(tmp_path,evidence):
    import fakeredis
    install_telemetry(tmp_path,evidence);state=evidence['state'];r=fakeredis.FakeRedis(decode_responses=True)
    persist_state(r,tmp_path,state);new=copy.deepcopy(state);new['pnl_fetched_at']+=1;checkpoint(tmp_path,new)
    broker=Mock(side_effect=lambda path,version: history() if path.startswith('/history/') else
                {'accounts':[{'accountId':'HT2Q8','preferred':True}]} if path=='/accounts' else
                {'positions':[]} if path=='/positions' else {'workingOrders':[]})
    prepared=[];original=r.set
    def prepare():
        assert broker.call_count==4
        assert json.loads(r.get('june_live_state'))==state
        prepared.append(True)
    def guarded_set(*args,**kwargs):
        if not prepared:raise OSError('OOM')
        return original(*args,**kwargs)
    with patch.object(r,'set',side_effect=guarded_set):
        recover_before_startup(r,tmp_path,broker,recovery_prepare=prepare)
    recovered=json.loads(r.get('june_live_state'));assert recovered==read_checkpoint(tmp_path)
    assert recovered['pnl_fetched_at']==new['pnl_fetched_at']
    assert recovered['last_submission_recovery']['checkpoint_polling_clock_reconciliation']['redis']==state['pnl_fetched_at']
    before=broker.call_count;recover_before_startup(r,tmp_path,broker,recovery_prepare=lambda:pytest.fail('duplicate prepare'))
    assert broker.call_count==before

def test_unproven_recovery_never_runs_retention(tmp_path,evidence):
    import fakeredis
    r=fakeredis.FakeRedis(decode_responses=True);persist_state(r,tmp_path,evidence['state'])
    recover_before_startup(r,tmp_path,Mock(side_effect=AssertionError('unexpected broker')),recovery_prepare=lambda:pytest.fail('unproven prepare'))
