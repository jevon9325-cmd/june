"""SILVER broker-stop closure with missed LS and pending stop acknowledgement."""
import ast, copy, json, time
from datetime import datetime, timezone
from pathlib import Path
import pytest

TREE=ast.parse(Path('june.py').read_text())
NAMES={'_live_broker_close_evidence','_live_poll_position_reconciliation',
       '_live_reconcile_positions','_live_finalize_reconciled_stale_primary'}

def setup(rows=None, activities=None, closed=False):
    deal='DIAAAAR9LQ3UZAR'
    proof={'account_id':'HT2Q8','verified':True}
    pos={'deal_id':deal,'instrument':'SILVER','direction':'long','entry_time':time.time()-1400,
         'stop_sync':{'status':'pending','deal_ref':'AMEND'}, 'ig_size':.04,
         'broker_entry_evidence':{'deal_id':deal,'account_id':'HT2Q8',
                                 'account_evidence':{'order':proof,'confirmation':proof}}}
    state={'open_position':pos,'pyramid_legs':[], 'balance_margin':0,
           'balance_day_start':122.57,'cb_active':False,'risk_epoch':{'baseline':122.57},
           'kill_switch':False,'settled_primary_keys':[]}
    inventory={'positions':[] if rows is None else rows}
    activity={'date':datetime.fromtimestamp(time.time()-300,timezone.utc).isoformat(),
              'status':'ACCEPTED','epic':'SILVER_EPIC','details':{'actions':[
        {'actionType':'POSITION_CLOSED','affectedDealId':deal}]}}
    data={'activities':[activity] if activities is None else activities}
    saves=[]; settlements=[]; requests=[]; evidence=[]
    def get(path,**kw):
        requests.append(path)
        return inventory if path=='/positions' else data
    def settle(p,*a,**kw):
        key=p['deal_id']
        if key not in state['settled_primary_keys']:
            state['settled_primary_keys'].append(key); settlements.append((key,kw))
    ns={'_live':state,'_live_sess':{'account_id':'HT2Q8'},'time':time,
        '_ig_live_get':get,'_ls_deal_closed':lambda d:closed and d==deal,
        '_ls_confirmed_pnl':lambda d:{'profit':.156,'level':6104} if closed else None,
        '_live_settle_primary_exit':settle,
        '_live_save_state':lambda:saves.append(json.dumps(state)),
        '_live_capture_evidence':lambda *a,**kw:evidence.append((a,kw)),
        '_live_capture_active':lambda *a:None,'_live_log':lambda *a:None,
        '_recon_iso':lambda t:str(int(t)),'INSTRUMENTS':{'SILVER':'SILVER_EPIC'},
        '_recon_consecutive_404_with_deposit':0,'_PYRAMID_MAX_LEGS':4}
    for n in TREE.body:
        if isinstance(n,ast.FunctionDef) and n.name in NAMES:
            exec(compile(ast.Module(body=[n],type_ignores=[]),'june.py','exec'),ns)
    return ns, state, inventory, data, saves, settlements, requests, evidence

@pytest.mark.parametrize('closed',[False,True])
def test_confirmed_stop_closure_pending_sync_flat_snapshot_and_restart(closed):
    ns,s,inv,acts,saves,settles,req,evidence=setup(closed=closed)
    original=copy.deepcopy(s)
    ns['_live_poll_position_reconciliation']()
    assert s['open_position'] is None
    assert len(settles)==1
    assert json.loads(saves[-1])['open_position'] is None
    assert {k:s[k] for k in ['cb_active','risk_epoch','balance_day_start','kill_switch']}=={
        k:original[k] for k in ['cb_active','risk_epoch','balance_day_start','kill_switch']}
    # Restart from the real persisted snapshot, duplicate evidence cannot re-settle.
    s.clear();s.update(json.loads(saves[-1]))
    ns['_live_reconcile_positions']()
    assert s['open_position'] is None and len(settles)==1
    assert set(req)<= {'/positions','/history/activity'}
    assert any(x[0][1]=='authoritative_broker_close' for x in evidence)

@pytest.mark.parametrize('activities',[[],[{'status':'ACCEPTED','epic':'SILVER_EPIC','details':{
    'actions':[{'actionType':'POSITION_PARTIALLY_CLOSED','affectedDealId':'DIAAAAR9LQ3UZAR'}]}}]])
def test_transient_rest_absence_margin_zero_and_restart_retain_position(activities):
    ns,s,inv,acts,saves,settles,req,_=setup(activities=activities)
    ns['_live_poll_position_reconciliation']()
    assert s['open_position']['stop_sync']['status']=='pending'
    assert s['orphan_suspected'] and settles==[]
    s.clear();s.update(json.loads(saves[-1]))
    ns['_live_reconcile_positions']()
    assert s['open_position'] and s['orphan_suspected'] and not settles

def test_rest_present_without_stream_close_is_open_and_ack_is_refreshed():
    ns,s,inv,acts,saves,settles,req,_=setup(rows=[{'position':{
        'dealId':'DIAAAAR9LQ3UZAR','size':.04,'stopLevel':6104.11297}}])
    p=s['open_position']; p['fill_price']=6100.1;p['intended_stop_level']=6104.11297
    p['stop_sync'].update(target=6104.11297)
    ns['_live_poll_position_reconciliation']()
    assert s['open_position'] and not settles
    assert '/history/activity' not in req
    assert p['stop_sync']['status']=='acknowledged'
    assert json.loads(saves[-1])['open_position']['stop_sync']['status']=='acknowledged'

def test_explicit_ls_close_wins_over_rest_cache_lag():
    ns,s,inv,acts,saves,settles,req,_=setup(closed=True,rows=[{'position':{
        'dealId':'DIAAAAR9LQ3UZAR','size':.04}}])
    ns['_live_poll_position_reconciliation']()
    assert s['open_position'] is None and len(settles)==1

@pytest.mark.parametrize('bad',['wrong_deal','wrong_epic','rejected','partial','paging','unverified'])
def test_ambiguous_activity_never_clears(bad):
    ns,s,inv,acts,saves,settles,req,_=setup()
    a=acts['activities'][0]
    if bad=='wrong_deal':a['details']['actions'][0]['affectedDealId']='OTHER'
    if bad=='wrong_epic':a['epic']='OTHER'
    if bad=='rejected':a['status']='REJECTED'
    if bad=='partial':a['details']['actions'][0]['actionType']='POSITION_PARTIALLY_CLOSED'
    if bad=='paging':acts['metadata']={'paging':{'next':'untrusted'}}
    if bad=='unverified':s['open_position']['broker_entry_evidence']['account_evidence']={}
    ns['_live_poll_position_reconciliation']()
    assert s['open_position'] and not settles and s['orphan_suspected']

def test_addons_pending_and_review_remain_tracked_after_confirmed_primary():
    ns,s,inv,acts,saves,settles,req,_=setup()
    leg={'deal_id':'ADDON','stop_sync':{'status':'pending'}}
    s.update(pyramid_legs=[leg],pyramid_entry_pending={'deal_ref':'PENDING'},
             manual_review_required=True,rolling_fuel={'amount':1})
    ns['_live_poll_position_reconciliation']()
    assert s['open_position'] is None
    assert s['pyramid_legs']==[leg] and s['pyramid_entry_pending']
    assert s['manual_review_required'] and s['rolling_fuel']=={'amount':1}
    assert len(settles)==1 and set(req)<={'/positions','/history/activity'}

def test_unavailable_inventory_and_poll_failure_retain_tracking():
    ns,s,inv,acts,saves,settles,req,_=setup()
    inv.clear()
    ns['_live_poll_position_reconciliation']()
    assert s['open_position'] and s['orphan_suspected'] and not settles

def test_untracked_broker_exposure_blocks_risk_after_primary_clear():
    ns,s,inv,acts,saves,settles,req,_=setup(rows=[{'position':{'dealId':'UNKNOWN'}}])
    ns['_live_poll_position_reconciliation']()
    assert s['open_position'] is None
    assert s['orphan_suspected'] and s['manual_review_required']
    assert len(settles)==1

def test_duplicate_final_activity_is_deduplicated_but_distinct_closes_are_ambiguous():
    ns,s,inv,acts,saves,settles,req,_=setup()
    acts['activities'].append(copy.deepcopy(acts['activities'][0]))
    assert ns['_live_broker_close_evidence'](s['open_position'],inv)
    acts['activities'][-1]['dealId']='SECOND_CLOSE_EVENT'
    assert ns['_live_broker_close_evidence'](s['open_position'],inv) is None

def test_broker_exception_does_not_escape_or_clear_primary():
    ns,s,inv,acts,saves,settles,req,_=setup()
    def failed(*a,**kw):raise TimeoutError('offline')
    ns['_ig_live_get']=failed
    ns['_live_poll_position_reconciliation']()
    assert s['open_position'] and s['orphan_suspected'] and not settles

def test_holding_refresh_precedes_exits_and_stop_retries():
    fn=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='_run_live_step_observed')
    text=ast.unparse(fn)
    assert text.index('_live_poll_position_reconciliation()')<text.index('_live_check_exit(')
    assert text.index('_live_poll_position_reconciliation()')<text.index('_live_retry_stop_sync(')

@pytest.mark.parametrize('closed',[False,True])
def test_real_canonical_settlement_and_performance_are_exactly_once(closed):
    ns,s,inv,acts,saves,settles,req,_=setup(closed=closed)
    rows=[];perf=[]
    class Redis:
        def lrange(self,key,start,end):return rows[start:] if end==-1 else rows[start:end+1]
        def lpush(self,key,raw):rows.insert(0,raw)
        def ltrim(self,*a):pass
        def expire(self,*a):pass
    ns.update(json=json,_redis=lambda:Redis(),_LIVE_TRADE_HIST_KEY='history',
              _LIVE_TRADE_HIST_CAP=2000,_LIVE_TRADE_HIST_TTL=86400,
              _live_lot_sizes={'SILVER':1},_LIVE_LOT_SIZE_FX=1,_live_equity_cfd=set(),
              _IG_EQUITY_COMMISSION_USD=0,_live_perf_record=lambda *a,**kw:perf.append(kw),
              _live_observe=lambda *a,**kw:None,_live_write_htf_event=lambda *a,**kw:None)
    for n in TREE.body:
        if isinstance(n,ast.FunctionDef) and n.name in {
            '_live_settlement_key','_live_already_settled','_live_mark_settled','_live_settle_primary_exit'}:
            exec(compile(ast.Module(body=[n],type_ignores=[]),'june.py','exec'),ns)
    s['open_position'].update(fill_price=6100.1,notional=244.09)
    original=copy.deepcopy(s['open_position'])
    ns['_live_reconcile_positions']()
    assert s['open_position'] is None and len(rows)==1
    record=json.loads(rows[0])
    assert record['settlement_state']==('CONFIRMED' if closed else 'PROVISIONAL')
    assert record['dollar_pnl']==(.156 if closed else None)
    assert record["exit_authority"] == "UNKNOWN"
    assert len(perf)==0
    # Crash/restart reintroduces an old active snapshot: durable settlement history
    # prevents economic duplication, explicit broker proof clears tracking again.
    s['open_position']=original;s['settled_primary_keys']=[]
    ns['_live_reconcile_positions']()
    assert s['open_position'] is None and len(rows)==1 and len(perf)==0


def test_live_gold_shape_narrow_empty_wider_contains_exact_accepted_close():
    ns,s,inv,acts,saves,settles,req,_=setup()
    calls=[]
    def get(path,**kw):
        if path=='/positions':return inv
        calls.append(kw['params'])
        return {'activities':[]} if len(calls)==1 else acts
    ns['_ig_live_get']=get
    proof=ns['_live_broker_close_evidence'](s['open_position'],inv)
    assert proof and proof['activity']==acts['activities'][0]
    assert len(calls)==2
    assert int(calls[1]['from'])<int(calls[0]['from'])
    assert int(calls[1]['to'])>int(calls[0]['to'])
    ns['_live_reconcile_positions']()
    assert s['open_position'] is None and len(settles)==1


@pytest.mark.parametrize('offset',[3600,-86400])
def test_padded_query_cannot_certify_future_or_pre_entry_close(offset):
    ns,s,inv,acts,saves,settles,req,_=setup()
    acts['activities'][0]['date']=datetime.fromtimestamp(time.time()+offset,timezone.utc).isoformat()
    assert ns['_live_broker_close_evidence'](s['open_position'],inv) is None


def test_padded_history_page_missing_or_truncated_retains_tracking():
    for second in (None,{'activities':[]}, {'activities':[], 'metadata':{'paging':{'next':'more'}}}):
        ns,s,inv,acts,saves,settles,req,_=setup()
        responses=iter([{'activities':[]},second])
        ns['_ig_live_get']=lambda *a,**kw:next(responses)
        assert ns['_live_broker_close_evidence'](s['open_position'],inv) is None
