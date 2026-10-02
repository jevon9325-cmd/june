"""Offline affirmative attribution and accounting/learning separation regressions."""
import ast
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time
from unittest.mock import Mock
import pytest

from exit_authority import *
from test_broker_identity import execute, function, TREE
from test_live_state_reconciliation import setup
from test_build4cc_provenance import load_helpers
from test_settlement_integrity import _drive, rec, tx
from test_live_accounting import close_harness
from campaign_telemetry import Store


def position(deal='D1', sym='GOLD', epic='GOLD_EPIC'):
    proof={'verified':True,'account_id':'HT2Q8'}
    return dict(deal_id=deal,instrument=sym,direction='short',entry_time=time.time()-3600,
                exit_authority_epic=epic, broker_entry_evidence={
                    'deal_id':deal,'account_id':'HT2Q8',
                    'account_evidence':{'order':proof,'confirmation':proof}})


def activity(p,channel='MOBILE',ref='EXTERNAL',partial=False,stop=False):
    actions=[{'affectedDealId':p['deal_id'],'actionType':
              'POSITION_PARTIALLY_CLOSED' if partial else 'POSITION_CLOSED'}]
    if stop:actions.append({'affectedDealId':p['deal_id'],'actionType':'STOP_ORDER_FILLED'})
    return dict(status='ACCEPTED',channel=channel,epic=p['exit_authority_epic'],
                date=datetime.fromtimestamp(time.time()-60,timezone.utc).isoformat(),
                dealId='CLOSE_'+ref,details={'dealReference':ref,'actions':actions,
                                           'level':99.,'size':.06})


def intent(p,ref='JUNE',partial=False,accepted=True):
    capture(p,'close_intent',{'order':{'dealId':p['deal_id'],'size':.03 if partial else .06},
                             'expected_exit_reason':'partial_take_profit' if partial else 'reversal',
                             'local_request_time':time.time()})
    response={'dealReference':ref}
    capture(p,'close_response',{'response':response})
    capture(p,'close_confirmation_observed',{'response':response,'confirmation':{
        'dealStatus':'ACCEPTED' if accepted else 'REJECTED', 'dealReference':ref,
        'affectedDeals':[{'dealId':p['deal_id'],'status':
                          'PARTIALLY_CLOSED' if partial else 'FULLY_CLOSED'}]}})
    return p


@pytest.mark.parametrize('channel',['MOBILE','WEB','DEALER'])
def test_affirmative_external_channel_retains_durable_identity(channel):
    p=position();a=activity(p,channel)
    classify(p,[a,a]);first=deepcopy(p)
    assert p['exit_authority']==EXTERNAL_OPERATOR
    assert p['operator_intervention'] and p['accounting_eligible']
    assert not learning_eligible(p) and len(p['exit_authority_evidence'])==1
    p=json.loads(json.dumps(p));classify(p,[]);classify(p,[a])
    assert p==first


@pytest.mark.parametrize('channel',['PUBLIC_WEB_API','PUBLIC_FIX_API','SYSTEM',None])
def test_no_matching_june_request_is_unknown_not_manual(channel):
    p=position();classify(p,[activity(p,channel)])
    assert p['exit_authority']==UNKNOWN and not p['operator_intervention']
    assert not learning_eligible(p) and p['accounting_eligible']


def test_request_only_and_rejected_request_cannot_certify_june():
    p=position();capture(p,'close_intent',{'order':{'dealId':'D1'},'local_request_time':1})
    classify(p);assert p['exit_authority']==UNKNOWN
    intent(p,accepted=False);classify(p);assert p['exit_authority']==UNKNOWN


def test_linked_june_confirmation_survives_restart_missing_activity():
    p=intent(position());classify(p)
    assert p['exit_authority']==JUNE_STRATEGY and learning_eligible(p)
    p=json.loads(json.dumps(p));classify(p,[])
    assert learning_eligible(p)
    assert p['exit_authority_evidence'][0]['intent_id']==p['close_intents'][0]['id']


def test_linked_activity_can_resolve_lost_confirmation():
    p=intent(position(),accepted=False)
    p['close_intents'][0].pop('confirmation')
    classify(p,[activity(p,'PUBLIC_WEB_API',ref='JUNE')])
    assert p['exit_authority']==JUNE_STRATEGY and learning_eligible(p)


def test_june_partial_and_final_have_distinct_intents_and_learning():
    p=intent(position(),ref='PARTIAL',partial=True);record_partial(p)
    intent(p,ref='FINAL');classify(p)
    assert p['exit_authority']==JUNE_STRATEGY and learning_eligible(p)
    assert len(p['close_intents'])==2 and len(p['exit_authority_legs'])==1
    classify(p,[]);assert len(p['exit_authority_legs'])==1


def test_external_partial_vetoes_autonomous_final_campaign_learning():
    p=intent(position(),ref='FINAL')
    classify(p,[activity(p,partial=True)])
    assert p['exit_authority']==JUNE_STRATEGY and p['operator_intervention']
    assert not learning_eligible(p)
    assert p['exit_authority_legs'][0]['authority']==EXTERNAL_OPERATOR


def test_unknown_partial_vetoes_autonomous_final_learning():
    p=intent(position(),ref='FINAL')
    p['exit_authority_legs']=[{'identity':'partial','authority':UNKNOWN}]
    classify(p);assert not learning_eligible(p)


def test_acknowledged_june_stop_with_explicit_execution_is_eligible():
    p=position();p.update(acknowledged_stop_level=101.,
                        stop_sync={'status':'acknowledged','deal_id':'D1','target':101.})
    a=activity(p,'SYSTEM',stop=True);a['details']['stopLevel']=101.
    classify(p,[a]);assert p['exit_authority']==BROKER_PROTECTION
    assert learning_eligible(p) and not p['operator_intervention']


@pytest.mark.parametrize('defect',['pending','wrong_deal','wrong_level','no_execution','malformed'])
def test_stop_intention_or_system_channel_alone_is_not_protection(defect):
    p=position();p.update(acknowledged_stop_level=101.,
                        stop_sync={'status':'acknowledged','deal_id':'D1','target':101.})
    a=activity(p,'SYSTEM',stop=True);a['details']['stopLevel']=101.
    if defect=='pending':p['stop_sync']['status']='pending'
    if defect=='wrong_deal':p['stop_sync']['deal_id']='OTHER'
    if defect=='wrong_level':a['details']['stopLevel']=102.
    if defect=='no_execution':a['details']['actions']=a['details']['actions'][:1]
    if defect=='malformed':a['details']['stopLevel']='not a number'
    classify(p,[a]);assert p['exit_authority']==UNKNOWN and not learning_eligible(p)


def test_affirmative_initial_attached_stop_execution():
    p=position();p['broker_entry_evidence'].update(
        submitted_order={'stopLevel':101.},
        accepted_confirmation={'dealStatus':'ACCEPTED','dealId':'D1','stopLevel':101.})
    a=activity(p,'SYSTEM',stop=True);a['details']['stopLevel']=101.
    classify(p,[a]);assert p['exit_authority']==BROKER_PROTECTION and learning_eligible(p)


def test_conflicting_operator_and_june_evidence_is_sticky_unknown():
    p=intent(position());classify(p);a=activity(p,ref='JUNE')
    classify(p,[a]);assert p['exit_authority']==UNKNOWN and p['exit_authority_conflict']
    assert p['operator_intervention'] and not learning_eligible(p)
    p=json.loads(json.dumps(p));classify(p,[]);assert p['exit_authority']==UNKNOWN


@pytest.mark.parametrize('defect',['deal','epic','account','date','future'])
def test_identity_mismatch_cannot_establish_operator(defect):
    p=position();a=activity(p)
    if defect=='deal':a['details']['actions'][0]['affectedDealId']='OTHER'
    if defect=='epic':a['epic']='OTHER'
    if defect=='account':p['broker_entry_evidence']['account_evidence']['order']={'verified':False}
    if defect=='date':a['date']='unknown'
    if defect=='future':a['date']=datetime.fromtimestamp(time.time()+3600,timezone.utc).isoformat()
    classify(p,[a]);assert p['exit_authority']==UNKNOWN


@pytest.mark.parametrize('pnl',[1.25,-1.25])
@pytest.mark.parametrize('authority',[EXTERNAL_OPERATOR,UNKNOWN])
def test_broker_confirmed_external_or_unknown_outcomes_never_reach_learning_planes(pnl,authority):
    ns,r=load_helpers()
    outcome={'exit_authority_schema':1,'exit_authority':authority,
             'strategy_learning_eligible':False,'dollar_pnl':pnl,
             'evidence_class':'BROKER_CONFIRMED_LIVE','settlement_state':'CONFIRMED'}
    ns['_live_perf_record']('GOLD',pnl>0,.2,pnl_dollar=pnl,deal_id='D1',
                            evidence_class='BROKER_CONFIRMED_LIVE',outcome=outcome)
    assert r.data=={} and not ns['_confirmed_outcome_record'](outcome)
    assert ns['_performance_learning_rows']([outcome])==[]


@pytest.mark.parametrize('authority',[JUNE_STRATEGY,BROKER_PROTECTION])
def test_eligible_confirmed_performance_delivers_at_most_once(authority):
    ns,r=load_helpers();p=position();classify(p)
    p.update(exit_authority=authority,strategy_learning_eligible=True)
    for _ in range(3):
        ns['_live_perf_record']('GOLD',True,.2,pnl_dollar=1.,deal_id='D1',
                                evidence_class='BROKER_CONFIRMED_LIVE',outcome=p)
    rows=json.loads(r.data['june_perf_stats:GOLD'])['trades']
    assert len(rows)==1 and rows[0]['exit_authority']==authority


def canonical_harness(ns,state):
    rows=[];perf=[]
    class Redis:
        def lrange(self,key,start,end):return rows[start:] if end==-1 else rows[start:end+1]
        def lpush(self,key,raw):rows.insert(0,raw)
        def ltrim(self,*a):pass
        def expire(self,*a):pass
        def lindex(self,key,i):return rows[i]
        def lset(self,key,i,value):rows[i]=value
    ns.update(json=json,_redis=lambda:Redis(),_LIVE_TRADE_HIST_KEY='history',
              _LIVE_TRADE_HIST_CAP=2000,_LIVE_TRADE_HIST_TTL=86400,
              _live_lot_sizes={'SILVER':1},_LIVE_LOT_SIZE_FX=1,_live_equity_cfd=set(),
              _IG_EQUITY_COMMISSION_USD=0,_live_perf_record=lambda *a,**kw:perf.append(kw),
              _live_observe=lambda *a,**kw:None,_live_write_htf_event=lambda *a,**kw:None)
    execute([function(n) for n in ['_live_settlement_key','_live_already_settled',
                                  '_live_mark_settled','_live_settle_primary_exit']],ns)
    state['open_position'].update(fill_price=100.,notional=100.,original_notional=100.)
    return rows,perf


def test_startup_manual_close_pending_stop_c4_fallback_exact_accounting_restart():
    ns,s,inv,acts,saves,settles,req,_=setup()
    rows,perf=canonical_harness(ns,s)
    acts['activities'][0].update(channel='MOBILE',dealId='BROKER_CLOSE')
    acts['activities'][0]['details']['dealReference']='MOBILE_REF'
    old=deepcopy(s);calls=[]
    def get(path,**kw):
        calls.append(path)
        if path=='/positions':return inv
        return {'activities':[]} if calls.count(path)%2==1 else acts
    ns['_ig_live_get']=get
    ns['_ig_live_post']=Mock(side_effect=AssertionError('No reconciliation order'))
    ns['_live_reconcile_positions']()
    assert s['open_position'] is None and len(rows)==1 and not perf
    record=json.loads(rows[0]);assert record['exit_authority']==EXTERNAL_OPERATOR
    assert record['operator_intervention'] and record['dollar_pnl'] is None
    assert record['stop_sync']['status']=='pending' # retained forensic evidence, not active stop
    assert json.loads(saves[-1])['open_position'] is None
    # Exact broker economics arrive later; do not infer economics from activity.
    from test_settlement_integrity import tx
    record.update(instrument='SILVER',direction='long',entry_price=100.,ig_size=.04,
                  exit_epoch=900000)
    booked=tx('TX1','Silver',100.,99.,'+0.04','$-0.04')
    event=deepcopy(acts['activities'][0]);event['details']['level']=99.
    durable,learned,view,_=_drive([record],[event,event],[booked,booked])
    assert durable[0]['dollar_pnl']==-.04 and durable[0]['recon_tx_count']==1
    assert not learned and durable[0]['operator_intervention']
    # Crash restores stale primary, durable settlement identity prevents another economic record.
    s['open_position']=old['open_position'];s['settled_primary_keys']=[]
    ns['_live_reconcile_positions']()
    assert s['open_position'] is None and len(rows)==1 and not perf
    for key in ('balance_day_start','risk_epoch','cb_active','kill_switch'):
        assert s[key]==old[key]
    ns['_ig_live_post'].assert_not_called()


@pytest.mark.parametrize('pnl',[1.25,-1.25])
def test_manual_ls_exact_pnl_retained_without_thesis_streak_or_perf(pnl):
    ns,s,inv,acts,saves,settles,req,_=setup()
    rows,perf=canonical_harness(ns,s)
    p=s['open_position'];p['exit_authority_epic']='SILVER_EPIC'
    a=acts['activities'][0];a['channel']='MOBILE'
    classify(p,[a]);ns['_live_b1_record_failure']=Mock()
    ns['_live_settle_primary_exit'](p,'stop_loss','LS.FULLY_CLOSED',confirmed_pnl=pnl)
    assert json.loads(rows[0])['dollar_pnl']==pnl and not perf
    # The real thesis consumer independently checks authority.
    ns.update(_sim_combo_key=lambda a,b:a+b,_B1_FAILURES_KEY='failures',_B1_THESIS_SCHEMA_VERSION=1)
    execute([function('_live_b1_record_failure')],ns)
    ns['_live_b1_record_failure'](p,json.loads(rows[0]),'LS')
    assert 'failures' not in s


def test_manual_loss_remains_in_actual_account_cb():
    ns=dict(_live={'balance_day_start':100.,'balance_total':88.,
                   'exit_authority':EXTERNAL_OPERATOR,'strategy_learning_eligible':False},
            _june_live_trading_enabled=True,_redis=Mock(return_value=Mock()),_live_log=Mock())
    for node in TREE.body:
        if isinstance(node,ast.Assign) and isinstance(node.targets[0],ast.Name):
            name=node.targets[0].id
            if name.startswith('_LIVE_CB_') or name=='_LIVE_CIRCUIT_BREAKER_PCT':
                ns[name]=ast.literal_eval(node.value)
    execute([function('_live_check_circuit_breaker')],ns)
    ns['_live_check_circuit_breaker']()
    assert ns['_june_live_trading_enabled'] is False
    ns['_redis']().set.assert_any_call('june_live_enabled','false')
    assert ns['_live']['balance_day_start']==100.


def test_campaign_telemetry_keeps_operator_provenance_and_exact_loss(tmp_path):
    p=position();p.update(fill_price=100.,ig_size=.06)
    classify(p,[activity(p)])
    store=Store(tmp_path/'campaign.sqlite3')
    state={'open_position':p};details={**contract(p),'dollar_pnl':-2.,'exit_reason':'broker_side_disappearance'}
    store.observe(state,{},account='HT2Q8',now=time.time(),unit=lambda *a:1.,event='entry')
    for _ in range(2):
        store.observe(state,{},account='HT2Q8',now=time.time(),unit=lambda *a:1.,
                      event='primary_settled',position=p,details=details)
    store.observe({}, {},account='HT2Q8',now=time.time(),unit=lambda *a:1.)
    with sqlite3.connect(store.path) as db:
        data=json.loads(db.execute('SELECT data FROM campaigns').fetchone()[0])
        assert data['realized_by_deal']['D1']==-2.
        assert data['exit_authorities_by_deal']['D1']['operator_intervention']
        assert db.execute("SELECT count(*) FROM events WHERE kind='primary_closed'").fetchone()[0]==1


def test_actual_june_close_partial_final_provenance_and_consumers():
    from test_live_accounting import LiveAccountingTests
    ns=close_harness();case=LiveAccountingTests()
    case.partial(ns,102.);case.residual(ns,99.)
    record=ns['_live']['trade_history'][0]
    assert record['exit_authority']==JUNE_STRATEGY and learning_eligible(record)
    assert record['dollar_pnl']==5. and len(record['exit_authority_legs'])==1
    assert ns['_live_perf_record'].call_count==1
    assert ns['_live_update_streak'].call_count==1
    assert ns['_sim_15m_record'].call_count==1


def test_confirmed_nonjune_close_avoids_adaptation_even_if_exit_trigger_fires():
    ns=close_harness()
    ns['_ls_position_guard_check'].return_value=(False,'REST-deal')
    ns['_live_close_position']('stop_loss',{'GOLD':{'price':99.}})
    ns['_ig_live_post'].assert_not_called()
    ns['_live_update_streak'].assert_not_called()
    ns['_live_perf_record'].assert_not_called()


def test_legacy_outcomes_are_not_mass_reclassified_or_mutated():
    p={'dollar_pnl':1.,'evidence_class':'LEGACY_LIVE'};old=deepcopy(p)
    assert learning_eligible(p) and p==old
    ns,_=load_helpers();assert ns['_performance_learning_rows']([p])==[p]


def test_performance_delivery_tombstone_survives_rolling_window_eviction_and_restart():
    ns,r=load_helpers();p=intent(position());classify(p)
    for i in range(20):
        ns['_live_perf_record']('GOLD',True,.2,pnl_dollar=1.,deal_id='D'+str(i),
                                evidence_class='BROKER_CONFIRMED_LIVE',outcome=p)
    before=deepcopy(r.data)
    # D0 is no longer in the rolling window; a replay after restart remains consumed.
    assert not any(t['deal_id']=='D0' for t in json.loads(r.data['june_perf_stats:GOLD'])['trades'])
    ns2,_=load_helpers();ns2['_redis']=lambda:r
    ns2['_live_perf_record']('GOLD',True,.2,pnl_dollar=1.,deal_id='D0',
                             evidence_class='BROKER_CONFIRMED_LIVE',outcome=p)
    assert r.data==before


def test_atomic_performance_delivery_rejects_concurrent_history_without_marking_consumed():
    from exit_authority import commit_performance
    ns,r=load_helpers();r.data['stats']='changed'
    with pytest.raises(RuntimeError):
        commit_performance(r,'stats','old',{'trades':[]},'deal:D1')
    assert r.data=={'stats':'changed'}


def test_redis_delivery_failure_is_not_silently_marked_successful():
    ns,r=load_helpers();p=intent(position());classify(p)
    def failed(*args):raise OSError('Redis unavailable')
    r.eval=failed
    with pytest.raises(OSError):
        ns['_live_perf_record']('GOLD',True,.2,pnl_dollar=1.,deal_id='D1',
                                evidence_class='BROKER_CONFIRMED_LIVE',outcome=p)
    assert r.data=={}


def test_settlement_replay_external_unknown_skip_june_and_protection_once():
    for authority in [EXTERNAL_OPERATOR,UNKNOWN,JUNE_STRATEGY,BROKER_PROTECTION]:
        record=rec();record.update(dollar_pnl=1.,settlement_state='CONFIRMED',reconciled=True,
                                  evidence_class='BROKER_CONFIRMED_LIVE',exit_authority_schema=1,
                                  exit_authority=authority,strategy_learning_eligible=authority in ELIGIBLE)
        durable,perf,view,saves=_drive([record],[],[])
        expected=int(authority in ELIGIBLE)
        assert len(perf)==expected
        durable,perf,view,saves=_drive(durable,[],[],perf=perf)
        assert len(perf)==expected and durable[0]['dollar_pnl']==1.


def test_campaign_path_carries_exit_authority_without_mutating_position(tmp_path):
    p=position();p.update(fill_price=100.,ig_size=.06);classify(p,[activity(p)])
    old=deepcopy(p);store=Store(tmp_path/'path.sqlite3');state={'open_position':p}
    store.record_path(state,{'GOLD':{'price':99.,'bid':98.9,'offer':99.1,'spread_pct':.2}},
                      account='HT2Q8',now=time.time(),unit=lambda *a:1.)
    with sqlite3.connect(store.path) as db:
        payload=json.loads(db.execute('SELECT payload FROM path').fetchone()[0])
        # Path stores a per-leg provenance contract, campaign summary lives in events.
        assert any(leg.get('operator_intervention') for leg in payload['legs'])
    assert p==old


@pytest.mark.parametrize('channel,authority',[('MOBILE',EXTERNAL_OPERATOR),('SYSTEM',BROKER_PROTECTION)])
def test_confirmed_ls_exit_resolves_later_authority_without_rewriting_money(channel,authority):
    from test_settlement_integrity import FakeList
    p=position();p.update(exit_epoch=time.time()-60,dollar_pnl=-1.25,settlement_state='CONFIRMED',
                        evidence_class='BROKER_CONFIRMED_LIVE')
    p.update(acknowledged_stop_level=101.,stop_sync={'status':'acknowledged','deal_id':'D1','target':101.})
    classify(p);a=activity(p,channel,stop=channel=='SYSTEM');a['details']['stopLevel']=101.
    redis=FakeList([p]);raw=redis.lrange('history',0,-1);calls=[];events=[]
    def get(*args,**kwargs):calls.append(kwargs);return {'activities':[a,a]}
    refresh_confirmed_authorities(raw,redis,'history',get,account='HT2Q8',now=time.time(),
                                 observe=lambda *args:events.append(args))
    result=redis.parsed()[0]
    assert result['exit_authority']==authority and result['dollar_pnl']==-1.25
    assert len(calls)==1 and len(events)==1
    before=deepcopy(result)
    refresh_confirmed_authorities(raw,redis,'history',get,account='HT2Q8',now=time.time()+360,
                                 observe=lambda *args:events.append(args))
    assert redis.parsed()[0]==before and len(events)==1


def test_authority_refresh_is_bounded_account_pinned_and_does_not_touch_legacy():
    from test_settlement_integrity import FakeList
    records=[]
    for i in range(4):
        p=position('D'+str(i));p.update(exit_epoch=time.time()-60,dollar_pnl=1.,settlement_state='CONFIRMED')
        classify(p);records.append(p)
    legacy={'deal_id':'OLD','dollar_pnl':2.,'settlement_state':'CONFIRMED'}
    records.append(legacy);redis=FakeList(records);raw=list(redis.items);calls=[]
    def get(*args,**kwargs):calls.append(args);return {'activities':[]}
    refresh_confirmed_authorities(raw,redis,'h',get,account='WRONG',now=time.time(),observe=lambda *a:None)
    assert not calls and redis.parsed()==records
    refresh_confirmed_authorities(raw,redis,'h',get,account='HT2Q8',now=time.time(),observe=lambda *a:None)
    assert len(calls)==2 and redis.parsed()[-1]==legacy


def test_retained_real_gold_mobile_close_fixture():
    evidence=json.loads(Path('fixtures/manual_close_gold_20261002.json').read_text())
    p=evidence['position'];classify(p,evidence['activities'])
    assert p['deal_id']=='DIAAAAR9LXBQHBA'
    assert p['exit_authority']==EXTERNAL_OPERATOR and p['operator_intervention']
    assert not learning_eligible(p) and p['accounting_eligible']
    proof=p['exit_authority_evidence'][0]
    assert proof['broker_close_id']=='DIAAAAR9LYCE4BA'
    assert proof['broker_reference']=='CHET9H9ACC589V'


def test_refresh_failures_do_not_suppress_exit_management_or_rewrite_economics():
    from test_settlement_integrity import FakeList
    p=position();p.update(exit_epoch=time.time()-60,dollar_pnl=-2.,settlement_state='CONFIRMED')
    classify(p);redis=FakeList([p]);raw=list(redis.items);events=[]
    def get(*args,**kwargs):raise TimeoutError('unavailable')
    refresh_confirmed_authorities(raw,redis,'h',get,account='HT2Q8',now=time.time(),
                                 observe=lambda *a:events.append(a))
    result=redis.parsed()[0]
    assert result['dollar_pnl']==-2. and result['exit_authority']==UNKNOWN
    assert events[0][0]=='exit_authority_reconcile_deferred'


def test_conflicting_confirmation_reference_cannot_certify_june():
    p=intent(position());classify(p)
    capture(p,'close_confirmation_observed',{'response':{'dealReference':'OTHER'},
                                             'confirmation':{'dealStatus':'ACCEPTED'}})
    classify(p)
    assert p['exit_authority']==UNKNOWN and p['exit_authority_conflict']
    assert not learning_eligible(p)


def test_rejected_confirmation_conflicting_with_accepted_activity_fails_honestly():
    p=intent(position(),accepted=False)
    classify(p,[activity(p,'PUBLIC_WEB_API',ref='JUNE')])
    assert p['exit_authority']==UNKNOWN and p['exit_authority_conflict']


def test_late_settlement_and_provenance_remain_with_original_campaign(tmp_path):
    store=Store(tmp_path/'late.sqlite3');old=position('OLD');old.update(fill_price=100.,ig_size=.06)
    classify(old,[activity(old)])
    new=position('NEW');new.update(fill_price=101.,ig_size=.06)
    now=time.time();unit=lambda *a:1.
    store.observe({'open_position':old},{},account='HT2Q8',now=now,unit=unit,event='entry')
    store.observe({}, {},account='HT2Q8',now=now+1,unit=unit)
    state={'open_position':new}
    store.observe(state,{},account='HT2Q8',now=now+2,unit=unit,event='entry')
    details={**contract(old),'dollar_pnl':-2.}
    for event in ['settlement_confirmed','exit_authority_resolved','settlement_confirmed']:
        store.observe(state,{},account='HT2Q8',now=now+3,unit=unit,
                      event=event,position=old,details=details)
    with sqlite3.connect(store.path) as db:
        rows=[json.loads(r[0]) for r in db.execute('SELECT data FROM campaigns')]
        original=next(r for r in rows if r['primary_deal_id']=='OLD')
        current=next(r for r in rows if r['primary_deal_id']=='NEW')
        assert original['realized_by_deal']['OLD']==-2. and original['operator_intervention']
        assert 'OLD' not in current['realized_by_deal']
        assert 'exit_authorities_by_deal' not in current
        assert db.execute("SELECT count(*) FROM events WHERE kind='settlement_confirmed'").fetchone()[0]==1
