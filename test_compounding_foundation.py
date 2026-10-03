"""Offline regression/certification. Broker effects are exclusively mocks."""
import ast
import json
import pathlib
import sqlite3
import subprocess
from copy import deepcopy
from unittest.mock import Mock
import pytest
from campaign_telemetry import Store
from winner_protection import protect, reconcile_broker_stop, normalized_target
from protection_geometry import amendment_geometry, status
from compounding_observation import reconstruct, decision_view, protection_values, remember_quote, quote_view

ROOT=pathlib.Path(__file__).resolve().parent
FIX=json.loads((ROOT/'fixtures/compounding_natgas_friday.json').read_text(encoding='utf-8'))


def primary():
    return dict(deal_id=FIX['primary_opening']['dealId'],instrument='NATGAS',direction='long',
                fill_price=3049.,ig_size=.1,pos_size=77.30,leverage=4.,broker_stop_level=3034.,
                stop_pct=.005,entry_time=1790950773.,tp_pct=.000988)


def geometry(p=None,target=3063.99990,mid=3075.,**over):
    args=dict(mid=mid,pip=.01,price_unit=.01,min_points=10,spread_native=3.)
    args.update(over)
    return amendment_geometry(p or primary(),target,**args)


@pytest.mark.parametrize('symbol,points,pip,unit,expected',[('HO',50,.01,.01,51.),
    ('OIL',6,.01,.01,7.),('NATGAS',10,.01,.01,11.),('SILVER',4,.01,.01,5.),
    ('GOLD',1,1.,1.,2.),('COCOA',10,1.,1.,11.),('SUGAR',1,1.,1.,2.)])
def test_inverse_order_units(symbol,points,pip,unit,expected):
    # Actual Friday startup metadata; not a claim about all historical rules.
    p=dict(primary(),instrument=symbol,fill_price=100.,broker_stop_level=90.,stop_pct=.1)
    g=geometry(p,target=105.,mid=120.,min_points=points,pip=pip,price_unit=unit,spread_native=0.)
    assert g['buffered_minimum_native']==expected


def test_friday_unit_defect_changes_attempt_not_stop():
    # Original guard .11, spread3.01 => passes. Correct native minimum11 => defer.
    p=primary();g=geometry(p,target=3053.49966,mid=3059.5)
    assert abs(3059.5-3053.49966)>max(11*.01,3.+.01)
    assert not g['can_send'] and g['normalized_requested_stop']==3053.49966
    assert g['protection_status']=='LOCALLY_TOO_CLOSE'


def test_dynamic_percentage_minimum():
    p=dict(primary(),fill_price=100.,broker_stop_level=90.,stop_pct=.1)
    g=geometry(p,target=101.,mid=120.,min_points=1,pip=1,price_unit=1,min_fraction=.1,spread_native=0)
    assert g['buffered_minimum_native']==13.


@pytest.mark.parametrize('direction,target,mid,allowed',[('long',3064.,3075.,True),
    ('long',3064.,3074.99999,False),('long',3080.,3075.,False),
    ('short',3034.,3023.,True),('short',3010.,3023.,False)])
def test_direction_and_normalized_boundary(direction,target,mid,allowed):
    p=dict(primary(),direction=direction,broker_stop_level=3080. if direction=='short' else 3034.)
    assert geometry(p,target,mid)['can_send'] is allowed


def test_guard_checks_stronger_effective_floor_not_weaker_proposal():
    p=dict(primary(),intended_stop_level=3070.)
    g=geometry(p,target=3060.,mid=3075.)
    assert g['normalized_requested_stop']==3070. and not g['can_send']


@pytest.mark.parametrize('over',[{'pip':0},{'price_unit':None},{'min_points':float('nan')},
    {'spread_native':-1},{'min_fraction':float('inf')}])
def test_bad_geometry_defers_without_stalling_exits(over):
    assert geometry(**over)['protection_status']=='UNKNOWN_GEOMETRY'
    assert not geometry(**over)['can_send']


def request(p,reply=None,observer=None,can_send=True):
    return protect(p,3063.9999,put=Mock(return_value={'dealReference':'stop-ref'}),
        confirm=Mock(return_value=reply),save=Mock(),now=1790951791.,log=Mock(),
        can_send=can_send,observe=observer)


def accepted(p,stop=3063.9999):
    return dict(dealStatus='ACCEPTED',dealReference='stop-ref',dealId='AMENDMENT',
                affectedDeals=[dict(dealId=p['deal_id'],status='AMENDED')],stopLevel=stop)


def test_pending_is_not_too_close_and_unknown_response_distinct():
    p=primary();seen=[];request(p,observer=lambda k,d:seen.append((k,d)))
    assert status(p)=='BROKER_REQUEST_PENDING'
    assert {k for k,d in seen}>={'protection_request','protection_confirmation','protection_result'}
    p['stop_sync']['deal_ref']=None
    assert status(p)=='UNKNOWN_OUTCOME'


def test_accepted_affected_deals_certifies_only_real_broker_floor():
    p=primary();assert request(p,accepted(p))
    assert p['acknowledged_stop_level']==3063.9999 and status(p)=='BROKER_ACKNOWLEDGED'


def test_rejected_and_mismatched_confirmation():
    p=primary();reply=accepted(p);reply['dealStatus']='REJECTED';request(p,reply)
    assert status(p)=='BROKER_REJECTED' and 'acknowledged_stop_level' not in p
    p=primary();reply=accepted(p);reply['affectedDeals']=[{'dealId':'OTHER'}];request(p,reply)
    assert 'acknowledged_stop_level' not in p


@pytest.mark.parametrize('broker,ack',[(3063.9999,True),(3065.,True),(3060.,False)])
def test_snapshot_after_restart_stronger_and_weaker(broker,ack):
    p=primary();request(p);p=json.loads(json.dumps(p));seen=[]
    result=reconcile_broker_stop(p,broker,p['deal_id'],now=1790951861.,
                                observe=lambda k,d:seen.append((k,d)))
    assert result is ack
    if ack:assert p['acknowledged_stop_level']==broker and status(p)=='BROKER_SNAPSHOT_CONFIRMED'
    else:assert 'acknowledged_stop_level' not in p and seen[-1][0]=='protection_snapshot_weaker'


def test_observer_failure_does_not_change_broker_outcome():
    p=primary();assert request(p,accepted(p),observer=Mock(side_effect=OSError('disk-full')))
    assert p['acknowledged_stop_level']==3063.9999


def observe(store,s,event,details=None,position=None,now=1790950773.,price=3075.):
    store.observe(s,{'NATGAS':{'price':price,'spread_pct':.0984,'direction':'bull','change_5m':.3}},
                  account='HT2Q8',now=now,unit=lambda symbol,price:price,event=event,
                  details=details,position=position)


def cid(db):return db.execute('select id from campaigns').fetchone()[0]


def test_historical_equivalent_recorder_chain_without_journal(tmp_path):
    store=Store(tmp_path/'telemetry.sqlite3',event_cap=4)
    p=primary();s={'open_position':p,'pyramid_legs':[],'balance_total':120.79}
    observe(store,s,'entry',{'opening_confirmation':FIX['primary_opening']},price=3049.)
    sig=FIX['qualification']['signal'];store.observe(s,{'NATGAS':{'price':sig['mid'],'spread_pct':sig['spread_pct']}},
        account='HT2Q8',now=FIX['qualification']['at'],unit=lambda sym,price:price,
        details={'pyramid_trigger':.0015})
    p['ig_size']=.05;p['partial_dollar_pnl']=.25
    observe(store,s,'partial_tp_confirmed',{'realized_pnl':.25},now=1790951349.)
    g=geometry(p);observe(store,s,'protection_geometry',g,now=1790951791.)
    callback=lambda k,d:observe(store,s,k,d,position=p,now=1790951791.)
    request(p,observer=callback)
    # Persist/restart: independent store and reconstructed position retain identity.
    p=json.loads(json.dumps(p));s['open_position']=p;store=Store(store.path,event_cap=4)
    reconcile_broker_stop(p,3063.9999,p['deal_id'],now=1790951861.,
        observe=lambda k,d:observe(store,s,k,d,position=p,now=1790951861.))
    observe(store,s,'pyramid_decision',FIX['approval'],now=1790951867.)
    observe(store,s,'addon_proposed',{'candidate_quantity':.01},now=1790951867.)
    observe(store,s,'addon_submission',{'order':{'size':.01,'direction':'BUY'},'decision':'SUBMIT'},now=1790951867.1)
    observe(store,s,'addon_confirmation',{'confirmation':FIX['addon_opening']},now=1790951867.2)
    addon=dict(p,deal_id=FIX['addon_opening']['dealId'],fill_price=3074.,ig_size=.01,
               leg_index=2,leg_generation=1,partial_dollar_pnl=0.,broker_stop_level=3059.)
    s['pyramid_legs']=[addon];observe(store,s,'addon_opened',position=addon,now=1790951867.3)
    observe(store,s,'pyramid_decision',{'decision':'reject','reason':'duplicate_accepted_quote_evaluation'},now=1790951944.)
    observe(store,s,'protection_position_gone',{'protection_status':'BROKER_POSITION_GONE',
        'source':'REST.absent+activity.POSITION_CLOSED'},now=1790952084.)
    observe(store,s,'primary_settled',{'dollar_pnl':.95,'source':'broker_transactions'},position=p,now=1790952084.1)
    s['open_position']=None
    observe(store,s,'leg_closed',{'realized_pnl':-.13,'reason':'orphan_primary_closed'},position=addon,now=1790952087.)
    s['pyramid_legs']=[];observe(store,s,'after_evaluation',now=1790952088.)
    with sqlite3.connect(store.path) as db:
        result=reconstruct(db,cid(db),coverage=json.loads(store.coverage_path.read_text()))
        assert result['decision_chain_complete'],result['missing']
        assert result['qualification']==FIX['qualification']['at']
        assert result['decisions'][0]['known_gate_evidence']['f50']['f50_legal_ig']==.01
        assert result['decisions'][1]['first_binding_gate']=='duplicate_accepted_quote_evaluation'
        assert result['generations']==[0,1]
        assert len(result['events'])>4  # critical chain survives generic event cap
        assert db.execute('select count(distinct campaign) from links').fetchone()[0]==1
        data=json.loads(db.execute('select data from campaigns').fetchone()[0]);assert sum(data['realized_by_deal'].values())==pytest.approx(.82)
        receipt=db.execute("select id,payload from compounding_events where kind='addon_confirmation'").fetchone()
        corrupt=json.loads(receipt[1]);corrupt['event_details']['confirmation']['dealId']='UNRELATED'
        db.execute('update compounding_events set payload=? where id=?',(json.dumps(corrupt),receipt[0]))
        bad=reconstruct(db,cid(db),coverage=json.loads(store.coverage_path.read_text()))
        assert not bad['decision_chain_complete'] and 'accepted_addon_confirmation' in bad['missing']
        db.execute('update compounding_events set payload=? where id=?',(receipt[1],receipt[0]));db.commit()


@pytest.mark.parametrize('reason',['campaign_capacity_exhausted','f50_requires_profit_protected',
    'f50_mindeal_blocked: mindeal_blocked','continuation_absolute_cost_veto'])
def test_first_binding_gate_truth_not_unreached_passes(reason):
    view=decision_view({'decision':'reject','reason':reason})
    assert view['first_binding_gate']==reason and set(view['known_gate_evidence'].values())=={'UNOBSERVED'}


def test_rolling_parent_and_duplicate_delivery(tmp_path):
    store=Store(tmp_path/'telemetry.sqlite3');p=primary()
    s={'open_position':p,'pyramid_legs':[]};observe(store,s,'entry')
    a=dict(p,deal_id='gen2',leg_index=2,leg_generation=2);s['pyramid_legs']=[a]
    s['rolling_realized_harvest']={'deal_id':'gen1','realized_pnl_estimate':.21}
    observe(store,s,'v1_gen2_admitted',{'admit':True,'d1':0,'fuel_required':.20372})
    observe(store,s,'v1_gen2_opened',position=a);observe(store,s,'v1_gen2_opened',position=a)
    with sqlite3.connect(store.path) as db:
        rows=db.execute("select payload from compounding_events where kind='v1_gen2_opened'").fetchall()
        assert len(rows)==1
        leg=json.loads(rows[0][0])['legs'][1];assert leg['parent_deal_id']=='gen1' and leg['primary_deal_id']==p['deal_id']


def test_external_close_provenance_and_gap_cannot_certify(tmp_path):
    store=Store(tmp_path/'telemetry.sqlite3');p=primary();s={'open_position':p,'pyramid_legs':[]}
    observe(store,s,'entry');observe(store,s,'exit_authority_resolved',
        {'exit_authority_schema':1,'exit_authority':'EXTERNAL_OPERATOR','operator_intervention':True},position=p)
    store.note_gap(now=1,event='protection_request',error=OSError())
    with sqlite3.connect(store.path) as db:
        result=reconstruct(db,cid(db),coverage=json.loads(store.coverage_path.read_text()))
        assert not result['decision_chain_complete'] and result['recording_coverage']['gap_count']==1
        assert 'EXTERNAL_OPERATOR' in json.dumps(result)


def test_software_floor_not_broker_profit():
    p=dict(primary(),defensive_soft_sl=3070.)
    value=protection_values([p],lambda sym,price:price)[0]
    assert value['broker_gross_at_stop']==pytest.approx(-1.5)


def test_private_quote_does_not_change_signal_contract():
    quote={'bid':3073.,'offer':3076.,'mid':3074.5,'spread':3.};before=deepcopy(quote)
    remember_quote('fixture',quote,'IG.demo.markets.snapshot',{'updateTimeUTC':'14:37:00'})
    assert quote==before and quote_view('fixture')['values']==quote


def test_strategy_freeze():
    from test_decision_ledger import WithoutObservation
    before=subprocess.check_output(['git','show','06b600f:june.py'],cwd=ROOT,text=True,encoding='utf-8')
    old=ast.parse(before);new=WithoutObservation().visit(ast.parse((ROOT/'june.py').read_text(encoding='utf-8')))
    funcs=['_live_compute_ig_size','_live_compute_stop_pts','_live_try_entry','_live_open_position',
           '_live_tier_risk_pct','_live_check_circuit_breaker','_live_poll_balance','_live_close_position',
           'compute_signal','_live_fetch_market_data','_live_campaign_unit','_live_defensive_scaling_evidence',
           '_live_check_pyramid_entry','_live_evaluate_rolling_replacement','_live_check_pyramid_exits',
           '_live_close_addon_leg']
    for name in funcs:
        a=next(n for n in old.body if isinstance(n,ast.FunctionDef) and n.name==name)
        b=next(n for n in new.body if isinstance(n,ast.FunctionDef) and n.name==name)
        assert ast.dump(a)==ast.dump(b),name
    constants=lambda tree:{n.targets[0].id:ast.dump(n.value) for n in tree.body if isinstance(n,ast.Assign)
        and isinstance(n.targets[0],ast.Name)}
    assert constants(old)==constants(new)
    for name in ['defensive_scaling.py','continuation_economics.py','winner_accounting.py',
                 'rolling_fuel.py','rolling_build4a.py','winner_continuity.py','exit_authority.py','settlement_reconcile.py']:
        original=subprocess.check_output(['git','show','06b600f:'+name],cwd=ROOT)
        assert original.replace(b'\r\n',b'\n')==(ROOT/name).read_bytes().replace(b'\r\n',b'\n'),name


@pytest.mark.parametrize('observer_raises',[False,True])
def test_gen2_actual_early_gate_recorded_without_affecting_authority(observer_raises):
    tree=ast.parse((ROOT/'june.py').read_text(encoding='utf-8'))
    node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_live_v1_submit_gen2_replacement')
    observer=Mock(side_effect=OSError('disk-full') if observer_raises else None)
    namespace={'_ROLLING_V1_ENABLED':False,'_live_log':Mock(),'_live_observe':observer}
    exec(compile(ast.Module(body=[node],type_ignores=[]),'actual_gen2_function','exec'),namespace)
    assert namespace[node.name]({},primary()) is None
    assert observer.call_args.args[0]=='v1_gen2_gate'
    assert observer.call_args.args[3]['reason']=='v1_disabled'
