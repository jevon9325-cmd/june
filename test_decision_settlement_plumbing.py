"""Prospective checkpoint enrichment and definitive delivery rejection regressions."""
import json
import sqlite3
from pathlib import Path
from copy import deepcopy
from unittest.mock import Mock

import pytest
from redis.exceptions import OutOfMemoryError

from decision_ledger import unpack
from exit_authority import PerformanceWriteRejected, commit_performance
from settlement_discovery import SettlementRegistry, commit_performance_once
from test_decision_ledger import recorder, install, selector, cycle_rows
from test_build4ca_defensive import _base_ns
from test_durable_settlement_discovery import Cache


@pytest.mark.parametrize('raw,final',[(-1.25,-.625),(1.25,.625),(0.,0.)])
@pytest.mark.parametrize('terminal',['rejected','selected','fallback'])
def test_early_checkpoint_enriched_with_exact_source_scores(tmp_path,raw,final,terminal):
    rec=recorder(tmp_path);ns={'_sim_eligible':['GOLD','MISSING']}
    cid=rec.begin({},ns)
    rec.note('universe',{'signals':{'GOLD':{'direction':'bear','change_5m':99}}},ns)
    rec.checkpoint()  # first persisted universe deliberately has no scores
    rec.note('score_rank',{'sym':'GOLD','vol':raw,'eff_vol':final,'sig':{}},ns)
    rec.note('ranking',{'candidates':[('GOLD',final)],'signals':{'GOLD':{'change_5m':99}}},ns)
    rec.note('gate',{'sym':'GOLD'},ns,gate_name='fixture',result='FAIL' if terminal=='rejected' else 'PASS')
    if terminal=='selected':rec.note('submission',{'sym':'GOLD','order_body':{}},ns)
    if terminal=='fallback':
        rec.note('fallback',{'sym':'GOLD','_skip':['GOLD']},ns)
        rec.note('score_rank',{'sym':'GOLD','vol':88,'eff_vol':77},ns)
        rec.note('ranking',{'candidates':[('GOLD',77)]},ns)
    rec.note('finish',{},ns)
    with sqlite3.connect(rec.store.path) as db:
        row=db.execute('SELECT initial_rank,raw_score,final_score,payload FROM decision_candidates WHERE decision_cycle_id=? AND instrument=?',(cid,'GOLD')).fetchone()
        assert row[:3]==(1,raw,final)
        candidate=unpack(row[3]);assert (candidate['raw_score'],candidate['final_score'])==(raw,final)
        ev=unpack(db.execute("SELECT payload FROM decision_events WHERE decision_cycle_id=? AND kind='RANK_COMPONENTS' ORDER BY sequence LIMIT 1",(cid,)).fetchone()[0])
        assert (ev['raw'],ev['final'])==row[1:3]
        assert db.execute("SELECT raw_score,final_score,initial_rank FROM decision_candidates WHERE instrument='MISSING'").fetchone()==(None,None,None)
    assert rec.failures==0 and rec.drops==0


def test_ranking_only_source_has_truthful_missing_raw_and_terminal_is_immutable(tmp_path):
    rec=recorder(tmp_path);ns={'_sim_eligible':['GOLD']};cid=rec.begin({},ns)
    rec.note('universe',{'signals':{'GOLD':{}}},ns);rec.checkpoint()
    rec.note('ranking',{'candidates':[('GOLD',0.)]},ns)
    stale=deepcopy(rec._local.doc)
    rec.note('finish',{},ns)
    stale['candidates']['GOLD'].update(raw_score=999,final_score=999)
    rec.store.persist(stale)
    with sqlite3.connect(rec.store.path) as db:
        assert db.execute('SELECT raw_score,final_score FROM decision_candidates').fetchone()==(None,0.)


@pytest.mark.parametrize('raw',[0.,.001,-.001])
def test_rejected_before_adjusted_ranking_retains_only_exact_raw_gate_input(tmp_path,raw):
    rec=recorder(tmp_path);ns={'_sim_eligible':['GOLD','SILVER']};cid=rec.begin({},ns)
    rec.note('universe',{'signals':{'GOLD':{'direction':'bull'},'SILVER':{}}},ns)
    rec.checkpoint()
    rec.note('gate',{'sym':'GOLD','vol':raw,'thresh':1.},ns,
             function='_live_select_instrument',input_names=('vol','thresh'),gate_name='threshold',result='FAIL')
    # Stale vol exists in loop locals when a subsequent instrument fails an
    # eligibility/missing-signal gate. Undeclared values must not become scores.
    rec.note('gate',{'sym':'SILVER','vol':88},ns,function='_live_select_instrument',input_names=('sym',),gate_name='eligible',result='FAIL')
    rec.note('ranking',{'candidates':[]},ns);rec.note('finish',{},ns)
    with sqlite3.connect(rec.store.path) as db:
        assert db.execute("SELECT raw_score,final_score,initial_rank FROM decision_candidates WHERE instrument='GOLD'").fetchone()==(raw,None,None)
        assert db.execute("SELECT raw_score,final_score FROM decision_candidates WHERE instrument='SILVER'").fetchone()==(None,None)
        event=unpack(db.execute("SELECT payload FROM decision_events WHERE kind='GATE' AND candidate_id='GOLD'").fetchone()[0])
        assert event['inputs']['vol']==raw


def test_actual_selector_scores_equal_events_and_signal_inputs_unchanged(tmp_path):
    ns,signals=_base_ns();selector(ns);rec=recorder(tmp_path);install(ns,rec)
    signals['SILVER']=dict(signals['GOLD'],change_5m=-.5)
    signals['COPPER']=dict(signals['GOLD'],change_5m=0.)
    original=deepcopy(signals);cid=rec.begin({},ns)
    assert ns['_live_select_instrument'](signals,'neutral')==['GOLD','SILVER','COPPER']
    rec.note('finish',{},ns)
    _,events,candidates=cycle_rows(rec,cid)
    for c in candidates:
        ev=next((e for e in events if e['kind']=='RANK_COMPONENTS' and e['candidate_id']==c['candidate_id']),None)
        if ev:assert (c['raw_score'],c['final_score'])==(ev['raw'],ev['final'])
        else:assert c['raw_score'] is None and c['final_score'] is None
    assert signals==original


@pytest.mark.parametrize('rejection',[OutOfMemoryError('command not allowed when used memory > maxmemory'),-2])
def test_definitive_oom_releases_only_own_prepared_receipt_and_retry_survives_restart(tmp_path,rejection):
    registry=SettlementRegistry(tmp_path/'settlements.sqlite3','A');cache=Cache()
    registry.put({'deal_id':'D1','instrument':'GOLD','settlement_state':'CONFIRMED','dollar_pnl':.2,'perf_fed':False})
    cache.eval=Mock(side_effect=rejection) if isinstance(rejection,Exception) else Mock(return_value=rejection)
    with pytest.raises(PerformanceWriteRejected):
        commit_performance_once(registry,cache,'june_perf_stats:GOLD',None,{'trades':[{'observation_id':'deal:D1'}]},'deal:D1')
    assert registry.get('deal:D1')['settlement_state']=='CONFIRMED'
    assert not registry.get('deal:D1')['perf_fed'] and cache.data=={}
    with registry.connect() as db:assert db.execute('SELECT COUNT(*) FROM performance_receipts').fetchone()[0]==0
    cache=Cache();registry=SettlementRegistry(registry.path,'A')
    assert commit_performance_once(registry,cache,'june_perf_stats:GOLD',None,{'trades':[{'observation_id':'deal:D1'}]},'deal:D1')
    assert registry.get('deal:D1')['perf_fed']
    cache.data.clear()
    assert not commit_performance_once(SettlementRegistry(registry.path,'A'),cache,'june_perf_stats:GOLD',None,{},'deal:D1')
    assert cache.evals==1


def test_transport_failure_and_legacy_prepared_remain_quarantined(tmp_path):
    registry=SettlementRegistry(tmp_path/'settlements.sqlite3','A');cache=Cache()
    cache.eval=Mock(side_effect=OSError('lost acknowledgement'))
    with pytest.raises(OSError):commit_performance_once(registry,cache,'june_perf_stats:GOLD',None,{},'deal:D1')
    with pytest.raises(RuntimeError,match='uncertain'):
        commit_performance_once(SettlementRegistry(registry.path,'A'),Cache(),'june_perf_stats:GOLD',None,{},'deal:D1')


def test_single_atomic_mutation_keeps_marker_and_stats_together():
    cache=Cache();cache.eval=Mock(return_value=1)
    assert commit_performance(cache,'june_perf_stats:GOLD',None,{},'deal:D1')
    script=cache.eval.call_args.args[0]
    assert "redis.pcall('MSET'" in script and "redis.call('SET'" not in script


@pytest.mark.parametrize('deal,expected,reason',[
    ('DIAAAAR78ADVZAE',.0,None),
    ('DIAAAAR788TXRA3',.0,None),
    ('DIAAAAR8LW8UXAS',.0,None),
    ('DIAAAAR9P26LJAK',None,'quantity_coverage_incomplete'),
    ('DIAAAAR9UCFXRAY',-.52,None),
])
def test_retained_real_broker_campaigns_exclude_price_collisions_without_relaxing_quantity(deal,expected,reason):
    import settlement_reconcile as sr
    from test_settlement_integrity import instr_matcher
    fixture=json.loads(Path(__file__).with_name('fixtures').joinpath('plumbing_settlement_identity_20261004.json').read_text())
    rec=next(r for r in fixture['records'] if r['deal_id']==deal)
    before=deepcopy(fixture)
    # The runtime removes exact broker activity redelivery before matching.
    activities=list({json.dumps(a,sort_keys=True):a for a in fixture['activities']}.values())
    verdict=sr.reconcile_settlement(rec,activities,fixture['transactions'],instr_matcher)
    assert fixture==before
    if reason:
        assert verdict['verdict']==sr.DEFER and verdict['reason']==reason
        assert verdict['aggregated_abs_size']==.08 and verdict['opening_qty']==.09
    else:
        assert verdict['verdict']==sr.CONFIRM
        accepted=rec['broker_entry_evidence']['accepted_confirmation']
        rows=[t for t in fixture['transactions'] if t['openDateUtc']==accepted['date'].split('.')[0]]
        assert verdict['dollar_pnl']==round(sum(float(t['profitAndLoss'].replace('$','')) for t in rows),4)
        if deal=='DIAAAAR9UCFXRAY':assert verdict['dollar_pnl']==expected


@pytest.mark.parametrize('defect',['different_time','different_direction','missing_time','nonfinite_size','different_offset'])
def test_accepted_opening_narrowing_cannot_infer_missing_or_conflicting_transaction_identity(defect):
    from settlement_reconcile import _opening_identity_transactions
    r={'deal_id':'D1','entry_price':100.,'ig_size':.02,'broker_entry_evidence':{
        'account_id':'A','deal_id':'D1','accepted_confirmation':{'dealId':'D1','dealStatus':'ACCEPTED',
        'status':'OPEN','date':'2026-10-04T12:00:00.123','level':100.,'size':.02,'direction':'SELL'}}}
    row={'openDateUtc':'2026-10-04T12:00:00','size':'-.02'}
    if defect=='different_time':row['openDateUtc']='2026-10-04T12:00:01'
    if defect=='different_direction':row['size']='+.02'
    if defect=='missing_time':row.pop('openDateUtc')
    if defect=='nonfinite_size':row['size']='NaN'
    if defect=='different_offset':row['openDateUtc']='2026-10-04T12:00:00+01:00'
    assert _opening_identity_transactions(r,[row])==[]
    assert _opening_identity_transactions(r,[{'openDateUtc':'2026-10-04T12:00:00','size':'-.02'}])
    assert _opening_identity_transactions(r,[{'openDateUtc':'2026-10-04T13:00:00+01:00','size':'-.02'}])


def test_legacy_missing_opening_contract_keeps_conservative_matcher():
    from settlement_reconcile import _opening_identity_transactions
    rows=[{'openDateUtc':'2026-10-04T12:00:00','size':'-.02'}]
    assert _opening_identity_transactions({'deal_id':'D1'},rows) is rows
