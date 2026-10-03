"""Offline actual-funnel characterization and prospective evidence guarantees."""
import ast
from copy import deepcopy
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from decision_ledger import Recorder, Store, encoded, unpack
from test_broker_identity import function, execute
from test_build4ca_defensive import _base_ns
from test_winner_accounting import harness


def observation_call(node):
    return (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
            and ast.unparse(node.value.func).startswith("globals().get('_decision_record',"))


class WithoutObservation(ast.NodeTransformer):
    """Erase only new optional hook statements; preserve all original expressions."""
    def visit_Expr(self, node):
        return None if observation_call(node) else self.generic_visit(node)

    def visit_Try(self, node):
        if (not node.handlers and not node.orelse and len(node.finalbody)==1
                and observation_call(node.finalbody[0])):
            return [self.visit(n) for n in node.body]
        if any(isinstance(n, ast.ImportFrom) and n.module=='decision_ledger' for n in node.body):
            return None
        return self.generic_visit(node)

    def visit_FunctionDef(self, node):
        return None if node.name=='_decision_record' else self.generic_visit(node)

    def visit_Assign(self, node):
        if any(isinstance(n, ast.Name) and n.id=='_DECISION_GATE_CATALOG' for n in node.targets):
            return None
        return self.generic_visit(node)


@lru_cache(maxsize=1)
def original_tree():
    tree=ast.parse(Path(__file__).with_name('june.py').read_text(encoding='utf-8'))
    return WithoutObservation().visit(tree)


@lru_cache(maxsize=1)
def catalog():
    tree=ast.parse(Path(__file__).with_name('june.py').read_text(encoding='utf-8'))
    return ast.literal_eval(next(n.value for n in tree.body if isinstance(n,ast.Assign)
                                and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='_DECISION_GATE_CATALOG'))


def recorder(tmp_path, **options):
    return Recorder(Store(tmp_path/'decisions.sqlite3'), asynchronous=False,
                    clock=lambda:1000.,catalog=catalog(),**options)


def install(ns, rec):
    ns['_decision_record']=lambda event, values, **meta:rec.note(event,values,ns,**meta)
    ns['_live_sess']={'account_id':'fixture-account'}


def selector(ns):
    ns.update(_sim_eligible=['GOLD','SILVER','COPPER','MISSING'],
              _live_is_eligible=Mock(return_value=True),_sim_vol_bucket=lambda _: 'high',
              _live_is_paused=Mock(return_value=False),_sim_corr_weight=Mock(return_value=1.),
              _live_perf_blocked=Mock(return_value=False))
    execute([function('_live_select_instrument')],ns)


def run_primary(ns, signals, rec):
    execute([function('_live_try_entry')],ns)
    install(ns,rec)
    cycle=rec.begin({'regime':'neutral'},ns)
    try:ns['_live_try_entry'](signals,'neutral')
    finally:rec.note('finish',{},ns)
    return cycle


def cycle_rows(rec, cid):
    with sqlite3.connect(rec.store.path) as db:
        header=unpack(db.execute('SELECT payload FROM decision_cycles WHERE decision_cycle_id=?',(cid,)).fetchone()[0])
        events=[unpack(row[0]) for row in db.execute('SELECT payload FROM decision_events WHERE decision_cycle_id=? ORDER BY sequence',(cid,))]
        candidates=[unpack(row[0]) for row in db.execute('SELECT payload FROM decision_candidates WHERE decision_cycle_id=? ORDER BY initial_rank',(cid,))]
    return header,events,candidates


def test_entire_trading_ast_matches_verified_parent():
    # Filled from b01e0cf's full module AST, not from the instrumented source.
    expected='faf9d0123eafbfc7a8ee8bd678851c6b00d8012876ba3072103e19f80f31e4be'
    actual=hashlib.sha256(ast.dump(original_tree(),include_attributes=False).encode()).hexdigest()
    assert actual==expected


def test_complete_universe_exact_ranking_ties_and_not_reached(tmp_path):
    ns,signals=_base_ns();selector(ns)
    signals['SILVER']=dict(signals['GOLD'],change_5m=.5)
    signals['COPPER']=dict(signals['GOLD'],change_5m=.8,spread_atr_wide=True)
    signals['OUTSIDE']=dict(signals['GOLD'])
    rec=recorder(tmp_path);install(ns,rec);cid=rec.begin({'regime':'neutral'},ns)
    before=deepcopy(signals);assert ns['_live_select_instrument'](signals,'neutral')==['GOLD','SILVER','COPPER']
    rec.note('finish',{},ns)
    assert signals==before
    header,events,candidates=cycle_rows(rec,cid)
    assert {c['instrument'] for c in candidates}=={'GOLD','SILVER','COPPER','MISSING','OUTSIDE'}
    ranked={c['instrument']:c for c in candidates}
    assert ranked['GOLD']['rank']==1 and ranked['SILVER']['rank']==2
    assert ranked['COPPER']['raw_score']==.8 and ranked['COPPER']['final_score']==.4
    assert ranked['GOLD']['rank_gap_to_next']==0 and ranked['OUTSIDE']['economics'] is None
    assert ranked['MISSING']['signal'] is None
    with sqlite3.connect(rec.store.path) as db:
        results={r[0] for r in db.execute('SELECT DISTINCT result FROM decision_gate_events')}
    assert results=={'PASS','FAIL','NOT_REACHED'}
    assert not header['gaps']
    assert any(e['kind']=='RANK_COMPONENTS' and e['final']==.4 for e in events)


@pytest.mark.parametrize('scenario',['admit','global','spread','exhaust','disabled','min_notional','margin','mindeal','thesis','none'])
@pytest.mark.parametrize('recording',['disabled','enabled','failed'])
def test_actual_funnel_trading_trace_equivalence(tmp_path,scenario,recording):
    def setup():
        ns,signals=_base_ns()
        selector(ns)
        ns['_sim_eligible']=['GOLD','SILVER']
        signals['SILVER']=dict(signals['GOLD'],change_5m=.3)
        ns['_CONTINUOUS_INSTRUMENTS']={'GOLD','SILVER'}
        ns['_live_fx_instruments']={'GOLD','SILVER'}
        ns['_live_open_position']=Mock()
        ns['_MINDEAL_OVERSIZE_MAX']=3.5
        ns['_last_cycle_direction']={};ns['_live_reversal_exits']={}
        if scenario=='global':ns['_live']['manual_review_required']=True
        if scenario=='spread':ns['_spread_atr_threshold'].return_value=.001
        if scenario=='exhaust':ns['_exhaustion_ratio'].return_value=4.
        if scenario=='disabled':ns['_june_live_trading_enabled']=False
        if scenario=='min_notional':ns['_sim_check_min_feasible'].side_effect=lambda s,*a:s!='GOLD'
        if scenario=='margin':ns['_live_fx_instruments']=set();ns['_live_equity_cfd']={'GOLD','SILVER'};ns['_live_margin']={'SILVER':.01}
        if scenario=='mindeal':
            ns['_live_fx_instruments']=set();ns['_live_margin']={'GOLD':.01,'SILVER':.01}
            ns['_live_min_deal']={'GOLD':1.,'SILVER':.001}
        if scenario=='thesis':
            ns['_live_b1_build_snapshot']=lambda *a:{}
            ns['_live_b1_reentry_allowed']=lambda s,*a:s!='GOLD'
        if scenario=='none':ns['_sim_eligible']=[]
        return ns,signals
    old,signals=setup()
    parent=original_tree()
    for name in ('_live_select_instrument','_live_try_entry'):
        execute([next(n for n in parent.body if isinstance(n,ast.FunctionDef) and n.name==name)],old)
    old['_live_try_entry'](signals,'neutral')
    current,signals=setup();rec=recorder(tmp_path,enabled=recording!='disabled')
    if recording=='failed':rec.store.persist=Mock(side_effect=OSError('disk full'))
    cid=run_primary(current,signals,rec)
    assert current['_live']==old['_live']
    # Every existing helper sees the same arguments and invocation order per helper.
    for key,value in old.items():
        if isinstance(value,Mock):assert current[key].call_args_list==value.call_args_list,key
    if recording=='enabled':
        header,events,_=cycle_rows(rec,cid)
        if scenario in ('min_notional','margin','mindeal','thesis'):
            assert [e['inputs']['sym'] for e in events if e['kind']=='FUNCTION_RETURN' and e.get('function')=='_live_try_entry'][-1]=='GOLD'
            assert any(e['kind']=='FALLBACK' for e in events)
            assert current['_live_open_position'].call_args.args[0]=='SILVER'
        if scenario in ('spread','exhaust'):assert not current['_live_open_position'].called


def opener_ns():
    ns=harness();ns['_live'].update(open_position=None,balance=100.,balance_total=100.)
    ns.update(_LIVE_SPROUT_MINDEAL_MULTS={'DEFAULT':1},_LIVE_PHASE_GATE_BAL=200.,
              _LIVE_PHASE_CONSERVATIVE_LEV=3,_IG_EQUITY_COMMISSION_USD=9,
              fetch_price=Mock(return_value={'mid':100.}),_PRESUBMIT_DRIFT_CAP=.01,
              _fallback_epics=set(),_last_cycle_direction={},_sim_claudia_pts=Mock(return_value=0),
              _live_capture_active=Mock(),_sim_combo_key=lambda s,d:f'{s}_{d}')
    execute([function('_live_open_position')],ns)
    return ns


@pytest.mark.parametrize('scenario',['accepted','rejected','unconfirmed','post_failed','post_empty','no_reference','other_confirmation','commission','drift','trade_guard'])
def test_original_order_geometry_and_broker_linkage(tmp_path,scenario):
    def setup():
        ns=opener_ns()
        if scenario=='rejected':ns['_live_confirm_deal'].return_value={'dealStatus':'REJECTED','reason':'fixture'}
        if scenario=='unconfirmed':ns['_live_confirm_deal'].return_value=None
        if scenario=='post_failed':ns['_ig_live_post'].return_value=None
        if scenario=='post_empty':ns['_ig_live_post'].return_value={}
        if scenario=='no_reference':ns['_ig_live_post'].return_value={'unexpected_field':True,'dealStatus':'ACCEPTED','dealId':'unconfirmed-id'}
        if scenario=='other_confirmation':ns['_live_confirm_deal'].return_value={'dealStatus':'PENDING','unexpected_field':True}
        if scenario=='commission':ns['_live_equity_cfd']={'GOLD'}
        if scenario=='drift':ns['fetch_price'].return_value={'mid':102.}
        if scenario=='trade_guard':ns['_live_trade_guard'].return_value=False
        return ns
    signals={'GOLD':{'price':100.,'change_5m':.5}}
    old=setup();execute([next(n for n in original_tree().body if isinstance(n,ast.FunctionDef) and n.name=='_live_open_position')],old)
    old['_live_open_position']('GOLD','long',signals,20.,2,6)
    ns=setup();rec=recorder(tmp_path);install(ns,rec);cid=rec.begin({'regime':'neutral'},ns)
    rec.note('universe',{'signals':signals},ns)
    ns['_live_open_position']('GOLD','long',signals,20.,2,6);rec.note('finish',{},ns)
    assert ns['_live']==old['_live']
    for key,value in old.items():
        if isinstance(value,Mock):assert ns[key].call_args_list==value.call_args_list,key
    _,events,_=cycle_rows(rec,cid)
    selected=[e for e in events if e['kind']=='SELECTED_FOR_SUBMISSION']
    if scenario in ('commission','drift','trade_guard'):assert not selected;assert not ns['_ig_live_post'].called
    else:
        assert selected[0]['order']==ns['_ig_live_post'].call_args.args[1]
        assert selected[0]['inputs']['stop_pct']==.01 and selected[0]['inputs']['tp_pct']==.02
        assert selected[0]['inputs']['ig_size']==.4 and selected[0]['inputs']['actual_n']==40.
    if scenario=='accepted':
        with sqlite3.connect(rec.store.path) as db:
            link=db.execute("SELECT account,deal_id,campaign_id FROM decision_outcomes WHERE status='ACCEPTED'").fetchone()
            assert link==('fixture-account','a1',hashlib.sha256(json.dumps(['fixture-account','a1']).encode()).hexdigest())
            assert db.execute('SELECT pinned FROM decision_cycles').fetchone()[0]==1
    if scenario in ('post_failed','post_empty','no_reference','other_confirmation'):
        with sqlite3.connect(rec.store.path) as db:
            statuses=[r[0] for r in db.execute('SELECT status FROM decision_outcomes ORDER BY sequence')]
            assert statuses[-1]=={'post_failed':'SUBMISSION_FAILED','post_empty':'SUBMISSION_FAILED',
                                  'no_reference':'SUBMISSION_REFERENCE_MISSING',
                                  'other_confirmation':'CONFIRMATION_OTHER_STATUS'}[scenario]
            assert 'ACCEPTED' not in statuses
            assert db.execute('SELECT pinned FROM decision_cycles').fetchone()[0]==1
        assert len(selected)==1


def test_strategic_bytes_immutable_versions_dedup_and_reference_schema(tmp_path):
    rec=recorder(tmp_path)
    body={'artifact_id':'forecast-study','version':3,'timestamp':'2026-10-03T00:00:00Z',
          'source_file':'provided-by-producer','folder_ref':'folder/3','chart_ref':'chart/2',
          'content_hash':'producer-hash','future_component':{'x':7}}
    rec.input('barbie_forecast:GOLD',json.dumps(body).encode())
    for index in range(3):
        cid=rec.begin({'regime':'neutral'},{})
        if index==2:
            body['future_component']['x']=8
            rec.input('barbie_forecast:GOLD',body)
            body['future_component']['x']=99
        rec.note('finish',{})
    with sqlite3.connect(rec.store.path) as db:
        versions=[unpack(r[0]) for r in db.execute("SELECT payload FROM strategic_artifacts WHERE artifact_type='barbie_forecast'")]
        assert sorted(v['future_component']['x'] for v in versions)==[7,8]
        assert db.execute("SELECT artifact_id,artifact_version,source_file,folder_ref,chart_ref,producer_content_hash FROM strategic_artifacts WHERE artifact_type='barbie_forecast' LIMIT 1").fetchone()==('forecast-study','3','provided-by-producer','folder/3','chart/2','producer-hash')
        assert db.execute('SELECT COUNT(DISTINCT decision_cycle_id) FROM decision_cycles').fetchone()[0]==3
    another=recorder(tmp_path);assert another.runtime_id!=rec.runtime_id
    assert another.begin({}, {})!=cid


def test_replay_exactly_once_terminal_immutable(tmp_path):
    rec=recorder(tmp_path);cid=rec.begin({},{});doc=rec._local.doc
    rec.note('finish',{});snapshot=deepcopy(doc);rec.store.persist(snapshot)
    snapshot['terminal_state']='FORGED';rec.store.persist(snapshot)
    with sqlite3.connect(rec.store.path) as db:
        assert db.execute('SELECT COUNT(*),terminal_state FROM decision_cycles').fetchone()==(1,'NO_TRADE_GLOBAL_GATE')
        assert db.execute('SELECT COUNT(*) FROM decision_events').fetchone()[0]==2


@pytest.mark.parametrize('failure',['locked','corrupt','full','serialization','queue','context'])
def test_failures_never_change_caller_or_certify_missing_evidence(tmp_path,failure):
    rec=recorder(tmp_path)
    if failure=='locked':
        lock=rec.store.connect();lock.execute('BEGIN IMMEDIATE')
    if failure=='corrupt':rec.store.path.write_bytes(b'not sqlite')
    if failure=='full':rec.store.max_bytes=1
    if failure=='context':rec.input('bad',object())
    rec.begin({},{});before=rec._local.doc['decision_cycle_id']
    if failure=='serialization':rec._local.doc['unserializable']=object()
    if failure=='queue':
        rec.asynchronous=True;rec.queue=__import__('queue').Queue(maxsize=1);rec.queue.put('occupied')
    rec.note('finish',{})
    assert rec._local.doc is None
    assert rec.failures or rec.drops
    if failure=='locked':lock.rollback();lock.close()
    if failure=='context':
        assert cycle_rows(rec,before)[0]['gaps']==['strategic_snapshot:bad']


def test_slow_worker_does_not_wait_on_trading_thread(tmp_path):
    entered=threading.Event();release=threading.Event()
    store=Store(tmp_path/'async.sqlite3')
    def slow(doc):entered.set();release.wait(5)
    store.persist=slow
    rec=Recorder(store,clock=lambda:1000.,queue_size=1)
    try:
        start=time.monotonic();rec.begin({},{});rec.checkpoint()
        assert entered.wait(1)
        rec.checkpoint();rec.note('finish',{})
        assert time.monotonic()-start<1 and rec.drops==1
    finally:release.set();rec.stop()


def test_pinned_evidence_retention_and_complete_archive(tmp_path):
    rec=recorder(tmp_path);rec.store.max_cycles=1;ns={'_live_sess':{'account_id':'A'}}
    cid=rec.begin({},ns)
    rec.note('submission',{'sym':'GOLD','order_body':{'size':.1}},ns)
    rec.note('confirmation',{'sym':'GOLD','confirm':{'dealStatus':'ACCEPTED','dealId':'D'}},ns)
    rec.note('finish',{},ns)
    rec.begin({},ns);rec.note('finish',{},ns)
    with sqlite3.connect(rec.store.path) as db:assert db.execute('SELECT decision_cycle_id FROM decision_cycles').fetchall()==[(cid,)]
    proof={'broker_final':True,'account':'A','deal_id':'D','settlement_identity':'broker-evidence-row',
           'settlement_utc':'2026-10-03T00:00:00Z','unresolved':False,'evidence_hash':'a'*64}
    with pytest.raises(ValueError):rec.store.archive_settled(cid,tmp_path/'bad.json',dict(proof,unresolved=True))
    archive=tmp_path/'archive.json';digest=rec.store.archive_settled(cid,archive,proof)
    assert digest==hashlib.sha256(archive.read_bytes()).hexdigest()
    assert {'decision_gate_catalog','strategic_context_snapshots','strategic_artifacts'}<=set(json.loads(archive.read_bytes())['tables'])
    rec.begin({},ns);rec.note('finish',{},ns)
    with sqlite3.connect(rec.store.path) as db:
        assert db.execute('SELECT decision_cycle_id FROM decision_cycles').fetchone()[0]!=cid
        assert db.execute('SELECT archive_hash FROM decision_retention_receipts').fetchone()[0]==digest


def test_actual_score_components_and_future_components(tmp_path):
    rec=recorder(tmp_path);cid=rec.begin({},{});
    rec.note('score',{'sym':'GOLD','clearance':2.,'claudia_pts':.5,'barbie_pts':.25,
                      'future_pts':-.1,'raw':4.65,'_final_conv':5})
    rec.note('finish',{})
    event=next(e for e in cycle_rows(rec,cid)[1] if e['kind']=='SCORE_COMPONENTS')
    assert event['components']['future_pts']==-.1 and event['total']==4.65 and event['conviction']==5


def test_primary_exception_remains_incomplete_and_propagates(tmp_path):
    rec=recorder(tmp_path);rec.begin({}, {})
    with pytest.raises(RuntimeError):
        try:raise RuntimeError('original failure')
        finally:rec.note('finish',{})
    with sqlite3.connect(rec.store.path) as db:
        assert db.execute('SELECT complete,terminal_state FROM decision_cycles').fetchone()==(0,'PRIMARY_EXECUTION_EXCEPTION')


def test_actual_conviction_total_and_components_without_extra_reads(tmp_path):
    ns={'_SIM_LOW_VOL_THRESH':.2,'_SIM_HIGH_VOL_THRESH':.5,
        '_sim_has_boost':Mock(return_value=True),'_sim_weekend_pts':Mock(return_value=0.),
        '_sim_claudia_pts':Mock(return_value=.25),'_sim_forecast_pts':Mock(return_value=.25),
        '_slow_grind_pts':Mock(return_value=.1),'_forecast_boundary_cache':{}}
    rec=recorder(tmp_path);install(ns,rec);cid=rec.begin({},ns)
    execute([function('_sim_conviction_gauge')],ns)
    value=ns['_sim_conviction_gauge']('GOLD','long',.5,.25,1.25,'GOLD_long','strict',.8)
    rec.note('finish',{},ns)
    event=next(e for e in cycle_rows(rec,cid)[1] if e['kind']=='SCORE_COMPONENTS')
    assert event['total']==pytest.approx(7.6) and event['conviction']==value==8
    assert sum(event['components'].values())==pytest.approx(event['total'])
    for name in ('_sim_has_boost','_sim_weekend_pts','_sim_claudia_pts','_sim_forecast_pts','_slow_grind_pts'):
        ns[name].assert_called_once()


def test_actual_optional_hook_swallows_recorder_exceptions(monkeypatch):
    import decision_ledger
    broken=Mock();broken.note.side_effect=OSError('broken observer')
    monkeypatch.setattr(decision_ledger,'_default',broken)
    ns={};execute([function('_decision_record')],ns)
    assert ns['_decision_record']('gate',{'sym':'GOLD'}) is None
    broken.note.assert_called_once()


def test_startup_identity_and_diagnostic_failure_are_fail_open(monkeypatch,tmp_path):
    import decision_ledger
    import subprocess
    monkeypatch.setattr(subprocess,'check_output',Mock(side_effect=OSError('git unavailable')))
    monkeypatch.setattr(decision_ledger.logging,'warning',Mock(side_effect=OSError('logging unavailable')))
    monkeypatch.setattr(decision_ledger,'_default',None)
    decision_ledger.initialize(tmp_path/'june.py',())
    assert decision_ledger._default is None


def test_helper_records_computed_cross_combo_count_without_repeating_history(tmp_path):
    rec=recorder(tmp_path);cid=rec.begin({}, {})
    rec.note('helper_snapshot',{'sym':'GOLD','n':80,'K':37,'outcomes':[True,False],
             'all_combos':{'large_unused_copy':object()},'threshold':.3,'ci_hi':.2},
             {'_SIM_COMBO_CI_ALPHA':.05},function='_sim_combo_wr_gate',input_names=('_SIM_COMBO_CI_ALPHA',))
    rec.note('finish',{})
    header,events,_=cycle_rows(rec,cid)
    event=next(e for e in events if e['kind']=='HELPER_SNAPSHOT')
    assert event['inputs']['K']==37 and event['inputs']['_SIM_COMBO_CI_ALPHA']==.05
    assert 'all_combos' not in event['inputs'] and not header['gaps']


def test_disk_pressure_reuses_space_and_preserves_unsettled(tmp_path):
    import random
    store=Store(tmp_path/'bounded.sqlite3',max_bytes=3*1024*1024)
    rec=Recorder(store,asynchronous=False,clock=lambda:1000.)
    pinned=rec.begin({},{});rec.note('submission',{'sym':'GOLD','order_body':{'size':.1}})
    rec.note('finish',{})
    generator=random.Random(0)
    for _ in range(60):
        rec.begin({}, {})
        rec.note('branch',{'sym':'GOLD'},expression=generator.randbytes(60000).hex())
        rec.note('finish',{})
    assert rec.failures==0 and rec.drops==0
    assert store.disk_bytes()<=store.max_bytes
    with sqlite3.connect(store.path) as db:
        assert db.execute('SELECT pinned FROM decision_cycles WHERE decision_cycle_id=?',(pinned,)).fetchone()==(1,)
        assert db.execute('SELECT COUNT(*) FROM decision_cycles').fetchone()[0]<61
        assert db.execute('PRAGMA page_count').fetchone()[0]*4096<=store.max_bytes*.9


def test_gate_ignores_previous_loop_candidate_locals(tmp_path):
    rec=recorder(tmp_path);cid=rec.begin({}, {})
    rec.note('gate',{'sym':'SILVER','vol':99.,'sig':{'price':999.}},input_names=('sym',),
             gate_name='_live_select_instrument:eligibility',result='FAIL')
    rec._local.symbol='SILVER'
    rec.note('gate',{'bal':0},input_names=('bal',),function='_live_try_entry',
             gate_name='_live_try_entry:balance',result='FAIL')
    rec.note('finish',{})
    gate=next(e for e in cycle_rows(rec,cid)[1] if e['kind']=='GATE')
    assert gate['inputs']=={'sym':'SILVER'}
    global_gate=next(e for e in cycle_rows(rec,cid)[1] if e.get('gate_name')=='_live_try_entry:balance')
    assert global_gate['candidate_id'] is None and global_gate['inputs']=={'bal':0}


def test_idle_worker_reports_capture_gaps_and_bounds_diagnostics(tmp_path,monkeypatch):
    import decision_ledger
    import queue
    rec=recorder(tmp_path);rec.failures=1
    warning=Mock();monkeypatch.setattr(decision_ledger.logging,'warning',warning)
    monkeypatch.setattr(decision_ledger.time,'monotonic',lambda:1000.)
    rec.queue=Mock();rec.queue.get.side_effect=[queue.Empty,None]
    rec._worker();rec._diagnose()
    warning.assert_called_once()
    monkeypatch.setattr(decision_ledger.time,'monotonic',lambda:1061.)
    warning.side_effect=OSError('diagnostic failed')
    rec._diagnose()
    assert warning.call_count==2
