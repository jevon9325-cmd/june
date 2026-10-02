"""Offline durable discovery integration against real June AST + broker matcher.

Production is never imported or contacted. Every database is under tmp_path;
fixtures retain real account/deal shapes and booked broker evidence, not forced
promotion outcomes. Existing tests cover the underlying full matching matrix.
"""
from copy import deepcopy
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import json
import sqlite3

import pytest

from settlement_discovery import SettlementRegistry, SettlementView, commit_performance_once
from test_broker_identity import execute, function
from test_settlement_integrity import rec, act, tx, instr_matcher, FakeList, _drive

FIXTURE = json.loads(Path(__file__).with_name('fixtures').joinpath(
    'lost_settlements_20261002.json').read_text(encoding='utf-8'))
NOW = 1790947000.0


class Cache(FakeList):
    def __init__(self, records=()):
        super().__init__(records)
        self.data = {}
        self.fail = False
        self.evals = 0

    def lrange(self, *args):
        if self.fail:
            raise OSError('Redis unavailable')
        return super().lrange(*args)

    def get(self, key):
        if self.fail:
            raise OSError('Redis unavailable')
        return self.data.get(key)

    def eval(self, script, n, *args):
        if self.fail:
            raise OSError('Redis unavailable')
        self.evals += 1
        if n == 1:
            key, idx, previous, raw = args
            if self.lindex(key, int(idx)) == previous:
                self.lset(key, int(idx), raw)
                return 'OK'
            return 0
        key, marker, previous, stats, oid = args
        if self.get(marker):
            return 0
        if (self.get(key) or '') != previous:
            return -1
        self.data[key], self.data[marker] = stats, oid
        return 1


def runtime(registry, cache, *, activities=None, transactions=None, live=None,
            now=NOW, perf=None, evidence_path=None):
    requests = []
    deliveries = [] if perf is None else perf

    def get(path, **kwargs):
        requests.append((path, kwargs))
        if path == '/history/transactions':
            return {'transactions': transactions or []}
        return {'activities': activities or []}

    def deliver(sym, won, sar, pnl_dollar=0.0, **kwargs):
        oid = kwargs['settlement_identity']
        key = 'june_perf_stats:' + sym
        previous = cache.get(key)
        stats = json.loads(previous) if previous else {'trades': []}
        stats['trades'].append({'observation_id': oid, 'pnl_dollar': pnl_dollar})
        if commit_performance_once(registry, cache, key, previous, stats, oid):
            deliveries.append((oid, pnl_dollar))

    ns = dict(json=json, time=SimpleNamespace(time=lambda: now),
              _redis=lambda: cache, _live_settlement_registry=lambda: registry,
              _live_sess={'account_id': registry.account}, _live=live or {},
              _live_evidence_capture=lambda: SimpleNamespace(path=str(evidence_path or 'missing-fixture.sqlite3')),
              _LIVE_TRADE_HIST_KEY='hist', _RECON_MIN_AGE_SECS=120,
              _RECON_GIVEUP_SECS=14*86400, _RECON_BASE_BACKOFF_SECS=300,
              _RECON_MAX_BACKOFF_SECS=6*3600, _RECON_MAX_PER_CYCLE=5,
              _RECON_SCHEMA_VERSION=1, _ig_live_get=get,
              _live_perf_record=deliver, _recon_instr_matches=instr_matcher,
              _live_observe=Mock(), _live_log=Mock(), _live_save_state=Mock())
    execute([function('_recon_iso'), function('_live_settlement_view'),
             function('_live_reconcile_provisional_settlements'),
             function('_live_refresh_trade_history_snapshot')], ns)
    ns['_live_reconcile_provisional_settlements']()
    return ns, requests, deliveries


@pytest.mark.parametrize('shape', ['empty', 'one', 'truncated', 'missing_oldest',
                                 'missing_newest', 'duplicate', 'stale', 'recent_only'])
def test_lost_three_broker_economics_independent_of_cache(tmp_path, shape):
    registry = SettlementRegistry(tmp_path/'settlements.sqlite3', FIXTURE['account'])
    records = deepcopy(FIXTURE['settlements'])
    for r in records + records:
        registry.put(r, imported=True)
    cache_rows = {
        'empty': [], 'one': records[:1], 'truncated': records[1:2],
        'missing_oldest': records[1:], 'missing_newest': records[:-1],
        'duplicate': records * 2,
        'stale': [dict(r, recon_attempts=999, recon_last_attempt=NOW) for r in records],
        'recent_only': [rec('RECENT', exit_epoch=int(NOW))],
    }[shape]
    cache = Cache(cache_rows)
    stale = deepcopy(records)
    ns, requests, deliveries = runtime(
        registry, cache, activities=FIXTURE['activities'], transactions=FIXTURE['transactions'],
        live={'trade_history': stale})
    actual = {r['deal_id']: r['dollar_pnl'] for r in registry.records() if r['deal_id'] != 'RECENT'}
    assert actual == {'DIAAAAR9NPGYTAY': .05, 'DIAAAAR9NRACEAN': .43, 'DIAAAAR9NUQ6WAL': -.20}
    assert round(sum(actual.values()), 2) == .28
    assert all(r['settlement_state'] == 'CONFIRMED' for r in stale)
    assert len([p for p, _ in requests if p == '/history/transactions']) == 1
    assert len([p for p, _ in requests if p == '/history/activity']) == 3
    assert not deliveries  # actual SYSTEM/UNKNOWN provenance is preserved
    assert all(r['exit_authority'] == 'UNKNOWN' for r in registry.records() if r['deal_id'] != 'RECENT')
    assert len(cache.items) == len(cache_rows)  # no reconstruction required
    restarted = SettlementRegistry(registry.path, FIXTURE['account'])
    _, requests2, deliveries2 = runtime(restarted, cache, activities=FIXTURE['activities'],
                                       transactions=FIXTURE['transactions'])
    assert not any(p == '/history/transactions' for p, _ in requests2)
    assert deliveries2 == []


def test_baseline_lost_rows_not_enumerated():
    # The deployed AST harness without a durable view reads only Redis.
    durable, perf, _, _ = _drive([], FIXTURE['activities'], FIXTURE['transactions'], now=NOW)
    assert durable == [] and perf == []


def test_missing_snapshot_not_invented_or_needed_for_learning(tmp_path):
    registry = SettlementRegistry(tmp_path/'s.sqlite3', 'A')
    registry.put(rec('D1', entry=100, qty=.02, exit_epoch=int(NOW)-1000))
    cache = Cache()
    args = dict(activities=[act('D1', 99, affected='C1')],
                transactions=[tx('C1', 'Spot Gold', 100, 99, '-0.02', '$0.02')])
    ns, _, perf = runtime(registry, cache, **args)
    assert perf == [('deal:D1', .02)] and cache.items == []
    assert not ns['_live_save_state'].called  # empty snapshot does not feed or invent rows
    restarted = SettlementRegistry(registry.path, 'A')
    for _ in range(3):
        _, _, perf = runtime(restarted, cache, perf=perf, **args)
    assert perf == [('deal:D1', .02)]
    assert restarted.get('deal:D1')['perf_fed'] is True
    # Loss of Redis stats and permanent marker still cannot replay local receipt.
    cache.data.clear()
    _, _, perf = runtime(restarted, cache, perf=perf, **args)
    assert perf == [('deal:D1', .02)] and not cache.data


def test_late_partial_final_restart_and_duplicate_shapes(tmp_path):
    registry = SettlementRegistry(tmp_path/'s.sqlite3', 'A')
    r = rec('D1', entry=100, qty=.04, partial_pnl=.02, exit_epoch=int(NOW)-1000)
    r['campaign_id'] = 'C'
    registry.put(r)
    registry.put(dict(r, settlement_identity='campaign:C'), imported=True)
    cache = Cache([r, r])
    _, calls, perf = runtime(registry, cache, activities=[act('D1', 99, partial=True)],
                            transactions=[tx('P', 'Spot Gold', 100, 99, '-0.02', '$0.02')])
    assert len([p for p, _ in calls if p == '/history/activity']) == 1
    assert registry.get('deal:D1')['dollar_pnl'] is None and perf == []
    r = registry.get('deal:D1'); r['recon_last_attempt'] = 0; registry.put(r)
    restarted = SettlementRegistry(registry.path, 'A')
    _, _, perf = runtime(restarted, cache,
        activities=[act('D1', 99, partial=True), act('D1', 98, affected='F')],
        transactions=[tx('P', 'Spot Gold', 100, 99, '-0.02', '$0.02'),
                      tx('F', 'Spot Gold', 100, 98, '-0.02', '$0.04')])
    assert registry.get('deal:D1')['dollar_pnl'] == .06
    assert perf == [('deal:D1', .06)] and len(registry.records()) == 1
    registry.put(dict(r, recon_last_attempt=0), imported=True)
    assert registry.get('deal:D1')['settlement_state'] == 'CONFIRMED'


@pytest.mark.parametrize('channel', ['MOBILE', 'WEB', 'SYSTEM', 'API'])
def test_external_unknown_provenance_never_trains(tmp_path, channel):
    registry = SettlementRegistry(tmp_path/'s.sqlite3', 'A')
    r = rec('D1', entry=100, qty=.02, exit_epoch=int(NOW)-1000)
    r.update(exit_authority_schema=1, exit_authority='UNKNOWN',
             exit_authority_epic='CS.D.CFDGOLD.BMU.IP', strategy_learning_eligible=False,
             entry_time=1790930000,
             broker_entry_evidence={'deal_id':'D1','account_id':'A','account_evidence':{
                 'order':{'account_id':'A','verified':True},
                 'confirmation':{'account_id':'A','verified':True}}})
    registry.put(r)
    activity = act('D1', 99, affected='EXTERNAL')
    activity.update(channel=channel, status='ACCEPTED', date='2026-10-02T12:00:00')
    activity['details']['size'] = .02
    activity['details']['direction'] = 'BUY'
    _, _, perf = runtime(registry, Cache(), activities=[activity],
                        transactions=[tx('EXTERNAL', 'Spot Gold', 100, 99, '-0.02', '$0.02')])
    outcome = registry.get('deal:D1')
    assert outcome['dollar_pnl'] == .02 and perf == []
    assert outcome['exit_authority']==('EXTERNAL_OPERATOR' if channel in ('MOBILE','WEB') else 'UNKNOWN')


def evidence_db(path, events):
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('CREATE TABLE evidence(id TEXT PRIMARY KEY,payload TEXT,observed_utc TEXT)')
        for i, row in enumerate(events):
            db.execute('INSERT INTO evidence VALUES(?,?,?)',
                       (str(i), json.dumps(row['payload']), row['observed_utc']))


def test_legacy_local_archive_only_three_recover_with_broker_rules(tmp_path):
    path = tmp_path/'evidence.sqlite3'
    evidence_db(path, FIXTURE['clear_events'])
    registry = SettlementRegistry(tmp_path/'s.sqlite3', FIXTURE['account'])
    ns, calls, perf = runtime(registry, Cache(), evidence_path=path,
                             activities=FIXTURE['activities'], transactions=FIXTURE['transactions'])
    assert sorted(r['dollar_pnl'] for r in registry.records()) == [-.20, .05, .43]
    assert all(r['discovery_learning_quarantined'] for r in registry.records())
    assert perf == []
    # Discovery now uses compact state, after the bootstrap source disappears.
    path.unlink()
    ns, _, perf = runtime(SettlementRegistry(registry.path, FIXTURE['account']), Cache())
    assert len(registry.records()) == 3 and not perf


def test_cursor_restart_atomic_and_quarantine_no_foreign_or_addon(tmp_path):
    rows = deepcopy(FIXTURE['clear_events'])
    foreign = deepcopy(rows[0]); foreign['payload']['account_id'] = 'FOREIGN'
    addon = deepcopy(rows[0]); addon['payload']['current_role'] = 'add_on'
    path = tmp_path/'evidence.sqlite3'; evidence_db(path, [foreign, addon] + rows)
    registry = SettlementRegistry(tmp_path/'s.sqlite3', FIXTURE['account'])
    for _ in range(5):
        SettlementRegistry(registry.path, FIXTURE['account']).import_evidence(path, limit=1)
    assert len(registry.records()) == 3
    registry.import_evidence(path, limit=1)
    assert len(registry.records()) == 3
    assert SettlementRegistry(registry.path, 'FOREIGN').records() == []


def test_new_settlement_event_replay_without_historical_quarantine(tmp_path):
    r = deepcopy(FIXTURE['settlements'][0])
    row = {'payload': {'event':'settlement_created','account_id':FIXTURE['account'],
                      'deal_id':r['deal_id'],'current_role':'primary','details':{'settlement':r}},
           'observed_utc':'2026-10-02T12:00:00+00:00'}
    path=tmp_path/'evidence.sqlite3'; evidence_db(path,[row]+FIXTURE['clear_events'][:1])
    registry=SettlementRegistry(tmp_path/'s.sqlite3',FIXTURE['account'])
    registry.import_evidence(path)
    assert len(registry.records()) == 1
    assert not registry.records()[0].get('discovery_learning_quarantined')


def test_unavailable_redis_preserves_existing_no_query_fail_safe(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    registry.put(rec(exit_epoch=int(NOW)-1000))
    cache=Cache();cache.fail=True
    _, calls, perf=runtime(registry,cache)
    assert calls==[] and perf==[] and registry.records()[0]['dollar_pnl'] is None


def test_cache_write_failure_cannot_prevent_promotion(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    r=rec('D1',entry=100,qty=.02,exit_epoch=int(NOW)-1000); registry.put(r)
    cache=Cache([r]);cache.eval=Mock(side_effect=OSError('cache write failed'))
    ns,_,perf=runtime(registry,cache,activities=[act('D1',99)],
                     transactions=[tx('F','Spot Gold',100,99,'-.02','$0.02')])
    assert registry.get('deal:D1')['dollar_pnl']==.02
    assert cache.parsed()[0]['dollar_pnl'] is None and perf==[]
    # Perf acknowledgement failed before Redis commit, so local economics remain
    # confirmed and uncertain delivery remains fail-closed (no guessed delivery).


def test_conflicting_identity_and_confirmed_regression_refused(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    r=rec();registry.put(r)
    with pytest.raises(ValueError):registry.put(dict(r,settlement_identity='deal:OTHER'))
    with pytest.raises(ValueError):registry.put(dict(r,broker_entry_evidence={'account_id':'B'}))
    c=dict(r,settlement_state='CONFIRMED',dollar_pnl=.2);registry.put(c)
    registry.put(r,imported=True)
    with pytest.raises(ValueError):registry.put(r)
    assert registry.get('deal:D1')['dollar_pnl']==.2


def test_same_campaign_distinct_deals_never_collapse(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    for deal in ('D1','D2'):
        registry.put(dict(rec(deal),campaign_id='C',settlement_identity='campaign:C'))
    assert {r['settlement_identity'] for r in registry.records()}=={'deal:D1','deal:D2'}


def test_confirmed_pending_delivery_survives_empty_cache_and_restart(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    registry.put(dict(rec(),settlement_state='CONFIRMED',dollar_pnl=.5,reconciled=True,
                      perf_fed=False,evidence_class='BROKER_CONFIRMED_LIVE'))
    _,_,perf=runtime(registry,Cache())
    assert perf==[('deal:D1',.5)]
    cache=Cache()
    for _ in range(3):
        _,_,perf=runtime(SettlementRegistry(registry.path,'A'),cache,perf=perf)
    assert perf==[('deal:D1',.5)] and cache.data=={}


def test_legacy_confirmed_unknown_delivery_is_not_backfilled(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    r=dict(rec(),settlement_state='CONFIRMED',dollar_pnl=.5,reconciled=True,
           evidence_class='BROKER_CONFIRMED_LIVE')
    registry.put(r,imported=True)
    _,_,perf=runtime(registry,Cache([r]))
    assert not perf and registry.get('deal:D1')['discovery_learning_quarantined']


def test_perf_atomic_ack_crash_then_redis_loss_cannot_duplicate(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A');cache=Cache()
    registry.acknowledge_performance=Mock(side_effect=OSError('crash after Redis commit'))
    with pytest.raises(OSError):
        commit_performance_once(registry,cache,'june_perf_stats:GOLD',None,{'trades':[1]},'deal:D1')
    assert cache.evals==1
    registry=SettlementRegistry(registry.path,'A')
    assert not commit_performance_once(registry,cache,'june_perf_stats:GOLD',None,{'trades':[1]},'deal:D1')
    cache.data.clear()
    assert not commit_performance_once(registry,cache,'june_perf_stats:GOLD',None,{'trades':[1]},'deal:D1')
    assert cache.evals==1


def test_perf_uncertain_ack_and_lost_marker_quarantines_replay(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A');cache=Cache()
    cache.eval=Mock(side_effect=OSError('connection lost during eval'))
    with pytest.raises(OSError):
        commit_performance_once(registry,cache,'june_perf_stats:GOLD',None,{},'deal:D1')
    cache=Cache()
    with pytest.raises(RuntimeError,match='uncertain'):
        commit_performance_once(SettlementRegistry(registry.path,'A'),cache,'june_perf_stats:GOLD',None,{},'deal:D1')
    assert cache.evals==0


def test_perf_definitive_cas_failure_remains_retryable(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A');cache=Cache()
    cache.data['june_perf_stats:GOLD']='changed'
    with pytest.raises(RuntimeError,match='concurrently'):
        commit_performance_once(registry,cache,'june_perf_stats:GOLD','old',{},'deal:D1')
    assert commit_performance_once(registry,cache,'june_perf_stats:GOLD','changed',{},'deal:D1')


def test_already_settled_identity_survives_both_redis_markers_lost(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A');registry.put(rec())
    ns=dict(_live={},_live_settlement_registry=lambda:registry,_redis=lambda:Cache(),json=json,
            _LIVE_TRADE_HIST_KEY='hist',_live_settlement_key=lambda pos:'deal:'+pos['deal_id'],_live_log=Mock())
    execute([function('_live_already_settled')],ns)
    assert ns['_live_already_settled']({'deal_id':'D1'})
    assert not ns['_live_already_settled']({'deal_id':'D2'})


def test_unavailable_archive_does_not_block_registered_requests(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    registry.put(rec('D1',entry=100,qty=.02,exit_epoch=int(NOW)-1000))
    registry.import_evidence=Mock(side_effect=OSError('archive unavailable'))
    ns,_,perf=runtime(registry,Cache(),activities=[act('D1',99)],
                     transactions=[tx('F','Spot Gold',100,99,'-.02','$0.02')])
    assert registry.get('deal:D1')['dollar_pnl']==.02
    assert perf==[('deal:D1',.02)]
    assert any('legacy evidence import pending' in c.args[0] for c in ns['_live_log'].call_args_list)


def test_creation_hooks_persist_actual_partial_final_record_before_clear(tmp_path):
    from test_live_accounting import close_harness, LiveAccountingTests
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    ns=close_harness(); capture=Mock()
    ns.update(_live_settlement_registry=lambda:registry,
              _live_evidence_capture=lambda:capture,_live_sess={'account_id':'A'})
    execute([function('_live_persist_settlement')],ns)
    case=LiveAccountingTests();case.partial(ns,102.);case.residual(ns,99.)
    row=registry.get('deal:fixture-deal')
    assert row['dollar_pnl']==5. and row['ig_size']==10.
    assert row['partial_dollar_pnl']==10. and row['settlement_state']=='CONFIRMED'
    assert capture.capture.call_args.args[1]=='settlement_created'
    assert row['exit_authority']=='JUNE_STRATEGY'


def test_storage_failures_do_not_block_protective_exit():
    from test_live_accounting import close_harness, LiveAccountingTests
    ns=close_harness(); capture=Mock();capture.capture.side_effect=OSError('journal full')
    ns.update(_live_settlement_registry=Mock(side_effect=OSError('registry full')),
              _live_evidence_capture=lambda:capture,_live_sess={'account_id':'A'})
    execute([function('_live_persist_settlement')],ns)
    LiveAccountingTests().residual(ns,99.)
    assert ns['_ig_live_post'].called
    assert ns['_live']['open_position'] is None
    assert len(ns['_live']['trade_history'])==1
    assert any('durable discovery pending' in c.args[0] for c in ns['_live_log'].call_args_list)


def test_new_provisional_creation_is_durable_before_cache_and_restart(tmp_path):
    from test_build4ca2_settlement import _ns
    ns=_ns()
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    ns.update(_live_settlement_registry=lambda:registry,
              _live_evidence_capture=lambda:Mock(),_live_sess={'account_id':'A'})
    execute([function('_live_persist_settlement')],ns)
    pos={'deal_id':'D1','instrument':'GOLD','direction':'short','fill_price':100.,
         'ig_size':.02,'entry_time':100.,'notional':2.}
    ns['_live_settle_primary_exit'](pos,'broker_side_disappearance','REST.absent')
    assert registry.get('deal:D1')['dollar_pnl'] is None
    assert registry.get('deal:D1')['settlement_state']=='PROVISIONAL'


@pytest.mark.parametrize('authority',['JUNE_STRATEGY','BROKER_PROTECTION','EXTERNAL_OPERATOR','UNKNOWN'])
def test_real_perf_consumer_with_registry_receipt_and_restart(tmp_path,authority):
    from test_exit_authority import load_helpers
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    row=dict(rec('D1'),settlement_state='CONFIRMED',dollar_pnl=.5,reconciled=True,
             perf_fed=False,evidence_class='BROKER_CONFIRMED_LIVE',exit_authority_schema=1,
             exit_authority=authority,strategy_learning_eligible=authority in ('JUNE_STRATEGY','BROKER_PROTECTION'))
    registry.put(row)
    ns,cache=load_helpers()
    ns['_live_settlement_registry']=lambda:registry
    execute([function('_live_commit_performance')],ns)
    for _ in range(3):
        ns['_live_perf_record']('GOLD',True,None,pnl_dollar=.5,deal_id='D1',
                                settlement_identity='deal:D1',settlement_state='CONFIRMED',
                                evidence_class='BROKER_CONFIRMED_LIVE',outcome=row)
    eligible=row['strategy_learning_eligible']
    trades=json.loads(cache.data.get('june_perf_stats:GOLD','{}')).get('trades',[])
    assert len(trades)==int(eligible)
    ns2,_=load_helpers(); ns2['_redis']=lambda:cache
    ns2['_live_settlement_registry']=lambda:SettlementRegistry(registry.path,'A')
    execute([function('_live_commit_performance')],ns2)
    cache.data.clear()
    ns2['_live_perf_record']('GOLD',True,None,pnl_dollar=.5,deal_id='D1',
                             settlement_identity='deal:D1',settlement_state='CONFIRMED',
                             evidence_class='BROKER_CONFIRMED_LIVE',outcome=row)
    assert cache.data=={}


def test_invalid_archive_row_is_retained_without_stranding_later_identity(tmp_path):
    rows=deepcopy(FIXTURE['clear_events'])
    bad=deepcopy(rows[0]);bad['payload']['position']['deal_id']='MISMATCH'
    broken=deepcopy(rows[0]);broken['observed_utc']='invalid UTC'
    path=tmp_path/'evidence.sqlite3';evidence_db(path,[broken,bad]+rows)
    registry=SettlementRegistry(tmp_path/'s.sqlite3',FIXTURE['account'])
    registry.import_evidence(path)
    assert len(registry.records())==3
    with closing(registry.connect()) as db:
        assert db.execute('SELECT count(*) FROM discovery_quarantine').fetchone()[0]==1
    with closing(sqlite3.connect(path)) as db:
        assert db.execute('SELECT count(*) FROM evidence').fetchone()[0]==5


@pytest.mark.parametrize('confirmed_first',[False,True])
def test_bootstrap_confirmed_duplicate_precedes_stale_provisional(tmp_path,confirmed_first):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    provisional=rec('D1',exit_epoch=int(NOW)-1000)
    confirmed=dict(provisional,settlement_state='CONFIRMED',dollar_pnl=.4,
                   reconciled=True,perf_fed=True,evidence_class='BROKER_CONFIRMED_LIVE')
    rows=[confirmed,provisional] if confirmed_first else [provisional,confirmed]
    _,calls,perf=runtime(registry,Cache(rows))
    assert registry.get('deal:D1')['dollar_pnl']==.4
    assert not calls and not perf


def test_archive_partial_marker_keeps_transaction_only_aggregation(tmp_path):
    row={'payload':{'event':'before_primary_clear','account_id':'A','current_role':'primary',
                    'deal_id':'D1','position':{'deal_id':'D1','instrument':'GOLD','direction':'short',
                    'fill_price':100.,'ig_size':.02,'original_ig_size':.04,
                    'partial_dollar_pnl':.02}},'observed_utc':'2026-10-02T12:00:00+00:00'}
    path=tmp_path/'evidence.sqlite3';evidence_db(path,[row])
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    _,_,perf=runtime(registry,Cache(),evidence_path=path,
                     transactions=[tx('P','Spot Gold',100,99,'-.02','$0.02'),
                                   tx('F','Spot Gold',100,98,'-.02','$0.04')])
    outcome=registry.get('deal:D1')
    assert outcome['dollar_pnl']==.06 and outcome['recon_evidence_path']=='transaction_only'
    assert not perf


def test_snapshot_persistence_failure_does_not_undo_durable_economics_or_refeed(tmp_path):
    registry=SettlementRegistry(tmp_path/'s.sqlite3','A')
    r=rec('D1',entry=100,qty=.02,exit_epoch=int(NOW)-1000);registry.put(r)
    ns,_,perf=runtime(registry,Cache(),live={'trade_history':[deepcopy(r)]},
                     activities=[act('D1',99)],transactions=[tx('F','Spot Gold',100,99,'-.02','$0.02')])
    ns['_live']['trade_history']=[deepcopy(r)]
    ns['_live_save_state']=Mock(side_effect=OSError('snapshot persistence failed'))
    ns['_live_refresh_trade_history_snapshot']()
    ns['_live_refresh_trade_history_snapshot']()
    assert registry.get('deal:D1')['dollar_pnl']==.02
    assert ns['_live']['trade_history'][0]['dollar_pnl']==.02
    assert perf==[('deal:D1',.02)]
