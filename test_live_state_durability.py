import ast
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import subprocess
from unittest.mock import Mock, patch

import pytest
from live_state_integrity import assess, guard_startup, StateRecoveryRequired
import live_state_durability as durable

ROOT = Path(__file__).resolve().parent
OPEN = json.loads((ROOT/'fixtures/silver_restart_opening_20261004.json').read_text())
PARTIAL = json.loads((ROOT/'fixtures/silver_restart_partial_20261004.json').read_text())


def flat():
    state = deepcopy(OPEN['state'])
    state.update(open_position=None, pyramid_legs=[])
    return state


def client(state):
    raw = json.dumps(state) if state is not None else None
    r = Mock()
    r.get.side_effect = lambda key: raw if key == 'june_live_state' else None
    r.exists.return_value = False
    r.scan_iter.return_value = iter(())
    return r


def functions(state, tmp_path, source=None):
    tree = ast.parse(source or (ROOT/'june.py').read_text(encoding='utf-8'))
    names = {'_live_load_state', '_live_reconcile_positions', '_live_poll_balance',
             '_live_save_state', '_live_persist_state', '_live_startup'}
    ns = {'_live':state, 'json':json, 'time':__import__('time'),
          'datetime':datetime, 'timezone':timezone, '__file__':str(tmp_path/'june.py'),
          '_live_capture_active':Mock(), '_live_validate_rolling_state_on_load':Mock(),
          '_live_reconcile_fuel_reservation':Mock(), '_live_log':Mock(),
          '_live_capture_evidence':Mock(), '_live_observe':Mock(),
          '_redis':Mock(return_value=client(state)),
          '_live_balance_polled_at':0, '_LIVE_POLL_INTERVAL':300,
          '_live_expire_prior_epoch_global_defensive':Mock(),
          '_live_broker_close_evidence':Mock(return_value=None),
          '_live_settle_primary_exit':Mock(side_effect=AssertionError('No fabricated settlement')),
          '_live_finalize_reconciled_stale_primary':Mock(side_effect=AssertionError('No fabricated clear')),
          '_PYRAMID_MAX_LEGS':3, '_recon_consecutive_404_with_deposit':0,
          'INSTRUMENTS':{'SILVER':OPEN['broker']['positions'][0]['market']['epic']}}
    nodes=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]), '<real persistence and reconciliation>', 'exec'), ns)
    return ns


def test_reproduce_exact_1520b57_loader_crash_before_repair(tmp_path):
    source=subprocess.check_output(['git','show','1520b57:june.py'], cwd=ROOT, text=True, encoding='utf-8')
    ns=functions({},tmp_path,source)
    assert ns['_live_load_state'](persisted_state=deepcopy(OPEN['state'])) is False
    # The real supplement emits the warning that reaches the unbound logging.
    from rolling_build4a import b4a_validate_rolling_state_extended
    assert any('MISSING_CAMPAIGN_ID' in v for v in b4a_validate_rolling_state_extended(OPEN['state']))


@pytest.mark.parametrize('fixture', [OPEN, PARTIAL])
def test_actual_crashing_and_partial_shapes_load_and_reconcile_idempotently(tmp_path, fixture):
    original=deepcopy(fixture['state']);ns=functions({},tmp_path)
    ns['_ig_live_get']=Mock(return_value=deepcopy(fixture['broker']))
    # Reconciliation saves go to an isolated checkpoint/Redis mock only.
    for _ in range(2):
        assert ns['_live_load_state'](persisted_state=deepcopy(original))
        ns['_live_reconcile_positions']()
        restored=ns['_live']
        for key in ('balance_day_start','balance_day_start_date','global_mode',
                    'failed_theses','streak_state','cumulative_earned_pnl', 'total_trades'):
            assert restored[key] == original[key]
        for key in ('deal_id','direction','ig_size','fill_price','broker_entry_evidence',
                    'original_ig_size','partial_exit_done','partial_dollar_pnl'):
            if key in original['open_position']:
                assert restored['open_position'][key] == original['open_position'][key]
        assert restored['pyramid_legs']==[]
        assert not restored.get('orphan_suspected')
        ns['_live_settle_primary_exit'].assert_not_called()
    assert original['open_position']['deal_id']=='DIAAAAR9UUY48A4'
    if fixture is PARTIAL:
        assert restored['open_position']['original_ig_size']==0.04
        assert restored['open_position']['ig_size']==0.02
        assert restored['open_position']['partial_exit_done']


@pytest.mark.parametrize('legacy', [False, True])
def test_complete_and_optional_legacy_fields_preserved_without_fresh_defaults(tmp_path, legacy):
    state=flat()
    if legacy:
        for key in ('htf_combo_outcomes', 'rolling_liquidity_before', 'entry_quant_diagnostics'):
            state.pop(key, None)
    ns=functions({'sentinel':'prior'},tmp_path)
    assert ns['_live_load_state'](persisted_state=state)
    for key,value in state.items():assert ns['_live'][key]==value


@pytest.mark.parametrize('raw', [None, '', '{', '{}', 'null', '[]'])
def test_loader_missing_malformed_partial_never_mutates_existing_state(tmp_path, raw):
    ns=functions({'sentinel':'unchanged'},tmp_path)
    ns['_redis']().get.side_effect=None;ns['_redis']().get.return_value=raw
    assert ns['_live_load_state']() is False
    assert ns['_live']=={'sentinel':'unchanged'}


def test_loader_redis_read_error_preserves_prior_state(tmp_path):
    ns=functions({'sentinel':'unchanged'},tmp_path)
    ns['_redis']().get.side_effect=ConnectionError('read failed')
    assert ns['_live_load_state']() is False
    assert ns['_live']=={'sentinel':'unchanged'}


@pytest.mark.parametrize('broker', [[], OPEN['broker']['positions']])
def test_missing_live_state_and_both_missing_fail_closed_even_with_durable_image(tmp_path, broker):
    durable.checkpoint(tmp_path, flat())
    r=client(None)
    with pytest.raises(StateRecoveryRequired):
        guard_startup(r,tmp_path,Mock(return_value={'positions':broker}))
    r.set.assert_not_called()


def test_missing_auxiliary_baseline_preserves_known_embedded_truth(tmp_path):
    state=flat();r=client(state)
    assert guard_startup(r,tmp_path,Mock())==state
    assert durable.baseline_for_day(r,tmp_path,state['balance_day_start_date'],state)==117.74
    r.set.assert_not_called();r.setex.assert_not_called()


def test_durable_checkpoint_preserves_all_account_and_campaign_authorities(tmp_path):
    state=deepcopy(PARTIAL['state']);r=client(state)
    durable.persist_state(r,tmp_path,state)
    assert durable.read_checkpoint(tmp_path)==state
    assert durable.read_baseline(tmp_path,'2026-10-05')==117.74
    assert r.set.call_args.kwargs=={}  # no TTL: not a volatile-LRU victim
    assert guard_startup(r,tmp_path,Mock())==state


def test_redis_oom_keeps_checkpoint_and_detects_stale_redis_on_restart(tmp_path):
    state=flat();r=client(state);durable.persist_state(r,tmp_path,state)
    newer=deepcopy(state);newer['failed_theses']['SILVER_short']={'failure_id':'new-durable'}
    r.set.side_effect=RuntimeError('OOM command not allowed')
    with pytest.raises(RuntimeError, match='OOM'):
        durable.persist_state(r,tmp_path,newer)
    assert durable.read_checkpoint(tmp_path)==newer
    with pytest.raises(StateRecoveryRequired, match='divergent'):
        guard_startup(r,tmp_path,Mock())


def test_redis_unacknowledged_write_is_not_success(tmp_path):
    r=client(flat());r.set.return_value=False
    with pytest.raises(RuntimeError,match='not acknowledged'):
        durable.persist_state(r,tmp_path,flat())
    assert durable.read_checkpoint(tmp_path)==flat()


def test_disk_failure_prevents_pre_submit_redis_write(tmp_path):
    r=client(flat())
    with patch.object(durable,'checkpoint',side_effect=OSError('disk full')):
        with pytest.raises(OSError):durable.persist_state(r,tmp_path,flat())
    r.set.assert_not_called()


def test_corrupt_checkpoint_rejected_without_redis_repair(tmp_path):
    state=flat();durable.checkpoint(tmp_path,state)
    with sqlite3.connect(tmp_path/durable.NAME) as db:
        db.execute("UPDATE checkpoint SET sha256='corrupt'")
    with pytest.raises(StateRecoveryRequired):guard_startup(client(state),tmp_path,Mock())


@pytest.mark.parametrize('value', ['NaN', '0', 'malformed', '118.23'])
def test_conflicting_or_unreadable_baseline_fails_closed(tmp_path, value):
    state=flat();r=client(state)
    r.get.side_effect=lambda key:json.dumps(state) if key=='june_live_state' else value
    with pytest.raises(StateRecoveryRequired):
        durable.baseline_for_day(r,tmp_path,'2026-10-05',state)


def test_baseline_read_failure_is_not_absence(tmp_path):
    r=client(flat());r.get.side_effect=ConnectionError()
    with pytest.raises(StateRecoveryRequired):durable.baseline_for_day(r,tmp_path,'2026-10-05',flat())


@pytest.mark.parametrize('cached', [None, '120.0'])
def test_rollover_present_or_missing_cache_preserves_existing_seed_policy(tmp_path,cached):
    state=flat();state['balance_day_start_date']='2026-10-04'
    ns=functions(state,tmp_path)
    class FixedDate(datetime):
        @classmethod
        def now(cls,tz=None):return cls(2026,10,5,tzinfo=timezone.utc)
    ns['datetime']=FixedDate
    ns['_redis']().get.side_effect=lambda key:cached
    ns['_ig_live_get']=Mock(return_value={'accounts':[{'preferred':True,'balance':{
        'available':118.0,'deposit':2.0,'balance':120.0,'profitLoss':1.0}}]})
    ns['_live_poll_balance']()
    assert state['balance_day_start']==120.0
    assert state['balance_day_start_date']=='2026-10-05'
    assert durable.read_checkpoint(tmp_path)['balance_day_start']==120.0
    assert durable.read_baseline(tmp_path,'2026-10-05')==120.0
    ns['_live_poll_balance']()
    assert state['balance_day_start']==120.0


def test_same_day_missing_cache_never_reseeds_from_current_broker_cash(tmp_path):
    state=flat();ns=functions(state,tmp_path)
    ns['_ig_live_get']=Mock(return_value={'accounts':[{'preferred':True,'balance':{
        'available':118.0,'deposit':2.0,'balance':120.0,'profitLoss':1.0}}]})
    # Freeze date to fixture date regardless of real test date.
    class FixedDate(datetime):
        @classmethod
        def now(cls,tz=None):return cls(2026,10,5,tzinfo=timezone.utc)
    ns['datetime']=FixedDate
    ns['_live_poll_balance']()
    assert state['balance_day_start']==117.74
    assert durable.read_checkpoint(tmp_path)['balance_day_start']==117.74
    ns['_redis']().setex.assert_not_called()


def test_rollover_baseline_oom_survives_durably_and_cannot_be_reseeded(tmp_path):
    r=client(flat());r.setex.side_effect=RuntimeError('OOM')
    durable.persist_baseline(r,tmp_path,'2026-10-06',120.0)
    assert durable.baseline_for_day(r,tmp_path,'2026-10-06',flat())==120.0
    with pytest.raises(RuntimeError,match='Conflicting'):
        durable.persist_baseline(r,tmp_path,'2026-10-06',121.0)
    assert durable.read_baseline(tmp_path,'2026-10-06')==120.0


def test_no_strategy_policy_change_in_balance_poll():
    old=ast.parse(subprocess.check_output(['git','show','1520b57:june.py'],cwd=ROOT,text=True,encoding='utf-8'))
    new=ast.parse((ROOT/'june.py').read_text(encoding='utf-8'))
    def poll(tree):return next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_live_poll_balance')
    # Only persistence/lookup statements in the rollover branch changed.
    def normalized(node):
        for n in ast.walk(node):
            if isinstance(n,ast.If) and ast.unparse(n.test).startswith("_live.get('balance_day_start_date')"):
                marker=next(i for i,x in enumerate(n.body) if isinstance(x,ast.If) and ast.unparse(x.test).startswith("_live.get('global_mode')"))
                n.body=n.body[marker:]
        return ast.dump(node)
    assert normalized(poll(old))==normalized(poll(new))
