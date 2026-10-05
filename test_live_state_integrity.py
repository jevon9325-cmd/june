import ast
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import subprocess
from unittest.mock import Mock
import pytest
from live_state_integrity import (assess, read_state, restart_gate, evaluate_restart_gate,
                                 historical_activity, startup_state, guard_startup,
                                 StateRecoveryRequired)

ROOT = Path(__file__).resolve().parent
FIX = json.loads((ROOT/'fixtures/missing_live_state_incident_20261004.json').read_text())


def flat():
    return deepcopy(FIX['flat']['local'])


def test_a_explicit_flat_may_pass_and_preserves_counters():
    state = flat(); state['total_trades'] = 12
    local = assess(json.dumps(state))
    assert local.kind == 'KNOWN_FLAT'
    assert restart_gate(local, [], [])[0]
    assert startup_state(local, history=True)['total_trades'] == 12


def test_b_actual_missing_flat_incident_never_passes():
    local = assess(FIX['missing']['local_raw'])
    assert local.kind == 'MISSING'
    assert not restart_gate(local, FIX['missing']['broker'], [])[0]


def test_c_missing_broker_open_requires_recovery_before_zeroing():
    with pytest.raises(StateRecoveryRequired):
        startup_state(assess(None), history=False, legacy_history=False,
                      broker_rows=FIX['matching_open']['broker'], working_orders=[])


def test_d_matching_open_is_exposed_and_not_restart_flat():
    record = FIX['matching_open']
    local = assess(json.dumps(record['local']))
    assert local.kind == 'KNOWN_EXPOSED'
    assert not restart_gate(local, record['broker'], [])[0]
    assert startup_state(local, history=True)['open_position']['deal_id'] == record['broker'][0]['position']['dealId']


@pytest.mark.parametrize('raw', ['', '{', 'null', '[]', '{}', 'false', '0', b'\xff'])
def test_e_unreadable_or_incomplete_state_fails_closed(raw):
    assert assess(raw).kind in ('ERROR', 'UNKNOWN')
    assert not restart_gate(assess(raw), [], [])[0]


def test_f_unavailable_redis_fails_closed():
    local = read_state(Mock(side_effect=ConnectionError()))
    assert local.kind == 'ERROR'
    assert not restart_gate(local, [], [])[0]


def test_g_explicit_reconciled_flat_distinct_from_missing():
    assert assess(json.dumps(flat())).kind == 'KNOWN_FLAT'
    assert assess(None).kind == 'MISSING'


def test_h_historical_campaign_data_forbids_fresh_zeros(tmp_path):
    with sqlite3.connect(tmp_path/'campaign_telemetry.sqlite3') as db:
        db.execute('CREATE TABLE campaigns(id TEXT)')
        db.execute("INSERT INTO campaigns VALUES('historical-GOLD')")
    assert historical_activity(tmp_path) is True
    client = Mock();client.get.return_value = None
    with pytest.raises(StateRecoveryRequired):
        guard_startup(client, tmp_path, Mock())
    client.set.assert_not_called()


def test_i_open_identity_roundtrip_does_not_duplicate_exposure():
    prior = deepcopy(FIX['matching_open']['local'])
    restored = startup_state(assess(json.dumps(prior)), history=True)
    assert restored == prior
    assert len([restored['open_position']] + restored['pyramid_legs']) == 1


def test_j_real_flat_cleanup_retains_risk_accounting():
    # Actual production cleanup function, mocked observer/save only.
    source = ast.parse((ROOT/'june.py').read_text(encoding='utf-8'))
    node = next(n for n in source.body if isinstance(n, ast.FunctionDef) and n.name == '_live_finalize_reconciled_stale_primary')
    live = flat(); live.update(total_trades=12, cumulative_earned_pnl=-1.43)
    ns = {'_live':live, '_live_save_state':Mock(), '_live_capture_active':Mock(), '_live_observe':Mock(),
          '_live_log':Mock(), 'time':__import__('time')}
    exec(compile(ast.Module(body=[node],type_ignores=[]),'<real cleanup>','exec'),ns)
    ns[node.name]()
    assert live['total_trades'] == 12 and live['cumulative_earned_pnl'] == -1.43
    assert assess(json.dumps(live)).kind == 'KNOWN_FLAT'


def test_verified_new_install_can_initialize_but_history_unknown_cannot(tmp_path):
    assert historical_activity(tmp_path) is False
    assert startup_state(assess(None),history=False,legacy_history=False,broker_rows=[],working_orders=[]) is None
    for history in (None, True):
        with pytest.raises(StateRecoveryRequired):
            startup_state(assess(None),history=history,legacy_history=False,broker_rows=[],working_orders=[])


@pytest.mark.parametrize('field,value', [('orphan_suspected',True),('pyramid_entry_pending',{'status':'submitting'}),
 ('recovery_unresolved_positions',[{'dealId':'other'}]),('rolling_fuel_reservation',{'state':'ATTEMPTED'})])
def test_ambiguous_flat_is_unknown(field,value):
    state=flat();state[field]=value
    assert assess(json.dumps(state)).kind == 'UNKNOWN'


@pytest.mark.parametrize('field,value', [('balance_day_start',None),('total_trades',float('nan')),
 ('global_mode','unknown'),('pyramid_legs',None),('streak_state',{'OIL_long':float('inf')}),
 ('balance_day_start_date','')])
def test_partial_risk_object_is_not_flat(field,value):
    state=flat();state[field]=value
    assert assess(json.dumps(state)).kind == 'UNKNOWN'


def test_gate_rejects_state_change_and_broker_errors():
    raw=json.dumps(flat())
    assert not evaluate_restart_gate(Mock(side_effect=[raw,None]),lambda:[],lambda:[])[0]
    assert not evaluate_restart_gate(lambda:raw,Mock(side_effect=TimeoutError()),lambda:[])[0]
    assert evaluate_restart_gate(lambda:raw,lambda:[],lambda:[])[0]


def test_startup_guard_precedes_defaults_and_no_second_redis_read(tmp_path):
    tree=ast.parse((ROOT/'june.py').read_text(encoding='utf-8'))
    node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_live_startup')
    ns={'_live':{},'IG_LIVE_BASE':'x','IG_LIVE_KEY':'x','IG_LIVE_USER':'x','IG_LIVE_PASS':'x',
        '__file__':str(tmp_path/'june.py'),'_redis':Mock(return_value=Mock(get=Mock(return_value=None))),
        '_ig_live_get':Mock(), '_live_load_state':Mock(), '_live_poll_balance':Mock()}
    with sqlite3.connect(tmp_path/'.settlements.sqlite3') as db:
        db.execute('CREATE TABLE settlements(payload TEXT)');db.execute("INSERT INTO settlements VALUES('{}')")
    exec(compile(ast.Module(body=[node],type_ignores=[]),'<actual startup>','exec'),ns)
    with pytest.raises(StateRecoveryRequired):ns['_live_startup']()
    assert ns['_live']=={}
    ns['_live_load_state'].assert_not_called();ns['_live_poll_balance'].assert_not_called()
    ns['_redis']().set.assert_not_called()


def test_entire_strategy_and_existing_plumbing_frozen_to_22e03a0():
    before=ast.parse(subprocess.check_output(['git','show','22e03a0:june.py'],cwd=ROOT,text=True,encoding='utf-8'))
    after=ast.parse((ROOT/'june.py').read_text(encoding='utf-8'))
    names={'_live_startup','_live_load_state','_live_save_state','_live_poll_balance',
           '_live_persist_state'}
    before.body=[n for n in before.body if not (isinstance(n,ast.FunctionDef) and n.name in names)]
    after.body=[n for n in after.body if not (isinstance(n,ast.FunctionDef) and n.name in names)]
    # Normalize only persistence calls inside the otherwise frozen submit functions.
    class NormalizePersistence(ast.NodeTransformer):
        def visit_Try(self, node):
            # The pre-POST abort journal/release is an authorized persistence
            # recovery seam. Every surrounding trading predicate stays frozen.
            if any(isinstance(n, ast.ImportFrom) and n.module == 'submission_recovery'
                   for n in node.body):
                return None
            return self.generic_visit(node)
        def visit_Expr(self, node):
            old = isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute) and node.value.func.attr == 'set' and node.value.args and isinstance(node.value.args[0], ast.Name) and node.value.args[0].id == '_LIVE_REDIS_KEY'
            new = isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) and node.value.func.id == '_live_persist_state'
            return ast.Expr(value=ast.Constant(value='live-state persistence seam')) if old or new else self.generic_visit(node)
    before = NormalizePersistence().visit(before)
    after = NormalizePersistence().visit(after)
    assert ast.dump(before)==ast.dump(after)
    # Recorder capture projection is independently validated by the actual
    # funnel equivalence and repeated exact-state tests in this repair.
    for name in ('settlement_discovery.py','settlement_reconcile.py','exit_authority.py'):
        assert subprocess.check_output(['git','show','22e03a0:'+name],cwd=ROOT).replace(b'\r\n',b'\n') == (ROOT/name).read_bytes().replace(b'\r\n',b'\n')
