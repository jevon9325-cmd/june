"""Bounded target-evidence fallback and exact live OIL recovery regressions."""
import ast
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from test_live_state_reconciliation import setup

FIXTURE=Path('fixtures/stale_oil_history_20261002.json')
HAS_PROVENANCE=Path('exit_authority.py').exists()


def response_sequence(ns, inventory, responses):
    calls=[]
    def get(path,**kwargs):
        if path=='/positions':return inventory
        assert path=='/history/activity'
        calls.append(kwargs)
        value=responses[min(len(calls)-1,len(responses)-1)]
        if isinstance(value,Exception):raise value
        return deepcopy(value)
    ns['_ig_live_get']=get
    return calls


def unrelated(row, *, wrong_instrument=False):
    row=deepcopy(row)
    if wrong_instrument:row['epic']='OTHER_EPIC'
    else:row['details']['actions'][0]['affectedDealId']='OTHER_DEAL'
    return row


def test_authoritative_narrow_match_uses_one_query_only():
    ns,s,inventory,data,*_=setup()
    calls=response_sequence(ns,inventory,[data,AssertionError('wide should not run')])
    assert ns['_live_broker_close_evidence'](s['open_position'],inventory)['activity']==data['activities'][0]
    assert len(calls)==1


@pytest.mark.parametrize('kind',['empty','other_instrument','same_instrument_wrong_deal','rejected',
                                  'partial','missing_date','old_target','future_target','unaccepted_target'])
def test_no_authoritative_target_match_runs_one_bounded_wider_retry(kind):
    ns,s,inventory,data,*_=setup();a=deepcopy(data['activities'][0])
    if kind=='other_instrument':a=unrelated(a,wrong_instrument=True)
    if kind=='same_instrument_wrong_deal':a=unrelated(a)
    if kind=='rejected':a['status']='REJECTED'
    if kind=='unaccepted_target':a['status']='UNKNOWN'
    if kind=='partial':a['details']['actions'][0]['actionType']='POSITION_PARTIALLY_CLOSED'
    if kind=='missing_date':a.pop('date')
    if kind=='old_target':a['date']=datetime.fromtimestamp(s['open_position']['entry_time']-3600,timezone.utc).isoformat()
    if kind=='future_target':a['date']=datetime.fromtimestamp(ns['time'].time()+3600,timezone.utc).isoformat()
    narrow={'activities':[] if kind=='empty' else [a]}
    calls=response_sequence(ns,inventory,[narrow,data])
    proof=ns['_live_broker_close_evidence'](s['open_position'],inventory)
    assert proof and proof['activity']==data['activities'][0] and len(calls)==2
    assert int(calls[1]['params']['from'])<int(calls[0]['params']['from'])
    assert int(calls[1]['params']['to'])>int(calls[0]['params']['to'])


@pytest.mark.parametrize('bad',['empty','wrong_deal','wrong_epic','partial','rejected','future','old'])
def test_wider_non_authoritative_results_retain_primary_and_ambiguity(bad):
    ns,s,inventory,data,saves,settles,*_=setup();a=deepcopy(data['activities'][0])
    if bad=='wrong_deal':a=unrelated(a)
    if bad=='wrong_epic':a=unrelated(a,wrong_instrument=True)
    if bad=='partial':a['details']['actions'][0]['actionType']='POSITION_PARTIALLY_CLOSED'
    if bad=='rejected':a['status']='REJECTED'
    if bad=='future':a['date']=datetime.fromtimestamp(ns['time'].time()+3600,timezone.utc).isoformat()
    if bad=='old':a['date']=datetime.fromtimestamp(s['open_position']['entry_time']-3600,timezone.utc).isoformat()
    calls=response_sequence(ns,inventory,[{'activities':[unrelated(data['activities'][0])]},
                                         {'activities':[] if bad=='empty' else [a]}])
    old=deepcopy(s['open_position']);ns['_live_poll_position_reconciliation']()
    assert s['open_position']==old and s['orphan_suspected'] and not settles
    assert len(calls)==2


def test_broker_present_skips_history_even_if_wider_has_close():
    ns,s,inventory,data,saves,settles,*_=setup(rows=[{'position':{'dealId':'DIAAAAR9LQ3UZAR','size':.04}}])
    calls=response_sequence(ns,inventory,[data])
    ns['_live_reconcile_positions']()
    assert s['open_position'] and not settles and not calls


def test_partial_and_final_evidence_return_only_authoritative_final():
    ns,s,inventory,data,*_=setup();partial=deepcopy(data['activities'][0])
    partial['details']['actions'][0]['actionType']='POSITION_PARTIALLY_CLOSED'
    calls=response_sequence(ns,inventory,[{'activities':[partial]},
                                         {'activities':[partial,data['activities'][0]]}])
    assert ns['_live_broker_close_evidence'](s['open_position'],inventory)['activity']==data['activities'][0]
    assert len(calls)==2


def test_identical_redelivery_is_single_narrow_match():
    ns,s,inventory,data,*_=setup();a=data['activities'][0]
    calls=response_sequence(ns,inventory,[{'activities':[a,a,a]}])
    assert ns['_live_broker_close_evidence'](s['open_position'],inventory)
    assert len(calls)==1


def test_genuine_narrow_conflict_cannot_be_erased_by_wider_unique_result():
    ns,s,inventory,data,*_=setup();a=deepcopy(data['activities'][0]);a['dealId']='CLOSE_A'
    b=deepcopy(a);b['dealId']='CLOSE_B'
    calls=response_sequence(ns,inventory,[{'activities':[a,b]},{'activities':[a]}])
    assert ns['_live_broker_close_evidence'](s['open_position'],inventory) is None
    assert len(calls)==2


@pytest.mark.parametrize('bad',[None,{}, {'activities':None},
                                {'activities':[],'metadata':{'paging':{'next':'untrusted'}}},
                                TimeoutError('history unavailable')])
@pytest.mark.parametrize('which',['narrow','wide'])
def test_missing_failed_truncated_history_fail_closed(bad,which):
    ns,s,inventory,data,saves,settles,*_=setup()
    responses=[bad,data] if which=='narrow' else [{'activities':[]},bad]
    calls=response_sequence(ns,inventory,responses)
    old=deepcopy(s['open_position']);ns['_live_poll_position_reconciliation']()
    assert s['open_position']==old and s['orphan_suspected'] and not settles
    assert len(calls)==(1 if which=='narrow' else 2)


def oil_setup():
    fixture=json.loads(FIXTURE.read_text());ns,s,*_=setup()
    s.update(open_position=deepcopy(fixture['position']),orphan_suspected=True,
             manual_review_required=True,position_reconciled_at=0)
    ns['time']=SimpleNamespace(time=lambda:datetime.fromisoformat(fixture['observed_utc']).timestamp())
    ns['INSTRUMENTS']={'OIL':'CC.D.LCO.BMU.IP'}
    calls=response_sequence(ns,fixture['inventory'],[fixture['narrow'],fixture['wider']])
    return ns,s,fixture,calls


def old_helper(ns):
    source=subprocess.check_output(['git','show','c4cd227:june.py'],text=True)
    node=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef)
              and n.name=='_live_broker_close_evidence')
    exec(compile(ast.Module(body=[node],type_ignores=[]),'c4_baseline_helper','exec'),ns)


def test_exact_real_oil_fixture_reproduces_c4_nonempty_failure():
    ns,s,fixture,calls=oil_setup();old_helper(ns)
    assert len(fixture['narrow']['activities'])==4
    assert not any(a.get('affectedDealId')==s['open_position']['deal_id']
                   for row in fixture['narrow']['activities']
                   for a in (row.get('details') or {}).get('actions',[]))
    assert ns['_live_broker_close_evidence'](s['open_position'],fixture['inventory']) is None
    assert len(calls)==1


def test_exact_real_oil_fixture_returns_broker_close_without_inventing_pnl():
    ns,s,fixture,calls=oil_setup()
    proof=ns['_live_broker_close_evidence'](s['open_position'],fixture['inventory'])
    assert len(calls)==2 and proof['activity']['dealId']=='DIAAAAR9L7HMNAH'
    assert proof['activity']['date']=='2026-10-02T06:13:54' and 'pnl' not in proof


def test_real_oil_startup_persistence_and_restart_clear_only_with_authority():
    ns,s,fixture,calls=oil_setup();saves=[];settled=[]
    ns['_live_save_state']=lambda:saves.append(json.loads(json.dumps(s)))
    ns['_live_settle_primary_exit']=lambda *a,**kw:settled.append((a,kw))
    ns['_ig_live_post']=Mock(side_effect=AssertionError('recovery must not submit'))
    ns['_ig_live_put']=Mock(side_effect=AssertionError('recovery must not amend stop'))
    before=deepcopy(s)
    ns['_live_reconcile_positions']()
    assert s['open_position'] is None and not s.get('orphan_suspected') and not s.get('manual_review_required')
    assert saves[-1]['open_position'] is None and len(settled)==1
    assert settled[0][1]['confirmed_pnl'] is None and settled[0][1]['confirmed_exit_price'] is None
    s.clear();s.update(saves[-1]);ns['_live_reconcile_positions']()
    assert s['open_position'] is None and len(settled)==1
    for key in ('balance_day_start','risk_epoch','cb_active','kill_switch'):
        assert s[key]==before[key]
    ns['_ig_live_post'].assert_not_called();ns['_ig_live_put'].assert_not_called()


def test_restart_with_real_open_oil_remains_open_without_history_queries():
    ns,s,fixture,calls=oil_setup()
    inventory={'positions':[{'position':{'dealId':s['open_position']['deal_id'],'size':.04}}]}
    calls=response_sequence(ns,inventory,[fixture['wider']]);saves=[]
    ns['_live_save_state']=lambda:saves.append(deepcopy(s))
    old=deepcopy(s['open_position']);ns['_live_reconcile_positions']();ns['_live_save_state']()
    s.clear();s.update(saves[-1]);ns['_live_reconcile_positions']()
    assert s['open_position']['deal_id']==old['deal_id'] and not calls


@pytest.mark.skipif(not HAS_PROVENANCE,reason='run after integrating existing 7afccca')
def test_integrated_oil_recovery_is_unknown_authority_not_manual_or_software():
    from test_exit_authority import canonical_harness
    ns,s,fixture,calls=oil_setup();rows,perf=canonical_harness(ns,s)
    # canonical_harness adds SILVER economic fixtures; restore the actual OIL basis.
    s['open_position']=deepcopy(fixture['position'])
    ns['_live_reconcile_positions']()
    record=json.loads(rows[0])
    assert s['open_position'] is None and len(rows)==1 and not perf
    assert record['exit_authority']=='UNKNOWN' and not record['operator_intervention']
    assert record['dollar_pnl'] is None and record['settlement_state']=='PROVISIONAL'
    assert record['stop_sync']['status']=='pending' # forensic record only, no active primary
    # An old state snapshot plus the durable history cannot duplicate accounting.
    s['open_position']=deepcopy(fixture['position']);s['settled_primary_keys']=[]
    ns['_live_reconcile_positions']()
    assert s['open_position'] is None and len(rows)==1 and not perf


@pytest.mark.skipif(not HAS_PROVENANCE,reason='run after integrating existing 7afccca')
def test_integrated_manual_close_uses_relevant_wide_evidence_without_learning():
    from test_exit_authority import canonical_harness
    ns,s,fixture,calls=oil_setup();fixture['wider']=deepcopy(fixture['wider'])
    for a in fixture['wider']['activities']:
        if any(x.get('actionType')=='POSITION_CLOSED' for x in a['details']['actions']):
            a['channel']='MOBILE'
    calls=response_sequence(ns,fixture['inventory'],[fixture['narrow'],fixture['wider']])
    rows,perf=canonical_harness(ns,s);s['open_position']=deepcopy(fixture['position'])
    ns['_live_reconcile_positions']();record=json.loads(rows[0])
    assert s['open_position'] is None and len(rows)==1 and not perf
    assert record['exit_authority']=='EXTERNAL_OPERATOR' and record['operator_intervention']
    assert record['accounting_eligible'] and not record['strategy_learning_eligible']
