import json,sqlite3,copy
from pathlib import Path
import pytest
from decision_ledger import Recorder,Store,encoded,unpack,state_keys

def fixture():return json.loads(Path(__file__).with_name('fixtures').joinpath('natgas_prebroker_abort_20261005.json').read_text())['state']

def test_exact_live_account_gate_payload_repeated_cycles(tmp_path):
    state=fixture();rec=Recorder(Store(tmp_path/'ledger.sqlite3'),asynchronous=False)
    # Actual instrument-mode predicate from the deployed selector, evaluated
    # against preserved production state with its 257851-byte account history.
    expression='_live.get("instrument_mode", {}).get(sym, "normal") == "defensive"'
    for n in range(40):
        cid=rec.begin({}, {'_live':state})
        rec.note('universe',{'signals':{'GOLD':{'direction':'bull'}}},{'_sim_eligible':['GOLD']})
        for rank in range(35):
            rec.note('gate',{'sym':'GOLD','vol':.4},{'_live':state},function='_live_select_instrument',
                source_line=11390,expression=expression,input_names=('_live','sym'),gate_name='fixture',result='PASS')
        rec.note('score_rank',{'sym':'GOLD','vol':.4,'eff_vol':.2,'weight':.5,'corr_adj':1.},{})
        rec.note('finish',{}, {})
        with sqlite3.connect(rec.store.path) as db:
            row=db.execute('SELECT complete FROM decision_cycles WHERE decision_cycle_id=?',(cid,)).fetchone()
            assert row==(1,)
            c=db.execute('SELECT raw_score,final_score FROM decision_candidates WHERE decision_cycle_id=?',(cid,)).fetchone()
            assert c==(.4,.2)
            events=[unpack(x[0]) for x in db.execute("SELECT payload FROM decision_events WHERE decision_cycle_id=? AND kind='GATE'",(cid,))]
            assert len(events)==35
            assert all(e['inputs']['_live']=={'instrument_mode':state['instrument_mode']} for e in events)
            assert all(e['inputs']['_live_input_keys']==['instrument_mode'] for e in events)
    assert rec.failures==rec.drops==0

@pytest.mark.parametrize('expression,keys',[
    ('_live.get("open_position") or _live.get("pyramid_entry_pending")',('open_position','pyramid_entry_pending')),
    ('_live["global_mode"] == "normal"',('global_mode',)),
    ('_live.get(key)',None),('_live',None),('f(_live)',None),('bad syntax',None)])
def test_predicate_projection_fail_closed(expression,keys):assert state_keys(expression)==keys

def test_no_strategy_or_state_mutation(tmp_path):
    state=fixture();original=copy.deepcopy(state);rec=Recorder(Store(tmp_path/'x.sqlite3'),asynchronous=False)
    rec.begin({}, {'_live':state})
    rec.note('gate',{}, {'_live':state},expression='_live.get("unknown") is None',input_names=('_live',),result='PASS')
    event=rec._local.doc['events'][-1]
    assert event['inputs']['_live']=={} and event['inputs']['_live_input_keys']==['unknown']
    assert state==original
