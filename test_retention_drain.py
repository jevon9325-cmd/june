import sqlite3,json
from unittest.mock import patch
import fakeredis
from broker_capture import EvidenceCapture
from broker_pending import PendingCloseStore
from test_ledger_retention import provenance

def test_old_backlog_drains_despite_newest_unsettled_events(tmp_path):
    r=fakeredis.FakeRedis();factory=lambda a:PendingCloseStore(r,a)
    settled={'old'};j=EvidenceCapture(tmp_path/'e.sqlite3',factory,lambda _:None)
    for deal in ['old']+['new'+str(n) for n in range(80)]:
        pos={'deal_id':deal,'ig_size':1,'notional':1,'broker_entry_evidence':provenance(deal)}
        j.capture(pos,'close_intent');j.replay(1)
    assert r.hget(factory('fixture-account').key,factory('fixture-account')._field('old'))
    j.retention=lambda a,d:d in settled
    j._prune_settled_ledger(1)
    assert r.hget(factory('fixture-account').key,factory('fixture-account')._field('old')) is None
    with sqlite3.connect(j.path) as db:
        archives=db.execute('SELECT payload,digest FROM ledger_archives WHERE deal=?',('old',)).fetchall()
        assert archives and json.loads(archives[0][0])['deal_id']=='old'
    # Restart is idempotent; no broker/economic mutation, archive remains exact.
    EvidenceCapture(j.path,factory,lambda _:None,retention=j.retention).replay(1)
    assert r.hget(factory('fixture-account').key,factory('fixture-account')._field('new79'))

def test_archive_failure_never_releases(tmp_path):
    r=fakeredis.FakeRedis();factory=lambda a:PendingCloseStore(r,a)
    j=EvidenceCapture(tmp_path/'e.sqlite3',factory,lambda _:None,retention=lambda a,d:True)
    pos={'deal_id':'old','broker_entry_evidence':provenance('old')};j.capture(pos,'snapshot')
    j.retention=None;j.replay();j.retention=lambda a,d:True
    with patch('hashlib.sha256',side_effect=OSError('archive unavailable')):j._prune_settled_ledger(1)
    assert r.hget(factory('fixture-account').key,factory('fixture-account')._field('old'))

def test_version_match_prevents_release_of_new_evidence():
    r=fakeredis.FakeRedis();store=PendingCloseStore(r,'fixture-account')
    store.capture('d','x',{'a':1});raw=r.hget(store.key,store._field('d'))
    store.capture('d','x',{'a':2})
    assert store.prune_settled('d',True,expected_payload=raw) is False
    current=r.hget(store.key,store._field('d'))
    assert store.prune_settled('d',True,expected_payload=current) is True
    assert store.prune_settled('d',True,expected_payload=current) is False

def test_release_failure_archive_survives_restart_exactly_once(tmp_path):
    r=fakeredis.FakeRedis();factory=lambda a:PendingCloseStore(r,a)
    j=EvidenceCapture(tmp_path/'e.sqlite3',factory,lambda _:None)
    j.capture({'deal_id':'old','broker_entry_evidence':provenance('old')},'snapshot');j.replay()
    store=factory('fixture-account');original=r.hget(store.key,store._field('old'))
    j.retention=lambda a,d:True
    with patch.object(PendingCloseStore,'prune_settled',side_effect=OSError('Redis release failed')):
        j._prune_settled_ledger(1)
    assert r.hget(store.key,store._field('old'))==original
    with sqlite3.connect(j.path) as db:
        payload=db.execute('SELECT payload FROM ledger_archives').fetchone()[0]
        assert payload.encode()==original
    restarted=EvidenceCapture(j.path,factory,lambda _:None,retention=j.retention)
    restarted.replay(1);restarted.replay(1)
    assert not r.hget(store.key,store._field('old'))
    with sqlite3.connect(j.path) as db:
        assert db.execute('SELECT COUNT(*) FROM ledger_archives').fetchone()[0]==1
        assert json.loads(payload)['deal_id']=='old' # complete recovery copy remains
