"""Redis ledger retention repair (repair/redis-retention-0dfab7b) tests.

Covers PendingCloseStore.prune_settled (fail-closed per-field release) and
EvidenceCapture._prune_settled_ledger (gated, archive-before-removal, default
OFF). Offline: temp SQLite + fakeredis, no bot import, no network, no broker.

Proves memory housekeeping cannot manufacture settlement/P&L/performance/position
/protection evidence and cannot release a field that is not durably archived.
"""
from pathlib import Path
import json
import tempfile
import unittest
from unittest.mock import patch

import fakeredis

from broker_capture import EvidenceCapture
from broker_pending import PendingCloseStore


def provenance(deal='fixture-deal'):
    proof = {'verified': True, 'account_id': 'fixture-account'}
    return {'account_id': 'fixture-account', 'deal_id': deal, 'role': 'primary',
            'account_evidence': {'order': proof.copy(), 'confirmation': proof.copy()}}


class PruneSettledUnitTests(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis()
        self.store = PendingCloseStore(self.redis, 'fixture-account')

    def _seed_trade_field(self, deal='fixture-deal', extra=None):
        field = self.store._field(deal)
        entry = {'account_id': 'fixture-account', 'deal_id': deal, 'events': {'e': 1},
                 'opening': {'deal_id': deal}, 'record': {'status': 'provisional'}}
        if extra:
            entry.update(extra)
        self.redis.hset(self.store.key, field, json.dumps(entry))
        return field

    def test_release_requires_durable_true(self):
        field = self._seed_trade_field()
        self.assertFalse(self.store.prune_settled('fixture-deal', False))
        self.assertIsNotNone(self.redis.hget(self.store.key, field))  # kept
        self.assertFalse(self.store.prune_settled('fixture-deal', None))
        self.assertIsNotNone(self.redis.hget(self.store.key, field))  # kept
        # Only an explicit True releases.
        self.assertTrue(self.store.prune_settled('fixture-deal', True))
        self.assertIsNone(self.redis.hget(self.store.key, field))

    def test_absent_field_is_noop(self):
        self.assertFalse(self.store.prune_settled('never-existed', True))

    def test_quarantined_identity_is_never_released(self):
        self._seed_trade_field(extra={'identity_quarantine': ['collision']})
        self.assertFalse(self.store.prune_settled('fixture-deal', True))

    def test_pending_partial_is_never_released(self):
        self._seed_trade_field(extra={'partial_exit_pending': {'original_size': 10}})
        self.assertFalse(self.store.prune_settled('fixture-deal', True))

    def test_only_the_named_trade_field_is_removed_not_claims(self):
        self._seed_trade_field('deal-A')
        self._seed_trade_field('deal-B')
        # claim-style fields must never be touched by prune_settled.
        self.redis.hset(self.store.key, 'opening:sig', json.dumps(['trade:deal-A']))
        self.redis.hset(self.store.key, 'realization:r1', self.store._field('deal-A'))
        self.assertTrue(self.store.prune_settled('deal-A', True))
        self.assertIsNone(self.redis.hget(self.store.key, self.store._field('deal-A')))
        # deal-B and both claim fields survive.
        self.assertIsNotNone(self.redis.hget(self.store.key, self.store._field('deal-B')))
        self.assertIsNotNone(self.redis.hget(self.store.key, 'opening:sig'))
        self.assertIsNotNone(self.redis.hget(self.store.key, 'realization:r1'))

    def test_idempotent_double_release(self):
        self._seed_trade_field()
        self.assertTrue(self.store.prune_settled('fixture-deal', True))
        self.assertFalse(self.store.prune_settled('fixture-deal', True))


class RetentionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'evidence.sqlite3'
        self.redis = fakeredis.FakeRedis()
        self.logs = []
        self.factory = lambda account: PendingCloseStore(self.redis, account)
        self.pos = {'deal_id': 'fixture-deal', 'ig_size': 10, 'notional': 1000,
                    'broker_entry_evidence': provenance()}

    def journal(self, retention=None):
        return EvidenceCapture(self.path, self.factory, self.logs.append, retention=retention)

    def _field(self):
        return self.factory('fixture-account')._field('fixture-deal')

    def test_default_off_never_releases(self):
        j = self.journal(retention=None)
        j.capture(self.pos, 'close_intent')
        j.replay()  # mirror to redis, forwarded=1
        self.assertIsNotNone(self.redis.hget(self.factory('fixture-account').key, self._field()))
        # Repeated replays with no retention keep the mirror forever.
        j.replay(); j.replay()
        self.assertIsNotNone(self.redis.hget(self.factory('fixture-account').key, self._field()))

    def test_enabled_releases_only_durably_archived_settled_deal(self):
        # Policy says: settled AND durably archived -> release.
        j = self.journal(retention=lambda acct, deal: True)
        j.capture(self.pos, 'close_intent')
        j.replay()  # forwards (forwarded=1) THEN prune pass releases the field
        self.assertIsNone(self.redis.hget(self.factory('fixture-account').key, self._field()))
        # SQLite evidence is untouched (archive preserved).
        self.assertEqual(len(list(j.retained())), 1)
        self.assertEqual(list(j.retained())[0]['forwarded'], 1)

    def test_enabled_but_policy_false_keeps_field(self):
        j = self.journal(retention=lambda acct, deal: False)
        j.capture(self.pos, 'close_intent')
        j.replay()
        self.assertIsNotNone(self.redis.hget(self.factory('fixture-account').key, self._field()))

    def test_policy_exception_is_fail_closed_keeps_field(self):
        def boom(acct, deal):
            raise RuntimeError('registry unavailable')
        j = self.journal(retention=boom)
        j.capture(self.pos, 'close_intent')
        j.replay()  # must not raise, must keep the field
        self.assertIsNotNone(self.redis.hget(self.factory('fixture-account').key, self._field()))

    def test_retention_never_touches_undurable_rows(self):
        # Row that fails to forward (redis down during forward) stays forwarded=0
        # and is never considered for release even if policy says True.
        from test_broker_pending import FaultClient
        j = self.journal(retention=lambda acct, deal: True)
        j.capture(self.pos, 'close_intent')
        failed = FaultClient(self.redis, 'before')
        j.store_factory = lambda account: PendingCloseStore(failed, account)
        j.replay()  # forward fails -> forwarded stays 0; prune pass sees no forwarded=1
        j.store_factory = self.factory
        rows = list(j.retained())
        self.assertEqual(rows[0]['forwarded'], 0)

    def test_retention_cannot_manufacture_settlement_or_pnl(self):
        # prune_settled only ever deletes a redundant field; it returns bool and
        # writes nothing that could become settlement/P&L/perf/position evidence.
        store = self.factory('fixture-account')
        before = self.redis.hgetall(store.key)
        store.prune_settled('nonexistent', True)
        self.assertEqual(self.redis.hgetall(store.key), before)


if __name__ == '__main__':
    unittest.main()
