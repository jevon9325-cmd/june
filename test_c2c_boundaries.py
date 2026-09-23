"""C2c certification boundaries; fake Redis and captured-shape fixtures only."""
from copy import deepcopy
import unittest
import fakeredis

from broker_ledger import EvidenceError, reconcile_completed_trade, completed_history_view
from broker_cost import build_cost_record
from broker_transaction import normalize_transaction
from broker_pending import PendingCloseStore
from test_broker_ledger import position, realization
from test_broker_cost import comm_row
from test_broker_pending import register, history, COST_EVIDENCE
import json
from unittest.mock import Mock
from test_broker_pending import FaultClient
from redis.exceptions import ConnectionError


class SourceIdentityTests(unittest.TestCase):
    def test_identical_delivery_is_one_observation_not_proof_of_one_event(self):
        row = realization()
        single = reconcile_completed_trade(position(), [row], history_complete=True, costs_complete=True)
        duplicate = reconcile_completed_trade(position(), [row, deepcopy(row)],
                                              history_complete=True, costs_complete=True)
        self.assertEqual(single, duplicate)
        self.assertEqual(len(duplicate['realizations']), 1)
        self.assertEqual(duplicate['identity_state'], 'UNRESOLVED')
        self.assertEqual(duplicate['status'], 'provisional')
        self.assertIsNone(duplicate['net_realized_pnl'])
        with self.assertRaises(EvidenceError):
            completed_history_view(duplicate)

    def test_distinct_realization_periods_survive_shared_metadata(self):
        rows = [realization(quantity='-0.08', period='SEP-26'),
                realization(quantity='-0.08', period='DEC-26')]
        record = reconcile_completed_trade(position(), rows, history_complete=True, costs_complete=True)
        self.assertEqual(len(record['realizations']), 2)
        self.assertEqual(record['gross_realized_pnl'], '-0.32')
        self.assertEqual(record['identity_state'], 'UNRESOLVED')

    def test_distinct_cost_periods_and_timestamps_survive(self):
        rows = [{**comm_row(), 'period': 'SEP-26', 'dateUtc': '2026-09-21T07:00:00'},
                {**comm_row(), 'period': 'DEC-26', 'dateUtc': '2026-09-21T07:00:00'},
                {**comm_row(), 'period': 'DEC-26', 'dateUtc': '2026-09-21T07:00:01'}]
        costs = [build_cost_record(normalize_transaction(row, 'fixture-account'),
                                   'fixture-account', 'opening-1') for row in rows]
        record = reconcile_completed_trade(position(), [realization()], costs + costs,
                                            history_complete=True, costs_complete=True)
        self.assertEqual(len(record['costs']), 3)
        self.assertEqual(record['commissions'], '-27')
        self.assertIsNone(record['net_realized_pnl'])

    def test_caller_booleans_cannot_promote_history_to_identity_proof(self):
        pos = position(identity_evidence_complete=True, broker_event_id='invented')
        record = reconcile_completed_trade(pos, [realization(transactionId='invented')],
                                            history_complete=True, costs_complete=True)
        self.assertFalse(record['provenance']['identity_evidence_complete'])
        self.assertNotEqual(record['status'], 'complete')

    def test_store_repeated_history_stays_provisional_after_restart(self):
        client = fakeredis.FakeRedis()
        store = PendingCloseStore(client, 'fixture-account')
        register(store)
        first = store.reconcile('opening-1', history(), cost_evidence=COST_EVIDENCE)
        restarted = PendingCloseStore(client, 'fixture-account')
        again = restarted.reconcile('opening-1', history(), cost_evidence=COST_EVIDENCE)
        self.assertEqual(first, again)
        self.assertFalse(again['provenance']['economic_evidence_complete'])
        with self.assertRaises(EvidenceError):
            restarted.project_once('opening-1', 'fixture', lambda *_: self.fail('must not consume'))


class LateCollisionTests(unittest.TestCase):
    def setUp(self):
        self.client = fakeredis.FakeRedis()
        self.store = PendingCloseStore(self.client, 'fixture-account')
        register(self.store)
        record = self.store.reconcile('opening-1', history(), cost_evidence=COST_EVIDENCE)
        # A historical on-disk outcome/projection from the old implementation.
        # Do not give the new implementation a test-only certification bypass.
        record.update(status='complete', net_realized_pnl='-0.16', won=False)
        record['provenance']['economic_evidence_complete'] = True
        entry = self.store.get_entry('opening-1')
        entry['record'] = record
        self.client.hset(self.store.key, self.store._field('opening-1'), json.dumps(entry))
        self.prior = {'count': 1, 'net': -0.16}
        self.client.hset(self.store.key, 'consumer:fixture', json.dumps(self.prior))

    def collide(self, store=None):
        return register(store or self.store, position(deal_id='late-collision'))

    def test_late_collision_quarantines_existing_record_and_projection(self):
        self.collide()
        stored = json.loads(self.client.hget(self.store.key, self.store._field('opening-1')))
        self.assertEqual(stored['record']['status'], 'unresolved')
        self.assertIsNone(stored['record']['net_realized_pnl'])
        self.assertEqual(stored['record']['identity_state'], 'AMBIGUOUS')
        projection = self.store.get_projection('fixture')
        self.assertEqual(projection['status'], 'quarantined')
        self.assertIsNone(projection['value'])
        self.assertEqual(projection['prior_projection'], self.prior)
        self.assertTrue(projection['stage_e_recovery_required'])

    def test_collision_retry_and_restart_do_not_wrap_or_count_twice(self):
        self.collide()
        before = self.client.hgetall(self.store.key)
        restarted = PendingCloseStore(self.client, 'fixture-account')
        self.collide(restarted)
        self.assertEqual(before, self.client.hgetall(self.store.key))
        reducer = Mock()
        with self.assertRaises(EvidenceError):
            restarted.project_once('opening-1', 'fixture', reducer)
        reducer.assert_not_called()

    def test_legacy_complete_is_not_certified_by_read_or_scan(self):
        raw = json.loads(self.client.hget(self.store.key, self.store._field('opening-1')))
        with self.assertRaises(EvidenceError):
            completed_history_view(raw['record'])
        self.assertNotEqual(self.store.get_entry('opening-1')['record']['status'], 'complete')
        self.assertNotEqual(next(self.store.iter_entries())['record']['status'], 'complete')
        self.assertEqual(self.store.get_projection('fixture')['status'], 'quarantined')
        captured = self.store.capture('opening-1', 'fixture.retry', {})
        self.assertNotEqual(captured['record']['status'], 'complete')
        receipt = self.store.get_entry('opening-1')['ownership_evidence']
        registered = self.store.register_opening(position(), receipt)
        self.assertNotEqual(registered['record']['status'], 'complete')

    def test_collision_lost_acknowledgement_is_idempotent(self):
        pos = position(deal_id='late-collision')
        receipt = {'source': 'june.accepted_entry_confirm', 'dealId': pos['deal_id'],
                   'dealReference': 'entry-reference', 'dealStatus': 'ACCEPTED'}
        self.store.capture(pos['deal_id'], receipt['source'], {'receipt': receipt, 'position': pos})
        faulty = PendingCloseStore(FaultClient(self.client, 'after'), 'fixture-account')
        with self.assertRaises(ConnectionError):
            faulty.register_opening(pos, receipt)
        before = self.client.hgetall(self.store.key)
        self.store.register_opening(pos, receipt)
        self.assertEqual(before, self.client.hgetall(self.store.key))
        self.assertEqual(self.store.get_projection('fixture')['prior_projection'], self.prior)

    def test_collision_disconnect_before_exec_never_half_invalidates(self):
        pos = position(deal_id='late-collision')
        receipt = {'source': 'june.accepted_entry_confirm', 'dealId': pos['deal_id'],
                   'dealReference': 'entry-reference', 'dealStatus': 'ACCEPTED'}
        self.store.capture(pos['deal_id'], receipt['source'], {'receipt': receipt})
        before = self.client.hgetall(self.store.key)
        faulty = PendingCloseStore(FaultClient(self.client, 'before'), 'fixture-account')
        with self.assertRaises(ConnectionError):
            faulty.register_opening(pos, receipt)
        self.assertEqual(before, self.client.hgetall(self.store.key))
        self.store.register_opening(pos, receipt)
        self.assertEqual(self.store.get_projection('fixture')['status'], 'quarantined')

    def test_watch_retry_quarantines_concurrently_added_aggregate(self):
        pos = position(deal_id='late-collision')
        receipt = {'source': 'june.accepted_entry_confirm', 'dealId': pos['deal_id'],
                   'dealReference': 'entry-reference', 'dealStatus': 'ACCEPTED'}
        self.store.capture(pos['deal_id'], receipt['source'], {'receipt': receipt})
        def concurrent_projection():
            self.client.hset(self.store.key, 'consumer:concurrent', json.dumps({'count': 4}))
        racing = PendingCloseStore(FaultClient(self.client, 'race', concurrent_projection), 'fixture-account')
        racing.register_opening(pos, receipt)
        result = self.store.get_projection('concurrent')
        self.assertEqual(result['status'], 'quarantined')
        self.assertEqual(result['prior_projection'], {'count': 4})

    def test_late_entry_reference_collision_quarantines_commission_owner(self):
        entry = json.loads(self.client.hget(self.store.key, self.store._field('opening-1')))
        entry['record']['costs'] = [{'broker_reference': 'entry-reference', 'cost_id': 'historical-fee'}]
        self.client.hset(self.store.key, self.store._field('opening-1'), json.dumps(entry))
        register(self.store, position(deal_id='different-opening', entry_price='999'))
        self.assertEqual(self.store.get_entry('opening-1')['record']['identity_state'], 'AMBIGUOUS')
        raw = json.loads(self.client.hget(self.store.key, self.store._field('opening-1')))
        self.assertEqual(raw['record']['status'], 'unresolved')
        self.assertEqual(self.store.get_projection('fixture')['prior_projection'], self.prior)
