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
