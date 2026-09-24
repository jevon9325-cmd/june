"""Disk/Redis failures and actual extracted lifecycle functions; offline only."""
import ast
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import fakeredis

from broker_capture import EvidenceCapture
from broker_pending import PendingCloseStore
from test_broker_pending import FaultClient
from test_live_accounting import close_harness
from test_broker_identity import TREE


class Crash(BaseException):
    pass


def provenance(deal='fixture-deal', role='primary'):
    proof = {'verified': True, 'account_id': 'fixture-account'}
    return {'account_id': 'fixture-account', 'deal_id': deal, 'role': role,
            'account_evidence': {'order': proof.copy(), 'confirmation': proof.copy()},
            'submitted_order': {'size': 10}, 'broker_quantity': 10,
            'broker_opened_utc': None, 'identity_status': 'pending_broker_opening'}


def extract(names, ns):
    tree = TREE  # immutable source snapshot for this offline test process
    functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {f.name for f in functions} == set(names)
    exec(compile(ast.Module(body=functions, type_ignores=[]), 'actual_june_lifecycle', 'exec'), ns)


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'evidence.sqlite3'
        self.redis = fakeredis.FakeRedis()
        self.logs = []
        self.factory = lambda account: PendingCloseStore(self.redis, account)
        self.journal = EvidenceCapture(self.path, self.factory, self.logs.append)
        self.pos = {'deal_id': 'fixture-deal', 'ig_size': 10, 'notional': 1000,
                    'broker_entry_evidence': provenance()}

    def restart(self):
        return EvidenceCapture(self.path, self.factory, self.logs.append)

    def test_local_commit_survives_restart_without_redis(self):
        self.journal.store_factory = Mock(side_effect=OSError('offline'))
        self.assertEqual(self.journal.capture(self.pos, 'close_intent'), 'local_durable')
        self.journal.store_factory.assert_not_called()
        rows = list(self.restart().retained())
        self.assertEqual(rows[0]['evidence']['position'], self.pos)
        self.assertEqual(rows[0]['evidence']['status'], 'pending_evidence')

    def test_duplicate_capture_and_replay_are_idempotent(self):
        for _ in range(2):
            self.journal.capture(self.pos, 'close_intent')
        self.assertEqual(len(list(self.journal.retained())), 1)
        for _ in range(2):
            self.restart().replay()
        entry = self.factory('fixture-account').get_entry('fixture-deal')
        self.assertEqual(len(entry['events']), 1)
        self.assertIsNone(entry['record'])
        self.assertIsNone(entry['opening'])
        self.assertEqual(list(self.journal.retained())[0]['forwarded'], 1)

    def test_disk_failure_falls_back_to_redis(self):
        with patch.object(self.journal, '_append', side_effect=OSError('disk full')):
            self.assertEqual(self.journal.capture(self.pos, 'close_intent'), 'redis_durable')
        self.assertIsNotNone(self.factory('fixture-account').get_entry('fixture-deal'))
        self.assertTrue(any('STORAGE FAILURE' in s for s in self.logs))

    def test_both_sinks_fail_then_recover_without_fabricating_completion(self):
        with patch.object(self.journal, '_append', side_effect=OSError('disk full')), \
             patch.object(self.journal, '_forward', side_effect=OSError('disconnect')):
            self.assertEqual(self.journal.capture(self.pos, 'close_intent'), 'unresolved')
        self.assertEqual(len(self.journal.volatile), 1)
        self.assertTrue(any('process crash may lose' in s for s in self.logs))
        self.journal.replay()
        self.assertEqual(self.journal.volatile, {})
        self.assertIsNone(self.factory('fixture-account').get_entry('fixture-deal')['record'])

    def test_redis_disconnect_before_and_after_write(self):
        self.journal.capture(self.pos, 'close_intent')
        for mode in ('before', 'after'):
            with self.subTest(mode=mode):
                failed = FaultClient(self.redis, mode)
                self.journal.store_factory = lambda account: PendingCloseStore(failed, account)
                self.journal.replay()
                self.assertEqual(list(self.journal.retained())[0]['forwarded'], 0)
        self.restart().replay()
        self.assertEqual(len(self.factory('fixture-account').get_entry('fixture-deal')['events']), 1)

    def test_unknown_or_mismatched_identity_stays_quarantined(self):
        for opening in ({}, provenance('wrong-deal')):
            self.journal.capture({**self.pos, 'broker_entry_evidence': opening}, 'state_loaded',
                                 session_account='fixture-account')
        self.journal.capture(self.pos, 'close_intent')
        self.journal.replay()
        rows = list(self.journal.retained())
        self.assertEqual(sum(r['forwarded'] == -1 for r in rows), 2)
        self.assertEqual(sum(r['forwarded'] == 1 for r in rows), 1)
        self.assertEqual(len(self.factory('fixture-account').get_entry('fixture-deal')['events']), 1)

    def test_snapshot_detaches_mutable_position(self):
        self.journal.capture(self.pos, 'before_partial_quantity_change')
        self.pos['ig_size'] = 5
        self.pos['broker_entry_evidence']['role'] = 'changed'
        saved = list(self.restart().retained())[0]['evidence']['position']
        self.assertEqual(saved['ig_size'], 10)
        self.assertEqual(saved['broker_entry_evidence']['role'], 'primary')

    def test_lost_local_acknowledgement_cannot_erase_evidence(self):
        append = self.journal._append
        def lost_ack(*args):
            append(*args)
            raise OSError('lost ack')
        with patch.object(self.journal, '_append', side_effect=lost_ack):
            self.assertEqual(self.journal.capture(self.pos, 'close_intent'), 'redis_durable')
        self.restart().replay()
        self.assertEqual(len(self.factory('fixture-account').get_entry('fixture-deal')['events']), 1)

    def test_ram_retry_can_use_recovered_redis_while_disk_stays_failed(self):
        with patch.object(self.journal, '_append', side_effect=OSError('disk full')):
            with patch.object(self.journal, '_forward', side_effect=OSError('redis down')):
                self.journal.capture(self.pos, 'close_intent')
            self.journal.replay()
        self.assertEqual(self.journal.volatile, {})
        self.assertIsNotNone(self.factory('fixture-account').get_entry('fixture-deal'))

    def test_nonfinite_snapshot_survives_as_explicit_quarantine(self):
        self.pos['local_estimate'] = float('nan')
        self.assertEqual(self.journal.capture(self.pos, 'close_intent'), 'local_durable')
        saved = list(self.restart().retained())[0]['evidence']
        self.assertEqual(saved['status'], 'unresolved_serialization')
        self.assertIn('nan', saved['position_repr'])
        self.restart().replay()
        self.assertEqual(list(self.restart().retained())[0]['forwarded'], -1)


class RuntimeTests(CaptureTests):
    # Reuse fixtures, not inherited test methods (unittest still runs the useful
    # storage tests here too; load_tests below selects only runtime-prefixed tests).
    def runtime(self):
        ns = close_harness()
        ns['_live']['open_position']['broker_entry_evidence'] = provenance()
        ns['_live_sess'] = {'account_id': 'fixture-account'}
        ns['_live_evidence_capture'] = lambda: self.journal
        extract({'_live_capture_evidence', '_live_capture_active'}, ns)
        return ns

    def rows(self):
        return [r['evidence'] for r in self.restart().retained()]

    def full_close(self, ns):
        ns['_ig_live_get'].return_value = {'positions': []}
        ns['_live_close_position']('stop_loss', {'GOLD': {'price': 99}})

    def test_runtime_crashes_at_capture_request_confirmation_and_clear(self):
        for point in ('before_capture', 'after_capture', 'after_request', 'before_confirmation_capture', 'after_confirmation',
                      'before_clear', 'after_clear'):
            with self.subTest(point=point):
                self.path = Path(self.temp.name) / (point + '.sqlite3')
                self.journal = self.restart()
                ns = self.runtime()
                ns['_live_capture_active']('state_snapshot')
                capture = ns['_live_capture_evidence']
                active = ns['_live_capture_active']
                def failing_capture(pos, event, **details):
                    if point == 'before_capture' and event == 'close_intent':
                        raise Crash()
                    capture(pos, event, **details)
                    if ((point == 'after_capture' and event == 'close_intent') or
                        (point == 'after_confirmation' and event == 'close_confirmation_observed')):
                        raise Crash()
                def failing_active(event):
                    active(event)
                    if point == 'before_clear' and event == 'before_primary_clear':
                        raise Crash()
                ns['_live_capture_evidence'] = failing_capture
                ns['_live_capture_active'] = failing_active
                if point == 'after_request':
                    ns['_ig_live_post'].side_effect = Crash()
                if point == 'before_confirmation_capture':
                    ns['_live_confirm_deal'].side_effect = Crash()
                if point == 'after_clear':
                    ns['_live_save_state'] = Mock(side_effect=Crash())
                with self.assertRaises(Crash):
                    self.full_close(ns)
                rows = self.rows()
                self.assertTrue(any(r['position']['deal_id'] == 'fixture-deal' for r in rows))
                self.assertTrue(all(r['status'] == 'pending_evidence' for r in rows))
                events = {r['event'] for r in rows}
                expected = {'state_snapshot'}
                if point != 'before_capture':
                    expected.add('close_intent')
                if point in ('before_confirmation_capture', 'after_confirmation', 'before_clear', 'after_clear'):
                    expected.add('close_response')
                if point in ('after_confirmation', 'before_clear', 'after_clear'):
                    expected.add('close_confirmation_observed')
                if point in ('before_clear', 'after_clear'):
                    expected.update({'before_primary_clear', 'full_close_outcome_observed'})
                self.assertEqual(events, expected)
                self.assertEqual(ns['_ig_live_post'].call_count,
                                 0 if point in ('before_capture', 'after_capture') else 1)
                if point == 'after_clear':
                    self.assertIsNone(ns['_live']['open_position'])

    def test_runtime_all_storage_failed_does_not_block_protective_close(self):
        ns = self.runtime()
        with patch.object(self.journal, '_append', side_effect=OSError('disk full')), \
             patch.object(self.journal, '_forward', side_effect=OSError('redis down')):
            self.full_close(ns)
        ns['_ig_live_post'].assert_called_once()
        self.assertIsNone(ns['_live']['open_position'])
        self.assertTrue(self.journal.volatile)
        self.assertTrue(any('UNRESOLVED' in s for s in self.logs))

    def test_runtime_partial_then_crash_retains_original_basis_and_receipt(self):
        ns = self.runtime()
        def crash_after_reduction():
            if ns['_live']['open_position']['ig_size'] == 5:
                raise Crash()
        ns['_live_save_state'] = Mock(side_effect=crash_after_reduction)
        with self.assertRaises(Crash):
            ns['_live_partial_tp_exit']({'GOLD': {'price': 102}})
        self.assertEqual(ns['_live']['open_position']['ig_size'], 5)
        rows = self.rows()
        basis = next(r for r in rows if r['event'] == 'before_partial_quantity_change')
        self.assertEqual((basis['position']['ig_size'], basis['position']['notional']), (10, 1000))
        receipt = next(r for r in rows if r['event'] == 'close_confirmation_observed')
        self.assertEqual(receipt['details']['confirmation']['level'], 102)

    def test_runtime_promotion_keeps_both_identities_with_pending_primary(self):
        ns = self.runtime()
        addon = deepcopy(ns['_live']['open_position'])
        addon.update(deal_id='addon-deal', leg_index=2,
                     broker_entry_evidence=provenance('addon-deal', 'add_on'))
        ns['_live']['pyramid_legs'] = [addon]
        ns['_ls_connected'] = True
        ns['_ls_deal_closed'] = lambda deal: deal == 'fixture-deal'
        ns['_ig_live_get'] = lambda path, **kw: None if path == '/positions/otc' else {
            'positions': [{'position': {'dealId': 'addon-deal'}}]}
        ns['_live_save_state'] = Mock(side_effect=Crash())
        with self.assertRaises(Crash):
            ns['_live_close_position']('stop_loss', {'GOLD': {'price': 99}})
        promoted = ns['_live']['open_position']
        self.assertEqual(promoted['deal_id'], 'addon-deal')
        self.assertEqual(promoted['broker_entry_evidence']['role'], 'add_on')
        rows = self.rows()
        self.assertEqual({r['deal_id'] for r in rows}, {'fixture-deal', 'addon-deal'})
        self.assertTrue(all(r['status'] == 'pending_evidence' for r in rows))

    def test_runtime_addon_guard_and_accepted_removal_preserve_primary(self):
        for already_gone in (False, True):
            ns = self.runtime()
            extract({'_live_close_addon_leg'}, ns)
            leg = deepcopy(ns['_live']['open_position'])
            leg.update(deal_id='addon-deal', leg_index=2,
                       broker_entry_evidence=provenance('addon-deal', 'add_on'))
            ns['_live']['pyramid_legs'] = [leg]
            ns['_PYRAMID_UNLOCK_KEY'] = 'fixture-unlock'
            ns['_ls_position_guard_check'].return_value = (not already_gone, 'fixture')
            ns['_live_close_addon_leg'](leg, 'stop_loss', {'GOLD': {'price': 99}})
            self.assertEqual(ns['_live']['pyramid_legs'], [])
            self.assertEqual(ns['_live']['open_position']['deal_id'], 'fixture-deal')
            self.assertTrue(any(r['deal_id'] == 'addon-deal' for r in self.rows()))

    def test_runtime_broker_flat_reconciliation_is_observation_only(self):
        ns = self.runtime()
        extract({'_live_reconcile_positions'}, ns)
        ns['_ig_live_get'].return_value = {'positions': []}
        ns['_live_reconcile_positions']()
        self.assertIsNone(ns['_live']['open_position'])
        rows = self.rows()
        self.assertTrue(any(r['event'] == 'position_absence_observed' for r in rows))
        self.journal.replay()
        self.assertIsNone(self.factory('fixture-account').get_entry('fixture-deal')['record'])

    def test_runtime_state_overwrite_and_failed_save_preserve_evidence(self):
        ns = self.runtime()
        extract({'_live_save_state', '_live_load_state'}, ns)
        ns['_LIVE_REDIS_KEY'], ns['_LIVE_REDIS_TTL'] = 'fixture-state', 3600
        client = ns['_redis']()
        client.set.side_effect = OSError('disconnect')
        ns['_live_save_state']()
        client.get.return_value = '{"open_position":null,"pyramid_legs":[]}'
        self.assertTrue(ns['_live_load_state']())
        self.assertIsNone(ns['_live']['open_position'])
        self.assertTrue(any(r['event'] == 'before_state_load' for r in self.rows()))

    def test_runtime_ls_addon_removal_and_unresolved_addon_retention(self):
        ns = self.runtime()
        extract({'_live_check_pyramid_exits', 'run_live_step', '_run_live_step_observed'}, ns)
        ns['_PYRAMID_PROFIT_GATE_PCT'] = .0015
        leg = deepcopy(ns['_live']['open_position'])
        leg.update(deal_id='addon-deal', leg_index=2,
                   broker_entry_evidence=provenance('addon-deal', 'add_on'))
        ns['_live']['pyramid_legs'] = [leg]
        ns['_ls_deal_closed'] = lambda _: True
        ns['_live_check_pyramid_exits']({'GOLD': {'price': 100}})
        self.assertEqual(ns['_live']['pyramid_legs'], [])
        self.assertTrue(any(r['details'].get('observation_source') == 'LS.FULLY_CLOSED'
                            for r in self.rows()))
        ns['_live'].update(open_position=None, pyramid_legs=[leg], balance=0)
        ns['_current_cycle_signals_snap'] = {}
        for name in ('_live_poll_balance', '_live_poll_pnl', '_live_check_skim',
                     '_live_publish_eligible_instruments', '_live_check_circuit_breaker',
                     '_live_update_defensive_mode', '_live_close_all_addon_legs'):
            ns[name] = Mock()
        ns['_redis']().get.return_value = None
        ns['run_live_step']({'GOLD': {'price': 100}})
        self.assertEqual(ns['_live']['pyramid_legs'], [leg])
        ns['_live_close_all_addon_legs'].assert_called_once()
        ns['_live_check_circuit_breaker'].assert_not_called()

    def test_runtime_partial_flat_branches_preserve_basis(self):
        for proxy in (False, True):
            ns = self.runtime()
            ns['_ig_live_get'] = lambda path, **kw: (
                {'accounts': [{'preferred': True, 'balance': {'deposit': 0}}]}
                if path == '/accounts' else None if proxy else {'positions': []})
            ns['_live_partial_tp_exit']({'GOLD': {'price': 102}})
            if proxy:
                self.assertEqual(ns['_live']['open_position']['ig_size'], 10)
                self.assertTrue(ns['_live']['open_position']['partial_exit_pending'])
            else:
                self.assertIsNone(ns['_live']['open_position'])
                self.assertTrue(any(r['event'] == 'before_primary_clear' and
                                    r['position']['ig_size'] == 10 for r in self.rows()))

    def test_runtime_unavailable_inventory_is_not_fabricated_flat_evidence(self):
        ns = self.runtime()
        extract({'_live_reconcile_positions'}, ns)
        ns['_ig_live_get'].return_value = None
        ns['_live_reconcile_positions']()
        self.assertEqual(ns['_live']['open_position']['ig_size'], 10)
        self.assertTrue(ns['_live']['orphan_suspected'])
        self.assertTrue(any(r['event'] == 'inventory_unavailable' for r in self.rows()))
        self.assertFalse(any(r['event'] == 'position_absence_observed' for r in self.rows()))


def load_tests(loader, tests, pattern):
    suite = loader.loadTestsFromTestCase(CaptureTests)
    suite.addTests(RuntimeTests(n) for n in loader.getTestCaseNames(RuntimeTests)
                   if n.startswith('test_runtime_'))
    return suite


if __name__ == '__main__':
    unittest.main()
