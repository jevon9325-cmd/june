"""C2b storage boundary tests; temporary SQLite, fake Redis, no bot import."""
import json
from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import sys
from unittest.mock import Mock, patch
import unittest

from broker_capture import EvidenceCapture
from broker_pending import PendingCloseStore
from test_broker_pending import FaultClient
from test_evidence_capture import RuntimeTests


class StorageFailureTests(RuntimeTests):
    def test_extra_real_sqlite_lock_uses_independent_redis(self):
        self.journal.capture(self.pos, 'state_snapshot')
        blocker = sqlite3.connect(self.path)
        self.addCleanup(blocker.close)
        blocker.execute('BEGIN IMMEDIATE')
        try:
            self.assertEqual(self.journal.capture(self.pos, 'close_intent'), 'redis_durable')
        finally:
            blocker.rollback()
        self.assertTrue(any('OperationalError' in s for s in self.logs))
        self.assertEqual(len(self.factory('fixture-account').get_entry('fixture-deal')['events']), 1)
        self.assertEqual(len(list(self.restart().retained())), 1)
        self.assertEqual(self.journal.capture(self.pos, 'close_intent'), 'local_durable')
        self.restart().replay()
        self.assertEqual(len(self.factory('fixture-account').get_entry('fixture-deal')['events']), 2)

    def test_extra_corrupt_sqlite_is_not_overwritten_and_redis_preserves_capture(self):
        corrupt = b'not a SQLite database' * 256
        self.path.write_bytes(corrupt)
        self.assertEqual(self.journal.capture(self.pos, 'close_intent'), 'redis_durable')
        self.assertEqual(self.path.read_bytes(), corrupt)
        self.assertIsNotNone(self.factory('fixture-account').get_entry('fixture-deal'))
        self.journal.replay()
        self.assertTrue(any('REPLAY PENDING' in s for s in self.logs))

    def test_extra_missing_directory_and_failed_logger_do_not_block_exit(self):
        self.journal.path = str(Path(self.temp.name) / 'absent' / 'journal.sqlite3')
        self.journal.report = Mock(side_effect=OSError('logger unavailable'))
        ns = self.runtime()
        self.full_close(ns)
        ns['_ig_live_post'].assert_called_once()
        self.assertIsNone(ns['_live']['open_position'])
        entry = self.factory('fixture-account').get_entry('fixture-deal')
        self.assertTrue(entry['events'])
        self.assertIsNone(entry['record'])

    def test_extra_redis_fallback_lost_ack_retries_same_identity(self):
        failed = FaultClient(self.redis, 'after')
        self.journal.store_factory = lambda account: PendingCloseStore(failed, account)
        with patch.object(self.journal, '_append', side_effect=OSError('disk unavailable')):
            self.assertEqual(self.journal.capture(self.pos, 'close_intent'), 'unresolved')
        self.assertTrue(self.journal.volatile)
        self.assertEqual(len(self.factory('fixture-account').get_entry('fixture-deal')['events']), 1)
        self.journal.store_factory = self.factory
        self.journal.replay()
        self.assertFalse(self.journal.volatile)
        self.assertEqual(len(self.factory('fixture-account').get_entry('fixture-deal')['events']), 1)

    def test_extra_abrupt_process_death_before_commit_and_after_commit(self):
        # Kill the child inside the real SQLite transaction and just after commit.
        # No Python cleanup, exception handler, or connection finalizer runs.
        worker = r'''
import json, os, sqlite3, sys
from broker_capture import EvidenceCapture
path, boundary, position = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
connect = sqlite3.connect
class CrashConnection(sqlite3.Connection):
    def execute(self, sql, *args, **kwargs):
        result = super().execute(sql, *args, **kwargs)
        if boundary == 'before_commit' and sql.startswith('INSERT OR IGNORE'):
            os._exit(86)
        return result
    def __exit__(self, *args):
        result = super().__exit__(*args)
        if boundary == 'after_commit':
            os._exit(87)
        return result
sqlite3.connect = lambda *args, **kwargs: connect(*args, factory=CrashConnection, **kwargs)
EvidenceCapture(path, None, lambda _: None).capture(position, 'close_intent')
raise AssertionError('Crash boundary was not reached')
'''
        for boundary, code in [('before_commit', 86), ('after_commit', 87)]:
            with self.subTest(boundary=boundary):
                self.path = Path(self.temp.name) / (boundary + '.sqlite3')
                self.journal = self.restart()
                self.journal.capture(self.pos, 'state_snapshot')
                result = subprocess.run([sys.executable, '-B', '-c', worker,
                    str(self.path), boundary, json.dumps(self.pos)],
                    cwd=Path(__file__).resolve().parent, capture_output=True, timeout=30)
                self.assertEqual(result.returncode, code, result.stderr.decode(errors='replace'))
                rows = list(self.restart().retained())
                expected = {'state_snapshot'} | ({'close_intent'} if boundary == 'after_commit' else set())
                self.assertEqual({r['evidence']['event'] for r in rows}, expected)
                self.assertTrue(all(r['evidence']['status'] == 'pending_evidence' for r in rows))
                with closing(sqlite3.connect(self.path)) as db:
                    self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')

    def test_extra_synchronous_extra_and_old_pending_records_never_expire(self):
        self.journal.capture(self.pos, 'state_snapshot')
        self.journal.capture({**self.pos, 'deal_id': 'legacy', 'broker_entry_evidence': {}}, 'state_loaded')
        with self.journal._connect() as db:
            self.assertEqual(db.execute('PRAGMA synchronous').fetchone()[0], 3)
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0], 'delete')
            db.execute("UPDATE evidence SET observed_utc='2000-01-01T00:00:00+00:00'")
        db.close()
        self.journal.replay(limit=1)
        self.assertEqual(len(list(self.restart().retained())), 2)
        self.journal.replay(limit=1)
        rows = list(self.restart().retained())
        self.assertEqual({r['forwarded'] for r in rows}, {-1, 1})
        self.assertTrue(all(r['first_observed_utc'].startswith('2000-') for r in rows))

    def test_extra_failed_partial_verification_preserves_live_basis_and_receipt(self):
        ns = self.runtime()
        ns['_ig_live_get'].return_value = None  # both inventory and accounts unavailable
        ns['_live_partial_tp_exit']({'GOLD': {'price': 102.}})
        self.assertEqual(ns['_live']['open_position']['ig_size'], 10.)
        self.assertTrue(ns['_live']['open_position']['partial_exit_pending'])
        rows = self.rows()
        basis = next(r for r in rows if r['event'] == 'partial_residual_observed')
        self.assertEqual((basis['position']['ig_size'], basis['position']['notional']), (10., 1000.))
        receipt = next(r for r in rows if r['event'] == 'close_confirmation_observed')
        self.assertEqual(receipt['details']['confirmation']['dealStatus'], 'ACCEPTED')
        self.assertTrue(all(r['status'] == 'pending_evidence' for r in rows))


def load_tests(loader, tests, pattern):
    # Reuse fixtures/runtime helpers without re-running inherited suites.
    return unittest.TestSuite(StorageFailureTests(n) for n in loader.getTestCaseNames(StorageFailureTests)
                              if n.startswith('test_extra_'))


if __name__ == '__main__':
    unittest.main()
