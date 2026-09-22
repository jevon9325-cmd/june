"""Bounded-access regression and repeatable offline fixture measurement."""
import json
import statistics
import time
import unittest
from unittest.mock import patch

import fakeredis

from broker_pending import PendingCloseStore
from test_broker_pending import register, history, COST_EVIDENCE, count_outcome


class CountClient:
    def __init__(self, client):
        self.client = client
        self.reads = []
        self.writes = []

    def __getattr__(self, name):
        return getattr(self.client, name)

    def pipeline(self):
        pipe = self.client.pipeline()
        get, put = pipe.hget, pipe.hset

        def hget(key, field):
            self.reads.append(field)
            return get(key, field)

        def hset(key, *args, **kwargs):
            self.writes.extend(kwargs.get('mapping', {}))
            return put(key, *args, **kwargs)

        pipe.hget, pipe.hset = hget, hset
        pipe.hgetall = lambda *_: self.fail_scan()
        return pipe

    @staticmethod
    def fail_scan():
        raise AssertionError('whole-account read on single-position operation')


def seeded(count):
    client = fakeredis.FakeRedis()
    counted = CountClient(client)
    store = PendingCloseStore(counted, 'fixture-account')
    payload = json.dumps({'account_id': 'fixture-account', 'deal_id': 'old',
                          'events': {'old': {'evidence': 'x' * 1220}},
                          'opening': None, 'record': None})
    client.hset(store.key, mapping={'trade:old-' + str(i): payload for i in range(count)})
    return store, counted, len(payload.encode()) * count


class AccessTests(unittest.TestCase):
    def test_capture_reads_only_one_field_at_every_scale(self):
        for count in (100, 1000, 5000):
            with self.subTest(count=count):
                store, client, _ = seeded(count)
                store.capture('new', 'fixture', {'quantity': 1})
                self.assertEqual(client.reads, [store._field('new')])
                self.assertEqual(client.writes, [store._field('new')])
                self.assertEqual(client.hlen(store.key), count + 1)

    def test_register_reconcile_and_delivery_use_only_related_fields(self):
        store, client, _ = seeded(1000)
        register(store)
        self.assertEqual(len(client.reads), 3)  # capture + trade/opening index
        client.reads.clear()
        store.reconcile('opening-1', history(), cost_evidence=COST_EVIDENCE)
        self.assertEqual(len(client.reads), 3)  # trade, opening owners, realization
        client.reads.clear()
        store.project_once('opening-1', 'fixture', count_outcome)
        self.assertEqual(len(client.reads), 4)  # trade, owners, delivery, consumer
        client.reads.clear()
        store.project_once('opening-1', 'fixture', count_outcome)
        self.assertEqual(len(client.reads), 4)
        self.assertFalse(any(f.startswith('trade:old-') for f in client.reads))

    def test_incremental_enumeration_and_direct_lookup(self):
        store, client, _ = seeded(101)
        store.capture('new', 'fixture', {'quantity': 1})
        with patch.object(client.client, 'hgetall', side_effect=AssertionError('scan')):
            self.assertEqual(len(list(store.iter_entries(count=7))), 102)
            self.assertEqual(store.get_entry('new')['deal_id'], 'new')
            self.assertIsNone(store.get_entry('missing'))
        self.assertEqual(client.ttl(store.key), -1)

    def test_noop_capture_does_not_write_again(self):
        store, client, _ = seeded(100)
        store.capture('new', 'fixture', {'quantity': 1})
        client.writes.clear()
        store.capture('new', 'fixture', {'quantity': 1})
        self.assertEqual(client.writes, [])


def benchmark():
    for count in (100, 1000, 5000):
        store, client, size = seeded(count)
        elapsed = []
        for i in range(11):
            client.reads.clear()
            start = time.perf_counter()
            store.capture('target', 'fixture', {'revision': i})
            elapsed.append((time.perf_counter() - start) * 1000)
            assert len(client.reads) == 1
        print(f'{count}: payload_bytes={size}, fields_read=1, median_ms={statistics.median(elapsed):.3f}')


if __name__ == '__main__':
    import sys
    benchmark() if '--benchmark' in sys.argv else unittest.main()
