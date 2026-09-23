"""Offline Redis-emulator tests: no bot import, real Redis, or broker access.

Install fakeredis in an isolated test environment before running this module.
"""

import unittest
from unittest.mock import Mock

import fakeredis
from redis.exceptions import ConnectionError

from broker_history import fetch_transaction_history
from broker_ledger import EvidenceError
from broker_pending import PendingCloseStore
from test_broker_ledger import position, realization


START, END = "2026-09-21T00:00:00", "2026-09-22T00:00:00"
COST_EVIDENCE = {"source": "fixture.broker.cost_statement", "reference": "statement-1",
                 "covered_through": END, 'covered_from': START, 'schema_version': 2,
                 'account_id': 'fixture-account', 'deal_id': 'opening-1',
                 'posting_finality': {'source': 'fixture.broker.statement',
                     'reference': 'posting-final-1', 'final': True, 'posted_through': END},
                 'components': {kind: {'source': 'fixture.broker.statement',
                     'reference': 'final-' + kind, 'final': True, 'total': '0'}
                     for kind in ('commission', 'financing', 'other')}}


def page(rows, number=1, count=None, size=500):
    count = len(rows) if count is None else count
    return {"transactions": rows, "metadata": {"size": count, "pageData": {
        "pageNumber": number, "pageSize": size, "totalPages": (count + size - 1) // size}}}


def history(rows=None):
    rows = [realization()] if rows is None else rows
    return fetch_transaction_history(Mock(return_value=page(rows)), "fixture-account", START, END)


def register(store, pos=None):
    pos = position() if pos is None else pos
    receipt = {"source": "june.accepted_entry_confirm", "dealId": pos["deal_id"],
               "dealReference": "entry-reference", "dealStatus": "ACCEPTED"}
    store.capture(pos["deal_id"], receipt["source"], {"receipt": receipt, "position": pos})
    store.register_opening(pos, receipt)
    return pos


def count_outcome(state, record):
    return {"count": state.get("count", 0) + 1,
            "net": round(state.get("net", 0) + float(record["net_realized_pnl"]), 4)}


class FaultClient:
    """Inject one transport failure or concurrent write at the EXEC boundary."""
    def __init__(self, client, mode, concurrent=None):
        self.client, self.mode, self.concurrent = client, mode, concurrent
        self.fired = False

    def __getattr__(self, name):
        return getattr(self.client, name)

    def pipeline(self):
        pipe = self.client.pipeline()
        execute = pipe.execute

        def interrupted_execute(*args, **kwargs):
            fire = not self.fired
            self.fired = True
            if fire and self.mode == "before":
                raise ConnectionError("fixture disconnect before EXEC")
            if fire and self.mode == "race":
                self.concurrent()
            result = execute(*args, **kwargs)
            if fire and self.mode == "after":
                raise ConnectionError("fixture lost EXEC acknowledgement")
            return result
        pipe.execute = interrupted_execute
        return pipe


class HistoryTests(unittest.TestCase):
    def test_all_pages_and_cost_rows_preserved(self):
        rows = [realization(), {"transactionType": "COMM", "reference": "fee", "profitAndLoss": "-$9"}]
        get = Mock(side_effect=[page([rows[0]], 1, 2, 1), page([rows[1]], 2, 2, 1)])
        batch = fetch_transaction_history(get, "fixture-account", START, END, page_size=1)
        self.assertEqual(batch["transactions"], rows)
        self.assertTrue(batch["history_complete"])
        self.assertEqual([c.kwargs["params"]["pageNumber"] for c in get.call_args_list], [1, 2])
        for call in get.call_args_list:
            self.assertEqual(call.args, ("/history/transactions",))
            self.assertEqual(call.kwargs["version"], "2")
            self.assertEqual(call.kwargs["params"]["type"], "ALL")
            self.assertEqual(call.kwargs["params"]["from"], START)
            self.assertEqual(call.kwargs["params"]["to"], END)

    def test_missing_page_does_not_return_partial_success(self):
        get = Mock(side_effect=[page([realization()], 1, 2, 1), None])
        with self.assertRaises(EvidenceError):
            fetch_transaction_history(get, "fixture-account", START, END, page_size=1)

    def test_unstable_repeated_and_truncated_pages_rejected(self):
        first = page([realization()], 1, 2, 1)
        for second in [page([realization()], 2, 3, 1), page([realization()], 2, 2, 1),
                       page([], 2, 2, 1), page([realization("other")], 1, 2, 1)]:
            with self.subTest(second=second), self.assertRaises(EvidenceError):
                fetch_transaction_history(Mock(side_effect=[first, second]),
                                          "fixture-account", START, END, page_size=1)

    def test_empty_is_valid_but_missing_metadata_is_not(self):
        self.assertEqual(history([])["transactions"], [])
        with self.assertRaises(EvidenceError):
            fetch_transaction_history(Mock(return_value={"transactions": []}), "fixture-account", START, END)

    def test_metaData_alias_and_page_budget(self):
        response = page([realization()])
        response["metaData"] = response.pop("metadata")
        self.assertTrue(fetch_transaction_history(Mock(return_value=response), "fixture-account", START, END)["history_complete"])
        with self.assertRaises(EvidenceError):
            fetch_transaction_history(Mock(return_value=page([realization()], 1, 2, 1)),
                                      "fixture-account", START, END, page_size=1, max_pages=1)


class PendingTests(unittest.TestCase):
    def setUp(self):
        self.server = fakeredis.FakeServer()
        self.client = fakeredis.FakeRedis(server=self.server)
        self.store = PendingCloseStore(self.client, "fixture-account")

    def restart(self, client=None):
        return PendingCloseStore(client or fakeredis.FakeRedis(server=self.server), "fixture-account")

    def complete(self):
        register(self.store)
        return self.store.reconcile("opening-1", history(), cost_evidence=COST_EVIDENCE)

    def test_capture_survives_restart_and_duplicate_snapshots(self):
        snapshot = {"ig_size": .16, "notional": 696.4048, "role": "primary"}
        self.store.capture("opening-1", "june.pre_clear", snapshot)
        snapshot["ig_size"] = 999
        recovered = self.restart()
        first = recovered.entries()[0]
        event = next(iter(first["events"].values()))
        self.assertEqual(event["evidence"]["ig_size"], .16)
        recovered.capture("opening-1", "june.pre_clear", event["evidence"])
        self.assertEqual(len(recovered.entries()[0]["events"]), 1)
        self.assertEqual(self.client.ttl(self.store.key), -1)
        self.assertIsNone(first["record"])

    def test_partial_delayed_history_and_delayed_costs_survive_restart(self):
        register(self.store)
        first = realization("partial", "-0.08", "0.09", closeLevel="4351.44")
        record = self.store.reconcile("opening-1", history([first]))
        self.assertEqual(record["status"], "pending_realizations")
        restarted = self.restart()
        with self.assertRaises(EvidenceError):
            restarted.project_once("opening-1", "learning", count_outcome)
        record = restarted.reconcile("opening-1", history([first, realization("residual", "-0.08", "-0.08")]))
        self.assertEqual(record["status"], "pending_costs")
        record = restarted.reconcile("opening-1", history([first, realization("residual", "-0.08", "-0.08")]),
                                     cost_evidence=COST_EVIDENCE)
        self.assertEqual(record["net_realized_pnl"], "0.01")

    def test_broker_close_without_software_confirm(self):
        register(self.store, position(exit_reason="unknown"))
        self.store.capture("opening-1", "june.reconcile_absent", {"exit_reason": "unknown"})
        result = self.restart().reconcile("opening-1", history(), cost_evidence=COST_EVIDENCE)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["exit_reason"], "unknown")
        self.assertEqual(self.restart().project_once("opening-1", "fixture-performance", count_outcome),
                         {"count": 1, "net": -.16})

    def test_repeated_observation_and_projection_are_idempotent(self):
        self.complete()
        first = self.store.project_once("opening-1", "performance", count_outcome)
        restarted = self.restart()
        restarted.reconcile("opening-1", history(), cost_evidence=COST_EVIDENCE)
        reducer = Mock(side_effect=AssertionError("must not repeat consumer"))
        self.assertEqual(restarted.project_once("opening-1", "performance", reducer), first)
        reducer.assert_not_called()
        self.assertEqual(len(restarted.entries()), 1)

    def test_disconnect_before_commit_leaves_no_marker_or_projection(self):
        self.complete()
        failed = self.restart(FaultClient(self.client, "before"))
        with self.assertRaises(ConnectionError):
            failed.project_once("opening-1", "performance", count_outcome)
        self.assertEqual(self.restart().project_once("opening-1", "performance", count_outcome), {"count": 1, "net": -.16})

    def test_disconnect_after_commit_retry_does_not_double_count(self):
        self.complete()
        failed = self.restart(FaultClient(self.client, "after"))
        with self.assertRaises(ConnectionError):
            failed.project_once("opening-1", "performance", count_outcome)
        reducer = Mock(side_effect=AssertionError("already committed"))
        self.assertEqual(self.restart().project_once("opening-1", "performance", reducer), {"count": 1, "net": -.16})
        reducer.assert_not_called()

    def test_watch_conflict_preserves_concurrent_capture(self):
        self.complete()
        racing = self.restart(FaultClient(self.client, "race", lambda: self.store.capture("other-deal", "fixture", {"role": "add_on"})))
        self.assertEqual(racing.project_once("opening-1", "performance", count_outcome), {"count": 1, "net": -.16})
        self.assertEqual(len(self.restart().entries()), 2)

    def test_capture_lost_acknowledgement_is_safe_to_retry(self):
        failed = self.restart(FaultClient(self.client, "after"))
        with self.assertRaises(ConnectionError):
            failed.capture("opening-1", "fixture", {"quantity": 1})
        self.restart().capture("opening-1", "fixture", {"quantity": 1})
        self.assertEqual(len(self.store.entries()[0]["events"]), 1)

    def test_conflicting_completed_evidence_retains_original(self):
        original = self.complete()
        with self.assertRaises(EvidenceError):
            self.store.reconcile("opening-1", history([realization(pnl="5")]), cost_evidence=COST_EVIDENCE)
        self.assertEqual(self.restart().entries()[0]["record"], original)

    def test_partial_evidence_cannot_be_erased_by_later_poll(self):
        register(self.store)
        original = self.store.reconcile("opening-1", history([realization(quantity="-0.08")]))
        with self.assertRaises(EvidenceError):
            self.store.reconcile("opening-1", history([]))
        self.assertEqual(self.restart().entries()[0]["record"], original)

    def test_reconciliation_commit_lost_ack_can_be_retried(self):
        register(self.store)
        failed = self.restart(FaultClient(self.client, "after"))
        with self.assertRaises(ConnectionError):
            failed.reconcile("opening-1", history(), cost_evidence=COST_EVIDENCE)
        result = self.restart().reconcile("opening-1", history(), cost_evidence=COST_EVIDENCE)
        self.assertEqual(result["net_realized_pnl"], "-0.16")
        self.assertEqual(len(self.store.entries()), 1)

    def test_independent_consumers_each_receive_once(self):
        self.complete()
        for consumer in ("performance", "history", "streak"):
            for _ in range(2):
                result = self.restart().project_once("opening-1", consumer, count_outcome)
                self.assertEqual(result["count"], 1)

    def test_opening_collision_blocks_both_positions(self):
        register(self.store)
        register(self.store, position(deal_id="another-deal"))
        for deal in ("opening-1", "another-deal"):
            with self.assertRaises(EvidenceError):
                self.store.reconcile(deal, history(), cost_evidence=COST_EVIDENCE)
        self.assertTrue(all(e["record"] is None for e in self.restart().entries()))

    def test_manual_recovery_is_not_automatically_june(self):
        self.store.capture("opening-1", "june.orphan_recovered", position())
        with self.assertRaises(EvidenceError):
            self.store.reconcile("opening-1", history(), cost_evidence=COST_EVIDENCE)
        with self.assertRaises(EvidenceError):
            self.store.register_opening(position(), {"source": "orphan"})

    def test_wrong_account_window_and_cost_coverage_rejected(self):
        register(self.store)
        for change in ({"account_id": "other"}, {"from": END}, {"history_complete": False}, {"to": START}):
            with self.subTest(change=change), self.assertRaises(EvidenceError):
                self.store.reconcile("opening-1", {**history(), **change}, cost_evidence=COST_EVIDENCE)
        for evidence in ({}, {**COST_EVIDENCE, "covered_through": START}):
            with self.assertRaises(EvidenceError):
                self.store.reconcile("opening-1", history(), cost_evidence=evidence)

    def test_projector_failure_cannot_commit_delivery_marker(self):
        self.complete()
        with self.assertRaises(RuntimeError):
            self.store.project_once("opening-1", "performance", Mock(side_effect=RuntimeError("fixture")))
        self.assertEqual(self.restart().project_once("opening-1", "performance", count_outcome)["count"], 1)

    def test_non_json_projection_cannot_commit(self):
        self.complete()
        with self.assertRaises(ValueError):
            self.store.project_once("opening-1", "performance", lambda *_: {"net": float("nan")})
        self.assertEqual(self.restart().project_once("opening-1", "performance", count_outcome)["count"], 1)


if __name__ == "__main__":
    unittest.main()
