"""C2c-8 (delayed evidence) and C2c-9 (restart / idempotency) tests.

Drives the full pipeline: normalize → lifecycle → attribute → store.
Uses fakeredis; no bot import, live Redis, or broker access.

C2c-8 — delayed evidence
  Broker history arrives asynchronously.  Tests cover:
    T0: neither DEAL nor COMM has arrived yet → pending_realizations
    T1: DEAL row arrived, COMM still absent  → pending_costs
    T2: DEAL + COMM + cost_evidence          → complete
  Key invariant: the record must NOT be prematurely finalized at T0 or T1.

C2c-9 — restart / idempotency
  Key invariant: the same economic event must never become two completed
  outcomes.  Tests cover: restart before evidence, restart mid-progression,
  repeated reconciliation, duplicate batches, lost acknowledgements, and the
  opening-collision guard that prevents two positions from claiming the same
  realization.
"""

import unittest
from unittest.mock import Mock

import fakeredis
from redis.exceptions import ConnectionError

from broker_ledger import EvidenceError
from broker_history import fetch_transaction_history
from broker_pending import PendingCloseStore
from broker_reconcile import reconcile_position
from test_broker_ledger import position, realization
from test_broker_pending import FaultClient, page, register, COST_EVIDENCE, START, END


ACCOUNT_ID = "fixture-account"
ENTRY_REF  = "open-ref-1"
POS        = position()  # default fixture: short Gold, opened_utc 2026-09-21T05:45:56


def raw_batch(rows=None):
    """Build a complete history batch from a list of raw IG rows."""
    rows = rows if rows is not None else []
    return fetch_transaction_history(
        Mock(return_value=page(rows)), ACCOUNT_ID, START, END)


def comm_raw(ref, amount="-9"):
    """Minimal raw IG commission row."""
    return {"transactionType": "COMM", "reference": ref,
            "profitAndLoss": "$" + amount, "currency": "$"}


# Canonical full-evidence row set: one closing DEAL + opening COMM + closing COMM
FULL_ROWS = [realization(), comm_raw(ENTRY_REF), comm_raw("close-1")]

# Expected net P&L: gross -0.16  + opening commission -9 + closing commission -9 = -18.16
EXPECTED_NET = "-18.16"


# ════════════════════════════════════════════════════════════════════════════
# C2c-8: Delayed broker evidence
# ════════════════════════════════════════════════════════════════════════════

class DelayedEvidenceTests(unittest.TestCase):
    """Broker history can arrive after June first observes the close."""

    def setUp(self):
        self.server = fakeredis.FakeServer()
        self.client = fakeredis.FakeRedis(server=self.server)
        self.store  = PendingCloseStore(self.client, ACCOUNT_ID)
        register(self.store, POS)

    def restart(self, client=None):
        return PendingCloseStore(
            client or fakeredis.FakeRedis(server=self.server), ACCOUNT_ID)

    # ── T0: neither DEAL nor COMM has arrived ──────────────────────────────

    def test_t0_neither_deal_nor_comm_is_pending_realizations(self):
        """C2c-8 T0: empty history window → pending_realizations; no premature finalization."""
        record, _ = reconcile_position(
            self.store, POS["deal_id"], raw_batch([]), POS)
        self.assertEqual(record["status"], "pending_realizations")
        self.assertIsNone(record["net_realized_pnl"])

    # ── T1: DEAL row arrived, COMM still absent ────────────────────────────

    def test_t1_deal_before_comm_is_pending_costs(self):
        """C2c-8 T1: DEAL row present, no COMM yet → pending_costs.

        This is the critical delayed-evidence case.  The realization IS matched
        (full_close) but commissions have not yet propagated through the broker's
        history API.  The record must NOT be finalized with zero costs assumed.
        The caller withholds cost_evidence precisely because costs are not yet
        confirmed complete.
        """
        record, attribution = reconcile_position(
            self.store, POS["deal_id"],
            raw_batch([realization()]), POS,
            entry_reference=ENTRY_REF)   # no cost_evidence supplied at T1
        self.assertEqual(record["status"], "pending_costs")
        # Realization IS present — the DEAL row was matched
        self.assertEqual(len(record["realizations"]), 1)
        self.assertEqual(record["realizations"][0]["broker_reference"], "close-1")
        # net_realized_pnl is None until costs are finalized
        self.assertIsNone(record["net_realized_pnl"])

    # ── T2: full evidence available ────────────────────────────────────────

    def test_t2_deal_and_comm_with_attestation_is_complete(self):
        """C2c-8 T2: DEAL + both COMM rows + cost_evidence → complete."""
        record, attribution = reconcile_position(
            self.store, POS["deal_id"], raw_batch(FULL_ROWS), POS,
            entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)
        self.assertEqual(record["status"], "complete")
        self.assertEqual(record["net_realized_pnl"], EXPECTED_NET)
        self.assertTrue(attribution["cost_complete"])
        self.assertEqual(len(attribution["position_costs"]), 2)

    # ── T1 → restart → T2 ─────────────────────────────────────────────────

    def test_delayed_comm_after_restart_completes_record(self):
        """C2c-8: DEAL at T1, COMM at T2 after restart → complete.

        No special-casing needed: re-calling reconcile_position is sufficient.
        The store advances from pending_costs to complete automatically.
        """
        # T1
        record, _ = reconcile_position(
            self.store, POS["deal_id"],
            raw_batch([realization()]), POS,
            entry_reference=ENTRY_REF)
        self.assertEqual(record["status"], "pending_costs")

        # Restart (simulates service restart between T1 and T2)
        restarted = self.restart()

        # T2: full evidence now available
        record, _ = reconcile_position(
            restarted, POS["deal_id"], raw_batch(FULL_ROWS), POS,
            entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)
        self.assertEqual(record["status"], "complete")
        self.assertEqual(record["net_realized_pnl"], EXPECTED_NET)

    # ── cost_evidence attestation is required; attribution alone is not enough ──

    def test_cost_evidence_required_even_when_all_costs_attributed(self):
        """C2c-8: attribution.cost_complete=True but no cost_evidence → pending_costs.

        Finding all cost rows programmatically is not sufficient for finalization.
        The store requires an explicit attestation ({source, reference,
        covered_through}) confirming that no further costs will arrive.
        """
        record, attribution = reconcile_position(
            self.store, POS["deal_id"], raw_batch(FULL_ROWS), POS,
            entry_reference=ENTRY_REF, cost_evidence=None)  # attestation withheld
        # Attribution successfully identified both COMM rows
        self.assertTrue(attribution["cost_complete"])
        self.assertEqual(len(attribution["position_costs"]), 2)
        # But the store refuses to finalize without explicit attestation
        self.assertEqual(record["status"], "pending_costs")
        self.assertIsNone(record["net_realized_pnl"])

    # ── excess_close is never resolved by guessing ─────────────────────────

    def test_excess_close_raises_not_guesses(self):
        """C2c-8: Two DEAL rows sharing this opening identity → raises immediately.

        reconcile_position never picks one arbitrarily.  The caller must supply
        opened_utc in position_evidence to discriminate same-price positions.
        """
        dup = realization("extra-ref", "-0.16", "0.20")  # second full close, same identity
        with self.assertRaises(EvidenceError) as ctx:
            reconcile_position(
                self.store, POS["deal_id"],
                raw_batch([realization(), dup]), POS,
                entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)
        self.assertIn("excess_close", str(ctx.exception))


# ════════════════════════════════════════════════════════════════════════════
# C2c-9: Restart / idempotency
# ════════════════════════════════════════════════════════════════════════════

class IdempotencyTests(unittest.TestCase):
    """The same economic event must never become two completed outcomes."""

    def setUp(self):
        self.server = fakeredis.FakeServer()
        self.client = fakeredis.FakeRedis(server=self.server)
        self.store  = PendingCloseStore(self.client, ACCOUNT_ID)
        register(self.store, POS)

    def restart(self, client=None):
        return PendingCloseStore(
            client or fakeredis.FakeRedis(server=self.server), ACCOUNT_ID)

    def complete(self, store=None):
        store = store or self.store
        record, _ = reconcile_position(
            store, POS["deal_id"], raw_batch(FULL_ROWS), POS,
            entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)
        self.assertEqual(record["status"], "complete")
        return record

    # ── restart before history ─────────────────────────────────────────────

    def test_restart_before_history_preserves_opening(self):
        """C2c-9: Service restart before any history arrives — opening survives."""
        restarted = self.restart()
        entry = restarted.get_entry(POS["deal_id"])
        self.assertIsNotNone(entry["opening"])   # opening registered in setUp
        self.assertIsNone(entry["record"])        # nothing reconciled yet

        # After restart, full reconciliation succeeds
        record = self.complete(restarted)
        self.assertEqual(record["status"], "complete")

    # ── repeated reconciliation — primary idempotency invariant ───────────

    def test_repeated_reconciliation_is_idempotent(self):
        """C2c-9 primary invariant: same evidence → same record, not two outcomes.

        Calling reconcile_position twice with identical evidence produces the
        same record both times.  The store keeps exactly one entry.
        """
        r1 = self.complete()
        r2, _ = reconcile_position(
            self.store, POS["deal_id"], raw_batch(FULL_ROWS), POS,
            entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)
        self.assertEqual(r1, r2)
        self.assertEqual(len(self.store.entries()), 1)

    def test_repeated_reconciliation_after_restart_is_idempotent(self):
        """C2c-9: Reconcile after restart with same evidence → identical record."""
        first = self.complete()
        record, _ = reconcile_position(
            self.restart(), POS["deal_id"], raw_batch(FULL_ROWS), POS,
            entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)
        self.assertEqual(first, record)

    # ── negative proof: changed evidence raises ────────────────────────────

    def test_changed_pnl_after_complete_raises(self):
        """C2c-9 negative proof: different P&L after completion raises EvidenceError.

        This proves the invariant from the other direction: you cannot silently
        overwrite a completed outcome — any change triggers an explicit error.
        """
        self.complete()
        modified = realization("close-1", "-0.16", "5.00")  # P&L changed to +5.00
        with self.assertRaises(EvidenceError):
            reconcile_position(
                self.store, POS["deal_id"],
                raw_batch([modified, comm_raw(ENTRY_REF), comm_raw("close-1")]),
                POS, entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)
        # Original record is unchanged
        surviving = self.store.get_entry(POS["deal_id"])["record"]
        self.assertEqual(surviving["net_realized_pnl"], EXPECTED_NET)

    # ── duplicate batch ────────────────────────────────────────────────────

    def test_duplicate_batch_is_idempotent(self):
        """C2c-9: Fetching the same history page twice → same outcome, no duplication.

        Simulates a re-fetch after restart that returns an identical batch.
        """
        batch = raw_batch(FULL_ROWS)
        r1, _ = reconcile_position(self.store, POS["deal_id"], batch, POS,
                                    entry_reference=ENTRY_REF,
                                    cost_evidence=COST_EVIDENCE)
        r2, _ = reconcile_position(self.store, POS["deal_id"], batch, POS,
                                    entry_reference=ENTRY_REF,
                                    cost_evidence=COST_EVIDENCE)
        self.assertEqual(r1, r2)

    # ── lost acknowledgement ───────────────────────────────────────────────

    def test_lost_ack_retry_is_safe(self):
        """C2c-9: ConnectionError after EXEC → state committed; retry returns same record.

        Simulates a transport failure where the EXEC succeeded on the server but
        the acknowledgement was lost.  Re-calling reconcile_position produces
        the same result without duplicating the outcome.
        """
        failed = self.restart(FaultClient(self.client, "after"))
        with self.assertRaises(ConnectionError):
            reconcile_position(failed, POS["deal_id"], raw_batch(FULL_ROWS), POS,
                                entry_reference=ENTRY_REF,
                                cost_evidence=COST_EVIDENCE)
        # State WAS committed (mode="after" fires after execute).
        # Retry with a fresh client sees the committed record.
        record, _ = reconcile_position(
            self.restart(), POS["deal_id"], raw_batch(FULL_ROWS), POS,
            entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)
        self.assertEqual(record["status"], "complete")
        self.assertEqual(record["net_realized_pnl"], EXPECTED_NET)

    # ── restart mid-progression ────────────────────────────────────────────

    def test_restart_between_deal_and_comm(self):
        """C2c-9: pending_costs state survives restart; COMM arrival completes record."""
        record, _ = reconcile_position(
            self.store, POS["deal_id"],
            raw_batch([realization()]), POS,
            entry_reference=ENTRY_REF)
        self.assertEqual(record["status"], "pending_costs")

        record, _ = reconcile_position(
            self.restart(), POS["deal_id"], raw_batch(FULL_ROWS), POS,
            entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)
        self.assertEqual(record["status"], "complete")
        self.assertEqual(record["net_realized_pnl"], EXPECTED_NET)

    # ── API failure before commit leaves state unchanged ──────────────────

    def test_api_failure_before_commit_preserves_prior_state(self):
        """C2c-9: Transport failure before EXEC commits nothing — prior state intact."""
        # Establish T1 state (pending_costs) via the unfaulted store
        reconcile_position(
            self.store, POS["deal_id"],
            raw_batch([realization()]), POS,
            entry_reference=ENTRY_REF)

        # Fault: failure before EXEC → no commit
        failed = self.restart(FaultClient(self.client, "before"))
        with self.assertRaises(ConnectionError):
            reconcile_position(failed, POS["deal_id"], raw_batch(FULL_ROWS), POS,
                                entry_reference=ENTRY_REF,
                                cost_evidence=COST_EVIDENCE)

        # State remains at T1 (pending_costs) — not advanced to complete
        entry = self.restart().get_entry(POS["deal_id"])
        self.assertIsNotNone(entry["record"])
        self.assertEqual(entry["record"]["status"], "pending_costs")

    # ── opening collision blocks both positions from completing ────────────

    def test_opening_collision_prevents_two_outcomes(self):
        """C2c-9: Two positions sharing the same opening identity → neither completes.

        This is the structural guarantee behind the 'same event not two outcomes'
        invariant: the store's opening_key claim prevents either position from
        being reconciled until the ambiguity is resolved manually.
        """
        pos2 = position(deal_id="colliding-deal")
        register(self.store, pos2)

        # Both reconcile calls must raise — ambiguous opening tuple
        with self.assertRaises(EvidenceError) as ctx1:
            reconcile_position(
                self.store, POS["deal_id"], raw_batch(FULL_ROWS), POS,
                entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)
        self.assertIn("Ambiguous", str(ctx1.exception))

        with self.assertRaises(EvidenceError):
            reconcile_position(
                self.store, "colliding-deal", raw_batch(FULL_ROWS), pos2,
                entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)

        # Both entries remain unreconciled — zero completed outcomes
        self.assertTrue(
            all(e["record"] is None for e in self.restart().entries()),
            "No entry should have a completed record when opening is ambiguous")

    # ── realization claim blocks cross-position attribution ────────────────

    def test_complete_outcome_delivers_exactly_once_per_consumer(self):
        """C2c-9: project_once delivers exactly once per consumer, even after restart
        and re-reconciliation.

        This proves the combined invariant: reconcile_position is idempotent
        (same record on re-call) AND project_once's delivery marker ensures the
        learning callback fires only once — not once per reconcile_position call.
        """
        self.complete()

        def count_up(state, record):
            return {"count": state.get("count", 0) + 1}

        # First delivery
        r1 = self.store.project_once(POS["deal_id"], "c2c9-consumer", count_up)
        self.assertEqual(r1["count"], 1)

        # Restart → re-reconcile (idempotent) → attempt re-delivery
        restarted = self.restart()
        reconcile_position(
            restarted, POS["deal_id"], raw_batch(FULL_ROWS), POS,
            entry_reference=ENTRY_REF, cost_evidence=COST_EVIDENCE)

        # project_once skips when delivery marker is present
        blocker = Mock(side_effect=AssertionError("callback must not fire again"))
        r2 = restarted.project_once(POS["deal_id"], "c2c9-consumer", blocker)
        blocker.assert_not_called()
        self.assertEqual(r2["count"], 1)  # same accumulated state, not doubled


if __name__ == "__main__":
    unittest.main()
