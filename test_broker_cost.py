"""Tests for broker_cost.py: classification, cost record building, and attribution.

Covers C2c-3 (lifecycle integration), C2c-4 (add-on/primary identity),
C2c-5 (cost attribution), C2c-6 (non-trading flows), C2c-7 (financing).

No bot import, live Redis, or broker access.
"""

import unittest

from broker_ledger import EvidenceError, reconcile_completed_trade
from broker_transaction import UNKNOWN, normalize_transaction, normalize_batch
from broker_cost import (POSITION, INSTRUMENT, ACCOUNT, NON_TRADING,
                          classify_cost, build_cost_record, attribute_costs)
from broker_match import collect_lifecycle_realizations
from test_broker_ledger import position, realization, complete


ACCOUNT_ID = "fixture-account"
DEAL_ID    = "opening-1"


# ── Row helpers ──────────────────────────────────────────────────────────────

def norm(row, account=ACCOUNT_ID):
    return normalize_transaction(row, account)


def batch(rows, account=ACCOUNT_ID):
    return normalize_batch({
        "account_id": account,
        "history_complete": True,
        "transactions": rows,
    })


def comm_row(reference="close-1", amount="-9"):
    """Raw IG commission row."""
    return {"transactionType": "COMM", "reference": reference,
            "profitAndLoss": "$" + amount, "currency": "$"}


def swap_row(instrument="Spot Gold ($1)", amount="-0.50"):
    """Raw IG financing row."""
    return {"transactionType": "SWAP", "instrumentName": instrument,
            "profitAndLoss": "$" + amount, "currency": "$"}


def depo_row(amount="150.00"):
    """Raw IG deposit row — account capital movement, not trade P&L."""
    return {"transactionType": "DEPO", "profitAndLoss": "$" + amount, "currency": "$"}


def pos_evidence(broker_instrument="Spot Gold ($1)", **changes):
    """Minimal position evidence dict for attribution tests."""
    base = {"account_id": ACCOUNT_ID, "deal_id": DEAL_ID,
            "broker_instrument": broker_instrument}
    base.update(changes)
    return base


# ════════════════════════════════════════════════════════════════════════════
# C2c-5 / C2c-6 / C2c-7: Cost classification
# ════════════════════════════════════════════════════════════════════════════

class CostClassificationTests(unittest.TestCase):

    def test_deposit_is_non_trading(self):
        """C2c-6: DEPO rows are NON_TRADING regardless of amount or description."""
        cl = classify_cost(norm(depo_row("150.00")))
        self.assertEqual(cl["scope"], NON_TRADING)
        self.assertIn("capital movement", cl["reason"])
        self.assertEqual(cl["amount"], "150.00")

    def test_withdrawal_is_non_trading(self):
        cl = classify_cost(norm({"transactionType": "WITH", "profitAndLoss": "$-50",
                                  "currency": "$"}))
        self.assertEqual(cl["scope"], NON_TRADING)

    def test_commission_with_reference_is_position_scope(self):
        """C2c-5: COMM with reference is a position-level attribution candidate."""
        cl = classify_cost(norm(comm_row("close-1", "-9")))
        self.assertEqual(cl["scope"], POSITION)
        self.assertEqual(cl["reference"], "close-1")
        self.assertIn("requires deal confirmation", cl["reason"])

    def test_commission_without_reference_is_account_scope(self):
        cl = classify_cost(norm({"transactionType": "COMM", "profitAndLoss": "$-9",
                                  "currency": "$"}))
        self.assertEqual(cl["scope"], ACCOUNT)
        self.assertEqual(cl["reference"], UNKNOWN)

    def test_financing_with_instrument_is_instrument_scope(self):
        """C2c-7: SWAP rows with instrument are INSTRUMENT scope — not POSITION."""
        cl = classify_cost(norm(swap_row("Spot Gold ($1)", "-0.50")))
        self.assertEqual(cl["scope"], INSTRUMENT)
        self.assertEqual(cl["instrument"], "Spot Gold ($1)")
        self.assertIn("multiple positions possible", cl["reason"])

    def test_financing_without_instrument_is_account_scope(self):
        cl = classify_cost(norm({"transactionType": "SWAP", "profitAndLoss": "$-0.30",
                                  "currency": "$"}))
        self.assertEqual(cl["scope"], ACCOUNT)
        self.assertEqual(cl["instrument"], UNKNOWN)

    def test_interest_and_other_types_are_account_scope(self):
        for tx_type in ("INTEREST", "MYSTERY"):
            with self.subTest(tx_type=tx_type):
                cl = classify_cost(norm({"transactionType": tx_type,
                                          "profitAndLoss": "$-1", "currency": "$"}))
                self.assertEqual(cl["scope"], ACCOUNT)


# ════════════════════════════════════════════════════════════════════════════
# C2c-5: Cost record building
# ════════════════════════════════════════════════════════════════════════════

class BuildCostRecordTests(unittest.TestCase):

    def test_commission_row_produces_valid_cost_record(self):
        """C2c-5: COMM row → dict usable as reconcile_completed_trade cost entry."""
        rec = build_cost_record(norm(comm_row("close-1", "-9")), ACCOUNT_ID, DEAL_ID)
        self.assertIsNotNone(rec)
        self.assertEqual(rec["account_id"], ACCOUNT_ID)
        self.assertEqual(rec["deal_id"], DEAL_ID)
        self.assertEqual(rec["kind"], "commission")
        self.assertEqual(rec["currency"], "USD")
        self.assertEqual(rec["broker_reference"], "close-1")
        self.assertEqual(rec["amount"], "-9")
        self.assertIn("COMM", rec["source"])
        self.assertTrue(rec["cost_id"])  # non-empty hash

    def test_financing_row_without_reference_returns_none(self):
        """C2c-7: IG SWAP rows carry no broker reference → returns None.
        Callers route SWAP rows via instrument_costs, not position_costs."""
        rec = build_cost_record(norm(swap_row()), ACCOUNT_ID, DEAL_ID)
        self.assertIsNone(rec)

    def test_financing_row_with_reference_produces_other_kind(self):
        """C2c-7: SWAP row that happens to carry a reference → kind='other' record."""
        row = {"transactionType": "SWAP", "reference": "swap-ref-1",
               "profitAndLoss": "$-0.50", "currency": "$"}
        rec = build_cost_record(norm(row), ACCOUNT_ID, DEAL_ID)
        self.assertIsNotNone(rec)
        self.assertEqual(rec["kind"], "other")
        self.assertIn("SWAP", rec["source"])

    def test_missing_reference_returns_none(self):
        """Cannot build a cost record without a broker reference."""
        row = {"transactionType": "COMM", "profitAndLoss": "$-9", "currency": "$"}
        result = build_cost_record(norm(row), ACCOUNT_ID, DEAL_ID)
        self.assertIsNone(result)

    def test_missing_amount_returns_none(self):
        row = {"transactionType": "COMM", "reference": "r1", "currency": "$"}
        result = build_cost_record(norm(row), ACCOUNT_ID, DEAL_ID)
        self.assertIsNone(result)

    def test_non_usd_currency_raises(self):
        row = {"transactionType": "COMM", "reference": "r1",
               "profitAndLoss": "-9", "currency": "GBP"}
        with self.assertRaises(EvidenceError):
            build_cost_record(norm(row), ACCOUNT_ID, DEAL_ID)

    def test_deal_row_raises(self):
        row = realization()  # transactionType=DEAL
        with self.assertRaises(EvidenceError):
            build_cost_record(norm(row), ACCOUNT_ID, DEAL_ID)

    def test_cost_id_is_stable_and_content_derived(self):
        """Same input → same cost_id; different input → different cost_id."""
        rec1 = build_cost_record(norm(comm_row("r1", "-9")), ACCOUNT_ID, DEAL_ID)
        rec2 = build_cost_record(norm(comm_row("r1", "-9")), ACCOUNT_ID, DEAL_ID)
        rec3 = build_cost_record(norm(comm_row("r2", "-9")), ACCOUNT_ID, DEAL_ID)
        self.assertEqual(rec1["cost_id"], rec2["cost_id"])
        self.assertNotEqual(rec1["cost_id"], rec3["cost_id"])

    def test_invalid_account_or_deal_id_raises(self):
        row_n = norm(comm_row())
        with self.assertRaises(EvidenceError):
            build_cost_record(row_n, "", DEAL_ID)
        with self.assertRaises(EvidenceError):
            build_cost_record(row_n, ACCOUNT_ID, "")


# ════════════════════════════════════════════════════════════════════════════
# C2c-5 + C2c-6 + C2c-7: Cost attribution
# ════════════════════════════════════════════════════════════════════════════

class AttributeCostsTests(unittest.TestCase):

    def _txs(self, rows):
        return batch(rows)

    def test_commission_matched_by_close_reference_becomes_position_cost(self):
        """C2c-5: COMM whose reference matches deal_references → position_costs."""
        rows = [realization("close-1"), comm_row("close-1", "-9")]
        result = attribute_costs(pos_evidence(), self._txs(rows),
                                 deal_references={"close-1"}, entry_reference="open-1")
        self.assertEqual(len(result["position_costs"]), 1)
        self.assertEqual(result["position_costs"][0]["kind"], "commission")
        self.assertEqual(result["position_costs"][0]["amount"], "-9")
        self.assertEqual(len(result["unattributed_costs"]), 0)

    def test_opening_commission_attributed_via_entry_reference(self):
        """C2c-5: COMM for opening leg matched by entry_reference, not deal_reference."""
        rows = [realization("close-1"), comm_row("open-1", "-9")]
        result = attribute_costs(pos_evidence(), self._txs(rows),
                                 deal_references={"close-1"}, entry_reference="open-1")
        self.assertEqual(len(result["position_costs"]), 1)
        self.assertEqual(result["position_costs"][0]["broker_reference"], "open-1")

    def test_both_commission_legs_attributed_together(self):
        """C2c-5: Opening + closing commission both attributed when refs supplied."""
        rows = [realization("close-1"),
                comm_row("close-1", "-9"),
                comm_row("open-1", "-9")]
        result = attribute_costs(pos_evidence(), self._txs(rows),
                                 deal_references={"close-1"}, entry_reference="open-1")
        self.assertEqual(len(result["position_costs"]), 2)
        self.assertTrue(result["cost_complete"])

    def test_commission_not_in_deal_refs_goes_to_unattributed(self):
        """C2c-5: COMM with unrecognised reference is NOT silently attributed."""
        rows = [comm_row("unknown-ref", "-9")]
        result = attribute_costs(pos_evidence(), self._txs(rows),
                                 deal_references={"close-1"}, entry_reference="open-1")
        self.assertEqual(len(result["position_costs"]), 0)
        self.assertEqual(len(result["unattributed_costs"]), 1)
        self.assertFalse(result["cost_complete"])

    def test_depo_is_excluded_from_all_trade_cost_buckets(self):
        """C2c-6: DEPO ($150 TBC concession or any deposit) → non_trading only."""
        rows = [depo_row("150.00"), realization("close-1")]
        result = attribute_costs(pos_evidence(), self._txs(rows),
                                 deal_references={"close-1"}, entry_reference="open-1")
        self.assertEqual(len(result["non_trading"]), 1)
        self.assertEqual(result["non_trading"][0]["scope"], NON_TRADING)
        self.assertEqual(len(result["position_costs"]), 0)
        self.assertEqual(len(result["account_costs"]), 0)

    def test_depo_does_not_make_cost_complete_false(self):
        """C2c-6: Non-trading flows don't block cost_complete."""
        rows = [depo_row("150.00"),
                realization("close-1"),
                comm_row("close-1", "-9"),
                comm_row("open-1", "-9")]
        result = attribute_costs(pos_evidence(), self._txs(rows),
                                 deal_references={"close-1"}, entry_reference="open-1")
        self.assertEqual(len(result["non_trading"]), 1)
        self.assertTrue(result["cost_complete"])

    def test_financing_for_same_instrument_blocks_cost_complete(self):
        """C2c-7: SWAP for this instrument → instrument_costs, cost_complete=False."""
        rows = [realization("close-1"),
                comm_row("close-1", "-9"),
                comm_row("open-1", "-9"),
                swap_row("Spot Gold ($1)", "-0.50")]
        result = attribute_costs(pos_evidence(), self._txs(rows),
                                 deal_references={"close-1"}, entry_reference="open-1")
        self.assertEqual(len(result["instrument_costs"]), 1)
        self.assertFalse(result["cost_complete"])
        self.assertIn("financing", result["cost_complete_reason"])

    def test_financing_for_different_instrument_goes_to_account_costs(self):
        """C2c-7: SWAP for a different instrument → account_costs (not instrument_costs)."""
        rows = [swap_row("US OIL", "-0.20")]  # different instrument
        result = attribute_costs(pos_evidence(broker_instrument="Spot Gold ($1)"),
                                 self._txs(rows),
                                 deal_references={"close-1"}, entry_reference="open-1")
        self.assertEqual(len(result["instrument_costs"]), 0)
        self.assertEqual(len(result["account_costs"]), 1)

    def test_missing_deal_references_makes_cost_incomplete(self):
        """Closing-leg references not supplied → cost_complete=False."""
        rows = [comm_row("close-1", "-9")]
        result = attribute_costs(pos_evidence(), self._txs(rows),
                                 deal_references=None, entry_reference="open-1")
        self.assertFalse(result["cost_complete"])
        self.assertIn("deal_references", result["cost_complete_reason"])

    def test_missing_entry_reference_makes_cost_incomplete(self):
        """Opening-leg reference not supplied → cost_complete=False."""
        rows = [comm_row("close-1", "-9")]
        result = attribute_costs(pos_evidence(), self._txs(rows),
                                 deal_references={"close-1"}, entry_reference=None)
        self.assertFalse(result["cost_complete"])
        self.assertIn("entry_reference", result["cost_complete_reason"])

    def test_empty_batch_with_refs_supplied_is_cost_complete(self):
        """No costs at all + refs supplied → cost_complete=True (no unknowns)."""
        result = attribute_costs(pos_evidence(), batch([realization("close-1")]),
                                 deal_references={"close-1"}, entry_reference="open-1")
        self.assertEqual(len(result["position_costs"]), 0)
        self.assertTrue(result["cost_complete"])

    def test_invalid_inputs_raise(self):
        with self.assertRaises(EvidenceError):
            attribute_costs("not-a-dict", [])
        with self.assertRaises(EvidenceError):
            attribute_costs(pos_evidence(), "not-a-list")


# ════════════════════════════════════════════════════════════════════════════
# C2c-5 integration: attribute → reconcile round-trip
# ════════════════════════════════════════════════════════════════════════════

class CostReconcileIntegrationTests(unittest.TestCase):
    """Confirms attribute_costs() output is directly usable by reconcile_completed_trade."""

    def test_equity_commission_flips_gross_winner(self):
        """C2c-5 integration: gross +$5.27 minus $9+$9 commission → net -$12.73."""
        pos = position(instrument="MU", broker_instrument="Micron Technology",
                       direction="long", entry_price="955.25", original_quantity="1",
                       notional_per_quantity="955.25")
        deal_row = realization(quantity="1", pnl="5.27", instrumentName="Micron Technology",
                               openLevel="955.25", closeLevel="960.52",
                               reference="close-mu-1")
        open_comm = comm_row("open-mu-1", "-9")
        close_comm = comm_row("close-mu-1", "-9")
        raw_rows = [deal_row, open_comm, close_comm]
        txs = normalize_batch({
            "account_id": pos["account_id"],
            "history_complete": True,
            "transactions": raw_rows,
        })
        lc = collect_lifecycle_realizations(
            {"account_id": pos["account_id"],
             "broker_instrument": "Micron Technology",
             "direction": "long",
             "entry_price": "955.25",
             "original_quantity": "1", "opened_utc": pos["opened_utc"]},
            txs)
        self.assertEqual(lc["lifecycle_state"], "full_close")
        costs = attribute_costs(
            {"account_id": pos["account_id"],
             "deal_id": pos["deal_id"],
             "broker_instrument": "Micron Technology"},
            txs,
            deal_references=lc["deal_references"],
            entry_reference="open-mu-1")
        self.assertEqual(len(costs["position_costs"]), 2)
        self.assertTrue(costs["cost_complete"])
        record = reconcile_completed_trade(
            pos, lc["raw_rows"], costs["position_costs"],
            history_complete=True, costs_complete=True)
        self.assertEqual(record["status"], "provisional")
        self.assertEqual(record["gross_realized_pnl"], "5.27")
        self.assertEqual(record["net_identified_pnl"], "-12.73")
        self.assertIsNone(record["won"])

    def test_partial_close_remains_pending_with_costs(self):
        """C2c-3 + C2c-5: partial realization with attributed costs → pending_realizations."""
        pos = position()
        partial_deal = realization("partial-1", "-0.08", "0.09")
        partial_comm = comm_row("partial-1", "-9")
        raw_rows = [partial_deal, partial_comm]
        txs = normalize_batch({
            "account_id": pos["account_id"],
            "history_complete": True,
            "transactions": raw_rows,
        })
        lc = collect_lifecycle_realizations(
            {"account_id": pos["account_id"],
             "broker_instrument": pos["broker_instrument"],
             "direction": pos["direction"],
             "entry_price": pos["entry_price"],
             "original_quantity": pos["original_quantity"], "opened_utc": pos["opened_utc"]},
            txs)
        self.assertEqual(lc["lifecycle_state"], "partial_close")
        costs = attribute_costs(
            {"account_id": pos["account_id"],
             "deal_id": pos["deal_id"],
             "broker_instrument": pos["broker_instrument"]},
            txs,
            deal_references=lc["deal_references"],
            entry_reference="open-gold-1")
        record = reconcile_completed_trade(
            pos, lc["raw_rows"], costs["position_costs"],
            history_complete=True, costs_complete=costs["cost_complete"])
        self.assertEqual(record["status"], "pending_realizations")
        self.assertIsNone(record["net_realized_pnl"])

    def test_missing_costs_leave_net_pnl_unknown(self):
        """C2c-5: costs_complete=False → net_realized_pnl is None, won is None."""
        pos = position()
        txs = normalize_batch({
            "account_id": pos["account_id"],
            "history_complete": True,
            "transactions": [realization("close-1")],
        })
        costs = attribute_costs(
            {"account_id": pos["account_id"],
             "deal_id": pos["deal_id"],
             "broker_instrument": pos["broker_instrument"]},
            txs,
            deal_references={"close-1"},
            entry_reference=None)  # entry_reference missing → cost_complete=False
        record = reconcile_completed_trade(
            pos, [realization("close-1")], costs["position_costs"],
            history_complete=True, costs_complete=costs["cost_complete"])
        self.assertEqual(record["status"], "pending_costs")
        self.assertIsNone(record["net_realized_pnl"])
        self.assertIsNone(record["won"])


# ════════════════════════════════════════════════════════════════════════════
# C2c-4: Add-on / primary identity
# ════════════════════════════════════════════════════════════════════════════

class AddOnPrimaryIdentityTests(unittest.TestCase):
    """Confirm primary and add-on positions cannot cross-attribute via collect/attribute.

    The existing broker_pending.register_opening() prevents same-tuple collision;
    these tests confirm the lifecycle and cost layers respect distinct identity.
    """

    def test_different_price_positions_do_not_cross_attribute(self):
        """Primary and add-on at different entry prices → each gets own realizations.

        Different references so deal_references sets are disjoint; discrimination is
        on open_price.  Same reference (the real two-Gold case) is tested via the
        shared-reference case in test_broker_transaction.py.
        """
        primary_close = realization("7PE8MUAP-P", "-0.16", "0.17")          # openLevel=4352.53
        addon_close   = realization("7PE8MUAP-A", "-0.04", "-0.11",
                                     openLevel="4341.05",
                                     openDateUtc="2026-09-21T05:46:56")

        txs = normalize_batch({
            "account_id": ACCOUNT_ID,
            "history_complete": True,
            "transactions": [primary_close, addon_close],
        })

        primary_pos = {"account_id": ACCOUNT_ID, "broker_instrument": "Spot Gold ($1)",
                       "direction": "short", "entry_price": "4352.53", "original_quantity": "0.16", "opened_utc": "2026-09-21T05:45:56"}
        addon_pos   = {"account_id": ACCOUNT_ID, "broker_instrument": "Spot Gold ($1)",
                       "direction": "short", "entry_price": "4341.05", "original_quantity": "0.04", "opened_utc": "2026-09-21T05:46:56"}

        primary_lc = collect_lifecycle_realizations(primary_pos, txs)
        addon_lc   = collect_lifecycle_realizations(addon_pos, txs)

        # Each gets only its own realization — no cross-attribution
        self.assertEqual(primary_lc["lifecycle_state"], "full_close")
        self.assertEqual(addon_lc["lifecycle_state"], "full_close")
        self.assertEqual(len(primary_lc["realizations"]), 1)
        self.assertEqual(len(addon_lc["realizations"]), 1)
        primary_ref = primary_lc["deal_references"]
        addon_ref   = addon_lc["deal_references"]
        self.assertTrue(primary_ref.isdisjoint(addon_ref),
                        "Primary and add-on deal references must not overlap")

    def test_unknown_origin_position_cannot_attribute_costs_without_deal_id(self):
        """C2c-4: position with no deal_id → build_cost_record raises EvidenceError."""
        row_n = normalize_transaction(comm_row("r1", "-9"), ACCOUNT_ID)
        with self.assertRaises(EvidenceError):
            build_cost_record(row_n, ACCOUNT_ID, "")  # empty deal_id

    def test_same_price_same_direction_signals_excess_to_caller(self):
        """C2c-4: same-price primary and add-on → excess_close, caller must not pick."""
        full_primary = realization("ref-p", "-0.16", "0.17")
        full_addon   = realization("ref-a", "-0.16", "-0.02")  # same price, same quantity
        txs = normalize_batch({
            "account_id": ACCOUNT_ID,
            "history_complete": True,
            "transactions": [full_primary, full_addon],
        })
        pos = {"account_id": ACCOUNT_ID, "broker_instrument": "Spot Gold ($1)",
               "direction": "short", "entry_price": "4352.53", "original_quantity": "0.16", "opened_utc": "2026-09-21T05:45:56"}
        result = collect_lifecycle_realizations(pos, txs)
        # 0.16 + 0.16 = 0.32 > 0.16 → excess_close; we can't know which belongs to primary
        self.assertEqual(result["lifecycle_state"], "excess_close")


if __name__ == "__main__":
    unittest.main()
