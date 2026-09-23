"""Tests for transaction normalization (broker_transaction), open/close matching (broker_match),
and lifecycle collection (broker_match.collect_lifecycle_realizations).

No bot import, live Redis, or broker access.
Reuses the realization() and position() fixtures from test_broker_ledger.
"""

import unittest
from copy import deepcopy

from broker_ledger import EvidenceError
from broker_transaction import UNKNOWN, normalize_transaction, normalize_batch, parse_utc
from broker_match import (EXACT, HIGH_CONFIDENCE, AMBIGUOUS, NO_MATCH, CONFLICT,
                          match_realizations, collect_lifecycle_realizations)
from test_broker_ledger import realization, position


ACCOUNT = "fixture-account"


# ── Batch helper ────────────────────────────────────────────────────────────

def _batch(rows=None):
    """Wrap rows in a complete fetch_transaction_history-shaped batch."""
    rows = [realization()] if rows is None else rows
    return normalize_batch({
        "account_id": ACCOUNT,
        "history_complete": True,
        "transactions": rows,
    })


# ── Position evidence helper ─────────────────────────────────────────────────

def pos(**changes):
    """Minimal position evidence dict for matching tests."""
    base = {
        "account_id": ACCOUNT,
        "broker_instrument": "Spot Gold ($1)",
        "direction": "short",
        "entry_price": "4352.53",
        "original_quantity": "0.16",
        "opened_utc": "2026-09-21T05:45:56",
    }
    base.update(changes)
    return base


# ════════════════════════════════════════════════════════════════════════════
# Normalization tests
# ════════════════════════════════════════════════════════════════════════════

class NormalizationTests(unittest.TestCase):

    def test_deal_row_all_fields_present(self):
        t = normalize_transaction(realization(), ACCOUNT)
        self.assertEqual(t["schema_version"], 1)
        self.assertEqual(t["account_id"], ACCOUNT)
        self.assertEqual(t["transaction_type"], "deal")
        self.assertEqual(t["raw_transaction_type"], "DEAL")
        self.assertEqual(t["reference"], "close-1")
        self.assertEqual(t["instrument_name"], "Spot Gold ($1)")
        self.assertEqual(t["direction"], "short")     # size="-0.16" → short
        self.assertEqual(t["close_quantity"], "0.16")
        self.assertEqual(t["open_price"], "4352.53")
        self.assertEqual(t["close_price"], "4353.56")
        self.assertNotEqual(t["open_utc"], UNKNOWN)
        self.assertNotEqual(t["close_utc"], UNKNOWN)
        self.assertEqual(t["currency"], "USD")         # "$" → "USD"
        self.assertEqual(t["source"], "IG.history.transactions.v2")
        # deal_id / opening_deal_id always UNKNOWN — not in IG transaction rows
        self.assertEqual(t["deal_id"], UNKNOWN)
        self.assertEqual(t["opening_deal_id"], UNKNOWN)

    def test_positive_size_maps_to_long(self):
        t = normalize_transaction(realization(quantity="0.16"), ACCOUNT)
        self.assertEqual(t["direction"], "long")
        self.assertEqual(t["close_quantity"], "0.16")

    def test_zero_size_is_unknown(self):
        t = normalize_transaction(realization(quantity="0"), ACCOUNT)
        self.assertEqual(t["direction"], UNKNOWN)
        self.assertEqual(t["close_quantity"], UNKNOWN)

    def test_commission_row_has_unknown_position_fields(self):
        row = {"transactionType": "COMM", "reference": "fee-1", "profitAndLoss": "-$9"}
        t = normalize_transaction(row, ACCOUNT)
        self.assertEqual(t["transaction_type"], "commission")
        self.assertEqual(t["reference"], "fee-1")
        self.assertEqual(t["instrument_name"], UNKNOWN)
        self.assertEqual(t["direction"], UNKNOWN)
        self.assertEqual(t["close_quantity"], UNKNOWN)
        self.assertEqual(t["open_price"], UNKNOWN)
        self.assertEqual(t["open_utc"], UNKNOWN)
        self.assertEqual(t["cash_amount"], "-9")

    def test_deposit_row(self):
        row = {"transactionType": "DEPO", "profitAndLoss": "$150", "currency": "USD"}
        t = normalize_transaction(row, ACCOUNT)
        self.assertEqual(t["transaction_type"], "deposit")
        self.assertEqual(t["cash_amount"], "150")
        self.assertEqual(t["currency"], "USD")
        self.assertEqual(t["direction"], UNKNOWN)
        self.assertEqual(t["instrument_name"], UNKNOWN)

    def test_unknown_transaction_type_becomes_other(self):
        row = {"transactionType": "MYSTERY"}
        t = normalize_transaction(row, ACCOUNT)
        self.assertEqual(t["transaction_type"], "other")
        self.assertEqual(t["raw_transaction_type"], "MYSTERY")

    def test_absent_transaction_type_is_unknown(self):
        t = normalize_transaction({}, ACCOUNT)
        self.assertEqual(t["transaction_type"], UNKNOWN)
        self.assertIsNone(t["raw_transaction_type"])

    def test_pnl_format_variants(self):
        cases = [
            ("$-0.16", "-0.16"),
            ("-$9", "-9"),
            ("$0.17", "0.17"),
            ("1.23", "1.23"),
            ("-1.23", "-1.23"),
            ("$-9", "-9"),
            ("$1,234.56", "1234.56"),
            ("$9", "9"),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                row = {**realization(), "profitAndLoss": raw}
                self.assertEqual(normalize_transaction(row, ACCOUNT)["cash_amount"], expected)

    def test_missing_and_unparseable_fields_yield_unknown(self):
        row = {
            "transactionType": "DEAL",
            "size": "not-a-number",
            "openLevel": "NaN",
            "profitAndLoss": "N/A",
            "openDateUtc": "not-a-date",
        }
        t = normalize_transaction(row, ACCOUNT)
        self.assertEqual(t["direction"], UNKNOWN)
        self.assertEqual(t["close_quantity"], UNKNOWN)
        self.assertEqual(t["open_price"], UNKNOWN)
        self.assertEqual(t["cash_amount"], UNKNOWN)
        self.assertEqual(t["open_utc"], UNKNOWN)

    def test_currency_normalization(self):
        cases = [
            ("$", "USD"),
            ("USD", "USD"),
            ("GBP", "GBP"),
            ("", UNKNOWN),
            (None, UNKNOWN),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                row = {**realization(), "currency": raw}
                self.assertEqual(normalize_transaction(row, ACCOUNT)["currency"], expected)

    def test_raw_provenance_preserved_and_isolated(self):
        row = realization()
        original = deepcopy(row)
        t = normalize_transaction(row, ACCOUNT)
        self.assertEqual(t["raw"], original)
        # Mutating the original row does not change the stored raw copy
        row["reference"] = "mutated"
        self.assertEqual(t["raw"]["reference"], "close-1")

    def test_parse_utc_normalizes_to_utc_with_offset(self):
        result = parse_utc("2026-09-21T05:45:56")
        self.assertIn("+00:00", result)
        self.assertTrue(result.startswith("2026-09-21"))

    def test_parse_utc_handles_z_suffix(self):
        result = parse_utc("2026-09-21T05:45:56Z")
        self.assertIn("+00:00", result)

    def test_normalize_transaction_raises_on_invalid_inputs(self):
        with self.assertRaises(EvidenceError):
            normalize_transaction("not-a-dict", ACCOUNT)
        with self.assertRaises(EvidenceError):
            normalize_transaction({}, "")
        with self.assertRaises(EvidenceError):
            normalize_transaction({}, None)

    def test_normalize_batch_requires_complete_batch(self):
        with self.assertRaises(EvidenceError):
            normalize_batch({"account_id": ACCOUNT, "transactions": []})
        with self.assertRaises(EvidenceError):
            normalize_batch({"account_id": "", "history_complete": True, "transactions": []})
        with self.assertRaises(EvidenceError):
            normalize_batch({"history_complete": True, "transactions": []})

    def test_normalize_batch_preserves_all_types(self):
        rows = [
            realization(),
            {"transactionType": "COMM", "profitAndLoss": "-$9"},
            {"transactionType": "DEPO", "profitAndLoss": "$150"},
        ]
        normalized = normalize_batch({
            "account_id": ACCOUNT,
            "history_complete": True,
            "transactions": rows,
        })
        self.assertEqual(len(normalized), 3)
        self.assertEqual(normalized[0]["transaction_type"], "deal")
        self.assertEqual(normalized[1]["transaction_type"], "commission")
        self.assertEqual(normalized[2]["transaction_type"], "deposit")
        self.assertTrue(all(t["account_id"] == ACCOUNT for t in normalized))

    def test_normalize_batch_empty_transactions(self):
        result = normalize_batch({
            "account_id": ACCOUNT,
            "history_complete": True,
            "transactions": [],
        })
        self.assertEqual(result, [])


# ════════════════════════════════════════════════════════════════════════════
# Matching tests
# ════════════════════════════════════════════════════════════════════════════

class MatchingTests(unittest.TestCase):

    def test_exact_match_all_fields(self):
        """Default realization matches default position on all identity fields."""
        txs = _batch()
        result = match_realizations(pos(), txs)
        self.assertEqual(result["confidence"], EXACT)
        self.assertEqual(len(result["matches"]), 1)
        m = result["matches"][0]
        self.assertIn("instrument_name", m["basis"])
        self.assertIn("open_price", m["basis"])
        self.assertIn("direction", m["basis"])
        self.assertIn("close_quantity", m["basis"])

    def test_exact_identity_when_position_quantity_unknown(self):
        """Exact opening identity does not require full-close quantity."""
        txs = _batch()
        result = match_realizations(pos(original_quantity=None), txs)
        self.assertEqual(result["confidence"], EXACT)
        self.assertNotIn("close_quantity", result["matches"][0]["basis"])

    def test_no_match_empty_transaction_list(self):
        result = match_realizations(pos(), [])
        self.assertEqual(result["confidence"], NO_MATCH)

    def test_no_match_wrong_account(self):
        txs = _batch()
        result = match_realizations(pos(account_id="different-account"), txs)
        self.assertEqual(result["confidence"], NO_MATCH)

    def test_no_match_wrong_instrument(self):
        txs = _batch([realization(instrumentName="Different Instrument")])
        result = match_realizations(pos(), txs)
        self.assertEqual(result["confidence"], NO_MATCH)

    def test_no_match_wrong_price(self):
        txs = _batch([realization(openLevel="9999.00")])
        result = match_realizations(pos(), txs)
        self.assertEqual(result["confidence"], NO_MATCH)

    def test_no_match_commission_row_ignored(self):
        """Commission rows never close positions; they must not produce candidates."""
        txs = _batch([{"transactionType": "COMM", "reference": "fee", "profitAndLoss": "-$9"}])
        result = match_realizations(pos(), txs)
        self.assertEqual(result["confidence"], NO_MATCH)

    def test_conflict_direction_mismatch_with_matching_price(self):
        """account + instrument + open_price all match but direction contradicts."""
        # positive quantity → long; position is short → CONFLICT
        txs = _batch([realization(quantity="0.16")])
        result = match_realizations(pos(direction="short"), txs)
        self.assertEqual(result["confidence"], CONFLICT)
        self.assertIn("direction_conflict", result["matches"][0]["basis"])

    def test_no_conflict_when_price_differs(self):
        """Direction mismatch alone (different price) is a non-match, not a CONFLICT."""
        txs = _batch([realization(quantity="0.16", openLevel="9999.00")])
        result = match_realizations(pos(direction="short"), txs)
        # instrument matches, price doesn't → score < 3 → no conflict flag
        self.assertEqual(result["confidence"], NO_MATCH)

    def test_reference_alone_is_insufficient(self):
        """Same reference as position but different open_price → NO_MATCH.

        This is the spec requirement: reference is never used as a matching key.
        """
        txs = _batch([realization("close-1", "-0.16", "-0.16", openLevel="9999.00")])
        result = match_realizations(pos(), txs)  # position entry_price=4352.53
        self.assertEqual(result["confidence"], NO_MATCH)

    def test_ambiguous_two_partial_closes_same_price(self):
        """Two partial closes at the same open_price/direction: cannot pick one → AMBIGUOUS."""
        partial_a = realization("partial-a", "-0.08", "0.09")
        partial_b = realization("partial-b", "-0.08", "0.05")
        # pos has original_quantity=None so quantity check skipped; both have exact opening identity
        txs = _batch([partial_a, partial_b])
        result = match_realizations(pos(original_quantity=None), txs)
        self.assertEqual(result["confidence"], AMBIGUOUS)
        self.assertEqual(len(result["matches"]), 2)

    def test_shared_reference_two_gold_positions_resolved_by_open_price(self):
        """Two realizations share reference '7PE8MUAP' but differ in openLevel.

        This is the documented real case: two distinct Gold positions with the same
        closing reference.  The matcher MUST NOT merge them on reference alone.
        Each position must match its own transaction by open_price.
        """
        first_tx  = realization("7PE8MUAP", "-0.16", "0.17")                # openLevel=4352.53
        second_tx = realization("7PE8MUAP", "-0.04", "-0.11",
                                openLevel="4341.05",
                                openDateUtc="2026-09-21T05:46:56")

        primary_pos = pos(entry_price="4352.53", original_quantity="0.16")
        addon_pos   = pos(entry_price="4341.05", original_quantity="0.04",
                          opened_utc="2026-09-21T05:46:56")

        txs = _batch([first_tx, second_tx])

        # Primary matches only the first (open_price=4352.53)
        primary_result = match_realizations(primary_pos, txs)
        self.assertEqual(primary_result["confidence"], EXACT)
        matched_price = primary_result["matches"][0]["transaction"]["open_price"]
        self.assertEqual(matched_price, "4352.53")

        # Add-on matches only the second (open_price=4341.05)
        addon_result = match_realizations(addon_pos, txs)
        self.assertEqual(addon_result["confidence"], EXACT)
        matched_price = addon_result["matches"][0]["transaction"]["open_price"]
        self.assertEqual(matched_price, "4341.05")

    def test_exact_match_promoted_by_open_utc_when_quantity_absent(self):
        """open_utc is a confirming field: HIGH_CONFIDENCE + matching UTC → EXACT."""
        txs = _batch()  # openDateUtc="2026-09-21T05:45:56" → normalized "+00:00"
        # Without opening UTC the identity is ambiguous
        result_hc = match_realizations(pos(original_quantity=None, opened_utc=None), txs)
        self.assertEqual(result_hc["confidence"], AMBIGUOUS)
        # With matching opened_utc: EXACT
        result_ex = match_realizations(
            pos(original_quantity=None, opened_utc="2026-09-21T05:45:56+00:00"), txs)
        self.assertEqual(result_ex["confidence"], EXACT)
        self.assertIn("open_utc", result_ex["matches"][0]["basis"])

    def test_opened_utc_normalized_before_comparison(self):
        """Position's opened_utc without timezone offset still matches normalized UTC."""
        txs = _batch()
        result = match_realizations(
            pos(original_quantity=None, opened_utc="2026-09-21T05:45:56"), txs)
        self.assertEqual(result["confidence"], EXACT)
        self.assertIn("open_utc", result["matches"][0]["basis"])

    def test_partial_close_quantity_does_not_change_identity(self):
        """Exact opening identity does not require full-close quantity."""
        partial_tx = realization("partial", "-0.08", "0.09")
        txs = _batch([partial_tx])
        result = match_realizations(pos(), txs)  # pos quantity=0.16; tx quantity=0.08
        self.assertEqual(result["confidence"], EXACT)
        self.assertNotIn("close_quantity", result["matches"][0]["basis"])

    def test_long_position_exact_match(self):
        """Long position matches positive-size transaction by instrument + price + direction + quantity."""
        long_tx = realization(quantity="1", pnl="5.27", instrumentName="Micron Technology",
                              openLevel="955.25", closeLevel="960.52")
        long_pos = pos(broker_instrument="Micron Technology", direction="long",
                       entry_price="955.25", original_quantity="1")
        txs = _batch([long_tx])
        result = match_realizations(long_pos, txs)
        self.assertEqual(result["confidence"], EXACT)
        self.assertIn("direction", result["matches"][0]["basis"])

    def test_match_realizations_raises_on_invalid_position(self):
        with self.assertRaises(EvidenceError):
            match_realizations("not-a-dict", [])

    def test_match_realizations_raises_on_invalid_transactions(self):
        with self.assertRaises(EvidenceError):
            match_realizations(pos(), "not-a-list")

    def test_conflict_and_no_candidates_reports_conflict(self):
        """Conflict without any candidates → CONFLICT result."""
        txs = _batch([realization(quantity="0.16")])  # long vs position short
        result = match_realizations(pos(direction="short"), txs)
        self.assertEqual(result["confidence"], CONFLICT)
        self.assertTrue(len(result["matches"]) >= 1)

    def test_notes_field_always_present(self):
        for confidence, p, rows in [
            ("expect_exact",  pos(), [realization()]),
            ("expect_no_match", pos(), []),
            ("expect_conflict", pos(direction="short"), [realization(quantity="0.16")]),
        ]:
            with self.subTest(case=confidence):
                txs = _batch(rows) if rows else []
                result = match_realizations(p, txs)
                self.assertIn("notes", result)
                self.assertIsInstance(result["notes"], str)


# ════════════════════════════════════════════════════════════════════════════
# Lifecycle collection tests (C2c-3 partial + residual resolution)
# ════════════════════════════════════════════════════════════════════════════

class LifecycleTests(unittest.TestCase):
    """Tests for collect_lifecycle_realizations() — C2c-3 partial/residual reconciliation.

    This resolves the Stage 1 AMBIGUOUS limitation for the partial + residual case:
    match_realizations() returns AMBIGUOUS for two partial closes of the same position
    because it can't pick one.  collect_lifecycle_realizations() ACCUMULATES both
    and returns full_close when their quantities sum to original_quantity.
    """

    def test_single_full_close_is_full_close(self):
        result = collect_lifecycle_realizations(pos(), _batch())
        self.assertEqual(result["lifecycle_state"], "full_close")
        self.assertTrue(result["quantity_accounted"])
        self.assertEqual(result["total_closed_quantity"], "0.16")
        self.assertEqual(len(result["realizations"]), 1)

    def test_partial_plus_residual_resolves_to_full_close(self):
        """Stage 1 AMBIGUOUS case resolved: two partials sum to original_quantity."""
        partial_a = realization("partial-a", "-0.08", "0.09")
        partial_b = realization("partial-b", "-0.08", "-0.04")
        txs = _batch([partial_a, partial_b])
        result = collect_lifecycle_realizations(pos(), txs)
        # Both partials are collected; sum(0.08 + 0.08) == original_quantity(0.16)
        self.assertEqual(result["lifecycle_state"], "full_close")
        self.assertEqual(len(result["realizations"]), 2)
        self.assertTrue(result["quantity_accounted"])
        self.assertEqual(result["total_closed_quantity"], "0.16")

    def test_partial_only_is_partial_close(self):
        """Single partial close (0.08 of 0.16 original) → partial_close, residual pending."""
        partial = realization("partial-a", "-0.08", "0.09")
        txs = _batch([partial])
        result = collect_lifecycle_realizations(pos(), txs)
        self.assertEqual(result["lifecycle_state"], "partial_close")
        self.assertFalse(result["quantity_accounted"])
        self.assertEqual(result["total_closed_quantity"], "0.08")

    def test_three_partial_closes_accumulate(self):
        """Multiple partials: three sub-quantities that sum to original_quantity."""
        t1 = realization("r1", "-0.06", "0.05")
        t2 = realization("r2", "-0.06", "0.05")
        t3 = realization("r3", "-0.04", "-0.03")
        txs = _batch([t1, t2, t3])
        result = collect_lifecycle_realizations(pos(), txs)
        self.assertEqual(result["lifecycle_state"], "full_close")
        self.assertEqual(len(result["realizations"]), 3)
        self.assertEqual(result["total_closed_quantity"], "0.16")

    def test_excess_close_signals_shared_opening_identity(self):
        """Two full-close transactions for same price/direction signal position mixing.

        This is the residual Stage 1 AMBIGUOUS limitation: without opened_utc we
        cannot separate Position A's close from Position B's close when both were
        opened at the same price.  excess_close is the correct signal.
        """
        full_a = realization("full-a", "-0.16", "0.17")   # Position A's close
        full_b = realization("full-b", "-0.16", "-0.14")  # Position B's close
        txs = _batch([full_a, full_b])
        result = collect_lifecycle_realizations(pos(), txs)
        self.assertEqual(result["lifecycle_state"], "excess_close")
        self.assertFalse(result["quantity_accounted"])
        self.assertIn("excess", result["notes"])
        self.assertIn("opened_utc", result["notes"])
        # Both transactions collected — caller must NOT pick arbitrarily
        self.assertEqual(len(result["realizations"]), 2)

    def test_no_match_empty_transactions(self):
        result = collect_lifecycle_realizations(pos(), [])
        self.assertEqual(result["lifecycle_state"], "no_match")
        self.assertFalse(result["quantity_accounted"])
        self.assertEqual(result["deal_references"], set())

    def test_no_match_commission_row_not_a_realization(self):
        comm = {"transactionType": "COMM", "reference": "fee", "profitAndLoss": "-$9"}
        txs = _batch([comm])
        result = collect_lifecycle_realizations(pos(), txs)
        self.assertEqual(result["lifecycle_state"], "no_match")

    def test_conflict_lifecycle(self):
        """Direction-contradicting transaction → conflict state."""
        long_tx = realization(quantity="0.16")  # positive = long; position is short
        txs = _batch([long_tx])
        result = collect_lifecycle_realizations(pos(direction="short"), txs)
        self.assertEqual(result["lifecycle_state"], "conflict")
        self.assertEqual(result["realizations"], [])

    def test_deal_references_collected_from_all_realizations(self):
        """deal_references collects all references from matched DEAL rows."""
        t1 = realization("ref-a", "-0.08", "0.05")
        t2 = realization("ref-b", "-0.08", "-0.03")
        txs = _batch([t1, t2])
        result = collect_lifecycle_realizations(pos(), txs)
        self.assertEqual(result["deal_references"], {"ref-a", "ref-b"})

    def test_raw_rows_populated_for_reconcile_completed_trade(self):
        """raw_rows contains the original IG row dicts for downstream reconciliation."""
        txs = _batch()
        result = collect_lifecycle_realizations(pos(), txs)
        self.assertEqual(len(result["raw_rows"]), 1)
        self.assertIn("transactionType", result["raw_rows"][0])
        self.assertEqual(result["raw_rows"][0]["transactionType"], "DEAL")

    def test_lifecycle_with_unknown_position_quantity(self):
        """When original_quantity is None: cannot determine lifecycle completeness."""
        txs = _batch()
        result = collect_lifecycle_realizations(pos(original_quantity=None), txs)
        # Still collects the match; lifecycle state is partial_close (indeterminate)
        self.assertEqual(len(result["realizations"]), 1)
        self.assertFalse(result["quantity_accounted"])

    def test_lifecycle_raises_on_invalid_inputs(self):
        with self.assertRaises(EvidenceError):
            collect_lifecycle_realizations("not-a-dict", [])
        with self.assertRaises(EvidenceError):
            collect_lifecycle_realizations(pos(), "not-a-list")


if __name__ == "__main__":
    unittest.main()
