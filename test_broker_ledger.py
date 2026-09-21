"""Offline broker fixtures. Import only the pure ledger, never the live bot."""

from copy import deepcopy
import json
import unittest

from broker_ledger import EvidenceError, completed_history_view, reconcile_completed_trade


def position(**changes):
    value = dict(account_id="fixture-account", deal_id="opening-1", instrument="GOLD",
                 broker_instrument="Spot Gold ($1)", direction="short", currency="USD",
                 opened_utc="2026-09-21T05:45:56", entry_price="4352.53",
                 original_quantity="0.16", notional_per_quantity="4352.53",
                 role="primary", strategy_context={"conviction": 3}, exit_reason="dple_trail")
    value.update(changes)
    return value


def realization(reference="close-1", quantity="-0.16", pnl="-0.16", **changes):
    value = dict(transactionType="DEAL", instrumentName="Spot Gold ($1)",
                 openDateUtc="2026-09-21T05:45:56", openLevel="4352.53",
                 dateUtc="2026-09-21T05:52:47", closeLevel="4353.56", size=quantity,
                 reference=reference, profitAndLoss="$" + pnl, currency="$")
    value.update(changes)
    return value


def complete(pos, rows, costs=()):
    return reconcile_completed_trade(pos, rows, costs, history_complete=True, costs_complete=True)


class BrokerLedgerTests(unittest.TestCase):
    def test_real_gold_partial_winner_not_residual_loser(self):
        rows = [realization("7H459DAM", "-0.08", "-0.08"),
                realization("7H3QM7AD", "-0.08", "0.09", closeLevel="4351.44",
                            dateUtc="2026-09-21T05:50:31")]
        record = complete(position(), rows)
        self.assertEqual(record["net_realized_pnl"], "0.01")
        self.assertEqual(record["remaining_quantity"], "0")
        self.assertEqual(record["original_notional"], "696.4048")
        self.assertEqual(record["remaining_notional"], "0")
        self.assertTrue(record["won"])
        self.assertEqual(completed_history_view(record)["dollar_pnl"], .01)
        self.assertEqual(record["realizations"][0]["broker_reference"], "7H3QM7AD")

    def test_partial_outcome_combinations(self):
        for a, b, result in [("2", "-1", "1"), ("2", "-3", "-1"), ("-2", "3", "1")]:
            with self.subTest(a=a, b=b):
                rec = complete(position(), [realization("a", "-0.08", a), realization("b", "-0.08", b)])
                self.assertEqual(rec["net_realized_pnl"], result)
                self.assertEqual(rec["won"], result == "1")

    def test_multiple_partials(self):
        rec = complete(position(), [realization("a", "-0.04", "0.1"),
                                    realization("b", "-0.04", "0.2"),
                                    realization("c", "-0.08", "-0.15")])
        self.assertEqual(rec["net_realized_pnl"], "0.15")
        self.assertEqual(rec["partial_close_count"], 2)

    def test_partial_remains_pending_with_proportional_exposure(self):
        rec = complete(position(), [realization(quantity="-0.04", pnl="0.1")])
        self.assertEqual(rec["status"], "pending_realizations")
        self.assertEqual(rec["remaining_quantity"], "0.12")
        self.assertEqual(rec["remaining_notional"], "522.3036")
        self.assertIsNone(rec["net_realized_pnl"])
        self.assertIsNone(rec["won"])
        with self.assertRaises(EvidenceError):
            completed_history_view(rec)

    def test_empty_history_does_not_mean_flat_or_zero_profit(self):
        rec = complete(position(), [])
        self.assertEqual(rec["status"], "pending_realizations")
        self.assertIsNone(rec["exit_utc"])

    def test_incomplete_pagination_prevents_completion(self):
        rec = reconcile_completed_trade(position(), [realization()], costs_complete=True)
        self.assertEqual(rec["status"], "pending_realizations")

    def test_unknown_costs_do_not_become_zero_cost_profit(self):
        rec = reconcile_completed_trade(position(), [realization(pnl="1")], history_complete=True)
        self.assertEqual(rec["status"], "pending_costs")
        self.assertIsNone(rec["won"])

    def test_broker_stop_needs_no_software_close_receipt(self):
        rec = complete(position(exit_reason="broker_close"), [realization()])
        self.assertEqual(rec["status"], "complete")
        self.assertEqual(rec["exit_reason"], "broker_close")
        self.assertFalse(rec["won"])

    def test_repeat_observations_and_order_independence(self):
        a = realization("a", "-0.08", "0.09")
        b = realization("b", "-0.08", "-0.08")
        self.assertEqual(complete(position(), [a, b]), complete(position(), [b, a, b, a]))

    def test_shared_closing_reference_does_not_merge_positions(self):
        first = realization("7PE8MUAP", "-0.16", "0.17")
        second = realization("7PE8MUAP", "-0.04", "-0.11", openLevel="4341.05",
                             openDateUtc="2026-09-21T05:46:56")
        addon = position(deal_id="addon", entry_price="4341.05", original_quantity="0.04",
                         opened_utc="2026-09-21T05:46:56", notional_per_quantity="4341.05", role="addon")
        r1 = complete(position(), [first, second])
        r2 = complete(addon, [first, second])
        self.assertEqual(r1["net_realized_pnl"], "0.17")
        self.assertEqual(r2["net_realized_pnl"], "-0.11")
        self.assertNotEqual(r1["trade_id"], r2["trade_id"])
        self.assertNotEqual(r1["realizations"][0]["realization_id"], r2["realizations"][0]["realization_id"])
        self.assertEqual(r2["original_notional"], "173.642")

    def test_conflicting_broker_revision_requires_review(self):
        with self.assertRaises(EvidenceError):
            complete(position(), [realization(), realization(pnl="-0.17")])

    def test_over_realization_requires_review(self):
        with self.assertRaises(EvidenceError):
            complete(position(), [realization(quantity="-0.17")])

    def test_equity_commission_flips_gross_winner(self):
        pos = position(instrument="MU", broker_instrument="Micron", direction="long",
                       entry_price="955.25", original_quantity="1", notional_per_quantity="955.25")
        row = realization(quantity="1", pnl="5.27", instrumentName="Micron",
                          openLevel="955.25", closeLevel="960.52")
        fees = [dict(account_id=pos["account_id"], deal_id=pos["deal_id"], cost_id=k,
                     amount="-9", kind="commission", currency="USD",
                     broker_reference=k, source="IG.history.transactions") for k in ("entry-fee", "exit-fee")]
        rec = complete(pos, [row], fees + fees)
        self.assertEqual(rec["gross_realized_pnl"], "5.27")
        self.assertEqual(rec["net_realized_pnl"], "-12.73")
        self.assertFalse(rec["won"])
        self.assertEqual(completed_history_view(rec)["commission"], 18)

    def test_unattributed_costs_are_not_assigned_by_symbol(self):
        other = dict(account_id="different-account", deal_id="opening-1", amount="-99")
        self.assertEqual(complete(position(), [realization()], [other])["commissions"], "0")

    def test_currency_is_not_silently_converted(self):
        with self.assertRaises(EvidenceError):
            complete(position(), [realization(currency="GBP")])

    def test_missing_and_non_finite_identity_rejected(self):
        for changes in [dict(deal_id=""), dict(original_quantity="NaN"), dict(entry_price="Infinity"),
                        dict(original_quantity="0"), dict(direction="unknown")]:
            with self.subTest(changes=changes), self.assertRaises(EvidenceError):
                complete(position(**changes), [realization()])

    def test_strategy_estimate_cannot_override_broker_pnl(self):
        rec = complete(position(strategy_context={"dollar_pnl": 1000, "conviction": 3}), [realization()])
        self.assertEqual(completed_history_view(rec)["dollar_pnl"], -.16)

    def test_unknown_exposure_is_not_invented(self):
        rec = complete(position(notional_per_quantity=None), [realization()])
        self.assertIsNone(rec["original_notional"])
        self.assertIsNone(completed_history_view(rec)["pnl_pct"])

    def test_json_round_trip_and_inputs_unchanged(self):
        pos, rows = position(), [realization()]
        original = deepcopy((pos, rows))
        rec = complete(pos, rows)
        self.assertEqual(json.loads(json.dumps(rec)), rec)
        self.assertEqual((pos, rows), original)
        rec["strategy_context"]["conviction"] = 10
        self.assertEqual(pos["strategy_context"]["conviction"], 3)


if __name__ == "__main__":
    unittest.main()
