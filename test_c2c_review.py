"""Adversarial C2c review; injected data and fake Redis only."""
import unittest
from copy import deepcopy
import fakeredis
from broker_ledger import EvidenceError
from broker_match import match_realizations, collect_lifecycle_realizations
from broker_transaction import normalize_batch
from broker_pending import PendingCloseStore
from broker_reconcile import reconcile_position
from test_broker_ledger import position, realization
from test_broker_pending import register, history


def normalized(rows):
    return normalize_batch(dict(account_id='fixture-account', history_complete=True, transactions=rows))


class ReviewMatchingTests(unittest.TestCase):
    def test_different_opening_utc_is_not_a_match_despite_identical_price_size(self):
        rows = normalized([realization(openDateUtc='2026-09-21T05:46:56')])
        self.assertEqual(match_realizations(position(), rows)['confidence'], 'NO_MATCH')
        self.assertEqual(collect_lifecycle_realizations(position(), rows)['realizations'], [])

    def test_missing_opening_utc_cannot_reach_high_or_exact_confidence(self):
        for stamp in (None, 'UNKNOWN', ''):
            result = match_realizations(position(opened_utc=stamp), normalized([realization()]))
            self.assertEqual(result['confidence'], 'AMBIGUOUS')
            lifecycle = collect_lifecycle_realizations(position(opened_utc=stamp), normalized([realization()]))
            self.assertFalse(lifecycle['quantity_accounted'])
            self.assertEqual(lifecycle['realizations'], [])

    def test_partial_from_another_opening_cannot_fill_missing_residual(self):
        rows = normalized([realization('a', '-0.08', '1'),
            realization('b', '-0.08', '2', openDateUtc='2026-09-21T05:46:56')])
        result = collect_lifecycle_realizations(position(), rows)
        self.assertEqual(result['lifecycle_state'], 'partial_close')
        self.assertEqual(len(result['realizations']), 1)

    def test_duplicate_partial_does_not_double_quantity(self):
        row = realization('a', '-0.08', '1')
        result = collect_lifecycle_realizations(position(), normalized([row, deepcopy(row)]))
        self.assertEqual(result['total_closed_quantity'], '0.08')

    def test_pipeline_rejects_identity_different_from_registered_opening(self):
        store = PendingCloseStore(fakeredis.FakeRedis(), 'fixture-account')
        pos = register(store)
        for changes in ({'deal_id': 'other'}, {'entry_price': '999'},
                        {'opened_utc': '2026-09-21T06:00:00'}, {'original_quantity': '99'}):
            with self.subTest(changes=changes), self.assertRaises(EvidenceError):
                reconcile_position(store, pos['deal_id'], history(), {**pos, **changes})

    def test_pipeline_rejects_unrelated_opening_commission_reference(self):
        store = PendingCloseStore(fakeredis.FakeRedis(), 'fixture-account')
        pos = register(store)
        with self.assertRaises(EvidenceError):
            reconcile_position(store, pos['deal_id'], history(), pos, entry_reference='unrelated')


class ReviewParsingTests(unittest.TestCase):
    def test_ledger_and_normalizer_agree_on_money_signs(self):
        from broker_ledger import reconcile_completed_trade
        for cash, expected in [("$-0.16", "-0.16"), ("-$0.16", "-0.16"),
                               ("-$9", "-9"), (" $0.17 ", "0.17"),
                               ("1,234.50", "1234.5"), ("-0", "0"), ("+0", "0")]:
            with self.subTest(cash=cash):
                row = realization(profitAndLoss=cash)
                record = reconcile_completed_trade(position(), [row])
                self.assertEqual(record['gross_realized_pnl'], expected)
                self.assertEqual(float(normalized([row])[0]['cash_amount']), float(expected))

    def test_malformed_cash_never_becomes_number(self):
        from broker_ledger import reconcile_completed_trade
        for cash in ('1,2', '1,,234', '(9)', '-$-9', 'NaN', '', '$'):
            with self.subTest(cash=cash):
                row = realization(profitAndLoss=cash)
                self.assertEqual(normalized([row])[0]['cash_amount'], 'UNKNOWN')
                with self.assertRaises(EvidenceError):
                    reconcile_completed_trade(position(), [row])

    def test_date_only_opening_is_not_exact_broker_time(self):
        from broker_ledger import reconcile_completed_trade
        row = realization(openDateUtc='2026-09-21')
        pos = position(opened_utc='2026-09-21')
        self.assertNotEqual(match_realizations(pos, normalized([row]))['confidence'], 'EXACT')
        with self.assertRaises(EvidenceError):
            reconcile_completed_trade(pos, [row])
