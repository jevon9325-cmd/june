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
from broker_cost import build_cost_record
from test_broker_cost import comm_row


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


class ReviewCostIdentityTests(unittest.TestCase):
    def cost(self, deal='opening-1', **changes):
        row = {**comm_row(), 'dateUtc': '2026-09-21T07:00:00',
               'instrumentName': 'Spot Gold ($1)', **changes}
        return build_cost_record(normalized([row])[0], 'fixture-account', deal)

    def test_cost_identity_does_not_depend_on_claimant(self):
        self.assertEqual(self.cost()['cost_id'], self.cost('another')['cost_id'])

    def test_distinct_postings_and_instruments_do_not_collapse(self):
        costs = [self.cost(), self.cost(dateUtc='2026-09-21T07:00:01'),
                 self.cost(instrumentName='Silver')]
        self.assertEqual(len({c['cost_id'] for c in costs}), 3)

    def test_duplicate_delivery_and_overlapping_window_after_restart(self):
        client = fakeredis.FakeRedis()
        store = PendingCloseStore(client, 'fixture-account')
        pos = register(store)
        cost = self.cost()
        first = store.reconcile(pos['deal_id'], history(), [cost, deepcopy(cost)])
        batch = history()
        batch['to'] = '2026-09-23T00:00:00'
        again = PendingCloseStore(client, 'fixture-account').reconcile(pos['deal_id'], batch, [cost])
        self.assertEqual(first, again)
        self.assertEqual(again['commissions'], '-9')

    def test_cost_claim_is_atomic_across_positions_and_restart(self):
        client = fakeredis.FakeRedis()
        store = PendingCloseStore(client, 'fixture-account')
        a = register(store)
        b = register(store, position(deal_id='opening-2', entry_price='999'))
        store.reconcile(a['deal_id'], history(), [self.cost()])
        before = client.hgetall(store.key)
        with self.assertRaisesRegex(EvidenceError, 'Cost already attributed'):
            PendingCloseStore(client, 'fixture-account').reconcile(
                b['deal_id'], history([realization(openLevel='999')]), [self.cost(b['deal_id'])])
        self.assertEqual(client.hgetall(store.key), before)

    def test_distinct_similar_costs_both_contribute(self):
        store = PendingCloseStore(fakeredis.FakeRedis(), 'fixture-account')
        pos = register(store)
        record = store.reconcile(pos['deal_id'], history(),
            [self.cost(), self.cost(dateUtc='2026-09-21T07:00:01')])
        self.assertEqual(record['commissions'], '-18')
        self.assertEqual(len(record['costs']), 2)

    def test_cost_claim_survives_lost_acknowledgement(self):
        from test_broker_pending import FaultClient
        from redis.exceptions import ConnectionError
        client = fakeredis.FakeRedis()
        store = PendingCloseStore(client, 'fixture-account')
        pos = register(store)
        faulty = PendingCloseStore(FaultClient(client, 'after'), 'fixture-account')
        with self.assertRaises(ConnectionError):
            faulty.reconcile(pos['deal_id'], history(), [self.cost()])
        record = store.reconcile(pos['deal_id'], history(), [self.cost()])
        self.assertEqual(record['commissions'], '-9')
        self.assertEqual(len(record['costs']), 1)

    def test_legacy_cost_record_requires_review_without_migration(self):
        import json
        client = fakeredis.FakeRedis()
        store = PendingCloseStore(client, 'fixture-account')
        pos = register(store)
        store.reconcile(pos['deal_id'], history(), [self.cost()])
        entry = store.get_entry(pos['deal_id'])
        entry.pop('cost_claim_version')
        client.hset(store.key, store._field(pos['deal_id']), json.dumps(entry))
        before = client.hgetall(store.key)
        with self.assertRaisesRegex(EvidenceError, 'Legacy cost'):
            store.reconcile(pos['deal_id'], history(), [self.cost()])
        self.assertEqual(before, client.hgetall(store.key))

    def test_competing_claim_retries_watch_and_refuses_loser(self):
        from test_broker_pending import FaultClient
        client = fakeredis.FakeRedis()
        store = PendingCloseStore(client, 'fixture-account')
        a = register(store)
        b = register(store, position(deal_id='opening-2', entry_price='999'))
        def win():
            store.reconcile(a['deal_id'], history(), [self.cost()])
        racing = PendingCloseStore(FaultClient(client, 'race', win), 'fixture-account')
        with self.assertRaisesRegex(EvidenceError, 'Cost already attributed'):
            racing.reconcile(b['deal_id'], history([realization(openLevel='999')]),
                             [self.cost(b['deal_id'])])
        self.assertIsNone(store.get_entry(b['deal_id'])['record'])


class ReviewCostAttributionTests(unittest.TestCase):
    def test_shared_gold_close_reference_is_unattributed_for_both_positions(self):
        from broker_cost import attribute_costs
        a = position()
        b = position(deal_id='second', entry_price='999')
        rows = normalized([realization('shared'), realization('shared', openLevel='999'),
                           comm_row('shared')])
        for pos in (a, b):
            result = attribute_costs(pos, rows, {'shared'}, 'entry-reference')
            self.assertEqual(result['position_costs'], [])
            self.assertEqual(len(result['unattributed_costs']), 1)
            self.assertFalse(result['cost_complete'])

    def test_shared_entry_reference_is_not_awarded_to_first_claimant(self):
        store = PendingCloseStore(fakeredis.FakeRedis(), 'fixture-account')
        a = register(store)
        b = register(store, position(deal_id='second', entry_price='999'))
        for pos, row in ((a, realization()), (b, realization(openLevel='999'))):
            with self.assertRaisesRegex(EvidenceError, 'Ambiguous commission'):
                reconcile_position(store, pos['deal_id'], history([row, comm_row('entry-reference')]),
                                   pos, entry_reference='entry-reference')
            self.assertIsNone(store.get_entry(pos['deal_id'])['record'])


class ReviewCostFinalityTests(unittest.TestCase):
    def setUp(self):
        from test_broker_pending import COST_EVIDENCE
        self.client = fakeredis.FakeRedis()
        self.store = PendingCloseStore(self.client, 'fixture-account')
        self.pos = register(self.store)
        self.evidence = deepcopy(COST_EVIDENCE)

    def reconcile(self, rows=None, evidence=None, batch=None):
        return reconcile_position(self.store, self.pos['deal_id'],
            batch or history([realization()] if rows is None else rows), self.pos,
            entry_reference='entry-reference', cost_evidence=evidence)[0]

    def assert_pending(self, record):
        self.assertEqual(record['status'], 'pending_costs')
        self.assertIsNone(record['net_realized_pnl'])
        self.assertTrue(record['provenance']['history_fetch_complete'])
        self.assertFalse(record['provenance']['economic_evidence_complete'])

    def test_legacy_coverage_through_close_is_not_finality(self):
        old = {k: self.evidence[k] for k in ('source', 'reference', 'covered_through')}
        self.assert_pending(self.reconcile(evidence=old))

    def test_no_commission_visible_with_positive_expected_cost_stays_pending(self):
        self.evidence['components']['commission']['total'] = '-9'
        self.assert_pending(self.reconcile(evidence=self.evidence))

    def test_each_cost_component_and_posting_horizon_must_be_final(self):
        for component in ('commission', 'financing', 'other'):
            evidence = deepcopy(self.evidence)
            evidence['components'][component]['final'] = False
            self.assert_pending(self.reconcile(evidence=evidence))
        evidence = deepcopy(self.evidence)
        evidence['posting_finality']['final'] = False
        self.assert_pending(self.reconcile(evidence=evidence))

    def test_immature_history_window_cannot_cover_later_posting_horizon(self):
        self.evidence['posting_finality']['posted_through'] = '2026-09-23T00:00:00'
        self.assert_pending(self.reconcile(evidence=self.evidence))

    def test_zero_cost_requires_explicit_final_zero_components(self):
        self.assert_pending(self.reconcile())
        record = self.reconcile(evidence=self.evidence)
        self.assertEqual(record['status'], 'complete')
        self.assertEqual(record['net_realized_pnl'], '-0.16')

    def test_delayed_commission_after_restart_completes_only_at_final_total(self):
        self.evidence['components']['commission']['total'] = '-18'
        self.assert_pending(self.reconcile(evidence=self.evidence))
        self.store = PendingCloseStore(self.client, 'fixture-account')
        rows = [realization(), comm_row('entry-reference')]
        self.assert_pending(self.reconcile(rows, self.evidence))
        rows.append(comm_row('close-1'))
        record = self.reconcile(rows, self.evidence)
        self.assertEqual(record['status'], 'complete')
        self.assertEqual(record['net_realized_pnl'], '-18.16')

    def test_delayed_descriptive_financing_remains_unresolved_after_restart(self):
        self.evidence['components']['financing']['final'] = False
        self.assert_pending(self.reconcile(evidence=self.evidence))
        self.store = PendingCloseStore(self.client, 'fixture-account')
        self.evidence['components']['financing'].update(final=True, total='-0.50')
        rows = [realization(), {'transactionType': 'SWAP', 'currency': '$',
            'profitAndLoss': '-$0.50', 'instrumentName':
            'Daily Financing Adjustment - FX Interest for 1 day Spot Gold ($1)'}]
        self.assert_pending(self.reconcile(rows, self.evidence))

    def test_reference_less_commission_and_unknown_account_costs_block_finality(self):
        for kind in ('COMM', 'SWAP', 'INTEREST', 'MYSTERY'):
            with self.subTest(kind=kind):
                self.setUp()
                self.assert_pending(self.reconcile([realization(),
                    {'transactionType': kind, 'currency': '$', 'profitAndLoss': '-$9'}],
                    self.evidence))

    def test_direct_store_cannot_bypass_unresolved_raw_cost(self):
        record = self.store.reconcile(self.pos['deal_id'], history([realization(),
            {'transactionType': 'SWAP', 'currency': '$', 'profitAndLoss': '-$9'}]),
            cost_evidence=self.evidence)
        self.assert_pending(record)

    def test_cost_statement_must_cover_correct_position_and_entire_lifecycle(self):
        for change in ({'deal_id': 'another'}, {'account_id': 'another'},
                       {'currency': 'EUR'}, {'currency': None},
                       {'covered_from': '2026-09-21T06:00:00'},
                       {'covered_through': '2026-09-21T00:00:00'}):
            with self.subTest(change=change), self.assertRaises(EvidenceError):
                self.reconcile(evidence={**self.evidence, **change})

    def test_capital_flows_do_not_block_positive_zero_cost_evidence(self):
        rows = [realization(), {'transactionType': 'DEPO', 'profitAndLoss': '$150'},
                {'transactionType': 'WITH', 'profitAndLoss': '-$20'}]
        record = self.reconcile(rows, self.evidence)
        self.assertEqual(record['net_realized_pnl'], '-0.16')

    def test_financing_cannot_cancel_other_credit_to_invent_zero_cost(self):
        self.evidence['components']['financing']['total'] = '-1'
        self.evidence['components']['other']['total'] = '1'
        self.assert_pending(self.reconcile(evidence=self.evidence))

    def test_cost_posting_outside_history_is_refused(self):
        self.evidence['components']['commission']['total'] = '-9'
        row = {**comm_row(), 'dateUtc': '2026-09-23T00:00:00'}
        with self.assertRaisesRegex(EvidenceError, 'Cost posting outside'):
            self.reconcile([realization(), row], self.evidence)

    def test_legacy_completed_record_cannot_be_projected(self):
        import json
        self.reconcile(evidence=self.evidence)
        entry = self.store.get_entry(self.pos['deal_id'])
        entry['record']['provenance'].pop('economic_evidence_complete')
        self.client.hset(self.store.key, self.store._field(self.pos['deal_id']), json.dumps(entry))
        with self.assertRaisesRegex(EvidenceError, 'Legacy economic completion'):
            self.store.project_once(self.pos['deal_id'], 'fixture', lambda *_: {})

    def test_unresolved_cost_cannot_disappear_after_restart(self):
        for kind in ('COMM', 'SWAP', 'MYSTERY'):
            with self.subTest(kind=kind):
                self.setUp()
                row = {'transactionType': kind, 'profitAndLoss': '-$9', 'currency': '$'}
                self.assert_pending(self.reconcile([realization(), row], self.evidence))
                self.store = PendingCloseStore(self.client, 'fixture-account')
                before = self.client.hgetall(self.store.key)
                with self.assertRaisesRegex(EvidenceError, 'Previously observed cost rows'):
                    self.reconcile(evidence=self.evidence)
                self.assertEqual(before, self.client.hgetall(self.store.key))


class ReviewRealizationIdentityTests(unittest.TestCase):
    def test_same_reference_and_opening_values_across_instruments_stay_distinct(self):
        from broker_ledger import reconcile_completed_trade
        a = reconcile_completed_trade(position(), [realization()])
        b = reconcile_completed_trade(position(deal_id='silver', broker_instrument='Silver'),
                                      [realization(instrumentName='Silver')])
        self.assertNotEqual(a['realizations'][0]['realization_id'],
                            b['realizations'][0]['realization_id'])

    def test_cross_instrument_realizations_can_both_be_persisted(self):
        store = PendingCloseStore(fakeredis.FakeRedis(), 'fixture-account')
        a = register(store)
        b = register(store, position(deal_id='silver', broker_instrument='Silver'))
        rows = history([realization(), realization(instrumentName='Silver')])
        self.assertEqual(len(store.reconcile(a['deal_id'], rows)['realizations']), 1)
        self.assertEqual(len(store.reconcile(b['deal_id'], rows)['realizations']), 1)

    def test_legacy_realizations_are_quarantined_without_rekeying(self):
        import json
        from test_broker_pending import COST_EVIDENCE
        client = fakeredis.FakeRedis()
        store = PendingCloseStore(client, 'fixture-account')
        pos = register(store)
        store.reconcile(pos['deal_id'], history(), cost_evidence=COST_EVIDENCE)
        entry = store.get_entry(pos['deal_id'])
        entry['record']['provenance'].pop('realization_identity_version')
        client.hset(store.key, store._field(pos['deal_id']), json.dumps(entry))
        before = client.hgetall(store.key)
        with self.assertRaisesRegex(EvidenceError, 'Legacy realization'):
            store.reconcile(pos['deal_id'], history(), cost_evidence=COST_EVIDENCE)
        with self.assertRaisesRegex(EvidenceError, 'Legacy realization'):
            store.project_once(pos['deal_id'], 'fixture', lambda *_: {})
        self.assertEqual(before, client.hgetall(store.key))
