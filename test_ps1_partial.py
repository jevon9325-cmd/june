"""Partial-close failure injection against extracted production functions."""
import unittest
from unittest.mock import Mock
from test_live_accounting import close_harness


class PartialResidualTests(unittest.TestCase):
    def test_unavailable_or_malformed_inventory_never_forgets_residual(self):
        for evidence in (None, {}, {'positions': None}, {'positions': [{}]}):
            ns = close_harness()
            ns['_ig_live_get'].return_value = evidence
            ns['_live_partial_tp_exit']({'GOLD': {'price': 102}})
            self.assertIsNotNone(ns['_live']['open_position'])
            self.assertEqual(ns['_live']['open_position']['ig_size'], 10)
            self.assertTrue(ns['_live']['open_position']['partial_exit_pending'])

    def test_lost_response_or_confirmation_does_not_send_a_full_close(self):
        for response, confirmation in ((None, None), ({'dealReference': 'ref'}, None)):
            ns = close_harness()
            ns['_ig_live_post'].return_value = response
            ns['_live_confirm_deal'].return_value = confirmation
            ns['_live_close_position'] = Mock()
            ns['_live_partial_tp_exit']({'GOLD': {'price': 102}})
            ns['_live_partial_tp_exit']({'GOLD': {'price': 102}})
            ns['_live_close_position'].assert_not_called()
            ns['_ig_live_post'].assert_called_once()
            self.assertEqual(ns['_live']['open_position']['ig_size'], 10)

    def test_rejected_partial_preserves_quantity_without_full_fallback(self):
        ns = close_harness()
        ns['_live_confirm_deal'].return_value = {'dealStatus': 'REJECTED'}
        ns['_live_close_position'] = Mock()
        ns['_live_partial_tp_exit']({'GOLD': {'price': 102}})
        ns['_live_close_position'].assert_not_called()
        self.assertEqual(ns['_live']['open_position']['ig_size'], 10)
        self.assertFalse(ns['_live']['open_position'].get('partial_exit_pending'))

    def test_other_positions_do_not_hide_confirmed_residual(self):
        ns = close_harness()
        ns['_ig_live_get'].return_value = {'positions': [
            {'position': {'dealId': 'other', 'size': 3}},
            {'position': {'dealId': 'fixture-deal', 'size': 5}}]}
        ns['_live_partial_tp_exit']({'GOLD': {'price': 102}})
        self.assertEqual(ns['_live']['open_position']['ig_size'], 5)
        self.assertTrue(ns['_live']['open_position']['partial_exit_done'])

    def test_unchanged_broker_quantity_is_not_a_confirmed_partial(self):
        ns = close_harness()
        ns['_ig_live_get'].return_value = {'positions': [
            {'position': {'dealId': 'fixture-deal', 'size': 10}}]}
        ns['_live_partial_tp_exit']({'GOLD': {'price': 102}})
        self.assertEqual(ns['_live']['open_position']['ig_size'], 10)
        self.assertTrue(ns['_live']['open_position']['partial_exit_pending'])

    def test_valid_inventory_absence_is_distinct_from_unavailable(self):
        ns = close_harness()
        ns['_ig_live_get'].return_value = {'positions': []}
        ns['_live_partial_tp_exit']({'GOLD': {'price': 102}})
        self.assertIsNone(ns['_live']['open_position'])
