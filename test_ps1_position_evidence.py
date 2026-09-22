"""Account margin is never proof of an individual deal's lifecycle."""
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from test_broker_identity import function, execute
from test_live_accounting import close_harness


class PositionEvidenceTests(unittest.TestCase):
    def guard(self, data, connected=True, closed=False):
        ns = dict(_ls_connected=connected, _ls_deal_closed=Mock(return_value=closed),
                  _ls_get_margin=Mock(return_value=0), _guard_consecutive_unavail={},
                  _GUARD_UNAVAIL_ESCALATION_THRESHOLD=3, _live_log=Mock(),
                  _ig_live_get=Mock(return_value=data))
        execute([function('_ls_position_guard_check')], ns)
        return ns

    def test_zero_margin_cannot_override_specific_position_presence(self):
        ns = self.guard({'positions': [{'position': {'dealId': 'deal', 'size': 1}}]})
        self.assertTrue(ns['_ls_position_guard_check']('GOLD', 'deal')[0])
        ns['_ls_get_margin'].assert_not_called()

    def test_unavailable_inventory_and_zero_margin_are_unknown_not_flat(self):
        for data in (None, {}, {'positions': None}, {'positions': [{}]}):
            ns = self.guard(data, connected=False)
            self.assertIsNone(ns['_ls_position_guard_check']('GOLD', 'deal')[0])

    def test_other_deal_does_not_prove_this_deal_open(self):
        ns = self.guard({'positions': [{'position': {'dealId': 'other'}}]})
        self.assertFalse(ns['_ls_position_guard_check']('GOLD', 'deal')[0])

    def test_matched_stream_closure_is_specific_proof(self):
        ns = self.guard(None, closed=True)
        self.assertFalse(ns['_ls_position_guard_check']('GOLD', 'deal')[0])

    def test_zero_cached_margin_does_not_skip_max_hold_exit(self):
        ns = dict(_live={'open_position': {'instrument': 'GOLD', 'direction': 'long',
                    'deal_id': 'deal', 'entry_time': 0}},
                  time=SimpleNamespace(time=lambda: 1000), _SIM_MAX_HOLD_SECS=100,
                  _ls_deal_closed=Mock(return_value=False), _ls_connected=True,
                  _ls_get_margin=Mock(return_value=0), _live_reconcile_positions=Mock(),
                  _live_save_state=Mock(), _live_log=Mock(), _live_close_position=Mock())
        execute([function('_live_check_exit')], ns)
        ns['_live_check_exit']({}, 'fixture')
        ns['_live_close_position'].assert_called_once_with('max_hold', {})

    def test_failed_recovery_inventory_cannot_clear_tracking(self):
        ns = close_harness()
        ns['_ig_live_get'].side_effect = lambda path, **kw: (
            {'accounts': [{'preferred': True, 'balance': {'deposit': 0}}]}
            if path == '/accounts' else None)
        execute([function('_live_reconcile_positions')], ns)
        ns['_live_reconcile_positions']()
        self.assertIsNotNone(ns['_live']['open_position'])

    def test_manual_review_and_unavailable_inventory_do_not_veto_protective_delete(self):
        ns = close_harness()
        ns['_live']['manual_review_required'] = True
        ns['_ls_position_guard_check'].return_value = (None, 'unavailable')
        ns['_ig_live_get'].return_value = None
        ns['_live_close_position']('stop_loss', {'GOLD': {'price': 99}})
        ns['_ig_live_post'].assert_called_once()
        self.assertTrue(ns['_ig_live_post'].call_args.kwargs['close'])
        self.assertIsNotNone(ns['_live']['open_position'])

    def test_addon_acceptance_without_disappearance_preserves_tracking(self):
        ns = close_harness()
        leg = dict(ns['_live']['open_position'])
        ns['_live']['pyramid_legs'] = [leg]
        execute([function('_live_close_addon_leg')], ns)
        ns['_live_close_addon_leg'](leg, 'pyramid_agg_stop', {'GOLD': {'price': 99}})
        self.assertEqual(len(ns['_live']['pyramid_legs']), 1)

    def test_accepted_full_close_with_unavailable_or_present_inventory_is_not_completed(self):
        for data in (None, {}, {'positions': [{}]},
                     {'positions': [{'position': {'dealId': 'fixture-deal'}}]}):
            ns = close_harness()
            ns['_ig_live_get'].return_value = data
            ns['_live_close_position']('stop_loss', {'GOLD': {'price': 99}})
            self.assertIsNotNone(ns['_live']['open_position'])
            ns['_live_perf_record'].assert_not_called()
            self.assertFalse(ns['_live'].get('trade_history'))

    def test_recovery_refreshes_ambiguous_partial_size_without_claiming_economics(self):
        ns = close_harness()
        ns['_live']['open_position']['partial_exit_pending'] = {'status': 'unknown'}
        execute([function('_live_reconcile_positions')], ns)
        ns['_live_reconcile_positions']()
        pos = ns['_live']['open_position']
        self.assertEqual((pos['ig_size'], pos['notional']), (5, 500))
        self.assertEqual(pos['original_ig_size'], 10)
        self.assertTrue(pos['partial_exit_pending'])
        self.assertNotIn('partial_dollar_pnl', pos)

    def test_full_close_uses_refreshed_partial_residual_size(self):
        ns = close_harness()
        ns['_live']['open_position']['partial_exit_pending'] = {'status': 'unknown'}
        execute([function('_live_reconcile_positions')], ns)
        ns['_ig_live_get'].side_effect = [
            {'positions': [{'position': {'dealId': 'fixture-deal', 'size': 5}}]},
            {'positions': []}]
        ns['_live_close_position']('stop_loss', {'GOLD': {'price': 99}})
        self.assertEqual(ns['_ig_live_post'].call_args.args[1]['size'], 5)
        self.assertIsNone(ns['_live']['open_position'])
