"""Ensure partial stop synchronization mutates persisted position, not its copy."""
import unittest
from test_live_accounting import close_harness


class PartialStopPersistenceTests(unittest.TestCase):
    def test_acknowledged_stop_saved_on_actual_position(self):
        ns = close_harness()
        ns['_live_confirm_deal'].side_effect = [
            {'dealStatus': 'ACCEPTED', 'level': 102.},
            {'dealStatus': 'ACCEPTED', 'dealReference': 'fixture-stop',
             'dealId': 'fixture-deal', 'stopLevel': 99.9}]
        ns['_live_partial_tp_exit']({'GOLD': {'price': 102.}})
        pos = ns['_live']['open_position']
        self.assertEqual(pos['acknowledged_stop_level'], 99.9)
        self.assertEqual(pos['stop_sync']['status'], 'acknowledged')

    def test_failed_stop_pending_and_software_floor_saved(self):
        ns = close_harness()
        ns['_ig_live_put'].return_value = None
        ns['_live_partial_tp_exit']({'GOLD': {'price': 102.}})
        pos = ns['_live']['open_position']
        self.assertEqual(pos['stop_sync']['status'], 'pending')
        self.assertEqual(pos['defensive_soft_sl'], 99.9)
        self.assertNotIn('acknowledged_stop_level', pos)
