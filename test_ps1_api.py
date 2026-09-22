"""Offline IG wire-contract regression tests; never import the trading service."""
import ast
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from test_broker_identity import runtime, function


class PositionContractTests(unittest.TestCase):
    def test_close_override_survives_auth_retry(self):
        ns = runtime()
        ns['authenticate_live'] = Mock(return_value=True)
        ns['requests'].post.side_effect = [SimpleNamespace(status_code=401),
            SimpleNamespace(status_code=200, json=lambda: {'dealReference': 'close-ref'})]
        body = dict(dealId='existing-deal', direction='SELL', size=2,
                    orderType='MARKET', timeInForce='FILL_OR_KILL')
        ns['_ig_live_post']('/positions/otc', body, version='1', close=True)
        self.assertEqual(ns['requests'].post.call_count, 2)
        for call in ns['requests'].post.call_args_list:
            self.assertEqual(call.kwargs['headers']['_method'], 'DELETE')
            self.assertEqual(call.kwargs['headers']['Version'], '1')
            self.assertEqual(call.kwargs['json'], body)

    def test_opening_post_is_unchanged(self):
        ns = runtime()
        ns['requests'].post.return_value = SimpleNamespace(status_code=200, json=lambda: {})
        ns['_ig_live_post']('/positions/otc', {'epic': 'fixture', 'forceOpen': True})
        self.assertNotIn('_method', ns['requests'].post.call_args.kwargs['headers'])

    def test_invalid_close_cannot_fall_back_to_an_opening_order(self):
        ns = runtime()
        base = dict(dealId='deal', direction='SELL', size=2,
                    orderType='MARKET', timeInForce='FILL_OR_KILL')
        for change in ({'dealId': ''}, {'size': 0}, {'size': float('nan')},
                       {'size': float('inf')}, {'direction': 'unknown'}, {'epic': 'extra'}):
            self.assertIsNone(ns['_ig_live_post']('/positions/otc', {**base, **change}, version='1', close=True))
        ns['requests'].post.assert_not_called()

    def test_every_close_helper_uses_only_deal_specific_close_fields(self):
        for name in ('_live_close_position', '_live_partial_tp_exit', '_live_close_addon_leg'):
            node = function(name)
            body = next(n.value for n in ast.walk(node) if isinstance(n, ast.Assign)
                        and any(isinstance(t, ast.Name) and t.id == 'close_body' for t in n.targets))
            self.assertEqual({k.value for k in body.keys},
                {'dealId', 'direction', 'size', 'orderType', 'timeInForce'}, name)
            call = next(n for n in ast.walk(node) if isinstance(n, ast.Call)
                        and isinstance(n.func, ast.Name) and n.func.id == '_ig_live_post')
            self.assertTrue(any(k.arg == 'close' and k.value.value is True for k in call.keywords), name)

    def test_inventory_calls_use_documented_v2_contract(self):
        for name in ('_live_close_position', '_live_partial_tp_exit', '_live_reconcile_positions'):
            calls = [n for n in ast.walk(function(name)) if isinstance(n, ast.Call)
                     and isinstance(n.func, ast.Name) and n.func.id == '_ig_live_get'
                     and n.args and isinstance(n.args[0], ast.Constant)
                     and str(n.args[0].value).startswith('/positions')]
            self.assertTrue(calls, name)
            for call in calls:
                self.assertEqual(call.args[0].value, '/positions')
                self.assertTrue(any(k.arg == 'version' and k.value.value == '2' for k in call.keywords))
