"""Recovery provenance and SOYBEANS native/cache-unit consistency."""
from datetime import datetime, timezone
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from test_broker_identity import function, execute, TREE
from test_live_accounting import close_harness


def inventory(deal='recovered', direction='BUY', created=None):
    pos = dict(dealId=deal, dealReference='ref-' + deal, size=1, level=100)
    if direction is not None:
        pos['direction'] = direction
    if created is not None:
        pos['createdDateUTC'] = created
    return {'position': pos, 'market': {'epic': 'FIXTURE.EPIC'}}


class RecoveryProvenanceTests(unittest.TestCase):
    def runtime(self, rows, existing=False):
        ns = close_harness()
        if not existing:
            ns['_live']['open_position'] = None
        ns.update(datetime=datetime, timezone=timezone, _PYRAMID_MAX_LEGS=2)
        ns['_ig_live_get'].return_value = {'positions': rows}
        names = ['_live_reconcile_positions']
        if any(getattr(n, 'name', None) == '_live_recovery_metadata' for n in TREE.body):
            names.append('_live_recovery_metadata')
        execute([function(n) for n in names], ns)
        return ns

    def test_management_roles_do_not_claim_historical_origin(self):
        ns = self.runtime([inventory('one'), inventory('two')])
        ns['_live_reconcile_positions']()
        for pos, role in ((ns['_live']['open_position'], 'primary'),
                          (ns['_live']['pyramid_legs'][0], 'addon')):
            self.assertEqual(pos['historical_origin'], 'unknown')
            self.assertEqual(pos['management_role'], role)
            self.assertEqual(pos['entry_time_source'], 'recovery_observation')
            self.assertIsNone(pos['broker_opened_utc'])
            self.assertEqual(pos['direction'], 'long')

    def test_missing_direction_is_not_invented_as_buy(self):
        ns = self.runtime([inventory(direction=None)])
        ns['_live_reconcile_positions']()
        self.assertIsNone(ns['_live']['open_position'])
        self.assertTrue(ns['_live']['orphan_suspected'])
        self.assertEqual(ns['_live']['recovery_unresolved_positions'][0]['position']['dealId'], 'recovered')

    def test_existing_primary_extra_deal_has_unknown_origin(self):
        ns = self.runtime([inventory('fixture-deal'), inventory('extra')], existing=True)
        ns['_live_reconcile_positions']()
        self.assertEqual(ns['_live']['pyramid_legs'][0]['historical_origin'], 'unknown')
        self.assertEqual(ns['_live']['open_position']['deal_id'], 'fixture-deal')

    def test_broker_utc_and_fraction_are_preserved(self):
        date = '2026-09-22T10:15:30.123Z'
        ns = self.runtime([inventory(created=date)])
        ns['_live_reconcile_positions']()
        pos = ns['_live']['open_position']
        self.assertEqual(pos['broker_opened_utc'], '2026-09-22T10:15:30.123000Z')
        self.assertEqual(pos['entry_time_source'], 'broker_createdDateUTC')
        self.assertAlmostEqual(pos['entry_time'], datetime.fromisoformat(date).timestamp())

    def test_date_only_does_not_invent_an_opening_time_at_midnight(self):
        ns = self.runtime([inventory(created='2026-09-22')])
        ns['_live_reconcile_positions']()
        pos = ns['_live']['open_position']
        self.assertIsNone(pos['broker_opened_utc'])
        self.assertEqual(pos['entry_time_source'], 'recovery_observation')

    def test_opposite_direction_extra_is_not_claimed_as_a_pyramid_leg(self):
        ns = self.runtime([inventory('fixture-deal'), inventory('other', direction='SELL')], existing=True)
        ns['_live_reconcile_positions']()
        self.assertFalse(ns['_live'].get('pyramid_legs'))
        self.assertTrue(ns['_live']['recovery_unresolved_positions'])
        self.assertEqual(ns['_live']['open_position']['deal_id'], 'fixture-deal')

    def test_invalid_extra_does_not_prevent_management_of_valid_unknown_origin(self):
        ns = self.runtime([inventory('valid'), inventory('invalid', direction=None)])
        ns['_live_reconcile_positions']()
        self.assertEqual(ns['_live']['open_position']['deal_id'], 'valid')
        self.assertTrue(ns['_live']['recovery_unresolved_positions'])

    def test_known_addon_recovered_as_primary_keeps_opening_evidence_without_duplicate(self):
        ns = self.runtime([inventory('known')])
        proof = {'role': 'add_on', 'deal_id': 'known'}
        ns['_live']['pyramid_legs'] = [dict(instrument='GOLD', direction='long',
            deal_id='known', fill_price=100, ig_size=1, notional=100,
            broker_entry_evidence=proof)]
        ns['_live_reconcile_positions']()
        self.assertEqual(ns['_live']['open_position']['broker_entry_evidence'], proof)
        self.assertEqual(ns['_live']['open_position']['management_role'], 'primary')
        self.assertFalse(ns['_live']['pyramid_legs'])


class SoybeansUnitsTests(unittest.TestCase):
    def runtime(self):
        ns = dict(_sim_min_notional={'SOYBEANS': 53.644},
                  _live_lot_sizes={'SOYBEANS': 1.}, _live_min_deal={'SOYBEANS': .04},
                  _live_price_unit={'SOYBEANS': .01}, _LIVE_LOT_SIZE_FX=100000.,
                  _LIVE_LOT_NOTIONAL_OVERRIDES={'SOYBEANS': 100.},
                  _live_equity_cfd=set(), _live_fx_instruments=set())
        execute([function('_live_entry_price_ref'), function('_live_compute_ig_size')], ns)
        return ns

    def test_corrected_cache_inverts_to_native_price(self):
        ns = self.runtime()
        self.assertAlmostEqual(ns['_live_entry_price_ref']('SOYBEANS'), 1341.1)

    def test_non_override_instrument_keeps_existing_units(self):
        ns = self.runtime()
        ns['_sim_min_notional']['GOLD'] = 100
        ns['_live_lot_sizes']['GOLD'] = 1
        ns['_live_min_deal']['GOLD'] = .1
        self.assertEqual(ns['_live_entry_price_ref']('GOLD'), 1000)

    def test_order_size_is_native_and_override_does_not_change_it(self):
        ns = self.runtime()
        # Existing sizing floors may exceed the broker minimum; verify the unit,
        # not a new allocation rule.
        ns.update(_live_log=Mock(), _live_min_lot_floor={}, _LIVE_MIN_LOT_FLOOR={})
        size = ns['_live_compute_ig_size']('SOYBEANS', 134.11, 1341.1)
        self.assertAlmostEqual(size, .1)
        ns['_LIVE_LOT_NOTIONAL_OVERRIDES']['SOYBEANS'] = 999
        self.assertEqual(ns['_live_compute_ig_size']('SOYBEANS', 134.11, 1341.1), size)

    def test_eligibility_uses_same_native_minimum_exposure(self):
        ns = self.runtime()
        redis = Mock()
        ns.update(time=SimpleNamespace(time=lambda: 1000), _live_elig_publish_next=0,
            _live={'balance_total': 183.07}, _IG_EQUITY_COMMISSION_USD=9.,
            INSTRUMENTS={'SOYBEANS': 'CC.D.S.BMU.IP'}, _live_margin={'SOYBEANS': .02},
            _real_margin_fraction=lambda *_: .02, _redis=lambda: redis,
            json=json, _live_log=Mock())
        execute([function('_live_publish_eligible_instruments')], ns)
        ns['_live_publish_eligible_instruments']({'SOYBEANS': {'price': 1341.1}})
        row = json.loads(redis.set.call_args.args[1])['instruments']['SOYBEANS']
        self.assertAlmostEqual(row['min_notional'], .04 * 1341.1)
        self.assertAlmostEqual(row['min_margin'], 1.0729)

    def test_broker_metadata_corrects_legacy_sub_dollar_cache(self):
        ns = self.runtime()
        ns['_sim_min_notional']['SOYBEANS'] = .53644  # former cent-conversion error
        ns.update(_live_broker_market_evidence={}, _live_pip_sizes={}, _live_margin={},
            _live_min_stop_pts={}, _live_min_stop_pct={}, _live_ccy={}, _live_fx_base={},
            _sim_eligible=set(), _SPREAD_ATR_ASSET_CLASS={}, _live_log=Mock(),
            _live_parse_pip_size=lambda _: .01, _redis=Mock(), json=json,
            _NOTIONAL_REDIS_KEY='fixture', _NOTIONAL_REDIS_TTL=100)
        ns['_ig_live_get'] = Mock(return_value={
            'instrument': {'lotSize': 1, 'contractSize': '1', 'unit': 'CONTRACTS',
                'onePipMeans': '1 cents per bushel', 'valueOfOnePip': '1.00',
                'marginFactor': 2, 'marginFactorUnit': 'PERCENTAGE',
                'currencies': [{'name': 'USD', 'baseExchangeRate': 1}]},
            'dealingRules': {'minDealSize': {'value': .04},
                'minNormalStopOrLimitDistance': {'value': 4, 'unit': 'POINTS'}},
            'snapshot': {'bid': 1340.5, 'offer': 1341.7}})
        execute([function('_live_fetch_market_data')], ns)
        self.assertTrue(ns['_live_fetch_market_data']('SOYBEANS', 'CC.D.S.BMU.IP'))
        self.assertAlmostEqual(ns['_sim_min_notional']['SOYBEANS'], 53.644)
        self.assertAlmostEqual(ns['_live_entry_price_ref']('SOYBEANS'), 1341.1)

    def test_cached_stop_fallback_agrees_with_explicit_native_price(self):
        ns = self.runtime()
        ns.update(_live_pip_sizes={'SOYBEANS': .01}, _LIVE_FX_PIP=.0001,
                  _live_min_stop_pts={'SOYBEANS': 4}, _live_min_stop_pct={})
        execute([function('_live_compute_stop_pts')], ns)
        self.assertEqual(ns['_live_compute_stop_pts']('SOYBEANS', .01),
                         ns['_live_compute_stop_pts']('SOYBEANS', .01, 1341.1))

    def test_confirmed_fill_pnl_uses_native_dollar_per_point(self):
        ns = close_harness(symbol='SOYBEANS')
        ns['_live']['open_position'].update(ig_size=.04, fill_price=1341.1, notional=53.644)
        ns['_live_price_unit']['SOYBEANS'] = .01
        ns['_ig_live_get'].return_value = {'positions': []}
        ns['_live_confirm_deal'].return_value = {'dealStatus': 'ACCEPTED', 'level': 1342.1}
        ns['_live_close_position']('take_profit', {'SOYBEANS': {'price': 1342.1}})
        self.assertEqual(ns['_live']['trade_history'][0]['dollar_pnl'], .04)
