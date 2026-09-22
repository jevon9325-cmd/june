"""Execute actual close functions by AST extraction; no live bot import or I/O."""

import ast
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock


def close_harness(direction="long", symbol="GOLD"):
    wanted = {"_live_close_position", "_live_partial_tp_exit"}
    tree = ast.parse(Path(__file__).with_name("june.py").read_text(encoding="utf-8"))
    functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    assert {n.name for n in functions} == wanted
    pos = dict(instrument=symbol, direction=direction, ig_size=10., fill_price=100.,
               notional=1000., pos_size=20., leverage=5, deal_id="fixture-deal",
               entry_time=100., conviction=3, stop_pct=.01, tp_pct=.01)
    memory = {"open_position": pos, "balance": 30.}
    redis = Mock()
    ns = dict(_live=memory, json=json, time=SimpleNamespace(time=lambda: 1000., sleep=lambda _: None),
              _live_lot_sizes={symbol: 1.}, _LIVE_LOT_SIZE_FX=1.,
              _live_price_unit={symbol: 1.}, _live_equity_cfd=set(),
              _live_min_deal={symbol: .01}, _live_pip_sizes={symbol: .01},
              _LIVE_FX_PIP=.01, _live_min_stop_pts={symbol: 4},
              INSTRUMENTS={symbol: "FIXTURE.EPIC"}, _june_live_trading_enabled=True,
              _live_trade_guard=Mock(return_value=True),
              _ls_position_guard_check=Mock(return_value=(True, "fixture")),
              _ls_get_margin=Mock(return_value=1.), _ls_connected=False,
              _ig_live_post=Mock(return_value={"dealReference": "fixture-close"}),
              _ig_live_put=Mock(return_value={"dealReference": "fixture-stop"}),
              _ig_live_get=Mock(return_value={"positions": [{"position": {"dealId": "fixture-deal"}}]}),
              _live_confirm_deal=Mock(return_value={"dealStatus": "ACCEPTED", "level": 102.}),
              _sim_get_spread_floor=Mock(return_value=.001),
              _redis=Mock(return_value=redis), _LIVE_TRADE_HIST_KEY="fixture-history",
              _LIVE_TRADE_HIST_CAP=2000, _LIVE_TRADE_HIST_TTL=86400,
              _IG_EQUITY_COMMISSION_USD=9., _LIVE_PHASE_GATE_BAL=200.,
              _LIVE_DEF_INSTR_STOPOUTS=1, _sim_combo_key=lambda s, d: s + "_" + d)
    for name in ("_live_log", "_live_save_state", "_live_reconcile_positions",
                 "_live_update_streak", "_live_write_htf_event", "_live_perf_record",
                 "_sim_15m_record", "_live_check_phase"):
        ns[name] = Mock()
    exec(compile(ast.Module(body=functions, type_ignores=[]), "extracted_june_closes", "exec"), ns)
    return ns


class LiveAccountingTests(unittest.TestCase):
    def partial(self, ns, price):
        ns["_live_confirm_deal"].return_value = {"dealStatus": "ACCEPTED", "level": price}
        ns["_live_partial_tp_exit"]({ns["_live"]["open_position"]["instrument"]: {"price": price}})

    def residual(self, ns, price):
        ns["_live_confirm_deal"].return_value = {"dealStatus": "ACCEPTED", "level": price}
        ns["_ig_live_get"].return_value = {"positions": []}
        ns["_live_close_position"]("dple_trail", {"GOLD": {"price": price}})

    def test_partial_quantity_notional_and_policy_fields(self):
        ns = close_harness()
        self.partial(ns, 102.)
        pos = ns["_live"]["open_position"]
        self.assertEqual(pos["ig_size"], 5.)
        self.assertEqual(pos["notional"], 500.)
        self.assertEqual(pos["original_ig_size"], 10.)
        self.assertEqual(pos["original_notional"], 1000.)
        self.assertEqual(pos["partial_dollar_pnl"], 10.)
        self.assertEqual((pos["pos_size"], pos["leverage"], pos["tp_pct"]), (20., 5, .01))
        body = ns["_ig_live_post"].call_args.args[1]
        self.assertEqual((body["size"], body["direction"], body["dealId"]), (5., "SELL", "fixture-deal"))
        self.assertEqual(ns["_ig_live_put"].call_args.args[1]["stopLevel"], 99.9)
        ns["_live_perf_record"].assert_not_called()

    def test_whole_position_outcome_all_consumers(self):
        for partial, residual, expected in [(102., 99., 5.), (102., 97., -5.), (98., 103., 5.)]:
            with self.subTest(partial=partial, residual=residual):
                ns = close_harness()
                self.partial(ns, partial)
                self.residual(ns, residual)
                state = ns["_live"]
                record = state["trade_history"][0]
                self.assertEqual(record["dollar_pnl"], expected)
                self.assertEqual(record["notional"], 1000.)
                self.assertEqual(record["ig_size"], 10.)
                self.assertEqual(record["pnl_pct"], expected / 1000.)
                self.assertEqual(record["pnl_source"], "confirmed_fill_estimate")
                self.assertEqual(state["long_pnl"], expected)
                perf = ns["_live_perf_record"].call_args
                self.assertEqual(perf.kwargs["pnl_dollar"], expected)
                self.assertEqual(perf.args[1], expected > 0)
                self.assertEqual(ns["_live_update_streak"].call_args.args[2], expected > 0)
                self.assertEqual(ns["_sim_15m_record"].call_args.args[3], expected > 0)
                written = json.loads(ns["_redis"]().lpush.call_args.args[1])
                self.assertEqual(written, record)
                self.assertIsNone(state["open_position"])
                self.assertEqual(ns["_ig_live_post"].call_args.args[1]["size"], 5.)

    def test_multiple_partials_accumulate_without_losing_original_basis(self):
        ns = close_harness()
        self.partial(ns, 102.)
        self.partial(ns, 104.)
        pos = ns["_live"]["open_position"]
        self.assertEqual((pos["ig_size"], pos["notional"]), (2.5, 250.))
        self.assertEqual((pos["original_ig_size"], pos["original_notional"]), (10., 1000.))
        self.assertEqual(pos["partial_dollar_pnl"], 20.)
        self.residual(ns, 96.)
        self.assertEqual(ns["_live"]["trade_history"][0]["dollar_pnl"], 10.)

    def test_short_partial_entry_basis(self):
        ns = close_harness(direction="short")
        self.partial(ns, 98.)
        self.residual(ns, 101.)
        self.assertEqual(ns["_live"]["short_pnl"], 5.)
        self.assertEqual(ns["_live"]["trade_history"][0]["dollar_pnl"], 5.)

    def test_full_close_without_partial_unchanged(self):
        ns = close_harness()
        self.residual(ns, 102.)
        rec = ns["_live"]["trade_history"][0]
        self.assertEqual((rec["dollar_pnl"], rec["ig_size"], rec["notional"]), (20., 10., 1000.))
        self.assertEqual(rec["partial_dollar_pnl"], 0.)

    def test_disabled_guard_changes_nothing(self):
        ns = close_harness()
        original = deepcopy(ns["_live"])
        ns["_live_trade_guard"].return_value = False
        self.partial(ns, 102.)
        self.assertEqual(ns["_live"], original)
        ns["_ig_live_post"].assert_not_called()

    def test_rejected_partial_does_not_change_accounting(self):
        ns = close_harness()
        original = deepcopy(ns["_live"])
        ns["_live_close_position"] = Mock()
        ns["_live_confirm_deal"].return_value = {"dealStatus": "REJECTED"}
        ns["_live_partial_tp_exit"]({"GOLD": {"price": 102.}})
        self.assertEqual(ns["_live"], original)
        ns["_live_close_position"].assert_called_once_with("take_profit", {"GOLD": {"price": 102.}})

    def test_minimum_deal_fallback_unchanged(self):
        ns = close_harness()
        ns["_live_min_deal"]["GOLD"] = 6.
        ns["_live_close_position"] = Mock()
        self.partial(ns, 102.)
        ns["_live_close_position"].assert_called_once()
        ns["_ig_live_post"].assert_not_called()
        self.assertNotIn("original_ig_size", ns["_live"]["open_position"])


if __name__ == "__main__":
    unittest.main()
