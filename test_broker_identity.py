"""Offline identity tests and actual June function/annotation extraction."""

import ast
from copy import deepcopy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
import unittest
from unittest.mock import Mock

from broker_identity import opening_evidence, response_account_evidence


TREE = ast.parse(Path(__file__).with_name("june.py").read_text(encoding="utf-8"))


def function(name):
    return next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == name)


def execute(nodes, ns):
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "extracted_june_identity", "exec"), ns)


def session(account="fixture-account"):
    return {"account_id": account, "account_currency": "USD", "cst": "fixture-cst", "token": "fixture-token"}


def fixtures():
    ctx = response_account_evidence(session(), {"CST": "fixture-cst", "X-SECURITY-TOKEN": "fixture-token"})
    order = {"epic": "FIXTURE.GOLD", "direction": "BUY", "size": .16, "currencyCode": "USD"}
    response = {"dealReference": "entry-1", "_june_account_evidence": ctx}
    confirm = {"dealReference": "entry-1", "dealId": "deal-1", "dealStatus": "ACCEPTED",
               "status": "OPEN", "direction": "BUY", "epic": "FIXTURE.GOLD", "level": 4352.53,
               "size": .16, "date": "2026-09-21T05:45:56", "_june_account_evidence": ctx}
    return order, response, confirm


def runtime():
    ns = {"Optional": Optional, "time": SimpleNamespace(time=lambda: 1000.),
          "_live_sess": session(), "_live_api_paused_until": 0,
          "IG_LIVE_BASE": "https://fixture.invalid", "IG_LIVE_KEY": "fixture-key",
          "IG_LIVE_USER": "fixture-user", "IG_LIVE_PASS": "fixture-password",
          "_ensure_live_session": Mock(return_value=True), "_live_log": Mock(),
          "_live_capture_active": Mock(),
          "_ts": lambda: "fixture", "_ls_init_session": Mock(),
          "response_account_evidence": response_account_evidence,
          "opening_evidence": opening_evidence, "_live_broker_market_evidence": {},
          "json": json, "_LIVE_REDIS_KEY": "fixture-state", "_LIVE_REDIS_TTL": 100,
          "_live": {}, "requests": Mock()}
    for name in ("authenticate_live", "_ig_live_post", "_ig_live_get", "_live_entry_evidence",
                 "_live_save_state", "_live_load_state"):
        execute([function(name)], ns)
    return ns


class IdentityTests(unittest.TestCase):
    def test_response_account_is_verified_without_tokens_in_evidence(self):
        ctx = response_account_evidence(session(), {"CST": "fixture-cst", "X-SECURITY-TOKEN": "fixture-token"})
        self.assertTrue(ctx["verified"])
        self.assertEqual(ctx["account_id"], "fixture-account")
        self.assertNotIn("fixture-token", json.dumps(ctx))
        self.assertNotIn("fixture-cst", json.dumps(ctx))

    def test_changed_missing_or_unknown_session_is_unverified(self):
        for sess, headers in [(session(), {}), (session(), {"CST": "other", "X-SECURITY-TOKEN": "fixture-token"}),
                              (session(None), {"CST": "fixture-cst", "X-SECURITY-TOKEN": "fixture-token"})]:
            self.assertFalse(response_account_evidence(sess, headers)["verified"])

    def test_local_or_confirmation_date_never_becomes_exact_opening_utc(self):
        order, response, confirm = fixtures()
        for date in ("2026-09-21T05:45:56", "2026-09-21T05:45:56Z", None):
            confirm["date"] = date
            evidence = opening_evidence("GOLD", "primary", order, response, confirm, local={"entry_time": 123})
            self.assertIsNone(evidence["broker_opened_utc"])
            self.assertEqual(evidence["identity_status"], "pending_broker_opening")
            self.assertEqual(evidence["confirmation_utc"] is not None, bool(date and date.endswith("Z")))

    def test_missing_fill_fields_do_not_use_order_or_local_estimates(self):
        order, response, confirm = fixtures()
        confirm.pop("size")
        confirm.pop("level")
        evidence = opening_evidence("GOLD", "primary", order, response, confirm, local={"fill_price": 4352.53})
        self.assertIsNone(evidence["broker_quantity"])
        self.assertIsNone(evidence["broker_level"])
        self.assertEqual(evidence["identity_status"], "unverified_entry")

    def test_actual_quantity_is_separate_from_submitted_quantity(self):
        order, response, confirm = fixtures()
        confirm["size"] = .12
        evidence = opening_evidence("GOLD", "primary", order, response, confirm)
        self.assertEqual(evidence["broker_quantity"], .12)
        self.assertEqual(evidence["submitted_order"]["size"], .16)

    def test_conflicting_account_reference_or_epic_remains_unverified(self):
        for change in ({"dealReference": "other"}, {"epic": "other"},
                       {"_june_account_evidence": {"verified": True, "account_id": "other"}}):
            order, response, confirm = fixtures()
            confirm.update(change)
            self.assertEqual(opening_evidence("GOLD", "primary", order, response, confirm)["identity_status"], "unverified_entry")

    def test_primary_addon_and_market_sources_are_preserved_without_aliasing(self):
        order, response, confirm = fixtures()
        market = {"epic": "FIXTURE.GOLD", "fields": {"lotSize": 1, "unit": "AMOUNT"}}
        evidence = opening_evidence("GOLD", "add_on", order, response, confirm, market, {"parent_deal_id": "primary-1"})
        market["fields"]["lotSize"] = 999
        confirm["size"] = 999
        self.assertEqual(evidence["role"], "add_on")
        self.assertEqual(evidence["local_context"]["parent_deal_id"], "primary-1")
        self.assertEqual(evidence["broker_market_snapshot"]["fields"]["lotSize"], 1)
        self.assertEqual(evidence["broker_quantity"], .16)
        self.assertNotIn("notional", evidence)

    def test_market_epic_mismatch_is_not_silently_used(self):
        evidence = opening_evidence("GOLD", "primary", *fixtures(), market={"epic": "OLD.EPIC"})
        self.assertIsNone(evidence["broker_market_snapshot"])
        self.assertIn("market_epic_mismatch", evidence["identity_issues"])


class RuntimeIdentityTests(unittest.TestCase):
    def test_authentication_retains_explicit_account_without_currency_default(self):
        ns = runtime()
        ns["requests"].post.return_value = SimpleNamespace(status_code=200, headers={"CST": "new", "X-SECURITY-TOKEN": "new-token"},
                                                          json=lambda: {"currentAccountId": "actual-account"})
        with redirect_stdout(io.StringIO()):
            self.assertTrue(ns["authenticate_live"]())
        self.assertEqual(ns["_live_sess"]["account_id"], "actual-account")
        self.assertIsNone(ns["_live_sess"]["account_currency"])

    def test_post_preserves_order_payload_and_attaches_account(self):
        ns = runtime()
        order, response, _ = fixtures()
        original = deepcopy(order)
        ns["requests"].post.return_value = SimpleNamespace(status_code=200, json=lambda: {"dealReference": "entry-1"})
        result = ns["_ig_live_post"]("/positions/otc", order)
        self.assertEqual(order, original)
        self.assertEqual(ns["requests"].post.call_args.kwargs["json"], original)
        self.assertEqual(result["_june_account_evidence"]["account_id"], "fixture-account")
        self.assertEqual(result["dealReference"], response["dealReference"])

    def test_get_only_enriches_confirms(self):
        ns = runtime()
        ns["requests"].get.return_value = SimpleNamespace(status_code=200, json=lambda: {"dealStatus": "ACCEPTED"})
        self.assertIn("_june_account_evidence", ns["_ig_live_get"]("/confirms/entry-1"))
        self.assertNotIn("_june_account_evidence", ns["_ig_live_get"]("/accounts"))

    def test_reauthentication_uses_account_of_retried_request(self):
        ns = runtime()
        def reauthenticate():
            ns["_live_sess"].update(account_id="new-account", cst="new-cst", token="new-token")
            return True
        ns["authenticate_live"] = reauthenticate
        ns["requests"].post.side_effect = [SimpleNamespace(status_code=401), SimpleNamespace(status_code=200, json=lambda: {"dealReference": "entry-1"})]
        result = ns["_ig_live_post"]("/positions/otc", {})
        self.assertEqual(result["_june_account_evidence"]["account_id"], "new-account")

    def test_concurrent_session_change_prevents_response_attribution(self):
        ns = runtime()
        def reply():
            ns["_live_sess"].update(account_id="other", token="other-token")
            return {"dealReference": "entry-1"}
        ns["requests"].post.return_value = SimpleNamespace(status_code=200, json=reply)
        self.assertFalse(ns["_ig_live_post"]("/positions/otc", {})["_june_account_evidence"]["verified"])

    def test_capture_error_does_not_raise_into_trading_path(self):
        ns = runtime()
        ns["opening_evidence"] = Mock(side_effect=ValueError("fixture failure"))
        result = ns["_live_entry_evidence"]("GOLD", "primary", *fixtures())
        self.assertEqual(result["identity_status"], "capture_error")
        self.assertEqual(result["accepted_confirmation"]["dealId"], "deal-1")
        ns["_live_log"].assert_called_once()

    def test_actual_entry_annotations_persist_and_restore(self):
        ns = runtime()
        order, response, confirm = fixtures()
        ns.update(sym="GOLD", order_body=order, body=order, resp=response, confirm=confirm,
                  conviction=4, leg_index=2, primary={"deal_id": "primary-1"}, leg={"entry_time": 123})
        ns["_live"] = {"open_position": {"entry_time": 123}, "pyramid_legs": []}
        for name in ("_live_open_position", "_live_add_pyramid_leg"):
            annotations = [n for n in function(name).body if isinstance(n, ast.Assign)
                           and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
                           and n.value.func.id == "_live_entry_evidence"]
            self.assertEqual(len(annotations), 1)
            execute(annotations, ns)
        ns["_live"]["pyramid_legs"].append(ns["leg"])
        memory = {}
        client = SimpleNamespace(set=lambda key, value, **_: memory.update({key: value}), get=memory.get)
        ns["_redis"] = lambda: client
        ns["_live_save_state"]()
        ns["_live"] = {}
        self.assertTrue(ns["_live_load_state"]())
        self.assertEqual(ns["_live"]["open_position"]["broker_entry_evidence"]["role"], "primary")
        self.assertEqual(ns["_live"]["pyramid_legs"][0]["broker_entry_evidence"]["role"], "add_on")

    def test_promotion_preserves_addon_origin(self):
        evidence = opening_evidence("GOLD", "add_on", *fixtures(), local={"parent_deal_id": "primary-1"})
        assignment = next(n for n in ast.walk(function("_live_close_position")) if isinstance(n, ast.Assign)
                          and any(isinstance(t, ast.Name) and t.id == "_promoted" for t in n.targets))
        ns = {"_addon_leg": {"broker_entry_evidence": evidence}, "sym": "GOLD", "pos": {},
              "_addon_deal": "deal-1", "_a_fill": 4352.53, "_agg_stop": None,
              "time": SimpleNamespace(time=lambda: 123)}
        execute([assignment], ns)
        self.assertEqual(ns["_promoted"]["broker_entry_evidence"], evidence)
        self.assertEqual(ns["_promoted"]["broker_entry_evidence"]["role"], "add_on")


if __name__ == "__main__":
    unittest.main()
