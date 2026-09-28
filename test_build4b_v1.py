"""Profit Upgrade V1 integration tests.

Covers §12 admission matrix + §5 exactly-once via the pure admission function
(b4a_v1_admit_gen2) and the fuel reservation reconciler (rolling_fuel), plus
constant/contract checks against june.py.

Broker submission itself is exercised at the fuel-state-machine level in
test_rolling_fuel.py; here we prove the ADMISSION decisions and reconciliation
are correct, that gen-3 is impossible, and gen-1 is unchanged.
"""
import pytest
import rolling_build4a as rb4
import rolling_fuel as rf
from rolling_build4a import B4A_UNKNOWN


# ── V1 contract / constants ─────────────────────────────────────────────────
class TestV1Contract:
    def test_generation_cap_is_2(self):
        import june
        assert june._ROLLING_MAX_GENERATIONS == 2

    def test_gen3_impossible(self):
        import june
        assert 3 > june._ROLLING_MAX_GENERATIONS

    def test_rollback_flag_present(self):
        import june
        assert hasattr(june, "_ROLLING_V1_ENABLED")

    def test_v1_max_mult_is_1(self):
        import june
        assert june._ROLLING_V1_MAX_MULT == 1

    def test_single_submission_boundary_marker(self):
        """Exactly one grep-able gen-2 broker submission boundary."""
        import pathlib
        src = pathlib.Path(june_path()).read_text(encoding="utf-8")
        assert src.count("V1_GEN2_SUBMISSION_BOUNDARY") >= 1
        assert src.count("def _live_v1_submit_gen2_replacement(") == 1


def june_path():
    import june
    return june.__file__


# ── Admission matrix (§12) ──────────────────────────────────────────────────
class TestAdmission:
    def test_self_funded_admits_d1_zero(self):
        r = rb4.b4a_v1_admit_gen2(a_remaining=2.0, candidate_stop_risk=1.27,
                                  ledger_b=1.27, r_primary=1.5)
        assert r["admit"] is True
        assert r["reason"] == "self_funded_d1_zero"
        assert abs(float(r["d1"])) < 1e-9

    def test_insufficient_fuel_rejected(self):
        r = rb4.b4a_v1_admit_gen2(a_remaining=0.5, candidate_stop_risk=1.27,
                                  ledger_b=1.27, r_primary=1.5)
        assert r["admit"] is False
        assert r["reason"] == "insufficient_realized_fuel_for_self_funding"
        # D1 would be positive -> not self funded
        assert r["d1"] != B4A_UNKNOWN and float(r["d1"]) > 0

    def test_unknown_stop_risk_rejected(self):
        r = rb4.b4a_v1_admit_gen2(a_remaining=2.0, candidate_stop_risk=B4A_UNKNOWN,
                                  ledger_b=1.0, r_primary=1.5)
        assert r["admit"] is False
        assert r["reason"] == "candidate_stop_risk_unknown"

    def test_unknown_fuel_rejected(self):
        r = rb4.b4a_v1_admit_gen2(a_remaining=B4A_UNKNOWN, candidate_stop_risk=1.0,
                                  ledger_b=1.0, r_primary=1.5)
        assert r["admit"] is False
        assert r["reason"] == "realized_fuel_unknown"

    def test_nonpositive_stop_rejected(self):
        r = rb4.b4a_v1_admit_gen2(a_remaining=2.0, candidate_stop_risk=0.0,
                                  ledger_b=1.0, r_primary=1.5)
        assert r["admit"] is False
        assert r["reason"] == "candidate_stop_risk_nonpositive"

    def test_gen3_rejected_by_admission(self):
        r = rb4.b4a_v1_admit_gen2(a_remaining=100.0, candidate_stop_risk=1.0,
                                  ledger_b=1.0, r_primary=1.5,
                                  candidate_generation=3, max_generations=2)
        assert r["admit"] is False
        assert "exceeds_cap" in r["reason"]

    def test_rollback_flag_disables(self):
        r = rb4.b4a_v1_admit_gen2(a_remaining=100.0, candidate_stop_risk=1.0,
                                  ledger_b=1.0, r_primary=1.5, v1_enabled=False)
        assert r["admit"] is False
        assert r["reason"] == "v1_disabled_rollback_flag"

    def test_exact_boundary_admits(self):
        """A_remaining exactly equals stop risk -> D1==0 -> admit."""
        r = rb4.b4a_v1_admit_gen2(a_remaining=1.27, candidate_stop_risk=1.27,
                                  ledger_b=1.27, r_primary=1.5)
        assert r["admit"] is True

    def test_d2_never_authorises(self):
        """Even with strong protection B, insufficient fuel is still rejected
        (D2 small must NOT authorise the trade)."""
        r = rb4.b4a_v1_admit_gen2(a_remaining=0.1, candidate_stop_risk=1.27,
                                  ledger_b=1.27, r_primary=1.5)
        assert r["admit"] is False  # B fully covers D2 but D1>0 -> reject

    def test_d1_is_unconditional_b_does_not_reduce(self):
        r = rb4.b4a_v1_admit_gen2(a_remaining=0.0, candidate_stop_risk=1.0,
                                  ledger_b=100.0, r_primary=1.5)
        # huge B, zero fuel -> D1 still full stop, reject
        assert r["admit"] is False
        assert float(r["d1"]) == pytest.approx(1.0)


# ── §5 exactly-once via reconciler ──────────────────────────────────────────
class Saver:
    def __init__(self): self.calls = 0
    def __call__(self): self.calls += 1


def live_with_reservation(state, deal_id=None, amount=1.27):
    live = {
        "rolling_realized_harvest": {"deal_id": "GEN1", "realized_pnl_estimate": 5.0,
                                     "exit_price_source": "broker_confirmed"},
        "rolling_profit_deployed": 0.0,
        "rolling_campaign_id": "campaign_P1",
        rf.RESERVATION_KEY: {"state": state, "amount": amount,
                             "campaign_id": "campaign_P1", "reservation_id": "res1",
                             "deal_ref": "ref1" if state != rf.RESERVED else None,
                             "deal_id": deal_id},
    }
    return live


class TestReconcileOnLoad:
    def test_reserved_released_on_restart(self):
        """RESERVED (no order sent) -> released, fuel restored."""
        live = live_with_reservation(rf.RESERVED)
        act = rf.reconcile_on_load(live, broker_deal_ids=set(), save=Saver())
        assert act["action"] == "released"
        assert rf.active_reservation(live) is None

    def test_submitted_opens_when_broker_has_deal(self):
        live = live_with_reservation(rf.SUBMITTED, deal_id="deal-XYZ")
        act = rf.reconcile_on_load(live, broker_deal_ids={"deal-XYZ"}, save=Saver())
        assert act["action"] == "opened"
        assert rf.active_reservation(live)["state"] == rf.OPEN

    def test_submitted_released_when_verified_absent(self):
        # Known dealId provably absent from a verified inventory -> proven-absence release.
        live = live_with_reservation(rf.SUBMITTED, deal_id="deal-XYZ")
        act = rf.reconcile_on_load(live, broker_deal_ids={"other-deal"}, save=Saver())
        assert act["action"] == "released_known_absent"
        assert rf.active_reservation(live) is None

    def test_submitted_retained_on_unknown_inventory(self):
        """Unknown inventory (None) must NOT release -> fail closed."""
        live = live_with_reservation(rf.SUBMITTED, deal_id="deal-XYZ")
        act = rf.reconcile_on_load(live, broker_deal_ids=None, save=Saver())
        assert act["action"] == "retained_unknown_inventory"
        assert rf.active_reservation(live)["state"] == rf.SUBMITTED

    def test_open_retained(self):
        live = live_with_reservation(rf.OPEN, deal_id="deal-XYZ")
        act = rf.reconcile_on_load(live, broker_deal_ids={"deal-XYZ"}, save=Saver())
        assert act["action"] == "open_retained"
        assert rf.active_reservation(live)["state"] == rf.OPEN

    def test_no_reservation_noop(self):
        live = {"rolling_campaign_id": "c"}
        act = rf.reconcile_on_load(live, broker_deal_ids=set(), save=Saver())
        assert act["action"] == "none"


# ── campaign cleanup clears reservation ─────────────────────────────────────
class TestCampaignCleanupReservation:
    def test_cleanup_clears_reservation(self):
        live = live_with_reservation(rf.OPEN, deal_id="deal-XYZ")
        cleared = rb4.b4a_clear_campaign_rolling_state(live, reason="take_profit")
        assert "rolling_fuel_reservation" in cleared
        assert live.get(rf.RESERVATION_KEY) is None

    def test_cleanup_no_reservation_ok(self):
        live = {"rolling_campaign_id": "campaign_P1", "rolling_capacity_slot": "bootstrap"}
        cleared = rb4.b4a_clear_campaign_rolling_state(live, reason="stop_loss")
        assert "rolling_capacity_slot" in cleared


# ── fuel reconciliation invariant after settle ──────────────────────────────
class TestFuelReconciliation:
    def test_loss_settle_reconciles(self):
        live = {
            "rolling_realized_harvest": {"deal_id": "GEN1", "realized_pnl_estimate": 5.0,
                                         "exit_price_source": "broker_confirmed"},
            "rolling_profit_deployed": 0.0, "rolling_campaign_id": "campaign_P1",
        }
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        rf.mark_submitted(live, "res1", "ref1", save=s)
        rf.mark_open(live, "res1", "deal1", save=s)
        rf.settle_loss(live, "res1", save=s)
        rf.reconcile_invariant(live)
        assert live["rolling_profit_deployed"] == pytest.approx(1.27)

    def test_harvest_settle_reconciles(self):
        live = {
            "rolling_realized_harvest": {"deal_id": "GEN1", "realized_pnl_estimate": 5.0,
                                         "exit_price_source": "broker_confirmed"},
            "rolling_profit_deployed": 0.0, "rolling_campaign_id": "campaign_P1",
        }
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        rf.mark_submitted(live, "res1", "ref1", save=s)
        rf.mark_open(live, "res1", "deal1", save=s)
        rf.settle_harvest(live, "res1", 2.0, save=s)
        # after harvest settle, reservation is terminal -> fuel un-committed
        assert rf.a_remaining(live) == pytest.approx(5.0)


# ── D-1 REPAIR: ATTEMPTED state + lost-POST fail-closed ──────────────────────
class TestD1LostPost:
    def _s(self):
        class S:
            def __init__(s): s.n = 0
            def __call__(s): s.n += 1
        return S()

    def _live_reserved(self, realized=5.0):
        return {"rolling_realized_harvest": {"deal_id": "GEN1", "realized_pnl_estimate": realized,
                                             "exit_price_source": "broker_confirmed"},
                "rolling_profit_deployed": 0.0, "rolling_campaign_id": "campaign_P1"}

    def test_attempted_state_exists(self):
        assert hasattr(rf, "ATTEMPTED")

    def test_mark_attempted_before_post(self):
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        assert live[rf.RESERVATION_KEY]["state"] == rf.ATTEMPTED
        assert rf.a_remaining(live) == pytest.approx(5.0 - 1.27)  # still committed

    def test_reserved_release_ok_but_attempted_cannot_plain_release(self):
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        with pytest.raises(rf.FuelError):
            rf.release(live, "r1", "should_fail", save=s)  # plain release forbidden post-attempt

    def test_lost_post_attempted_no_dealid_stays_locked(self):
        """T5/T6: ATTEMPTED, no dealId, verified inventory -> ambiguous_locked, NOT released."""
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)  # POST about to happen; response lost
        act = rf.reconcile_on_load(live, broker_deal_ids={"unrelated"}, save=s)
        assert act["action"] == "ambiguous_locked"
        assert rf.active_reservation(live)["state"] == rf.ATTEMPTED  # still committed/locked
        assert rf.a_remaining(live) == pytest.approx(5.0 - 1.27)  # fuel NOT restored

    def test_lost_post_attempted_unknown_inventory_retained(self):
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        act = rf.reconcile_on_load(live, broker_deal_ids=None, save=s)
        assert act["action"] == "retained_unknown_inventory"
        assert rf.active_reservation(live)["state"] == rf.ATTEMPTED

    def test_attempted_known_deal_present_opens(self):
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        rf.mark_submitted(live, "r1", "ref1", save=s)
        # broker truth: we learned the dealId and it is present at broker
        live[rf.RESERVATION_KEY]["deal_id"] = "deal-1"
        act = rf.reconcile_on_load(live, broker_deal_ids={"deal-1"}, save=s)
        assert act["action"] == "opened"

    def test_needs_reconciliation_covers_attempted(self):
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        assert rf.needs_broker_reconciliation(live) is True


# ── D-2 REPAIR: gen-2 terminal settlement live-wired ─────────────────────────
class TestD2Settlement:
    def test_settle_harvest_reachable_and_uncommits(self):
        live = {"rolling_realized_harvest": {"deal_id": "GEN1", "realized_pnl_estimate": 5.0,
                                             "exit_price_source": "broker_confirmed"},
                "rolling_profit_deployed": 0.0, "rolling_campaign_id": "campaign_P1"}
        s = type("S", (), {"__call__": lambda self: None})()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        rf.mark_submitted(live, "r1", "ref1", save=s)
        rf.mark_open(live, "r1", "deal1", save=s)
        rf.settle_harvest(live, "r1", 2.0, save=s)
        assert live[rf.RESERVATION_KEY]["state"] == rf.SETTLED
        assert rf.a_remaining(live) == pytest.approx(5.0)  # fuel un-committed

    def test_settle_loss_consumes_once(self):
        live = {"rolling_realized_harvest": {"deal_id": "GEN1", "realized_pnl_estimate": 5.0,
                                             "exit_price_source": "broker_confirmed"},
                "rolling_profit_deployed": 0.0, "rolling_campaign_id": "campaign_P1"}
        s = type("S", (), {"__call__": lambda self: None})()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        rf.mark_submitted(live, "r1", "ref1", save=s)
        rf.mark_open(live, "r1", "deal1", save=s)
        rf.settle_loss(live, "r1", save=s)
        assert live["rolling_profit_deployed"] == pytest.approx(1.27)
        with pytest.raises(rf.FuelError):
            rf.settle_loss(live, "r1", save=s)  # cannot settle twice

    def test_production_settle_helper_exists(self):
        import june
        assert hasattr(june, "_live_settle_gen2_reservation")

    def test_settle_functions_wired_in_production(self):
        import pathlib
        src = pathlib.Path(june_path()).read_text(encoding="utf-8")
        assert "settle_harvest(" in src and "settle_loss(" in src
        assert src.count("_live_settle_gen2_reservation(leg") >= 3  # 3 close paths


# ── D-1 REPAIR: ATTEMPTED state + lost-POST fail-closed ──────────────────────
class TestD1LostPost:
    def _s(self):
        class S:
            def __init__(s): s.n = 0
            def __call__(s): s.n += 1
        return S()

    def _live_reserved(self, realized=5.0):
        return {"rolling_realized_harvest": {"deal_id": "GEN1", "realized_pnl_estimate": realized,
                                             "exit_price_source": "broker_confirmed"},
                "rolling_profit_deployed": 0.0, "rolling_campaign_id": "campaign_P1"}

    def test_attempted_state_exists(self):
        assert hasattr(rf, "ATTEMPTED")

    def test_mark_attempted_before_post(self):
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        assert live[rf.RESERVATION_KEY]["state"] == rf.ATTEMPTED
        assert rf.a_remaining(live) == pytest.approx(5.0 - 1.27)  # still committed

    def test_reserved_release_ok_but_attempted_cannot_plain_release(self):
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        with pytest.raises(rf.FuelError):
            rf.release(live, "r1", "should_fail", save=s)  # plain release forbidden post-attempt

    def test_lost_post_attempted_no_dealid_stays_locked(self):
        """T5/T6: ATTEMPTED, no dealId, verified inventory -> ambiguous_locked, NOT released."""
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)  # POST about to happen; response lost
        act = rf.reconcile_on_load(live, broker_deal_ids={"unrelated"}, save=s)
        assert act["action"] == "ambiguous_locked"
        assert rf.active_reservation(live)["state"] == rf.ATTEMPTED  # still committed/locked
        assert rf.a_remaining(live) == pytest.approx(5.0 - 1.27)  # fuel NOT restored

    def test_lost_post_attempted_unknown_inventory_retained(self):
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        act = rf.reconcile_on_load(live, broker_deal_ids=None, save=s)
        assert act["action"] == "retained_unknown_inventory"
        assert rf.active_reservation(live)["state"] == rf.ATTEMPTED

    def test_attempted_known_deal_present_opens(self):
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        rf.mark_submitted(live, "r1", "ref1", save=s)
        # broker truth: we learned the dealId and it is present at broker
        live[rf.RESERVATION_KEY]["deal_id"] = "deal-1"
        act = rf.reconcile_on_load(live, broker_deal_ids={"deal-1"}, save=s)
        assert act["action"] == "opened"

    def test_needs_reconciliation_covers_attempted(self):
        live = self._live_reserved(); s = self._s()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        assert rf.needs_broker_reconciliation(live) is True


# ── D-2 REPAIR: gen-2 terminal settlement live-wired ─────────────────────────
class TestD2Settlement:
    def test_settle_harvest_reachable_and_uncommits(self):
        live = {"rolling_realized_harvest": {"deal_id": "GEN1", "realized_pnl_estimate": 5.0,
                                             "exit_price_source": "broker_confirmed"},
                "rolling_profit_deployed": 0.0, "rolling_campaign_id": "campaign_P1"}
        s = type("S", (), {"__call__": lambda self: None})()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        rf.mark_submitted(live, "r1", "ref1", save=s)
        rf.mark_open(live, "r1", "deal1", save=s)
        rf.settle_harvest(live, "r1", 2.0, save=s)
        assert live[rf.RESERVATION_KEY]["state"] == rf.SETTLED
        assert rf.a_remaining(live) == pytest.approx(5.0)  # fuel un-committed

    def test_settle_loss_consumes_once(self):
        live = {"rolling_realized_harvest": {"deal_id": "GEN1", "realized_pnl_estimate": 5.0,
                                             "exit_price_source": "broker_confirmed"},
                "rolling_profit_deployed": 0.0, "rolling_campaign_id": "campaign_P1"}
        s = type("S", (), {"__call__": lambda self: None})()
        rf.reserve(live, 1.27, "campaign_P1", "r1", save=s)
        rf.mark_attempted(live, "r1", save=s)
        rf.mark_submitted(live, "r1", "ref1", save=s)
        rf.mark_open(live, "r1", "deal1", save=s)
        rf.settle_loss(live, "r1", save=s)
        assert live["rolling_profit_deployed"] == pytest.approx(1.27)
        with pytest.raises(rf.FuelError):
            rf.settle_loss(live, "r1", save=s)  # cannot settle twice

    def test_production_settle_helper_exists(self):
        import june
        assert hasattr(june, "_live_settle_gen2_reservation")

    def test_settle_functions_wired_in_production(self):
        import pathlib
        src = pathlib.Path(june_path()).read_text(encoding="utf-8")
        assert "settle_harvest(" in src and "settle_loss(" in src
        assert src.count("_live_settle_gen2_reservation(leg") >= 3  # 3 close paths
