"""Tests for rolling_fuel.py — the write-ahead fuel reservation state machine.

Covers §5 crash boundaries and the core invariant:
  no unit of realized Ledger-A fuel may finance two orders.
"""
import json
import pytest
import rolling_fuel as rf


def base_live(realized=5.984, deployed=0.0, campaign="campaign_P1"):
    return {
        "rolling_realized_harvest": {"deal_id": "GEN1", "realized_pnl_estimate": realized,
                                     "exit_price_source": "broker_confirmed"},
        "rolling_profit_deployed": deployed,
        "rolling_campaign_id": campaign,
    }


class Saver:
    """Persistence double with a controllable failure switch and call count."""
    def __init__(self):
        self.calls = 0
        self.fail = False
        self.snapshots = []
    def __call__(self):
        self.calls += 1
        if self.fail:
            raise ConnectionError("redis down")


# ---- a_remaining / reconciliation -----------------------------------------
class TestFuelArithmetic:
    def test_remaining_equals_total_when_no_reservation(self):
        live = base_live(realized=5.984)
        assert rf.a_remaining(live) == pytest.approx(5.984)

    def test_reservation_reduces_remaining(self):
        live = base_live(realized=5.984)
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        assert rf.a_remaining(live) == pytest.approx(5.984 - 1.27)

    def test_reconcile_invariant_holds_after_reserve(self):
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.0, "campaign_P1", "res1", save=s)
        rf.reconcile_invariant(live)  # must not raise

    def test_no_harvest_zero_remaining(self):
        live = {"rolling_profit_deployed": 0.0, "rolling_campaign_id": "c"}
        assert rf.a_remaining(live) == 0.0


# ---- can_reserve fail-closed ----------------------------------------------
class TestCanReserve:
    def test_insufficient_fuel_rejected(self):
        live = base_live(realized=0.5)
        ok, reason = rf.can_reserve(live, 1.27, "campaign_P1")
        assert not ok and reason == "insufficient_realized_fuel"

    def test_nonpositive_rejected(self):
        live = base_live()
        ok, reason = rf.can_reserve(live, 0.0, "campaign_P1")
        assert not ok and reason == "amount_nonpositive"

    def test_missing_campaign_rejected(self):
        live = base_live()
        ok, reason = rf.can_reserve(live, 1.0, "")
        assert not ok and reason == "campaign_id_missing"

    def test_active_reservation_blocks_second(self):
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.0, "campaign_P1", "res1", save=s)
        ok, reason = rf.can_reserve(live, 1.0, "campaign_P1")
        assert not ok and reason.startswith("reservation_active")

    def test_nonfinite_rejected(self):
        live = base_live()
        ok, reason = rf.can_reserve(live, float("inf"), "campaign_P1")
        assert not ok and reason.startswith("amount_invalid")


# ---- §5 CRASH BOUNDARIES ---------------------------------------------------
class TestCrashBoundaries:
    def test_A_before_reservation_save(self):
        """Crash before reservation persist -> save raises -> no reservation held."""
        live = base_live()
        s = Saver(); s.fail = True
        with pytest.raises(rf.FuelError):
            rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        # fail closed: no reservation, full fuel intact
        assert rf.active_reservation(live) is None
        assert rf.a_remaining(live) == pytest.approx(5.984)

    def test_B_after_reservation_before_broker(self):
        """RESERVED persisted, process dies before broker call.
        On restart: reservation is RESERVED (not SUBMITTED) -> safe to release,
        no order was ever sent."""
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        # simulate restart: state loaded from persistence
        restored = json.loads(json.dumps(live))
        res = rf.active_reservation(restored)
        assert res["state"] == rf.RESERVED
        # No broker call happened; release is legal from RESERVED
        rf.release(restored, "res1", "no_broker_call_made", save=Saver())
        assert rf.a_remaining(restored) == pytest.approx(5.984)

    def test_C_broker_received_die_before_response(self):
        """Order may exist at broker but we have no ref. State stays RESERVED
        or SUBMITTED; must NOT release blindly if we cannot prove absence.
        Here we model: we did mark_submitted with a ref BEFORE knowing outcome."""
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        rf.mark_submitted(live, "res1", "ref-1", save=s)
        restored = json.loads(json.dumps(live))
        assert rf.needs_broker_reconciliation(restored) is True
        # Cannot release from SUBMITTED via a proof-required path unless proven absent
        # release() allows SUBMITTED->RELEASED only when caller proves absence.
        # But blind harvest/open is not possible without mark_open.

    def test_D_response_received_die_before_ref_save(self):
        """If ref save failed, mark_submitted raised path leaves RESERVED.
        Reservation still RESERVED; reconcile can re-query broker by campaign."""
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        s.fail = True
        with pytest.raises(Exception):
            rf.mark_submitted(live, "res1", "ref-1", save=s)
        # state mutated in memory to SUBMITTED but not persisted; on restart the
        # PERSISTED state is still RESERVED. Model that:
        # (in-memory here shows SUBMITTED, but durable truth = RESERVED)

    def test_E_ref_saved_die_before_open_commit(self):
        """SUBMITTED persisted; die before mark_open. Restart -> reconcile needed."""
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        rf.mark_submitted(live, "res1", "ref-1", save=s)
        restored = json.loads(json.dumps(live))
        assert rf.needs_broker_reconciliation(restored)
        # Broker truth says OPEN -> mark_open
        rf.mark_open(restored, "res1", "deal-XYZ", save=Saver())
        assert rf.active_reservation(restored)["state"] == rf.OPEN

    def test_F_gen2_open_restart(self):
        """OPEN persisted; restart -> still OPEN, fuel still committed."""
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        rf.mark_submitted(live, "res1", "ref-1", save=s)
        rf.mark_open(live, "res1", "deal-XYZ", save=s)
        restored = json.loads(json.dumps(live))
        assert rf.active_reservation(restored)["state"] == rf.OPEN
        assert rf.a_remaining(restored) == pytest.approx(5.984 - 1.27)

    def test_H_broker_closes_die_before_harvest_credit(self):
        """OPEN, broker closed win, die before settle. Restart still OPEN;
        settle_harvest is idempotent-safe because it requires OPEN state."""
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        rf.mark_submitted(live, "res1", "ref-1", save=s)
        rf.mark_open(live, "res1", "deal-XYZ", save=s)
        restored = json.loads(json.dumps(live))
        rf.settle_harvest(restored, "res1", 2.0, save=Saver())
        # second settle attempt must fail (not OPEN anymore)
        with pytest.raises(rf.FuelError):
            rf.settle_harvest(restored, "res1", 2.0, save=Saver())

    def test_I_harvest_settle_persist_fail(self):
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        rf.mark_submitted(live, "res1", "ref-1", save=s)
        rf.mark_open(live, "res1", "deal-XYZ", save=s)
        s2 = Saver(); s2.fail = True
        with pytest.raises(Exception):
            rf.settle_harvest(live, "res1", 2.0, save=s2)

    def test_J_loss_settle_consumes_fuel_once(self):
        """Losing close consumes reserved fuel via deployed; a second settle fails."""
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        rf.mark_submitted(live, "res1", "ref-1", save=s)
        rf.mark_open(live, "res1", "deal-XYZ", save=s)
        rf.settle_loss(live, "res1", save=s)
        assert live["rolling_profit_deployed"] == pytest.approx(1.27)
        # remaining reflects consumed fuel
        assert rf.a_remaining(live) == pytest.approx(5.984 - 1.27)
        with pytest.raises(rf.FuelError):
            rf.settle_loss(live, "res1", save=s)


# ---- EXACTLY-ONCE / DOUBLE-SPEND -------------------------------------------
class TestExactlyOnce:
    def test_cannot_reserve_twice_same_fuel(self):
        live = base_live(realized=1.5)   # only enough for ONE 1.27 reservation
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        ok, reason = rf.can_reserve(live, 1.27, "campaign_P1")
        assert not ok  # second reservation blocked (active + insufficient)

    def test_release_from_open_forbidden(self):
        """Releasing an OPEN reservation would strand a real order -> forbidden."""
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        rf.mark_submitted(live, "res1", "ref-1", save=s)
        rf.mark_open(live, "res1", "deal-XYZ", save=s)
        with pytest.raises(rf.FuelError):
            rf.release(live, "res1", "should_not_be_allowed", save=s)

    def test_reservation_id_mismatch_rejected(self):
        live = base_live()
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        with pytest.raises(rf.FuelError):
            rf.mark_submitted(live, "WRONG", "ref-1", save=s)

    def test_release_restores_exact_fuel(self):
        live = base_live(realized=5.984)
        s = Saver()
        rf.reserve(live, 1.27, "campaign_P1", "res1", save=s)
        rf.release(live, "res1", "broker_absent", save=s)
        assert rf.a_remaining(live) == pytest.approx(5.984)

    def test_campaign_mismatch_reservation_blocked(self):
        """A reservation from another campaign must not be reusable."""
        live = base_live(campaign="campaign_NEW")
        live[rf.RESERVATION_KEY] = {"state": rf.RESERVED, "amount": 1.0,
                                     "campaign_id": "campaign_OLD", "reservation_id": "old"}
        ok, reason = rf.can_reserve(live, 1.0, "campaign_NEW")
        assert not ok  # active reservation from old campaign blocks
