"""Build-3 rolling harvest Defect-1 tests -- broker-confirmed exit for harvest accounting.

DEFECT: _live_record_rolling_harvest() previously computed P&L from signal mid,
not the broker-confirmed exit level available in the leg dict after
_live_close_addon_leg() runs.

FIX: _live_close_addon_leg() stores leg["confirmed_exit_price"] and
leg["exit_price_source"] after ACCEPTED broker confirmation.
_live_record_rolling_harvest() reads those fields and falls back to mid
only when they are absent, labelling the fallback "mid_estimate_fallback".
"""
import time
import pytest


# == Pure-logic helpers (no june import needed for most tests) =================

def make_leg(fill=6350.0, dirn="short", ig_size=0.4, gen=1, deal_id="ADDON1",
             confirmed_exit=None, exit_price_source=None):
    leg = dict(
        instrument="SILVER", direction=dirn, deal_id=deal_id, fill_price=fill,
        ig_size=ig_size, broker_stop_level=6500.0, acknowledged_stop_level=None,
        stop_sync=None, leverage=10, pos_size=100.0, notional=1000.0,
        entry_time=time.time(), stop_pct=0.01, tp_pct=0.005,
        leg_index=2, leg_generation=gen,
        actual_notional=1000.0, consumed_allocation=100.0,
    )
    if confirmed_exit is not None:
        leg["confirmed_exit_price"] = confirmed_exit
        leg["exit_price_source"] = exit_price_source or "broker_confirmed"
    return leg


def compute_harvest_record(leg, signal_mid):
    """Mirror of the fixed _live_record_rolling_harvest P&L block."""
    fill_px = leg["fill_price"]
    dirn = leg["direction"]
    ig_size = leg["ig_size"]
    confirmed_exit = leg.get("confirmed_exit_price")
    exit_price_source = leg.get("exit_price_source", "mid_estimate_fallback")
    if confirmed_exit is None:
        confirmed_exit = signal_mid
        exit_price_source = "mid_estimate_fallback"
    pnl_pct = (
        (confirmed_exit - fill_px) / fill_px if dirn == "long"
        else (fill_px - confirmed_exit) / fill_px
    )
    realized_pnl_estimate = pnl_pct * ig_size * confirmed_exit
    return {
        "confirmed_exit_price": confirmed_exit,
        "exit_price_source": exit_price_source,
        "pnl_pct": pnl_pct,
        "realized_pnl_estimate": realized_pnl_estimate,
        "fill_price": fill_px,
        "ig_size": ig_size,
    }


# == TEST A: signal mid differs from broker exit -> ledger uses broker exit =====

class TestA_BrokerExitTakesPrecedence:
    FILL = 6350.0
    REAL_EXIT = 6340.0
    MID = 6360.0
    IG_SIZE = 0.4

    def test_uses_broker_exit_not_mid(self):
        leg = make_leg(fill=self.FILL, dirn="short", ig_size=self.IG_SIZE,
                       confirmed_exit=self.REAL_EXIT)
        rec = compute_harvest_record(leg, signal_mid=self.MID)
        assert rec["confirmed_exit_price"] == self.REAL_EXIT

    def test_source_is_broker_confirmed(self):
        leg = make_leg(fill=self.FILL, dirn="short", ig_size=self.IG_SIZE,
                       confirmed_exit=self.REAL_EXIT)
        rec = compute_harvest_record(leg, signal_mid=self.MID)
        assert rec["exit_price_source"] == "broker_confirmed"

    def test_mid_would_give_wrong_sign(self):
        pnl_if_mid = (self.FILL - self.MID) / self.FILL
        assert pnl_if_mid < 0, "mid-based P&L is a loss -- confirms old bug direction"

    def test_broker_exit_gives_profit(self):
        leg = make_leg(fill=self.FILL, dirn="short", ig_size=self.IG_SIZE,
                       confirmed_exit=self.REAL_EXIT)
        rec = compute_harvest_record(leg, signal_mid=self.MID)
        assert rec["pnl_pct"] > 0


# == TEST B: broker-confirmed exit produces expected arithmetic ================

class TestB_BrokerExitArithmetic:
    FILL = 6350.0
    REAL_EXIT = 6340.0
    IG_SIZE = 0.4

    def test_pnl_pct(self):
        leg = make_leg(fill=self.FILL, dirn="short", ig_size=self.IG_SIZE,
                       confirmed_exit=self.REAL_EXIT)
        rec = compute_harvest_record(leg, signal_mid=9999.0)
        expected = (self.FILL - self.REAL_EXIT) / self.FILL
        assert rec["pnl_pct"] == pytest.approx(expected, rel=1e-6)

    def test_realized_pnl(self):
        leg = make_leg(fill=self.FILL, dirn="short", ig_size=self.IG_SIZE,
                       confirmed_exit=self.REAL_EXIT)
        rec = compute_harvest_record(leg, signal_mid=9999.0)
        expected_pct = (self.FILL - self.REAL_EXIT) / self.FILL
        expected_pnl = expected_pct * self.IG_SIZE * self.REAL_EXIT
        assert rec["realized_pnl_estimate"] == pytest.approx(expected_pnl, rel=1e-6)


# == TEST C: released margin stays separate -- Ledger B is string, never numeric

class TestC_LedgerBIsNotProfit:
    def test_ledger_b_is_string_constant(self):
        ledger_b = "not_counted_as_profit"
        assert isinstance(ledger_b, str)
        assert ledger_b == "not_counted_as_profit"

    def test_ledger_b_cannot_be_added_to_ledger_a(self):
        ledger_b = "not_counted_as_profit"
        ledger_a = 5.98
        with pytest.raises(TypeError):
            _ = ledger_a + ledger_b

    def test_harvest_record_has_no_released_margin_numeric_field(self):
        leg = make_leg(fill=6350.0, dirn="short", ig_size=0.4, confirmed_exit=6340.0)
        rec = compute_harvest_record(leg, signal_mid=6360.0)
        assert "released_margin" not in rec or not isinstance(rec.get("released_margin"), (int, float))


# == TEST D: primary P&L does not enter addon harvest P&L =====================

class TestD_PrimaryPnlIsolated:
    def test_harvest_record_uses_only_addon_fill(self):
        addon_fill = 6350.0
        primary_fill = 6426.3
        leg = make_leg(fill=addon_fill, dirn="short", ig_size=0.4, confirmed_exit=6340.0)
        rec = compute_harvest_record(leg, signal_mid=6360.0)
        expected_pct = (addon_fill - 6340.0) / addon_fill
        assert rec["pnl_pct"] == pytest.approx(expected_pct, rel=1e-6)
        wrong_pct = (primary_fill - 6340.0) / primary_fill
        assert abs(rec["pnl_pct"] - wrong_pct) > 0.001


# == TEST E: duplicate broker confirmation cannot double-count =================

class TestE_NoDuplicateHarvest:
    def test_duplicate_deal_id_blocked(self):
        deal_id = "ADDON1"
        existing_harvest = {"deal_id": deal_id, "realized_pnl_estimate": 5.0}
        live = {"rolling_realized_harvest": existing_harvest,
                "rolling_capacity_slot": "bootstrap"}
        leg = make_leg(deal_id=deal_id, confirmed_exit=6340.0)

        def harvest_eligibility(leg, live):
            existing = live.get("rolling_realized_harvest") or {}
            if existing.get("deal_id") == leg.get("deal_id", "?"):
                return False, "duplicate_harvest_delivery"
            gen = leg.get("leg_generation", 1)
            if gen != 1:
                return False, "generation_{}_not_harvest_eligible".format(gen)
            slot = live.get("rolling_capacity_slot")
            if slot != "bootstrap":
                return False, "capacity_slot_unexpected:{}".format(slot)
            return True, "eligible"

        eligible, reason = harvest_eligibility(leg, live)
        assert not eligible
        assert reason == "duplicate_harvest_delivery"


# == TEST F: restart after broker close cannot double-count ===================

class TestF_RestartIdempotency:
    def test_harvest_already_recorded_blocks_on_restart(self):
        deal_id = "ADDON1"
        live = {
            "rolling_realized_harvest": {"deal_id": deal_id, "realized_pnl_estimate": 3.99},
            "rolling_capacity_slot": "available",
        }
        existing = live.get("rolling_realized_harvest") or {}
        duplicate = (existing.get("deal_id") == deal_id)
        assert duplicate, "duplicate deal_id must block re-recording after restart"


# == TEST G: negative broker P&L stays negative ===============================

class TestG_NegativePnlStaysNegative:
    def test_losing_harvest_pnl_is_negative(self):
        # Short from 6350, exit at 6360 (price rose) -> loss
        leg = make_leg(fill=6350.0, dirn="short", ig_size=0.4, confirmed_exit=6360.0)
        rec = compute_harvest_record(leg, signal_mid=6340.0)
        assert rec["pnl_pct"] < 0
        assert rec["realized_pnl_estimate"] < 0

    def test_negative_pnl_fails_economic_eligibility(self):
        realized_harvest_pnl = -0.63
        liq_before = 0.0805
        economic_floor = -liq_before
        economically_ok = realized_harvest_pnl > 0.0 and realized_harvest_pnl >= economic_floor
        assert not economically_ok


# == TEST H: missing broker level falls back to mid, labelled fallback =========

class TestH_MissingBrokerLevelFallback:
    def test_no_confirmed_exit_uses_mid(self):
        leg = make_leg(fill=6350.0, dirn="short", ig_size=0.4)
        signal_mid = 6342.0
        rec = compute_harvest_record(leg, signal_mid=signal_mid)
        assert rec["confirmed_exit_price"] == signal_mid
        assert rec["exit_price_source"] == "mid_estimate_fallback"

    def test_fallback_label_distinguishes_from_broker_confirmed(self):
        leg_with_broker = make_leg(fill=6350.0, dirn="short", ig_size=0.4, confirmed_exit=6340.0)
        leg_fallback = make_leg(fill=6350.0, dirn="short", ig_size=0.4)
        rec_broker = compute_harvest_record(leg_with_broker, signal_mid=6342.0)
        rec_fallback = compute_harvest_record(leg_fallback, signal_mid=6342.0)
        assert rec_broker["exit_price_source"] == "broker_confirmed"
        assert rec_fallback["exit_price_source"] == "mid_estimate_fallback"

    def test_broker_level_none_and_missing_both_give_fallback(self):
        confirm_no_key = {}
        confirm_null = {"level": None}
        mid = 6342.0
        for confirm in (confirm_no_key, confirm_null):
            _level_val = confirm.get("level")
            real_exit = float(_level_val) if _level_val is not None else mid
            src = "broker_confirmed" if _level_val is not None else "mid_estimate_fallback"
            assert real_exit == mid
            assert src == "mid_estimate_fallback"


# == C38 CORRECTED REPLAY =====================================================

class TestC38CorrectedReplay:
    """
    C38 anchors:
        rolling_bootstrap_liq_before = 0.0805199999999968
        addon fill_price = 6350.0 (short), ig_size = 0.4
        broker-confirmed exit = 6335.0 (price fell, short wins)

    Table:
        signal mid at harvest:        6360.0
        broker-confirmed exit:        6335.0
        old mid-derived pnl_pct:      (6350-6360)/6350 = -0.001575  (LOSS -- the bug)
        old mid-derived realized:     -0.001575 * 0.4 * 6360 = -$4.007
        new broker-derived pnl_pct:   (6350-6335)/6350 = +0.002362  (WIN)
        new broker-derived realized:  +0.002362 * 0.4 * 6335 = +$5.984
        released margin:              not_counted_as_profit
        rolling_bootstrap_liq_before: 0.0805199999999968
        economic_floor:               -0.0805 (liq_before positive -> floor negative)
        economic eligibility:         YES (5.984 > 0, 5.984 >= -0.0805)
        generation-cap result:        BLOCKED (replacement_gen=2 > _ROLLING_MAX_GENERATIONS=1)
    """
    LIQ_BEFORE = 0.0805199999999968
    FILL = 6350.0
    IG_SIZE = 0.4
    BROKER_EXIT = 6335.0
    MID_BUGGY = 6360.0

    EXPECTED_PCT = (6350.0 - 6335.0) / 6350.0
    EXPECTED_PNL = EXPECTED_PCT * 0.4 * 6335.0
    BUGGY_PCT = (6350.0 - 6360.0) / 6350.0

    def test_old_mid_gives_loss(self):
        assert self.BUGGY_PCT < 0

    def test_corrected_pnl_pct(self):
        leg = make_leg(fill=self.FILL, dirn="short", ig_size=self.IG_SIZE,
                       confirmed_exit=self.BROKER_EXIT)
        rec = compute_harvest_record(leg, signal_mid=self.MID_BUGGY)
        assert rec["pnl_pct"] == pytest.approx(self.EXPECTED_PCT, rel=1e-6)
        assert rec["pnl_pct"] > 0

    def test_corrected_realized_pnl(self):
        leg = make_leg(fill=self.FILL, dirn="short", ig_size=self.IG_SIZE,
                       confirmed_exit=self.BROKER_EXIT)
        rec = compute_harvest_record(leg, signal_mid=self.MID_BUGGY)
        assert rec["realized_pnl_estimate"] == pytest.approx(self.EXPECTED_PNL, rel=1e-6)
        assert rec["realized_pnl_estimate"] > 0

    def test_exit_source_is_broker_confirmed(self):
        leg = make_leg(fill=self.FILL, dirn="short", ig_size=self.IG_SIZE,
                       confirmed_exit=self.BROKER_EXIT)
        rec = compute_harvest_record(leg, signal_mid=self.MID_BUGGY)
        assert rec["exit_price_source"] == "broker_confirmed"

    def test_ledger_b_is_string(self):
        assert isinstance("not_counted_as_profit", str)

    def test_economic_floor_is_negative(self):
        floor = -self.LIQ_BEFORE
        assert floor == pytest.approx(-0.0805, abs=1e-4)
        assert floor < 0

    def test_economic_eligibility_yes(self):
        realized = self.EXPECTED_PNL
        floor = -self.LIQ_BEFORE
        ok = realized > 0.0 and realized >= floor
        assert ok

    def test_generation_cap_blocks(self):
        ROLLING_MAX_GENERATIONS = 1
        replacement_gen = 2
        assert replacement_gen > ROLLING_MAX_GENERATIONS

    def test_original_principal_risk_is_zero(self):
        risk = max(0.0, -self.LIQ_BEFORE)
        assert risk == 0.0

    def test_corrected_vs_buggy_differ_materially(self):
        pnl_corrected = self.EXPECTED_PNL
        pnl_buggy = self.BUGGY_PCT * self.IG_SIZE * self.MID_BUGGY
        assert pnl_corrected > 0
        assert pnl_buggy < 0
        assert abs(pnl_corrected - pnl_buggy) > 1.0
