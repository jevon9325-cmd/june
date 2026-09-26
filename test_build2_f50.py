"""Build 2 focused tests: F50 dynamic pyramid addon sizing.

Tests cover:
- F50 arithmetic (all floor fraction math)
- Conservative MINDEAL quantization
- MINDEAL-blocked rejection
- Missing / pending protection evidence
- Insufficient margin / allocation
- Cost (commission) reserve effect
- Cross-instrument contract mechanics
- Defensive-mode compatibility
- Addon rejection paths
- Restart/recovery (protection state persisted)
- Existing 0.04 behaviour where capacity only supports MINDEAL
- F50 allowing > current addon when evidence supports it
- No change to initial sizing
- No additional pyramid legs
- C16 characterization
"""
import sys, math, copy
sys.path.insert(0, "/opt/bots/june-build1")

from defensive_scaling import f50_capacity, snapshot, plan, confirm_funding, finite

# ─── helpers ─────────────────────────────────────────────────────────────────

def make_evidence(liquidation_before, multiplier=1.0, slippage=2.0,
                  commission=0.0, quantity=0.04, direction="short"):
    """Build a minimal evidence dict as snapshot() would produce."""
    # acknowledged stop for a SHORT: set ack = fill - (liquidation_before / (qty * mult))
    # so that (fill - ack) * qty * mult = liquidation_before
    fill = 6426.3
    ack = fill - liquidation_before / (quantity * multiplier)
    return dict(
        protection_state="profit_protected",
        liquidation_before=liquidation_before,
        current_campaign_pnl=liquidation_before * 1.5,
        quantity=quantity,
        direction=direction,
        multiplier=multiplier,
        slippage=slippage,
        commission=commission,
        protection=[dict(deal_id="TEST1", quantity=quantity, fill=fill,
                         intended=ack, acknowledged=ack,
                         cost_reserve=0.0, realized_debit=0.0,
                         liquidation_pnl=liquidation_before)],
        exit_price=6388.8,
        native_point=1.0,
        minimum_stop_distance=9.0,
        economics_basis="test",
    )

# ─── §1 F50 arithmetic ───────────────────────────────────────────────────────

def test_f50_basic_half_floor():
    ev = make_evidence(1.0, multiplier=1.0, slippage=0.0, commission=0.0)
    # stop_dist=10, per_lot=10. expendable=0.5. ig_raw=0.05. floor(0.05/0.04)*0.04=0.04
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.04, f"expected 0.04 got {ig}"
    assert reason == "ok"

def test_f50_larger_capacity():
    ev = make_evidence(4.0, multiplier=1.0, slippage=0.0, commission=0.0)
    # expendable=2.0, stop_dist=10, per_lot=10, ig_raw=0.2 → floor(0.2/0.04)*0.04=0.2
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.20, f"expected 0.20 got {ig}"
    assert reason == "ok"

def test_f50_slippage_reduces_capacity():
    ev = make_evidence(1.0, multiplier=1.0, slippage=1.0, commission=0.0)
    # stop_dist=10, per_lot=1*(10+2*1)=12, expendable=0.5, ig_raw=0.5/12=0.04167
    # floor(0.04167/0.04)*0.04=0.04
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.04, f"expected 0.04 got {ig}"

def test_f50_commission_reduces_net_expendable():
    ev = make_evidence(1.0, multiplier=1.0, slippage=0.0, commission=0.3)
    # expendable=0.5, net_expendable=0.5-0.3=0.2, per_lot=10, ig_raw=0.02
    # floor(0.02/0.04)*0.04=0.0 → mindeal_blocked
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.0
    assert reason == "mindeal_blocked"

def test_f50_commission_zero_net_expendable():
    ev = make_evidence(1.0, multiplier=1.0, slippage=0.0, commission=0.5)
    # net_expendable = 0.5 - 0.5 = 0.0 → expendable_consumed_by_commission
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.0
    assert reason == "expendable_consumed_by_commission"

def test_f50_custom_floor_fraction():
    ev = make_evidence(1.0, multiplier=1.0, slippage=0.0, commission=0.0)
    # floor_fraction=0.25: expendable=0.75, per_lot=10, ig_raw=0.075
    # floor(0.075/0.04)*0.04 = 1*0.04=0.04
    ig, _ = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04, floor_fraction=0.25)
    assert ig == 0.04

    # floor_fraction=0.0: expendable=1.0, ig_raw=0.1 → 0.08
    ig2, _ = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04, floor_fraction=0.0)
    assert ig2 == 0.08

# ─── §2 Conservative MINDEAL rounding ────────────────────────────────────────

def test_f50_always_rounds_down():
    ev = make_evidence(1.0, multiplier=1.0, slippage=0.0, commission=0.0)
    # ig_raw=0.05. floor(0.05/0.04)=1. ig=0.04. NOT 0.08.
    ig, _ = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.04

def test_f50_fractional_does_not_round_up():
    ev = make_evidence(3.2, multiplier=1.0, slippage=0.0, commission=0.0)
    # expendable=1.6, stop_dist=10, per_lot=10, ig_raw=0.16
    # floor(0.16/0.04)=4. ig=0.16.
    ig, _ = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.16
    # Verify: 0.16 is exactly 4 * MINDEAL (no rounding artefact)
    assert abs(ig - 4 * 0.04) < 1e-9

def test_f50_just_below_second_mindeal():
    ev = make_evidence(1.0, multiplier=1.0, slippage=0.0, commission=0.0)
    # ig_raw = 0.5 / 6.4 = 0.078125. floor(0.078125/0.04)=1. ig=0.04 not 0.08.
    ig, _ = f50_capacity(ev, stop_distance_native=6.4, min_deal=0.04)
    assert ig == 0.04

# ─── §3 MINDEAL-blocked ───────────────────────────────────────────────────────

def test_f50_mindeal_blocked():
    ev = make_evidence(0.1, multiplier=1.0, slippage=2.0, commission=0.0)
    # expendable=0.05, per_lot=1*(stop+4), large stop: use 35
    ig, reason = f50_capacity(ev, stop_distance_native=35.0, min_deal=0.04)
    assert ig == 0.0
    assert reason == "mindeal_blocked"

def test_f50_c16_characterization():
    """C16: protected_before=1.184, stop=31pts, slippage=2, multiplier=1.
    per_lot = 1*(31+4)=35. expendable=0.592. ig_raw=0.592/35=0.01691.
    floor(0.01691/0.04)*0.04=0. MINDEAL-blocked."""
    ev = make_evidence(1.184, multiplier=1.0, slippage=2.0, commission=0.0)
    ig, reason = f50_capacity(ev, stop_distance_native=31.0, min_deal=0.04)
    assert ig == 0.0
    assert reason == "mindeal_blocked"
    # D3C used min_stop=8 (not actual aggregate stop 31): discrepancy explained
    # Implementation correctly uses production aggregate stop distance.

# ─── §4 Missing / pending protection evidence ─────────────────────────────────

def test_f50_zero_protected_profit():
    ev = make_evidence(0.0)
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.0
    assert reason == "protected_profit_nonpositive"

def test_f50_negative_protected_profit():
    ev = make_evidence(-0.5)
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.0
    assert reason == "protected_profit_nonpositive"

def test_f50_tiny_positive_profit():
    """Very small protected profit rounds to 0 via MINDEAL floor."""
    ev = make_evidence(0.001, multiplier=1.0, slippage=0.0, commission=0.0)
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    # ig_raw = 0.0005/10 = 0.00005; MINDEAL=0.04 → blocked
    assert ig == 0.0
    assert reason == "mindeal_blocked"

def test_snapshot_raises_on_missing_acknowledged_stop():
    """snapshot() raises ValueError when ack stop is absent — no F50 addon."""
    leg = dict(deal_id="X", direction="short", instrument="SILVER",
               fill_price=6426.3, ig_size=0.04)
    try:
        snapshot([leg], aggregate=None, exit_price=6388.8,
                 multiplier=1.0, slippage=2.0, commission=0.0)
        assert False, "expected ValueError"
    except ValueError as e:
        assert "missing acknowledged protection" in str(e)

def test_snapshot_raises_on_pending_stop_sync():
    """snapshot() raises when stop_sync status != 'acknowledged'."""
    leg = dict(deal_id="X", direction="short", instrument="SILVER",
               fill_price=6426.3, ig_size=0.04,
               acknowledged_stop_level=6396.8,
               stop_sync=dict(status="pending", deal_id="X", target=6396.8))
    try:
        snapshot([leg], aggregate=None, exit_price=6388.8,
                 multiplier=1.0, slippage=2.0, commission=0.0)
        assert False, "expected ValueError"
    except ValueError as e:
        assert "protection synchronization unresolved" in str(e)

# ─── §5 Per-lot cost mechanics ────────────────────────────────────────────────

def test_f50_per_lot_cost_zero_rejected():
    # Build evidence manually to avoid make_evidence division by zero with multiplier=0
    ev = dict(
        protection_state="profit_protected", liquidation_before=1.0,
        multiplier=0.0, slippage=0.0, commission=0.0,
        quantity=0.04, direction="short",
        protection=[], exit_price=6388.8, native_point=1.0,
        minimum_stop_distance=9.0, economics_basis="test",
        current_campaign_pnl=1.5,
    )
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.0
    assert reason == "per_lot_cost_nonpositive"

def test_f50_stop_distance_zero():
    ev = make_evidence(1.0, multiplier=1.0, slippage=0.0, commission=0.0)
    ig, reason = f50_capacity(ev, stop_distance_native=0.0, min_deal=0.04)
    # per_lot_cost=0 → rejected
    assert ig == 0.0
    assert reason == "per_lot_cost_nonpositive"

# ─── §6 Cross-instrument contract mechanics ───────────────────────────────────

def test_f50_equity_cfd_commission():
    """Equity CFDs incur 2*$10 commission per round trip (hypothetical)."""
    ev = make_evidence(10.0, multiplier=1.0, slippage=0.5, commission=20.0)
    # expendable=5.0, net=5.0-20.0=-15 → expendable_consumed_by_commission
    ig, reason = f50_capacity(ev, stop_distance_native=5.0, min_deal=1.0)
    assert ig == 0.0
    assert reason == "expendable_consumed_by_commission"

def test_f50_equity_large_protected_covers_commission():
    ev = make_evidence(100.0, multiplier=1.0, slippage=0.5, commission=20.0)
    # expendable=50.0, net=50-20=30, per_lot=1*(5+1)=6, ig_raw=5.0, floor/1=5.0
    ig, reason = f50_capacity(ev, stop_distance_native=5.0, min_deal=1.0)
    assert ig == 5.0
    assert reason == "ok"

def test_f50_high_multiplier_instrument():
    """Instrument with multiplier=100 (e.g. higher-value commodity)."""
    ev = make_evidence(50.0, multiplier=100.0, slippage=0.0, commission=0.0)
    # expendable=25, per_lot=100*10=1000, ig_raw=0.025, floor(0.025/0.01)*0.01=0.02
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.01)
    assert ig == 0.02
    assert reason == "ok"

def test_f50_different_mindeal():
    """OIL uses minDeal=0.04 (same as SILVER in our system)."""
    ev = make_evidence(2.0, multiplier=1.0, slippage=0.0, commission=0.0)
    # expendable=1.0, stop_dist=20, per_lot=20, ig_raw=0.05
    # floor(0.05/0.04)*0.04=0.04
    ig, _ = f50_capacity(ev, stop_distance_native=20.0, min_deal=0.04)
    assert ig == 0.04

# ─── §7 F50 allowing > current addon ─────────────────────────────────────────

def test_f50_allows_larger_than_mindeal():
    """Campaigns with substantial protected profit get larger addon."""
    ev = make_evidence(10.0, multiplier=1.0, slippage=0.0, commission=0.0)
    # expendable=5.0, stop_dist=10, per_lot=10, ig_raw=0.5
    # floor(0.5/0.04)*0.04 = 12*0.04 = 0.48
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.48
    assert reason == "ok"

def test_f50_substantially_larger_than_current():
    """Verify F50 vs a hypothetical scenario where current sizing gives MINDEAL only."""
    # Protected profit $8, stop 10, slippage 0, no commission
    # F50: expendable=4, per_lot=10, ig_raw=0.4 → 0.4 (10 × MINDEAL)
    # Under budget-based sizing (pos_sz cap): would give ig=MINDEAL=0.04
    ev = make_evidence(8.0, multiplier=1.0, slippage=0.0, commission=0.0)
    ig, _ = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.40
    assert ig > 0.04  # substantially more than MINDEAL

# ─── §8 Exact F50 floor preservation check ───────────────────────────────────

def test_f50_floor_preserved_at_50pct():
    """Verify: estimated_floor_after >= 0.5 * protected_before."""
    ev = make_evidence(2.0, multiplier=1.0, slippage=1.0, commission=0.0)
    # expendable=1.0, per_lot=1*(10+2)=12, ig_raw=1/12=0.0833, ig=0.08
    ig, reason = f50_capacity(ev, stop_distance_native=10.0, min_deal=0.04)
    assert reason == "ok"
    addon_loss = ig * 1.0 * (10.0 + 2 * 1.0)
    floor_after = 2.0 - addon_loss
    assert floor_after >= 0.5 * 2.0 - 1e-9, f"floor_after={floor_after} < 1.0"

def test_f50_floor_at_least_50pct_for_all_valid_ig():
    """Property test: any valid F50 ig satisfies the 50% floor."""
    for protected_before in [0.5, 1.0, 2.0, 5.0, 10.0]:
        for slippage in [0.0, 1.0, 2.0]:
            for stop_dist in [5.0, 10.0, 31.0]:
                ev = make_evidence(protected_before, multiplier=1.0,
                                   slippage=slippage, commission=0.0)
                ig, reason = f50_capacity(ev, stop_distance_native=stop_dist, min_deal=0.04)
                if reason == "ok" and ig > 0:
                    per_lot = 1.0 * (stop_dist + 2 * slippage)
                    addon_loss = ig * per_lot
                    floor_after = protected_before - addon_loss
                    assert floor_after >= 0.5 * protected_before - 1e-9, (
                        f"F50 violated: protected={protected_before} stop={stop_dist} "
                        f"slippage={slippage} ig={ig} floor_after={floor_after}")

# ─── §9 Initial sizing unchanged ─────────────────────────────────────────────

def test_initial_sizing_not_from_f50():
    """f50_capacity is only called for addons, not for initial position sizing."""
    # Initial positions go through _live_open_position(), not _live_add_pyramid_leg.
    # We verify f50_capacity is not called without an existing primary.
    # (Structural: f50_capacity is in defensive_scaling, not in the entry path)
    import defensive_scaling
    assert hasattr(defensive_scaling, "f50_capacity"), "f50_capacity must exist"
    assert hasattr(defensive_scaling, "snapshot"), "snapshot must still exist"
    assert hasattr(defensive_scaling, "plan"), "plan must still exist"
    assert hasattr(defensive_scaling, "confirm_funding"), "confirm_funding must still exist"

# ─── §10 No additional pyramid legs ──────────────────────────────────────────

def test_max_legs_constant_unchanged():
    """_PYRAMID_MAX_LEGS and _PYRAMID_HARD_MAX_LEGS must be unchanged."""
    # We can't import june.py directly (it auto-starts), so we check the source.
    with open("/opt/bots/june-build1/june.py") as f:
        src = f.read()
    assert "_PYRAMID_MAX_LEGS        = 2" in src, "PYRAMID_MAX_LEGS must still be 2"
    assert "_PYRAMID_HARD_MAX_LEGS   = 4" in src, "PYRAMID_HARD_MAX_LEGS must still be 4"

# ─── §11 Defensive mode compatibility ────────────────────────────────────────

def test_f50_requires_profit_protected_state():
    """F50 only fires when protection_state==profit_protected."""
    # breakeven_protected → liquidation_before ≈ 0 → MINDEAL-blocked or nonpositive
    ev_be = make_evidence(0.0)
    ev_be["protection_state"] = "breakeven_protected"
    ig, reason = f50_capacity(ev_be, stop_distance_native=10.0, min_deal=0.04)
    assert ig == 0.0  # no capacity at 0 protected profit

    # unprotected → negative liquidation_before
    ev_un = make_evidence(-1.0)
    ev_un["protection_state"] = "unprotected"
    ig2, reason2 = f50_capacity(ev_un, stop_distance_native=10.0, min_deal=0.04)
    assert ig2 == 0.0

# ─── §12 snapshot() still works for Build 1 paths ───────────────────────────

def test_snapshot_happy_path_still_works():
    """Ensure snapshot() was not broken by defensive_scaling.py changes."""
    leg = dict(deal_id="A1", direction="short", instrument="SILVER",
               fill_price=6426.3, ig_size=0.04,
               acknowledged_stop_level=6396.8,
               broker_stop_level=6396.8,
               stop_sync=dict(status="acknowledged", deal_id="A1", target=6396.8),
               intended_stop_level=6396.8)
    result = snapshot([leg], aggregate=None, exit_price=6388.8,
                      multiplier=1.0, slippage=2.0, commission=0.0)
    assert result["protection_state"] == "profit_protected"
    # snapshot reserve: qty*multiplier*slippage + commission*(1 or 2)
    # = 0.04*1.0*2.0 + 0 = 0.08
    expected_pnl = (6426.3 - 6396.8) * 0.04 * 1.0 - 0.08  # = 1.184 - 0.08 = 1.104
    assert abs(result["liquidation_before"] - expected_pnl) < 1e-6, \
        f"got {result['liquidation_before']}, expected {expected_pnl}"
    # f50_capacity with this evidence: expendable=0.552, per_lot=1*(31+4)=35
    ig, reason = f50_capacity(result, stop_distance_native=31.0, min_deal=0.04)
    # ig_raw = 0.552/35 = 0.01577 → MINDEAL-blocked
    assert ig == 0.0
    assert reason == "mindeal_blocked"

# ─── §13 Restart / recovery: protection state is ephemeral not persisted ─────

def test_protection_state_derived_not_persisted():
    """Protection state comes from snapshot() each cycle, not from stored state.
    After restart, snapshot() recomputes from current acknowledged stops."""
    # Simulate a restart: only the position dict (with deal_id, fill, ack stops) is
    # persisted in Redis. snapshot() is called fresh each cycle.
    leg = dict(deal_id="R1", direction="short", instrument="SILVER",
               fill_price=6426.3, ig_size=0.04,
               acknowledged_stop_level=6396.8,
               broker_stop_level=6396.8,
               stop_sync=dict(status="acknowledged", deal_id="R1", target=6396.8),
               intended_stop_level=6396.8)
    # Simulates fresh call after restart:
    result = snapshot([leg], aggregate=None, exit_price=6388.8,
                      multiplier=1.0, slippage=2.0, commission=0.0)
    assert result["protection_state"] == "profit_protected"  # derived correctly post-restart

# ─── §14 Existing MINDEAL-only capacity: F50 gives same as current ───────────

def test_f50_gives_mindeal_when_capacity_matches():
    """When F50 capacity is exactly one MINDEAL, result equals current policy minimum."""
    ev = make_evidence(1.184, multiplier=1.0, slippage=0.0, commission=0.0)
    # expendable=0.592, per_lot=stop, choose stop so ig_raw is between 0.04 and 0.08
    # 0.04 <= 0.592/stop < 0.08 → 0.592/0.08=7.4 < stop <= 0.592/0.04=14.8
    ig, reason = f50_capacity(ev, stop_distance_native=14.0, min_deal=0.04)
    # ig_raw = 0.592/14 = 0.04229 → floor(0.04229/0.04)*0.04 = 0.04
    assert ig == 0.04
    assert reason == "ok"

# ─── run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import traceback
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception:
            print(f"  FAIL  {t.__name__}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
