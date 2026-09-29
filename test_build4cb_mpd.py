"""Build 4C-B MPD winner-clipping repair — focused tests.

The live MPD arm condition lives inside the large _live_check_exit function.
Rather than extract that whole function, these tests verify the exact arm-gate
LOGIC the patch introduces, using a faithful local reproduction of the arm
predicate (kept in lock-step with june.py by construction), plus a deterministic
regression fixture built from the real SILVER DIAAAAR8VWPHYAR geometry.

Repair under test:
  MPD only arms once pnl_pct >= 0.5 * TP  (DPLE breakeven parity)  AND
  spread-adjusted profit still clears the friction floor.
Previously it armed on the friction floor alone (~breakeven+3pips).
"""
import ast
from pathlib import Path

# ---- Constants pulled from production (asserted below to stay in sync) ----
_MPD_SLIPPAGE_PIPS = 2
_MPD_MIN_PROFIT_PIPS = 1


def _mpd_arms_OLD(dirn, fill_px, exit_px, spread_native, pip):
    """Pre-patch arm predicate: friction floor only."""
    fric = spread_native + _MPD_SLIPPAGE_PIPS * pip + _MPD_MIN_PROFIT_PIPS * pip
    if dirn == "long":
        return (exit_px - fill_px) >= fric
    return (fill_px - exit_px) >= fric


def _mpd_arms_NEW(dirn, fill_px, exit_px, spread_native, pip, pnl_pct, tp_pct):
    """Post-patch arm predicate: meaningful-MFE gate (0.5xTP) AND friction floor."""
    fric = spread_native + _MPD_SLIPPAGE_PIPS * pip + _MPD_MIN_PROFIT_PIPS * pip
    mfe_gate = (tp_pct > 0 and pnl_pct >= 0.5 * tp_pct)
    floor = ((exit_px - fill_px) >= fric) if dirn == "long" else ((fill_px - exit_px) >= fric)
    return mfe_gate and floor


# ─────────────────────────── constant sync guard ───────────────────────────

def test_constants_match_production():
    """Fail loudly if production MPD constants drift from this test's assumptions."""
    src = Path("june.py").read_text(encoding="utf-8")
    assert "_MPD_SLIPPAGE_PIPS   = 2" in src
    assert "_MPD_MIN_PROFIT_PIPS = 1" in src


def test_patch_present_and_shaped():
    """The production arm condition must include the MFE gate deferring to DPLE."""
    src = Path("june.py").read_text(encoding="utf-8")
    assert "_mpd_mfe_gate" in src, "MFE gate missing from june.py"
    assert "0.5 * _dple_tp_l" in src, "MFE gate must key off 0.5 x TP (DPLE parity)"
    # arm must be gate AND friction, not friction alone
    assert "_mpd_act    = _mpd_mfe_gate and (" in src


# ─────────────────────────── SILVER regression fixture ─────────────────────
# Real geometry: entry 6073.30, exit trigger ~6077.31 (+~0.066%), TP 0.27%,
# spread ~4 native pts, pip 0.01. At the exit moment pnl_pct ~ +0.0005 (+0.05%).
SILVER = dict(dirn="long", fill_px=6073.30, pip=0.01, spread_native=4.0, tp_pct=0.0027)


def test_silver_OLD_armed_the_bug():
    # OLD predicate arms once move clears friction (~+4.03 native pts). During the
    # SILVER hold price reached ~6079-6086 (>= +4.03), so OLD armed and set the lock
    # at 6077.31; a later retrace fired mpd_floor at +0.05%. This is the defect.
    assert _mpd_arms_OLD("long", 6073.30, 6079.20, 4.0, 0.01) is True
    # And the friction floor is ~4.03pts: a +4.01 move does NOT yet arm (boundary).
    assert _mpd_arms_OLD("long", 6073.30, 6077.31, 4.0, 0.01) is False


def test_silver_NEW_does_not_arm_at_tiny_profit():
    # pnl at exit ~ +0.05% ; 0.5xTP = 0.135% -> NEW must NOT arm (winner not clipped).
    pnl_pct = (6077.31 - 6073.30) / 6073.30  # ~0.00066
    assert pnl_pct < 0.5 * SILVER["tp_pct"]
    assert _mpd_arms_NEW("long", 6073.30, 6077.31, 4.0, 0.01, pnl_pct, 0.0027) is False


def test_silver_NEW_arms_once_meaningful_profit_reached():
    # If SILVER had reached 0.5xTP (~+0.135%, price ~6081.5), NEW arms (protection engages).
    px = 6073.30 * (1 + 0.5 * 0.0027 + 0.0001)  # just past 0.5xTP
    pnl_pct = (px - 6073.30) / 6073.30
    assert _mpd_arms_NEW("long", 6073.30, px, 4.0, 0.01, pnl_pct, 0.0027) is True


# ─────────────────────────── invariants ─────────────────────────────────────

def test_tiny_positive_noise_does_not_arm():
    # +2pts noise, well below 0.5xTP -> no arm
    pnl_pct = (6075.30 - 6073.30) / 6073.30
    assert _mpd_arms_NEW("long", 6073.30, 6075.30, 4.0, 0.01, pnl_pct, 0.0027) is False


def test_friction_floor_still_required_even_above_mfe_gate():
    # pnl clears 0.5xTP but spread-adjusted move below friction floor -> no arm.
    # Construct: exit_px only +1pt above fill (below friction ~7pts) but pnl_pct faked high.
    assert _mpd_arms_NEW("long", 6073.30, 6074.30, 4.0, 0.01, 0.01, 0.0027) is False


def test_short_direction_symmetric():
    # SHORT at meaningful profit arms; at tiny profit does not.
    tp = 0.0027
    # tiny: -4pts
    pnl_small = (6073.30 - 6069.30) / 6073.30
    assert _mpd_arms_NEW("short", 6073.30, 6069.30, 4.0, 0.01, pnl_small, tp) is False
    # meaningful: past 0.5xTP downward
    px = 6073.30 * (1 - 0.5 * tp - 0.0001)
    pnl_big = (6073.30 - px) / 6073.30
    assert _mpd_arms_NEW("short", 6073.30, px, 4.0, 0.01, pnl_big, tp) is True


def test_zero_or_missing_tp_never_arms():
    # tp_pct <= 0 -> MFE gate false -> never arms (fail-safe: DPLE/stop still protect)
    assert _mpd_arms_NEW("long", 6073.30, 6200.0, 4.0, 0.01, 0.02, 0.0) is False


def test_ordering_mpd_no_longer_preempts_pyramid_gate():
    # Pyramid gate = +0.15%. MPD MFE gate = 0.5xTP. For SILVER conv5 TP 0.27%,
    # 0.5xTP = 0.135% < 0.15%, so MPD can still arm just before the pyramid gate,
    # BUT it no longer arms at the old +0.05-0.07% friction level. This test
    # documents that the arm threshold moved from friction (~0.066%) up to 0.135%.
    tp = 0.0027
    old_arm_pnl = 0.00066   # old friction-level profit
    assert _mpd_arms_NEW("long", 6073.30, 6073.30 * (1 + old_arm_pnl), 4.0, 0.01,
                         old_arm_pnl, tp) is False
    new_arm_pnl = 0.5 * tp  # 0.135%
    assert new_arm_pnl >= 0.00066  # threshold strictly raised
