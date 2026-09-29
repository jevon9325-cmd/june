"""Build 4C-C generalized session classifier — focused tests.

Extracts the production _now_mins / is_overnight / _current_sub_session from
june.py and drives them with a controllable clock so canonical-session
classification is deterministic and DST-correct. Also asserts the migration /
schema-version and no-new-restriction invariants via source inspection.
"""
import datetime as _dt
from datetime import timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from types import SimpleNamespace
from test_broker_identity import execute, function

_US_EAST_TZ = ZoneInfo("America/New_York")
_UK_TZ = ZoneInfo("Europe/London")
OVERNIGHT_START_MIN = 21 * 60
OVERNIGHT_END_MIN = 7 * 60


class _FrozenDatetime:
    """Minimal datetime shim: .now(tz) returns a fixed instant (tz-aware)."""
    _fixed_utc = None  # aware UTC datetime

    @classmethod
    def now(cls, tz=None):
        u = cls._fixed_utc
        return u.astimezone(tz) if tz is not None else u.replace(tzinfo=None)


def _classifier_at(utc_dt, sym="GOLD"):
    """Run the extracted _current_sub_session at a given aware-UTC instant."""
    _FrozenDatetime._fixed_utc = utc_dt
    ns = dict(
        datetime=_FrozenDatetime, timezone=timezone,
        _US_EAST_TZ=_US_EAST_TZ, _UK_TZ=_UK_TZ,
        OVERNIGHT_START_MIN=OVERNIGHT_START_MIN, OVERNIGHT_END_MIN=OVERNIGHT_END_MIN,
        _SESSION_BOUNDARY_OVERRIDES={},
    )
    for fn in ("_now_mins", "is_overnight", "_current_sub_session"):
        execute([function(fn)], ns)
    return ns["_current_sub_session"](sym)


def _utc(y, m, d, hh, mm):
    return _dt.datetime(y, m, d, hh, mm, tzinfo=timezone.utc)


# ─────────────────────── overnight boundaries (fixed UTC) ───────────────────

def test_overnight_core():
    assert _classifier_at(_utc(2026, 1, 15, 3, 0)) == "overnight"     # 03:00 UTC
    assert _classifier_at(_utc(2026, 1, 15, 23, 0)) == "overnight"    # 23:00 UTC

def test_overnight_end_boundary_0700_becomes_daytime():
    # 06:59 UTC still overnight; 07:00 UTC leaves overnight.
    assert _classifier_at(_utc(2026, 1, 15, 6, 59)) == "overnight"
    assert _classifier_at(_utc(2026, 1, 15, 7, 0)) != "overnight"

def test_overnight_start_boundary_2100():
    assert _classifier_at(_utc(2026, 1, 15, 20, 59)) != "overnight"
    assert _classifier_at(_utc(2026, 1, 15, 21, 0)) == "overnight"


# ─────────────────────── daytime split (WINTER / EST, UTC-5) ────────────────
# Winter: NYSE 09:30 ET = 14:30 UTC; noon ET = 17:00 UTC.

def test_winter_pre_nyse():
    # 07:00-14:30 UTC -> pre_nyse
    assert _classifier_at(_utc(2026, 1, 15, 7, 0)) == "pre_nyse"
    assert _classifier_at(_utc(2026, 1, 15, 14, 29)) == "pre_nyse"

def test_winter_nyse_morning():
    # 14:30-17:00 UTC -> nyse_morning
    assert _classifier_at(_utc(2026, 1, 15, 14, 30)) == "nyse_morning"
    assert _classifier_at(_utc(2026, 1, 15, 16, 59)) == "nyse_morning"

def test_winter_nyse_afternoon():
    # 17:00-21:00 UTC -> nyse_afternoon
    assert _classifier_at(_utc(2026, 1, 15, 17, 0)) == "nyse_afternoon"
    assert _classifier_at(_utc(2026, 1, 15, 20, 59)) == "nyse_afternoon"


# ─────────────────────── daytime split (SUMMER / EDT, UTC-4) ────────────────
# Summer: NYSE 09:30 ET = 13:30 UTC; noon ET = 16:00 UTC.

def test_summer_pre_nyse():
    assert _classifier_at(_utc(2026, 7, 15, 13, 29)) == "pre_nyse"

def test_summer_nyse_morning_boundary_shifts_with_DST():
    # 13:30 UTC is nyse_morning in summer (would still be pre_nyse in winter).
    assert _classifier_at(_utc(2026, 7, 15, 13, 30)) == "nyse_morning"
    # sanity: same wall-clock UTC in winter is still pre_nyse
    assert _classifier_at(_utc(2026, 1, 15, 13, 30)) == "pre_nyse"

def test_summer_nyse_afternoon():
    assert _classifier_at(_utc(2026, 7, 15, 16, 0)) == "nyse_afternoon"


# ─────────────────────── UK/US DST mismatch week ────────────────────────────
# ~2026-03-11: US already on EDT (Mar 8), UK still on GMT (until Mar 29).
# Classifier is NYSE(ET)-anchored, so it must follow US EDT here.

def test_us_uk_dst_mismatch_week_follows_us():
    # 13:30 UTC during mismatch week: US EDT -> NYSE open -> nyse_morning
    assert _classifier_at(_utc(2026, 3, 11, 13, 30)) == "nyse_morning"


# ─────────────────────── universe coverage (no generic 'day') ───────────────

def test_all_asset_classes_get_granular_daytime():
    # Previously only OIL/SILVER/NATGAS/WHEAT got the split; now ALL do.
    day_utc = _utc(2026, 7, 15, 15, 0)  # summer, well into NYSE morning
    for sym in ["GOLD", "SUGAR", "COCOA", "HO", "EURUSD", "SPX500", "UK100",
                "GER40", "AAPL", "SQQQ", "BTC", "OIL", "SILVER", "NATGAS", "WHEAT"]:
        s = _classifier_at(day_utc, sym)
        assert s in ("pre_nyse", "nyse_morning", "nyse_afternoon"), (sym, s)
        assert s != "day", f"{sym} still collapses to generic 'day'"

def test_no_instrument_returns_generic_day_ever():
    # Sweep the daytime window for a non-legacy instrument; must never be 'day'.
    for hh in range(7, 21):
        s = _classifier_at(_utc(2026, 7, 15, hh, 0), "AAPL")
        assert s != "day"


# ─────────────────────── determinism / no I/O ───────────────────────────────

def test_deterministic_same_utc_same_result():
    t = _utc(2026, 7, 15, 15, 0)
    assert _classifier_at(t, "GOLD") == _classifier_at(t, "GOLD")


# ─────────────────────── source-level invariants (migration + safety) ───────

def _src():
    return Path("june.py").read_text(encoding="utf-8")

def test_schema_version_present_and_stamped():
    s = _src()
    assert "_SESSION_SCHEMA_VERSION = 2" in s
    assert '"session_schema": _SESSION_SCHEMA_VERSION' in s

def test_sar_still_requires_min_samples_sparse_not_a_block():
    # The SAR block still gates on >= _PERF_BLOCK_SAR_SESSION_MIN valid samples,
    # so a fresh/empty new granular bucket cannot create a restriction.
    s = _src()
    assert "len(sar_vals) >= _PERF_BLOCK_SAR_SESSION_MIN" in s

def test_legacy_fallback_match_preserved():
    # SAR eval must still fall back to legacy 'session'/'day' tag for old records.
    s = _src()
    assert 't.get("sub_session", t.get("session", "day")) == cur_sub_session' in s

def test_no_risk_or_winner_constant_changed():
    # Guard: none of the protected constants/strings were touched by this patch.
    s = _src()
    assert "_MPD_SLIPPAGE_PIPS   = 2" in s
    assert "_MPD_MIN_PROFIT_PIPS = 1" in s
    assert "_mpd_mfe_gate" in s                      # MPD repair intact
    assert "_PYRAMID_PROFIT_GATE_PCT = 0.0015" in s
    assert "_LIVE_CIRCUIT_BREAKER_PCT = -0.05" in s
    assert "_ROLLING_MAX_GENERATIONS       = 2" in s
