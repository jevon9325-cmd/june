"""Offline execution of production functions; no bot import or external I/O."""
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from defensive_state import update
from test_broker_identity import execute, function


def account(**changes):
    return dict(balance_day_start=162.27, balance_total=151.55,
                balance_fetched_at=1000., **changes)


def advance(state, now=1000.):
    return update(state, now=now, max_age=360, micro_balance=30,
                  micro_floor=1, micro_pct=.15, floor=10, pct=.025)


def step_harness():
    ns = dict(_live=account(open_position={"instrument": "GOLD"}),
              _sim={}, _current_cycle_signals_snap={}, json=json,
              datetime=datetime, _UK_TZ=timezone.utc,
              time=SimpleNamespace(time=lambda: 1000.),
              _LIVE_POLL_INTERVAL=300, POLL_ACTIVE=60, _LIVE_CB_MICRO_THRESH=30,
              _LIVE_DEF_MICRO_FLOOR_USD=1, _LIVE_DEF_MICRO_PCT=.15,
              _LIVE_DEF_FLOOR_USD=10, _LIVE_DEF_PCT=.025, _LIVE_DEF_TIMEOUT_SECS=1800,
              _redis=Mock(return_value=Mock(get=Mock(return_value=None))))
    for name in ("_live_poll_balance", "_live_poll_pnl", "_live_check_skim",
                 "_live_publish_eligible_instruments", "_sim_apply_pos_adjust",
                 "_live_check_exit", "_live_check_pyramid_exits", "_live_check_pyramid_entry",
                 "_live_retry_stop_sync", "_live_check_circuit_breaker", "_live_save_state",
                 "_live_observe", "_live_log", "_live_try_entry", "_live_shadow_evaluate_blocked"):
        ns[name] = Mock()
    execute([function(n) for n in ("_live_update_defensive_mode", "_run_live_step_observed")], ns)
    return ns


def test_activation_while_holding_actual_step():
    ns = step_harness()
    ns["_run_live_step_observed"]({})
    assert ns["_live"]["global_mode"] == "defensive"
    ns["_live_check_exit"].assert_called_once()
    ns["_live_check_pyramid_entry"].assert_called_once()


@pytest.mark.parametrize("elapsed", [0, 1800, 3600, 86400])
def test_time_alone_cannot_recover(elapsed):
    state = account()
    advance(state)
    state["balance_fetched_at"] += elapsed
    assert advance(state, 1000 + elapsed) is None
    assert state["global_mode"] == "defensive"


def test_real_recovery_and_exact_boundary():
    state = account()
    advance(state)
    state["balance_total"] = 157.27
    assert advance(state) is None
    state["balance_total"] = 157.28
    assert advance(state)["new"] == "normal"


def test_midnight_and_restart_are_not_recovery():
    state = account()
    advance(state)
    state = json.loads(json.dumps(state))
    state["balance_day_start"] = 151.55
    assert advance(state) is None
    assert state["global_mode_reference"] == 162.27
    state["balance_total"] = 158
    assert advance(state)["new"] == "normal"


@pytest.mark.parametrize("failure", ["calculation", "storage", "telemetry"])
def test_failure_does_not_suppress_exit(failure):
    ns = step_harness()
    name = {"calculation": "_live_update_defensive_mode", "storage": "_live_save_state",
            "telemetry": "_live_observe"}[failure]
    ns[name] = Mock(side_effect=RuntimeError("unavailable"))
    ns["_run_live_step_observed"]({})
    ns["_live_check_exit"].assert_called_once()
    ns["_live_check_pyramid_exits"].assert_called_once()
    ns["_live_retry_stop_sync"].assert_called_once()
    ns["_live_check_pyramid_entry"].assert_not_called()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), None])
def test_invalid_balance_cannot_recover(value):
    state = account(global_mode="defensive")
    state["balance_total"] = value
    with pytest.raises((ValueError, TypeError)):
        advance(state)
    assert state["global_mode"] == "defensive"


def test_stale_balance_cannot_recover():
    state = account(global_mode="defensive")
    state["balance_total"] = 170
    with pytest.raises(ValueError, match="stale"):
        advance(state, 2000)


def test_instrument_recovery_remains_independent():
    ns = step_harness()
    ns["_live"].update(instrument_mode={"GOLD": "defensive"},
                       instrument_mode_entered_at={"GOLD": -1000})
    ns["_run_live_step_observed"]({})
    assert ns["_live"]["instrument_mode"]["GOLD"] == "normal"
    assert ns["_live"]["global_mode"] == "defensive"
