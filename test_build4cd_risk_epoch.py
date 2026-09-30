"""Build 4C-D risk-epoch and global defensive lifecycle tests.

All tests are local AST/unit characterization tests. They do not connect to
Redis, IG, the broker, or a running service. The deployed production source is
selected explicitly through _cd_june.py when present, otherwise the downloaded
_prod_4cd_june.py snapshot.
"""
import ast
import json
import math
import warnings
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

JUNE_SOURCE = next(path for path in (
    Path("_cd_june.py"), Path("_prod_4cd_june.py"), Path("june.py")
) if path.exists())
DEF_SOURCE = next(path for path in (
    Path("_cd_defensive_state.py"), Path("_defensive_state_4cd.py"),
    Path("defensive_state.py")
) if path.exists())
JUNE_TREE = ast.parse(JUNE_SOURCE.read_text(encoding="utf-8"))
DEF_TREE = ast.parse(DEF_SOURCE.read_text(encoding="utf-8"))


def _function_node(tree, name):
    return next((n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name), None)


def _exec_function(tree, name, ns):
    node = _function_node(tree, name)
    assert node is not None, f"missing production function: {name}"
    exec(compile(ast.Module(body=[node], type_ignores=[]), f"extract_{name}", "exec"), ns)
    return ns[name]


def _load_expirer(state, logs=None):
    logs = logs if logs is not None else []
    ns = {
        "_live": state,
        "datetime": datetime,
        "timezone": timezone,
        "_live_log": lambda msg: logs.append(msg),
    }
    return _exec_function(JUNE_TREE, "_live_expire_prior_epoch_global_defensive", ns)


def _load_defensive_update():
    ns = {"math": math}
    return _exec_function(DEF_TREE, "update", ns)


def _prior_epoch_state(**overrides):
    state = {
        "balance": 135.83,
        "balance_total": 135.83,
        "balance_margin": 0.0,
        "balance_day_start": 135.83,
        "balance_day_start_date": "2026-09-30",
        "balance_fetched_at": 1_790_740_000.0,
        "global_mode": "defensive",
        "global_mode_reference": 148.59,
        "global_mode_bal_entry": 137.13,
        "global_mode_entered_at": 1_790_590_444.6245,  # 2026-09-28
        "global_mode_reference_date": "2026-09-28",
        "global_mode_defensive_since": 1_790_590_444.6245,
        "global_mode_defensive_date": "2026-09-28",
        "open_position": None,
        "kill_switch": False,
        "failed_theses": {"SILVER_long": {"failure_id": "DIAAAAR82UWDLBA"}},
        "trade_history": [{"evidence_class": "BROKER_CONFIRMED_LIVE", "dollar_pnl": 1.0}],
        "settled_primary_keys": ["deal:DIAAAAR82UWDLBA"],
        "pyramid_legs": [{"deal_id": "addon-1", "leg_generation": 1}],
        "rolling_capacity_slot": "bootstrap",
        "rolling_realized_harvest": {"deal_id": "addon-0"},
        "cumulative_earned_pnl": -120.14,
        "session": "day",
        "sub_session": "nyse_afternoon",
    }
    state.update(overrides)
    return state


def _update(state, now=1_790_740_100.0):
    return _load_defensive_update()(state, now=now, max_age=600.0,
                                    micro_balance=30.0, micro_floor=1.0,
                                    micro_pct=0.15, floor=10.0, pct=0.025)


# ───────────────────────── defensive risk-epoch lifecycle ─────────────────────
def test_prior_epoch_defensive_releases_and_clears_only_global_episode_fields():
    state = _prior_epoch_state()
    original = {k: state[k] for k in ("failed_theses", "trade_history", "pyramid_legs",
                                       "settled_primary_keys", "open_position", "cumulative_earned_pnl")}
    logs = []
    assert _load_expirer(state, logs)("2026-09-30") is True
    assert state["global_mode"] == "normal"
    assert state["global_mode_bal_entry"] == 0.0
    assert state["global_mode_entered_at"] == 0.0
    for key in ("global_mode_reference", "global_mode_reference_date",
                "global_mode_defensive_since", "global_mode_defensive_date"):
        assert key not in state
    assert original == {k: state[k] for k in original}
    assert any("prior UTC risk epoch expired" in msg for msg in logs)


def test_same_day_restart_preserves_defensive_episode():
    state = _prior_epoch_state(global_mode_entered_at=1_790_740_044.0,
                               global_mode_reference=135.83,
                               global_mode_reference_date="2026-09-30")
    assert _load_expirer(state)("2026-09-30") is False
    assert state["global_mode"] == "defensive"
    assert state["global_mode_reference"] == 135.83
    assert state["global_mode_entered_at"] == 1_790_740_044.0


def test_normal_state_is_noop():
    state = _prior_epoch_state(global_mode="normal")
    before = dict(state)
    assert _load_expirer(state)("2026-09-30") is False
    assert state == before


def test_invalid_or_missing_origin_timestamp_fails_closed():
    for value in (None, 0.0, "not-a-time", float("nan")):
        state = _prior_epoch_state(global_mode_entered_at=value)
        before = dict(state)
        assert _load_expirer(state)("2026-09-30") is False
        assert state == before


def test_future_origin_timestamp_does_not_clear_defensive():
    future = datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp()
    state = _prior_epoch_state(global_mode_entered_at=future)
    assert _load_expirer(state)("2026-09-30") is False
    assert state["global_mode"] == "defensive"


def test_expiration_is_idempotent_and_does_not_oscillate():
    state = _prior_epoch_state()
    exp = _load_expirer(state)
    assert exp("2026-09-30") is True
    assert exp("2026-09-30") is False
    assert state["global_mode"] == "normal"


def test_new_day_loss_can_reenter_defensive_after_expiration():
    state = _prior_epoch_state(balance_total=124.0)
    assert _load_expirer(state)("2026-09-30") is True
    transition = _update(state)
    assert transition and transition["new"] == "defensive"
    assert state["global_mode"] == "defensive"
    assert state["global_mode_reference"] == state["balance_day_start"]
    assert datetime.fromtimestamp(state["global_mode_entered_at"], timezone.utc).date() == datetime.fromisoformat("2026-09-30").date()


def test_current_day_defensive_reference_is_not_rebased_by_restart():
    state = _prior_epoch_state(global_mode_entered_at=1_790_740_044.0,
                               global_mode_reference=135.83)
    _update(state, now=1_790_740_100.0)
    before = dict(state)
    assert _load_expirer(state)("2026-09-30") is False
    assert state == before


def test_direct_defensive_dependency_alone_reproduces_old_carryover():
    """Characterization: the dependency intentionally preserves old reference;
    the new epoch hook must run before it."""
    state = _prior_epoch_state()
    assert _update(state) is None
    assert state["global_mode"] == "defensive"
    assert state["global_mode_reference"] == 148.59


def test_neutral_regime_state_is_not_part_of_epoch_reset():
    state = _prior_epoch_state(regime="neutral")
    assert _load_expirer(state)("2026-09-30") is True
    assert state["regime"] == "neutral"
    assert state["global_mode"] == "normal"


def test_directional_regime_state_is_not_part_of_epoch_reset():
    state = _prior_epoch_state(regime="bull")
    assert _load_expirer(state)("2026-09-30") is True
    assert state["regime"] == "bull"

# ───────────────────────── authoritative baseline poll ───────────────────────
class _FakeRedis:
    def __init__(self, values=None):
        self.values = dict(values or {})
        self.writes = []
    def get(self, key):
        return self.values.get(key)
    def setex(self, key, ttl, value):
        self.values[key] = str(value)
        self.writes.append((key, ttl, str(value)))
    def set(self, key, value, **kwargs):
        self.values[key] = value
        self.writes.append((key, kwargs, value))


def _load_poll(state, redis_values, account, now_epoch=1_790_740_100.0):
    fake = _FakeRedis(redis_values)
    logs = []
    ns = {
        "_live": state,
        "time": SimpleNamespace(time=lambda: now_epoch),
        "datetime": datetime,
        "timezone": timezone,
        "_live_balance_polled_at": 0.0,
        "_LIVE_POLL_INTERVAL": 300.0,
        "_ig_live_get": lambda *a, **k: {"accounts": [account]},
        "_redis": lambda: fake,
        "_live_log": lambda msg: logs.append(msg),
        "_live_save_state": lambda: None,
        "_live_expire_prior_epoch_global_defensive": _load_expirer(state, logs),
    }
    fn = _exec_function(JUNE_TREE, "_live_poll_balance", ns)
    fn()
    return fake, logs, ns


def _account(available=135.83, balance=135.83, pnl=0.0, deposit=0.0):
    return {"preferred": True, "accountType": "CFD",
            "balance": {"available": available, "balance": balance,
                        "profitLoss": pnl, "deposit": deposit}}


def test_same_day_poll_does_not_reseed_baseline_or_clear_defensive():
    state = _prior_epoch_state(global_mode_entered_at=1_790_740_044.0,
                               global_mode_reference=135.83)
    fake, _, _ = _load_poll(
        state, {"june_balance_day_start:2026-09-30": "135.83"},
        _account(available=120.0, balance=120.0),
    )
    assert state["balance_day_start"] == 135.83
    assert state["balance_day_start_date"] == "2026-09-30"
    assert state["global_mode"] == "defensive"
    assert not [w for w in fake.writes if w[0] == "june_balance_day_start:2026-09-30"]


def test_new_day_poll_restores_authoritative_per_date_baseline_and_expires_old_defensive():
    state = _prior_epoch_state(balance_day_start=148.59,
                               balance_day_start_date="2026-09-29")
    fake, _, _ = _load_poll(
        state, {"june_balance_day_start:2026-09-30": "135.83"},
        _account(available=135.83, balance=135.83),
    )
    assert state["balance_day_start"] == 135.83
    assert state["balance_day_start_date"] == "2026-09-30"
    assert state["global_mode"] == "normal"
    assert state.get("global_mode_reference") is None
    assert fake.values["june_balance_day_start:2026-09-30"] == "135.83"


def test_new_day_first_seed_uses_authoritative_cash_plus_margin_once():
    state = _prior_epoch_state(balance_day_start=148.59,
                               balance_day_start_date="2026-09-29")
    fake, _, _ = _load_poll(state, {}, _account(available=135.83, balance=135.83, deposit=2.0))
    assert state["balance_day_start"] == 137.83
    assert state["balance_day_start_date"] == "2026-09-30"
    assert fake.values["june_balance_day_start:2026-09-30"] == "137.83"
    assert state["global_mode"] == "normal"


def test_open_position_spanning_rollover_is_not_closed_or_repriced():
    position = {"deal_id": "LIVE-1", "instrument": "SILVER", "fill_price": 6100.0}
    state = _prior_epoch_state(open_position=position)
    _load_poll(state, {"june_balance_day_start:2026-09-30": "135.83"}, _account())
    assert state["open_position"] is position
    assert state["open_position"]["fill_price"] == 6100.0
    assert state["global_mode"] == "normal"

# ───────────────────────── circuit breaker invariance ─────────────────────────
def _load_cb(state, enabled=True):
    fake = _FakeRedis()
    logs = []
    ns = {
        "_live": state,
        "_june_live_trading_enabled": enabled,
        "_LIVE_CB_MICRO_THRESH": 30.0,
        "_LIVE_CB_MICRO_FLOOR_USD": 2.0,
        "_LIVE_CB_MICRO_PCT": 0.30,
        "_LIVE_CB_FLOOR_USD": 20.0,
        "_LIVE_CIRCUIT_BREAKER_PCT": -0.05,
        "_redis": lambda: fake,
        "_live_log": lambda msg: logs.append(msg),
    }
    fn = _exec_function(JUNE_TREE, "_live_check_circuit_breaker", ns)
    fn()
    return ns, fake, logs


def test_effective_seedling_cb_remains_approximately_12_percent():
    day = 135.83
    state = {"balance_day_start": day, "balance_total": day * (1.0 - 0.13)}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        ns, fake, _ = _load_cb(state)
    assert ns["_june_live_trading_enabled"] is False
    assert fake.values["june_live_enabled"] == "false"
    # 12% tier floor must be the firing threshold, not the 5% constant alone.
    state2 = {"balance_day_start": day, "balance_total": day * (1.0 - 0.11)}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        ns2, fake2, _ = _load_cb(state2)
    assert ns2["_june_live_trading_enabled"] is True
    assert "june_live_enabled" not in fake2.values


def test_epoch_expiration_does_not_clear_current_day_cb_or_kill_switch_fields():
    state = _prior_epoch_state(june_live_enabled=False,
                               circuit_breaker_fired=True,
                               circuit_breaker_date="2026-09-29")
    _load_expirer(state)("2026-09-30")
    assert state["june_live_enabled"] is False
    assert state["circuit_breaker_fired"] is True
    assert state["circuit_breaker_date"] == "2026-09-29"


def test_same_day_restart_cannot_refresh_cb_allowance():
    state = _prior_epoch_state(
        global_mode_entered_at=1_790_740_044.0,
        global_mode_reference=135.83,
        global_mode_reference_date="2026-09-30",
        june_live_enabled=False,
        circuit_breaker_fired=True)
    before = dict(state)
    assert _load_expirer(state)("2026-09-30") is False  # current-day episode
    assert state == before

# ───────────────────────── domain invariance ───────────────────────────────────
def test_b1_provenance_build4_and_history_survive_rollover():
    state = _prior_epoch_state()
    before = {k: json.dumps(state[k], sort_keys=True) for k in (
        "failed_theses", "trade_history", "settled_primary_keys", "pyramid_legs",
        "rolling_capacity_slot", "rolling_realized_harvest", "cumulative_earned_pnl")}
    _load_expirer(state)("2026-09-30")
    after = {k: json.dumps(state[k], sort_keys=True) for k in before}
    assert after == before


def test_c1_simulation_state_is_outside_live_epoch_mutation():
    live = _prior_epoch_state()
    sim = {"win_moves": {"SILVER_short": [0.001, 0.002]},
           "loss_moves": {}, "evidence_class": "SIM_INVALID_SCALE"}
    before = json.dumps(sim, sort_keys=True)
    _load_expirer(live)("2026-09-30")
    assert json.dumps(sim, sort_keys=True) == before


def test_session_and_macro_metadata_are_preserved():
    state = _prior_epoch_state(session="day", sub_session="nyse_afternoon",
                               macro_regime="neutral", regime_confidence="low")
    _load_expirer(state)("2026-09-30")
    assert state["session"] == "day"
    assert state["sub_session"] == "nyse_afternoon"
    assert state["macro_regime"] == "neutral"
    assert state["regime_confidence"] == "low"


def test_rollover_does_not_create_or_delete_realized_pnl():
    state = _prior_epoch_state(balance=135.83, balance_total=135.83,
                               cumulative_earned_pnl=-120.14)
    before = (state["balance"], state["balance_total"], state["cumulative_earned_pnl"])
    _load_expirer(state)("2026-09-30")
    assert (state["balance"], state["balance_total"], state["cumulative_earned_pnl"]) == before


def test_current_day_defensive_remains_deterministic_after_restart_and_update():
    state = _prior_epoch_state(global_mode_entered_at=1_790_740_044.0,
                               global_mode_reference=135.83,
                               balance_total=124.0)
    _update(state)
    assert state["global_mode"] == "defensive"
    before_reference = state["global_mode_reference"]
    assert _load_expirer(state)("2026-09-30") is False
    assert state["global_mode"] == "defensive"
    assert state["global_mode_reference"] == before_reference


def test_prior_day_reference_cannot_trap_equal_new_day_equity_after_release():
    state = _prior_epoch_state(balance_total=135.83)
    _load_expirer(state)("2026-09-30")
    assert state["global_mode"] == "normal"
    assert state.get("global_mode_reference") is None
    assert _update(state) is None


def test_source_keeps_production_defensive_neutral_policy_and_sessions_unchanged():
    source = JUNE_SOURCE.read_text(encoding="utf-8")
    assert "_live_observe(\"defensive_eval_proceed\"" in source
    assert "defensive-neutral" not in source.lower() or "defensive_eval_proceed" in source
    assert "def _current_sub_session" in source
    assert "session_schema" in source


def test_source_keeps_circuit_breaker_constant_unchanged():
    source = JUNE_SOURCE.read_text(encoding="utf-8")
    assert "_LIVE_CIRCUIT_BREAKER_PCT = -0.05" in source
    assert "_LIVE_CB_FLOOR_USD        = 20.0" in source


def test_source_invokes_epoch_expiration_after_successful_balance_account_path():
    source = JUNE_SOURCE.read_text(encoding="utf-8")
    poll = source[source.index("def _live_poll_balance"):source.index("def _live_poll_pnl")]
    assert "_live_expire_prior_epoch_global_defensive(_today_utc)" in poll
    assert "if not data:" in poll
    assert poll.index("_live_expire_prior_epoch_global_defensive") > poll.index("_live[\"balance_total\"]")
