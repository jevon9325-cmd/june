"""Build 4C-A2 focused tests: canonical primary-exit settlement contract.

Offline AST extraction of the production settlement functions (no bot import,
no external I/O). Exercises provenance (CONFIRMED/PROVISIONAL/UNKNOWN),
idempotency, direction preservation, adaptive-consumer feed rules, and that
UNKNOWN never becomes 0.
"""
import json
from types import SimpleNamespace
from unittest.mock import Mock
from test_broker_identity import execute, function


def _fresh_redis():
    m = Mock()
    m.lrange = Mock(return_value=[])   # durable-history idempotency backstop: empty
    return m


def _ns(*, equity_cfd=None, redis_mock=None):
    live = {"balance": 139.31, "trade_history": [], "settled_primary_keys": []}
    perf_calls = []

    def _perf(sym, won, sar, pnl_dollar=0.0, entry_sar=None, persistence_confirmed=None,
              excluded_defect_id=None):
        perf_calls.append({"sym": sym, "won": won, "pnl_dollar": pnl_dollar})

    observe_calls = []

    ns = dict(
        _live=live, json=json,
        time=SimpleNamespace(time=lambda: 100000.0),
        _live_lot_sizes={}, _LIVE_LOT_SIZE_FX=1.0,
        _live_equity_cfd=(equity_cfd or set()),
        _IG_EQUITY_COMMISSION_USD=9.0,
        _LIVE_TRADE_HIST_KEY="june_live_trade_history_full",
        _LIVE_TRADE_HIST_CAP=2000, _LIVE_TRADE_HIST_TTL=1000,
        _redis=Mock(return_value=(redis_mock or _fresh_redis())),
        _live_save_state=Mock(),
        _live_perf_record=_perf,
        _live_observe=lambda *a, **k: observe_calls.append((a, k)),
        _live_log=Mock(),
    )
    for fn in ("_live_settlement_key", "_live_already_settled", "_live_mark_settled",
               "_live_settle_primary_exit"):
        execute([function(fn)], ns)
    ns["_perf_calls"] = perf_calls
    ns["_observe_calls"] = observe_calls
    return ns


def _pos(**kw):
    base = dict(instrument="GOLD", direction="long", deal_id="DEAL1",
                fill_price=4000.0, ig_size=0.1, notional=400.0,
                original_notional=400.0, entry_time=99000.0, conviction=7)
    base.update(kw)
    return base


def _last(ns):
    return ns["_live"]["trade_history"][-1]


# ── Provenance ───────────────────────────────────────────────────────────────

def test_confirmed_pnl_from_broker_profit():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(), "broker_ls_fully_closed",
                                    source="ls_fully_closed", confirmed_pnl=-1.08)
    r = _last(ns)
    assert r["settlement_state"] == "CONFIRMED"
    assert r["dollar_pnl"] == -1.08
    assert r["pnl_source"] == "broker_confirmed_realized_pnl"


def test_confirmed_from_exit_price_fill_estimate():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(direction="short", fill_price=4000.0),
                                    "broker_side_disappearance",
                                    source="reconciliation.flat_check",
                                    confirmed_exit_price=3960.0)
    r = _last(ns)
    assert r["settlement_state"] == "CONFIRMED"
    # short: (4000-3960)/4000 * 400 = +4.0
    assert abs(r["dollar_pnl"] - 4.0) < 1e-6
    assert r["exit_price"] == 3960.0


def test_provisional_when_no_broker_truth_pnl_is_none_not_zero():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(), "broker_side_disappearance",
                                    source="reconciliation.flat_check")
    r = _last(ns)
    assert r["settlement_state"] == "PROVISIONAL"
    assert r["dollar_pnl"] is None          # UNKNOWN, never 0
    assert r["pnl_pct"] is None
    assert r["exit_price"] is None
    assert r["reconciled"] is False


def test_unknown_never_becomes_zero_explicit():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(), "x", source="s")
    r = _last(ns)
    assert r["dollar_pnl"] is not None or r["dollar_pnl"] is None
    assert r["dollar_pnl"] is None  # explicitly None, not 0/0.0
    assert r["dollar_pnl"] != 0


# ── Direction / sign preservation ──────────────────────────────────────────────

def test_profitable_broker_exit_stays_positive():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(), "dple", source="ls_fully_closed",
                                    confirmed_pnl=2.40)
    assert _last(ns)["dollar_pnl"] == 2.40


def test_losing_broker_stop_stays_negative():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(), "stop_loss", source="ls_fully_closed",
                                    confirmed_pnl=-0.96)
    assert _last(ns)["dollar_pnl"] == -0.96


def test_direction_preserved_long():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(direction="long"), "x", source="s",
                                    confirmed_pnl=1.0)
    assert _last(ns)["direction"] == "long"


def test_direction_preserved_short():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(direction="short"), "x", source="s",
                                    confirmed_pnl=1.0)
    assert _last(ns)["direction"] == "short"


def test_broker_exit_price_preferred_over_none():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(), "x", source="s",
                                    confirmed_pnl=1.5, confirmed_exit_price=4010.0)
    assert _last(ns)["exit_price"] == 4010.0


# ── Idempotency ────────────────────────────────────────────────────────────────

def test_idempotent_same_deal_settles_once():
    ns = _ns()
    p = _pos(deal_id="DUP1")
    ns["_live_settle_primary_exit"](p, "x", source="s", confirmed_pnl=1.0)
    ns["_live_settle_primary_exit"](p, "x", source="s", confirmed_pnl=1.0)
    assert len(ns["_live"]["trade_history"]) == 1


def test_dealless_position_settles_once_by_composite():
    ns = _ns()
    p = _pos(deal_id="")
    ns["_live_settle_primary_exit"](dict(p), "x", source="s")
    ns["_live_settle_primary_exit"](dict(p), "x", source="s")
    assert len(ns["_live"]["trade_history"]) == 1


def test_mark_settled_blocks_future_close_double_record():
    ns = _ns()
    p = _pos(deal_id="D2")
    ns["_live_mark_settled"](p)              # simulate June-DELETE path marking
    ns["_live_settle_primary_exit"](p, "x", source="reconcile")
    assert len(ns["_live"]["trade_history"]) == 0  # reconcile no-ops


# ── Adaptive-consumer feed rules ───────────────────────────────────────────────

def test_confirmed_feeds_perf_record_once():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(), "x", source="s", confirmed_pnl=-1.0)
    assert len(ns["_perf_calls"]) == 1
    assert ns["_perf_calls"][0]["won"] is False


def test_provisional_does_not_feed_perf_record():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(), "x", source="s")  # UNKNOWN
    assert len(ns["_perf_calls"]) == 0   # never train on UNKNOWN


def test_settlement_emits_campaign_telemetry():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(), "x", source="s", confirmed_pnl=1.0)
    events = [a[0] for (a, k) in ns["_observe_calls"] if a]
    assert "primary_settled" in events


# ── Commission handling on equity CFDs ─────────────────────────────────────────

def test_durable_backstop_dedups_via_redis_history():
    # Simulate a crash-then-restart: settled_primary_keys empty, but the deal is
    # already in the durable Redis history -> must be treated as settled (no dup).
    rmock = _fresh_redis()
    rmock.lrange = Mock(return_value=[json.dumps({"deal_id": "CRASH1"})])
    ns = _ns(redis_mock=rmock)
    ns["_live_settle_primary_exit"](_pos(deal_id="CRASH1"), "x",
                                    source="reconciliation.flat_check", confirmed_pnl=1.0)
    assert len(ns["_live"]["trade_history"]) == 0  # deduped by durable history


def test_settlement_persists_state_before_return():
    ns = _ns()
    ns["_live_settle_primary_exit"](_pos(), "x", source="s", confirmed_pnl=1.0)
    assert ns["_live_save_state"].called  # durable persist inside settle


def test_equity_commission_applied_on_fill_estimate():
    ns = _ns(equity_cfd={"AAL"})
    # long AAL, exit above entry; gross then minus $18 round-trip
    ns["_live_settle_primary_exit"](_pos(instrument="AAL", direction="long",
                                         fill_price=100.0, notional=400.0,
                                         original_notional=400.0),
                                    "x", source="s", confirmed_exit_price=110.0)
    r = _last(ns)
    # gross = (110-100)/100 * 400 = 40 ; minus 18 commission = 22
    assert abs(r["dollar_pnl"] - 22.0) < 1e-6
    assert r["commission"] == 18.0
