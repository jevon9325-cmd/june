"""Affirmative local-state evidence for restart and startup. No trading policy."""
import json
import math
import sqlite3
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path


class StateRecoveryRequired(RuntimeError):
    """Missing/unverifiable history must not become a fresh account."""


@dataclass(frozen=True)
class Assessment:
    kind: str
    reason: str
    state: dict | None = None


NUMBERS = ('balance', 'balance_total', 'balance_margin', 'balance_day_start',
           'cumulative_earned_pnl', 'total_trades', 'total_wins', 'total_losses',
           'long_pnl', 'short_pnl', 'long_trades', 'short_trades', 'long_wins',
           'short_wins', 'skimmed_total', 'skim_pending', 'live_phase',
           'live_phase_trades', 'live_phase_wins', 'live_phase_losses',
           'live_phase_consec_losses', 'global_mode_bal_entry',
           'global_mode_entered_at', 'next_skim_threshold', 'last_half_skim_time')
MAPS = ('boost_expiry', 'pause_expiry', 'streak_state', 'instrument_mode',
        'instrument_mode_entered_at', 'instrument_stopouts_today',
        'instrument_won_after_def', 'failed_theses')
LISTS = ('pnl_seen_refs', 'trade_history', 'failed_thesis_history', 'pyramid_legs')


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def assess(raw):
    if raw is None:
        return Assessment('MISSING', 'live-state object absent')
    try:
        state = json.loads(raw)
    except (ValueError, TypeError, UnicodeError):
        return Assessment('ERROR', 'live-state object unreadable')
    if not isinstance(state, dict):
        return Assessment('UNKNOWN', 'live-state object is not a mapping')
    def invalid_number(value):
        if isinstance(value, float):
            return not math.isfinite(value)
        if isinstance(value, dict):
            return any(invalid_number(v) for v in value.values())
        if isinstance(value, list):
            return any(invalid_number(v) for v in value)
        return False
    if invalid_number(state) or any(not finite(state.get(k)) for k in NUMBERS):
        return Assessment('UNKNOWN', 'required risk/accounting number absent or invalid')
    if any(not isinstance(state.get(k), dict) for k in MAPS):
        return Assessment('UNKNOWN', 'required admission/learning map absent or invalid')
    if any(not isinstance(state.get(k), list) for k in LISTS):
        return Assessment('UNKNOWN', 'required history/exposure list absent or invalid')
    if (state.get('global_mode') not in ('normal', 'defensive')
            or not isinstance(state.get('balance_day_start_date'), str)
            or state.get('balance_day_start', 0) <= 0
            or 'open_position' not in state or 'pyramid_agg_stop_level' not in state
            or 'live_phase_entry_balance' not in state
            or state.get('skim_phase') not in ('pre300', 'h100', 'half')):
        return Assessment('UNKNOWN', 'required baseline/mode/exposure evidence absent')
    try:
        datetime.strptime(state['balance_day_start_date'], '%Y-%m-%d')
    except ValueError:
        return Assessment('UNKNOWN', 'baseline epoch invalid')
    positions = ([state['open_position']] if state['open_position'] is not None else []) + state['pyramid_legs']
    for pos in positions:
        if (not isinstance(pos, dict) or not isinstance(pos.get('deal_id'), str)
                or not pos['deal_id'] or pos.get('direction') not in ('long', 'short')
                or not finite(pos.get('ig_size')) or pos['ig_size'] <= 0
                or not finite(pos.get('fill_price')) or pos['fill_price'] <= 0):
            return Assessment('UNKNOWN', 'position identity/quantity unreadable')
    if any(state.get(k) for k in ('primary', 'addons', 'entry_pending', 'pending_close',
                                  'pending_exit', 'pending_order', 'pyramid_entry_pending', 'pending_submission',
                                  'orphan_suspected', 'manual_review_required',
                                  'recovery_unresolved_positions')):
        return Assessment('UNKNOWN', 'pending/orphan/recovery ambiguity', state)
    reservation = state.get('rolling_fuel_reservation')
    if reservation and (not isinstance(reservation, dict)
                        or reservation.get('state') not in ('RELEASED', 'SETTLED', 'HARVESTED', 'CLOSED_LOSS')):
        return Assessment('UNKNOWN', 'unresolved fuel reservation', state)
    return Assessment('KNOWN_EXPOSED' if positions else 'KNOWN_FLAT',
                      'affirmative persisted exposure and risk state', state)


def read_state(reader):
    try:
        return assess(reader())
    except Exception:
        return Assessment('ERROR', 'local-state read failed')


def inventory(rows):
    if not isinstance(rows, list):
        return None
    ids = []
    for row in rows:
        pos = row.get('position') if isinstance(row, dict) else None
        if (not isinstance(pos, dict) or not isinstance(pos.get('dealId'), str)
                or not pos['dealId'] or not finite(pos.get('size')) or pos['size'] <= 0
                or pos.get('direction') not in ('BUY', 'SELL')):
            return None
        ids.append(pos['dealId'])
    return ids if len(set(ids)) == len(ids) else None


def restart_gate(local, broker_rows, working_orders):
    ids = inventory(broker_rows)
    if ids is None or not isinstance(working_orders, list):
        return False, 'broker inventory/orders unknown'
    if local.kind != 'KNOWN_FLAT':
        return False, 'local state '+local.kind+': '+local.reason
    if ids or working_orders:
        return False, 'broker exposure or pending orders'
    return True, 'affirmative broker/local flat; local risk/accounting present'


def evaluate_restart_gate(local_reader, broker_reader, orders_reader):
    """Read-only fresh gate; reject local-state changes during broker observation."""
    try:
        raw = local_reader()
        local = assess(raw)
        rows, orders = broker_reader(), orders_reader()
        if local_reader() != raw:
            return False, Assessment('UNKNOWN', 'local state changed during gate'), 'unstable snapshot'
    except Exception:
        return False, Assessment('ERROR', 'gate read failed'), 'gate read failed'
    allowed, reason = restart_gate(local, rows, orders)
    return allowed, local, reason


def historical_activity(root):
    """Read-only; absent stores are fresh, unreadable/unrecognized stores unknown."""
    contracts = {'.live-state-checkpoint.sqlite3': ('checkpoint',),
                 '.settlements.sqlite3': ('settlements',),
                 '.broker-evidence.sqlite3': ('evidence',),
                 'june_decision_ledger.sqlite3': ('decision_cycles',),
                 'campaign_telemetry.sqlite3': ('campaigns',)}
    for name, tables in contracts.items():
        path = Path(root)/name
        if not path.exists():
            continue
        try:
            with sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True) as db:
                if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                    return None
                for table in tables:
                    if db.execute('SELECT 1 FROM '+table+' LIMIT 1').fetchone():
                        return True
        except (sqlite3.Error, OSError):
            return None
    return False


def startup_state(local, *, history, broker_rows=None, working_orders=None, legacy_history=None):
    if local.kind in ('KNOWN_FLAT', 'KNOWN_EXPOSED'):
        return local.state
    # Only proven new installations can use startup defaults. Existing accounts
    # must preserve/reconstruct their state before startup; no silent zeros.
    if (local.kind == 'MISSING' and history is False and legacy_history is False
            and inventory(broker_rows) == [] and working_orders == []):
        return None
    raise StateRecoveryRequired('LIVE STATE RECOVERY REQUIRED: '+local.kind+'; '+local.reason)


def guard_startup(redis_client, root, broker_get):
    from submission_recovery import recover_before_startup
    recover_before_startup(redis_client, root, broker_get)
    local = read_state(lambda: redis_client.get('june_live_state'))
    if local.kind in ('KNOWN_FLAT', 'KNOWN_EXPOSED'):
        from live_state_durability import verify_checkpoint
        verify_checkpoint(redis_client, root, local.state)
        return local.state
    history = historical_activity(root)
    legacy = None
    try:
        legacy = any(redis_client.exists(k) for k in (
            'june_live_trade_history_full',
            'june_balance_day_start:'+datetime.now(timezone.utc).strftime('%Y-%m-%d')))
        legacy = legacy or any(next(iter(redis_client.scan_iter(pattern)), None) is not None
                               for pattern in ('june_perf_delivery:*', 'june_perf_stats:*',
                                               'june_broker_ledger_v1:*'))
    except Exception:
        pass
    positions = orders = None
    if local.kind == 'MISSING' and history is False and legacy is False:
        try:
            positions = broker_get('/positions', version='2')
            orders = broker_get('/workingorders', version='2')
        except Exception:
            pass
    return startup_state(local, history=history, legacy_history=legacy,
                         broker_rows=positions.get('positions') if isinstance(positions, dict) else None,
                         working_orders=orders.get('workingOrders') if isinstance(orders, dict) else None)
