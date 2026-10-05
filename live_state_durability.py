"""Durable live-state checkpoints; Redis loss never authorizes stale replay.

The current checkpoint and UTC baselines are committed with SQLite FULL sync
before Redis writes. Recovery from a missing/divergent live key remains an
explicit recovery incident, not automatic reconstruction from an old image.
"""
import hashlib
import json
import math
from pathlib import Path
import sqlite3

NAME = '.live-state-checkpoint.sqlite3'


def _connect(root):
    db = sqlite3.connect(str(Path(root) / NAME), timeout=5)
    db.execute('PRAGMA synchronous=FULL')
    db.execute('CREATE TABLE IF NOT EXISTS checkpoint '
               '(id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL, sha256 TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS baselines '
               '(day TEXT PRIMARY KEY, value REAL NOT NULL CHECK(value>0))')
    return db


def _reader(root):
    path = Path(root) / NAME
    if not path.exists():
        return None
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)
    if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
        db.close()
        raise RuntimeError('Live-state checkpoint integrity failure')
    return db


def _baseline(db, day, value):
    from datetime import datetime
    datetime.strptime(day, '%Y-%m-%d')
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or value <= 0):
        raise ValueError('Invalid authoritative daily baseline')
    prior = db.execute('SELECT value FROM baselines WHERE day=?', (day,)).fetchone()
    if prior and not math.isclose(prior[0], value, abs_tol=1e-9, rel_tol=0):
        raise RuntimeError('Conflicting authoritative daily baseline')
    db.execute('INSERT OR IGNORE INTO baselines VALUES(?,?)', (day, value))


def checkpoint(root, state):
    if not isinstance(state, dict):
        raise ValueError('Live checkpoint must be a mapping')
    payload = json.dumps(state, sort_keys=True, separators=(',', ':'), allow_nan=False)
    db = _connect(root)
    try:
        with db:
            if state.get('balance_day_start_date') and state.get('balance_day_start', 0) > 0:
                _baseline(db, state['balance_day_start_date'], state['balance_day_start'])
            db.execute('INSERT OR REPLACE INTO checkpoint VALUES(1,?,?)',
                       (payload, hashlib.sha256(payload.encode()).hexdigest()))
    finally:
        db.close()
    return payload


def read_checkpoint(root):
    db = _reader(root)
    if db is None:
        return None
    try:
        row = db.execute('SELECT payload,sha256 FROM checkpoint WHERE id=1').fetchone()
        if row is None:
            return None
        if hashlib.sha256(row[0].encode()).hexdigest() != row[1]:
            raise RuntimeError('Live-state checkpoint digest failure')
        state = json.loads(row[0])
        if not isinstance(state, dict):
            raise RuntimeError('Live-state checkpoint is not a mapping')
        return state
    finally:
        db.close()


def read_baseline(root, day):
    db = _reader(root)
    if db is None:
        return None
    try:
        row = db.execute('SELECT value FROM baselines WHERE day=?', (day,)).fetchone()
        return row[0] if row else None
    finally:
        db.close()


def persist_state(redis_client, root, state):
    payload = checkpoint(root, state)
    # Critical working state must not be eligible for volatile-LRU eviction.
    # SET without expiry also removes an inherited TTL atomically.
    if not redis_client.set('june_live_state', payload):
        raise RuntimeError('Live-state Redis write not acknowledged')


def persist_baseline(redis_client, root, day, value):
    db = _connect(root)
    try:
        with db:
            _baseline(db, day, value)
    finally:
        db.close()
    # This key is a cache of the committed baseline and full live checkpoint.
    try:
        redis_client.setex('june_balance_day_start:' + day, 36 * 3600, str(value))
    except Exception as exc:
        import logging
        logging.warning('Daily-baseline Redis mirror failed: %s', type(exc).__name__)


def baseline_for_day(redis_client, root, day, state):
    """Resolve existing truth; None means a genuine unseeded UTC epoch only."""
    from live_state_integrity import StateRecoveryRequired
    try:
        raw = redis_client.get('june_balance_day_start:' + day)
        saved = read_baseline(root, day)
        embedded = (state.get('balance_day_start')
                    if state.get('balance_day_start_date') == day else None)
        values = [float(v) for v in (raw, saved, embedded) if v is not None]
        if any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError('Invalid baseline')
        if values and any(not math.isclose(v, values[0], abs_tol=1e-9, rel_tol=0) for v in values):
            raise ValueError('Conflicting baseline evidence')
        return values[0] if values else None
    except Exception as exc:
        raise StateRecoveryRequired('Daily baseline evidence unreadable/conflicting') from exc


def verify_checkpoint(redis_client, root, state):
    from datetime import datetime, timezone
    from live_state_integrity import StateRecoveryRequired
    try:
        prior = read_checkpoint(root)
        if prior is not None and prior != state:
            raise ValueError('Checkpoint and Redis diverge; no stale replay')
        baseline_for_day(redis_client, root, datetime.now(timezone.utc).strftime('%Y-%m-%d'), state)
    except StateRecoveryRequired:
        raise
    except Exception as exc:
        raise StateRecoveryRequired('Live checkpoint unreadable/divergent') from exc
