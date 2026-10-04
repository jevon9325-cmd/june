"""Compact account-scoped settlement discovery, independent of Redis history.

Discovery is never economic certification. Legacy close snapshots create only
PROVISIONAL requests, and their uncertain historical learning is quarantined.
SQLite uses the same local durability policy as broker_capture; host/disk loss
still requires backups. Redis remains required for the existing learning sink.
"""
from contextlib import closing
from copy import deepcopy
from datetime import datetime
import json
import os
import sqlite3

from exit_authority import contract, UNKNOWN


def identity(record):
    # A campaign is context: distinct broker deals must never collapse by campaign.
    deal = record.get('deal_id')
    if not isinstance(deal, str) or not deal:
        raise ValueError('Settlement requires a broker deal identity')
    declared = record.get('settlement_identity')
    if declared and declared not in ('deal:' + deal, 'campaign:' + str(record.get('campaign_id'))):
        raise ValueError('Conflicting settlement identity')
    return 'deal:' + deal


class SettlementRegistry:
    def __init__(self, path, account):
        if not account:
            raise ValueError('Verified current account required')
        self.path, self.account = str(path), account

    def connect(self):
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        db = sqlite3.connect(self.path, timeout=0.1)
        try:
            db.execute('PRAGMA synchronous=EXTRA')
            db.execute('CREATE TABLE IF NOT EXISTS settlements ('
                       'account TEXT NOT NULL, identity TEXT NOT NULL, payload TEXT NOT NULL, '
                       'PRIMARY KEY(account,identity))')
            db.execute('CREATE TABLE IF NOT EXISTS discovery_cursors ('
                       'account TEXT NOT NULL, source TEXT NOT NULL, cursor INTEGER NOT NULL, '
                       'PRIMARY KEY(account,source))')
            db.execute('CREATE TABLE IF NOT EXISTS discovery_quarantine ('
                       'account TEXT NOT NULL, source TEXT NOT NULL, source_row INTEGER NOT NULL, '
                       'reason TEXT NOT NULL, PRIMARY KEY(account,source,source_row))')
            db.execute('CREATE TABLE IF NOT EXISTS performance_receipts ('
                       'account TEXT NOT NULL, instrument TEXT NOT NULL, identity TEXT NOT NULL, '
                       'status TEXT NOT NULL, PRIMARY KEY(account,instrument,identity))')
            return db
        except Exception:
            db.close()
            raise

    def _put(self, db, record, *, imported=False):
        if not isinstance(record, dict):
            raise ValueError('Settlement must be an object')
        r = deepcopy(record)
        ident = identity(r)
        opening = r.get('broker_entry_evidence') or {}
        if opening.get('account_id') and opening['account_id'] != self.account:
            raise ValueError('Settlement account mismatch')
        if r.get('settlement_state') not in ('PROVISIONAL', 'CONFIRMED'):
            raise ValueError('Unsupported settlement state')
        if r['settlement_state'] == 'PROVISIONAL' and r.get('dollar_pnl') is not None:
            raise ValueError('Provisional economics are unknown')
        r['settlement_identity'] = ident
        old = db.execute('SELECT payload FROM settlements WHERE account=? AND identity=?',
                         (self.account, ident)).fetchone()
        if old:
            old = json.loads(old[0])
            if imported:
                # Snapshots cannot overwrite durable state, backoff, economics
                # or delivery acknowledgements, even when stale.
                r = old
            elif old['settlement_state'] == 'CONFIRMED':
                if r['settlement_state'] != 'CONFIRMED' or r.get('dollar_pnl') != old.get('dollar_pnl'):
                    raise ValueError('Confirmed economics are immutable')
                if old.get('perf_fed'):
                    r['perf_fed'] = True
            # Unknown historical delivery must not become newly replayable.
            if old.get('discovery_learning_quarantined'):
                r['discovery_learning_quarantined'] = True
        elif imported and r['settlement_state'] == 'CONFIRMED' and 'perf_fed' not in r:
            r['discovery_learning_quarantined'] = True
            r['perf_fed'] = True
        db.execute('INSERT INTO settlements(account,identity,payload) VALUES(?,?,?) '
                   'ON CONFLICT(account,identity) DO UPDATE SET payload=excluded.payload',
                   (self.account, ident, json.dumps(r, allow_nan=False)))
        return r

    def put(self, record, *, imported=False):
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            return self._put(db, record, imported=imported)

    def records(self):
        with closing(self.connect()) as db:
            return [json.loads(row[0]) for row in db.execute(
                'SELECT payload FROM settlements WHERE account=? ORDER BY rowid', (self.account,))]

    def get(self, ident):
        with closing(self.connect()) as db:
            row = db.execute('SELECT payload FROM settlements WHERE account=? AND identity=?',
                             (self.account, ident)).fetchone()
            return json.loads(row[0]) if row else None

    def claim_performance(self, instrument, ident):
        """Write-ahead at-most-once fence for Redis acknowledgement/crash ambiguity.

        A PREPARED receipt is never automatically reissued after uncertain delivery.
        This deliberately favors no replay over inventing delivery success; normal
        Redis atomic stats+identity commits and existing policy remain unchanged.
        """
        with closing(self.connect()) as db, db:
            return db.execute('INSERT OR IGNORE INTO performance_receipts VALUES(?,?,?,?)',
                              (self.account, instrument, ident, 'PREPARED')).rowcount == 1

    def acknowledge_performance(self, instrument, ident):
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT INTO performance_receipts VALUES(?,?,?,?) '
                       'ON CONFLICT(account,instrument,identity) DO UPDATE SET status=excluded.status',
                       (self.account, instrument, ident, 'DELIVERED'))
            row = db.execute('SELECT payload FROM settlements WHERE account=? AND identity=?',
                             (self.account, ident)).fetchone()
            if row:
                rec = json.loads(row[0])
                rec['perf_fed'] = True
                self._put(db, rec)

    def import_evidence(self, path, limit=500):
        """Incremental, account-verified legacy recovery; never reads Redis hash.

        Cursor advances atomically with projections. Raw telemetry is needed only
        for initial legacy bootstrap, not for enumeration of registered identities.
        Invalid/unverified/add-on snapshots cannot certify primary settlements.
        """
        if not os.path.exists(path):
            return
        source = os.path.abspath(path)
        with closing(self.connect()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            cursor = db.execute('SELECT cursor FROM discovery_cursors WHERE account=? AND source=?',
                                (self.account, source)).fetchone()
            cursor = cursor[0] if cursor else 0
            with closing(sqlite3.connect('file:' + source.replace('\\', '/') + '?mode=ro', uri=True)) as raw:
                rows = raw.execute('SELECT rowid,payload,observed_utc FROM evidence WHERE rowid>? '
                                   'ORDER BY rowid LIMIT ?', (cursor, limit)).fetchall()
            for rowid, payload, observed in rows:
                try:
                    event = json.loads(payload)
                    if event.get('account_id') != self.account or event.get('current_role') != 'primary':
                        continue
                    if event.get('event') == 'settlement_created':
                        record = (event.get('details') or {}).get('settlement')
                    elif event.get('event') == 'before_primary_clear':
                        pos = event.get('position') or {}
                        if not pos.get('deal_id') or pos['deal_id'] != event.get('deal_id'):
                            continue
                        record = {**contract(pos), 'deal_id': pos['deal_id'],
                                  'instrument': pos.get('instrument'), 'direction': pos.get('direction'),
                                  'entry_price': pos.get('fill_price'),
                                  'ig_size': pos.get('original_ig_size', pos.get('ig_size')),
                                  'partial_dollar_pnl': pos.get('partial_dollar_pnl', 0.0),
                                  'notional': pos.get('original_notional', pos.get('notional')),
                                  'campaign_id': pos.get('campaign_id') or pos.get('rolling_campaign_id'),
                                  'exit_epoch': int(datetime.fromisoformat(observed).timestamp()),
                                  'exit_reason': 'durable_close_snapshot_recovery',
                                  'settlement_source': 'local_evidence.before_primary_clear',
                                  'settlement_state': 'PROVISIONAL', 'dollar_pnl': None,
                                  'pnl_source': 'unknown_pending_reconciliation',
                                  'reconciled': False, 'evidence_class': 'PROVISIONAL',
                                  'discovery_learning_quarantined': True}
                        if 'exit_authority_schema' not in record:
                            record.update(exit_authority_schema=1, exit_authority=UNKNOWN,
                                          strategy_learning_eligible=False)
                    else:
                        continue
                    if isinstance(record, dict):
                        if record.get('deal_id') != event.get('deal_id'):
                            raise ValueError('Evidence envelope/settlement identity mismatch')
                        self._put(db, record, imported=True)
                except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
                    # Retain the source row and a durable error reference. A bad
                    # historical envelope cannot strand later valid settlements.
                    db.execute('INSERT OR REPLACE INTO discovery_quarantine VALUES(?,?,?,?)',
                               (self.account, source, rowid, type(exc).__name__))
            if rows:
                db.execute('INSERT INTO discovery_cursors VALUES(?,?,?) '
                           'ON CONFLICT(account,source) DO UPDATE SET cursor=excluded.cursor',
                           (self.account, source, rows[-1][0]))


class SettlementView:
    """Identity-stable adapter for the existing broker reconciler's list protocol.

    All writes commit SQLite first. Redis updates are optional cache projections,
    located by identity rather than by potentially shifted list indexes. Missing
    Redis rows are never manufactured for discovery or performance delivery.
    """
    def __init__(self, registry, redis, key):
        self.registry, self.redis, self.key = registry, redis, key
        self.items = registry.records()

    def lrange(self, key, start, end):
        return [json.dumps(r) for r in self.items[start:None if end == -1 else end + 1]]

    def lindex(self, key, index):
        ident = identity(self.items[index])
        record = self.registry.get(ident)
        return json.dumps(record) if record else None

    def lset(self, key, index, raw):
        r = json.loads(raw)
        if identity(r) != identity(self.items[index]):
            raise ValueError('Settlement view identity changed')
        r = self.registry.put(r)
        self.items[index] = r
        try:
            for idx, cached in enumerate(self.redis.lrange(self.key, 0, -1) or []):
                try:
                    match = identity(json.loads(cached)) == identity(r)
                except (ValueError, TypeError):
                    continue
                if match:
                    # Lua CAS: never overwrite a new LPUSH occupant or newer row.
                    self.redis.eval("if redis.call('LINDEX',KEYS[1],ARGV[1]) == ARGV[2] then "
                                    "return redis.call('LSET',KEYS[1],ARGV[1],ARGV[3]) end return 0",
                                    1, self.key, idx, cached, json.dumps(r))
        except Exception:
            pass  # cache failure cannot roll back durable reconciliation


def commit_performance_once(registry, redis, key, previous, stats, observation_id):
    """Keep existing atomic Redis learning contract, with a local delivery fence.

    PREPARED plus a lost Redis acknowledgement is explicitly uncertain. If Redis
    has the permanent identity we can acknowledge; otherwise leave it pending and
    refuse automatic replay. A definitive CAS rejection is safely retryable.
    """
    from exit_authority import commit_performance, identity as digest, PerformanceWriteRejected
    if not observation_id:
        return commit_performance(redis, key, previous, stats, observation_id)
    instrument = key.removeprefix('june_perf_stats:')
    marker = 'june_perf_delivery:' + digest([key, observation_id])
    record = registry.get(observation_id)
    if record and (record.get('perf_fed') or record.get('discovery_learning_quarantined')):
        return False
    if redis.get(marker):
        registry.acknowledge_performance(instrument, observation_id)
        return False
    if not registry.claim_performance(instrument, observation_id):
        with closing(registry.connect()) as db:
            status = db.execute('SELECT status FROM performance_receipts WHERE account=? '
                                'AND instrument=? AND identity=?',
                                (registry.account, instrument, observation_id)).fetchone()[0]
        if status == 'DELIVERED':
            return False
        raise RuntimeError('Performance delivery acknowledgement uncertain; replay quarantined')
    try:
        committed = commit_performance(redis, key, previous, stats, observation_id)
    except RuntimeError as exc:
        if (isinstance(exc, PerformanceWriteRejected)
                or str(exc) == 'Performance history changed concurrently; replay required'):
            with closing(registry.connect()) as db, db:
                db.execute('DELETE FROM performance_receipts WHERE account=? AND instrument=? '
                           'AND identity=? AND status=?',
                           (registry.account, instrument, observation_id, 'PREPARED'))
        raise
    registry.acknowledge_performance(instrument, observation_id)
    return committed
