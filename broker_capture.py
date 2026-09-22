"""Local write-ahead evidence, independent of the replaceable active-state blob.

SQLite commits use synchronous=EXTRA with a rollback journal. No pruning, broker
requests, reconciliation, or learning occurs here. Local disk durability and Redis
persistence are operational prerequisites, not guarantees against disk/host loss.
"""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
import sqlite3
from itertools import islice

from broker_ledger import _key
from broker_pending import _json


class EvidenceCapture:
    def __init__(self, path, store_factory, report):
        self.path, self.store_factory, self.report = str(path), store_factory, report
        self.volatile = {}  # Never silently evict evidence after both sinks fail.

    def _report(self, message):
        try:
            self.report(message)
        except Exception:
            pass  # Even an unavailable logger must not suppress protective exits.

    def _connect(self):
        # Restrictive creation permissions on POSIX; Windows uses directory ACLs.
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        db = sqlite3.connect(self.path, timeout=0.1)
        try:
            db.execute('PRAGMA synchronous=EXTRA')
            db.execute('CREATE TABLE IF NOT EXISTS evidence ('
                       'id TEXT PRIMARY KEY, payload TEXT NOT NULL, '
                       'observed_utc TEXT NOT NULL, forwarded INTEGER NOT NULL DEFAULT 0)')
            db.execute('CREATE INDEX IF NOT EXISTS evidence_pending ON evidence(forwarded)')
            return db
        except Exception:
            db.close()
            raise

    def _append(self, event_id, event):
        with closing(self._connect()) as db, db:
            db.execute('INSERT OR IGNORE INTO evidence(id,payload,observed_utc) VALUES(?,?,?)',
                       (event_id, _json(event), datetime.now(timezone.utc).isoformat()))

    def _forward(self, event):
        account, deal = event['account_id'], event['deal_id']
        if not account or not deal:
            raise ValueError('Unverified opening account/deal; retain local quarantine')
        self.store_factory(account).capture(deal, 'june.lifecycle.' + event['event'], event)

    def capture(self, position, event, *, session_account=None, current_role=None, details=None):
        """Return durable/unresolved status; ordinary failures never escape.

        Only original opening provenance supplies the journal routing account.
        A current session/account or recovered position cannot establish ownership.
        Estimates and disappearance observations remain raw pending evidence.
        """
        try:
            pos = deepcopy(position)
            opening = pos.get('broker_entry_evidence') or {}
            account = opening.get('account_id')
            account_evidence = opening.get('account_evidence') or {}
            proofs = [account_evidence.get(k) or {} for k in ('order', 'confirmation')]
            verified = (bool(account) and all(p.get('verified') is True and
                        p.get('account_id') == account for p in proofs)
                        and opening.get('deal_id') == pos.get('deal_id'))
            envelope = {'schema_version': 1, 'event': event, 'status': 'pending_evidence',
                        'account_id': account if verified else None,
                        'deal_id': pos.get('deal_id'),
                        'observed_session_account': session_account,
                        'current_role': current_role, 'original_role': opening.get('role'),
                        'position': pos, 'details': deepcopy(details or {}),
                        'provenance': 'raw_local_snapshot_not_broker_history'}
            # Validate and detach the complete payload before any state can mutate.
            envelope = json.loads(_json(envelope))
            event_id = _key(envelope)
        except Exception as exc:
            # Keep malformed/non-finite input durably as explicit quarantine,
            # rather than dropping otherwise available evidence on serialization.
            envelope = {'schema_version': 1, 'event': 'serialization_failure',
                        'status': 'unresolved_serialization', 'account_id': None,
                        'deal_id': None, 'requested_event': str(event),
                        'position_repr': repr(position), 'details_repr': repr(details),
                        'error_type': type(exc).__name__}
            event_id = _key(envelope)
            self._report('EVIDENCE UNRESOLVED: serialization failed (' + type(exc).__name__ +
                         '); retaining raw representation in quarantine; protective exit remains enabled')
        try:
            self._append(event_id, envelope)
            self.volatile.pop(event_id, None)
            return 'local_durable'
        except Exception as exc:
            self._report('EVIDENCE STORAGE FAILURE: local journal ' + type(exc).__name__ +
                         '; attempting independent Redis capture')
        try:
            self._forward(envelope)
            self.volatile.pop(event_id, None)
            return 'redis_durable'
        except Exception as exc:
            self.volatile[event_id] = envelope
            self._report('EVIDENCE UNRESOLVED: both durable paths unavailable (' +
                         type(exc).__name__ + '); protective exit remains enabled; '
                         'RAM retry only, process crash may lose evidence; event=' + _json(envelope))
            return 'unresolved'

    def replay(self, limit=10):
        """Bounded retry; no broker access and no activation of recovered trades.

        Redis acknowledgements may be lost. Mark forwarded only after success;
        retrying the exact immutable event cannot duplicate a ledger event.
        Quarantined rows remain on disk; they cannot block verified rows.
        """
        if type(limit) is not int or limit <= 0:
            raise ValueError('Positive replay limit required')
        for event_id in list(islice(self.volatile, limit)):
            event = self.volatile[event_id]
            if not isinstance(event, dict):
                continue
            try:
                self._append(event_id, event)
                del self.volatile[event_id]
            except Exception:
                try:
                    self._forward(event)  # Redis may recover while disk is still down.
                    del self.volatile[event_id]
                except Exception:
                    break
        try:
            with closing(self._connect()) as db:
                # Quarantine uses forwarded=-1, meaning retained, NOT delivered.
                rows = db.execute('SELECT id,payload FROM evidence WHERE forwarded=0 '
                                  'ORDER BY rowid LIMIT ?', (limit,)).fetchall()
                for event_id, raw in rows:
                    event = json.loads(raw)
                    if not event['account_id'] or not event['deal_id']:
                        with db:
                            db.execute('UPDATE evidence SET forwarded=-1 WHERE id=?', (event_id,))
                        self._report('EVIDENCE QUARANTINED: opening account/deal unverified; '
                                     'retained locally for C2c identity recovery')
                        continue
                    self._forward(event)
                    with db:
                        db.execute('UPDATE evidence SET forwarded=1 WHERE id=?', (event_id,))
        except Exception as exc:
            self._report('EVIDENCE REPLAY PENDING: ' + type(exc).__name__ + '; retained for retry')

    def retained(self):
        """Explicit forensic enumeration, not called on a risk/update path."""
        with closing(self._connect()) as db:
            for event_id, payload, observed, forwarded in db.execute(
                    'SELECT id,payload,observed_utc,forwarded FROM evidence ORDER BY rowid'):
                yield {'id': event_id, 'evidence': json.loads(payload),
                       'first_observed_utc': observed, 'forwarded': forwarded}
