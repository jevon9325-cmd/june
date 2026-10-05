"""June-local, best-effort primary-decision evidence. No trading decisions or I/O
on the caller thread. Data is captured at existing evaluation points, never by
evaluating an unvisited candidate. SQLite and content hashing run in one worker.
"""
import collections
import ast
from functools import lru_cache
import contextlib
import hashlib
import json
import logging
import os
from pathlib import Path
import queue
import sqlite3
import sys
import threading
import time
import traceback
import uuid
import zlib

SCHEMA_VERSION = 1
MAX_DOCUMENT_BYTES = 1024 * 1024
MAX_COPY_BUDGET = 4 * MAX_DOCUMENT_BYTES
MAX_CONTEXT_BYTES = 128 * 1024
MAX_EVENTS = 4096
MAX_CANDIDATES = 256
DEFAULT_MAX_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_CYCLES = 100000
DEFAULT_RETENTION = 90 * 86400

STATE_FIELDS = ('balance', 'balance_total', 'balance_fetched_at', 'balance_day_start',
                'balance_day_start_date', 'skimmed_total', 'global_mode',
                'global_mode_reference', 'instrument_mode', 'instrument_cooldown',
                'pause_expiry', 'live_phase', 'orphan_suspected', 'manual_review_required')
MEMORY_CONTEXT = ('_claudia_directive_notes', '_claudia_corr_notes', '_correlation_map',
                  '_barbie_overrides', '_barbie_combo_thresholds', '_sim_regime_three_way')
SCALARS = frozenset(('sym direction direction_str signal_dir regime combo bal total skimmed '
                    'vol chg thresh weight corr_adj eff_vol gate_mode rel_score conv '
                    '_atr5 _atr5_fb _sp_raw _sar5 _thr5 price_1m cur_px rev_pct change_15m '
                    'has_15m blocked _gmode _imode _in_defensive _observer_key _obs_floor '
                    '_def_floor _eff_conv_floor _htf_b _htf_m _htf_opp_blocked _ex_ratio '
                    '_cv_lev _phase_ceil lev _tier_pct pos_size notional _sar_live _scale '
                    '_macro_scale _claudia_dir _conf_note _compress_sl _md_mid _md_lot '
                    '_md_mdl _md_formula _md_ratio mid_price leverage conviction ig_size '
                    'lot_sz price_unit actual_n _log_n _eff_lev _spread_floor stop_pct '
                    'stop_mult stop_dist tp_pct _exp_gross _rt_comm _margin_raw _mfrac '
                    '_eq_fx _usd_n _req_mg _avail _fresh_mid _drift fill_price deal_id '
                    'deal_ref status margin_rate min_n clearance raw _final_conv '
                    '_cap_ok _cap_reason _md_ratio _notional_skip _skip').split())


def freeze(value, depth=0, budget=None):
    """Private immutable-compatible copy; never calls repr/custom serializers."""
    if budget is None:budget=[MAX_COPY_BUDGET]
    budget[0]-=32 + (len(value)*4 if type(value) is str else 0)
    if budget[0]<0:raise ValueError('decision evidence copy limit')
    if depth > 16:
        raise ValueError('decision evidence nesting limit')
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        if not (-float('inf') < value < float('inf')):
            raise ValueError('nonfinite decision evidence')
        return value
    if type(value) is dict:
        return {str(k): freeze(v, depth + 1,budget) for k, v in value.items() if type(k) in (str, int)}
    if type(value) in (tuple, list, set, frozenset, collections.deque):
        return [freeze(v, depth + 1,budget) for v in value]
    raise TypeError('unsupported decision evidence type')


@lru_cache(maxsize=512)
def state_keys(expression):
    """Literal keys actually read by a recorded predicate; never evaluate it."""
    try:
        tree=ast.parse(expression,mode='eval');keys=set();covered=set()
        for node in ast.walk(tree):
            if (isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute)
                    and isinstance(node.func.value,ast.Name) and node.func.value.id=='_live'
                    and node.func.attr=='get' and node.args and isinstance(node.args[0],ast.Constant)
                    and isinstance(node.args[0].value,str)):
                keys.add(node.args[0].value);covered.add(id(node.func.value))
            if (isinstance(node,ast.Subscript) and isinstance(node.value,ast.Name)
                    and node.value.id=='_live' and isinstance(node.slice,ast.Constant)
                    and isinstance(node.slice.value,str)):
                keys.add(node.slice.value);covered.add(id(node.value))
        if any(isinstance(n,ast.Name) and n.id=='_live' and id(n) not in covered for n in ast.walk(tree)):
            return None
        return tuple(sorted(keys))
    except (ValueError,SyntaxError,TypeError):return None


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode('utf-8')


def unpack(value):
    return json.loads(zlib.decompress(value))


class Store:
    """Worker/offline API only. Never invoked on June's execution thread."""
    def __init__(self, path, *, max_bytes=DEFAULT_MAX_BYTES,
                 max_cycles=DEFAULT_MAX_CYCLES, retention=DEFAULT_RETENTION):
        self.path = Path(path)
        self.max_bytes, self.max_cycles, self.retention = max_bytes, max_cycles, retention

    def connect(self):
        db = sqlite3.connect(self.path, timeout=0)
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA busy_timeout=0')
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA wal_autocheckpoint=256')
        db.execute('PRAGMA journal_size_limit=1048576')
        page = db.execute('PRAGMA page_size').fetchone()[0]
        db.execute(f'PRAGMA max_page_count={max(32, int(self.max_bytes * .9) // page)}')
        db.executescript('''
        CREATE TABLE IF NOT EXISTS decision_cycles(
          decision_cycle_id TEXT PRIMARY KEY, at REAL NOT NULL, account TEXT,
          runtime_id TEXT, commit_id TEXT, strategy_config_id TEXT,
          strategic_context_id TEXT, gate_catalog_id TEXT, selected_candidate TEXT, terminal_state TEXT,
          complete INTEGER NOT NULL DEFAULT 0, pinned INTEGER NOT NULL DEFAULT 0,
          terminal_seq INTEGER, payload BLOB NOT NULL);
        CREATE INDEX IF NOT EXISTS decision_cycle_time ON decision_cycles(at);
        CREATE INDEX IF NOT EXISTS decision_cycle_selected ON decision_cycles(selected_candidate);
        CREATE INDEX IF NOT EXISTS decision_cycle_context ON decision_cycles(strategic_context_id);
        CREATE TABLE IF NOT EXISTS decision_candidates(
          decision_cycle_id TEXT REFERENCES decision_cycles ON DELETE CASCADE,
          candidate_id TEXT, instrument TEXT, direction TEXT, initial_rank INTEGER,
          raw_score REAL, final_score REAL, payload BLOB NOT NULL,
          PRIMARY KEY(decision_cycle_id,candidate_id));
        CREATE INDEX IF NOT EXISTS decision_candidate_instrument ON decision_candidates(instrument);
        CREATE TABLE IF NOT EXISTS decision_events(
          decision_cycle_id TEXT REFERENCES decision_cycles ON DELETE CASCADE,
          sequence INTEGER, at REAL, kind TEXT, candidate_id TEXT, gate_name TEXT,
          result TEXT, payload BLOB NOT NULL,
          PRIMARY KEY(decision_cycle_id,sequence));
        CREATE INDEX IF NOT EXISTS decision_event_candidate ON decision_events(decision_cycle_id,candidate_id);
        CREATE TABLE IF NOT EXISTS decision_gate_catalog(
          gate_catalog_id TEXT, gate_name TEXT, catalog_order INTEGER, payload BLOB,
          PRIMARY KEY(gate_catalog_id,gate_name));
        CREATE VIEW IF NOT EXISTS decision_gate_events AS
          SELECT e.decision_cycle_id,e.candidate_id,e.gate_name,e.sequence,e.result,
                 NULL AS catalog_order,e.payload FROM decision_events e WHERE e.kind='GATE'
          UNION ALL
          SELECT c.decision_cycle_id,c.candidate_id,g.gate_name,NULL,'NOT_REACHED',
                 g.catalog_order,NULL
          FROM decision_candidates c JOIN decision_cycles d USING(decision_cycle_id)
          JOIN decision_gate_catalog g ON g.gate_catalog_id=d.gate_catalog_id
          WHERE NOT EXISTS(SELECT 1 FROM decision_events e
            WHERE e.decision_cycle_id=c.decision_cycle_id AND e.candidate_id=c.candidate_id
            AND e.kind='GATE' AND e.gate_name=g.gate_name);
        CREATE TABLE IF NOT EXISTS strategic_context_snapshots(
          strategic_context_id TEXT PRIMARY KEY, payload BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS strategic_artifacts(
          artifact_hash TEXT PRIMARY KEY, artifact_type TEXT, artifact_id TEXT,
          artifact_version TEXT, source_timestamp TEXT, source_file TEXT,
          folder_ref TEXT, chart_ref TEXT, producer_content_hash TEXT, payload BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS decision_strategic_refs(
          decision_cycle_id TEXT REFERENCES decision_cycles ON DELETE CASCADE,
          sequence INTEGER, artifact_key TEXT, artifact_hash TEXT REFERENCES strategic_artifacts,
          observed_at REAL, ttl REAL, consumed INTEGER, payload BLOB NOT NULL,
          PRIMARY KEY(decision_cycle_id,sequence,artifact_key));
        CREATE INDEX IF NOT EXISTS decision_context_refs ON decision_strategic_refs(artifact_hash);
        CREATE TABLE IF NOT EXISTS decision_outcomes(
          decision_cycle_id TEXT REFERENCES decision_cycles ON DELETE CASCADE,
          sequence INTEGER, account TEXT, deal_id TEXT, deal_reference TEXT,
          campaign_id TEXT, status TEXT, payload BLOB NOT NULL,
          PRIMARY KEY(decision_cycle_id,sequence));
        CREATE INDEX IF NOT EXISTS decision_outcome_deal ON decision_outcomes(account,deal_id);
        CREATE INDEX IF NOT EXISTS decision_outcome_campaign ON decision_outcomes(campaign_id);
        CREATE TABLE IF NOT EXISTS decision_retention_receipts(
          decision_cycle_id TEXT PRIMARY KEY, archived_at REAL, archive_hash TEXT,
          settlement_identity TEXT, proof BLOB NOT NULL);
        ''')
        return db

    def disk_bytes(self):
        return sum(p.stat().st_size for p in (self.path, Path(str(self.path) + '-wal')) if p.exists())

    def persist(self, document):
        raw = encoded(document)
        if len(raw) > MAX_DOCUMENT_BYTES:
            raise ValueError('decision evidence document limit')
        if self.disk_bytes() >= self.max_bytes:
            raise OSError('decision ledger capacity; pinned evidence preserved')
        with contextlib.closing(self.connect()) as db, db:
            cid = document['decision_cycle_id']
            existing = db.execute('SELECT terminal_seq FROM decision_cycles WHERE decision_cycle_id=?', (cid,)).fetchone()
            if existing and existing[0] is not None:
                return  # terminal documents are immutable and replay-idempotent
            self.prune(db, document['at'])
            if not existing and db.execute('SELECT COUNT(*) FROM decision_cycles').fetchone()[0] >= self.max_cycles:
                raise OSError('decision ledger cycle cap; unresolved evidence preserved')
            contexts = []
            for ref in document['strategic_refs']:
                body = encoded(ref['body'])
                if len(body) > MAX_CONTEXT_BYTES:
                    raise ValueError('strategic context capacity')
                aid = hashlib.sha256(encoded([ref['artifact_key'], ref['body']])).hexdigest()
                content = ref['body'] if type(ref['body']) is dict else {}
                # Preserve producer-provided IDs; never manufacture Forecast Folder links.
                db.execute('INSERT OR IGNORE INTO strategic_artifacts VALUES(?,?,?,?,?,?,?,?,?,?)',
                           (aid, ref['artifact_type'], content.get('artifact_id'),
                            str(content.get('version')) if content.get('version') is not None else None,
                            str(content.get('timestamp',content.get('generated_at'))) if content.get('timestamp',content.get('generated_at')) is not None else None,
                            content.get('source_file'),content.get('folder_ref'),content.get('chart_ref'),content.get('content_hash'),
                            zlib.compress(body)))
                contexts.append([ref['artifact_key'], aid, ref['sequence']])
            context_id = hashlib.sha256(encoded(contexts)).hexdigest()
            db.execute('INSERT OR IGNORE INTO strategic_context_snapshots VALUES(?,?)',
                       (context_id, zlib.compress(encoded(contexts))))
            catalog_id=hashlib.sha256(encoded(document['gate_catalog'])).hexdigest()
            for gate in document['gate_catalog']:
                db.execute('INSERT OR IGNORE INTO decision_gate_catalog VALUES(?,?,?,?)',
                           (catalog_id,gate['gate_name'],gate['catalog_order'],zlib.compress(encoded(gate))))
            header = {k: v for k, v in document.items() if k not in ('candidates', 'events', 'strategic_refs', 'outcomes','gate_catalog')}
            header['gate_catalog_id']=catalog_id
            db.execute('INSERT OR IGNORE INTO decision_cycles VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (cid, document['at'], document.get('account'), document['runtime_id'],
                        document.get('commit_id'), document.get('strategy_config_id'), context_id,catalog_id,
                        None, None, 0, 0, None, zlib.compress(encoded(header))))
            for c in document['candidates'].values():
                db.execute('INSERT INTO decision_candidates VALUES(?,?,?,?,?,?,?,?) '
                           'ON CONFLICT(decision_cycle_id,candidate_id) DO UPDATE SET '
                           'direction=excluded.direction,initial_rank=excluded.initial_rank,'
                           'raw_score=excluded.raw_score,final_score=excluded.final_score,payload=excluded.payload',
                           (cid, c['candidate_id'], c['instrument'], c.get('direction'), c.get('rank'),
                            c.get('raw_score'), c.get('final_score'), zlib.compress(encoded(c))))
            for ev in document['events']:
                db.execute('INSERT OR IGNORE INTO decision_events VALUES(?,?,?,?,?,?,?,?)',
                           (cid, ev['sequence'], ev['at'], ev['kind'], ev.get('candidate_id'),
                            ev.get('gate_name'), ev.get('result'), zlib.compress(encoded(ev))))
            for ref, context in zip(document['strategic_refs'], contexts):
                db.execute('INSERT OR IGNORE INTO decision_strategic_refs VALUES(?,?,?,?,?,?,?,?)',
                           (cid, ref['sequence'], ref['artifact_key'], context[1], ref['observed_at'],
                            ref.get('ttl'), int(ref['consumed']), zlib.compress(encoded({k:v for k,v in ref.items() if k != 'body'}))))
            for out in document['outcomes']:
                db.execute('INSERT OR IGNORE INTO decision_outcomes VALUES(?,?,?,?,?,?,?,?)',
                           (cid, out['sequence'], document.get('account'), out.get('deal_id'),
                            out.get('deal_reference'), out.get('campaign_id'), out['status'], zlib.compress(encoded(out))))
            if document.get('terminal_state') is not None:
                db.execute('UPDATE decision_cycles SET strategic_context_id=?,selected_candidate=?,terminal_state=?,complete=?,pinned=?,terminal_seq=?,payload=? WHERE decision_cycle_id=? AND terminal_seq IS NULL',
                           (context_id, document.get('selected_candidate'), document['terminal_state'],
                            int(not document['gaps']), int(document.get('submission_started', False)),
                            len(document['events']), zlib.compress(encoded(header)), cid))

    def prune(self, db, now):
        # Incomplete and submitted cycles remain pinned until certified offline archive.
        db.execute('DELETE FROM decision_cycles WHERE complete=1 AND pinned=0 AND at<?', (now - self.retention,))
        excess = db.execute('SELECT COUNT(*) FROM decision_cycles').fetchone()[0] - self.max_cycles + 1
        if excess > 0:
            db.execute('DELETE FROM decision_cycles WHERE decision_cycle_id IN (SELECT decision_cycle_id FROM decision_cycles WHERE complete=1 AND pinned=0 ORDER BY at LIMIT ?)', (excess,))
        page_size=db.execute('PRAGMA page_size').fetchone()[0]
        used_pages=db.execute('PRAGMA page_count').fetchone()[0]-db.execute('PRAGMA freelist_count').fetchone()[0]
        if used_pages*page_size>self.max_bytes*.78:
            # Reuse freed pages under pressure; never evict submitted/incomplete evidence.
            db.execute('DELETE FROM decision_cycles WHERE decision_cycle_id IN (SELECT decision_cycle_id FROM decision_cycles WHERE complete=1 AND pinned=0 ORDER BY at LIMIT 256)')
        db.execute('DELETE FROM strategic_artifacts WHERE artifact_hash NOT IN (SELECT artifact_hash FROM decision_strategic_refs)')
        db.execute('DELETE FROM strategic_context_snapshots WHERE strategic_context_id NOT IN (SELECT strategic_context_id FROM decision_cycles)')
        db.execute('DELETE FROM decision_gate_catalog WHERE gate_catalog_id NOT IN (SELECT gate_catalog_id FROM decision_cycles)')

    def archive_settled(self, cid, archive_path, settlement_proof):
        """Explicit OFFLINE archive, never called by trading. Requires account/deal
        identity and a caller-certified final settlement. Archive first, then release.
        Archives are external intentional evidence, not a second live unbounded store.
        """
        if settlement_proof.get('broker_final') is not True:
            raise ValueError('final broker settlement proof required')
        if (not settlement_proof.get('settlement_identity') or not settlement_proof.get('settlement_utc')
                or settlement_proof.get('unresolved') is not False
                or len(settlement_proof.get('evidence_hash','')) != 64):
            raise ValueError('certified final settlement evidence required')
        with contextlib.closing(self.connect()) as db:
            outs = db.execute('SELECT account,deal_id FROM decision_outcomes WHERE decision_cycle_id=? AND status=?', (cid, 'ACCEPTED')).fetchall()
            if (settlement_proof.get('account'), settlement_proof.get('deal_id')) not in outs:
                raise ValueError('settlement identity does not match accepted opening')
            rows = {}
            for table in ('decision_cycles','decision_candidates','decision_events','decision_strategic_refs','decision_outcomes'):
                cur = db.execute(f'SELECT * FROM {table} WHERE decision_cycle_id=?', (cid,))
                rows[table] = [{desc[0]: (v.hex() if isinstance(v, bytes) else v) for desc,v in zip(cur.description,r)} for r in cur.fetchall()]
            refs = db.execute('SELECT DISTINCT artifact_hash FROM decision_strategic_refs WHERE decision_cycle_id=?', (cid,)).fetchall()
            rows['strategic_artifacts'] = [{d[0]:(v.hex() if isinstance(v,bytes) else v) for d,v in zip(cur.description,r)} for (aid,) in refs for cur in [db.execute('SELECT * FROM strategic_artifacts WHERE artifact_hash=?',(aid,))] for r in cur.fetchall()]
            cycle=db.execute('SELECT strategic_context_id,gate_catalog_id FROM decision_cycles WHERE decision_cycle_id=?',(cid,)).fetchone()
            for table,key,identity in [('strategic_context_snapshots','strategic_context_id',cycle[0]),('decision_gate_catalog','gate_catalog_id',cycle[1])]:
                cur=db.execute(f'SELECT * FROM {table} WHERE {key}=?',(identity,))
                rows[table]=[{d[0]:(v.hex() if isinstance(v,bytes) else v) for d,v in zip(cur.description,r)} for r in cur.fetchall()]
            data = encoded({'schema_version':SCHEMA_VERSION, 'tables':rows, 'settlement_proof':settlement_proof})
            path=Path(archive_path)
            with path.open('xb') as f:
                f.write(data); f.flush(); os.fsync(f.fileno())
            digest=hashlib.sha256(data).hexdigest()
            with db:
                db.execute('INSERT OR IGNORE INTO decision_retention_receipts VALUES(?,?,?,?,?)',
                           (cid,time.time(),digest,str(settlement_proof.get('settlement_identity')),zlib.compress(encoded(settlement_proof))))
                db.execute('UPDATE decision_cycles SET pinned=0 WHERE decision_cycle_id=?',(cid,))
            return digest


class Recorder:
    def __init__(self, store=None, *, enabled=True, asynchronous=True, queue_size=64,
                 clock=time.time, identity=None, catalog=()):
        self.store = store or Store(Path(__file__).with_name('june_decision_ledger.sqlite3'))
        self.enabled, self.asynchronous, self.clock = enabled, asynchronous, clock
        self.runtime_id = uuid.uuid4().hex
        self.identity = identity or {}
        self.catalog = tuple(catalog)
        self._local = threading.local()
        self._inputs = collections.OrderedDict()
        self._input_errors = set()
        self.queue = queue.Queue(maxsize=queue_size)
        self.drops = 0
        self.failures = 0
        self._last_diagnostic = 0
        self._last_error = None
        self._thread = None
        # Production constructs this during module initialization, not entry evaluation.
        if asynchronous and enabled:
            self._thread = threading.Thread(target=self._worker, name='june-decision-ledger', daemon=True)
            self._thread.start()

    def _worker(self):
        while True:
            try:doc = self.queue.get(timeout=1)
            except queue.Empty:
                self._diagnose();continue
            try:
                if doc is None:return
                self.store.persist(doc)
            except Exception:
                self.failures += 1
                self._last_error = ('persist', doc.get('decision_cycle_id'), traceback.format_exc())
            finally:
                self.queue.task_done()
            self._diagnose()

    def _diagnose(self):
        # Worker-only, including an idle queue after capture/serialization failures.
        now = time.monotonic()
        if (self.failures or self.drops) and now - self._last_diagnostic >= 60:
            self._last_diagnostic = now
            try:
                logging.warning('DECISION LEDGER GAP: failed=%d dropped=%d; trading unaffected', self.failures, self.drops)
                if self._last_error:
                    logging.warning('DECISION LEDGER FAILURE DETAIL: %s', self._last_error)
            except Exception:
                pass

    def drain(self):
        """Tests/offline shutdown only. Never called from the live path."""
        self.queue.join()

    def stop(self):
        if self._thread:
            self.queue.put(None)
            self.queue.join()
            self._thread.join(2)

    def _event(self, kind, *, symbol=None, **fields):
        doc = getattr(self._local, 'doc', None)
        if doc is None:return
        if len(doc['events']) >= MAX_EVENTS:
            doc['gaps'].append('event_cap');return
        cid = cid_for(doc,symbol) if symbol is not None else None
        ev = dict(sequence=len(doc['events'])+1, at=self.clock(), kind=kind, candidate_id=cid, **freeze(fields))
        doc['events'].append(ev)
        return ev

    def input(self, key, body, *, ttl=None, artifact_type=None, consumer=None):
        if not self.enabled:return
        try:
            if type(body) is bytes:body=body.decode('utf-8')
            if type(body) is str:
                try:value=json.loads(body)
                except (ValueError,TypeError):value={'raw_unparsed':body}
            else:value=freeze(body)
            entry={'artifact_key':key,'artifact_type':artifact_type or key.split(':')[0],
                   'body':value,'observed_at':self.clock(),'ttl':ttl,'consumer':consumer}
            if len(encoded(value))>MAX_CONTEXT_BYTES:raise ValueError('context size')
            self._inputs[key]=entry
            self._input_errors.discard(key)
            if len(self._inputs)>128:self._inputs.popitem(last=False)
            doc=getattr(self._local,'doc',None)
            if doc is not None:
                entry={**entry,'sequence':len(doc['strategic_refs'])+1,'consumed':True}
                doc['strategic_refs'].append(entry)
        except Exception:
            if len(self._input_errors)<128:self._input_errors.add(key)
            doc=getattr(self._local,'doc',None)
            if doc is not None:doc['gaps'].append('strategic_snapshot')
            self.failures+=1

    def begin(self, values, namespace):
        if not self.enabled:return None
        try:
            if getattr(self._local,'doc',None) is not None:return self._local.doc['decision_cycle_id']
            state=namespace.get('_live') or {}; account=(namespace.get('_live_sess') or {}).get('account_id')
            now=self.clock();cid='jdl1:'+self.runtime_id+':'+uuid.uuid4().hex
            doc={'schema_version':SCHEMA_VERSION,'decision_cycle_id':cid,'runtime_id':self.runtime_id,
                 'at':now,'account':account,'commit_id':self.identity.get('commit_id'),
                 'strategy_config_id':self.identity.get('strategy_config_id'),'state':freeze({k:state.get(k) for k in STATE_FIELDS}),
                 'live_enabled':namespace.get('_june_live_trading_enabled'),'regime':values.get('regime'),
                 'candidates':{},'events':[],'strategic_refs':[],'outcomes':[], 'gaps':['strategic_snapshot:'+k for k in sorted(self._input_errors)],
                 'submission_started':False,'terminal_state':None,'selected_candidate':None,
                 'gate_catalog':self.catalog,'ranking_rounds':0,'attempts':0}
            self._local.doc=doc;self._local.symbol=None
            for key,entry in self._inputs.items():
                doc['strategic_refs'].append({**freeze(entry),'sequence':len(doc['strategic_refs'])+1,'consumed':False})
            for key in MEMORY_CONTEXT:
                self.input('memory:'+key,namespace.get(key),artifact_type='JUNE_CONSUMED_MEMORY',consumer='cycle_start')
            self._event('CYCLE_BEGIN')
            return cid
        except Exception:
            self._local.doc=None;self.failures+=1

    def checkpoint(self):
        doc=getattr(self._local,'doc',None)
        if doc is None:return
        try:
            snapshot=freeze(doc)
            if self.asynchronous:self.queue.put_nowait(snapshot)
            else:self.store.persist(snapshot)  # explicit offline/test mode only
        except Exception:
            doc['gaps'].append('storage_enqueue_or_write');self.drops+=1
            self._last_error = ('checkpoint', doc.get('decision_cycle_id'), traceback.format_exc())

    def note(self, event, values, namespace=None, **metadata):
        """All observation exceptions are contained here. Never returns a verdict."""
        if not self.enabled:return
        try:
            namespace=namespace or {}
            if event=='begin':self.begin(values,namespace);return
            if event=='strategic_read':
                self.input(metadata['key'],values.get(metadata['variable']),consumer=metadata.get('function'));return
            doc=getattr(self._local,'doc',None)
            if doc is None:return
            symbol=values.get('sym',getattr(self._local,'symbol',None))
            if event=='gate' and metadata.get('function') in ('_live_try_entry','_live_select_instrument') and 'sym' not in values:
                symbol=None  # global/source-generation gates are not the previous pick
            if event=='helper_snapshot':
                inputs={k:freeze(v) for k,v in values.items() if k not in ('all_combos','signals','_ext')
                        and type(v) in (str,bool,int,float,dict,list,tuple,set,type(None))}
            elif event=='gate' and 'input_names' in metadata:
                # Loop locals can still contain the previous instrument's values.
                # Record the predicate's inputs, not those stale unrelated locals.
                inputs={}
            else:
                inputs={k:freeze(v) for k,v in values.items() if k in SCALARS or k.endswith('_pts')}
            for name in metadata.get('input_names',()):
                source=values if name in values else namespace
                if name in ('signals','_ext') and type(source.get(name)) is dict:
                    # The immutable universe/signal-version events retain all values.
                    # A membership gate only needs the original map's keys here.
                    inputs[name+'_keys']=list(source[name]);continue
                if name not in inputs and name in source and type(source[name]) in (int,float,str,bool,dict,list,tuple,set,type(None)):
                    keys=state_keys(metadata.get('expression')) if name=='_live' else None
                    if keys is not None and type(source[name]) is dict:
                        # Preserve present/missing keys and exact values used by
                        # this predicate, rather than duplicate unrelated history.
                        inputs[name]=freeze({k:source[name][k] for k in keys if k in source[name]})
                        inputs['_live_input_keys']=list(keys)
                    else:inputs[name]=freeze(source[name])
            for name in ('_b1_snapshot','thesis_snapshot','_htf_cg_verdict','_htf_cg_note','_conf_note'):
                if name in values:inputs[name]=freeze(values[name])
            if event=='universe':
                sig=values.get('signals') or {};universe=namespace.get('_sim_eligible') or ()
                symbols=list(dict.fromkeys(list(universe)+list(sig)))
                if len(symbols)>MAX_CANDIDATES:doc['gaps'].append('candidate_cap');symbols=symbols[:MAX_CANDIDATES]
                for s in symbols:
                    if s in doc['candidates']:
                        if sig.get(s)!=doc['candidates'][s]['signal']:
                            self._event('SIGNAL_VERSION',symbol=s,signal=sig.get(s))
                        continue
                    signal=freeze(sig.get(s));raw_direction=(signal or {}).get('direction')
                    direction='long' if raw_direction=='bull' else 'short' if raw_direction=='bear' else None
                    metadata_maps=('_live_margin','_live_lot_sizes','_live_min_deal','_live_price_unit',
                                   '_live_market_status','_live_ccy','_live_fx_base','_sim_min_notional','INSTRUMENTS')
                    doc['candidates'][s]={'candidate_id':cid_for(doc,s),'instrument':s,'direction':direction,
                                          'signal':signal,'in_universe':s in universe,'rank':None,
                                          'raw_score':None,'final_score':None,'economics':None,
                                          'contract_metadata':freeze({k:(namespace.get(k) or {}).get(s) for k in metadata_maps})}
                self._event('UNIVERSE',symbols=symbols)
            elif event=='candidate':
                self._local.symbol=symbol;self._event('CANDIDATE_FILTER_VISIT',symbol=symbol)
            elif event=='ranking':
                pairs=values.get('candidates') or [];doc['ranking_rounds']+=1
                ranked=[]
                for rank,(s,score) in enumerate(pairs,1):
                    candidate=doc['candidates'].get(s)
                    if candidate and candidate['rank'] is None:
                        candidate.update(rank=rank,final_score=score,
                                         rank_gap_to_next=score-pairs[rank][1] if rank<len(pairs) else None)
                    ranked.append({'instrument':s,'rank':rank,'score':score})
                self._event('RANKING',round=doc['ranking_rounds'],ranked=ranked)
                self.checkpoint()  # snapshot fixed before selecting the first survivor
            elif event=='attempt':
                ranked=values.get('_ranked') or []
                if ranked:
                    symbol=ranked[0];self._local.symbol=symbol;doc['attempts']+=1
                    self._event('ATTEMPT',symbol=symbol,attempt=doc['attempts'],ranked_survivors=ranked,
                                skipped=values.get('_notional_skip') or [])
            elif event=='fallback':
                self._event('FALLBACK',symbol=symbol,skipped=values.get('_skip'),inputs=inputs)
            elif event=='gate':
                # Threshold-rejected candidates still have an exact raw rank
                # input. Only declared gate inputs are safe: other loop locals
                # may belong to the previous instrument. No final score exists
                # until the selector actually computes its adjusted rank.
                candidate=doc['candidates'].get(symbol)
                if (metadata.get('function')=='_live_select_instrument'
                        and 'vol' in inputs and candidate is not None
                        and candidate['raw_score'] is None):
                    candidate.update(raw_score=freeze(inputs['vol']),
                                     raw_score_sequence=len(doc['events'])+1)
                self._event('GATE',symbol=symbol,inputs=inputs,**metadata)
            elif event=='branch':
                self._event('BRANCH',symbol=symbol,inputs=inputs,**metadata)
            elif event=='score':
                components={k:v for k,v in inputs.items() if k.endswith('_pts') or k=='clearance'}
                self._event('SCORE_COMPONENTS',symbol=symbol,components=components,total=values.get('raw'),
                            conviction=values.get('_final_conv'),score_kind='conviction_not_ranking')
            elif event=='score_rank':
                candidate=doc['candidates'].get(symbol)
                if candidate is not None and 'ranking_score_sequence' not in candidate:
                    # Exact selector locals, frozen before later gates/fallbacks.
                    # Conviction SCORE_COMPONENTS is a different score contract.
                    candidate.update(raw_score=freeze(values.get('vol')),
                                     final_score=freeze(values.get('eff_vol')),
                                     ranking_score_sequence=len(doc['events'])+1)
                self._event('RANK_COMPONENTS',symbol=symbol,raw=values.get('vol'),final=values.get('eff_vol'),
                            regime_weight=values.get('weight'),correlation_weight=values.get('corr_adj'),
                            wide_spread=(values.get('sig') or {}).get('spread_atr_wide'),signal=values.get('sig'))
            elif event=='submission':
                doc['submission_started']=True;doc['selected_candidate']=cid_for(doc,symbol)
                self._event('SELECTED_FOR_SUBMISSION',symbol=symbol,inputs=inputs,order=values.get('order_body'))
                self.checkpoint()
            elif event in ('submission_response','confirmation'):
                raw=values.get('resp' if event=='submission_response' else 'confirm')
                safe={k:raw.get(k) for k in ('dealReference','dealId','dealStatus','status','reason','level','size','direction','date') if k in raw} if type(raw) is dict else None
                if event=='submission_response':
                    status=('SUBMISSION_FAILED' if not raw else 'REFERENCE_RECEIVED'
                            if raw.get('dealReference') else 'SUBMISSION_REFERENCE_MISSING')
                else:
                    status=('UNCONFIRMED' if not raw else 'ACCEPTED'
                            if raw.get('dealStatus')=='ACCEPTED' else 'REJECTED'
                            if raw.get('dealStatus')=='REJECTED' else 'CONFIRMATION_OTHER_STATUS')
                account_evidence=raw.get('_june_account_evidence') if type(raw) is dict else None
                ev=self._event('BROKER_LINK',symbol=symbol,status=status,receipt=safe,account_evidence=account_evidence)
                deal=(safe or {}).get('dealId');ref=(safe or {}).get('dealReference')
                campaign=hashlib.sha256(json.dumps([doc['account'],deal]).encode()).hexdigest() if deal and status=='ACCEPTED' and doc['account'] else None
                doc['outcomes'].append({'sequence':ev['sequence'],'status':status,'deal_id':deal,'deal_reference':ref,'campaign_id':campaign,'receipt':safe,'account_evidence':freeze(account_evidence)})
            elif event=='finish':
                doc['terminal_state']='SELECTED_FOR_SUBMISSION' if doc['submission_started'] else 'NO_TRADE_GLOBAL_GATE' if not doc['ranking_rounds'] else 'NO_CANDIDATES' if not doc['attempts'] else 'NO_TRADE_PATH_ENDED'
                if doc['outcomes'] and doc['outcomes'][-1]['status'] in ('SUBMISSION_FAILED','REJECTED'):doc['terminal_state']='ENTRY_SUBMISSION_FAILED'
                if sys.exc_info()[0] is not None:
                    doc['gaps'].append('primary_execution_exception');doc['terminal_state']='PRIMARY_EXECUTION_EXCEPTION'
                # The durable view represents catalog-minus-observed as NOT_REACHED.
                # This avoids storing and copying thousands of identical null events.
                self._event('CYCLE_TERMINAL',terminal_state=doc['terminal_state'])
                self.checkpoint();self._local.doc=None;self._local.symbol=None
            else:
                self._event(event.upper(),symbol=symbol,inputs=inputs,**metadata)
        except Exception:
            doc=getattr(self._local,'doc',None)
            if doc is not None:doc['gaps'].append('observation_failure')
            self.failures+=1


def cid_for(document, symbol):
    # Identity is the composite (decision_cycle_id, candidate_id), never symbol alone.
    return symbol


_default = None


def initialize(path, catalog):
    """Startup only; git/config identity is resolved once by the worker setup."""
    global _default
    try:
        import subprocess
        root=Path(path).resolve().parent
        commit=subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD'],text=True,timeout=2).strip()
        identity={'commit_id':commit,'strategy_config_id':hashlib.sha256(Path(path).read_bytes()).hexdigest()}
        _default=Recorder(Store(root/'june_decision_ledger.sqlite3'),identity=identity,catalog=catalog,
                          enabled=os.environ.get('JUNE_DECISION_LEDGER_ENABLED','1')!='0')
    except Exception:
        # Missing optional telemetry module/database/identity must never stop June.
        _default=None
        try:logging.warning('DECISION LEDGER GAP: startup unavailable; trading unaffected')
        except Exception:pass


def observe(event, values, namespace, **metadata):
    try:
        if _default is not None:_default.note(event,values,namespace,**metadata)
    except Exception:
        pass
