# Durable settlement discovery

This repair is based on `c66258373a13d1bdf14c97d924008fe161c9d96e`.
It changes settlement persistence and recovery. Broker matching, exit provenance,
order handling, and strategy parameters retain their existing authority.

## Failure and authority

The deployed reconciler enumerated `june_live_trade_history_full` alone. That
Redis list had a 2,000-row cap and a refreshed 30-day TTL. The 50-row
`june_live_state.trade_history` was another snapshot. Neither was a durable
enumeration of all outstanding settlements. Three settlements had been selected
by reconciliation before their list rows disappeared. Their SQLite lifecycle
evidence survived. The exact deletion or eviction operation was not proved.

The new `.settlements.sqlite3` registry, optionally located by
`JUNE_SETTLEMENT_PATH`, owns settlement enumeration and updates. Its key is
`(authenticated account, deal:<broker deal ID>)`. Campaign identity is retained
as context; different deals in one campaign are not collapsed. The registry
holds one settlement record per identity, retry state, and compact delivery
receipts. It uses SQLite transactions and `synchronous=EXTRA`, like the existing
local evidence journal. It needs persistent disk and backups; this task does not
implement retention, replication, or host-loss recovery.

A discovered provisional record requests reconciliation. It cannot establish
P&L. `settlement_reconcile.reconcile_settlement` still decides economics from
broker evidence, including its existing activity-anchored and transaction-only
rules, ambiguity checks, and partial/final quantity coverage. It is unchanged.
Existing already-confirmed records are persisted as existing outcomes; this
repair does not recalculate the baseline's immediate-close fill estimates.

## Creation, bootstrap, and restart

Both primary settlement creation paths write the compact registry before their
Redis snapshot append. They also capture a `settlement_created` event through
the existing independent evidence journal. An accounting write failure is
reported and cannot prevent a protective exit.

Normal reconciliation imports retained Redis/state records idempotently and
scans the local `.broker-evidence.sqlite3` journal incrementally, at most 500
events per view preparation. Confirmed cached copies are considered before stale
provisional duplicates during bootstrap. Once registered, the compact record
owns subsequent changes; importing an old snapshot cannot overwrite it.

The legacy importer accepts matching-account primary events. A
`before_primary_clear` event supplies deal, opening quantity/price, time,
partial-exit context, and provenance. It creates only a PROVISIONAL record with
null P&L. It never treats absence or local estimates as economic confirmation.
Legacy records whose prior learning delivery cannot be established remain
explicitly excluded from historical learning replay. This does not suppress
their economic reconciliation. New `settlement_created` events preserve the
normal pending-delivery contract.

The source cursor and imported records commit together. Malformed rows retain a
`discovery_quarantine` reference and remain in the source file, while later valid
rows can advance. A missing or temporarily unreadable legacy journal does not
block identities already in the compact registry. The append-only source and
its cursor must be handled together by any future archive/restore design.

Every registered unresolved identity remains enumerable across restarts,
independently of Redis history length. Existing age, backoff, broker-history
window, and per-cycle query limits still apply. Bootstrap may need multiple
cycles to scan an existing journal. Enumerability does not promise broker
history beyond the existing 14-day reconciliation window.

## Confirmation, snapshots, and delivery

`SettlementView` provides stable identity-based slots to the existing
reconciler. Updates commit SQLite first. CONFIRMED economics cannot regress to
PROVISIONAL or be replaced with different P&L. SQLite serializes the read/check/
write transition. Duplicate inputs produce one canonical record and one due
broker request per identity in a cycle.

Redis history is an optional projection. Existing matching rows are updated by
identity with an atomic compare-and-set, so a concurrent list insertion cannot
redirect an update to another trade. Missing rows are not inserted merely to
feed performance. A failed cache write cannot undo durable confirmation.

Snapshot refresh copies confirmed economics and provenance to matching stale
provisional copies. It creates no rows and makes no performance call. Its
persistence can fail without undoing durable reconciliation.

Eligible performance uses the unchanged Redis atomic stats-plus-permanent-marker
commit from `exit_authority.commit_performance`, plus a local write-ahead receipt.
The local receipt protects delivery after restart and subsequent Redis marker
loss. A definite Redis CAS rejection is retryable. If the process loses the
acknowledgement of a Redis commit, the permanent Redis marker resolves it on
retry. If that marker is also absent, delivery remains explicitly uncertain and
automatic replay is refused; `perf_fed` is not falsely acknowledged. This is an
at-most-once guarantee, not a promise of eventual delivery through simultaneous
loss of all delivery evidence. Economic confirmation is retained independently.

External/manual and UNKNOWN outcomes retain their existing learning exclusions.
Learning eligibility never establishes economic authority. Historical raw
snapshots with uncertain delivery history are not a license to backfill training.

When Redis itself is unavailable, the reconciler retains the existing fail-safe:
it makes no settlement broker-history queries or learning delivery that cycle.
An empty, missing, truncated, duplicate, or stale history list is different from
an unavailable Redis service and does not block compact-registry enumeration.

## Evidence needed before any later retention change

The large `june_broker_ledger_v1:<account hash>` is not read by this discovery
implementation. Normal registered settlements no longer need old forensic
snapshots for enumeration. Legacy bootstrap currently uses the local journal,
so it must remain intact until import coverage and quarantines are reviewed.

| Purpose | Evidence/state to preserve |
| --- | --- |
| Settlement | Compact records, stable account/deal identity, original quantity/price, partial context, broker reconciliation result and sources; broker activity/transactions remain the economic authority. |
| Restart | Compact records, import cursors and source continuity, performance receipts, active/pending state, verified opening account/deal receipts. Registry rows never reactivate positions. |
| Stop acknowledgement | `stop_sync`, acknowledged broker stop levels, exact deal/ref linkage and accepted broker responses. Requested or pending stops are not acknowledgements. |
| Performance dedup | Compact delivery receipts and `perf_fed` state, existing permanent `june_perf_delivery:*` markers, and provenance. Bounded rolling stats alone are not a delivery ledger. |
| Continuation/pyramid experiments | Active campaign/leg identities, partial outcomes, reservation/continuation state, exit-authority legs, and separate campaign telemetry. This repair changes none of their decision rules. |
| Forensics | Raw opening, stop, close-intent/response/confirmation, state/position snapshots, and recorder paths with time/account/deal linkage. Historical raw telemetry may be archived only under a separately tested recovery contract. |

The current June wiring uses the large hash through evidence capture/replay. Its
raw events remain useful recovery and forensic evidence; the generic ledger's
ownership/economic projections are not the current settlement dispatcher. No
hash field, TTL, archive, or pruning operation is added here.

## Regression evidence

`fixtures/lost_settlements_20261002.json` retains the three original provisional
shapes, local clear snapshots, and the actual broker activity/transaction rows.
The fixtures contain no authentication credentials. Tests derive +$0.05, +$0.43,
and -$0.20 (net +$0.28) through the unchanged broker matcher. All three original
SYSTEM closures remain UNKNOWN for learning. No production record was promoted.

The tests cover empty/truncated/stale/duplicate Redis history, restart, archive
bootstrap, existing confirmed rows, identity aliases, partial/final and late
evidence, provenance, snapshot failures, creation hooks, storage failure during
protective exits, delivery crash boundaries, and cache-independent discovery.
The final task report records focused/full counts and comparison with baseline.

This candidate requires a separately authorized deployment. The next task is a
tested Redis retention/archive design after this repair is validated/deployed.
