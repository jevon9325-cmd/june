# Broker-truth repair checkpoint and continuation

## Authority and restrictions

Repository: `C:\Users\jevon\trading_alerts\june-broker-truth-repair`.
Branch: `repair/broker-truth-ledger`.
Origin: `https://github.com/jevon9325-cmd/june.git`.
Checkpoint tag: `checkpoint/broker-truth-repair-20260921`.
Baseline: `23170d943be5e140556324faf39b88397b84b09a`.

GitHub HEAD/main and production HEAD were freshly checked and identical before
editing. Production tracked files had no diff. Service MainPID 2660947 started
2026-09-21 14:40:50 UTC, after june.py's 10:03:40 UTC modification. Production's
untracked helpers/backups were not changed. This checkout was initially clean.
The dirty parent repository is excluded: do not inspect, clean, stage, or commit
its files. All repair commits stay in this nested independent repository.

No deployment, push, service restart, live broker actions, production Redis
writes, historical migration, trading-switch change, or strategy redesign is
authorized. Use offline mocks and never import the live bot to run tests.

## Pre-edit trace (baseline line numbers)

| Case | Confirmation, quantity and notional | Outcome writes and missing path |
| --- | --- | --- |
| Ordinary full close | `_live_close_position` 8496: accepted `_live_confirm_deal` supplies exit price; estimated dollars use stored notional | 8625-8700: residual dollars in history and directional P&L; whole-position `won` includes partial; calls performance, streak, HTF event, 15m reliability, and phase progression |
| Broker stop | `_live_check_exit` 9210: LS FULLY_CLOSED or zero margin calls reconciliation | `_live_reconcile_positions` 10889 clears stale position without performance/history completion |
| Partial then residual | `_live_partial_tp_exit` 8950: accepted receipt, then remaining-position verification; 9139 reduces ig_size but leaves notional; partial dollars use current mid rather than entry basis | Saves partial amount in live position, overwriting rather than accumulating; ordinary residual path subsequently disagrees across consumers |
| Add-on close | `_live_close_addon_leg` 9539 confirms then removes leg; `_live_check_pyramid_exits` 9679 also removes broker-closed legs | No normal performance/history write; unlock counter and live state only. `_live_add_pyramid_leg` 9941 stores intended notional; promotion copies it |
| Broker-side close without normal confirmation | Pre-close guard/reconciliation may establish absence; startup may reconstruct orphan exposure with incomplete metadata | State may clear without a completed outcome; ambiguous absence must not be interpreted as zero-dollar realized P&L |

Existing writes: `june_live_state`, `june_live_trade_history_full`,
`june_perf_stats:{sym}`, `june_perf_block_wr:{sym}`, session SAR block/observer
keys, pyramid unlock counter; `_sim_15m_record` changes SIM reliability state.
Consumers include performance/observer gates, live streaks, phase progression,
HTF calibration, shared SIM/live 15m reliability, and Barbie history summaries.
Barbie's combo/target/reversal tuning is largely SIM-based, not broker-net.

`_live_poll_pnl` 6968 separately sums account DEAL rows using reference-only
deduplication; it excludes separately booked commissions and cannot identify
June ownership. Do not relabel that field as canonical June net earnings.

## Gross-loss severity finding

`_live_perf_record` and migration logic sum negative amounts without offsetting
wins. The original severity commit `1de1592` and comments do not establish intent
to use algebraic net losses. Preserve existing arithmetic, thresholds, and gate
behavior. Any semantic change needs a separate decision; do not silently fix it
while repairing the data source.

## Minimal staged plan

1. Pure canonical reconciliation plus broker-shaped offline fixtures.
2. Partial quantity/notional accounting, without exit/order policy changes.
3. Durable pending-close capture and paginated read-only history adapter; broker
   exits and ordinary exits must use the same idempotent outcome delivery.
4. Add-on actual-exposure accounting and lifecycle capture, retaining promotion
   and aggregate-stop policies.
5. Small consumer compatibility/deduplication fixes; audit remaining SIM inputs.

## Stage A: pure reconciliation foundation

Added `broker_ledger.py`, not imported or called by June yet. No existing
production functions or constants changed in this stage.

`reconcile_completed_trade` separates broker realizations, explicit cost
attribution, and strategy metadata. Pending quantity, incomplete pagination, or
unknown costs cannot yield a completed outcome. `completed_history_view` derives
one whole-position result and refuses pending evidence. Decimal arithmetic
preserves quantity conservation. Reference plus opening/closing tuple deduplicates
realizations; conflicting observations fail for review. Missing exposure stays
unknown. Only verified USD accounting is supported; do not guess FX conversions.

The adapter must supply exact broker opening identity, original broker quantity,
verified unit notional, and explicit cost ownership. Local order timestamps and
name-only fee allocation are insufficient. Before reconciling multiple tracked
positions, enforce unique ownership of each broker realization; identical opening
tuples belonging to multiple deals must remain ambiguous, not be assigned twice.

Tests cover observed gold partial accounting and MU commission cases, partial
win/loss combinations, multiple partials, pending history/costs, broker-only
realizations, duplicate observations, shared references, conflicting evidence,
excess quantity, non-finite values, currency rejection, metadata isolation and
serialization. Existing risk tests remain offline but mirror some bot logic;
they are not sufficient integration proof by themselves.

## Required next-stage recovery design

The pure module provides deterministic record identity, NOT crash-safe consumer
delivery. Before switching any learning consumers, retain pending opening/close
evidence durably across all clear/remove/promotion paths. Read every history page.
Order acknowledgement/position absence alone cannot establish realized dollars.
Broker history and commissions may arrive later; keep retrying pending records.
An append-once ledger marker followed by non-atomic consumer updates is unsafe:
a crash can lose delivery, while retries can double-count streaks and phases.
Test crashes/restarts and Redis failures at each delivery boundary before wiring.
Do not mix provisional estimated outcomes with authoritative completed outcomes.
Do not invent a broker-stop exit reason from account-level zero margin alone.

Historical production records remain unchanged. Scope limitations still pending:
HTF one-to-many matching, SIM/live mixing, severity semantics, and account-level
earned P&L/skimming require explicit classification rather than a broad rewrite.

## Stage B: partial-close accounting

Stage A commit: `67d5676c9c702909f7065e2828e70e20b3bd478e`.
Only `_live_partial_tp_exit` and `_live_close_position` changed in June.
Accepted, verified partial closes now preserve original quantity/notional, reduce
remaining notional proportionally, calculate partial dollars on entry basis and
accumulate partial realizations. Final close history, directional totals and
performance dollars use partial plus residual results, consistent with win/loss.
History retains the residual and partial components and explicitly labels
`pnl_source=confirmed_fill_estimate`. It is NOT broker-history reconciliation.
Original `pos_size` and leverage remain unchanged because pyramiding uses them
as policy inputs. No order payload, exit trigger, stop, target or gate changed.

Eight new tests execute the actual two functions via AST extraction with mocked
broker/Redis/time dependencies. They cover partial quantity/notional, all three
requested partial/residual sign combinations and consumer agreement, repeated
partials, short positions, ordinary full close, disabled guard, rejected partial
and minimum-deal fallback. Compile checks passed; the combined suite passed
66 tests (8 live accounting, 19 reconciliation, 39 existing risk tests).
AST comparison confirmed all other functions, imports and module constants
unchanged. Caller signatures are unchanged. Full diff and field references were
reviewed; existing CRLF line endings in June were retained.

Limitations: old already-partially-closed positions lack the new original basis
and are not migrated. Existing fill/notional approximations, flat equity fee
estimate and unknown financing remain; final `exit_price` is the residual fill,
while `pnl_pct` represents combined gross return. These provisional consumers
remain contaminated until canonical delivery is implemented and verified.

Read-only production source search identified Claudia `_build_june_performance_str`
and its Barbie-review calibration using numeric `dollar_pnl`; the field type is
unchanged and whole-position totals repair this particular inconsistency.
Both consumers attempt GET/JSON-object access to June's persistent history key,
where June writes a Redis list with LPUSH. This pre-existing compatibility issue
was observed in source, not tested against production Redis, and is NOT repaired
here. The former catches the outer error and may return Unavailable without its
intended state fallback. The calibration reader catches the persistent-key error
and retains the rolling state sample. Do not silently broaden this June patch
to sister repositories. No matching direct field references were found in the
other top-level Python files under /opt/bots (not an exhaustive dynamic audit).

Learning status at this checkpoint: performance/WR blocks, observer severity and
recovery, streaks and leverage phases receive more consistent ordinary-close
estimates but still lack broker-managed/add-on coverage and actual costs. HTF
matching and shared 15-minute reliability still require separate assessment.
Combo/adaptive target SIM inputs remain intentionally SIM-based. Barbie/Claudia
live summaries retain incomplete history and the compatibility limitation above.
Earned-P&L/skimming remain account-level DEAL estimates, not canonical June net.
No consumer is newly certified trustworthy by these two stages alone.

## Stage C1: durable evidence and paginated history components (not runtime wired)

Resumption verified branch `repair/broker-truth-ledger`, clean HEAD
`c080e6981cf88b95630ddabec49e8e06c7b6fac2`, checkpoint and both prior commits.
The original 66 tests passed again before editing. No production/GitHub refresh
was performed in this substage; their equality is the original pre-edit check.

New `broker_history.fetch_transaction_history` reads fixed UTC windows using
IG GET /history/transactions v2, ALL types, all pages. Parameters and response
fields were checked against https://labs.ig.com/reference/history-transactions.html.
Missing/unstable/truncated/repeated pages fail rather than returning incomplete
success. Pagination does not establish final cost completeness. The injected
GET callback must stay bound to the verified account; the reader cannot enforce
account identity from transaction rows that do not expose an account ID.

New `broker_pending.PendingCloseStore` uses the existing redis dependency:

- `capture`: immutable, deduplicated raw snapshots, without TTL.
- `register_opening`: exact opening identity plus explicit June accepted-entry
  receipt; recovered/manual positions do not automatically become June trades.
- `reconcile`: calls the pure ledger; pending history/costs remain pending;
  conflicting or disappearing evidence cannot erase previous observations.
  Opening-tuple collisions block attribution and delivery for both positions.
- `project_once`: atomic journal-owned consumer state plus delivery marker in
  one HSET inside WATCH/MULTI/EXEC. Pure reducers only; never wrap existing
  side-effecting June learning calls in this function.
- `entries`: restores captured/pending/completed work after application restart.

One account-scoped `june_broker_ledger_v1:<account hash>` Redis hash contains the
journal. No deletion/expiry, historical migration, or production access occurs.
Transport errors propagate; an ambiguous acknowledgement is safe to retry.
The journal currently reads the complete hash per update, so growing-history
performance needs assessment before integration. Redis data durability remains
an operational prerequisite; tests simulate application/client restart, not
Redis disk failure. All known opening identities must be registered before
delivery; a newly discovered collision blocks subsequent delivery but cannot
undo effects already delivered before that evidence was known.

New `test_broker_pending.py`: 22 tests using fakeredis, including delayed partial
and cost evidence, restart recovery, broker-only close, duplicate delivery,
pre/post-EXEC disconnect, lost capture/reconciliation acknowledgement, concurrent
WATCH conflict, separate consumers, malformed pages, unknown ownership, wrong
account/window, insufficient cost coverage, changed completed P&L and reducer
failure. The broker-close test delivers to a fixture projection, NOT actual
`_live_perf_record`. No June runtime function changed in C1.
Compile checks passed and the combined suite passed all 88 tests (22 new plus
66 baseline tests). Full diff, whitespace, file scope and reference checks passed.

Test-only dependencies installed inside this checkout's `.git/test-deps`:
fakeredis 2.38.0, redis 8.1.0, sortedcontainers 2.4.0. Production requirements are
unchanged. The initial sandbox run could not read those installed files; rerun
with local dependency access passed. This was a test-environment permission
failure, not a production operation. For a fresh checkout, install fakeredis
in an isolated test environment before running these tests.

### Next exact integration work (C2, then D/E)

1. Reverify branch/HEAD/status. Inspect all clear/remove/promotion paths before
   inserting capture, including `_live_close_position`, `_live_partial_tp_exit`,
   `_live_close_addon_leg`, `_live_check_pyramid_exits`, `_live_reconcile_positions`
   and `_live_save_state`. Preserve evidence across Redis failures without
   silently suppressing protective exits or clearing the last recoverable copy.
2. Retain actual accepted entry receipts, exact broker opening UTC/name/quantity
   and verified session account for primary/add-on positions. Current entry_time
   is local time; `_live_sess` does not retain currentAccountId. Do not guess
   either identity. Unknown legacy/recovered ownership remains unattributed.
3. Bind a retry worker to the account-pinned history reader and establish explicit
   cost attribution/completeness evidence. Absence of fees is not proof of zero
   costs. Cost evidence in tests is a fixture, not an implemented IG cost adapter.
4. Integrate existing learning updates atomically with delivery or an equivalent
   proven recovery design. Existing consumers update multiple Redis keys and
   in-memory state; C1 projections do not make those writes exactly-once. Test
   actual close/reconciliation functions and crash boundaries before switching
   away from Stage B estimates. Preserve chronological streak/phase semantics
   when broker history arrives late; do not silently redesign them.
5. Complete add-on native-unit exposure accounting while preserving promoted
   `pos_size`/pyramid sizing policy, then consumer compatibility and final audit.

The repair is incomplete. C1 modules have no callers in june.py, and do not
improve production coverage yet. No existing adaptive consumer is newly trusted.
No push/deployment/restart/broker action/production Redis write occurred.

## Stage C2a: runtime entry receipts and account provenance

User continuation authority is attachment
`59c9b11a-0112-4941-8458-cc427685d0d0/pasted-text.txt`. It separates C2 evidence
generation from Stage E learning integration and requires stopping after C2.
Reverified repository/origin (credential-free URL), branch, clean C1 HEAD
`e6bfb9368ab2fa46edf7b4febae564a512937be6`, checkpoint, repair commits and all
88 baseline tests before edits. No discrepancy. Backup: `.git/june.py.before-c2a`.

New `broker_identity.response_account_evidence` compares request tokens with
the authenticated session, retaining account ID/currency but never tokens.
Changed/missing account context stays unverified. `opening_evidence` separates
submitted orders and local context from broker confirmation size/level/receipt.
It never substitutes local entry_time, requested size or estimated fill for
missing broker fields. Raw market unit fields are retained without conversion
or default values; this stage does not calculate actual notional from them.

Runtime changes (existing signatures unchanged):
- `authenticate_live`: retain actual currentAccountId/currencyIsoCode, no guessed
  account or USD currency in the evidence fields.
- `_ig_live_get`: add `_june_account_evidence` only to successful /confirms replies.
- `_ig_live_post`: add that field only to successful /positions/otc replies.
- `_live_fetch_market_data`: retain raw instrument unit fields by symbol/epic in
  a separate evidence cache, never read by sizing or stop logic.
- New `_live_entry_evidence`: isolate evidence formatting errors from trading;
  return explicit capture_error with raw receipt/order instead of raising.
- `_live_open_position`, `_live_add_pyramid_leg`: attach broker_entry_evidence
  with role primary/add_on and local identifiers before existing state save.
- `_live_close_position`: promotion copies the add-on evidence unchanged,
  preserving add_on origin. No other close behavior changed.

API evidence: https://labs.ig.com/reference/confirms-deal-reference.html calls
`date` a transaction date; it does not establish exact position opening UTC.
https://labs.ig.com/reference/positions-deal-id.html v2 exposes createdDateUTC,
instrumentName, contractSize, currency and size. No new broker read was executed.
C2a therefore deliberately leaves broker_opened_utc unknown. Even timezone-aware
confirm dates are confirmation_utc only. Identity remains pending_broker_opening
or unverified_entry until the next stage obtains exact broker opening identity.
Market names still need verification against transaction instrument names.

All five POST callers and all five `_live_confirm_deal` callers retain their
existing keys/behavior; the added response key is not sent back to the broker.
Other GET responses are unchanged. New position metadata is additive; existing
history/learning projections do not read it. Cross-sister runtime code was not
modified. AST scope audit found exactly the seven existing functions above plus
the new helper; all previous function signatures/module constants/order payload
construction were unchanged. Existing June CRLF bytes were preserved.

Sixteen new offline tests cover token-free attribution, unknown/changing account,
401 reauthentication, concurrent session change, no timestamp/size/price guessing,
receipt mismatch, raw unit/role preservation, formatting failure, actual entry
annotation statements, existing save/load round-trip and promotion construction.
Tests execute extracted actual wrappers/helpers/metadata statements, not a live
bot import. They do not claim full entry-to-close crash recovery coverage.
Compile checks and all 104 offline tests passed (88 baseline plus 16 identity).
The final metadata-only test import cleanup was also compiled and retested.
Diff whitespace/stat/full inspection, caller/reference and AST scope checks passed.

### Scale measurement before pending-store runtime integration

Local fakeredis measurement used generated ~1,367-byte journal records, then one
PendingCloseStore.capture update. 100 records: 136,700 payload bytes, 7.3 ms;
1,000: 1,367,000 bytes, 89.6 ms; 5,000: 6,835,000 bytes, 711.9 ms. These are local
emulator timings, not production latency estimates. The important result is that
C1 HGETALL/deserialization/deepcopy scales with ALL retained history per update.
This is an unbounded-cost problem and must be corrected BEFORE wiring that store
into runtime. Prefer targeted hash-field reads/claims under the existing atomic
transaction, with incremental pending enumeration. Do not delete unresolved
evidence or historical ownership/dedup markers to hide the growth. No pruning,
retention change or Redis redesign was introduced in C2a.

### Exact C2b continuation and unresolved failure windows

The new receipt currently persists only through the EXISTING `_live_save_state`
Redis blob. Its failures are logged, not durably queued; a subsequent clear can
still lose evidence. C2a does NOT satisfy the pre-clear durability invariant.
Do not claim otherwise. PendingCloseStore/history worker remain unwired.

Before adding lifecycle capture, correct the full-hash update cost above. Then
establish a durable fallback/retry path for Redis outage that does not suppress
protective exits. Trace every destructive path, including:
- normal full close final clear and primary/add-on promotion;
- `_live_partial_tp_exit` both broker-flat early clears and residual mutation;
- `_live_close_addon_leg` pre-guard removal and confirmed-close removal;
- `_live_check_pyramid_exits` broker-closed leg removal;
- `run_live_step` stale pyramid-leg clear;
- `_live_reconcile_positions` stale clear and orphan/recovered replacements;
- `_live_save_state`, `_live_load_state`, startup/default initialization.
Retain original primary evidence before promotion, independently from the add-on.
Current orphan recovery must not manufacture June ownership for manual positions.

C2c must enrich exact identity and connect account-pinned history/cost handling.
C2d must test the user's twelve failure cases against actual runtime lifecycle
functions, plus delayed/out-of-order confirmation metadata for Stage E. Needed
ordering fields include exact broker final exit UTC, trade identity and separate
observation/reconciliation times; arrival order must not become trade chronology.
Chronology integration/test work is NOT completed by C2a. Do not wire adaptive
consumers or proceed automatically into Stage E after completing C2.

## Offline checks and continuation

Run in this repository:

```
# PowerShell; local emulator dependency must be readable:
$env:PYTHONPATH = (Join-Path (Get-Location) '.git/test-deps')
python -m py_compile broker_identity.py test_broker_identity.py broker_ledger.py broker_history.py broker_pending.py test_broker_pending.py test_broker_ledger.py test_live_accounting.py june.py
python -m unittest -q test_broker_identity test_broker_pending test_live_accounting test_broker_ledger test_risk_safety
git diff --check
git diff --stat
git diff
git status --short
```

Inspect `git log` for completed stage commits and current HEAD. Recheck status
before editing. Resume with C2b's integration steps above. Stages A/B/C1/C2a do NOT
repair live coverage or make existing learning data broker-truth. Not deployed;
production remains unchanged. At every stopping point include the user's exact
CHATGPT HANDOFF structure directly in the response, self-contained and preferably
under 1,500 words; do not require a reader to open this document.
