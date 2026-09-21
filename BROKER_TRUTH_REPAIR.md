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

## Offline checks and continuation

Run in this repository:

```
python -m py_compile broker_ledger.py test_broker_ledger.py june.py
python -m unittest -q test_broker_ledger test_risk_safety
git diff --check
git diff --stat
git diff
git status --short
```

Inspect `git log` for completed stage commits and current HEAD. Recheck status
before editing. Stage A alone does NOT repair live coverage or make any existing
learning consumer trustworthy. Not deployed; production remains unchanged.
