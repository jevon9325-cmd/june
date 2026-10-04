# Decision and settlement plumbing repair, 2026-10-04

Parent: 3577dfce3af29ef02be8976214636c26f670fa5b. No June trading source,
strategy constants, authority classification or economic aggregation changed.

The read-only baseline disproved the claim that all prospective score columns
were empty: 46 ranked candidates agreed exactly with their RANK_COMPONENTS
events. Another 28 candidates had an exact raw `vol` in their threshold GATE
events but no raw column. Record that declared input even when rejected, and
retain a truthful NULL final score when adjusted ranking was never reached.
Never read stale unrelated loop locals or confuse conviction with ranking.

Persist exact selector `vol`/`eff_vol`, preserve first-ranking semantics during
fallbacks, and enrich candidate columns/payload at subsequent checkpoints.
Previously INSERT OR IGNORE could freeze an early unscored checkpoint. Completed
cycles remain immutable. Event payloads remain unchanged. No historical backfill.

Four CONFIRMED outcomes have historical PREPARED delivery receipts after Redis
OOM on October 2. They are distinct from the 29 PROVISIONAL settlements. Current
memory is healthy; absent Redis markers cannot prove a historical write never
happened. Preserve this historical uncertainty and its no-replay fence.

For future delivery, replace two separate SET mutations with one atomic MSET
inside the existing marker/CAS script. Only a definitive OOM refusal releases
that attempt's PREPARED receipt, allowing retry after Redis recovery. Connection
loss, lost acknowledgement and old PREPARED receipts remain conservatively
quarantined. Durable broker economics do not depend on Redis delivery success.

Broker history also proves price collisions across separate campaigns. When a
retained accepted broker opening matches deal, opening price and quantity, narrow
transaction candidates by that opening's UTC second and broker direction before
the existing matcher runs. This excludes other campaigns; it does not certify
economics. Keep exact quantity coverage, final-close evidence, price matching,
deduplication and P&L aggregation unchanged. Unsupported opening contracts keep
the existing conservative matcher. Rounded partial quantities remain unresolved.

Sixteen of the 29 baseline provisionals are broker-confirmed but blocked by these
price collisions; one recent OIL close is already confirmable under the parent
and waiting on backoff. Nine legacy and three other records still fail exact
quantity or close-level evidence. Legacy learning quarantine, external/manual
exclusion and UNKNOWN authority exclusion are preserved on promotion.

Regression tests use exact captured broker rows and cover score/event agreement,
early checkpoints, rejected/selected/fallback candidates, truthful NULLs, stale
locals, terminal immutability, OOM retry, restart dedup, lost-ack quarantine,
opening identity collisions and refusal to relax rounded quantity coverage.
An isolated Redis process also exercises the actual Lua, OOM refusal, recovery,
CAS and durable delivery dedup without contacting production Redis.

The foundation freeze test now distinguishes permitted plumbing changes from
strategy policy: only performance commit and a broker-opening exclusion are
allowed; the remaining authority and settlement AST is still compared exactly
with its frozen parent. Full validation and live deployment evidence are retained
in the task report outside this candidate checkout.
