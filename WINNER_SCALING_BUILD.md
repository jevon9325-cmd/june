# Winner scaling implementation record

Baseline: fb8bb1c02eb94b5a88e913352142244cbb261d6e, verified over read-only SSH
on 2026-09-24 at `/opt/bots/june`. Tracked tree clean; exact tag
`baseline/production-20260923-fb8bb1c`. Service active/running, PID 2699577,
started 2026-09-23 19:34:34 UTC, expected interpreter and june.py path.
This verifies on-disk revision and service continuity, not a hash of loaded Python.
Fresh broker inventory has not been certified in this build. No production writes,
orders, restart, deployment, or push. Development checkout cloned read-only from
production because the saved candidate bundle lacked prerequisite objects.
Checkpoint: `checkpoint/pre-winner-scaling-20260924`.

## Stage A

Lifecycle: `run_live_step` evaluates primary exits first (including TP/partial),
then pyramid exits, then pyramid entry. Entry checks profit gate, defensive gates,
SAR, and the Redis evidence-unlocked leg cap (2/3/4). Addon sizing reuses primary
leverage, with explicit size decay for legs 3 and 4. Removing the outer no-addon
condition restores these already-implemented concurrent legs; cap and unlock
thresholds are unchanged. Addon close removes the leg, releasing its consumption.

`pos_size` consumers: primary opening persistence; pyramid allocation and sizing;
single surviving addon promotion; orphan recovery. Partial close preserves it.
Its existing reservation semantics are retained. Actual primary entry-basis
exposure above that reservation also consumes capacity. Current `ig_size` and
`notional` remain the partial-adjusted deployed basis. The new pure snapshot
computes actual quantities/exposure independently of legacy addon notional.
Legacy addon leverage inherits the primary leverage used by its original path.
Recovered/promoted positions retain their existing conservative reservation;
deciding to reclaim skimmed reservation remains an explicit future policy choice.

Post-rounding addon validation uses quantity times the inverse sizing unit:
commodities use native price times lot; equities use price unit and FX conversion;
FX retains the existing base-unit sizing convention. It checks remaining
allocation, commodity 3.5x oversizing/equity leverage tolerance, and total campaign
margin against usable equity. Missing/nonfinite required metadata fails closed.
Initial sizing and order-body formulas are unchanged. Accepted addon `notional`
now holds fill-sized exposure, with separate `intended_notional`, `actual_notional`,
`leverage`, and `consumed_allocation`. Market slippage can exceed preflight capacity;
it is recorded, never hidden or followed by an invented corrective trade.

Submission intent persists before POST. Unknown outcomes, stale/duplicate deal
identity and lost responses block additional openings across restart, while
protective exits remain available. Explicit rejection releases the intent.
Unknown intent requires broker-evidence reconciliation/operator review before
release; it is deliberately not cleared merely because one inventory poll is flat.
This is conservative retry safety, not exactly-once execution certification.

Validation: 20 new accounting tests; baseline suite 330 tests. Stage verification
also includes existing live accounting/PS1 tests, Python compilation, diff checks,
full diff and caller review. No strategy constants or close payloads changed.

Stage A verified commit: `30eed81`; full offline suite **350 passed**.

## Stage B

Protection comparison is directional (higher for long, lower for short).
Partial TP takes the stronger DPLE floor and preserves previously tightened
stop percentages. DPLE M1/M2 thresholds and 50% peak trail remain unchanged.
Barbie pending position adjustments already explicitly tighten only; unchanged.

The aggregate stop takes the strongest existing absolute floor AND the level
needed to preserve the old legs' estimated liquidation P&L after adding the new
quantity. Same-instrument price-point economics cancel the common dollar
multiplier; this selects no new profit target. Realized P&L is unchanged during
addition and cancels in the comparison. Costs, gaps and slippage are not certified.
The normal opening stopDistance policy is unchanged; post-fill amendments and
software protection may be tighter to enforce the invariant. Broker minimum
distance can prevent immediate synchronization; software protection remains.

`defensive_stop_level`, `defensive_soft_sl`, and `intended_stop_level` express
software/intended protection. `broker_stop_level` / `acknowledged_stop_level`
advance only after an ACCEPTED confirmation matching reference, deal and level.
`stop_sync` stores target, reference, attempt timestamp and status. HTTP success
alone is insufficient. Legacy defensive fields are never certified as broker ack.
Promotion preserves that separation. Rejected/unknown PUTs do not remove the
software floor; pending references survive JSON state and retry after protective
exit checks. No changes to close POST bodies, PS1 semantics or C2b evidence storage.

Stage B adds 19 tests including real exit/partial functions, long/short,
timeout/rejection/lost/stale/duplicate replies, restart and the OIL scenario.
Full offline suite: **369 passed**. Python compilation/3.12 grammar, full diff,
caller review and unchanged strategy assignments are checked before commit.

Stage B commit: `659ca82`. Subsequent telemetry integration review found the
partial-close path passed its local position copy into stop synchronization.
A separately verified follow-up passes the persisted position instead. Two
regressions verify acknowledged and pending fields actually survive in `_live`.
The follow-up index was exported independently of unfinished telemetry work:
**35 targeted/regression tests and all 371 offline tests passed**, with compile
and diff checks. This changes no stop target, threshold or order payload.

Stage B follow-up commit: `4a6b52f`.

## Stage C

Dedicated `campaign_telemetry.sqlite3` beside the module; never the C2b broker
evidence database. Tables: campaigns (summary/extrema), links (account/deal to
stable original campaign), events (deduplicated decisions), samples (cadence).
Normal pre-evaluation samples only: configured `POLL_ACTIVE` (default 60 seconds),
not ticks. Actual elapsed time and quote timestamps (including unavailable ones)
are recorded; operational delays/maintenance change effective resolution.
Decision hooks and after-evaluation snapshots do not manufacture extra price
samples. Direct bid/offer is preferred; otherwise mid/spread reconstruction is
explicitly labeled. Missing side/spread/metadata yields gaps, not zero-cost prices.

Campaign return is estimated realized-plus-open P&L divided by original primary
entry notional (or explicitly first-observed basis when original data is absent).
Dollar/return MAE/MFE and their observed timestamps are prospective sample extrema.
Primary and campaign peaks, maximum/current exposure, quantities, realized P&L
known to June and protection economics are retained. Costs are PROVISIONAL or
UNKNOWN; nothing here certifies complete costs, fills, net finality or tick extrema.
Unknown leg disappearance marks incomplete realized P&L. Promotion retains the
original campaign link. Flatness/closure is labeled local management evidence;
unresolved addon intent prevents a final-close claim. Same-cycle close/re-entry
is handled without requiring a flat callback.

Events cover entry, favorable movement, DPLE M1/M2, MPD activation, TP reached,
partial request/confirmation, MINDEAL fallback, pyramid threshold/proposal,
accepted/rejected/unknown addon outcome, opened/closed legs, reversal signal/exit,
stop/max-hold exits and final campaign close. Threshold events are first-observed;
decision hooks distinguish actual activation from an inferred threshold crossing.
Protection snapshots separate intended, acknowledged and software floors and
estimate liquidation economics before unknown costs/slippage. Event identifiers
deduplicate repeated state/decision observations within retained history.

Retention: 30 days plus global caps of 50,000 samples and 20,000 events; keep at
most 2,000 closed campaign summaries. Active summaries preserve extrema across
sample pruning and restart. Links cascade with deleted summaries. SQLite page
limit is 128 MiB; deleted pages are reused, not VACUUMed during trading. A rollback
journal may add temporary disk usage. No telemetry pruning touches broker evidence.
The 50 ms SQLite lock timeout bounds lock waits; storage/import/serialization
failures are logged by the non-throwing adapter. Tested disk-failure injection and
an actual exclusive SQLite lock cannot veto protective close. No telemetry writes
to Redis or trade state. C2c remains unwired.

Comparisons supported: path around TP/skim/addon decisions, retained/residual
quantity and exposure, realized estimates, sample MFE giveback, and planned P&L
at each protection change. Counterfactual strategies still require an offline
simulator and cannot recover unobserved post-close prices, intracycle extrema,
or unknown costs. Do not interpret these observations as randomized causal proof.

Stage C validation: 28 new tests, all 399 offline tests; telemetry/evidence
regressions, compilation, Python 3.12 grammar, diff/caller review. AST comparison
with telemetry calls removed and the evaluation wrapper unwrapped finds **zero
changes to existing trading function bodies**. Strategy assignments are identical.

Stage C commit: `572fb32`.

## Stage D acceptance audit

`python -X utf8 audit_winner_scaling.py` parses all 38 Python files under Python
3.12 grammar and compiles them without importing June. Local execution uses Python
3.14; production was freshly read-only verified as 3.12.3. Runtime execution under
production Python, fresh broker inventory and live amendment behavior remain for
the separate controlled deployment audit; no live behavior is certified here.

The audit asserts all module assignments unchanged, primary opening/size/stop
functions unchanged after removing observation calls, unchanged initial entry
ranking, unchanged opening and close body definitions, unchanged PS1 POST/DELETE
implementation and unchanged capture/replay/reconciliation functions. All 11
existing `broker_*.py` modules are byte-equivalent after newline normalization.
No C2c import or integration was introduced. Stage C's isolated AST comparison
shows no trading function changes after removing its observation hooks.

Acceptance coverage additionally found and fixed malformed addon sizing metadata
escaping a guard; numeric primary allocation/leverage are validated before use.
A live-path test confirms all four legs use the existing size-decay schedule and
cap. Telemetry prefers retained opening-account evidence over a changed current
session, and labels the fallback as session-only. An incomplete accepted addon
confirmation now explicitly preserves its raw response/confirmation in C2b before
failing closed, with no fabricated deal identity and no loss of pending intent.

Final validation: **403 offline tests**, including **73 new tests** over the 330-test
baseline, plus the AST/grammar/payload audit and diff checks. All existing C2a/C2b,
PS1 and C2c test modules are included in discovery. Storage/restart compatibility
is exercised by JSON round trips, legacy positions without new fields, telemetry
SQLite restart/pruning/real-lock tests and the unchanged broker-evidence suites.
New runtime modules use only the Python standard library and each other; no new
third-party package is needed. SQLite telemetry is additive, in a separate file;
existing Redis TTL/key and broker SQLite schemas are unchanged.

Acceptance checklist:

| Requirement | Result |
| --- | --- |
| Initial sizing, tier percentages, initial leverage mapping | Unchanged |
| TP, partial fraction, pyramid trigger and size-decay schedule | Unchanged |
| DPLE activation/trail and MPD thresholds | Unchanged |
| Reversal patience, max hold, entry scoring/ranking, universe | Unchanged |
| PS1 close payloads/DELETE and evidence guards | Preserved |
| C2b evidence preservation | Preserved; unresolved addon capture added |
| C2c | Dormant/unwired |
| Telemetry cannot veto exits | Failure-injection and real SQLite lock tests pass |
| Failed/unknown broker PUT is not an acknowledgement | Matching accepted confirmation required |
| Protection monotonicity | Directional price and planned campaign P&L invariants tested |
| Final rounded addon exposure | Recomputed, budget/oversize/margin checked |
| Duplicates/restart | Pending intent, cap, identity and state-roundtrip tests pass |

Order payloads: **YES, stop-amendment values can change** to retain stronger
protection. PUT keys/shape remain the same. Initial primary/addon opening body
definitions and close payloads are unchanged; some formerly unsafe addon attempts
are now rejected before sending. Initial sizing and strategy sizing policy are
unchanged; accounting/capacity enforcement has been repaired.

Final production recheck: still `fb8bb1c`, clean tracked tree, PID 2699577 and start
time 2026-09-23 19:34:34 UTC unchanged. No deploy, push, restart, production Redis
write or broker order. Fresh broker inventory: **NOT CERTIFIED** (no live broker
session was acquired or queried during this build).

Remaining limits: unknown addon submissions deliberately require broker-evidence
review before releasing their pending gate; they are not automatically retried.
Primary reservation is not reclaimed after skim. Recovery/promotion reservation
semantics remain as before and are not silently reinterpreted. FX sizing retains
the pre-existing base-unit convention; the baseline's structural live FX exclusion
is unchanged. Quote-to-fill slippage can exceed a preflight budget; estimated
protection cannot guarantee realized net P&L through gaps, costs or outages.
