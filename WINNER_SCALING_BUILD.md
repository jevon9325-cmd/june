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
