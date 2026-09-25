# June dynamic defensive / protected winner scaling

## Baseline and isolation

- Authoritative baseline: `d805ac592aac34a51d79680456dd12d1b1e3de87`, tag `baseline/production-20260924-d805ac5`.
- Build branch: `build/dynamic-defensive-20260924` in `C:/Users/jevon/trading_alerts/june-dynamic-defensive`.
- Separate clone, clean baseline verified before edits; baseline is an ancestor of the build.
- Rollback checkpoint: `checkpoint/pre-dynamic-defensive-20260924`.
- Production checked over read-only SSH before editing and again at `2026-09-25T00:15:19Z`: HEAD and tag both equal the baseline; tracked tree clean; service active, PID `2711137`, start `2026-09-24 13:28:00 UTC` unchanged.
- No deployment, restart, production Redis writes, broker requests/orders, or remote Git writes performed.
- A: `72dd4ca` account state-machine correctness.
- B: `63fe9d8` acknowledged protection funding and winner decision.
- C: the commit containing this report: telemetry, final acceptance and tests. Use `git rev-parse HEAD` for the complete final identifier.

## Stage A: targeted interaction findings

1. The 1,800-second global timeout was an independent rule. It was not coupled to SAR sub-session buckets, overnight range tracking, spread/ATR gates, or UTC CB baseline timing. The same constant also served independent instrument win-or-time recovery.
2. Actual-improvement hysteresis already existed: recovery strictly below half the defensive activation threshold. The global timeout bypassed it. This build reuses that band; no replacement drawdown threshold was invented.
3. The updater was below the surviving-primary return in `_run_live_step_observed`. It now runs after account polling and before exit evaluation in a guarded block. Failure denies new exposure only; primary exits, addon exits and stop retries continue. The existing account polling cadence remains 300 seconds; freshness allowance is that cadence plus `POLL_ACTIVE`, not a new trading threshold.
4. Fresh entries retain global/instrument defensive+neutral restrictions. Previously addons duplicated both neutral restrictions without evaluating protection. Now account or instrument defensive mode requires acknowledged economic protection and an independently funded addon floor. Normal unprotected addons retain the previous controls; a claimed nonnegative floor in normal mode must meet the same funding proof.
5. Global and instrument defensive flags duplicated the same macro-neutral addon veto. They now share one economic decision without merging either state. Performance/SAR/session blocks remain independently authoritative for new exposure. They can overlap with defensive state, but use different evidence and are not bypassed.

No material contradiction with the supplied established findings was found. This build does not claim realized opportunity cost from the earlier observed campaigns.

## Account state machine

`defensive_state.update` validates finite equity and a fresh existing REST sample. Normal activates at the existing dollar/percentage threshold. Defensive recovers only with account improvement below the existing half-threshold band. Time alone cannot recover it.

An active episode retains its account reference across UTC rollover. Merely reseeding the CB day-start balance is not recovery. Recovery must clear the episode band and today's band. Legacy defensive state adopts the existing day-start reference; the polling path preserves that reference before changing the day baseline. Circuit-breaker baseline seeding, thresholds, evaluation placement, alerts and manual re-enable semantics are unchanged. Instrument daily counters and win-or-time recovery are separate and unchanged in policy.

State/reference fields persist through the existing live-state snapshot. Invalid/stale inputs preserve the current mode and block new risk for that cycle. They do not block exits. Account freshness is sampled REST freshness, not a claim of tick-current equity. Existing polling/storage failures outside this new state calculation were not broadly refactored.

## Protected winner policy and input reliability

The new pure `defensive_scaling` module has three operations: classify acknowledged economics, plan funding, verify acknowledged funding. There is no scoring formula.

| Input | Accepted meaning / limitation |
| --- | --- |
| Filled price, actual quantity, identity | Existing accepted opening/recovered metadata; missing, nonpositive, duplicate or incompatible legs fail closed. |
| Executable price | Existing bid/offer, otherwise explicitly labelled mid/spread reconstruction. Crossed quotes rejected. Synthetic equity spreads cannot certify execution economics. |
| Dollar multiplier | Existing inverse-sizing metadata for linear commodity/equity CFDs. FX allocation basis is not certified USD P&L, so it cannot qualify for the new protected route. |
| Intended DPLE/MPD/aggregate floor | A constraint to preserve; never proof of broker protection. The DPLE breakeven flag alone may describe a loss and is not sufficient. |
| Acknowledged stop | Existing `broker_stop_level` from accepted opening/position evidence or `acknowledged_stop_level` from matching accepted stop confirmation. Pending/rejected/wrong-deal synchronization or an intended floor stronger than acknowledgment refuses the addon. |
| Campaign economics | Sum of active-leg stop P&L, less existing MPD slippage reserve and known full round-trip equity commission. Positive realized partial profits are not spent; negative partial estimates are debited. Missing partial economics fail closed. These are conservative estimates, not broker-certified net P&L. |
| Closed-leg history | Fresh primary campaigns carry `scaling_history_complete`. Legacy/recovered/promoted or close-attempted campaigns without complete retained economics cannot qualify; no telemetry database is treated as authoritative trading state. |
| Financing | A position spanning the existing UK 22:00 financing cutover cannot qualify without cost evidence. This is an evidence restriction, not a new exit/session rule. |
| Allocation, MINDEAL, leverage | Existing campaign accounting and final rounded-size validation remain authoritative and run before funding amendments. |
| Conviction | Historical entry conviction is recorded, not misrepresented as a fresh conviction measurement. |
| Macro | Recorded. Neutral macro alone never refuses an otherwise qualified protected campaign. |

Classification uses estimated acknowledged liquidation economics: below zero = unprotected, zero = breakeven protected, above zero = profit protected. `1e-9` is numerical comparison tolerance, not an allowed loss budget. A current cost-reserved winner is also required.

Before POST, the proposed addon's initial attached-stop loss, entry/exit slippage reserve and known commission must be funded by tighter **acknowledged** stops on the old legs. The proposed stop distance is the existing opening payload's distance, converted through the existing metadata. Feasibility uses the existing broker minimum-stop calculation, including percentage minima. Existing MPD slippage constants are reused conservatively in native units; no new fixed amount is introduced.

For old quantity Q, dollar multiplier M and addon loss reserve L, each old stop tightens by `L / (Q * M)` in the protective direction. The old legs' acknowledged liquidation floor must then cover both L and their original floor. The estimated campaign floor therefore remains at least its pre-evaluation value, including during the interval before the new leg's aggregate amendment. Nonnegative after-addon economics alone is not enough if it would spend an already locked positive floor.

The addon is refused if those stops are infeasible, unacknowledged, stale/ambiguous, or fail the economic recheck. Trade authority and LS closure evidence are checked again after funding acknowledgments. Tightenings already made remain in place even if the addon is refused; this can tighten an existing winner earlier than before. That is an intentional preservation-of-floor tradeoff, not an initial-sizing change.

After an accepted fill, the existing `campaign_stop` and monotonic stop-synchronization code still protect DPLE/MPD/aggregate floors. The existing durable pending-intent-before-POST rule remains; pending risk context is retained with it. Accepted quote fingerprints prevent the same evaluation from opening another addon after restart. A changed quote can qualify for the next leg, subject to the existing leg cap and capacity. No size, leverage, MINDEAL or campaign-cap constants were changed.

One pre-existing regression expectation was deliberately updated: normal-mode MPD with an unacknowledged profit floor now refuses the addon rather than opening and relying only on later software protection. This implements the requested protection-evidence invariant; existing PS1/C2b tests remain unchanged.

## TP and pyramid independence

The `0.0015` / 0.15% gate is unchanged. TP and pyramid checks remain conceptually separate, with the existing exit-first evaluation order.

Current primary TP calls the existing partial-exit path, not an unconditional full campaign close. The residual already has no fixed TP ceiling. Partial-exit sizing/finality fallbacks can still cause a full close, and addon TP still closes that addon independently. This build leaves all of those mechanics intact. Removing those fallbacks or making aggregate protection the sole post-scaling authority needs a separate strategy/PS1 design because aggregate broker synchronization can remain unresolved.

## Observability and bounded storage

Every invocation reaching the 0.15% gate emits `pyramid_decision`, including early rejections for caps, pending state, authority, performance/session, market closure, pause and SAR gates. Context contains timestamp, instrument/direction, account/instrument modes, macro, entry conviction, current economics when certifiable, intended/acknowledged protection, before/after floor, proposal allocation/notional/leverage/quantity, capacity, funding targets, decision, exact reason/detail and submission outcome. Unavailable values are null with an evidence reason; an early-vetoed proposal is not fabricated.

Account transitions record prior/new state, equity, reference/day-start, loss, activation/recovery levels and reason even while flat. They use NULL campaign identity in the existing event table rather than inventing a campaign. Both event types share existing SQLite pruning: 20,000 events, 50,000 samples, 30-day retention and 128 MiB page limit. Existing event/snapshot protection observations continue. Telemetry failures remain best effort and never grant/veto protective exits; disk-full and locked-store behavior is tested.

## Tests and review

Baseline: **453 passed, 108 subtests passed** on local Python 3.14. Prior deployment evidence used a different runner/count; this report uses the actual same-runner baseline.

Final: **511 passed, 108 subtests passed**; **58 added test cases**, one existing protection expectation strengthened as described above.

The four warnings come from the unchanged CB alert's deprecated `datetime.utcnow()` call, newly exercised directly. Production modules also pass Python 3.12 grammar parsing; actual runtime tests were local Python 3.14, not the production interpreter.

- A boundary: compile, 73 relevant tests (including 15 new state-machine cases), diff check and clean commit/status; a final targeted state run also passed before commit.
- B boundary: compile, 84 relevant tests plus 6 subtests, diff check, full B diff review and clean commit/status.
- C/final: full offline suite, compileall, `git diff --check`, complete production diff review, AST/function-scope review, constants comparison, order payload comparison and caller checks.
- `audit_dynamic_defensive.py` regenerates `DYNAMIC_DEFENSIVE_AUDIT.json`. Exactly seven existing June functions change; one new integration helper is added. Initial/close order dictionaries are AST-identical. CB, initial entry/sizing, primary/partial/addon exit evaluation, performance/session, reconciliation, winner accounting/protection and broker C2c modules remain unchanged except the explicitly listed metadata/decision integration.

Deterministic coverage maps to all requested areas: held activation/recovery, timeout non-recovery, stale/invalid equity, updater/storage failure with exits, fresh-risk restriction, unprotected and breakeven/profit campaigns, neutral macro, unresolved broker sync, infeasible economics, capacity/MINDEAL, preserved protection, duplicate/restart behavior, session independence, actual circuit-breaker thresholds, existing PS1/C2b tests and telemetry/storage failure.

## Explicit acceptance answers

1. **Can global mode become defensive while a position is open? YES.** Updated on each live evaluation using the latest valid account sample.
2. **Recovery only from real evidence, not timeout? YES.** Fresh equity must clear the existing hysteresis bands; neither timeout nor UTC rollover suffices.
3. **Can defensive computation suppress protective exits? NO.** Failure blocks new risk only. Existing unrelated management failure modes are not claimed repaired.
4. **Fresh risk more restricted than protected scaling? YES.** Fresh defensive+neutral entry remains blocked; qualified protected scaling is permitted.
5. **Can unprotected winners bypass defensive controls? NO.** Directional macro is not a bypass.
6. **Can intended/unacknowledged protection qualify? NO.** Acknowledgment and economics are both required.
7. **Can genuine profit-protected campaigns scale in defensive+neutral? YES.** Subject to feasible acknowledged funding, independent gates, capacity and complete evidence.
8. **Can an addon make protected economics materially negative? NO in the admission estimate.** It must preserve the original floor, not just zero. Real market gaps, fills and non-guaranteed-stop slippage cannot be ruled out offline.
9. **Can MINDEAL/capacity be bypassed? NO in pre-submit sizing.** Final rounded proposed quantity uses the existing validators. MARKET execution is not a guarantee against fill-price movement changing realized exposure.
10. **Can addon synchronization weaken protection? NO planned/acknowledged old floor is lowered.** Initial addon risk is funded before POST; failed subsequent sync preserves software floors and prevents claiming confirmation. Broker gaps or failure to honor requested protection remain live risks.
11. **Session-performance blocks independent? YES.** Unchanged functions and independent blocking tests.
12. **Circuit-breaker semantics changed? NO.** Function, placement, constants and broker-independent controls are unchanged; actual threshold tests pass.
13. **Initial sizing changed? NO.** Initial sizing/entry payloads are unchanged; only a history-provenance field is added to fresh primary state.
14. **0.15% threshold changed? NO.** AST constant comparison passes.
15. **Broker opening/closing payload semantics changed? NO.** Same order dictionaries and transport paths. Existing stop-PUT operations now also run before a protected addon as deliberate funding amendments.
16. **C2c still unwired? YES.** No broker reconciliation/finality/cost-module or production wiring changes.
17. **Unproven without live trades:** attainable funding stops under real spreads/minima, acknowledgment availability/latency, native-unit metadata correctness at execution, actual slippage/gaps/commissions/funding, inventory races between old-stop acknowledgment and new entry, operational opportunity rate and profitability. No live opportunity-cost or performance improvement is claimed.

## Handoff

STATE-MACHINE FIX: fresh sampled-account hysteresis runs while holding; recovery requires account improvement; active episode survives midnight/restart.

DEFENSIVE SCALING POLICY: current economic winner, acknowledged nonnegative protection, existing capacity, feasible pre-entry funding acknowledgments preserving the original floor; otherwise addon-only refusal.

WHAT COUNTS AS PROTECTED: acknowledged liquidation estimate after cost reserves, with complete active-campaign evidence. Flags and software intent do not qualify.

FRESH RISK VS WINNER SCALING: fresh defensive rules preserved; protected neutral-macro scaling permitted. Normal claimed nonnegative protection is also safeguarded.

TP/PYRAMID INTERACTION: unchanged; separate follow-up required to replace partial/full-close fallback semantics.

0.15% GATE: UNCHANGED.

SESSION MECHANISMS: independent and unchanged. Instrument recovery timeout retained; global timeout removed.

TELEMETRY: existing bounded campaign/event architecture extended; no parallel logging store.

STRATEGY CONSTANTS CHANGED: NONE. Existing hysteresis and MPD buffers reused; zero protected-loss floor is an explicit invariant, not an empirical tuning threshold.

SIZING POLICY CHANGED: initial and nominal addon size/leverage formulas unchanged; addon admission/funding policy changed. No new size-decay rule.

ORDER PAYLOADS CHANGED: NO opening/closing payload changes. Additional monotonic stop amendments before protected addon admission.

CIRCUIT BREAKER CHANGED: NO.

C2c WIRED: NO.

KNOWN LIMITATIONS: estimates and sequential, non-guaranteed broker execution; evidence restrictions above; prefinancing can tighten a winner even when entry is later refused; no live efficacy claim.

READY FOR DEPLOYMENT AUDIT: YES. This is not deployment approval or live certification.

PRODUCTION MODIFIED: NO. DEPLOYED: NO. RESTARTED: NO. PRODUCTION REDIS WRITES: NO. BROKER ORDERS: NO.

NEXT: independent deployment audit of this isolated branch, especially native-unit stop-distance conversion, acknowledgment/funding sequence and evidence exclusions. Deploy only under separate authorization.
