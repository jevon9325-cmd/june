# Confirmed allocation and protected continuation

Baseline: `76951a7121c44ae6b35b7698c6bee2f62f5d581b`.

## Allocation

A matched accepted partial plus broker-observed residual quantity creates an
`allocation_confirmation` on the primary. It contains the original quantity and
reservation, confirmed residual, deal/direction, and confirmation reference.
The certificate is saved with the existing live-state snapshot.

Allocation charges the larger of proportional original reservation and residual
entry-basis exposure divided by leverage. A larger local or broker-observed
quantity wins. Pending partials retain the original reservation and original
quantity basis. Pending addon submissions reserve all remaining campaign capacity
until resolved; the existing submission lock still prevents another order.

Repeated calculations do not subtract a release: they derive the charge from the
same certificate. Restart preserves that calculation. Legacy positions without a
certificate retain the original reservation. Local flags, a sent request, realized
profit, or an accepted receipt without a matched residual cannot certify release.
No realized profit or F50 credit is created.

## Continuation spread treatment

Fresh-primary admission, thresholds and logging are unchanged. Unprotected addon
admission retains its existing ratio gate. A certified profitable campaign may
reach the protected continuation economic gate despite a high spread/ATR ratio.

The final existing F50 candidate must satisfy both:

1. Current spread percentage is no greater than the existing spread anomaly
   factor (3) times the rolling spread average, with the existing minimum five
   readings. Invalid/missing history, quote or cost metadata fails closed.
2. Dollar reserve is no greater than half the net broker-acknowledged protected
   profit:

   `q * multiplier * (nominal_stop_distance + spread + 2 * slippage) + commission`

This adds an explicit conservative spread reserve to the existing nominal
fill-to-stop loss reserve. It does not claim that a stopped trade necessarily pays
spread twice. The absolute dollar ceiling is the unchanged F50 budget; ATR does
not enlarge that budget. No new forecast, expected profit, MFE credit, spread/ATR
threshold, or F50 fraction is introduced.

The implementation validates the existing candidate; it does **not** downsize a
proposal to jointly fit spread, allocation or margin. All three retained SUGAR
funding groups afford one MINDEAL with costs. Of the 28 observations, the first
four existing .03 candidates fit the added cost reserve; the later .06/.08
candidates exceed it. Those 24 stay rejected. This is not a profitability claim.

Immediately before proceeding, a fresh broker inventory must match all campaign
deals, quantities, directions and instruments. Its stops must cover the locally
acknowledged floors used for funding. Protection is rechecked after the read;
authority, closure and unresolved-state guards remain binding. Missing/extra
inventory or disagreement blocks admission. No broker stop is amended to create
funding before submission.

A primary full-close attempt invalidates its continuation-history eligibility,
matching the existing conservative treatment of addon close attempts. This keeps
an unknown close outcome from funding a new protected addon. Exit conditions,
order contents and settlement/reconciliation behavior are unchanged. A rejected
full close also leaves continuation disabled for that campaign; protection and
exit management continue.

## Telemetry

Existing bounded `pyramid_decision` records now contain allocation details,
actual spread/ATR and threshold, per-component dollar reserves, F50 budget,
MINDEAL, candidate quantity, protection evidence and inventory verification.
The final decision/reason continues to distinguish approval from later rejection.

- `allocation_released_confirmed_partial`: certificate event and allocation reason.
- `allocation_retained_unresolved_partial`: retained basis or missing residual identity.
- `allocation_retained_pending_exposure`: remaining capacity reserved for uncertainty.
- `continuation_absolute_spread_veto`: existing widening criterion exceeded.
- `continuation_absolute_cost_veto`: full candidate reserve exceeds half protected P.
- `protected_continuation_economic_rejection`: missing/invalid evidence, with detail.
- `low_atr_continuation_admission`: spread economics passed despite ratio rejection;
  consult the final decision to establish submission or actual fill.
- `continuation_economics_admission`: economics passed without ratio override.
- Fresh-primary `spread_atr` block logging and unprotected addon `spread_atr_gate`
  remain compatible.

No extra per-cycle journal logging is added. A certificate event is emitted only
when the existing partial-confirmation path completes.

## Verification and exclusions

`test_confirmed_continuation.py` exercises residual certificates, pending and
ambiguous states, restart/idempotency, conservative disagreement, cost reserves,
the three SUGAR funding groups, abnormal spreads, actual admission with mocked
broker/storage operations, and excluded-policy invariants against the baseline.
Existing suites cover TP, DPLE, MPD, unsplittable TP, stops, reversal, maximum hold,
rolling harvest, Build-4, CB, kill switch and account/tier risk.

Two existing tests were made portable: an absolute Linux source path now uses
the test's adjacent source, and a git-source read explicitly decodes UTF-8. An
approval fixture uses a smaller explicit spread with matching history and broker
inventory so the newly reserved spread fits its unchanged F50 sizing.

No probe, unsplittable-winner transition, joint F50 sizing repair, instrument
discovery or dead-candidate policy is included.

## Deployment gate

Deploy only after green validation, narrow diff review and a fresh account
snapshot. The 2026-09-30 19:57:31 UTC snapshot showed baseline HEAD unchanged,
service active, NRestarts 0, broker/local flat, USD134.73 balance/equity, live
enabled, no CB alert, no pending addon or manual-review state. However, 20 retained
trade-history settlements remained PROVISIONAL with unknown P&L. The requested
"no unresolved broker truth" gate is not satisfied; no deployment or restart is
authorized by this document. Do not clear or relabel those records to pass it.

After that blocker is resolved or the user explicitly clarifies its scope, take
another fresh snapshot and follow the established backup, flat-state cutover,
production-runtime smoke test and post-start observation procedure. Never rely
on the earlier snapshot as permission for a later cutover.

## Final validation and latest gate snapshot

- Baseline suite: 1,105 passed, 2 pre-existing environment failures, 52 warnings,
  108 subtests. The failures were the absolute Linux test path and Windows text
  decoding documented above.
- Added: 79 tests, including parametrized cases.
- Focused final repair/crash suites: 156 passed, 31 subtests.
- Final full suite: 1,186 passed, zero failures, 52 existing datetime deprecation
  warnings, 108 subtests. Command: `python -X utf8 -m pytest -q`.
- Syntax/AST and `git diff --check`: passed. Tests ran offline on local Python
  3.14 with broker/storage mocks. Production Python 3.12 smoke/cutover testing
  was not performed because the account deployment gate remained blocked.
- Crash fixtures now inject after actual clear/promotion, preserving their
  lifecycle assertions despite the new pre-close continuation-disable save.
  A separate new test covers a crash at that earlier save.

The fresh post-validation snapshot at **2026-10-01 00:39:48 UTC** supersedes the
flat snapshot above. Production remains at the baseline, clean tracked files,
active PID2924018, NRestarts0. One GOLD short, quantity .08, deal
DIAAAAR9BBSRMAT, is present in both broker and local state with stop4147.09.
No pending addon, partial, manual-review, orphan or recovery-unresolved flag was
present. Live enabled=true; CB alert absent. Broker cash133.05, deposit1.15,
unrealized P&L-.08 and available131.82 reconcile. The local balance_total133.09
is a separate poll mark and is not presented as simultaneous broker equity.

There are now **21 PROVISIONAL retained settlements with unknown P&L**. The
requested no-unresolved-broker-truth gate remains unmet. The open GOLD position
is not the reason for withholding deployment, and was not closed for convenience.
No deployment, restart, broker order, direct Redis/SQLite mutation, block clearing
or daily-baseline reset was performed. June's own normal trading continued.

Roadmap only: Protected Continuation Probe; unsplittable-winner transition;
F50 joint quantity sizing if live evidence emerges; dynamic instrument discovery;
untradeable/dead-candidate hygiene.
