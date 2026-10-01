# Winner continuity + joint addon sizing — forensics, design, and the
# observation-only implementation

Branch: `feature/winner-continuity-joint-sizing-6a4b27e` · Parent: `6a4b27e` · NOT deployed.

## Question

1. Should June support a broker-protected UNSPLITTABLE WINNER CONTINUATION
   transition (instead of full-closing a 1×MINDEAL winner at TP)?
2. Should first-addon sizing be JOINTLY constrained (F50 ∧ allocation ∧ margin ∧
   MINDEAL ∧ cost/risk) rather than computing an F50 maximum and rejecting it?

## Verdicts (evidence-based)

Both behavioral changes are **NOT justified for implementation on current
evidence**. The only evidence-supported change — and the one implemented here —
is **observation-only telemetry** that records readiness and joint-sizing
diagnostics without altering any trading decision.

### Unsplittable winner continuation — PLAUSIBLE BUT UNPROVEN

- Source (6a4b27e, `_live_partial_tp_exit`): a 1×MINDEAL winner at TP cannot
  split (both halves must clear MINDEAL) → `mindeal_full_close_fallback` →
  `_live_close_position("take_profit")`. The campaign ends; addon evaluation is
  never reached; a subsequent same-direction re-entry is a fresh campaign that
  pays spread/slippage again and resets stop/TP.
- HO replay (retained forensics): four winner→immediate-reentry pairs; winners
  +$4.29, re-entries −$2.14, pairs net +$2.15. **Every winner's protective stop
  was eventually crossed.** HO#1 (`87SH6TA6`) had a *pending* (not acknowledged)
  stop at TP — a protection-gated continuation would NOT have admitted it. HO#2
  (`88LC4PAZ`) was acknowledged but reversed below the acknowledged stop within
  ~5 minutes. The 60-minute marks favor holding only if the crossed protective
  stops are ignored, which is not admissible.
- A safe continuation contract is designable (broker-acknowledged protection,
  net protected profit, explicit campaign giveback budget, retained
  emergency/reversal/max-hold/CB authority, re-entry suppression, restart-safe
  state, exactly-once settlement). But the evidence does not demonstrate it would
  improve results, and it would change trading decisions on an unproven edge.
  **Deferred.**

### Joint addon sizing — INSUFFICIENT EVIDENCE

- Source (6a4b27e, `_live_add_pyramid_leg`): the ordering is max-first — F50
  computes `_f50_ig` from protected profit (0.5 × liquidation_before), the
  notional is overridden to that size, and `validate_addon` checks
  allocation/margin/oversize *afterward*. So the structure the question describes
  does exist.
- But retained forensics: `F50_OVERSIZED_PROPOSAL_REJECTION = 0 actual events; 0
  reconstructed eligible maxima failing allocation/margin`. In every fundable
  snapshot the F50 maximum already fit allocation and margin, so a smaller legal
  quantity would never have rescued a rejected maximum. At the current account
  size F50 funds far less than one lot while allocation head-room is ~$20–24, so
  allocation never binds the F50 result. The change is theoretically cleaner but
  **empirically inert** on all retained evidence. **Deferred.**

## What was implemented (OBSERVATION ONLY)

New pure module `winner_continuity.py` (no I/O, fully unit-tested):
- `unsplittable_readiness(...)` → READY/NOT_READY verdict + protected_gross/net,
  current_value, giveback_budget. Fail-closed; requires broker-ACKNOWLEDGED
  protection (never a software floor); only applies to genuinely unsplittable
  positions.
- `joint_sizing_diagnostic(...)` → q_f50 / q_allocation / q_cost / q_joint /
  binding_constraint / joint_lt_f50, all MINDEAL-quantized.

`june.py` (+98 insertions, 0 deletions):
- At the `mindeal_full_close_fallback` branch, emit `unsplittable_winner_readiness`
  telemetry **before** the existing full close — which still executes unchanged.
- In the F50 addon path, emit `joint_addon_sizing_diagnostic` telemetry alongside
  the existing decision — the selected size remains exactly `_f50_ig`.
- Both are wrapped so any error is swallowed; neither can affect a trade.

### Guarantees

- No trading decision, order size, exit, gate, or threshold changed.
- Broker-protection authority, F50 fraction (0.50), MINDEAL, CB, sessions,
  fresh-primary sizing, DPLE/MPD, stop geometry, reversal, max-hold, instrument
  universe, settlement authority, stop-ack authority, Build-4 cap: all unchanged.
- `defensive_scaling.py`, `continuation_economics.py`, `winner_accounting.py`,
  `winner_protection.py`, `settlement_reconcile.py`, `rolling_build4a.py`:
  byte-identical to 6a4b27e.

### Accounting invariants (preserved, unchanged)

Protected profit is not spent twice; allocation is released only on confirmed
exposure reduction (4e22d32); a continuation would not release primary
allocation; realized harvest fuel and open protected profit remain distinct; F50
funding and Build-4 realized fuel remain distinct ledgers. The observation-only
telemetry touches none of these.

## Tests

`test_winner_continuity.py` (23 tests): readiness READY/NOT_READY across
software-only, no-ack, not-protective, below-minimum, splittable, ambiguous
(manual-review/orphan/pending-partial), missing-metadata, long/short; joint
diagnostic for F50-fits-allocation (no rescue), allocation-binds, cost-binds,
mindeal-blocked, boundary, insufficient-metadata; and AST wiring assertions that
the readiness observe precedes the unchanged full close and the F50 selected size
is unchanged.

Full suite: 1220 + 54 (isolated build1/build2) = 1274 passed (+23 vs the 1251
baseline). The only failures are the 3 pre-existing date-dependent
`test_build4cd_risk_epoch` tests (hardcoded 2026-09-30 vs current UTC), unchanged
by this work.

## Next recommended step

Deploy this observation-only build in a future authorized session, then collect
live `unsplittable_winner_readiness` and `joint_addon_sizing_diagnostic` events
across many campaigns. Only if that data shows (a) a material population of
READY unsplittable winners whose protected continuation would beat full-close
after costs, and/or (b) real `joint_lt_f50` events where allocation/margin binds
below the F50 maximum, should the corresponding behavioral change then be
designed, gated and separately authorized.
