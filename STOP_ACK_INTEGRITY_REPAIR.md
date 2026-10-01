# Broker stop-protection acknowledgement integrity repair

Branch: `repair/stop-ack-integrity-f629553`  ·  Parent: `f629553`  ·  NOT deployed.

## Problem

Live forensics (`/tmp/june_continuation_reentry_followup_20261001.md`) found 30
`protection synchronization unresolved` pyramid rejections across six campaigns.
In **four** campaigns the broker had **ACCEPTED** stop amendments, yet June never
converted that authoritative acceptance into a locally acknowledged protection,
so F50 correctly (fail-closed) refused to fund continuation. The acknowledgement
lifecycle itself was failing — not ordinary API latency (one OIL campaign held a
stable accepted target for ~7.5 minutes while still reporting `pending`).

## Root cause

1. **Confirm identity predicate (primary).** `winner_protection.protect()`
   matched a stop-amendment confirmation with
   `reply.get("dealId") == record["deal_id"]`. IG returns the **amendment's own**
   deal id at the top level; the amended **position** id appears in
   `reply["affectedDeals"][].dealId`. The equality therefore failed for every
   accepted amendment, so `stop_sync.status` never advanced past `pending` and
   `acknowledged_stop_level` was never set.

2. **No authoritative snapshot reconcile.** When a confirmation was lost or
   ambiguous, nothing consulted the broker **position** snapshot (`/positions`,
   which carries the applied `stopLevel`) to acknowledge a pending request.
   `_live_retry_stop_sync` only re-polled the same confirm (hitting bug #1), and
   `_live_reconcile_positions` fetched `/positions` but ignored the stop level.

The F50 consumer (`defensive_scaling.snapshot`) was already correct and
fail-closed; it was starved of a legitimately-acknowledged floor by the above.

## Repair (minimum, integrity-only)

### `winner_protection.py`
- `reply_targets_deal(reply, deal_id)` — identity helper: a confirm concerns the
  position when the position id is the top-level `dealId` **or** appears in
  `affectedDeals[].dealId`. Identity only; `protect()` still independently
  verifies `dealStatus == ACCEPTED` and the normalized stop level.
- `protect()` matching now uses `reply_targets_deal(...)` instead of the brittle
  direct equality. Everything else (rounding toward protection, software-floor
  retention on reject/timeout/unknown, already-acknowledged early return,
  pending-ref reuse after restart, exception-safe `pending`) is unchanged.
- `reconcile_broker_stop(position, broker_stop_level, broker_deal_id, *, now, log)`
  — pure fail-closed function: given an authoritative broker **position** stop on
  the same open deal that **covers** (equals within 1e-6, or is stronger than)
  the pending request, promote `stop_sync` to `acknowledged` and set the
  acknowledged floor to the **strongest broker-supported** level
  (`strongest(existing_ack, broker_stop)`). It never certifies the software /
  intended floor; if software protection has advanced beyond the broker stop,
  only the broker-supported portion is acknowledged (the stronger software floor
  stays merely `intended`, and `snapshot()` still rejects it under
  "intended floor exceeds acknowledged protection").
- `covers(direction, candidate, target)` — protection-ordering helper.

### `june.py`
- `_live_reconcile_positions()` — in the existing per-leg broker-match loop
  (which already reads each tracked deal's `/positions` row), read the row's
  `stopLevel` and call `reconcile_broker_stop(...)` when `stop_sync` is not yet
  acknowledged. This runs at startup and after failed closes, giving
  restart-safe (contract F) and lost-confirmation (contract B) recovery from
  broker truth. Fail-closed: a missing/incomplete row leaves it unresolved.

No change to F50 fraction/economics, MINDEAL, allocation, spread/ATR, TP /
partial-TP, DPLE/MPD, stop geometry, pyramid trigger, rolling harvest, Build-4,
reversal, max hold, sessions, defensive mode, CB, daily baseline, risk epoch,
sizing/leverage, or the instrument universe. `defensive_scaling.py`,
`continuation_economics.py`, `winner_accounting.py` are byte-identical to f629553.

## Authority contract (as implemented)

- A. ACCEPTED confirm + matching identity (top-level or affectedDeals) +
  normalized stop == target + intended still == target → acknowledged.
- B. Lost/ambiguous confirm + broker position snapshot shows requested-or-
  stronger stop on the same open deal → acknowledged via snapshot.
- C. Broker holds a stronger stop → request is satisfied; acknowledge the
  stronger broker level.
- D. Software advanced beyond the broker stop → only the broker-supported level
  is acknowledged; `snapshot()` continues to reject crediting the stronger
  software floor.
- E. Broker position gone → no snapshot row → nothing manufactured; closure/
  reconciliation handles it.
- F. Restart → acknowledged fields persist; a still-pending request is re-polled
  (`protect()` reuses the deal_ref) or reconciled from the next broker snapshot;
  it never regresses to unresolved merely due to a restart.

## Tests

`test_stop_ack_integrity.py` (38 tests): confirm-identity via affectedDeals, the
four live failed-ack shapes (R9B49TWBB, R9CU8SKAK, R9DRZ9EAQ, R9D8A47AQ),
snapshot reconcile (exact/stronger/weaker/software-beyond/wrong-deal/no-stop/
no-pending/idempotent/restart), `covers` normalization, F50 cases A–D, the
idempotency/race matrix (accept-then-stronger, duplicate confirm, retry-after-
accept no duplicate amendment, lost-confirm-then-snapshot, broker-gone, float-
equivalent), and source-wiring assertions.

Full suite: 1197 passed + 54 (build1/build2 isolated) = 1251, vs 1213 baseline
(+38 new). The only failures are 3 pre-existing date-dependent tests in
`test_build4cd_risk_epoch.py` (hardcoded 2026-09-30 vs current UTC), unchanged by
this work.

## Downstream reality (from forensics; not a strategy change)

Fixing acknowledgement is an **evidence-integrity** fix. Per the forensic offline
arithmetic, under unchanged downstream rules it does **not** by itself demonstrate
any admitted addon: 21 OIL + 3 software-only COCOA polls fail MINDEAL even after
granting the floor; 1 HO fails allocation; 4 COCOA (R9D8A47AQ) reach nonzero F50
size but fail spread cost (3 also fail allocation); 2 polls were on an
already-closed position. The repair makes accepted protection usable; it does not
loosen any economic gate.
