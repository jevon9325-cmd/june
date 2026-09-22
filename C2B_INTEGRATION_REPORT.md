# Approved pinned-main integration and C2b-2 recovery

Authority: user attachment `43aae812-e95f-4418-a041-de1b946798c9/pasted-text.txt`.
This report supersedes older continuation instructions where they describe the
pre-integration checkout. No production access or modification occurred during
this approved implementation turn. In particular, the prior 16:16 UTC flat
broker snapshot is historical and is not assumed current.

## I1/I2: independently verified integration

Original checkout `C:\Users\jevon\trading_alerts\june-broker-truth-repair` remains
on `repair/broker-truth-ledger` at
`b8922714cebc8508d58c42ed98dcd6832d5b164a`, with its seven staged files and eleven
additional unstaged test assertions untouched. The original staged/unstaged
binary patches, exact working-file snapshots and SHA256 manifest remain in
`C:\Users\jevon\trading_alerts\integration-audit-20260922-110855`.
The preservation verifier passed before integration and at final verification.

A separate clone with independent Git objects (`--no-hardlinks`) was created at
`C:\Users\jevon\trading_alerts\june-broker-truth-integration`; origin was set to
`https://github.com/jevon9325-cmd/june.git`. Branch:
`integration/broker-truth-main-20260922`. Starting HEAD b892271 and clean status
were verified before merging pinned
`55f41967a2b7537e7576970941a945bfeae0864c` with `--no-commit --no-ff`.
No moving remote reference was merged; no old LS/orphan commits were replayed.

Merge commit: **5084f695252c3b0a35862eeaf20471a9d31918fc**.
Parents, in order: b8922714cebc8508d58c42ed98dcd6832d5b164a and
55f41967a2b7537e7576970941a945bfeae0864c. Conflict-free merge; delta against first
parent is only june.py, 12 insertions and 2 deletions, exactly the pinned
SOYBEANS override. Full staged diff/stat/status and whitespace reviewed.

All Python sources compiled in memory, and **108 integration-only tests passed
in 65.649s** before any C2b-2 patch was applied. Three SOYBEANS characterization
probes executed against the integrated AST and passed. Those verify native
minimum-notional/sizing agreement, only two override readers, and the already
documented latent cached-price fallback mismatch (the latter is not fixed).

An AST transformer removing ONLY the known SOYBEANS dictionary and two formula
changes exactly reproduced b892271. Consequently all prior strategy constants,
sizing, entry/exit conditions and broker order payloads remain intact; the only
policy delta is the existing production eligibility/cache change. C2a metadata,
partial/residual accounting and C2b-1 targeted pending access survive unchanged.

## I3: exact draft reconstruction and bounded additions

After I2 passed and the merge was committed, applied the preserved staged patch,
then the unstaged patch, each after `git apply --check`. No wholesale copy of the
dirty original checkout. All reconstructed non-June contents match the saved
working-file copies after CRLF normalization, including the 11 extra test
assertions. June differs by the intentional SOYBEANS merge only. June remains
CRLF; Git's canonical blobs use LF. No formatting churn is included.

Runtime C2b code is the preserved draft: `broker_capture.py` and 78 additive
lines in June. Additional work in this turn is storage-boundary tests, this
report, a continuation note in BROKER_TRUTH_REPAIR.md, and correction of the
pending module's stale integration-status docstring. No new economic algorithm
or trading behavior was added during reconstruction.

## SQLite / Redis architecture and failure policy

SQLite is an append-only local evidence spool, not a second economic ledger.
Content-derived IDs deduplicate detached immutable envelopes. It uses a normal
rollback journal and `synchronous=EXTRA`; commit returns before the next
destructive runtime statement. Lock contention has a 100ms SQLite busy timeout.
That timeout bounds lock waiting, not arbitrary disk/filesystem stalls.

Redis receives pending raw evidence through PendingCloseStore. Its account hash
uses targeted HGET/HSET and retains ownership/delivery identities. No new runtime
history reconciliation, projection or learning consumer is connected. Only later
account-pinned broker history with sufficient identity/cost evidence can establish
an economic result. A close intent, accepted response, LS observation, margin
inference, or cleared active state cannot do so.

Capture policy:

1. SQLite commit succeeds: evidence is local-durable, independent of Redis.
2. SQLite fails/locks/corrupts: emit storage failure, try independent Redis
   capture. No automatic database deletion/recreation to hide corruption.
3. Redis succeeds: recoverable pending evidence resides there. Lost acknowledgement
   is ambiguous; retry retains the same event identity.
4. Both durable acknowledgements fail: retain detached event in RAM, emit explicit
   UNRESOLVED with the token-free raw snapshot, and continue the protective action.
   No claim of durability or reconciliation. Process loss can lose that RAM copy;
   older snapshots/logs/broker evidence may assist, but are not guaranteed recovery.
5. After the existing live risk step, replay at most ten RAM items and at most ten
   local rows per invocation. Forward marker is set only after Redis success.
   Retries after lost acknowledgement or a crash before marking are idempotent.

SQLite/Redis fallback is synchronous and can delay a close. Existing Redis socket
timeouts and transaction retries still apply; this is not a hard wall-clock exit
latency guarantee. Persistence failure is not used as a veto. There is no safe
promise of durable evidence when every durable sink is unavailable. Logger
failure on ordinary storage-failure reports is swallowed; storage tests verify
the existing protective-close function still sends its unchanged mocked order.

Only matching original order/confirmation account evidence and deal ID route to
the account journal. Session account is observational, not proof of entry origin.
Legacy/manual/conflicting origin remains local quarantine. Original add-on role
survives promotion. Neither quarantine nor forwarded means economically complete.
There is no automatic pruning/TTL, including stale pending or quarantined rows.

## Destructive-transition re-audit

The original pre-edit trace remains in BROKER_TRUTH_REPAIR.md. AST assignment and
reference searches were repeated on the integrated source. Stop/target/trailing
metadata changes do not remove original opening/quantity identity; state-save
snapshots capture their current values. This is not a complete broker amendment
event history. Startup fills missing defaults with setdefault after captured load,
and does not erase loaded primary/add-on evidence.

| Transition | Evidence surviving immediate death after mutation, when a durable sink acknowledges |
| --- | --- |
| Primary close POST/confirm/final clear | Opening/account snapshot and request intent; available raw response/confirmation; pre-clear primary and add-ons. Broker request result can remain ambiguous if death precedes receipt capture. |
| Partial close, remaining quantity/notional mutation | Pre-request quantity/notional and raw accepted receipt; pre-mutation basis and existing partial realizations. New residual can be reconstructed without calling it broker-confirmed. |
| Partial verification flat branches, including BOTH reads unavailable | Pre-clear original quantity/notional plus request/receipt. The underlying residual-tracking policy remains unchanged. |
| Add-on guard removal or ACCEPTED removal | Original add-on identity, entry/account provenance, available close intent/receipt and pre-replacement list snapshots. |
| LS add-on removal | Pending absence observation and original leg snapshot. Raw LS payload history is not added by this stage. |
| Primary close with surviving add-on / promotion | Both original identities captured before replacing primary; original add_on role copied unchanged; capture before clearing old add-on list. |
| Stale add-on list removal in run_live_step | Every remaining leg captured before clearing, including after an unsuccessful existing protective-close attempt. |
| Reconciliation flat clear / recovered primary replacement | Original state captured first; raw available response and account-margin proxy kept separate. No synthetic positions response promoted to broker fact. |
| Broker-recovered add-on / newly accepted entry | Available entry/recovery evidence captured before add-on append and independently of later Redis state save; unknown ownership stays unknown. |
| Redis active-state overwrite / load replacement | Current active snapshots captured before SET or in-memory load; loaded positions captured before subsequent reconciliation. |

If both sinks fail, each of these hooks yields explicit unresolved evidence
instead of pretending capture succeeded, and does not block the existing safety
action. No identified destructive clear/removal/quantity/promotion path is left
without a hook. Limits remain: accepted entry before first capture, receipt just
before process death, physical storage loss/corruption of already-captured data,
and simultaneous total storage unavailability. Capture cannot manufacture missing
legacy fields or broker economic facts. C2c and later verification are not begun.

## Verification

Baseline 128 tests are retained. Seven additional C2b tests cover:

- Real SQLite write-lock contention followed by Redis fallback and later recovery.
- Corrupt SQLite retained untouched while Redis captures evidence.
- Missing journal directory and unavailable logger while actual protective close continues.
- Redis fallback commits then loses acknowledgement; RAM/local retry produces one event.
- Abrupt child-process death inside a real SQLite INSERT transaction and immediately
  after COMMIT, without Python cleanup. Before commit: previous state snapshot
  survives and incomplete intent rolls back. After commit: intent survives even
  without caller acknowledgement. Both restart integrity checks pass.
- `synchronous=EXTRA`, rollback-journal mode, and year-2000 pending/forwarded/quarantined
  observations surviving replay without expiration.
- Actual partial-close function with both verification reads unavailable: active
  state still clears per existing policy, but original basis and raw receipt survive.

The first new crash test run found a test-only SQLite inspection handle left open
on Windows, causing temporary-directory cleanup failure after assertions passed.
The inspection now uses contextlib.closing; application code was not changed.
Existing tests also cover Redis outage/lost acknowledgement, double-sink failure,
restart, capture/forward duplicates, partial crash boundaries, promotion and
state load/save failures. Existing C1 tests cover duplicate reconciliation and
atomic fixture projections; they do NOT establish exactly-once production learning.

Final targeted lifecycle/storage run: **27 tests passed in 254.729s**.
Final complete offline discovery run: **135 tests passed in 315.361s**.
The expected `live_save_state failed: disconnect` warning is fault injection.
All Python files compile. Staged/unstaged whitespace checks passed; full runtime
diff, module/test contents, caller/key searches, destructive assignments and
file statistics were reviewed. Removing only new evidence additions from June's
AST exactly restores the integrated parent (5084f69), proving that pre-existing
constants, sizing, guards, order payloads and entry/exit policy are unchanged.
PendingCloseStore's executable AST is identical to its parent; only its stale
integration-status docstring changed. The final original-checkout preservation
verifier passed: staged/unstaged patches, status, saved checksums and original
working files are unchanged. The usage-limit interruption occurred after these
checks; continuation retrieved the successful full-suite result and rechecked
branch/HEAD/status and both diff checks before committing.
Pending-store executable code is unchanged; existing bounded-access regressions
remain part of the full suite. No performance-changing store change was made, so
the 100/1000/5000 retained-record benchmark need not be repeated for this stage.

## PRODUCTION SAFETY FOLLOW-UP — separate, unchanged

1. **Day-start / external capital flow:** prior read-only investigation observed
   32.84 in both state and today's baseline key rather than reported 182.86.
   The $150 Trade Based Concession - Jul26 is non-trading cash flow. Actual CB
   differs at equity $160 for those two baselines. Decide separately whether
   mid-day credits reset/increase loss baseline, remain excluded, or require
   explicit external-capital-flow accounting. No policy decision or Redis fix here.
2. **IG close/inventory semantics:** existing recovery requests GET /positions/otc;
   closes use ordinary opposite POST. Official API documents GET /positions and
   a dedicated DELETE close operation. Separately establish actual request and
   account/product response semantics and review historical incident evidence.
   No live test orders, request changes, or account switches here.
3. **Margin inference:** stale/account-wide margin and simultaneous positions can
   misclassify a deal. Existing guards remain unchanged; dedicated fix required.
4. **Partial residual tracking:** unavailable verification can clear remaining
   active tracking. New test proves pre-clear evidence survives; execution policy
   remains unchanged and needs separate remediation.
5. **Additional retained findings:** known Stage D add-on exposure discrepancy;
   arbitrary orphan primary/add-on role inference and version-field assumptions;
   stale-add-on clearing even after failed close; latent SOYBEANS cached-price
   fallback units; estimated/untrusted learning; synchronous capture latency and
   operational journal durability/backup requirements. None silently fixed.

No current broker state is inferred from the earlier snapshot. Production stayed
untouched; no SSH, broker calls, Redis calls, service control, deploy or push was
performed in this implementation task. Offline fixtures cannot certify production
safety. Keep these findings separate from the completed evidence-preservation work.

## Stop boundary and trust

C2b is verified complete within the explicit durable-sink failure policy and
documented crash/storage limits. It provides targeted pending access plus pre-mutation evidence capture with an
explicit protective-exit failure policy. It does not make live estimates or any
adaptive consumer broker-truth. SQLite and Redis retain evidence, not competing
economic conclusions. Production is not running this revision.

Stop after separately committing verified C2b-2. Next return for authorization:
urgent production safety follow-up remains separate; the next broker-truth stage
is C2c exact identity/history/cost integration only after explicit approval. Do
not begin C2d, Stage D or Stage E automatically. **NOT DEPLOYED.**
