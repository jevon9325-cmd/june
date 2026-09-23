# C2c senior review ? in progress

Baseline: branch integration/broker-truth-main-20260922, Claude HEAD
 d4948ed6750462e8db13dffd46ca7988fd1210f9. Independently ran 271 tests: PASS.
Reviewed c650770, 8ff2fa4, d4948ed against pre-C2c 9e7a2ab.
No June runtime changes in those commits or the correction below.

## Correction 1: opening identity and lifecycle collection

Six adversarial tests reproduced nine failures (including subtests) before repair:
known different opening UTC accepted despite equal price/size; missing UTC
reported EXACT; a partial from another opening filled a missing residual;
a duplicate partial doubled quantity; caller opening fields and entry references
were not checked against registered ownership.

Require exact opening UTC in addition to account/instrument/price/direction.
Missing UTC is ambiguous; different known UTC excludes a row. Deduplicate exact
normalized rows before quantity accounting. The pipeline validates caller identity
against immutable registered opening evidence and validates any supplied entry
reference against the accepted receipt. Existing successful fixtures now supply
their actual opening UTC and registered entry reference.

Validation: targeted 105 tests PASS; full offline 277 tests PASS; py_compile PASS;
git diff --check PASS; complete correction diff reviewed. New tests use fake Redis.
Runtime callers remain disconnected; no live payload or strategy changes.

## Outstanding review / blockers (not certified)

- Commission attribution currently trusts reference membership despite shared
  references. Cost IDs include claimant deal_id, omit posting time/instrument,
  and include amount. There are no atomic cost claims. Same-ref equal costs can
  collapse; one source cost can be attributed twice across positions.
- cost_evidence currently needs only source/reference/covered_through. Coverage
  through close time does not establish broker posting finality. Reference-less
  COMM and unrecognized financing descriptions can bypass completeness gating.
- SWAP descriptions need not equal the canonical instrument name; unresolved
  financing must not be silently excluded from net completion.
- Normalizer accepts -$9 but ledger raw-money parsing does not. Timestamp parser
  accepts date-only as midnight; malformed comma placement is stripped.
- Realization IDs do NOT contain June deal_id. Opening fields must exactly match
  source before ID construction, so changing claimant alone does not evade the
  existing atomic claim. Instrument is omitted from the ID, however, allowing
  false collisions between distinct instruments. More adversarial tests needed.
- Registered opening collisions are guarded, but registry completeness and late
  collisions after projection remain caller assumptions, not proven broker facts.
- Fetch completeness is page/window coverage only. Store requires cumulative
  prior rows; it rejects missing previous economics rather than merging windows.
- DEPO/WITH are excluded from costs; other unknown types are account scope.
- Existing Micron fixture proves one +5.27 gross / -18 cost / -12.73 net trade via
  pure ledger integration. Five-trade +3.68/-90/-86.32 full-pipeline proof remains
  to be added or located; do not claim it is already verified.

Next: independently reproduce and repair cost attribution/finality, source IDs,
parsing; test persistence/claim concurrency and the five-trade pipeline fixture;
finish contract review including broker_capture; give final trust verdict.
No production source/Redis/order/restart/deployment/push actions performed.

## Correction 2: cash and timestamp parsing

Use one strict cash parser in normalization and the ledger. Accept USD sign
placement before/after $, outer whitespace, correctly grouped commas, plain
signed decimal and signed zero. Reject malformed grouping, repeated signs,
parentheses (no supported source evidence), missing values and nonfinite money.
Require timestamps to include seconds; date-only/minute-only evidence remains
unknown rather than inventing precision. No tolerance or rounded-time matching.
Unsupported/missing currency remains refused; no FX conversion introduced.

Three new adversarial tests cover multiple sign, malformed-money and timestamp
cases. Targeted 77 tests PASS. Full suite: 280 tests PASS.
Cash helpers are offline only; _utc callers reviewed in history/pending and
normalization. June does not call these helpers. june.py remains byte-unchanged
from the pre-C2c baseline. py_compile and diff checks PASS; full diff reviewed.

## Durable continuation checkpoint

Correction commits: eb2635f (opening identity), 1e48c7d (parsing).
Current complete offline suite: 280 PASS, versus Claude baseline 271.
No failed or expected-failure tests retained. Working tree cleaned by commits.

Offline diagnostic probes after both corrections (fake Redis, no network):

| Probe | Observed result |
|---|---|
| Identical normalized COMM, claimant a vs b | Different cost IDs |
| Same reference/amount, posting 07:00 vs 08:00 | Same cost ID |
| Same COMM shared reference, two candidate positions | Assigned once to each |
| DEAL only plus current three-field cost attestation | complete, net -0.16 |
| DEAL plus reference-less -9 COMM and attestation | complete, net -0.16 |
| DEAL plus descriptive -0.50 SWAP and attestation | complete, net -0.16 |

The SWAP description was exactly: Daily Financing Adjustment - FX Interest for
1 day Spot Gold ($1). None of these results establishes legitimate zero costs.
The last two omit visible unresolved economic rows. The first three expose
identity/attribution defects. These are blockers, not approved behavior.

Cost correction has NOT been started. Next independent change must add regression
tests first, establish claimant-independent source cost identity plus atomic
claims, reject reference-only ambiguous allocation, and strengthen the finality
contract beyond covered_through. Do not invent source IDs or a posting-delay
threshold where broker evidence cannot distinguish rows. Preserve unresolved
rows rather than finalize. Inspect schema compatibility with saved pending and
completed records before changing IDs. Keep this separate from the two commits.

Continue remaining review of realization source identity (instrument omission),
late opening collisions, overlapping windows, economic finality, claim concurrency,
and five-equity-trade pipeline fixture. Broker capture source was inspected:
local SQLite immutable evidence and replay are not proof of broker economics;
unknown account/opening provenance remains quarantined. Full suite includes its
failure-injection tests. No Stage D/E or runtime adapters were connected.

Checkpoint verdict: REVIEW INCOMPLETE / NOT APPROVED. Do not interpret 280 passing
tests as certification of cost attribution or broker-net completed outcomes.
Stopped at independent verified commits per the requested token-discipline rule.

## Correction 3: source cost fingerprints and atomic claims

Resumed at the requested clean 8ff64e5; independently verified 280 tests PASS.
Eight added adversarial tests cover claimant independence, distinct posting times
and instruments, same-cost duplicate delivery/overlapping windows after restart,
two-position atomic claims, lost EXEC acknowledgement, WATCH contention, and
legacy record quarantine. Four tests failed before the implementation change.

Cost fingerprints now exclude June deal_id and include the normalized source
economic fields, with canonical numeric representations. Distinct postings with
shared reference/amount remain separate. Cost claims commit in the same WATCH /
HSET transaction as realization claims and the trade record. Legacy records with
costs but without the new claim-version marker are refused for reconciliation
and projection; no silent migration or replay is performed.

Validation: full offline suite 288 PASS; targeted review 17 PASS; py_compile and
git diff --check PASS; full diff reviewed. june.py unchanged from pre-C2c 9e7a2ab.

Limits: this is a source-row fingerprint, not a broker-issued unique event ID.
Indistinguishable distinct rows and corrected rows still need stronger evidence.
Atomic exclusion also does not prove that the first claimant is the right owner.
Reference-only ambiguous attribution and economic finality remain blockers.
No C2c certification, deployment, runtime wiring or production action.

## Correction 4: known shared-reference ambiguity

Shared closing references spanning different opening tuples in the input history
are now left unattributed. Registered entry references have an atomic owner index;
multiple owners prevent cost reconciliation and delivery. Accepted entry receipts
cannot be replaced after registration. Two adversarial tests exercise shared Gold
closing references and concurrent same-instrument positions sharing an entry ref.
The bounded-access test includes the additional reference-index read (no scan).
Full suite 290 PASS; compile/diff checks PASS; correction diff reviewed.

This detects known ambiguity; it does not establish completeness of the opening
registry, resolve absent history rows, or undo already delivered historical data.
Existing pre-index registrations need evidence review/re-registration before use.
The first-claim rule alone remains insufficient as economic ownership proof.

## Correction 5: positive economic finality contract

Legacy source/reference/covered_through attestation no longer finalizes a trade.
broker_finality.py defines version-2 input evidence: exact account/position scope,
full accrual coverage, separately referenced broker posting finality, fetched
coverage through that posting horizon, and explicitly final commission/financing/
other signed totals. Totals must reconcile to identified costs. No hard-coded
posting delay or wall-clock maturity is invented. Nonzero financing remains
pending because no verified position attribution exists; offsetting other credits
cannot hide it. Explicit final zero components can establish zero costs only
conditional on authentic, externally verified broker statement evidence.

Both pipeline and direct store calls block visible unresolved account costs,
reference-less commissions and descriptive/unknown financing. Capital flows stay
excluded. Provenance now separates history_fetch_complete from
economic_evidence_complete. Legacy completed records lacking that evidence cannot
be projected. This is a contract validator, NOT a broker statement authenticator;
there is no production adapter supplying such finality evidence. Synthetic fixture
attestations demonstrate validation, not that an IG posting-finality source exists.

Fourteen additional tests cover legacy coverage, absent expected commissions,
individual component finality, immature posting horizons, positive zero costs,
delayed entry/exit commissions and financing after restart, unknown account costs,
direct-store bypass, position/window mismatch, non-trading flows, offsetting costs,
out-of-window postings, and legacy projection quarantine. Full offline suite:
304 PASS. Compile and diff checks PASS; complete change reviewed.

## Correction 6: realization instrument discriminator

Two new adversarial tests reproduced a collision between Gold and Silver with
otherwise identical reference/opening/close fields; the second legitimate trade
was refused as already claimed. Realization identity now includes the source
instrument and a versioned namespace, still excluding the June claimant ID.
A third test verifies saved pre-versioned realization records are quarantined
without rekeying or projection. Existing claims/history are preserved for review.
Full suite 307 PASS; compile and diff checks PASS; complete diff reviewed.

## Correction 7: retain unresolved costs across restart

Final review reproduced another premature completion path: after observing an
unresolved COMM/SWAP/unknown row, a later batch omitting that row could certify
zero costs because only attributed costs survived in the journal. One adversarial
test with three subcases reproduced this before repair. The journal now retains
all observed non-capital cost rows atomically and refuses subsequent cumulative
history that omits any of them. No silent window merging or evidence deletion.
The prior independent-cost tests now use independent stores per scenario.
Full suite 308 PASS; compile/diff checks PASS; full correction reviewed.

## Correction 8: statement currency

Finality evidence must also explicitly match the position currency. Missing or
foreign currency cannot be silently treated as USD. Added both adversarial
subcases to the scope test. Full suite remains 308 PASS; compile/diff checks PASS.

## Review verdict at this continuation checkpoint: NOT APPROVED

Start: clean 8ff64e53b3aa97506039ff70606b626a95f7e82a, 280 tests PASS.
Branch remains integration/broker-truth-main-20260922. Previous Claude and Codex
commits preserved. This session added 28 test methods plus adversarial subcases.

Verified correction commits:
- 9afb8c3: source cost fingerprints and atomic cost claims (288 tests).
- 033ee97: known shared-reference ambiguity (290 tests).
- 11df42b: positive cost finality contract (304 tests).
- 05e34e4: realization instrument discriminator/legacy quarantine (307 tests).
- 3256c2a: unresolved-cost persistence across restart (308 tests).
- Statement currency guard is included with this final review update.

### Remaining blockers and evidence needed

1. Economic source identity is still not established for indistinguishable rows
   or revised representations of the same broker event. A normalized content
   fingerprint is not a broker-issued event identity. Known cross-position
   duplication and different-posting collisions are repaired, but that does not
   prove the absolute acceptance invariants for all distinct events/corrections.
   Need source evidence establishing event uniqueness and correction lineage;
   do not add hypothetical broker ID fields or row-index identities.
2. Opening registry completeness is still a prerequisite, not a proven fact.
   A fake-Redis diagnostic after all repairs completed/projected opening-1, then
   registered an identical opening under late-collision. The old record still
   reported complete and the journal projection still held count=1/net=-0.16.
   Subsequent projection calls are guarded, but existing delivered state is not
   invalidated. Need a defensible registry-completeness/finality boundary before
   certification; do not connect adaptive consumers to bypass this blocker.
3. Version-2 finality validation is conditional on externally verified broker
   statement evidence. No authenticated posting-finality document/source/adapter
   is established by this repository or this review. Synthetic finality fixtures
   must not be represented as real broker guarantees, a known latency bound, or
   proof of genuinely zero-cost historical trades. Financing remains unresolved.
4. Legacy saved records need explicit evidence review and migration design before
   activation. This change intentionally quarantines them; no production records
   were read, modified, rekeyed or redelivered. Pre-index opening registrations
   also need review. No automatic migration has been authorized or implemented.
5. The real five-equity-trade +3.68/-90/-86.32 full-pipeline evidence proof remains
   unestablished. The existing Micron fixture is not that proof. Do not manufacture
   broker statement rows or totals to turn a synthetic test into historical proof.

### Acceptance audit (FAIL includes not established, not just a failing test)

| # | Result | Reason |
|---|---|---|
| 1 | FAIL | Exact tuple checks work, but incomplete registry/late collisions remain unproven. |
| 2 | FAIL | Identical realization delivery is idempotent; broker correction lineage is not established. |
| 3 | FAIL | Instrument collision fixed; indistinguishable separate source events remain unproven. |
| 4 | FAIL | Atomic identical-cost claims work; content fingerprints cannot prove event identity across revisions. |
| 5 | FAIL | Distinct times/instruments preserved; economic multiplicity of indistinguishable rows is unknown. |
| 6 | PASS | Existing duplicate partial and residual fixtures reconcile once with exact Decimal quantities. |
| 7 | PASS | DEPO/WITH excluded; unknown account flows block net completion. |
| 8 | PASS | Unattributable financing remains pending and its observed source rows survive restart. |
| 9 | FAIL | Stronger gate tested, but authentic broker posting-finality evidence remains unestablished. |
| 10 | PASS | Identical cumulative evidence retries/overlaps, WATCH races and lost acknowledgements are idempotent; regressions refused. |
| 11 | FAIL | Missing fields/cost finality stay pending; economic multiplicity and registry completeness remain assumptions. |
| 12 | PASS | No runtime calls to reconcile_position/project_once; adaptive consumers remain disconnected. |

### Final verification and deployment assessment

- Full offline suite: 308 PASS (the live_save_state disconnect warning is the
  existing injected failure test, not a production connection).
- Compile: all 26 broker modules and test files PASS; final touched modules also
  compiled after the statement-currency guard.
- git diff --check against starting HEAD: PASS; independent correction diffs
  reviewed in full. Only offline broker evidence modules, tests and this report
  changed. No unrelated files changed.
- june.py byte-unchanged against pre-C2c 9e7a2ab. Live order payloads, strategy,
  sizing, tiers and leverage unchanged. No Stage D/E work.
- Callers/references reviewed: finality is called by the offline store; cost
  attribution by the offline pipeline. No adaptive consumer wiring introduced.
- Runtime/trading behavior changed: NO. Production modified/restarted: NO.
  Live Redis modified: NO (fake Redis only). Broker actions/deploy/push: NO.
- The complete C2b + PS1 + C2c candidate is NOT certified ready for controlled
  production deployment. C2c remains NOT APPROVED; no deployment is authorized.

Continuation should start from this committed clean checkpoint and the blockers
above. Do not reconstruct old history or interpret 308 passing tests as C2c
certification. Resolve the source-evidence and registry-finality contracts before
claiming all twelve acceptance invariants. Stop at C2c.

## Continuation from db4abef: blocker 1, source identity

Verified the requested branch, clean db4abef and 308 passing tests before editing.
Official schema inspected 2026-09-23:
https://labs.ig.com/reference/history-transactions.html
It documents row attributes and pagination, without declaring reference unique
or providing transaction revision lineage. This is insufficient evidence for
unconditional event uniqueness. Additional captured fields (including period)
now participate in observation fingerprints instead of disappearing.

All history-derived results now explicitly report identity_state=UNRESOLVED and
economic_state=PROVISIONAL (UNRESOLVED when no realizations exist). The former
complete branch now returns provisional with net_realized_pnl/won unset. The
existing gross/net_identified fields are observation arithmetic, not certified
economic totals. Caller booleans or invented ID fields cannot promote them.
Repeated identical observations count once; distinct exposed rows survive. Their
economic multiplicity/revision relationship remains unknown rather than guessed.
Source fingerprint versions advance to 3; previous records remain quarantined.

Five new tests cover identical delivery, distinct periods/times, false caller
identity flags and restart. Existing arithmetic tests still assert original
numeric results; certification/projection assertions now require refusal.
Transport/WATCH tests continue to exercise atomic provisional journal writes.
Full suite: 313 PASS. Compile/diff checks PASS; all changes reviewed.
This closes the false-certification path for new history-derived results.
Historical projections and posting-finality semantics are the next blockers.

## Continuation blocker 2: late contradiction and historical projections

Reproduced the old completed/projection state using an explicitly seeded legacy
journal fixture; no test-only certification bypass was added. Opening collisions
now atomically mark every affected record UNRESOLVED/AMBIGUOUS and retain its
previous version. Shared entry-reference collisions invalidate known commission
owners as well. Identity quarantine cannot be cleared by reconciliation retries.

Opaque journal aggregates cannot be safely inverted. On a collision all existing
account consumer fields are atomically wrapped as quarantined, with value=None
and the untouched prior_projection retained for audit. HSCAN occurs only on this
exceptional path under WATCH. A concurrent update forces a full retry. Duplicate
registration does not wrap the archive twice or change totals. No reducer runs.

Store read/scan/capture/registration views also quarantine legacy complete records;
get_projection never exposes a legacy aggregate as certified. Raw archival hash
fields are not an economic-truth API. Already consumed external learning effects
cannot be reversed within C2c: they require Stage E recovery/rebuild, which was
NOT implemented. That boundary is explicit on every quarantined projection.

Seven new tests cover the original failure, duplicate registration/restart,
legacy read boundaries, lost acknowledgement, pre-EXEC failure, WATCH concurrency,
and late commission-reference collision. Full suite 320 PASS; compile/diff checks
PASS; all correction changes reviewed. No live Redis or runtime adapter used.

## Resumed independent review: posting finality and evidence contract

Resumed with user authorization at f6e52b7, preserving both prior corrections and
all surviving changes. Reviewed actual diffs of 9a3d475 and f6e52b7, not just their
commit messages. Initial index was empty. The eight modified files were:
broker_cost.py (attribution wording), broker_finality.py (consistency versus
certification), broker_ledger.py (unresolved costs and output refusal),
broker_pending.py (fetch provenance, assertion retention and delivery refusal),
broker_reconcile.py (pass assertions for audit), test_broker_reconcile.py
(provisional semantics), test_c2c_boundaries.py (seven finality adversaries), and
test_c2c_review.py (wider fetch-window provenance). The untracked
C2C_EVIDENCE_CONTRACT.md documented precisely that boundary. No unrelated change
was found. Before editing: fresh full suite 327 PASS; 28 broker/test modules
compiled; staged and unstaged diff checks passed.

The remaining change is deliberately fail-closed. Version-2 statements are
UNVERIFIED audit assertions: matching their scope and totals never establishes
posting finality. economic_evidence_complete always returns false (or rejects
malformed input). Current records retain gross and identified-cost arithmetic,
cost_state=UNRESOLVED, broker_posting_finalized=false, and null certified net/win.
project_once and completed_history_view refuse every current or legacy outcome;
even forged VERIFIED/complete flags do not execute a reducer. There is no delay,
feature flag or fabricated broker identifier enabling finalization.

The evidence contract separates request completion, observed window coverage,
broker posting finality, and economic completeness. It documents the trusted
batch-input boundary, insufficient event/correction lineage, legacy quarantine,
required future authenticated evidence, and the alternative of explicitly named
policy-based provisional reporting. No such adapter or policy is implemented.

Official activity, confirmation, REST guide and statement instructions were
retrieved again. They do not establish an all-postings-final lifecycle guarantee.
The transaction-schema URL was unavailable on this retry; its earlier inspection
is recorded above. This conclusion is limited to reviewed sources, not all IG
services. Local capture retains evidence; it cannot authenticate posting finality.
Sources and retrieval limitations are linked in C2C_EVIDENCE_CONTRACT.md.

Three additional tests independently check backdated costs within an unchanged
covered window, distinct observations through the full pipeline plus duplicate
retry/restart, and cost revisions without lineage. Revised evidence cannot erase
previous observations. Retaining both representations remains explicitly
provisional, never proof of two economic events. Targeted boundary suite: 22 PASS.

## Final 12-point acceptance audit

This supersedes the earlier NOT APPROVED audit for the reviewed C2c candidate.
PASS means the guarantee is established under the user's explicit allowance for
AMBIGUOUS / UNRESOLVED / PROVISIONAL outcomes. It does not mean missing broker
evidence was obtained. Original question subjects and ordering are retained.

| # | Acceptance question | Result and evidence |
|---|---|---|
| 1 | Can opening ownership or a late collision falsely certify an outcome? | PASS: exact tuple/receipt checks; all history ownership remains unresolved; late collisions atomically quarantine records and existing aggregates. Matching, late-collision and WATCH tests pass. |
| 2 | Does repeated realization delivery count once without assuming correction lineage? | PASS: identical observations are idempotent; conflicting representations are refused or explicitly provisional; no certified output. SourceIdentityTests and ledger conflict/restart tests pass. |
| 3 | Are distinguishable realizations preserved and indistinguishable events handled honestly? | PASS: instrument/time/period discriminators survive; multiplicity remains UNRESOLVED; excess/conflicting quantities are refused. Full-pipeline distinct-observation test passes. |
| 4 | Can a source cost be claimed or delivered twice? | PASS: claimant-independent fingerprints and atomic claims reject competing owners; identical retry counts once; changed representations never establish event lineage or certified net. Claim-race/lost-ack/revision tests pass. |
| 5 | Do distinguishable costs survive without inventing uniqueness for identical rows? | PASS: distinct posting times/instruments/periods survive, duplicate observations count once; cost and identity states remain unresolved. Cost identity and pipeline tests pass. |
| 6 | Are partial and residual realizations reconciled without double-counting? | PASS: Decimal quantities, duplicate partial checks and cumulative history; late residual remains provisional. No residual-only certified outcome. |
| 7 | Are capital flows excluded while unknown account costs remain unresolved? | PASS: DEPO/WITH excluded; unknown/reference-less costs cannot establish completion. Classification and finality tests pass. |
| 8 | Is unattributable financing retained through delay/restart? | PASS: no guessed position allocation; observed financing survives and missing prior rows are refused. Delayed-financing/restart tests pass. |
| 9 | Is broker-net completion supported only by actual posting-finality evidence? | PASS by explicit refusal: no supported source establishes it, so all assertions and waits leave costs unresolved and certified net null. Consistency, long-wait, delayed and backdated tests pass. |
| 10 | Are retries, overlapping windows, failures and restarts safe? | PASS: atomic journal claims/quarantine, lost-ack retry, WATCH contention, cumulative evidence checks and no reducer delivery. Existing opaque projections are quarantined, not inverted. |
| 11 | Can incomplete/ambiguous evidence become falsely complete? | PASS: current inputs cannot emit COMPLETE; legacy reads quarantine; forged completion flags fail both output boundaries. Gross and identified net remain explicitly provisional. |
| 12 | Are runtime/adaptive consumers disconnected? | PASS: repository call search finds only definitions outside tests; june.py is unchanged from pre-C2c 9e7a2ab. No Stage D/E integration. |

C2c APPROVED as an offline evidence/uncertainty boundary. It is NOT a certified
net-P&L producer and NOT deployment-ready. COMPLETE is reserved and unreachable
for current sources; PROVISIONAL is observation arithmetic; UNRESOLVED means no
trusted conclusion (known collisions are AMBIGUOUS and quarantined). Identity
and cost uncertainty remain explicit even when provisional arithmetic is present.

The five-equity-trade historical +3.68/-90/-86.32 proof remains unestablished;
no synthetic fixture is represented as that historical proof. Legacy migration,
external already-consumed projections and consumer recovery remain a Stage E
boundary, not completed work. Their unresolved status does not enable output.
No strategy/sizing/order payload changes, broker actions, production Redis access,
deployment, push, production restart, or Stage D/E work occurred. Stop at C2c.

Final verification: python -m unittest discover -q: 330 tests PASS (18.262s).
The live_save_state disconnect warning is an injected offline failure test.
All 28 broker/test Python modules compile. git diff --check passes. No skipped or
expected-failure tests. The posting-finality correction includes the surviving
work, ten finality/contract tests beyond f6e52b7, and this completed audit.
