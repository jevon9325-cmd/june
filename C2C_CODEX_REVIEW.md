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
