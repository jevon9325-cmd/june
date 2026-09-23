# C2c evidence boundary

This contract supersedes earlier reports that treated complete pagination or a
caller-supplied cost attestation as sufficient for economic completion.

## Source assessment (2026-09-23)

The [IG transaction schema](https://labs.ig.com/reference/history-transactions.html)
does not establish unique economic-event identity or revision lineage. The
[activity schema](https://labs.ig.com/reference/history-activity.html) and
[confirmation schema](https://labs.ig.com/reference/confirms-deal-reference.html)
provide execution identifiers, but do not establish a unique join for every cost.
No authoritative lifecycle posting-finality signal was identified in those
contracts. This is a limit of the evidence reviewed, not a claim about every IG
service or private reporting interface.

The [REST guide](https://labs.ig.com/rest-trading-api-guide.html) describes request
success and pagination. [Statement guidance](https://www.ig.com/en/help-and-support/articles/692550-how-to-generate-a-statement-or-an-account-ledger-summary)
explains historical statement access; it does not promise that later adjustments
cannot occur. We therefore do not infer finality from either. The existing local
capture module stores receipts/snapshots, not an authenticated all-postings-final
assertion. No live account or broker API was accessed during this review.

The resumed review retrieved the activity, confirmation, REST guide and statement
pages again. The transaction-schema URL could not be retrieved on this retry;
its earlier inspection is recorded in C2C_CODEX_REVIEW.md at commit 9a3d475.
No unsupported new guarantee is inferred from that prior inspection.

## Four distinct facts

| Field | Meaning |
|---|---|
| history_request_complete | Account-pinned history request passed retrieval checks. |
| history_window_covered | All returned pages for the stated requested interval were collected. It is not a guarantee against later/backdated postings. |
| broker_posting_finalized | Authoritative finality for the lifecycle, including delayed costs/corrections. Always false for the currently supported sources. |
| economic_evidence_complete | Proven position/event identity, multiplicity, costs and posting finality together. Always false in current C2c. |

The legacy history_complete/history_fetch_complete aliases describe fetching only.
cost_statement_consistent checks asserted scope and totals; it is not evidence
authentication. cost_state stays UNRESOLVED even when such an assertion agrees.

Retrieval provenance assumes the caller supplied fetch_transaction_history's
account-pinned result. Direct store calls accept the batch assertion; they do
not authenticate a network response or independently re-fetch its pages. Neither
path can certify economics. Duplicate rows within pagination are refused as
unstable/ambiguous retrieval; duplicate deliveries of an accepted batch are
idempotent. Window coverage is always as observed, not a broker snapshot guarantee.

## Outcomes and numbers

- COMPLETE: reserved for fully certified economic truth. No current C2c input
  can produce it. Dictionary flags, fabricated IDs and waiting periods cannot
  enable this capability. net_realized_pnl and won remain null.
- PROVISIONAL: available observations support candidate arithmetic. Existing
  gross_realized_pnl, commissions, other_costs and net_identified_pnl fields are
  provisional observations, not certified economic totals or learning inputs.
  Exact repeated observations count once; all exposed discriminators are kept.
  No conclusion is drawn that identical broker rows imply one economic event.
- UNRESOLVED: missing, ambiguous or contradictory evidence prevents a trusted
  conclusion. identity_state/cost_state express this independently of available
  provisional arithmetic. Known collisions use identity_state=AMBIGUOUS and
  economic_state=UNRESOLVED; prior records are retained as quarantined audit data.

The lowercase status field retains workflow detail: pending_realizations,
pending_costs, provisional, or unresolved. It never says complete for new data.
An internally consistent cost assertion may move workflow to provisional, but
does not resolve identity_state or cost_state. Partial quantities and observed
close times describe candidate history, not a certified final lifecycle.

Conflicting realization amounts under the same fingerprint raise EvidenceError;
they are not silently overwritten. Changed cost representations may be separate
observations without known revision lineage. Candidate sums may therefore count
both representations and must never be interpreted as distinct-event truth.
Replacing or omitting previously journaled evidence is refused. Errors retain
the prior provisional/unresolved record and require caller evidence retention
and review; rejection does not certify that prior arithmetic. No row-index ID,
first-claim ownership proof, or fabricated correction lineage is introduced.

## Journal and projection safety

Records/cost claims are atomic under WATCH/HSET. Retries require cumulative
evidence; missing or changed prior observations are refused rather than erased.
Unknown COMM/SWAP/account rows survive restart. Unsupported old fingerprint
versions require explicit review; there is no automatic rekeying or redelivery.

A newly discovered opening collision quarantines affected records and every
existing opaque account projection in one transaction. A projection has no safe
generic inverse, so its value becomes null while prior_projection is retained.
Duplicates/restarts cannot apply a correction twice. Shared entry references
also quarantine affected known commission owners. Reads of legacy complete
records/projections return untrusted audit views even before a collision.

project_once and completed_history_view refuse all current and legacy outcomes;
reducers are never executed. Raw journal fields and archived prior records are
internal evidence, not supported certified-output APIs. External consumers that
already used a historical value require Stage E recovery/rebuild. C2c neither
reverses those effects nor claims that it has repaired external learning state.

## What would be required for future finalization

An independently reviewed evidence adapter would need authenticated, retained
broker records establishing account/position ownership, economic event identities
and corrections, complete realization quantity, each cost component including
explicit zero components, and authoritative posting finality over the lifecycle.
Its semantics must be supported by actual broker evidence, not merely a schema
containing final=True. It must address late contradictory evidence and legacy
record migration before enabling output. No such adapter is implemented here.

Alternatively an explicitly approved accounting policy could close a reporting
period provisionally while handling future adjustments. That would be policy
finality, not broker-guaranteed economic finality; it must retain that distinction
and cannot silently enable broker-net certification. No policy or waiting period
is selected by C2c. Adaptive consumers remain disconnected.
