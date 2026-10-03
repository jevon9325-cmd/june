# Prospective primary decision evidence

`decision_ledger.py` observes the existing primary funnel. It does not return
decisions, evaluate additional candidates, read Redis, change state, or call the
broker. The production observer uses a bounded background queue and a separate
`june_decision_ledger.sqlite3` beside `june.py`. `JUNE_DECISION_LEDGER_ENABLED=0`
disables only this observer. No dependency or producer contract is added.

Each runtime and cycle receives a UUID. Candidates share the cycle identity;
events have a cycle-local sequence and UTC Unix timestamp. Initial universe,
initial ranking, reranking and fallback visits are distinct evidence. The
actual ranking is absolute five-minute change times regime and correlation
weights, with the existing wide-spread reduction. Conviction components are
separate; they are not retroactively described as ranking contributions.

Ranking and submission hooks freeze and enqueue evidence **before** the next
trading statement. Physical SQLite commit occurs asynchronously; it can occur
after submission. Trading never waits for a flush. A process crash or queue /
write failure can leave a missing or incomplete cycle. Neither the absence of
a record nor a partial checkpoint is evidence of a complete decision.

## Reading a consistent offline copy

Use SQLite's backup API with a `mode=ro` source URI, rather than copying a live
database file without its WAL. Never import or execute `june.py` for research.
Payload columns contain zlib-compressed canonical UTF-8 JSON; decode them with
`decision_ledger.unpack`. Opening `Store.connect()` is a writer API and is not
appropriate for examining production evidence.

```sql
SELECT decision_cycle_id,at,account,commit_id,selected_candidate,terminal_state,
       complete,pinned FROM decision_cycles ORDER BY at;
SELECT instrument,direction,initial_rank,raw_score,final_score,payload
  FROM decision_candidates WHERE decision_cycle_id=? ORDER BY initial_rank;
SELECT sequence,kind,candidate_id,gate_name,result,payload
  FROM decision_events WHERE decision_cycle_id=? ORDER BY sequence;
SELECT candidate_id,gate_name,sequence,result,catalog_order
  FROM decision_gate_events WHERE decision_cycle_id=?;
```

`decision_gate_events` is a view: actual gate observations carry a sequence;
catalogue entries without an observation are `NOT_REACHED` with no invented
sequence. Catalogue order identifies a source site, not a counterfactual
evaluation order. Predicate, source line, actual inputs and available thresholds
are retained. A failed predicate branch does not imply a lower-ranked candidate
was visited. The `FALLBACK`, `ATTEMPT` and reranking events answer that question.
Economics for alternatives that never reached sizing remain null. Evaluated
economics live in the ordered event inputs, not fabricated candidate estimates.

Use `complete=1` **and** a terminal event for structurally complete recorded
cycles. A-grade research also requires verifying durable candidate coverage,
uninterrupted event sequence, consumed strategic versions and the downstream
evidence required by that research question. `complete` alone does not certify
broker settlement, global recorder coverage or every sister-bot artifact.

## Strategic versions

Content-addressed `strategic_artifacts` preserve the entire available producer
payload once, including unknown future fields. `decision_strategic_refs` records
the exact observations and consumption sequence; the final cycle context hash
references those versions. Cached observations are distinguished from reads
made within the active cycle. Current in-memory strategic state is also captured.
An initial checkpoint's context may differ from the terminal context when a
forecast is read later; use the ordered references for the decision timeline.

TTL is null when the existing reader does not obtain it. Source timestamps are
producer-provided `timestamp` or `generated_at`; observation time is separate.
Null folder/chart/version identifiers are intentional. Producer-supplied
`artifact_id`, `version`, `source_file`, `folder_ref`, `chart_ref`, `content_hash`
and artifact type (including `BARBIE_FORECAST_FOLDER`) can be preserved without
changing tables. An immutable recorder hash is not proof of an upstream folder
relationship. No new Redis read or sister-bot write is introduced.

## Downstream joins and retention

Join accepted `decision_outcomes` by verified account plus deal identity to
broker opening/settlement evidence. `campaign_id` uses the existing compounding
ledger's SHA-256 of the JSON `[account, primary_deal_id]` identity. Use the existing
compounding links when a campaign identity is already established. Deal reference
alone is a submission identity, not acceptance or settlement proof. Qualification,
protection, addons, rolling generations, MFE/MAE and exit authority remain in
the existing evidence stores. Match Miss Secretary outcomes only after separately
checking their semantics and account/deal identity.

Default bounds: 256 MiB disk budget, SQLite page ceiling at 90% of that budget
with WAL headroom, 100,000 cycles, 90 days for complete unsubmitted cycles,
64 queued documents, 256 candidates, 4,096 events, 1 MiB document and bounded
recursive copies (4 MiB accounting budget), 128 KiB strategic payload and 128 cached input keys. Complete unsubmitted cycles also expire oldest-first under disk pressure. Submitted
and incomplete evidence is preserved under pressure; recording then fails open
with bounded diagnostics. No growing Redis structure is created. Deployment
monitoring must treat `DECISION LEDGER GAP` as a research coverage gap.

`Store.archive_settled` is an explicit **offline** writer utility; it is never
called by June. It requires an accepted account/deal match and a caller-certified
final broker settlement identity/time/evidence hash with `unresolved=False`.
It writes an exclusive archive, flushes it, records its hash, and only then
unpins the cycle. Archive includes the catalogue, context and strategic versions.
The caller must verify actual final settlement; the utility does not contact the
broker or certify supplied assertions. Operator-managed external archives must
have their own bounded retention policy. There is no automatic live archive.
Rejected or unconfirmed submissions stay pinned; no inferred settlement unlocks
them. Exhausted capacity reduces recording coverage, never trading availability.
