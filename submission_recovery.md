# Pre-broker abort recovery and evidence plumbing

The 2026-10-05 NATGAS continuation never reached IG POST: Redis rejected the
durable-first persistence barrier, the exception returned, and immutable
compounding telemetry recorded `pending_intent_persistence_failed`. The full
checkpoint correctly retained the proposed intent, but no recovery transition
existed for that known abort.

New pre-broker aborts get a synchronous SQLite receipt before their pending
barrier is released. Legacy recovery requires the exact proposal/abort telemetry
pair (account, primary, attempt and timestamps), matching Redis/checkpoint, a
fully valid flat state after removal, empty authoritative inventory/orders and
complete bounded IG history without a contradictory matching result. Startup
uses the normal checkpoint-first writer to preserve the recovery receipt and
resolve this case. It never manufactures a broker rejection, addon, settlement
or P&L. A changed/unknown/accepted/lost-acknowledgement result cannot use this
abort path. It remains gated and startup escalates through StateRecoveryRequired;
the recovery request is bounded to one 500-record history page and does not
blindly retry a broker order. A missing/divergent checkpoint cannot authorize
automatic replay. The sole permitted difference is a strictly newer finite
`pnl_fetched_at` checkpoint polling timestamp, with every other field exactly
equal. Recovery preserves that newer clock and records both values. After all
proof succeeds, startup runs enabled normal archive-before-release settlement
retention before the checkpoint/Redis recovery write; it never changes policy.

Decision gate capture now projects only literal `_live` keys read by its
recorded expression. Present/missing keys, exact values, gate outcomes and the
expression remain available. Unknown/dynamic expressions retain the original
full capture. This removes repeated unrelated account-history copies without
changing candidate evaluation, ranking, raw/final scores or admission. Existing
size limits remain in force; failure diagnostics stay on the recorder worker.

Broker-ledger retention walks forwarded evidence using a rotating row cursor.
Before releasing a policy-certified settled field it commits the entire Redis
payload and its SHA-256 digest in `ledger_archives` in `.broker-evidence.sqlite3`.
WATCH/version matching prevents deletion after a concurrent append. Uncertain
settlements, identity quarantine and pending partials remain retained. Restart
rescan and archive/release retries are idempotent; ownership/delivery claim
fields remain untouched. Archives preserve a recovery copy, not authority to
replay economic deliveries or reset account state.
