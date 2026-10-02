Exit authority is independent of economic settlement. New exits always carry
an explicit contract; historic records without one retain the existing legacy
policy and are never inferred to be manual or backfilled by this repair.

* JUNE_STRATEGY requires a retained deal-specific June order linked through the
  returned broker reference to an accepted exact-deal close confirmation, or
  to accepted PUBLIC_WEB_API close activity for that reference.
* BROKER_PROTECTION requires accepted SYSTEM close activity with explicit
  STOP_ORDER_FILLED on the exact deal and a stop level matching acknowledged
  June protection or the confirmed attached stop submitted on entry. SYSTEM,
  LS FULLY_CLOSED, an intended stop, and price proximity alone are insufficient.
* EXTERNAL_OPERATOR requires accepted MOBILE/WEB/DEALER close activity with
  exact epic, affected deal, verified retained opening account and valid event
  bounds. This establishes external intervention, not the identity of a person.
* UNKNOWN covers unavailable or insufficient evidence. Conflicting affirmative
  evidence produces sticky UNKNOWN and retains both evidence and conflict flags.

Confirmed economics are retained for every class. `accounting_eligible` means
the record participates in accounting when the separate settlement contract
certifies money; it never makes provisional P&L a number or substitutes zero.
Only JUNE_STRATEGY and BROKER_PROTECTION are eligible for autonomous learning.
Unknown or externally determined partials veto whole-campaign learning. All
partial and final evidence remains available for accounting and diagnostics.

The filters cover performance delivery/replay, confirmed HTF outcome populations,
streak/boost/pause, 15-minute adaptation, phase performance, reversal re-entry,
stop/thesis failures and instrument performance/defensive effects. Account
balance, daily loss, CB, sizing inputs, realized economic totals and rolling
reservation/harvest economics continue to consume real money independently.

Confirmed UNKNOWN outcomes have a bounded, account-pinned, provenance-only
activity retry (two records per poll, five-minute minimum interval). It neither
rewrites economics nor delivers performance. Settlement replay delivers eligible
performance using stable observation identities. Exact activity redelivery is
deduplicated before the unchanged settlement matcher sees it; conflicting rows
remain distinct.

Performance stats and permanent `june_perf_delivery:<hash>` markers commit
atomically with Redis EVAL and compare-and-set. No expiry is intentional: the
rolling statistical window is not a delivery ledger. Redis script failure leaves
delivery pending rather than claiming success. Existing Redis persistence remains
an operational prerequisite; this repair cannot guarantee survival of Redis data
loss. The deployment review should verify EVAL capability before activation.

Campaign events retain authority contracts per deal. Late settlement/provenance
updates follow the original deal link even if another campaign is already open.
Path rows include available per-leg provenance; completed outcomes live in durable
campaign events and accounting history. No strategy constants are changed.

The real GOLD manual-close fixture retains only non-secret identity/provenance:
opening DIAAAAR9LXBQHBA, closing DIAAAAR9LYCE4BA, MOBILE reference CHET9H9ACC589V,
2026-10-02T04:47:57. Activity alone is not broker-confirmed monetary P&L.
