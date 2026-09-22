# PS1 production-safety investigation

Work is confined to `june-broker-truth-integration`, branch
`integration/broker-truth-main-20260922`. Production remains live and undisturbed.
No deployment, push, Redis write, order, account change or restart is authorized.
C2c is excluded.

## Baseline and durable checkpoint

Verified origin `https://github.com/jevon9325-cmd/june.git`, clean tree and HEAD
`d29c520b57875105e8ef361a18b5fae1db46c183`. Baseline: 135 offline tests passed
in 311.047 seconds. Tag `checkpoint/ps1-pre-repair-20260922` points at that HEAD.
Integration parent is `5084f695252c3b0a35862eeaf20471a9d31918fc`.

## PS1-A/B: HTTP contracts

Primary sources inspected on 2026-09-22:

- [IG OTC positions](https://labs.ig.com/reference/positions-otc.html):
  DELETE v1 closes; POST creates. A close specifies dealId OR epic/expiry.
- [IG FAQ](https://labs.ig.com/faq.html): POST with `_method: DELETE` is the
  documented alternative for clients sending a DELETE body.
- [IG inventory](https://labs.ig.com/reference/positions.html): GET v2 returns
  account positions, including size/level/createdDateUTC. It is not DMA-only.
- [IG confirmations](https://labs.ig.com/reference/confirms-deal-reference.html).

Actual wrapper `_ig_live_post` previously called `requests.post` without override,
including its 401 retry. All three close helpers used this opening contract.
Opposite direction and forceOpen=false do not establish deal-specific closure.
No claim is made that every historical close created an orphan: historical
receipts are insufficient to establish every broker outcome.

| Caller | Size | Direction | Before | After |
|---|---|---|---|---|
| `_live_close_position` | tracked primary size | opposite tracked direction | plain POST v1 | POST v1, `_method: DELETE` |
| `_live_partial_tp_exit` | rounded half size | opposite tracked direction | plain POST v1 | POST v1, `_method: DELETE` |
| `_live_close_addon_leg` | tracked addon size | opposite tracked direction | plain POST v1 | POST v1, `_method: DELETE` |

All use `/positions/otc`. Before fields: epic, expiry, direction, size,
orderType=MARKET, timeInForce=FILL_OR_KILL, forceOpen=false,
guaranteedStop=false, currencyCode=USD, dealId. After fields: dealId, direction,
size, orderType=MARKET, timeInForce=FILL_OR_KILL. The wrapper rejects missing
deal identity, invalid/nonpositive/nonfinite size and nonconforming close fields.
It retains the override on authentication retry. The broker-generated
dealReference still identifies the subsequent confirmation request; it is not
substituted for the original position's dealId.

Protective stop, max hold, reversal, trailing exits and primary pyramid exits
route through the primary helper. Aggregate pyramid exits and addon stop/target
exits route through the addon helper (and primary helper when applicable).
Opening primary and addon POSTs and deal-specific stop PUTs are unchanged.

Before repair, acceptance meant only `dealStatus=ACCEPTED`. Full close then used
inventory/stream verification; partial close could clear tracking on unavailable
inventory plus inferred zero margin; addons cleared on acceptance and inferred
orphans from margin changes. Acceptance alone does not prove disappearance.
The later PS1 substages assess those separate lifecycle defects.

All inventory GET calls changed to `/positions` v2. Full-close verification now
checks the target deal on every attempt rather than treating any other open
position as failure; existing addon promotion remains in that verifier.

Read-only historical production journal inspection (2026-09-21) found repeated
`/positions/otc` HTTP404 `error.position.notfound`. At 17:00 UTC a NATGAS primary
FULLY_CLOSED message was followed by an addon promotion with REST presence; the
addon closed at 17:01. GOLD showed the same pattern at 20:58, with an addon
surviving until 22:01 while OTC inventory 404s continued. These are historical
observations, not current unresolved positions. No live test orders were used.

Regression `test_ps1_api`: before implementation, two failures and one error
demonstrated extra opening fields, wrong inventory version and missing close
override support; opening control passed. All four pass after implementation.
Full-suite verification and commit identifiers are recorded below as completed.

## Remaining PS1 investigation

C: preserve partial residual state across unavailable/ambiguous broker evidence.
D: remove account-margin inference as deal-specific proof, preserve protective exits.
E: describe capital flows and obtain a policy decision if necessary; no C2c work.
F: distinguish management roles from unknown historical origin.
G: verify SOYBEANS eligibility, actual sizing, P&L and cached fallback units.

### A/B verification

Full offline suite: 139 passed (348.563s), plus the subsequently added invalid-payload test passed in the five-test targeted run. py_compile and diff --check passed. Full diff reviewed: only POST wrapper, three close helpers and inventory recovery changed; no strategy constants, sizing formulas, entry payloads or persisted schema changes. Checkpoint retained.
