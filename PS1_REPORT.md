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

Commit: `46df963`.

## PS1-C: partial residual tracking

Six deterministic regression cases initially produced five failures. A missing
inventory response followed by missing accounts became deposit=0 and cleared the
primary. A plain ACCEPTED receipt plus unchanged broker size still halved local
quantity. Multiple open deals skipped the residual update. Lost response,
missing confirmation and rejection triggered an unsolicited full-close fallback.

Repair records `partial_exit_pending` before transmitting and saves it with
existing state. Unknown outcomes retain the original quantity and basis. Another
partial TP is suppressed while pending; this flag does not suppress protective
full closes. Rejection removes the pending marker and retains quantity. A valid
inventory response lacking this deal proves absence; a matching residual size
proves the expected reduction. Missing/malformed inventory, missing size,
unchanged size or an unexpected reduction retain the original basis and marker.
Other open deals do not invalidate a matched residual. Only verified reduction
applies the existing partial-P&L estimate and breakeven-stop behavior.

C2b stores request, receipt and `partial_residual_observed` evidence independently
of lifecycle outcome. Durable evidence is not broker confirmation. Existing
storage tests now assert retained live state in the unavailable-evidence case;
successful accounting fixtures supply an actual residual size. Pending state is
additive and survives the existing JSON state serializer. An ambiguous pending
partial requires later quantity reconciliation; it is not marked economically
complete. No entry sizing, stop/target constants or alpha rules changed.

## PS1-E: capital-flow accounting (policy resolved; data prerequisite remains)

Source semantics: `_live_poll_balance` reads available cash into `balance`,
floating P&L into `balance_pnl`, cash balance plus floating P&L into
`balance_total`, and deposit/margin into `balance_margin`. It selects the first
preferred OR CFD account, which warrants separate account-identity scrutiny.
Polling is every five minutes, with a local fetch timestamp, not a broker
as-of timestamp. Day start is the first successful fetch in a UTC day, not an
exact midnight valuation. Its seed is available+margin; the source calls this
cash-only but that identity must not be assumed to exclude floating P&L.

The per-day `june_balance_day_start:YYYY-MM-DD` key lasts 36h and wins over the
state blob on restart. Thus editing only the blob can be undone by startup.
This is a supported explanation for 32.84 reappearing, not proof of who changed
which production key. PS1 has not modified either key or re-read current balances.

The circuit breaker compares equity against that persisted start figure. Its
intent explicitly includes unrealized loss and gates new entries. It uses a
tiered dollar loss budget, not merely the percentage named in its docstring.
At 32.84 the budget is 4.926; at 182.84 it is 20.00. Zero/negative current equity
is currently skipped as if unavailable, another risk requiring follow-up.

The reported $150 DEPO, `Trade Based Concession - Jul26`, is non-trading capital.
It increases equity and can mask losses under the current circuit breaker.
`_live_poll_pnl` filters to DEAL, so that DEPO is not cumulative earned P&L.
That filter does not establish complete broker-net economics: financing, fees,
credits and other transaction types need explicit classification, completeness
and attribution. Those history adapters belong to C2c and were not started.

| Model | Loss measurement | Example: start 32.84, credit 150, trading loss 10, equity 172.84 |
|---|---|---|
| A: fixed baseline | equity minus start | +140 apparent gain; daily loss masked |
| B: flow-adjusted baseline | equity minus start minus signed external flows | -10 trading result; includes change in floating P&L |
| C: broker-net realized P&L | realized deals plus attributable costs | -10 if realized; 0 if entirely unrealized |

B best matches the documented equity-loss concept, provided opening equity and
complete external-flow evidence are available. It still leaves a policy choice:
hold the day's risk budget at 4.926, or recalculate it after funding (20.00).
With a $10 loss the first breaches and the second does not. A withdrawal needs
the corresponding signed adjustment so returning capital is not called a loss.
Fees/financing should not automatically be excluded as external capital; their
treatment must match the chosen net trading-loss concept. C alone would drop
the current unrealized-loss protection. No baseline reset, formula change or
policy choice had been implemented when these options were presented.

The user subsequently selected: **keep the start-of-day loss budget; exclude
capital flows**. Thus the intended loss is `opening_equity + net_external_flows
- current_equity`, compared with the dollar budget fixed from opening capital.
This resolves the policy question. Implementation remains deferred because the
reported credit's booking time, inclusion in the day-start seed, and completeness
of other flows are not established. Adding 150 to today's baseline could double
count prior-day capital. No trusted runtime flow total or coverage marker exists.
Building that history source now would enter excluded C2c scope. The next step is
to establish dated, account-specific flow evidence and opening equity, then wire
the agreed calculation without silently changing its fixed risk budget.

### C verification

Six targeted residual tests passed; crash-boundary rerun plus those tests: seven passed. Full 146-test suite passed in 11.355s. The uncommitted PS1-D red tests were explicitly excluded from this C-only run. The earlier full run exposed an outdated crash injection at the first save; it now injects at the intended post-reduction save. Test harnesses reuse the same parsed, immutable source AST per process instead of reparsing for every fixture; production code is still executed, never imported. py_compile, diff --check and complete source/test diff review passed. Changed production scope: only `_live_partial_tp_exit`; its sole caller remains `_live_check_exit`. Payload contract, min-deal full-close fallback before submission, and successful verified partial economics remain unchanged.

Commit: `4688569`.

## PS1-D: position evidence and margin inference

| Baseline use | Baseline classification | Finding / repair |
|---|---|---|
| Guard: connected stream, margin=0 | PRIMARY inference | Stale zero suppressed live deals; removed |
| Guard: offline stream, accounts deposit zero/positive | SECONDARY-labelled but decisive | Account-wide value neither proves this deal absent nor present; replaced by inventory |
| `_live_check_exit`: cached zero margin | PRIMARY inference | Returned before max-hold/protective checks; removed |
| Partial verifier: missing account evidence defaults to zero | PRIMARY inference | Tracking loss; repaired in C |
| Full verifier: positive margin after this deal disappears | HEURISTIC treated as decisive orphan proof | Other concurrent exposure falsely blocked management; removed |
| Addon close: margin increase >0.10 | HEURISTIC treated as decisive orphan proof | Lag misses orphan; other exposure causes false alert; replaced with specific outcome evidence |
| Recovery: unavailable inventory plus deposit=0 | PRIMARY inference | Fabricated empty inventory; removed |
| Eligibility, sizing, margin gate, day-start seed | Account capacity/accounting, not individual lifecycle proof | Reviewed; no sizing/leverage constants changed |

`_ls_get_margin` has no observation timestamp. Connection state does not establish
freshness; reconnect retains account cache, and stream snapshot/lost-update hooks
do not establish a complete position inventory. Absence of a close event is not
proof that a deal is open. REST accounts are account-wide and can include multiple
positions, add-ons, delayed updates and stale local/Redis snapshots. Margin is
therefore **HEURISTIC ONLY for individual position state**, even when useful as
an account capacity measure. No margin read remains in the repaired lifecycle
decisions. A matched `affectedDeals` FULLY_CLOSED or stream closure is specific
proof; valid account inventory distinguishes presence, absence and unavailable.

Initial eight regressions produced six failures. The final matrix adds malformed
inventory, accepted-but-still-present full close, residual resynchronization and
protective sizing. Full and addon closes retain tracking unless matched closure
or valid inventory absence is observed. Full-close estimated history/learning is
no longer applied merely because an order was ACCEPTED while outcome is unknown.
New C2b outcome observations precede mutation. Unavailable or malformed recovery
inventory retains state and blocks entry immediately; it is never converted into
an empty broker response. No missing account field is interpreted as zero proof.

Protective full/addon exits may attempt a deal-specific DELETE when inventory is
unavailable or manual-review caution is set. This depends on A's restricted close
contract: it cannot become an opening/netting order. Positive deal-closure evidence
still suppresses an unnecessary request. Entry and addon expansion remain blocked
under unresolved inventory/manual-review/pending-partial conditions. Existing
kill-switch authorization is unchanged. Unresolved addon legs survive failed
close attempts and are retried by the existing orphan-exit path in later cycles.

A pending partial triggers quantity reconciliation before protective full close.
The actual positive finite broker residual updates tracked size and proportional
notional, retaining original basis and pending economic evidence. Missing partial
P&L is not invented. Missing quantity can still cause an oversized DELETE to be
rejected; it cannot create exposure. Delayed economics and restart idempotence
remain limitations for later ledger work. All new state fields are additive JSON.

### D verification

Targeted position-evidence plus C2b capture tests: 31 passed. Full suite: 157 passed in 10.596s. An initial full run correctly detected the added outcome event in two exact-event crash expectations; those now require it. py_compile, whitespace/stat/status checks and complete source/test diff review passed. AST scope: guard, primary close, partial comment, exit check, addon close, entry caution, pyramid-entry caution, main-loop orphan handling and recovery. All module-level assignments/constants are unchanged. No broker payload contract beyond A changed; a pending partial now supplies the verified residual size to an existing protective close.
