# Missing live-state startup / restart integrity

Parent: 22e03a008adbc9d7fc20bd1340544f3ee7d854f5. Its recorder, broker-opening
settlement matcher and atomic performance-delivery repairs remain byte-identical.

`live_state_integrity` distinguishes KNOWN_FLAT, KNOWN_EXPOSED, UNKNOWN,
MISSING and ERROR. A flat gate requires an explicit primary-null state with
empty addon inventory, required risk/accounting/admission fields, no pending
submission or recovery ambiguity, and independently verified broker flatness
and empty working orders. The gate rejects a local value that changes while
broker truth is read. Never use `GET(...) or '{}'` to prove flatness.

`deployment_state_gate.py` is a read-only VPS entry point, with no stop/restart,
orders or state mutations. Its result is an immediate snapshot. The existing
exact HEAD, service, control-state and no-order-race gates are still required;
a saved or earlier gate result is not permission to restart later.

Startup validates local state before live defaults, balance polling or live
state saves. Historic missing/unreadable/incomplete state raises
StateRecoveryRequired; it does not zero counters or create an ordinary fresh
account. Broker-open missing state requires recovery, never a new order.
Verified genuine new installs (no durable/legacy history, verified broker flat,
no working orders) can initialize normally. Database/Redis/broker errors make
fresh-install proof unknown and therefore block initialization. The verified
payload is passed into the existing loader to avoid a second Redis-read race.
Existing broker reconciliation and all trading policy remain unchanged.

This candidate does not reconstruct the October 4 incident state, does not
repair Redis capacity or claim to prevent server eviction, and cannot make the
current reset runtime economically equivalent by restarting. Unknown causal
attribution is not a reason to redesign retention or change Redis configuration.
The historic state disappearance and failed-open gate are distinct findings.

Do not restart this candidate merely because tests pass. The task requires it
to remain ready when another restart is not demonstrably necessary and safe.
Recovery of economically active failed-thesis/streak/accounting state requires
an authoritative reviewed projection; no incident snapshots are automatically
written into production.

Tests A–J exercise exact incident snapshots, corrupt/missing/error reads,
historical-data protection, genuine fresh installation, matching exposure,
changing gate snapshots, preserved counter roundtrip, and the actual flat
cleanup function. Full strategy AST outside startup/load is identical to the
parent; all existing strategy policy and plumbing module bytes are frozen.
