# Process kernel next collapses

This inventory is grounded in the production tree at
`fcbdfc7bb1811ab9e7ea19eb41cc2f3c1c2ced3b`. The objective is net deletion:
one process lifecycle, one journal, and domain projections over it.

The stress rerun establishes a prerequisite: first partition journal writes.
Adding the state below to the current growing `state.json` would increase CAS
conflicts and serialization cost.

## Slice 0: row-native process persistence

Implemented on `process-journal-kernel-transition` as an append-authoritative
command journal:

- every process mutation is one immutable row under
  `/processes/journal/records`;
- the backend sequence is the total order used to resolve exclusivity, claims,
  leases, tree capacity, checkpoints, and idempotency;
- process snapshots, gates, parent/child views, and lifecycle entries are
  deterministic projections over that log;
- the old `state.json` is accepted only as one-time migration input.

This uses one authority instead of dual-writing mutable process rows and a
transition log. In particular, libSQL does not expose the multi-key transaction
needed to make that dual-write design safe across crashes.

Acceptance:

- zero store failures in both rerun matrices through c100;
- cross-handle and cross-process row-ordering tests preserve every transition;
- no unconditional-write or non-event fallback;
- terminal history does not make one unrelated process transition O(total
  historical processes);
- restart projections reproduce live locks, gates, leases, and dependencies.

The first two gates are complete. The 2026-07-26 rerun through c100 has no
process-journal unavailable/storage failures; remaining failures are expected
exclusive-thread admission. Bounded compaction and durable indexed projection
checkpoints remain follow-up work before treating unbounded terminal-history
soaks as complete.

## Slice 1: retire the second process lifecycle

`ironclaw_processes` still has a separate capability/background process stack:

- `process_store.rs`: 920 lines
- `services.rs`: 378 lines
- `wrappers.rs`: 440 lines
- `types.rs`: 452 lines
- `host.rs`: 258 lines
- `cancellation.rs`: 139 lines

`ProcessStorePort` has its own `start`, `complete`, `fail`, `kill`, `get`, and
`records_for_scope` lifecycle beside `ProcessJournalStore`. `ProcessRecord` is a
second durable process record and `EventingProcessStore` projects a second event
path.

Move capability/background execution to `ProcessKind::Internal` or a named
`CapabilityInvocation` kind. Put its extension, capability, runtime, grants,
mount, reservation, and continuation data in typed process metadata. Adapt
`ProcessServices` and `ProcessHost` to the journal, then delete
`ProcessStorePort`, `ProcessStore`, and lifecycle wrappers. Keep externalized
result bodies behind result references; do not put arbitrary output in journal
metadata.

Expected result: the largest low-risk consolidation and one lifecycle for
agent, capability, and background work.

## Slice 2: dissolve `ironclaw_run_state`

`ironclaw_run_state/src/lib.rs` is 1,019 lines of invocation lifecycle:
`start`, approval/auth blocking, `complete`, `fail`, scoped lookup, and listing.
Those states overlap process submission, suspension, gates, and terminal
transitions.

Represent each host invocation as a process whose `InvocationId` is indexed
metadata. Approval and authorization blocking become process suspension with a
gate reference. Capability host helpers query the process projection.

Keep approval decision authority in the approval subsystem. The process journal
should record that a process is waiting on an approval and the durable decision
reference; it should not become the policy or approval authority.

Do this with Slice 1 so capability execution does not migrate through a third
temporary lifecycle.

## Slice 3: make child dependencies process edges

The subagent await-edge implementation duplicates generic process dependency
state:

- `await_edge/store.rs`: 1,457 lines
- `await_edge/boot_recovery.rs`: 720 lines
- `await_edge/resolver.rs`: 1,879 lines
- `await_edge/roster.rs`: 561 lines

Move parent-child wait relationships into a generic process dependency record:
open, settled, consumed/closed, terminal evidence, and reservation-release
state. Make edge mutation atomic with the corresponding process-tree capacity
change where required.

The process journal can enumerate unresolved dependencies directly, eliminating
the roster marker and most boot-recovery machinery. Agent-specific aggregation
of child results remains a runner projection.

This has the highest deletion potential, but follows Slice 0 because it requires
indexed edge queries and atomic edge/tree mutations.

## Slice 4: unify checkpoint metadata and payload

Checkpoint state is currently split between generic process checkpoint
metadata and a separate loop-host payload store:

- `loop_host/checkpoint_state_store.rs`: 447 lines
- `turns/checkpoint_state.rs`: 242 lines
- `turns/process_projection/loop_checkpoint.rs`: 128 lines

Extend the generic process checkpoint contract with a bounded opaque payload or
a host-owned artifact reference. Keep schema interpretation in the agent loop.
Then remove the separate checkpoint-state filesystem record and projection
bridge.

Do not embed unbounded or secret-bearing payloads directly in journal entries.

## Slice 5: make subagent goals immutable process input

`runner/subagent/goal_store.rs` is 527 lines for write-once, scoped payloads
keyed by child run. This is generic immutable process input, not lifecycle
authority.

Add a bounded `ProcessInputRef` backed by the host artifact/filesystem service,
store the reference on process submission, and delete the bespoke goal store.
The `SubagentGoal` schema remains agent-owned.

## Slice 6: generalize scheduler wake and cancellation

`runner/turn_scheduler.rs` is 1,129 lines and still owns a turn-named wake
channel around generic process claiming. Move claim-loop wakeup, lease
heartbeat, shutdown, and cancellation-handle registration into a generic
process supervisor.

The runner registers the executor for `ProcessKind::AgentTurn`; extension and
host runtimes register their own executors. Scheduling policy, model turns, and
agent-loop behavior stay outside the kernel.

This should follow lifecycle unification so it replaces both the turn scheduler
and `ProcessServices` background manager rather than creating another manager.

## Recommended order

1. Partition journal persistence and rerun the four stress artifacts.
2. Merge `ProcessStorePort` and `ironclaw_run_state` into the journal.
3. Move process results/evidence and cancellation registration behind the
   unified runtime.
4. Replace await-edge/roster/recovery with process dependencies.
5. Fold checkpoint payload and immutable process input into generic references.
6. Generalize scheduler/supervisor wiring and leave agent turns as an executor
   projection.

The surveyed files total roughly 10.7k lines. Not all are deletable, but Slices
1-3 should produce substantial net deletion because they remove complete stores,
state machines, wrappers, and recovery paths rather than merely relocating
types.
