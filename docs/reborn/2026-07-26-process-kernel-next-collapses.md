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

Status on `process-journal-kernel-transition`: production lifecycle persistence
is collapsed. Capability/background records are submitted as
`ProcessKind::CapabilityInvocation`, and `ProcessServices` uses a journal-backed
compatibility projection. `process_store.rs`, lifecycle decorators, and their
parallel state-machine tests are deleted. Externalized result bodies remain in
the dedicated result store.

The compatibility `ProcessStorePort`/`ProcessRecord` surface still exists for
the background manager and host API. It is no longer a durable authority and
should disappear with Slice 6's generic supervisor.

Terminal capability-obligation cleanup is now also a process-journal commit
observer. The observer is registered once against the final runtime and follows
governor replacement without replacing the lifecycle component. This removes
the semantic blocker that previously required host kill and supervisor
completion to pass through a `ProcessStorePort` wrapper; pre-submit handoff
claiming remains the only lifecycle action that must happen before a journal
commit.

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

Status on `process-journal-kernel-transition`: invocation lifecycle is now a
native process-journal projection. The host runtime maps `InvocationId`
directly to `ProcessId`, records authorization and approval waits as process
suspensions, and resumes/claims the same process before terminal transition.
The filesystem-backed `RunStateStore` and `/run-state` record path are deleted.

The compatibility lifecycle DTOs/ports, lifecycle fake, combined
run-state/approval port, and host-runtime combined-store wiring are deleted.
`CapabilityHost` and `DefaultHostRuntime` consume
`ProcessInvocationStatePort` directly. Approval and gate persistence moved into
`ironclaw_approvals`, and the `ironclaw_run_state` crate was deleted.

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

Status on `process-journal-kernel-transition`: implemented. Generic dependency
records, transitions, scoped queries, and host-wide unresolved queries are
owned by `ironclaw_processes`. Child submission atomically creates the child
process, reserves tree capacity, and opens its dependency in one journal row;
consume/abandon atomically closes the dependency and releases that reservation.

The runner await-edge store is now a projection adapter over
`ProcessDependencyPort`. The 1,457-line filesystem store, 561-line roster, and
most of the 720-line boot-recovery driver were deleted. Spawn no longer writes
an await edge before child submission, and terminal handling no longer
reconstructs missing process truth from turn/thread metadata. Agent-specific
result framing, group readiness, and parent resume remain in the runner.

The slice currently contributes roughly 3k net deleted lines across production
and tests while adding the generic process contract and atomicity/stress tests.

Historical inventory:

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

Status on `process-journal-kernel-transition`: implemented. A process
checkpoint command now carries a bounded, debug-redacted opaque payload beside
its ref and schema metadata, and the command is one physical journal row.
Agent-loop checkpoint records are projections over that row.

The host stages bytes only in memory until `checkpoint`, which commits payload
and metadata atomically. Resume and loop-exit evidence read the payload from
the process projection. The separate `/checkpoint-state` mount,
`CheckpointStateStorePort`, filesystem implementation, contract suite, and
composition wiring are deleted. Stable checkpoint scope binds the process
invocation axis to `TurnRunId`, so put/get cannot mint mismatched scopes.

Historical inventory:

Checkpoint state was split between generic process checkpoint
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

Status on `process-journal-kernel-transition`: implemented. Process submission
now accepts a bounded, debug-redacted immutable input payload and exposes only
its opaque schema ref in process snapshots and lifecycle events. The payload is
committed in the same physical journal row as child identity, tree reservation,
and dependency creation, then read through the scope-bound `ProcessInputPort`.

Subagent spawning serializes the agent-owned `SubagentGoalRecord` as
`subagent-goal:v1` process input. Prompt material projects it from the process
journal, with the persisted child-thread message retained as legacy fallback.
The 527-line goal store, its filesystem records, write/delete compensation,
runtime trait union, composition field, readiness component, and test doubles
are deleted.

The payload is stored directly in the private command row rather than through a
new artifact subsystem. That keeps submission atomic and avoids replacing one
small bespoke store with a larger generic one. The bounded payload is absent
from public process snapshots and event projections.

## Slice 6: generalize scheduler wake and cancellation

Status on `process-journal-kernel-transition`: generic claim-loop wakeup,
bounded concurrency, lease heartbeat/recovery, executor panic containment,
terminal-failure recording, and shutdown lease relinquishment now live in
`ironclaw_processes::ProcessSupervisor`. The former 1,129-line turn scheduler is
a small `ProcessKind::AgentTurn` adapter over that supervisor; its separate
executor-task and latency modules are deleted.

The runner registers the executor for `ProcessKind::AgentTurn`; extension and
host runtimes register their own executors. Scheduling policy, model turns, and
agent-loop behavior stay outside the kernel.

`ProcessServices` no longer spawns a detached lifecycle task. Its compatibility
`BackgroundProcessManager` journals bounded durable input, wakes a
`ProcessKind::CapabilityInvocation` supervisor, and registers cancellation when
the process is submitted. The same supervisor now owns claiming, bounded
concurrency, heartbeats, recovery, panic containment, and shutdown for turns and
capability work.

The remaining deletion is the compatibility `ProcessStorePort`/`ProcessRecord`
projection used by capability and host-runtime callers. It no longer schedules
or owns lifecycle state; callers can move incrementally to journal-native
snapshots and ports before the adapter is removed.

There is no longer a second `JournalProcessStore` object or a `ProcessStore`
type alias: `ProcessServices`
holds the authoritative `ProcessJournalStore` directly, and the legacy
`ProcessStorePort` methods are a compatibility implementation on that same
shared store.

`ProcessServices` is also no longer generic over lifecycle/result-store
implementations. It erases those behind its owned ports while retaining
concrete type identity for production-readiness validation. As a result,
`HostRuntimeServices` carries only its filesystem and resource-governor type
parameters instead of propagating process store types through composition.

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
