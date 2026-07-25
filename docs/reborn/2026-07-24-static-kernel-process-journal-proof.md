# Static Kernel Process Journal Proof

**Date:** 2026-07-24
**Status:** Code-backed migration slice

## Result

The static-kernel direction is viable for the durable lifecycle currently named
`turn-run`: queueing, claiming, leasing, suspension, cancellation, recovery,
terminal transitions, and journaling can be expressed as neutral process
vocabulary without changing runtime behavior.

The canonical journal vocabulary and store now live in
`crates/ironclaw_processes/src/journal.rs` and
`crates/ironclaw_processes/src/journal_store.rs`. The remaining turn adapter
code is split under `crates/ironclaw_turns/src/process_journal/`, where it maps
turn views and runner contracts into those process-owned contracts:

- `TurnRunRecord` maps to `JournaledProcessSnapshot`.
- `TurnRunState` maps to `JournaledProcessSnapshot`.
- `TurnLifecycleEvent` maps to `ProcessJournalEntry`.
- `TurnStatus` maps to `ProcessLifecycleStatus`.
- blocked gate status maps to `ProcessSuspension`.
- `TurnRunnerOutcome` maps to `ProcessOutcome`.
- runner lease and transition request envelopes map to process-shaped request
  envelopes.

The tests intentionally use exhaustive `match` tables. Adding a new turn status,
event kind, or runner outcome now forces an explicit process-kernel decision.

## Second Slice

The process crate now owns `ProcessTransitionPort` alongside the journal
vocabulary. The port covers claim, batch claim, heartbeat, lease recovery,
suspend, complete, cancel, fail, and relinquish transitions in process terms.

`ironclaw_turns::AgentTurnProcessTransitionAdapter` remains only as
`test-support` compatibility over the existing `TurnRunTransitionPort`. The
production path now uses the process journal store directly.

## Composition Slice

`ironclaw_reborn_composition` now wires process transition, journal, lifecycle,
and gate-query handles from the process journal store. Trigger active-run lookup
and blocked-auth fanout read through those process handles instead of deriving a
process projection from `TurnStateRowStore`.

## Scheduler Maintenance Slice

`ironclaw_runner::TurnRunScheduler` now carries a process transition port and
uses it for scheduler-owned lifecycle maintenance:

- batch claim for queued work;
- heartbeat for claimed work;
- expired lease recovery;
- shutdown relinquish;
- scheduler terminal failure recording.

Process claims for `AgentTurn` now carry enough typed `agent_turn` metadata to
reconstruct the current executor's `ClaimedTurnRun` view, including the resolved
run profile. The scheduler claims through `ProcessTransitionPort` and converts
to the turn executor view only at the executor boundary. The remaining turn-run
executor dependency is therefore an adapter/view concern, not scheduler kernel
state.

## Read-Side Journal Slice

`ironclaw_processes` now owns `ProcessJournalSource`, the canonical read-side
contract for process snapshots and ordered process journal pages. It covers
scoped reads and global durable projection reads without depending upward on
turn types.

`TurnEventProjectionFromProcessJournal` implements the old
`TurnEventProjectionSource` as a compatibility view over `ProcessJournalSource`.
Tests prove the old turn event view can be projected from process journal
entries.

## Boundary Found

The kernel should own the durable process state machine and journal. It should
not own agent-loop validation semantics.

The remaining agent-loop residue is visible at the turn transition adapter:
`ApplyValidatedLoopExitRequest` still carries `LoopExitMapping`. That is
acceptable for this slice because loop-exit validation is executor or extension
behavior. The kernel-facing transition receives process outcomes, while the
agent-loop adapter remains responsible for converting `LoopExit` into that
outcome.

## Migration Implication

This proves the next cut should be a rename-and-collapse, not a parallel layer:

1. Keep agent-turn metadata behind an extension/executor-specific payload.
2. Rename the remaining durable runner/store implementation from turn-run to
   process-run where it is still a turn compatibility view.
3. Remove test-support seams that pass `TurnStateRowStore` where a process
   lifecycle or gate source is meant.
4. Leave product/thread/chat admission APIs as adapters that start or resume an
   `agent_turn` process.

The process transition, journal source, lifecycle lookup, and gate query are now
implemented and production-wired from the process journal store.
`TurnRunTransitionPort` and `TurnEventProjectionSource` are compatibility views,
not canonical process ports.

The design would become counterproductive if a second process store or scheduler
is added beside `TurnStateStore` / `TurnRunTransitionPort`. The simplification
only materializes when those names and contracts are replaced in place.
