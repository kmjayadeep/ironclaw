# Static Kernel Process Journal Proof

**Date:** 2026-07-24
**Status:** Code-backed migration slice

## Result

The static-kernel direction is viable for the durable lifecycle currently named
`turn-run`: queueing, claiming, leasing, suspension, cancellation, recovery,
terminal transitions, and journaling can be expressed as neutral process
vocabulary without changing runtime behavior.

The canonical journal vocabulary now lives in
`crates/ironclaw_processes/src/journal.rs`. The adapter proof is in
`crates/ironclaw_turns/src/process_journal.rs`, which maps the existing turn
store and runner contracts into those process-owned contracts:

- `TurnRunRecord` maps to `JournaledProcessSnapshot`.
- `TurnRunState` maps to `JournaledProcessSnapshot`.
- `TurnLifecycleEvent` maps to `ProcessJournalEntry`.
- `TurnStatus` maps to `ProcessLifecycleStatus`.
- blocked gate status maps to `KernelProcessSuspension`.
- `TurnRunnerOutcome` maps to `ProcessOutcome`.
- runner lease and transition request envelopes map to process-shaped request
  envelopes.

The tests intentionally use exhaustive `match` tables. Adding a new turn status,
event kind, or runner outcome now forces an explicit process-kernel decision.

## Second Slice

The process crate now owns `ProcessTransitionPort` alongside the journal
vocabulary. The port covers claim, batch claim, heartbeat, lease recovery,
suspend, complete, cancel, fail, and relinquish transitions in process terms.

`ironclaw_turns::AgentTurnProcessTransitionAdapter` implements that process port
over the existing `TurnRunTransitionPort`. The adapter is tested against the
real in-memory turn store: a queued turn is submitted through the existing turn
store, then claimed, heartbeated, and completed through the process port. This
proves the process API can sit over the current durable state machine without
introducing a parallel process store.

## Composition Slice

`ironclaw_reborn_composition` now wires an internal
`Arc<dyn ProcessTransitionPort<Error = TurnError>>` from the same production
`TurnStateRowStore` used by the scheduler and runner. This does not expose a new
runtime API yet; it proves production composition can carry a process-owned
transition handle without duplicating the turn store or introducing a second
scheduler.

## Scheduler Maintenance Slice

`ironclaw_runner::TurnRunScheduler` now carries a process transition port and
uses it for scheduler-owned lifecycle maintenance:

- heartbeat for claimed work;
- expired lease recovery;
- shutdown relinquish;
- scheduler terminal failure recording.

The scheduler still claims `ClaimedTurnRun` through `TurnRunTransitionPort`
because the executor contract currently requires turn-specific run profile
payloads (`ResolvedRunProfile`, loop driver context, and turn scope). That is
the next hard boundary: full claim migration needs either an agent-turn process
claim payload or post-claim profile resolution behind the executor adapter.

## Boundary Found

The kernel should own the durable process state machine and journal. It should
not own agent-loop validation semantics.

The remaining agent-loop residue is visible in the turn adapter's
`KernelApplyValidatedExitRequest`: it still carries `LoopExitMapping`.
That is acceptable for this slice because loop-exit validation is executor or
extension behavior. The kernel-facing transition should eventually receive a
validated `KernelProcessOutcome`, while the agent-loop adapter remains
responsible for converting `LoopExit` into that outcome.

## Migration Implication

This proves the next cut should be a rename-and-collapse, not a parallel layer:

1. Add a process transition port over the existing turn transition port.
2. Move agent-turn metadata behind an extension/executor-specific payload.
3. Rename the durable runner/store implementation from turn-run to process-run.
4. Leave product/thread/chat admission APIs as adapters that start or resume an
   `agent_turn` process.

The first item is now implemented and production-wired as a migration façade.
The next reducer is the third item: rename/move the backing transition engine
and row-store contracts in place, with `TurnRunTransitionPort` becoming the
compatibility adapter rather than the canonical port.

The design would become counterproductive if a second process store or scheduler
is added beside `TurnStateStore` / `TurnRunTransitionPort`. The simplification
only materializes when those names and contracts are replaced in place.
