# ironclaw_run_state guardrails

- Own approval request records and gate records (the model-visible
  `GateRecord` a pending gate renders from, keyed by `GateRef`).
- Do not own invocation or process lifecycle. Those records and projections
  belong to `ironclaw_processes`.
- All lookups and transitions are resource-owner scoped (tenant/user/agent/project/mission/thread); wrong-scope access must look unknown.
- Durable persistence uses typed stores over a `ScopedFilesystem`; the
  PostgreSQL/libSQL choice is made at the `RootFilesystem` layer underneath.
- Do not persist raw replay input or runtime output in approval or gate records.
- Keep approval records as control-plane state, not authority by themselves.
