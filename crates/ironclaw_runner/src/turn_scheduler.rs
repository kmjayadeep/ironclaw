use std::{
    collections::HashMap, error::Error, fmt, panic::AssertUnwindSafe, sync::Arc, time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use futures_util::FutureExt;
use ironclaw_host_api::ProcessId;
use ironclaw_observability::live_latency_started_at;
use ironclaw_processes::{
    ClaimProcessesRequest, FailProcessRequest, ProcessKind, ProcessLeaseRequest, ProcessLeaseToken,
    ProcessTransitionPort, ProcessWorkerId, RecoverExpiredProcessLeasesRequest,
};
use ironclaw_turns::{
    SanitizedFailure, TurnError, TurnLeaseToken, TurnRunId, TurnRunWake, TurnRunWakeNotifier,
    TurnRunWakeNotifyError, TurnRunnerId, TurnScope, claimed_turn_run_from_process_claim,
    runner::ClaimedTurnRun,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
    time::{MissedTickBehavior, interval, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::debug;

mod executor_task;
mod latency;
use self::executor_task::ExecutorTaskOutcome;

const MAX_CLAIMS_PER_DRAIN_BATCH: usize = 128;

#[derive(Debug, Clone)]
pub struct TurnRunSchedulerConfig {
    max_concurrent_runs: usize,
    poll_interval: Duration,
    lease_recovery_interval: Duration,
    runner_heartbeat_interval: Duration,
    max_consecutive_heartbeat_failures: usize,
    terminal_failure_record_attempts: usize,
    terminal_failure_record_backoff: Duration,
    claim_error_backoff: Duration,
    wake_channel_capacity: usize,
}

impl Default for TurnRunSchedulerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_runs: 4,
            poll_interval: Duration::from_secs(5),
            lease_recovery_interval: Duration::from_secs(10),
            runner_heartbeat_interval: Duration::from_secs(30),
            max_consecutive_heartbeat_failures: 3,
            terminal_failure_record_attempts: 3,
            terminal_failure_record_backoff: Duration::from_millis(100),
            claim_error_backoff: Duration::from_secs(1),
            wake_channel_capacity: 128,
        }
    }
}

fn non_zero_duration(duration: Duration) -> Duration {
    if duration.is_zero() {
        Duration::from_millis(1)
    } else {
        duration
    }
}

impl TurnRunSchedulerConfig {
    pub fn max_concurrent_runs(&self) -> usize {
        self.max_concurrent_runs
    }

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    pub fn lease_recovery_interval(&self) -> Duration {
        self.lease_recovery_interval
    }

    pub fn runner_heartbeat_interval(&self) -> Duration {
        self.runner_heartbeat_interval
    }

    pub fn max_consecutive_heartbeat_failures(&self) -> usize {
        self.max_consecutive_heartbeat_failures
    }

    pub fn terminal_failure_record_attempts(&self) -> usize {
        self.terminal_failure_record_attempts
    }

    pub fn terminal_failure_record_backoff(&self) -> Duration {
        self.terminal_failure_record_backoff
    }

    pub fn claim_error_backoff(&self) -> Duration {
        self.claim_error_backoff
    }

    pub fn wake_channel_capacity(&self) -> usize {
        self.wake_channel_capacity
    }

    pub fn with_max_concurrent_runs(mut self, max_concurrent_runs: usize) -> Self {
        self.max_concurrent_runs = max_concurrent_runs.max(1);
        self
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = non_zero_duration(poll_interval);
        self
    }

    pub fn with_lease_recovery_interval(mut self, lease_recovery_interval: Duration) -> Self {
        self.lease_recovery_interval = non_zero_duration(lease_recovery_interval);
        self
    }

    pub fn with_runner_heartbeat_interval(mut self, runner_heartbeat_interval: Duration) -> Self {
        self.runner_heartbeat_interval = non_zero_duration(runner_heartbeat_interval);
        self
    }

    pub fn with_max_consecutive_heartbeat_failures(
        mut self,
        max_consecutive_heartbeat_failures: usize,
    ) -> Self {
        self.max_consecutive_heartbeat_failures = max_consecutive_heartbeat_failures.max(1);
        self
    }

    pub fn with_terminal_failure_record_attempts(
        mut self,
        terminal_failure_record_attempts: usize,
    ) -> Self {
        self.terminal_failure_record_attempts = terminal_failure_record_attempts.max(1);
        self
    }

    pub fn with_terminal_failure_record_backoff(
        mut self,
        terminal_failure_record_backoff: Duration,
    ) -> Self {
        self.terminal_failure_record_backoff = non_zero_duration(terminal_failure_record_backoff);
        self
    }

    pub fn with_claim_error_backoff(mut self, claim_error_backoff: Duration) -> Self {
        self.claim_error_backoff = non_zero_duration(claim_error_backoff);
        self
    }

    pub fn with_wake_channel_capacity(mut self, wake_channel_capacity: usize) -> Self {
        self.wake_channel_capacity = wake_channel_capacity.max(1);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnRunExecutorError {
    failure: SanitizedFailure,
}

impl TurnRunExecutorError {
    pub fn new(failure_category: impl Into<String>) -> Result<Self, String> {
        SanitizedFailure::new(failure_category).map(|failure| Self { failure })
    }

    /// Build from an already-sanitized failure, preserving its model-visible
    /// `detail`. The scheduler records `failure()` on the returned error, so
    /// this is how a driver-computed `SanitizedFailure` (category + scrubbed
    /// detail) reaches `TurnLifecycleEvent.detail` and the failure explainer.
    pub fn from_failure(failure: SanitizedFailure) -> Self {
        Self { failure }
    }

    pub fn failure(&self) -> &SanitizedFailure {
        &self.failure
    }

    pub fn failure_category(&self) -> &str {
        self.failure.category()
    }
}

impl fmt::Display for TurnRunExecutorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "turn run executor failed: {}",
            self.failure.category()
        )
    }
}

impl Error for TurnRunExecutorError {}

#[async_trait]
pub trait TurnRunExecutor: Send + Sync {
    async fn execute_claimed_run(
        &self,
        claimed: ClaimedTurnRun,
        process_transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>,
    ) -> Result<(), TurnRunExecutorError>;
}

pub struct TurnRunScheduler {
    process_transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>,
    executor: Arc<dyn TurnRunExecutor>,
    config: TurnRunSchedulerConfig,
    runner_id: TurnRunnerId,
}

impl TurnRunScheduler {
    pub fn new_with_process_transition(
        process_transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>,
        executor: Arc<dyn TurnRunExecutor>,
        config: TurnRunSchedulerConfig,
    ) -> Self {
        Self {
            process_transitions,
            executor,
            config,
            runner_id: TurnRunnerId::new(),
        }
    }

    pub fn start(self) -> TurnRunSchedulerHandle {
        let capacity = self.config.wake_channel_capacity();
        let (notifier, channel) = SchedulerTurnRunWakeNotifier::channel(capacity);
        self.start_with_channel(notifier, channel)
    }

    /// Start with a pre-created wake channel (from
    /// [`SchedulerTurnRunWakeNotifier::channel`]), consuming both the notifier
    /// and the channel. This is the cycle-breaking entry point used when the
    /// coordinator needs the notifier before the scheduler starts.
    pub fn start_with_channel(
        self,
        notifier: Arc<SchedulerTurnRunWakeNotifier>,
        channel: TurnRunWakeChannel,
    ) -> TurnRunSchedulerHandle {
        let TurnRunWakeChannel {
            command_tx,
            command_rx,
        } = channel;
        let shutdown_token = CancellationToken::new();
        let supervisor = tokio::spawn(run_scheduler_loop(
            command_rx,
            SchedulerLoopInit {
                command_tx: command_tx.clone(),
                process_transitions: self.process_transitions,
                executor: self.executor,
                config: self.config,
                runner_id: self.runner_id,
                shutdown_token: shutdown_token.clone(),
            },
        ));
        TurnRunSchedulerHandle {
            notifier,
            supervisor: Some(supervisor),
            shutdown_token,
        }
    }
}

/// The paired wake-channel bundle (sender + receiver) handed into
/// [`TurnRunScheduler::start_with_channel`].
///
/// Created together with a [`SchedulerTurnRunWakeNotifier`] by
/// [`SchedulerTurnRunWakeNotifier::channel`] to break the
/// coordinator↔scheduler build-order cycle: the caller mints both the
/// notifier and this channel before building the coordinator (so the
/// coordinator can hold the notifier first), then passes this bundle to
/// [`TurnRunScheduler::start_with_channel`] to wire the scheduler loop.
/// Both halves of the underlying mpsc channel are carried here so that
/// `start_with_channel` can clone the sender for internal re-queuing while
/// moving the receiver into the loop.
pub struct TurnRunWakeChannel {
    command_tx: mpsc::Sender<SchedulerCommand>,
    command_rx: mpsc::Receiver<SchedulerCommand>,
}

#[derive(Clone)]
pub struct SchedulerTurnRunWakeNotifier {
    command_tx: mpsc::Sender<SchedulerCommand>,
}

impl SchedulerTurnRunWakeNotifier {
    /// Create a notifier and its paired wake channel before the scheduler is
    /// started, breaking the coordinator↔scheduler build-order cycle.
    ///
    /// The returned notifier can be given to the turn coordinator immediately.
    /// Pass the channel to [`TurnRunScheduler::start_with_channel`] later to
    /// wire the scheduler loop.
    pub fn channel(capacity: usize) -> (Arc<SchedulerTurnRunWakeNotifier>, TurnRunWakeChannel) {
        let (command_tx, command_rx) = mpsc::channel(capacity.max(1));
        let notifier = Arc::new(SchedulerTurnRunWakeNotifier {
            command_tx: command_tx.clone(),
        });
        (
            notifier,
            TurnRunWakeChannel {
                command_tx,
                command_rx,
            },
        )
    }
}

impl fmt::Debug for SchedulerTurnRunWakeNotifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SchedulerTurnRunWakeNotifier")
    }
}

impl TurnRunWakeNotifier for SchedulerTurnRunWakeNotifier {
    fn notify_queued_run(&self, wake: TurnRunWake) -> Result<(), TurnRunWakeNotifyError> {
        let started_at = live_latency_started_at();
        let trace_fields = latency::run_fields_from_wake(started_at, &wake);
        let result = self
            .command_tx
            .try_send(SchedulerCommand::Wake(wake))
            .map_err(|_| TurnRunWakeNotifyError::DeliveryUnavailable);
        latency::notify_queued_run_result(trace_fields.as_ref(), started_at, &result);
        result
    }
}

pub struct TurnRunSchedulerHandle {
    notifier: Arc<SchedulerTurnRunWakeNotifier>,
    /// `Option` so that `shutdown()` can `take()` the handle without a
    /// partial move, which would be disallowed when `Drop` is implemented.
    /// `None` only after `shutdown()` completes or if construction somehow
    /// produced an absent supervisor (not possible via the public API).
    supervisor: Option<JoinHandle<()>>,
    /// Cancellation token for shutdown signalling.  Cancelling this token
    /// bypasses the bounded command queue entirely, so shutdown can never
    /// block even when the queue is full or the loop is parked in a
    /// long `claim_next_run` await.  Both `shutdown()` (async graceful path)
    /// and `Drop` (sync safety-net path) call `cancel()` on this token.
    shutdown_token: CancellationToken,
}

impl TurnRunSchedulerHandle {
    pub fn wake_notifier(&self) -> Arc<SchedulerTurnRunWakeNotifier> {
        Arc::clone(&self.notifier)
    }

    pub fn is_stopped(&self) -> bool {
        self.supervisor.as_ref().is_none_or(|s| s.is_finished())
    }

    /// Graceful shutdown: signal the scheduler loop to stop via the
    /// cancellation token (bypasses the command queue entirely — no
    /// back-pressure, no loss), then await the supervisor task.
    ///
    /// If the handle is dropped without calling `shutdown()` — for example
    /// when a build function returns `Err` after the scheduler has started —
    /// the `Drop` impl cancels the token synchronously instead.
    pub async fn shutdown(mut self) {
        self.shutdown_token.cancel();
        if let Some(supervisor) = self.supervisor.take() {
            #[allow(clippy::let_underscore_must_use)] // join result unused during shutdown
            let _ = supervisor.await;
        }
    }
}

impl Drop for TurnRunSchedulerHandle {
    fn drop(&mut self) {
        // Safety net for error paths: if `shutdown()` was not called (e.g. a
        // build function failed after starting the scheduler), cancel the token
        // so the background task terminates instead of running indefinitely.
        //
        // `cancel()` is synchronous, idempotent, and infallible — it never
        // blocks and never loses the signal regardless of command-queue state.
        // The graceful `shutdown()` path awaits task completion and is preferred
        // wherever an async context is available; Drop is the fallback for
        // synchronous or error-path drops.
        //
        // The supervisor `JoinHandle` is `Option` so that `shutdown()` can
        // `take()` it (avoiding a partial-move from a `Drop`-implementing type).
        // When Drop fires here the `JoinHandle` — if not already taken by
        // `shutdown()` — is dropped, which detaches the tokio task.  The
        // token cancellation above causes the detached task to self-terminate
        // on its next `select!` iteration.
        self.shutdown_token.cancel();
    }
}

#[derive(Debug)]
enum SchedulerCommand {
    Wake(TurnRunWake),
    Drain,
    RetryDrain,
}

/// Identity fields needed to relinquish a claimed run back to Queued.
struct RelinquishIdentity {
    run_id: TurnRunId,
    worker_id: ProcessWorkerId,
    lease_token: ProcessLeaseToken,
}

struct ProcessClaimIdentity {
    run_id: TurnRunId,
    process_id: ProcessId,
    worker_id: ProcessWorkerId,
    lease_token: ProcessLeaseToken,
}

struct SchedulerLoopInit {
    command_tx: mpsc::Sender<SchedulerCommand>,
    process_transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>,
    executor: Arc<dyn TurnRunExecutor>,
    config: TurnRunSchedulerConfig,
    runner_id: TurnRunnerId,
    shutdown_token: CancellationToken,
}

struct SchedulerDrainContext {
    process_transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>,
    executor: Arc<dyn TurnRunExecutor>,
    semaphore: Arc<Semaphore>,
    command_tx: mpsc::Sender<SchedulerCommand>,
    config: TurnRunSchedulerConfig,
    runner_id: TurnRunnerId,
}

async fn shutdown_scheduler(
    context: &SchedulerDrainContext,
    executor_tasks: &mut JoinSet<TurnRunId>,
    active_runs: HashMap<TurnRunId, RelinquishIdentity>,
) {
    // Abort all in-flight tasks first so there is no race between them
    // completing a transition and our relinquish.
    executor_tasks.shutdown().await;
    // Best-effort relinquish: return each aborted run to Queued so a
    // restart can pick it up instead of letting lease expiry mark it Failed.
    for (_run_id, identity) in active_runs {
        let result = context
            .process_transitions
            .relinquish_process(ProcessLeaseRequest {
                process_id: process_id_from_turn_run_id(identity.run_id),
                worker_id: identity.worker_id,
                lease_token: identity.lease_token,
            })
            .await;
        if let Err(error) = result {
            debug!(
                run_id = %identity.run_id,
                error = %error,
                "failed to relinquish in-flight run during scheduler shutdown; run will rely on lease recovery"
            );
        }
    }
}

async fn run_scheduler_loop(
    mut command_rx: mpsc::Receiver<SchedulerCommand>,
    init: SchedulerLoopInit,
) {
    let SchedulerLoopInit {
        command_tx,
        process_transitions,
        executor,
        config,
        runner_id,
        shutdown_token,
    } = init;
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_runs()));
    let mut executor_tasks: JoinSet<TurnRunId> = JoinSet::new();
    // Tracks every in-flight run so we can relinquish on shutdown.
    let mut active_runs: HashMap<TurnRunId, RelinquishIdentity> = HashMap::new();
    let mut poll_tick = interval(config.poll_interval());
    poll_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut recovery_tick = interval(config.lease_recovery_interval());
    recovery_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let context = SchedulerDrainContext {
        process_transitions,
        executor,
        semaphore,
        command_tx,
        config,
        runner_id,
    };
    let mut claim_retry_pending = false;

    loop {
        tokio::select! {
            // CancellationToken arm: bypasses the command queue entirely so
            // shutdown is never blocked by back-pressure or a parked await.
            _ = shutdown_token.cancelled() => {
                shutdown_scheduler(&context, &mut executor_tasks, active_runs).await;
                break;
            }
            Some(command) = command_rx.recv() => {
                match command {
                    SchedulerCommand::Wake(wake) => {
                        // Prefer the woken scope for locality; if that scope has no
                        // claimable work, fall back to the global queue below.
                        if !claim_retry_pending
                            && drain_queued_runs(
                                &context,
                                Some(wake.scope),
                                &mut executor_tasks,
                                &mut active_runs,
                                &shutdown_token,
                            ).await
                        {
                            claim_retry_pending = true;
                            schedule_drain_after(
                                context.command_tx.clone(),
                                context.config.claim_error_backoff(),
                            );
                        }
                        if !claim_retry_pending
                            && drain_queued_runs(
                                &context,
                                None,
                                &mut executor_tasks,
                                &mut active_runs,
                                &shutdown_token,
                            ).await
                        {
                            claim_retry_pending = true;
                            schedule_drain_after(
                                context.command_tx.clone(),
                                context.config.claim_error_backoff(),
                            );
                        }
                    }
                    SchedulerCommand::Drain => {
                        if !claim_retry_pending
                            && drain_queued_runs(
                                &context,
                                None,
                                &mut executor_tasks,
                                &mut active_runs,
                                &shutdown_token,
                            ).await
                        {
                            claim_retry_pending = true;
                            schedule_drain_after(
                                context.command_tx.clone(),
                                context.config.claim_error_backoff(),
                            );
                        }
                    }
                    SchedulerCommand::RetryDrain => {
                        claim_retry_pending = false;
                        if drain_queued_runs(
                            &context,
                            None,
                            &mut executor_tasks,
                            &mut active_runs,
                            &shutdown_token,
                        ).await {
                            claim_retry_pending = true;
                            schedule_drain_after(
                                context.command_tx.clone(),
                                context.config.claim_error_backoff(),
                            );
                        }
                    }
                }
            }
            _ = poll_tick.tick() => {
                if !claim_retry_pending
                    && drain_queued_runs(
                        &context,
                        None,
                        &mut executor_tasks,
                        &mut active_runs,
                        &shutdown_token,
                    ).await
                {
                    claim_retry_pending = true;
                    schedule_drain_after(
                        context.command_tx.clone(),
                        context.config.claim_error_backoff(),
                    );
                }
            }
            Some(result) = executor_tasks.join_next(), if !executor_tasks.is_empty() => {
                match result {
                    Ok(completed_run_id) => {
                        active_runs.remove(&completed_run_id);
                    }
                    Err(error) => {
                        debug!(error = %error, "turn run scheduler executor supervisor task failed");
                    }
                }
            }
            _ = recovery_tick.tick() => {
                recover_expired_leases(Arc::clone(&context.process_transitions)).await;
            }
        }
    }
}

/// Drains the queue of pending runs, spawning executor tasks until the semaphore
/// is exhausted, no run is available, or a claim error occurs.
///
/// Returns `true` if a claim error occurred (caller should schedule a retry),
/// `false` otherwise.
///
/// The `shutdown_token` is checked at the TOP of each iteration — before
/// starting a new process claim call — so that any in-flight claim always
/// finishes and its result is properly inserted into `active_runs` (or handled
/// as an error) before we bail out.  This shape is leak-proof: a claimed-but-
/// untracked run cannot occur because we never abandon an in-progress claim;
/// we only skip starting a NEW claim once cancellation has been observed.
async fn drain_queued_runs(
    context: &SchedulerDrainContext,
    scope_filter: Option<TurnScope>,
    executor_tasks: &mut JoinSet<TurnRunId>,
    active_runs: &mut HashMap<TurnRunId, RelinquishIdentity>,
    shutdown_token: &CancellationToken,
) -> bool {
    loop {
        // Check for cancellation before starting a new claim.  We do this at
        // the top of the loop (not inside the claim await) so that any claim
        // already in progress always completes and is tracked in active_runs
        // before we exit.  This prevents a "claimed in store but not tracked"
        // leak where the shutdown drain would never relinquish the run.
        if shutdown_token.is_cancelled() {
            return false;
        }

        let permits = acquire_claim_permits(&context.semaphore);
        if permits.is_empty() {
            return false;
        };
        let claim_started_at = live_latency_started_at();
        let scope_filter_fields = latency::scope_fields(claim_started_at, scope_filter.as_ref());
        let claim = context
            .process_transitions
            .claim_next_processes(ClaimProcessesRequest {
                worker_id: process_worker_id_from_turn_runner_id(context.runner_id),
                scope_filter: scope_filter.clone().map(|scope| scope.to_resource_scope()),
                process_id_filter: None,
                process_kind_filter: Some(ProcessKind::AgentTurn),
                max_processes: permits.len(),
            })
            .await;
        match claim {
            Ok(process_claims) if process_claims.is_empty() => return false,
            Ok(process_claims) => {
                let mut claimed_runs = Vec::with_capacity(process_claims.len());
                for process_claim in process_claims {
                    let relinquish_request = ProcessLeaseRequest {
                        process_id: process_claim.state.process_id,
                        worker_id: process_claim.worker_id.clone(),
                        lease_token: process_claim.lease_token.clone(),
                    };
                    match claimed_turn_run_from_process_claim(process_claim) {
                        Ok(claimed) => claimed_runs.push(claimed),
                        Err(error) => {
                            debug!(
                                error = %error,
                                process_id = %relinquish_request.process_id,
                                "turn run scheduler could not convert process claim to agent-turn claim"
                            );
                            if let Err(relinquish_error) = context
                                .process_transitions
                                .relinquish_process(relinquish_request)
                                .await
                            {
                                debug!(
                                    error = %relinquish_error,
                                    "turn run scheduler failed to relinquish invalid process claim"
                                );
                            }
                            return true;
                        }
                    }
                }
                latency::claim_next_runs_result(
                    scope_filter_fields.as_ref(),
                    claim_started_at,
                    &claimed_runs,
                );
                for (claimed, permit) in claimed_runs.into_iter().zip(permits) {
                    let run_id = claimed.state.run_id;
                    active_runs.insert(
                        run_id,
                        RelinquishIdentity {
                            run_id,
                            worker_id: process_worker_id_from_turn_runner_id(claimed.runner_id),
                            lease_token: process_lease_token_from_turn(claimed.lease_token),
                        },
                    );
                    let task_config = ExecutorTaskConfig {
                        process_transitions: Arc::clone(&context.process_transitions),
                        runner_heartbeat_interval: context.config.runner_heartbeat_interval(),
                        max_consecutive_heartbeat_failures: context
                            .config
                            .max_consecutive_heartbeat_failures(),
                        terminal_failure_record_attempts: context
                            .config
                            .terminal_failure_record_attempts(),
                        terminal_failure_record_backoff: context
                            .config
                            .terminal_failure_record_backoff(),
                    };
                    spawn_executor_task(
                        claimed,
                        Arc::clone(&context.executor),
                        context.command_tx.clone(),
                        permit,
                        task_config,
                        executor_tasks,
                    );
                }
            }
            Err(error) => {
                latency::claim_next_runs_error(scope_filter_fields.as_ref(), claim_started_at);
                debug!(error = %error, "turn run scheduler claim failed");
                return true;
            }
        }
    }
}

fn acquire_claim_permits(semaphore: &Arc<Semaphore>) -> Vec<OwnedSemaphorePermit> {
    let mut permits = Vec::new();
    for _ in 0..MAX_CLAIMS_PER_DRAIN_BATCH {
        let Ok(permit) = Arc::clone(semaphore).try_acquire_owned() else {
            break;
        };
        permits.push(permit);
    }
    permits
}

struct ExecutorTaskConfig {
    process_transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>,
    runner_heartbeat_interval: Duration,
    max_consecutive_heartbeat_failures: usize,
    terminal_failure_record_attempts: usize,
    terminal_failure_record_backoff: Duration,
}

fn spawn_executor_task(
    claimed: ClaimedTurnRun,
    executor: Arc<dyn TurnRunExecutor>,
    command_tx: mpsc::Sender<SchedulerCommand>,
    permit: tokio::sync::OwnedSemaphorePermit,
    task_config: ExecutorTaskConfig,
    executor_tasks: &mut JoinSet<TurnRunId>,
) {
    // Tag every tracing event emitted while this run executes with its
    // `thread_id` + `run_id` so the operator Logs panel's scoped (thread/run)
    // view is populated. `OperatorLogLayer` reads these correlation fields from
    // the enclosing span via `from_root`; without the span, scoped queries
    // match nothing and the panel shows "0 entries".
    let run_span = tracing::info_span!(
        "turn_run",
        thread_id = %claimed.state.scope.thread_id,
        run_id = %claimed.state.run_id,
    );
    // Capture these before `claimed` is moved into the async block so the
    // "turn run started" event can emit them as explicit fields. This makes
    // the event self-contained and allows test layers to find them without
    // relying on span registration timing (which can be racy under parallel
    // test execution when using `tracing::dispatcher::set_default`).
    let recovery_scope = claimed.state.scope.clone();
    let recovery_run_id_for_start = claimed.state.run_id;
    executor_tasks.spawn(
        async move {
            let recovery_run_id = claimed.state.run_id;
            let recovery_runner_id = claimed.runner_id;
            let recovery_lease_token = claimed.lease_token;
            let recovery_process_id = process_id_from_turn_run_id(recovery_run_id);
            let recovery_worker_id = process_worker_id_from_turn_runner_id(recovery_runner_id);
            let recovery_process_lease_token = process_lease_token_from_turn(recovery_lease_token);
            let recovery_process_claim = ProcessClaimIdentity {
                run_id: recovery_run_id,
                process_id: recovery_process_id,
                worker_id: recovery_worker_id.clone(),
                lease_token: recovery_process_lease_token.clone(),
            };
            tracing::debug!(
                thread_id = %recovery_scope.thread_id,
                run_id = %recovery_run_id_for_start,
                "turn run started",
            );
            let executor_started_at = live_latency_started_at();
            let mut heartbeat_tick = interval(task_config.runner_heartbeat_interval);
            heartbeat_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // Consume the immediate first tick so the heartbeat loop never fires
            // at t=0. The run's lease was just issued and valid; a t=0 heartbeat
            // would fail on CancelRequested status (heartbeat only accepts Running)
            // and prematurely terminate the executor task before the driver has a
            // chance to observe cancellation and write its reply to thread history.
            heartbeat_tick.tick().await;
            let executor_result = AssertUnwindSafe(executor.execute_claimed_run(
                claimed,
                Arc::clone(&task_config.process_transitions),
            ))
            .catch_unwind();
            tokio::pin!(executor_result);
            let mut heartbeats = InFlightHeartbeat::new();
            let mut consecutive_heartbeat_failures = 0usize;
            let outcome = loop {
                tokio::select! {
                    biased;
                    result = &mut executor_result => {
                        break executor_task::result_to_outcome(
                            &recovery_scope,
                            recovery_run_id,
                            executor_started_at,
                            result,
                        );
                    }
                    _ = heartbeat_tick.tick(), if heartbeats.is_idle() => {
                        heartbeats.spawn(
                            Arc::clone(&task_config.process_transitions),
                            recovery_process_id,
                            recovery_worker_id.clone(),
                            recovery_process_lease_token.clone(),
                            task_config.runner_heartbeat_interval,
                        );
                    }
                    Some(heartbeat) = heartbeats.join(), if heartbeats.is_running() => {
                        match heartbeat_outcome(heartbeat) {
                            HeartbeatOutcome::Succeeded => {
                                consecutive_heartbeat_failures = 0;
                            }
                            HeartbeatOutcome::Failed => {
                                consecutive_heartbeat_failures =
                                    consecutive_heartbeat_failures.saturating_add(1);
                                debug!(
                                    run_id = %recovery_run_id,
                                    consecutive_heartbeat_failures,
                                    max_consecutive_heartbeat_failures = task_config.max_consecutive_heartbeat_failures,
                                    "turn run scheduler heartbeat failed"
                                );
                            }
                            HeartbeatOutcome::TimedOut => {
                                debug!(
                                    run_id = %recovery_run_id,
                                    consecutive_heartbeat_failures,
                                    max_consecutive_heartbeat_failures = task_config.max_consecutive_heartbeat_failures,
                                    "turn run scheduler heartbeat timed out; preserving executor while store is slow"
                                );
                            }
                        }
                        if consecutive_heartbeat_failures
                            >= task_config.max_consecutive_heartbeat_failures
                        {
                            break ExecutorTaskOutcome::TerminalFailure(scheduler_failure(
                                "scheduler_heartbeat_failed",
                            ));
                        }
                    }
                }
            };
            heartbeats.abort_all();

            match outcome {
                ExecutorTaskOutcome::Completed => {}
                ExecutorTaskOutcome::TerminalFailure(Some(failure)) => {
                    if let Err(error) = record_terminal_failure(
                        Arc::clone(&task_config.process_transitions),
                        &recovery_process_claim,
                        failure,
                        task_config.terminal_failure_record_attempts,
                        task_config.terminal_failure_record_backoff,
                    )
                    .await
                    {
                        debug!(
                            error = %error,
                            run_id = %recovery_run_id,
                            "turn run scheduler terminal failure recording exhausted; relinquishing claimed run"
                        );
                        if let Err(relinquish_error) = task_config
                            .process_transitions
                            .relinquish_process(ProcessLeaseRequest {
                                process_id: recovery_process_claim.process_id,
                                worker_id: recovery_process_claim.worker_id.clone(),
                                lease_token: recovery_process_claim.lease_token.clone(),
                            })
                            .await
                        {
                            debug!(
                                error = %relinquish_error,
                                run_id = %recovery_run_id,
                                "turn run scheduler failed to relinquish run after terminal failure recording error"
                            );
                        }
                    }
                }
                ExecutorTaskOutcome::TerminalFailure(None) => {
                    debug!("turn run scheduler could not sanitize terminal failure category");
                }
            }

            tracing::debug!("turn run finished");
            drop(permit);
            if let Err(error) = command_tx.send(SchedulerCommand::Drain).await {
                tracing::debug!(?error, "post-run drain command send failed; scheduler channel likely closed");
            }
            // Return the run_id so the scheduler loop can remove it from active_runs.
            recovery_run_id
        }
        .instrument(run_span),
    );
}

struct InFlightHeartbeat {
    task: Option<JoinHandle<HeartbeatOutcome>>,
}

impl InFlightHeartbeat {
    fn new() -> Self {
        Self { task: None }
    }

    fn is_idle(&self) -> bool {
        self.task.is_none()
    }

    fn is_running(&self) -> bool {
        self.task.is_some()
    }

    fn spawn(
        &mut self,
        transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>,
        process_id: ProcessId,
        worker_id: ProcessWorkerId,
        lease_token: ProcessLeaseToken,
        timeout_after: Duration,
    ) {
        debug_assert!(self.task.is_none());
        // Heartbeat transitions may wait on the same store lock currently held by
        // the executor. Run them in child tasks so the executor future keeps polling.
        self.task = Some(tokio::spawn(async move {
            heartbeat_claimed_process(
                transitions,
                process_id,
                worker_id,
                lease_token,
                timeout_after,
            )
            .await
        }));
    }

    async fn join(&mut self) -> Option<Result<HeartbeatOutcome, tokio::task::JoinError>> {
        let task = self.task.as_mut()?;
        let result = task.await;
        self.task = None;
        Some(result)
    }

    fn abort_all(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeartbeatOutcome {
    Succeeded,
    Failed,
    TimedOut,
}

fn heartbeat_outcome(result: Result<HeartbeatOutcome, tokio::task::JoinError>) -> HeartbeatOutcome {
    match result {
        Ok(outcome) => outcome,
        Err(error) => {
            debug!(
                error = %error,
                "turn run scheduler heartbeat task join failed"
            );
            HeartbeatOutcome::Failed
        }
    }
}

async fn heartbeat_claimed_process(
    transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>,
    process_id: ProcessId,
    worker_id: ProcessWorkerId,
    lease_token: ProcessLeaseToken,
    timeout_after: Duration,
) -> HeartbeatOutcome {
    let heartbeat = transitions.heartbeat_process(ProcessLeaseRequest {
        process_id,
        worker_id,
        lease_token,
    });
    let result = tokio::time::timeout(timeout_after, heartbeat).await;
    match result {
        Ok(Ok(_)) => HeartbeatOutcome::Succeeded,
        Ok(Err(error)) => {
            debug!(error = %error, "turn run scheduler heartbeat failed");
            HeartbeatOutcome::Failed
        }
        Err(_) => {
            debug!(
                process_id = %process_id,
                timeout_after = ?timeout_after,
                "turn run scheduler heartbeat timed out"
            );
            HeartbeatOutcome::TimedOut
        }
    }
}

async fn record_terminal_failure(
    process_transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>,
    identity: &ProcessClaimIdentity,
    failure: SanitizedFailure,
    max_attempts: usize,
    retry_backoff: Duration,
) -> Result<(), TurnError> {
    for attempt in 1..=max_attempts {
        let result = process_transitions
            .fail_process(FailProcessRequest {
                process_id: identity.process_id,
                worker_id: identity.worker_id.clone(),
                lease_token: identity.lease_token.clone(),
                failure: failure.clone(),
                metadata: None,
            })
            .await;
        match result {
            Ok(_) => return Ok(()),
            Err(error) => {
                let retryable = matches!(error, TurnError::Unavailable { .. });
                debug!(
                    error = %error,
                    run_id = %identity.run_id,
                    process_id = %identity.process_id,
                    attempt,
                    max_attempts,
                    retryable,
                    "turn run scheduler terminal failure transition failed"
                );
                if !retryable || attempt == max_attempts {
                    return Err(error);
                }
            }
        }
        tokio::time::sleep(retry_backoff).await;
    }
    Ok(())
}

fn scheduler_failure(category: &'static str) -> Option<SanitizedFailure> {
    match SanitizedFailure::new(category) {
        Ok(failure) => Some(failure),
        Err(error) => {
            debug!(
                category,
                error, "turn run scheduler static terminal failure category failed validation"
            );
            None
        }
    }
}

fn process_id_from_turn_run_id(run_id: TurnRunId) -> ProcessId {
    ProcessId::from_uuid(run_id.as_uuid())
}

fn process_worker_id_from_turn_runner_id(runner_id: TurnRunnerId) -> ProcessWorkerId {
    ProcessWorkerId::from_trusted(runner_id.as_uuid().to_string())
}

fn process_lease_token_from_turn(lease_token: TurnLeaseToken) -> ProcessLeaseToken {
    ProcessLeaseToken::from_trusted(lease_token.as_uuid().to_string())
}

async fn recover_expired_leases(transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>) {
    let result: Result<_, TurnError> = transitions
        .recover_expired_process_leases(RecoverExpiredProcessLeasesRequest {
            now: Utc::now(),
            // Scheduler currently owns one global worker pool; if composition
            // introduces per-tenant schedulers, thread that scope filter here.
            scope_filter: None,
            process_kind_filter: Some(ProcessKind::AgentTurn),
        })
        .await;
    match result {
        Ok(response) => {
            for state in response.recovered {
                debug!(
                    thread_id = %state.scope.thread_id.as_ref().map(|id| id.as_str()).unwrap_or(""),
                    run_id = %state.process_id,
                    process_id = %state.process_id,
                    process_kind = ?state.process_kind,
                    status = ?state.status,
                    failure_category = %state
                        .failure
                        .as_ref()
                        .map(|failure| failure.category())
                        .unwrap_or("unknown"),
                    "turn run scheduler recovered expired lease"
                );
            }
        }
        Err(error) => {
            debug!(error = %error, "turn run scheduler lease recovery failed");
        }
    }
}

fn schedule_drain_after(command_tx: mpsc::Sender<SchedulerCommand>, delay: Duration) {
    // Best-effort timer: if shutdown closes the command channel first, send fails harmlessly.
    tokio::spawn(async move {
        sleep(delay).await;
        if let Err(error) = command_tx.send(SchedulerCommand::RetryDrain).await {
            tracing::debug!(
                ?error,
                "retry-drain command send failed; scheduler channel likely closed"
            );
        }
    });
}
