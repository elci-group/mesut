use crate::RuntimeConfig;
use mesut_core::{CancellationToken, Priority, TaskError, TaskId, TaskResult, TaskTiming, Work};
use mesut_executor::ArcExecutor;
use mesut_observe::{EventObserver, TaskEvent};
use mesut_scheduler::{
    AdaptiveSchedulingPolicy, BackpressurePolicy, DefaultSchedulingPolicy, LatencyHistogram,
    LoadAwareSchedulingPolicy, PrioritySchedulingPolicy, SchedulingCandidate, SchedulingMode,
    SchedulingPolicy, ShutdownState, WorkloadObserver,
};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

pub(crate) struct Entry {
    pub route: String,
    pub executor: ArcExecutor,
    pub token: CancellationToken,
    pub priority: Priority,
    pub sequence: u64,
    pub pending: Option<Work>,
    pub dispatched: bool,
    pub cancellation_sent: bool,
    pub deadline: Option<(Instant, Duration)>,
    pub admitted_at: Instant,
    pub timing: Arc<TaskTiming>,
}

#[derive(Default)]
pub(crate) struct State {
    pub tasks: HashMap<TaskId, Entry>,
    pub closed: bool,
    dispatching: bool,
    sequence: u64,
    pub runtime_state: mesut_scheduler::RuntimeState,
}

pub(crate) struct Coordination {
    pub state: Mutex<State>,
    pub observer: RwLock<Arc<dyn EventObserver>>,
    pub changed: Notify,
    pub config: RuntimeConfig,
    pub workload: WorkloadObserver,
    /// Runtime-level latency histograms (WS12 operator contract); bounded
    /// cardinality by construction.
    pub queue_latency: LatencyHistogram,
    pub execution_latency: LatencyHistogram,
}

impl Coordination {
    pub fn new(config: RuntimeConfig, observer: Arc<dyn EventObserver>) -> Arc<Self> {
        let workload =
            WorkloadObserver::with_limits(config.adaptive_window, config.adaptive_max_labels);
        Arc::new(Self {
            state: Mutex::new(State::default()),
            observer: RwLock::new(observer),
            changed: Notify::new(),
            config,
            workload,
            queue_latency: LatencyHistogram::default(),
            execution_latency: LatencyHistogram::default(),
        })
    }

    pub fn observe(&self, event: TaskEvent) {
        if self.config.enable_telemetry {
            let observer = self.observer.read().clone();
            // Observers are application code and cannot compromise task ownership.
            let _ =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer.observe(&event)));
        }
    }

    pub async fn admit(
        self: &Arc<Self>,
        work: Work,
        executor: ArcExecutor,
        route: String,
    ) -> TaskResult<()> {
        let id = work.id;
        let label = work.label.clone();
        let timeout = work.timeout.or(self.config.task_timeout);
        let start = Instant::now();
        // Adopt a caller-attached timing handle (measurement tools) or create
        // the one this task's phases are recorded into.
        let timing = work.timing().unwrap_or_else(|| TaskTiming::new().shared());
        timing.mark_submitted();
        let deadline = match timeout {
            Some(duration) => Some((
                start.checked_add(duration).ok_or_else(|| {
                    TaskError::InvalidConfiguration("deadline exceeds clock range".into())
                })?,
                duration,
            )),
            None => None,
        };
        if self.config.max_in_flight == 0
            || (self.config.max_queue_size == Some(0)
                && !matches!(self.config.saturation_policy, BackpressurePolicy::Accept))
        {
            let error = TaskError::InvalidConfiguration(
                "runtime admission and execution limits must be positive".into(),
            );
            self.reject(work, error.clone());
            return Err(error);
        }
        #[allow(unused_assignments)]
        let mut admitted_at: Option<Instant> = None;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(error) = work.cancellation.reason() {
                self.reject(work, error.clone());
                return Err(error);
            }
            if let Some((due, duration)) = deadline {
                if Instant::now() >= due {
                    let error = TaskError::Timeout(duration);
                    self.reject(work, error.clone());
                    return Err(error);
                }
            }
            let decision = {
                let mut state = self.state.lock();
                if state.closed {
                    Admission::Reject(TaskError::ExecutorUnavailable)
                } else if state.tasks.contains_key(&id) {
                    Admission::Reject(TaskError::InvalidConfiguration(
                        "duplicate active task ID".into(),
                    ))
                } else {
                    let limit = match self.config.saturation_policy {
                        BackpressurePolicy::Accept => usize::MAX,
                        BackpressurePolicy::Delay | BackpressurePolicy::Shed => self
                            .config
                            .max_queue_size
                            .unwrap_or(usize::MAX)
                            .min(self.config.backpressure_threshold.max(1)),
                        _ => self.config.max_queue_size.unwrap_or(usize::MAX),
                    };
                    let reject_busy =
                        matches!(self.config.saturation_policy, BackpressurePolicy::Reject)
                            && (state.tasks.len() >= self.config.max_in_flight
                                || !executor.is_available());
                    if state.tasks.len() < limit && !reject_busy {
                        let sequence = state.sequence;
                        state.sequence += 1;
                        admitted_at = Some(Instant::now());
                        state.tasks.insert(
                            id,
                            Entry {
                                route: route.clone(),
                                executor: executor.clone(),
                                token: work.cancellation.clone(),
                                priority: work.priority,
                                sequence,
                                pending: None,
                                dispatched: false,
                                cancellation_sent: false,
                                deadline,
                                admitted_at: admitted_at.unwrap_or_else(Instant::now),
                                timing: timing.clone(),
                            },
                        );
                        Admission::Accept
                    } else {
                        match self.config.saturation_policy {
                            BackpressurePolicy::Delay => Admission::Wait,
                            BackpressurePolicy::Shed => {
                                let victim = state
                                    .tasks
                                    .iter()
                                    .filter(|(_, e)| {
                                        e.pending.is_some() && e.priority < work.priority
                                    })
                                    .min_by_key(|(_, e)| {
                                        (e.priority, std::cmp::Reverse(e.sequence))
                                    })
                                    .map(|(id, _)| *id);
                                if let Some(id) = victim {
                                    Admission::Shed(
                                        state
                                            .tasks
                                            .get_mut(&id)
                                            .and_then(|e| e.pending.take())
                                            .map(Box::new),
                                    )
                                } else {
                                    Admission::Reject(TaskError::QueueFull)
                                }
                            }
                            _ => Admission::Reject(TaskError::QueueFull),
                        }
                    }
                }
            };
            match decision {
                Admission::Accept => break,
                Admission::Reject(error) => {
                    self.reject(work, error.clone());
                    return Err(error);
                }
                Admission::Shed(Some(victim)) => {
                    self.state.lock().runtime_state.add_shed();
                    (*victim).finish(Err(TaskError::QueueFull));
                }
                Admission::Shed(None) => (),
                Admission::Wait => {
                    tokio::select! { _ = &mut notified => (), _ = tokio::time::sleep(Duration::from_millis(1)) => () }
                }
            }
        }
        self.observe(TaskEvent::routed(id, route));
        let depth = self.state.lock().tasks.len();
        self.observe(TaskEvent::queued(id, depth));
        let weak = Arc::downgrade(self);
        let start_weak = weak.clone();
        let executor_id = executor.id();
        let callback_timing = timing.clone();
        let work = work
            .on_start(move || {
                if let Some(shared) = start_weak.upgrade() {
                    shared.observe(TaskEvent::started(id, executor_id.to_string()));
                }
            })
            .on_complete(move |result| {
                let timing = &callback_timing;
                if let Some(shared) = weak.upgrade() {
                    let elapsed = start.elapsed();
                    timing.mark_completed();
                    let phases = timing.phases();
                    let execution = timing.execution_time();
                    // The learned service-time estimate is the measured
                    // execution phase only — never the queue-inclusive total,
                    // which would grow with load and corrupt ordering.
                    if result.is_ok() {
                        if let (Some(label), Some(execution)) = (&label, execution) {
                            shared.workload.record(label, execution);
                        }
                    }
                    // Operator histograms: queue wait for every dispatched
                    // task; execution time for tasks that ran.
                    if let Some(phases) = &phases {
                        shared.queue_latency.record_us(phases.queue_us);
                    }
                    if let Some(execution) = execution {
                        shared
                            .execution_latency
                            .record_us(execution.as_micros().try_into().unwrap_or(u64::MAX));
                    }
                    {
                        let mut state = shared.state.lock();
                        state.tasks.remove(&id);
                        match result {
                            Ok(_) => state.runtime_state.add_completion(),
                            Err(TaskError::Cancelled | TaskError::Shutdown) => {
                                state.runtime_state.add_cancellation()
                            }
                            Err(_) => state.runtime_state.add_failure(),
                        }
                        state.runtime_state.record_executor_completion(
                            executor_id.to_string(),
                            elapsed.as_millis() as u64,
                            execution.map_or(0, |d| d.as_millis() as u64),
                            result.is_err(),
                        );
                    }
                    let event = match result {
                        Ok(_) => TaskEvent::completed(id, elapsed.as_millis(), phases),
                        Err(TaskError::Cancelled | TaskError::Shutdown) => {
                            TaskEvent::cancelled(id, "Cancellation requested".into())
                        }
                        Err(error) => TaskEvent::failed(id, error.to_string()),
                    };
                    shared.observe(event);
                    shared.changed.notify_waiters();
                }
            })
            .with_timing(timing.clone());
        {
            let mut state = self.state.lock();
            if let Some(entry) = state.tasks.get_mut(&id) {
                entry.pending = Some(work);
            }
        }
        self.kick();
        self.changed.notify_waiters();
        Ok(())
    }

    fn reject(&self, work: Work, error: TaskError) {
        self.state.lock().runtime_state.add_rejection();
        self.observe(TaskEvent::failed(work.id, error.to_string()));
        work.finish(Err(error));
    }

    fn kick(self: &Arc<Self>) {
        let mut state = self.state.lock();
        if state.dispatching {
            return;
        }
        state.dispatching = true;
        let shared = self.clone();
        tokio::spawn(async move {
            shared.dispatch().await;
        });
    }

    async fn dispatch(self: Arc<Self>) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (expired, cancellations, next) = {
                let mut state = self.state.lock();
                if state.tasks.is_empty() {
                    state.dispatching = false;
                    return;
                }
                let mut expired = Vec::new();
                let mut cancellations = Vec::new();
                for (id, entry) in &mut state.tasks {
                    if entry.pending.is_some() && entry.executor.is_shutdown() {
                        entry.token.cancel_with(TaskError::ExecutorUnavailable);
                    }
                    if let Some((due, duration)) = entry.deadline {
                        if Instant::now() >= due {
                            entry.token.cancel_with(TaskError::Timeout(duration));
                        }
                    }
                    if entry.token.is_cancelled() && !entry.cancellation_sent {
                        entry.cancellation_sent = true;
                        if let Some(work) = entry.pending.take() {
                            expired.push(work);
                        } else if entry.dispatched {
                            cancellations.push((*id, entry.executor.clone()));
                        }
                    }
                }
                let in_flight = state.tasks.values().filter(|e| e.dispatched).count();
                let now = Instant::now();
                let id = if in_flight < self.config.max_in_flight {
                    let candidates: Vec<_> = state
                        .tasks
                        .iter()
                        .filter(|(_, e)| {
                            e.pending.is_some()
                                && !e.token.is_cancelled()
                                && e.executor.is_available()
                        })
                        .map(|(id, e)| SchedulingCandidate {
                            task_id: *id,
                            priority: e.priority,
                            sequence: e.sequence,
                            executor_active: e.executor.active_tasks(),
                            expected_service_time: e
                                .pending
                                .as_ref()
                                .and_then(|work| work.label.as_deref())
                                .and_then(|label| self.workload.profile(label))
                                .map(|profile| profile.mean_service_time),
                            waited: now.duration_since(e.admitted_at),
                        })
                        .collect();
                    match self.config.scheduling {
                        SchedulingMode::Fifo => DefaultSchedulingPolicy.select(&candidates),
                        SchedulingMode::Priority => PrioritySchedulingPolicy.select(&candidates),
                        SchedulingMode::LoadAware => LoadAwareSchedulingPolicy.select(&candidates),
                        SchedulingMode::Adaptive => {
                            AdaptiveSchedulingPolicy::new(self.config.adaptive_aging)
                                .select(&candidates)
                        }
                    }
                } else {
                    None
                };
                let next = id.and_then(|id| {
                    state.tasks.get_mut(&id).map(|entry| {
                        entry.dispatched = true;
                        (
                            entry.pending.take(),
                            entry.executor.clone(),
                            entry.timing.clone(),
                        )
                    })
                });
                (expired, cancellations, next)
            };
            for work in expired {
                work.finish(Err(TaskError::Cancelled));
            }
            for (id, executor) in cancellations {
                let _ = executor.cancel(id).await;
            }
            if let Some((Some(work), executor, timing)) = next {
                timing.mark_dispatched();
                let _ = executor.submit(work).await;
                // Do not monopolize the Tokio worker while draining a large queue.
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! { _ = &mut notified => (), _ = tokio::time::sleep(Duration::from_millis(1)) => () }
        }
    }

    /// Force termination: the shutdown grace period expired with work still
    /// active. Queued work is cancelled, dispatchable handles are cancelled,
    /// and every tracked entry is dropped so no bookkeeping outlives the
    /// runtime's interest in it. Closures that never cooperate may still be
    /// running on executor threads; their late completions update counters
    /// but cannot resurrect tracked state.
    pub async fn force_terminate(self: &Arc<Self>) {
        let drained: Vec<(TaskId, Option<Work>, ArcExecutor, CancellationToken)> = {
            let mut state = self.state.lock();
            state.runtime_state.shutdown = ShutdownState::ForceTerminated;
            state
                .tasks
                .drain()
                .map(|(id, entry)| (id, entry.pending, entry.executor, entry.token.clone()))
                .collect()
        };
        self.changed.notify_waiters();
        for (id, pending, executor, token) in drained {
            if let Some(work) = pending {
                token.cancel_with(TaskError::Shutdown);
                work.finish(Err(TaskError::Shutdown));
            } else {
                let _ = executor.cancel(id).await;
            }
        }
    }
}

enum Admission {
    Accept,
    Wait,
    Reject(TaskError),
    Shed(Option<Box<Work>>),
}
