use crate::RuntimeConfig;
use mesut_core::{CancellationToken, Priority, TaskError, TaskId, TaskResult, Work};
use mesut_executor::ArcExecutor;
use mesut_observe::{EventObserver, TaskEvent};
use mesut_scheduler::{
    AdaptiveSchedulingPolicy, BackpressurePolicy, DefaultSchedulingPolicy,
    LoadAwareSchedulingPolicy, PrioritySchedulingPolicy, SchedulingCandidate, SchedulingMode,
    SchedulingPolicy, WorkloadObserver,
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
}

impl Coordination {
    pub fn new(config: RuntimeConfig, observer: Arc<dyn EventObserver>) -> Arc<Self> {
        let workload = WorkloadObserver::new(config.adaptive_window);
        Arc::new(Self {
            state: Mutex::new(State::default()),
            observer: RwLock::new(observer),
            changed: Notify::new(),
            config,
            workload,
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
                Admission::Shed(Some(victim)) => (*victim).finish(Err(TaskError::QueueFull)),
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
        let work = work
            .on_start(move || {
                if let Some(shared) = start_weak.upgrade() {
                    shared.observe(TaskEvent::started(id, executor_id.to_string()));
                }
            })
            .on_complete(move |result| {
                if let Some(shared) = weak.upgrade() {
                    let elapsed = start.elapsed();
                    if result.is_ok() {
                        if let Some(label) = &label {
                            shared.workload.record(label, elapsed);
                        }
                    }
                    {
                        let mut state = shared.state.lock();
                        state.tasks.remove(&id);
                        match result {
                            Ok(_) => state.runtime_state.add_completion(),
                            Err(TaskError::Cancelled) => state.runtime_state.add_cancellation(),
                            Err(_) => state.runtime_state.add_failure(),
                        }
                        state.runtime_state.record_executor_completion(
                            executor_id.to_string(),
                            elapsed.as_millis() as u64,
                            result.is_err(),
                        );
                    }
                    let event = match result {
                        Ok(_) => TaskEvent::completed(id, elapsed.as_millis()),
                        Err(TaskError::Cancelled) => {
                            TaskEvent::cancelled(id, "Cancellation requested".into())
                        }
                        Err(error) => TaskEvent::failed(id, error.to_string()),
                    };
                    shared.observe(event);
                    shared.changed.notify_waiters();
                }
            });
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
        self.state.lock().runtime_state.add_failure();
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
                            expected_duration: e
                                .pending
                                .as_ref()
                                .and_then(|work| work.label.as_deref())
                                .and_then(|label| self.workload.profile(label))
                                .map(|profile| profile.mean_duration),
                        })
                        .collect();
                    match self.config.scheduling {
                        SchedulingMode::Fifo => DefaultSchedulingPolicy.select(&candidates),
                        SchedulingMode::Priority => PrioritySchedulingPolicy.select(&candidates),
                        SchedulingMode::LoadAware => LoadAwareSchedulingPolicy.select(&candidates),
                        SchedulingMode::Adaptive => AdaptiveSchedulingPolicy.select(&candidates),
                    }
                } else {
                    None
                };
                let next = id.and_then(|id| {
                    state.tasks.get_mut(&id).map(|entry| {
                        entry.dispatched = true;
                        (entry.pending.take(), entry.executor.clone())
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
            if let Some((Some(work), executor)) = next {
                let _ = executor.submit(work).await;
                // Do not monopolize the Tokio worker while draining a large queue.
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! { _ = &mut notified => (), _ = tokio::time::sleep(Duration::from_millis(1)) => () }
        }
    }
}

enum Admission {
    Accept,
    Wait,
    Reject(TaskError),
    Shed(Option<Box<Work>>),
}
