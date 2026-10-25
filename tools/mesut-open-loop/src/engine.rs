//! The open-loop run engine: schedule arrivals on a clock independent of
//! completions, submit through the runtime, sample queue state while the
//! run is live, and summarise the measured window.

use crate::config::{RunConfig, SchedulerMode};
use mesut::prelude::*;
use mesut_benchkit::env as benv;
use mesut_benchkit::workload::{burn, burn_cooperative, cancel_flag};
use mesut_benchkit::{ServiceDist, Rng};
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Completed,
    Failed,
    Cancelled,
    Rejected,
    Shed,
    Undrained,
}

#[derive(Clone, Debug, Serialize)]
pub struct TaskRecord {
    pub arrival_index: u64,
    pub warmup: bool,
    pub label: String,
    pub priority: &'static str,
    pub target_service_us: u64,
    pub total_us: Option<u64>,
    pub queue_us: Option<u64>,
    pub execution_us: Option<u64>,
    pub sla_ms: Option<f64>,
    pub deadline_miss: bool,
    pub outcome: Outcome,
}

#[derive(Clone, Debug, Serialize)]
pub struct Sample {
    pub t: f64,
    pub queued: usize,
    pub in_flight: usize,
    pub rss_bytes: Option<u64>,
    pub cpu_ns: Option<u64>,
    pub threads: Option<usize>,
    pub fds: Option<usize>,
    pub completed: u64,
    pub failed: u64,
    pub cancelled: u64,
}

struct ArrivalTaskMeta {
    arrival_index: u64,
    warmup: bool,
    label: String,
    priority: &'static str,
    target_service_us: u64,
    sla_ms: Option<f64>,
}

struct Shared {
    records: Mutex<Vec<TaskRecord>>,
    pending: AtomicUsize,
    submitted: AtomicUsize,
    rejected: AtomicUsize,
    done: Mutex<Vec<(Instant, Instant)>>,
}

fn priority_name(p: mesut::Priority) -> &'static str {
    match p {
        mesut::Priority::Low => "low",
        mesut::Priority::Normal => "normal",
        mesut::Priority::High => "high",
        mesut::Priority::Critical => "critical",
    }
}

fn build_runtime(config: &RunConfig, telemetry: bool) -> Arc<MesuT> {
    MesuT::new(
        RuntimeConfig::new()
            .with_animations(false)
            .with_telemetry(telemetry)
            .with_scheduling(config.scheduler_mode.scheduling())
            .with_adaptive_aging(config.aging_duration())
            .with_adaptive_window(config.adaptive_window)
            .with_max_in_flight(config.max_in_flight)
            .with_max_queue_size(config.queue_limit)
            .with_backpressure(config.parse_backpressure()),
    )
    .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
    .with_compute_executor(Arc::new(
        RayonExecutor::new(
            mesut_rayon::RayonExecutorConfig::new().with_num_threads(config.worker_count),
        )
        .unwrap(),
    ))
    .with_blocking_executor(Arc::new(
        mesut_blocking::BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::new()
                .with_num_threads(config.worker_count.max(2)),
        ),
    ))
    .into()
}

fn make_work(
    config: &RunConfig,
    kind: mesut::WorkKind,
    priority: mesut::Priority,
    label: &str,
    service: Duration,
    timing: mesut::TaskTiming,
    shared: &Arc<Shared>,
    meta: ArrivalTaskMeta,
) -> Work {
    let shared = shared.clone();
    let work = match kind {
        mesut::WorkKind::Compute | mesut::WorkKind::Blocking => {
            let hint = if kind == mesut::WorkKind::Compute {
                mesut::RoutingHint::RequireCompute
            } else {
                mesut::RoutingHint::RequireBlocking
            };
            Work::new(kind)
                .with_hint(hint)
                .with_job(move |_| {
                    burn(service);
                    Ok(vec![])
                })
        }
        _ => Work::new(kind)
            .with_hint(mesut::RoutingHint::RequireAsync)
            .with_future(async move {
                tokio::time::sleep(service).await;
                Ok(vec![])
            }),
    };
    let timing_handle = timing.clone();
    work.with_priority(priority)
        .with_label(label.to_string())
        .with_timing(timing)
        .on_complete(move |_| {
            let phases = timing_handle.phases();
            let record = TaskRecord {
                arrival_index: meta.arrival_index,
                warmup: meta.warmup,
                label: meta.label,
                priority: meta.priority,
                target_service_us: meta.target_service_us,
                total_us: phases.as_ref().map(|_| {
                    // total is submit→complete; approximate from phases sum.
                    0
                }),
                queue_us: phases.as_ref().map(|p| p.queue_us),
                execution_us: phases.as_ref().map(|p| p.execution_us),
                sla_ms: meta.sla_ms,
                deadline_miss: false,
                outcome: Outcome::Completed,
            };
            let _ = record;
            let _ = shared;
        })
}

pub struct RunOutput {
    pub summary: Value,
    pub raw: Value,
}

/// One open-loop run. Wall-clock arrivals are generated on the configured
/// process; the run ends after `duration` of arrivals plus cooldown drain.
pub async fn run(config: &RunConfig) -> anyhow_free::Result<RunOutput> {
    run_inner(config, true).await
}

pub(crate) mod anyhow_free {
    pub type Result<T> = std::result::Result<T, String>;
}

pub(crate) async fn run_inner(config: &RunConfig, telemetry: bool) -> Result<RunOutput> {
    let runtime = build_runtime(config, telemetry);
    let shared = Arc::new(Shared {
        records: Mutex::new(Vec::new()),
        pending: AtomicUsize::new(0),
        submitted: AtomicUsize::new(0),
        rejected: AtomicUsize::new(0),
        done: Mutex::new(Vec::new()),
    });

    let t0 = Instant::now();
    let warmup = Duration::from_secs_f64(config.warmup_duration);
    let window = Duration::from_secs_f64(config.duration);
    let measured_start = t0 + warmup;
    let measured_end = measured_start + window;

    // Monitor task.
    let monitor_runtime = runtime.clone();
    let monitor_shared = shared.clone();
    let monitor_interval = Duration::from_secs_f64(config.monitor_interval.max(0.01));
    let monitor = tokio::spawn(async move {
        let mut samples = Vec::new();
        loop {
            let state = monitor_runtime.runtime_state();
            samples.push(Sample {
                t: t0.elapsed().as_secs_f64(),
                queued: state.queued_tasks,
                in_flight: state.in_flight_tasks,
                rss_bytes: benv::rss_bytes(),
                cpu_ns: benv::process_cpu_ns(),
                threads: benv::thread_count(),
                fds: benv::open_fds(),
                completed: state.total_completed,
                failed: state.total_failed,
                cancelled: state.total_cancelled,
            });
            tokio::time::sleep(monitor_interval).await;
            let _ = &monitor_shared;
        }
    });
    // The monitor never observes the run end via its own loop; we sample a
    // final state manually and abort the task below.

    // Cancellation probe task (optional).
    let probe_config = config.cancellation_probe.clone();
    let probe_runtime = runtime.clone();
    let probe_done = Arc::new(Mutex::new(Vec::<f64>::new()));
    let probe_done_clone = probe_done.clone();
    let probe_delay = warmup
        + probe_config
            .as_ref()
            .map(|p| Duration::from_secs_f64(p.after_s))
            .unwrap_or(Duration::MAX);
    let probe = tokio::spawn(async move {
        let Some(probe) = probe_config else { return };
        let mut handles = Vec::new();
        for _ in 0..probe.count {
            let flag = cancel_flag();
            let work = Work::new(mesut::WorkKind::Compute)
                .with_hint(mesut::RoutingHint::RequireCompute)
                .with_job(move |_| {
                    let _ = burn_cooperative(Duration::from_secs_f64(probe.service_s), &flag);
                    Ok(vec![])
                });
            let id = work.id;
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let work = work.on_complete(move |_| {
                let _ = tx.send(());
            });
            let _ = probe_runtime.submit(work).await;
            handles.push((id, rx, flag));
        }
        // Let the probe tasks start, then cancel and time the exit.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let cancelled_at = Instant::now();
        for (id, _, _) in &handles {
            let _ = probe_runtime.cancel(*id).await;
        }
        for (_, rx, _) in handles {
            if tokio::time::timeout(Duration::from_secs(30), rx).await.is_ok() {
                probe_done_clone.lock().unwrap().push(
                    cancelled_at.elapsed().as_secs_f64() * 1e3,
                );
            }
        }
    });

    // Arrival driver.
    let mut rng = Rng::new(config.seed);
    let mut next_t = 0.0f64;
    let mut arrival_index: u64 = 0;
    loop {
        next_t += config.arrival.process.next_interval(&mut rng, Duration::from_secs_f64(next_t));
        if next_t > config.duration {
            break;
        }
        let due = t0 + Duration::from_secs_f64(next_t);
        let now = Instant::now();
        if due > now {
            tokio::time::sleep_until(tokio::time::Instant::from(due)).await;
        }
        let warmup_task = due < measured_start;
        let offset = due.duration_since(t0);
        let (service, named_label) = config.service_distribution.sample(&mut rng, offset);
        let priority = config.priority_distribution.sample(&mut rng);
        let sla = config.deadline_distribution.sample(&mut rng);
        let kind = config.work_kind_distribution.sample(&mut rng);
        let label = ServiceDist::label_for(service, named_label);
        arrival_index += 1;

        let shared = shared.clone();
        shared.submitted.fetch_add(1, Ordering::AcqRel);
        if !warmup_task {
            shared.pending.fetch_add(1, Ordering::AcqRel);
        }
        let meta = ArrivalTaskMeta {
            arrival_index,
            warmup: warmup_task,
            label,
            priority: priority_name(priority),
            target_service_us: service.as_micros() as u64,
            sla_ms: sla.map(|d| d.as_secs_f64() * 1e3),
        };
        let timing = mesut::TaskTiming::new().shared();
        let submitted_at = Instant::now();
        let work = make_work(
            config,
            kind,
            priority,
            &meta.label.clone(),
            service,
            timing.clone(),
            &shared,
            meta,
        );
        let runtime = runtime.clone();
        let timing_for_record = timing;
        let sla_ms = meta_sla(&meta_ref(&work));
        let _ = tokio::spawn(async move {
            let record = match runtime.submit(work).await {
                Err(error) => {
                    let outcome = if matches!(error, mesut::TaskError::QueueFull) {
                        Outcome::Rejected
                    } else {
                        Outcome::Failed
                    };
                    shared.rejected.fetch_add(1, Ordering::AcqRel);
                    TaskRecord {
                        arrival_index: 0,
                        warmup: false,
                        label: String::new(),
                        priority: "",
                        target_service_us: 0,
                        total_us: None,
                        queue_us: None,
                        execution_us: None,
                        sla_ms: None,
                        deadline_miss: false,
                        outcome,
                    }
                }
                Ok(()) => {
                    shared.done.lock().unwrap().push((submitted_at, Instant::now()));
                    TaskRecord {
                        arrival_index: 0,
                        warmup: false,
                        label: String::new(),
                        priority: "",
                        target_service_us: 0,
                        total_us: None,
                        queue_us: None,
                        execution_us: None,
                        sla_ms: None,
                        deadline_miss: false,
                        outcome: Outcome::Completed,
                    }
                }
            };
            let _ = (record, timing_for_record, sla_ms);
        });
    }

    // Drain: wait for the measured window's tasks within cooldown.
    let cooldown = Duration::from_secs_f64(config.cooldown_duration);
    let drain_deadline = Instant::now() + cooldown;
    while shared.pending.load(Ordering::Acquire) > 0 {
        if Instant::now() > drain_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let undrained = shared.pending.load(Ordering::Acquire);

    let _ = probe.await;
    monitor.abort();
    let final_state = runtime.runtime_state();
    let shutdown = runtime.shutdown().await;

    // TODO(summarise): fold records into the summary.
    let _ = (final_state, shutdown);
    Err("engine summarise not yet implemented".into())
}

fn meta_sla(_: &()) -> Option<f64> {
    None
}
fn meta_ref(_: &Work) -> &() {
    &()
}
