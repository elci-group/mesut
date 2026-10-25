//! The open-loop run engine: schedule arrivals on a clock independent of
//! completions, submit through the runtime, sample queue state while the
//! run is live, and summarise the measured window.
//!
//! Record-keeping has exactly one path per task: the completion callback
//! forwards the outcome over a oneshot channel, and the submit task writes
//! the TaskRecord after observing both submission and completion. Admission
//! rejections are recorded submission-side, shed victims (also
//! `QueueFull`-terminated but already admitted) are distinguished by
//! submission success.

use crate::config::RunConfig;
use mesut::prelude::*;
use mesut_benchkit::env as benv;
use mesut_benchkit::workload::{burn, burn_cooperative, cancel_flag};
use mesut_benchkit::Rng;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Completed,
    Failed,
    Cancelled,
    Rejected,
    Shed,
}

#[derive(Clone, Debug, Serialize)]
pub struct TaskRecord {
    pub arrival_index: u64,
    /// Arrival-schedule offset from run start, seconds.
    pub submitted_offset_s: f64,
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

#[derive(Clone, Debug)]
struct ArrivalMeta {
    arrival_index: u64,
    submitted_offset_s: f64,
    warmup: bool,
    label: String,
    priority: &'static str,
    target_service_us: u64,
    sla_ms: Option<f64>,
}

struct Shared {
    records: Mutex<Vec<TaskRecord>>,
    /// Measured-window tasks not yet finished.
    pending: AtomicUsize,
    submitted_measured: AtomicUsize,
    rejected: AtomicUsize,
    handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    monitor_log: Arc<Mutex<Vec<Sample>>>,
}

fn priority_name(p: mesut_core::Priority) -> &'static str {
    match p {
        mesut_core::Priority::Low => "low",
        mesut_core::Priority::Normal => "normal",
        mesut_core::Priority::High => "high",
        mesut_core::Priority::Critical => "critical",
    }
}

pub(crate) fn build_runtime(config: &RunConfig, telemetry: bool) -> Arc<MesuT> {
    Arc::new(
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
                mesut_rayon::RayonExecutorConfig::new()
                    .with_num_threads(config.worker_count),
            )
            .unwrap(),
        ))
        .with_blocking_executor(Arc::new(mesut_blocking::BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::new()
                .with_num_threads(config.worker_count.max(2)),
        ))),
    )
}

#[allow(clippy::too_many_arguments)]
fn spawn_task(
    runtime: Arc<MesuT>,
    shared: Arc<Shared>,
    meta: ArrivalMeta,
    kind: mesut_core::WorkKind,
    priority: mesut_core::Priority,
    service: Duration,
) {
    shared.pending.fetch_add(1, Ordering::AcqRel);
    shared.submitted_measured.fetch_add(1, Ordering::AcqRel);
    let timing = mesut::TaskTiming::new().shared();
    let work = match kind {
        mesut_core::WorkKind::Compute | mesut_core::WorkKind::Blocking => {
            let hint = if kind == mesut_core::WorkKind::Compute {
                mesut::RoutingHint::RequireCompute
            } else {
                mesut::RoutingHint::RequireBlocking
            };
            Work::new(kind).with_hint(hint).with_job(move |_| {
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
    let (tx, rx) = oneshot::channel::<mesut::TaskResult<Vec<u8>>>();
    let work = work
        .with_priority(priority)
        .with_label(meta.label.clone())
        .with_timing(timing.clone())
        .on_complete(move |result| {
            // Single send; the submit task owns the record.
            let _ = tx.send(if result.is_ok() {
                Ok(vec![])
            } else {
                Err(result.as_ref().err().cloned().unwrap_or(mesut::TaskError::Cancelled))
            });
        });

    let handle = tokio::spawn({
        let shared = Arc::clone(&shared);
        async move {
        let submitted_at = Instant::now();
        match runtime.submit(work).await {
            Err(error) => {
                // Rejected at admission; the runtime already finished the
                // work, which fired the (ignored) oneshot above.
                let outcome =
                    if matches!(error, mesut::TaskError::QueueFull) {
                        Outcome::Rejected
                    } else {
                        Outcome::Failed
                    };
                shared.rejected.fetch_add(1, Ordering::AcqRel);
                shared.records.lock().unwrap().push(TaskRecord {
                    arrival_index: meta.arrival_index,
                    submitted_offset_s: meta.submitted_offset_s,
                    warmup: meta.warmup,
                    label: meta.label,
                    priority: meta.priority,
                    target_service_us: meta.target_service_us,
                    total_us: None,
                    queue_us: None,
                    execution_us: None,
                    sla_ms: meta.sla_ms,
                    deadline_miss: false,
                    outcome,
                });
            }
            Ok(()) => {
                let result = rx.await.unwrap_or_else(|_| Err(mesut::TaskError::Cancelled));
                let total = submitted_at.elapsed();
                let phases = timing.phases();
                let outcome = match &result {
                    Ok(_) => Outcome::Completed,
                    Err(mesut::TaskError::Cancelled | mesut::TaskError::Shutdown) => {
                        Outcome::Cancelled
                    }
                    Err(mesut::TaskError::QueueFull) => Outcome::Shed,
                    Err(_) => Outcome::Failed,
                };
                let deadline_miss = meta
                    .sla_ms
                    .is_some_and(|sla| outcome == Outcome::Completed && total.as_secs_f64() * 1e3 > sla);
                shared.records.lock().unwrap().push(TaskRecord {
                    arrival_index: meta.arrival_index,
                    submitted_offset_s: meta.submitted_offset_s,
                    warmup: meta.warmup,
                    label: meta.label,
                    priority: meta.priority,
                    target_service_us: meta.target_service_us,
                    total_us: Some(total.as_micros() as u64),
                    queue_us: phases.as_ref().map(|p| p.queue_us),
                    execution_us: phases.as_ref().map(|p| p.execution_us),
                    sla_ms: meta.sla_ms,
                    deadline_miss,
                    outcome,
                });
            }
        }
        shared.pending.fetch_sub(1, Ordering::AcqRel);
        }
    });
    shared.handles.lock().unwrap().push(handle);
}

pub struct RunOutput {
    pub summary: Value,
    pub records: Vec<TaskRecord>,
    pub samples: Vec<Sample>,
}

type RunResult = std::result::Result<RunOutput, String>;

/// One open-loop run. Arrivals follow the configured process for `duration`
/// seconds (after `warmup_duration` of history-building arrivals), then the
/// queue drains within `cooldown_duration`. Statistics cover the measured
/// window; warmup tasks execute and feed Adaptive history but are excluded
/// from latency aggregates.
pub async fn run(config: &RunConfig) -> RunResult {
    run_with_runtime(config, build_runtime(config, true), Instant::now()).await
}

pub(crate) async fn run_with_runtime(
    config: &RunConfig,
    runtime: Arc<MesuT>,
    t0: Instant,
) -> RunResult {
    let shared = Arc::new(Shared {
        records: Mutex::new(Vec::new()),
        pending: AtomicUsize::new(0),
        submitted_measured: AtomicUsize::new(0),
        rejected: AtomicUsize::new(0),
        handles: Mutex::new(Vec::new()),
        monitor_log: Arc::new(Mutex::new(Vec::new())),
    });

    let warmup = Duration::from_secs_f64(config.warmup_duration);
    let _window = Duration::from_secs_f64(config.duration);
    let measured_start = t0 + warmup;

    // Monitor task: samples runtime and process state until aborted. Samples
    // go through Shared so aborting loses nothing.
    let monitor_runtime = runtime.clone();
    let monitor_log = shared.monitor_log.clone();
    let monitor_interval =
        Duration::from_secs_f64((config.monitor_interval_ms / 1e3).max(0.005));
    let monitor = tokio::spawn(async move {
        loop {
            let state = monitor_runtime.runtime_state();
            monitor_log.lock().unwrap().push(Sample {
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
        }
    });

    // Cancellation probe (optional): long cooperative tasks cancelled
    // mid-flight inside the measured window.
    let probe_runtime = runtime.clone();
    let probe_done = Arc::new(Mutex::new(Vec::<f64>::new()));
    let probe_delay = config
        .cancellation_probe
        .as_ref()
        .map(|p| warmup + Duration::from_secs_f64(p.after_s));
    let probe = probe_delay.map(|delay| {
        let probe_config = config.cancellation_probe.clone().unwrap();
        let probe_done = probe_done.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let mut handles = Vec::new();
            for _ in 0..probe_config.count {
                let flag = cancel_flag();
                let work = Work::new(mesut_core::WorkKind::Compute)
                    .with_hint(mesut::RoutingHint::RequireCompute)
                    .with_job(move |_| {
                        let _ = burn_cooperative(
                            Duration::from_secs_f64(probe_config.service_s),
                            &flag,
                        );
                        Ok(vec![])
                    });
                let id = work.id;
                let (tx, rx) = oneshot::channel::<()>();
                let work = work.on_complete(move |_| {
                    let _ = tx.send(());
                });
                let _ = probe_runtime.submit(work).await;
                handles.push((id, rx));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            let cancelled_at = Instant::now();
            for (id, _) in &handles {
                let _ = probe_runtime.cancel(*id).await;
            }
            for (_, rx) in handles {
                if tokio::time::timeout(Duration::from_secs(30), rx)
                    .await
                    .is_ok()
                {
                    probe_done
                        .lock()
                        .unwrap()
                        .push(cancelled_at.elapsed().as_secs_f64() * 1e3);
                }
            }
        })
    });

    // Arrival driver: wall-clock scheduled submissions.
    let mut rng = Rng::new(config.seed);
    let mut next_t = 0.0f64;
    let mut arrival_index: u64 = 0;
    loop {
        next_t += config
            .arrival
            .process
            .next_interval(&mut rng, Duration::from_secs_f64(next_t));
        if next_t > config.duration {
            break;
        }
        let due = t0 + Duration::from_secs_f64(next_t);
        if due > Instant::now() {
            tokio::time::sleep_until(tokio::time::Instant::from(due)).await;
        }
        let warmup_task = Instant::now() < measured_start;
        let offset = Instant::now().duration_since(t0);
        let (service, named_label) = config.service_distribution.sample(&mut rng, offset);
        let priority = config.priority_distribution.sample(&mut rng);
        let sla = config.deadline_distribution.sample(&mut rng);
        let kind = config.work_kind_distribution.sample(&mut rng);
        let label = mesut_benchkit::ServiceDist::label_for(service, named_label);
        arrival_index += 1;
        if warmup_task {
            // Warmup tasks run and feed Adaptive history but keep no record.
            let timing = mesut::TaskTiming::new().shared();
            let work = match kind {
                mesut_core::WorkKind::Compute | mesut_core::WorkKind::Blocking => {
                    let hint = if kind == mesut_core::WorkKind::Compute {
                        mesut::RoutingHint::RequireCompute
                    } else {
                        mesut::RoutingHint::RequireBlocking
                    };
                    Work::new(kind).with_hint(hint).with_job(move |_| {
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
            let work = work
                .with_priority(priority)
                .with_label(label)
                .with_timing(timing);
            let _ = runtime.submit(work).await;
        } else {
            spawn_task(
                runtime.clone(),
                shared.clone(),
                ArrivalMeta {
                    arrival_index,
                    submitted_offset_s: next_t,
                    warmup: false,
                    label,
                    priority: priority_name(priority),
                    target_service_us: service.as_micros() as u64,
                    sla_ms: sla.map(|d| d.as_secs_f64() * 1e3),
                },
                kind,
                priority,
                service,
            );
        }
    }
    let arrivals_ended = Instant::now();

    // Drain within the cooldown budget.
    let drain_deadline = Instant::now() + Duration::from_secs_f64(config.cooldown_duration);
    while shared.pending.load(Ordering::Acquire) > 0 {
        if Instant::now() > drain_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let undrained = shared.pending.load(Ordering::Acquire);
    let drain_end = Instant::now();

    if let Some(probe) = probe {
        let _ = probe.await;
    }
    monitor.abort();
    let state_at_end = runtime.runtime_state();
    let shutdown_result = runtime.shutdown().await;
    let handles: Vec<_> = shared.handles.lock().unwrap().drain(..).collect();
    for handle in handles {
        let _ = handle.await;
    }

    let records: Vec<TaskRecord> = std::mem::take(&mut *shared.records.lock().unwrap());
    let samples: Vec<Sample> = std::mem::take(&mut *shared.monitor_log.lock().unwrap());

    let summary = summarise(
        config,
        &records,
        undrained,
        shared.submitted_measured.load(Ordering::Acquire),
        shared.rejected.load(Ordering::Acquire),
        &state_at_end,
        drain_end.duration_since(measured_start),
        probe_done.lock().unwrap().clone(),
        arrivals_ended,
        t0,
        shutdown_result.is_ok(),
    );
    Ok(RunOutput { summary, records, samples })
}

fn percentile(mut values: Vec<f64>, p: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let index = ((values.len() as f64 * p).ceil() as usize).saturating_sub(1);
    Some(values[index.min(values.len() - 1)])
}

fn dist_json(values: Vec<f64>) -> Value {
    match percentile(values.clone(), 0.5) {
        None => Value::Null,
        Some(p50) => json!({
            "count": values.len(),
            "mean": values.iter().sum::<f64>() / values.len() as f64,
            "p50": p50,
            "p95": percentile(values.clone(), 0.95),
            "p99": percentile(values.clone(), 0.99),
            "p999": percentile(values.clone(), 0.999),
            "max": values.iter().copied().fold(f64::MIN, f64::max),
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn summarise(
    config: &RunConfig,
    records: &[TaskRecord],
    undrained: usize,
    submitted_measured: usize,
    rejected: usize,
    state_at_end: &mesut::RuntimeState,
    measured_wall: Duration,
    cancellation_ms: Vec<f64>,
    arrivals_ended: Instant,
    t0: Instant,
    shutdown_ok: bool,
) -> Value {
    let measured: Vec<&TaskRecord> = records.iter().filter(|r| !r.warmup).collect();
    let completed: Vec<&TaskRecord> =
        measured.iter().copied().filter(|r| r.outcome == Outcome::Completed).collect();
    let totals: Vec<f64> = completed
        .iter()
        .filter_map(|r| r.total_us.map(|us| us as f64 / 1e3))
        .collect();
    let queues: Vec<f64> = completed
        .iter()
        .filter_map(|r| r.queue_us.map(|us| us as f64 / 1e3))
        .collect();
    let executions: Vec<f64> = completed
        .iter()
        .filter_map(|r| r.execution_us.map(|us| us as f64 / 1e3))
        .collect();

    // Per-label latency and starvation detail.
    let mut per_label: BTreeMap<String, Vec<&TaskRecord>> = BTreeMap::new();
    for record in measured.iter().copied() {
        per_label.entry(record.label.clone()).or_default().push(record);
    }
    let mut labels_json = serde_json::Map::new();
    for (label, label_records) in &per_label {
        let label_completed: Vec<&TaskRecord> =
            label_records.iter().copied().filter(|r| r.outcome == Outcome::Completed).collect();
        let label_queues: Vec<f64> = label_completed
            .iter()
            .filter_map(|r| r.queue_us.map(|us| us as f64 / 1e3))
            .collect();
        let label_totals: Vec<f64> = label_completed
            .iter()
            .filter_map(|r| r.total_us.map(|us| us as f64 / 1e3))
            .collect();
        let starved = label_completed
            .iter()
            .filter(|r| r.queue_us.is_some_and(|q| q as f64 / 1e3 > config.starvation_threshold))
            .count();
        labels_json.insert(
            label.clone(),
            json!({
                "submitted": label_records.len(),
                "completed": label_completed.len(),
                "completion_rate": if label_records.is_empty() { Value::Null } else {
                    json!(label_completed.len() as f64 / label_records.len() as f64)
                },
                "total_ms": dist_json(label_totals),
                "queue_ms": dist_json(label_queues),
                "starved_over_threshold": starved,
            }),
        );
    }

    let outcome_count = |outcome: Outcome| {
        measured.iter().filter(|r| r.outcome == outcome).count()
    };
    let sla_jobs: Vec<&TaskRecord> =
        completed.iter().copied().filter(|r| r.sla_ms.is_some()).collect();
    let deadline_misses = sla_jobs.iter().filter(|r| r.deadline_miss).count();
    let measured_window_s = (config.duration - config.warmup_duration).max(1e-9);
    let throughput = completed.len() as f64 / measured_window_s;
    let arrival_rate = config.arrival.arrival_rate;
    let mean_execution_ms = if executions.is_empty() {
        0.0
    } else {
        executions.iter().sum::<f64>() / executions.len() as f64
    };
    // Nominal capacity: workers / mean service. λ/μ against that estimate.
    let capacity_per_sec = if mean_execution_ms > 0.0 {
        config.worker_count as f64 * 1e3 / mean_execution_ms
    } else {
        0.0
    };

    json!({
        "scenario": config.name,
        "mode": config.scheduler_mode.name(),
        "seed": config.seed,
        "arrival_process": config.arrival.process.describe(),
        "arrival_rate_nominal": arrival_rate,
        "lambda_over_mu": if capacity_per_sec > 0.0 { json!(arrival_rate / capacity_per_sec) } else { Value::Null },
        "duration_s": config.duration,
        "warmup_s": config.warmup_duration,
        "measured_window_s": measured_window_s,
        "cooldown_s": config.cooldown_duration,
        "aging_ms": config.adaptive_aging,
        "starvation_threshold_ms": config.starvation_threshold,
        "worker_count": config.worker_count,
        "max_in_flight": config.max_in_flight,
        "queue_limit": config.queue_limit,
        "backpressure_mode": config.backpressure_mode,
        "submitted_measured": submitted_measured,
        "submission_rejected": rejected,
        "completed": completed.len(),
        "outcomes": {
            "completed": outcome_count(Outcome::Completed),
            "failed": outcome_count(Outcome::Failed),
            "cancelled": outcome_count(Outcome::Cancelled),
            "rejected": outcome_count(Outcome::Rejected),
            "shed": outcome_count(Outcome::Shed),
        },
        "undrained_at_cooldown": undrained,
        "shutdown_ok": shutdown_ok,
        "wall_measured_s": measured_wall.as_secs_f64(),
        "arrivals_ended_offset_s": arrivals_ended.duration_since(t0).as_secs_f64(),
        "throughput_tasks_per_sec": throughput,
        "total_ms": dist_json(totals),
        "queue_ms": dist_json(queues),
        "execution_ms": dist_json(executions),
        "deadline": if sla_jobs.is_empty() { Value::Null } else { json!({
            "sla_jobs": sla_jobs.len(),
            "misses": deadline_misses,
            "miss_rate": deadline_misses as f64 / sla_jobs.len() as f64,
        })},
        "starvation_over_threshold": completed.iter()
            .filter(|r| r.queue_us.is_some_and(|q| q as f64 / 1e3 > config.starvation_threshold))
            .count(),
        "per_label": labels_json,
        "cancellation_ms": if cancellation_ms.is_empty() { Value::Null } else { json!({
            "count": cancellation_ms.len(),
            "p50": percentile(cancellation_ms.clone(), 0.5),
            "max": cancellation_ms.iter().copied().fold(f64::MIN, f64::max),
        })},
        "runtime_counters": {
            "submitted": state_at_end.total_submitted,
            "completed": state_at_end.total_completed,
            "failed": state_at_end.total_failed,
            "cancelled": state_at_end.total_cancelled,
            "executor_busy_ms": state_at_end.executor_stats.values()
                .map(|s| s.busy_ms).sum::<u64>(),
        },
    })
}
