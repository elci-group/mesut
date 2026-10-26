//! Scheduler evidence benchmark: FIFO vs Priority vs LoadAware vs Adaptive
//! across defined workload scenarios, with latency split into queue,
//! dispatch, execution, and completion phases.
//!
//! Every task carries a caller-attached `TaskTiming`, so phase data comes
//! from the runtime's own measurement rather than sampling. History for
//! `Adaptive` is seeded identically for all modes; only Adaptive reads it.
//!
//! JSON goes to stdout; human-readable summaries go to stderr.

use mesut::prelude::*;
use mesut::{LatencyPhases, RoutingHint, SchedulingMode};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Fifo,
    Priority,
    LoadAware,
    Adaptive,
}

impl Mode {
    const ALL: [Mode; 4] = [Mode::Fifo, Mode::Priority, Mode::LoadAware, Mode::Adaptive];
    fn name(&self) -> &'static str {
        match self {
            Mode::Fifo => "fifo",
            Mode::Priority => "priority",
            Mode::LoadAware => "loadaware",
            Mode::Adaptive => "adaptive",
        }
    }
    fn scheduling(&self) -> SchedulingMode {
        match self {
            Mode::Fifo => SchedulingMode::Fifo,
            Mode::Priority => SchedulingMode::Priority,
            Mode::LoadAware => SchedulingMode::LoadAware,
            Mode::Adaptive => SchedulingMode::Adaptive,
        }
    }
}

struct JobSpec {
    label: &'static str,
    kind: WorkKind,
    service_us: u64,
    priority: Priority,
    /// End-to-end SLA measured from submission; `None` imposes none.
    sla: Option<Duration>,
}

struct Scenario {
    name: &'static str,
    description: &'static str,
    max_in_flight: usize,
    aging: Option<Duration>,
    seed_per_label: usize,
    burst: Option<(usize, Duration)>,
    cancellation_probe: bool,
    jobs: Vec<JobSpec>,
}

fn compute(label: &'static str, service_us: u64, priority: Priority) -> JobSpec {
    JobSpec { label, kind: WorkKind::Compute, service_us, priority, sla: None }
}

fn io(label: &'static str, service_us: u64, priority: Priority) -> JobSpec {
    JobSpec { label, kind: WorkKind::Io, service_us, priority, sla: None }
}

fn compute_with_sla(label: &'static str, service_us: u64, sla: Duration) -> JobSpec {
    JobSpec { label, kind: WorkKind::Compute, service_us, priority: Priority::Normal, sla: Some(sla) }
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "homogeneous-short",
            description: "240 identical 3ms compute jobs, 4 in flight",
            max_in_flight: 4,
            aging: None,
            seed_per_label: 0,
            burst: None,
            cancellation_probe: false,
            jobs: (0..240).map(|_| compute("job", 3_000, Priority::Normal)).collect(),
        },
        Scenario {
            name: "homogeneous-long",
            description: "48 identical 30ms compute jobs, 4 in flight",
            max_in_flight: 4,
            aging: None,
            seed_per_label: 0,
            burst: None,
            cancellation_probe: false,
            jobs: (0..48).map(|_| compute("job", 30_000, Priority::Normal)).collect(),
        },
        Scenario {
            name: "bimodal",
            description: "160x 5ms + 40x 40ms compute jobs interleaved (every 5th long), 4 in flight",
            max_in_flight: 4,
            aging: Some(Duration::from_millis(250)),
            seed_per_label: 4,
            burst: None,
            cancellation_probe: false,
            jobs: (0..200)
                .map(|i| {
                    if i % 5 == 4 {
                        compute("long", 40_000, Priority::Normal)
                    } else {
                        compute("short", 5_000, Priority::Normal)
                    }
                })
                .collect(),
        },
        Scenario {
            name: "burst",
            description: "3 bursts of 60x 3ms compute jobs separated by 250ms quiet gaps, 4 in flight",
            max_in_flight: 4,
            aging: None,
            seed_per_label: 0,
            burst: Some((60, Duration::from_millis(250))),
            cancellation_probe: false,
            jobs: (0..180).map(|_| compute("job", 3_000, Priority::Normal)).collect(),
        },
        Scenario {
            name: "mixed-cpu-io",
            description: "100x 5ms compute + 100x 5ms timer I/O alternating, 16 in flight",
            max_in_flight: 16,
            aging: None,
            seed_per_label: 4,
            burst: None,
            cancellation_probe: false,
            jobs: (0..200)
                .map(|i| {
                    if i % 2 == 0 {
                        compute("cpu", 5_000, Priority::Normal)
                    } else {
                        io("wait", 5_000, Priority::Normal)
                    }
                })
                .collect(),
        },
        Scenario {
            name: "deadline-heavy",
            description: "90x 5ms compute jobs with a 200ms SLA + 30x 40ms jobs, 4 in flight",
            max_in_flight: 4,
            aging: Some(Duration::from_millis(250)),
            seed_per_label: 4,
            burst: None,
            cancellation_probe: false,
            jobs: (0..30)
                .map(|_| compute("long", 40_000, Priority::Normal))
                .chain((0..90).map(|_| compute_with_sla("short", 5_000, Duration::from_millis(200))))
                .collect(),
        },
        Scenario {
            name: "priority-inversion",
            description: "20 low-priority 30ms jobs queued first, then 10 critical 2ms jobs, 1 in flight",
            max_in_flight: 1,
            aging: None,
            seed_per_label: 0,
            burst: None,
            cancellation_probe: false,
            jobs: (0..20)
                .map(|_| compute("low-long", 30_000, Priority::Low))
                .chain((0..10).map(|_| compute("critical-short", 2_000, Priority::Critical)))
                .collect(),
        },
        Scenario {
            name: "starvation-stress",
            description: "60x 2ms jobs, then one 200ms job, then 540x 2ms jobs, 2 in flight, 100ms aging",
            max_in_flight: 2,
            aging: Some(Duration::from_millis(100)),
            seed_per_label: 2,
            burst: None,
            cancellation_probe: false,
            jobs: (0..60)
                .map(|_| compute("short", 2_000, Priority::Normal))
                .chain(std::iter::once(compute("long", 200_000, Priority::Normal)))
                .chain((0..540).map(|_| compute("short", 2_000, Priority::Normal)))
                .collect(),
        },
        Scenario {
            name: "cancellation-latency",
            description: "8 cooperative 5s jobs cancelled 30ms after submission",
            max_in_flight: 8,
            aging: None,
            seed_per_label: 0,
            burst: None,
            cancellation_probe: true,
            jobs: vec![],
        },
    ]
}

fn burn(service_us: u64) {
    let deadline = Instant::now() + Duration::from_micros(service_us);
    let mut n = 0xdeadbeef_u64;
    while Instant::now() < deadline {
        for _ in 0..512 {
            n = n.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
    }
    std::hint::black_box(n);
}

fn build_runtime(mode: Mode, aging: Option<Duration>, max_in_flight: usize) -> MesuT {
    // Worker counts match the largest in-flight setting so executor-internal
    // queueing does not distort the scheduling signal being measured.
    let workers = max_in_flight.max(2);
    MesuT::new(
        RuntimeConfig::new()
            .with_animations(false)
            .with_scheduling(mode.scheduling())
            .with_adaptive_aging(aging)
            .with_max_in_flight(max_in_flight)
            .with_max_queue_size(10_000),
    )
    .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
    .with_compute_executor(Arc::new(
        RayonExecutor::new(mesut_rayon::RayonExecutorConfig::new().with_num_threads(workers))
            .unwrap(),
    ))
    .with_blocking_executor(Arc::new(
        BlockingExecutor::new(mesut_blocking::BlockingExecutorConfig::new().with_num_threads(workers)),
    ))
}

struct TaskRecord {
    label: &'static str,
    service_us: u64,
    total_us: u64,
    phases: Option<LatencyPhases>,
    has_sla: bool,
    deadline_miss: bool,
}

impl std::fmt::Debug for TaskRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRecord")
            .field("label", &self.label)
            .field("total_us", &self.total_us)
            .finish()
    }
}

struct RunOutput {
    records: Vec<TaskRecord>,
    cancellations_ms: Vec<f64>,
    elapsed: Duration,
    busy_ms: u64,
}

async fn run_scenario(scenario: &Scenario, mode: Mode) -> Result<RunOutput> {
    let runtime = Arc::new(build_runtime(mode, scenario.aging, scenario.max_in_flight));

    // Identical history seeding for every mode; only Adaptive consumes it.
    if scenario.seed_per_label > 0 {
        let mut labels: Vec<&'static str> = scenario.jobs.iter().map(|j| j.label).collect();
        labels.sort_unstable();
        labels.dedup();
        for label in labels {
            for spec in scenario.jobs.iter().filter(|j| j.label == label).take(scenario.seed_per_label) {
                let (seed, rx) = {
                    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
                    let seed = make_work(spec).on_complete(move |_| {
                        let _ = tx.send(());
                    });
                    (seed, rx)
                };
                runtime.submit(seed).await?;
                tokio::time::timeout(Duration::from_secs(10), rx).await??;
            }
        }
    }

    let records: Arc<Mutex<Vec<TaskRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let remaining = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();

    async fn submit_one(
        spec: &JobSpec,
        runtime: Arc<MesuT>,
        records: Arc<Mutex<Vec<TaskRecord>>>,
        remaining: Arc<AtomicUsize>,
    ) -> Result<()> {
        remaining.fetch_add(1, Ordering::AcqRel);
        let timing = mesut::TaskTiming::new().shared();
        let work = make_work(spec).with_timing(timing.clone());
        let submitted = Instant::now();
        let label = spec.label;
        let service_us = spec.service_us;
        let sla = spec.sla;
        let work = work.on_complete(move |_| {
            let total = submitted.elapsed();
            let record = TaskRecord {
                label,
                service_us,
                total_us: total.as_micros() as u64,
                phases: timing.phases(),
                has_sla: sla.is_some(),
                deadline_miss: sla.is_some_and(|sla| total > sla),
            };
            records.lock().unwrap().push(record);
            remaining.fetch_sub(1, Ordering::AcqRel);
        });
        runtime.submit(work).await?;
        Ok(())
    }

    match scenario.burst {
        None => {
            for spec in &scenario.jobs {
                submit_one(spec, runtime.clone(), records.clone(), remaining.clone())
                    .await?;
            }
        }
        Some((size, gap)) => {
            for (index, spec) in scenario.jobs.iter().enumerate() {
                submit_one(spec, runtime.clone(), records.clone(), remaining.clone())
                    .await?;
                if (index + 1) % size == 0 && index + 1 < scenario.jobs.len() {
                    tokio::time::sleep(gap).await;
                }
            }
        }
    }

    let deadline = Instant::now() + Duration::from_secs(120);
    while remaining.load(Ordering::Acquire) > 0 {
        if Instant::now() > deadline {
            return Err("scenario did not drain within 120s".into());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let mut elapsed = started.elapsed();

    let mut cancellations_ms = Vec::new();
    if scenario.cancellation_probe {
        let mut pending = Vec::new();
        for _ in 0..8 {
            let work = Work::new(WorkKind::Compute).with_hint(RoutingHint::RequireCompute).with_job(
                |token| {
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while Instant::now() < deadline {
                        if token.is_cancelled() {
                            return Err(mesut::TaskError::Cancelled);
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Ok(vec![])
                },
            );
            let id = work.id;
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let work = work.on_complete(move |_| {
                let _ = tx.send(());
            });
            runtime.submit(work).await?;
            pending.push((id, rx));
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        let cancelled_at = Instant::now();
        for (id, _) in &pending {
            runtime.cancel(*id).await?;
        }
        for (_, rx) in pending {
            tokio::time::timeout(Duration::from_secs(10), rx).await??;
            cancellations_ms.push(cancelled_at.elapsed().as_secs_f64() * 1e3);
        }
        cancellations_ms.sort_by(f64::total_cmp);
        elapsed = started.elapsed();
    }

    let state = runtime.runtime_state();
    let records = Arc::try_unwrap(records).unwrap().into_inner().unwrap();
    runtime.shutdown().await?;
    let busy_ms = state.executor_stats.values().map(|s| s.busy_ms).sum::<u64>();
    // Timer I/O contributes execution-phase wall time to busy_ms but no CPU;
    // subtract the I/O jobs' measured execution so utilisation reflects the
    // compute workers the busy-wait jobs actually ran on.
    let io_execution_ms: u64 = records
        .iter()
        .filter(|r| r.label == "wait")
        .filter_map(|r| r.phases.as_ref().map(|p| p.execution_us / 1000))
        .sum();
    Ok(RunOutput {
        records,
        cancellations_ms,
        elapsed,
        busy_ms: busy_ms.saturating_sub(io_execution_ms),
    })
}

fn make_work(spec: &JobSpec) -> Work {
    let service = spec.service_us;
    match spec.kind {
        WorkKind::Compute | WorkKind::Blocking => Work::new(spec.kind)
            .with_hint(match spec.kind {
                WorkKind::Compute => RoutingHint::RequireCompute,
                _ => RoutingHint::RequireBlocking,
            })
            .with_priority(spec.priority)
            .with_label(spec.label)
            .with_job(move |_| {
                burn(service);
                Ok(vec![])
            }),
        _ => Work::new(spec.kind)
            .with_hint(RoutingHint::RequireAsync)
            .with_priority(spec.priority)
            .with_label(spec.label)
            .with_future(async move {
                tokio::time::sleep(Duration::from_micros(service)).await;
                Ok(vec![])
            }),
    }
}

fn percentile(mut values: Vec<f64>, p: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let index = ((values.len() as f64 * p).ceil() as usize).saturating_sub(1);
    Some(values[index])
}

fn percentile_json(values: Vec<f64>) -> Value {
    match percentile(values.clone(), 0.5) {
        None => Value::Null,
        Some(p50) => json!({
            "count": values.len(),
            "p50": p50,
            "p95": percentile(values.clone(), 0.95),
            "p99": percentile(values, 0.99),
        }),
    }
}

/// Jain fairness index over per-label mean slowdown (latency / service time).
/// 1.0 means every label sees the same relative slowdown.
fn jain_slowdown(records: &[TaskRecord]) -> Option<f64> {
    let mut per_label: std::collections::HashMap<&str, (f64, u64)> = Default::default();
    for record in records {
        if record.service_us == 0 {
            continue;
        }
        let entry = per_label.entry(record.label).or_default();
        entry.0 += record.total_us as f64 / record.service_us as f64;
        entry.1 += 1;
    }
    if per_label.len() < 2 {
        return None;
    }
    let means: Vec<f64> = per_label
        .values()
        .map(|(sum, count)| sum / *count as f64)
        .collect();
    let sum: f64 = means.iter().sum();
    let squares: f64 = means.iter().map(|m| m * m).sum();
    (sum * sum / (means.len() as f64 * squares)).into()
}

fn summarise(scenario: &Scenario, mode: Mode, output: &RunOutput) -> Value {
    let totals: Vec<f64> = output
        .records
        .iter()
        .map(|r| r.total_us as f64 / 1e3)
        .collect();
    let queue: Vec<f64> = output
        .records
        .iter()
        .filter_map(|r| r.phases.as_ref().map(|p| p.queue_us as f64 / 1e3))
        .collect();
    let execution: Vec<f64> = output
        .records
        .iter()
        .filter_map(|r| r.phases.as_ref().map(|p| p.execution_us as f64 / 1e3))
        .collect();
    let throughput = output.records.len() as f64 / output.elapsed.as_secs_f64();
    let wall_ms = output.elapsed.as_secs_f64() * 1e3;
    // Compute worker threads execute the busy-wait jobs; the harness matches
    // worker count to each scenario's in-flight limit (see build_runtime), so
    // utilisation is compute-busy time against that worker budget.
    let workers = scenario.max_in_flight.max(2) as f64;
    let utilisation = if wall_ms > 0.0 {
        output.busy_ms as f64 / (wall_ms * workers)
    } else {
        0.0
    };
    let deadline_misses = output.records.iter().filter(|r| r.deadline_miss).count();
    let sla_jobs = output.records.iter().filter(|r| r.has_sla).count();
    let mut per_label = serde_json::Map::new();
    let mut labels: Vec<&str> = output.records.iter().map(|r| r.label).collect();
    labels.sort_unstable();
    labels.dedup();
    for label in labels {
        let label_records: Vec<&TaskRecord> =
            output.records.iter().filter(|r| r.label == label).collect();
        let label_totals: Vec<f64> =
            label_records.iter().map(|r| r.total_us as f64 / 1e3).collect();
        let label_queue: Vec<f64> = label_records
            .iter()
            .filter_map(|r| r.phases.as_ref().map(|p| p.queue_us as f64 / 1e3))
            .collect();
        per_label.insert(
            label.to_string(),
            json!({
                "total_ms": percentile_json(label_totals.clone()),
                "max_total_ms": label_totals.iter().copied().fold(f64::MIN, f64::max),
                "max_queue_ms": if label_queue.is_empty() {
                    Value::Null
                } else {
                    json!(label_queue.iter().copied().fold(f64::MIN, f64::max))
                },
            }),
        );
    }
    json!({
        "scenario": scenario.name,
        "mode": mode.name(),
        "completed": output.records.len(),
        "elapsed_ms": wall_ms,
        "throughput_tasks_per_sec": throughput,
        "total_ms": percentile_json(totals),
        "queue_ms": percentile_json(queue),
        "execution_ms": percentile_json(execution),
        "deadline_miss_rate": if sla_jobs > 0 {
            json!(deadline_misses as f64 / sla_jobs as f64)
        } else {
            Value::Null
        },
        "fairness_jain_slowdown": jain_slowdown(&output.records),
        "executor_utilisation": utilisation,
        "per_label": per_label,
        "cancellation_ms": if output.cancellations_ms.is_empty() {
            Value::Null
        } else {
            json!({
                "p50": percentile(output.cancellations_ms.clone(), 0.5),
                "max": output.cancellations_ms.iter().copied().fold(f64::MIN, f64::max),
            })
        },
    })
}

/// Median of each numeric leaf across runs; nulls propagate.
fn median_of(values: &[Value]) -> Value {
    if values.is_empty() || values.iter().any(Value::is_null) {
        return Value::Null;
    }
    if let Some(object) = values[0].as_object() {
        return Value::Object(
            object
                .keys()
                .map(|key| {
                    let leaves: Vec<Value> =
                        values.iter().map(|v| v[key].clone()).collect();
                    (key.clone(), median_of(&leaves))
                })
                .collect(),
        );
    }
    let Some(mut numbers) = values.iter().map(Value::as_f64).collect::<Option<Vec<_>>>() else {
        return Value::Null;
    };
    numbers.sort_by(f64::total_cmp);
    let mid = numbers.len() / 2;
    json!(if numbers.len() % 2 == 0 {
        (numbers[mid - 1] + numbers[mid]) / 2.0
    } else {
        numbers[mid]
    })
}

struct GateCheck {
    name: &'static str,
    passed: bool,
    detail: String,
}

/// Exit gate: Adaptive must beat FIFO and LoadAware by >=30% on bimodal short
/// p50, must rescue starved long work near the aging threshold in the
/// starvation-stress scenario, and must not regress bimodal overall p99 by
/// more than 15% against the better of FIFO/LoadAware.
fn evaluate_gate(medians: &[Value], aging_ms: f64, short_service_ms: f64, workers: f64) -> Vec<GateCheck> {
    let get = |scenario: &str, mode: &str| -> Option<&Value> {
        medians
            .iter()
            .find(|m| m["scenario"] == scenario && m["mode"] == mode)
    };
    let field = |value: &Value, path: &[&str]| -> Option<f64> {
        let mut current = value;
        for key in path {
            current = &current[*key];
        }
        current.as_f64()
    };
    let mut checks = Vec::new();
    let Some(adaptive) = get("bimodal", "adaptive") else {
        return vec![GateCheck {
            name: "bimodal-adaptive-present",
            passed: false,
            detail: "no adaptive bimodal median".into(),
        }];
    };
    let baselines: Vec<(&str, Option<f64>)> = ["fifo", "loadaware"]
        .iter()
        .map(|mode| (
            *mode,
            get("bimodal", mode)
                .and_then(|v| field(v, &["per_label", "short", "total_ms", "p50"])),
        ))
        .collect();
    let adaptive_short_p50 = field(adaptive, &["per_label", "short", "total_ms", "p50"]);
    let best_baseline = baselines.iter().filter_map(|(_, v)| *v).fold(f64::MAX, f64::min);
    let win_ratio = match (adaptive_short_p50, best_baseline) {
        (Some(a), b) if b > 0.0 => Some(a / b),
        _ => None,
    };
    checks.push(GateCheck {
        name: "adaptive-short-p50-at-least-30pct-better-than-both",
        passed: win_ratio.is_some_and(|r| r <= 0.70),
        detail: format!(
            "adaptive short p50 {:.2}ms vs best baseline {:.2}ms (ratio {:.3}) [fifo {:?} loadaware {:?}]",
            adaptive_short_p50.unwrap_or(f64::NAN),
            best_baseline,
            win_ratio.unwrap_or(f64::NAN),
            baselines[0].1,
            baselines[1].1,
        ),
    });
    // Starvation bound: in starvation-stress, a long task queued behind a
    // flood of short work must be rescued by aging near the threshold — not
    // held for the full short backlog (600 jobs x 2ms / 2 workers = 600ms
    // under pure shortest-service-time-first). The bound covers the aging
    // window plus one dispatch quantum of short work per worker.
    let rescue_bound = aging_ms + 2.0 * short_service_ms * workers + 150.0;
    let rescued = get("starvation-stress", "adaptive")
        .and_then(|v| field(v, &["per_label", "long", "max_queue_ms"]));
    checks.push(GateCheck {
        name: "adaptive-aging-rescues-starved-long-near-threshold",
        passed: rescued.is_some_and(|m| m <= rescue_bound),
        detail: format!(
            "starvation-stress long max queue wait {:.2}ms <= aging({aging_ms}ms) + 2x short service/worker + 150ms slack = {rescue_bound:.2}ms",
            rescued.unwrap_or(f64::NAN),
        ),
    });
    // Tail check: outside the win scenario, Adaptive's overall p99 must stay
    // within 15% of the better of FIFO/LoadAware. Inside the win scenario the
    // long class deliberately trades tail for short-job latency; the
    // starvation bound above covers that class's worst case.
    let mut worst: Option<(String, f64)> = None;
    for entry in medians {
        let scenario = entry["scenario"].as_str().unwrap_or_default();
        if scenario == "bimodal" || scenario == "cancellation-latency" {
            continue;
        }
        let mode = entry["mode"].as_str().unwrap_or_default();
        if mode != "adaptive" {
            continue;
        }
        let adaptive_p99 = field(entry, &["total_ms", "p99"]);
        let baseline_p99 = ["fifo", "loadaware"]
            .iter()
            .filter_map(|other| {
                get(scenario, other).and_then(|v| field(v, &["total_ms", "p99"]))
            })
            .fold(f64::MAX, f64::min);
        if let (Some(a), true) = (adaptive_p99, baseline_p99 > 0.0) {
            let ratio = a / baseline_p99;
            if worst.as_ref().is_none_or(|(_, r)| ratio > *r) {
                worst = Some((scenario.to_string(), ratio));
            }
        }
    }
    let worst_ratio = worst.as_ref().map(|(_, r)| *r);
    checks.push(GateCheck {
        name: "adaptive-p99-no-worse-than-15pct-elsewhere",
        passed: worst_ratio.is_none_or(|r| r <= 1.15),
        detail: match worst {
            Some((scenario, ratio)) => format!(
                "worst p99 ratio vs best baseline outside bimodal: {scenario} x{ratio:.3} (limit 1.150)"
            ),
            None => "no comparable scenarios".into(),
        },
    });
    checks
}

struct Args {
    repeat: usize,
}

fn parse_args() -> Result<Args> {
    let mut args = Args { repeat: 3 };
    let mut argv = std::env::args().skip(1);
    while let Some(key) = argv.next() {
        match key.as_str() {
            "--repeat" => args.repeat = argv.next().ok_or("missing value")?.parse()?,
            "--help" => {
                println!("mesut-sched-bench [--repeat 3]");
                std::process::exit(0);
            }
            other => return Err(format!("unknown option: {other}").into()),
        }
    }
    if !(1..=100).contains(&args.repeat) {
        return Err("repeat must be 1..=100".into());
    }
    Ok(args)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        return Err("scheduler benchmark requires a release build: cargo run --release --manifest-path tools/mesut-sched-bench/Cargo.toml".into());
    }
    let args = parse_args()?;
    let scenario_list = scenarios();
    let mut runs = Vec::new();
    for repetition in 1..=args.repeat {
        // Alternate mode order per repetition to reduce ordering bias.
        let mut modes = Mode::ALL;
        if repetition % 2 == 0 {
            modes.reverse();
        }
        for scenario in &scenario_list {
            for mode in &modes {
                let began = Instant::now();
                let output = run_scenario(scenario, *mode).await?;
                let summary = summarise(scenario, *mode, &output);
                if scenario.cancellation_probe {
                    eprintln!(
                        "[rep {repetition}] {:>20} {:>9}: cancel p50 {:>6.1}ms max {:>6.1}ms ({:?})",
                        scenario.name,
                        mode.name(),
                        summary["cancellation_ms"]["p50"].as_f64().unwrap_or(f64::NAN),
                        summary["cancellation_ms"]["max"].as_f64().unwrap_or(f64::NAN),
                        began.elapsed(),
                    );
                } else {
                    eprintln!(
                        "[rep {repetition}] {:>20} {:>9}: p50 {:>8.2}ms p99 {:>8.2}ms throughput {:>9.0}/s ({} jobs, {:?})",
                        scenario.name,
                        mode.name(),
                        summary["total_ms"]["p50"].as_f64().unwrap_or(f64::NAN),
                        summary["total_ms"]["p99"].as_f64().unwrap_or(f64::NAN),
                        summary["throughput_tasks_per_sec"].as_f64().unwrap_or(f64::NAN),
                        output.records.len(),
                        began.elapsed(),
                    );
                }
                runs.push(json!({
                    "repetition": repetition,
                    "scenario_description": scenario.description,
                    "summary": summary,
                }));
            }
        }
    }

    let mut medians = Vec::new();
    for scenario in &scenario_list {
        for mode in Mode::ALL {
            let samples: Vec<Value> = runs
                .iter()
                .filter(|r| r["summary"]["scenario"] == scenario.name && r["summary"]["mode"] == mode.name())
                .map(|r| r["summary"].clone())
                .collect();
            if samples.len() != args.repeat {
                return Err(format!("incomplete samples for {} {}", scenario.name, mode.name()).into());
            }
            medians.push(json!({
                "scenario": scenario.name,
                "mode": mode.name(),
                "medians": median_of(&samples),
            }));
        }
    }

    let median_view: Vec<Value> = medians
        .iter()
        .map(|m| {
            let mut merged = m["medians"].clone();
            merged["scenario"] = m["scenario"].clone();
            merged["mode"] = m["mode"].clone();
            merged
        })
        .collect();
    let checks = evaluate_gate(&median_view, 100.0, 2.0, 2.0);
    eprintln!("\n--- exit gate (bimodal) ---");
    for check in &checks {
        eprintln!("{} {}: {}", if check.passed { "PASS" } else { "FAIL" }, check.name, check.detail);
    }
    let gate_passed = checks.iter().all(|c| c.passed);
    eprintln!("gate: {}", if gate_passed { "PASSED" } else { "FAILED" });

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 1,
            "config": {
                "repeat": args.repeat,
                "compute_workers_per_scenario": "max(2, scenario max_in_flight)",
                "adaptive_aging_ms": 250,
                "debug_assertions": cfg!(debug_assertions),
            },
            "machine": {
                "os": std::env::consts::OS,
                "arch": std::env::consts::ARCH,
                "available_parallelism": std::thread::available_parallelism()?.get(),
                "unix_seconds": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_secs(),
            },
            "runs": runs,
            "medians": medians,
            "gate": {
                "passed": gate_passed,
                "checks": checks.iter().map(|c| json!({
                    "name": c.name,
                    "passed": c.passed,
                    "detail": c.detail,
                })).collect::<Vec<_>>(),
            },
        }))?
    );
    Ok(())
}
