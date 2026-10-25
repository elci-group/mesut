//! Scheduler-overhead microbenchmarks (WS5): the cost of Mesut's own
//! orchestration against direct Tokio/Rayon execution of identical work,
//! policy-selection cost, per-queued-task memory, and the telemetry tax.
//!
//! Gate D: for ~1 ms tasks, Adaptive's scheduling overhead (process-CPU per
//! task minus the best direct path) must stay below 10% of task service
//! time — or the measured supported granularity is documented instead.

use crate::config::{RunConfig, SchedulerMode};
use mesut::prelude::*;
use mesut_benchkit::env as benv;
use mesut_benchkit::workload::CalibratedBurn;
use mesut_benchkit::Rng;
use mesut_scheduler::{SchedulingCandidate, SchedulingPolicy};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

struct OverheadArgs {
    reps: usize,
    workers: usize,
    out: Option<std::path::PathBuf>,
    quick: bool,
}

fn parse_args(argv: &[String]) -> Result<OverheadArgs, String> {
    let mut args = OverheadArgs { reps: 7, workers: 8, out: None, quick: false };
    let mut it = argv.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--reps" => args.reps = it.next().ok_or("--reps needs a value")?.parse().map_err(|_| "bad reps")?,
            "--workers" => args.workers = it.next().ok_or("--workers needs a value")?.parse().map_err(|_| "bad workers")?,
            "--out" => args.out = Some(std::path::PathBuf::from(it.next().ok_or("--out needs a value")?)),
            "--quick" => args.quick = true,
            other => return Err(format!("unknown flag: {other}")),
        }
    }
    if !(1..=100).contains(&args.reps) {
        return Err("--reps must be within 1..=100".into());
    }
    Ok(args)
}

/// (name, service, counts): service 0 = no-op body.
fn size_plan(quick: bool) -> Vec<(&'static str, Duration, Vec<usize>)> {
    let full = vec![
        ("noop", Duration::ZERO, vec![1_000, 10_000, 100_000, 1_000_000]),
        ("10us", Duration::from_micros(10), vec![1_000, 10_000, 100_000, 1_000_000]),
        ("100us", Duration::from_micros(100), vec![1_000, 10_000, 100_000]),
        ("1ms", Duration::from_millis(1), vec![1_000, 10_000, 100_000]),
        ("10ms", Duration::from_millis(10), vec![1_000, 10_000]),
    ];
    if quick {
        return vec![
            ("noop", Duration::ZERO, vec![10_000]),
            ("10us", Duration::from_micros(10), vec![10_000]),
            ("100us", Duration::from_micros(100), vec![10_000]),
            ("1ms", Duration::from_millis(1), vec![10_000]),
            ("10ms", Duration::from_millis(10), vec![1_000]),
        ];
    }
    full
}

fn body(
    service: Duration,
    burn: &'static CalibratedBurn,
) -> impl FnOnce(mesut::CancellationToken) -> mesut::TaskResult<Vec<u8>> {
    move |_| {
        burn.burn(service);
        Ok(vec![])
    }
}

/// Latch counting completed tasks; the waiter parks on the condvar.
struct Latch {
    remaining: Mutex<usize>,
    done: Condvar,
}

impl Latch {
    fn new(count: usize) -> Arc<Self> {
        Arc::new(Self { remaining: Mutex::new(count), done: Condvar::new() })
    }
    fn dec(&self) {
        let mut remaining = self.remaining.lock().unwrap();
        *remaining -= 1;
        if *remaining == 0 {
            self.done.notify_all();
        }
    }
    fn wait(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut remaining = self.remaining.lock().unwrap();
        while *remaining > 0 {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (guard, _timeout) = self
                .done
                .wait_timeout(remaining, deadline - now)
                .unwrap();
            remaining = guard;
        }
        true
    }
}

fn build_mesut(mode: SchedulerMode, workers: usize, telemetry: bool) -> Arc<MesuT> {
    Arc::new(
        MesuT::new(
            RuntimeConfig::new()
                .with_animations(false)
                .with_telemetry(telemetry)
                .with_scheduling(mode.scheduling())
                .with_max_in_flight(workers.max(2))
                .with_max_queue_size(1_000_000)
                .with_backpressure(mesut::BackpressurePolicy::Accept),
        )
        .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
        .with_compute_executor(Arc::new(
            RayonExecutor::new(
                mesut_rayon::RayonExecutorConfig::new().with_num_threads(workers),
            )
            .unwrap(),
        ))
        .with_blocking_executor(Arc::new(mesut_blocking::BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::new().with_num_threads(workers.max(2)),
        ))),
    )
}

/// One timed cell on one path. Returns wall seconds and process-CPU
/// nanoseconds consumed for the whole cell. The burn is cycle-calibrated so
/// every path performs the same CPU work; the latch is awaited inside each
/// arm so shutdown never cancels queued work mid-measurement.
async fn timed_cell(
    path: &str,
    mode: Option<SchedulerMode>,
    service: Duration,
    count: usize,
    workers: usize,
    telemetry: bool,
    burn: &'static CalibratedBurn,
) -> Result<(f64, u64), String> {
    let latch = Latch::new(count);
    let cpu_before = benv::process_cpu_ns().unwrap_or(0);
    let began = Instant::now();
    match path {
        "direct-rayon" => {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()
                .map_err(|e| e.to_string())?;
            pool.install(|| {
                for _ in 0..count {
                    let latch = latch.clone();
                    rayon::spawn(move || {
                        burn.burn(service);
                        latch.dec();
                    });
                }
            });
        }
        "direct-tokio" => {
            for _ in 0..count {
                let latch = latch.clone();
                tokio::task::spawn_blocking(move || {
                    burn.burn(service);
                    latch.dec();
                });
            }
        }
        "mesut" => {
            let mode = mode.expect("mesut path needs a mode");
            let runtime = build_mesut(mode, workers, telemetry);
            for index in 0..count {
                let latch = latch.clone();
                let work = Work::new(mesut_core::WorkKind::Compute)
                    .with_hint(mesut::RoutingHint::RequireCompute)
                    // Eight shared labels: realistic recurring-workload
                    // history without unbounded cardinality.
                    .with_label(format!("ov-{}", index % 8))
                    .with_job(body(service, burn))
                    .on_complete(move |_| latch.dec());
                runtime
                    .submit(work)
                    .await
                    .map_err(|e| format!("submit: {e}"))?;
            }
            if !latch.wait(Duration::from_secs(600)) {
                return Err("mesut cell did not drain in 600s".into());
            }
            runtime.shutdown().await.map_err(|e| e.to_string())?;
        }
        other => return Err(format!("unknown path {other}")),
    }
    if !latch.wait(Duration::from_secs(600)) {
        return Err("cell did not drain in 600s".into());
    }
    let wall = began.elapsed().as_secs_f64();
    let cpu_after = benv::process_cpu_ns().unwrap_or(0);
    Ok((wall, cpu_after.saturating_sub(cpu_before)))
}

/// Measure the wall-time cost of `submit()` itself: a blocker occupies the
/// only in-flight slot, so each submit is pure admission.
async fn submission_overhead(mode: SchedulerMode, count: usize, workers: usize) -> Result<Value, String> {
    let runtime = build_mesut(mode, workers, true);
    // Occupy every in-flight slot with cooperative blockers.
    let in_flight = workers.max(2);
    let mut blockers = Vec::new();
    for _ in 0..in_flight {
        let flag = mesut_benchkit::workload::cancel_flag();
        let blocker_flag = flag.clone();
        let work = Work::new(mesut_core::WorkKind::Compute)
            .with_hint(mesut::RoutingHint::RequireCompute)
            .with_job(move |_| {
                let _ = mesut_benchkit::workload::burn_cooperative(Duration::from_secs(30), &blocker_flag);
                Ok(vec![])
            });
        runtime.submit(work).await.map_err(|e| e.to_string())?;
        blockers.push(flag);
    }
    // Give the blockers time to start and fill the slots.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut durations = Vec::with_capacity(count);
    for index in 0..count {
        let work = Work::new(mesut_core::WorkKind::Compute)
            .with_hint(mesut::RoutingHint::RequireCompute)
            .with_label(format!("sub-{index}"))
            .with_job(move |_| Ok(vec![]));
        let began = Instant::now();
        runtime.submit(work).await.map_err(|e| e.to_string())?;
        durations.push(began.elapsed().as_secs_f64() * 1e6);
    }
    for flag in blockers {
        flag.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    runtime.shutdown().await.map_err(|e| e.to_string())?;
    let describe = mesut_benchkit::stats::describe(&durations)
        .map(|d| serde_json::to_value(d).unwrap())
        .unwrap_or(Value::Null);
    Ok(json!({ "mode": mode.name(), "count": count, "submit_us": describe }))
}

/// Policy-selection cost on synthetic candidate slices.
fn selection_cost(candidates_count: usize, iterations: usize) -> Value {
    let mut rng = Rng::new(0x5EED);
    let mut candidates = Vec::with_capacity(candidates_count);
    for index in 0..candidates_count {
        candidates.push(SchedulingCandidate {
            task_id: mesut_core::TaskId::new(),
            priority: mesut_core::Priority::Normal,
            sequence: index as u64,
            executor_active: (index % 4) as usize,
            expected_service_time: Some(Duration::from_micros(rng.uniform_range(100.0, 50_000.0) as u64)),
            waited: Duration::from_micros(rng.uniform_range(0.0, 500.0) as u64),
        });
    }
    let measure = |name: &str, policy: &dyn SchedulingPolicy| {
        let began = Instant::now();
        let mut picked = 0usize;
        for _ in 0..iterations {
            picked += policy.select(&candidates).is_some() as usize;
        }
        let per_select_ns = began.elapsed().as_nanos() as f64 / iterations as f64;
        json!({
            "policy": name,
            "candidates": candidates_count,
            "iterations": iterations,
            "picked": picked,
            "ns_per_select": per_select_ns,
        })
    };
    use mesut_scheduler::{
        AdaptiveSchedulingPolicy, DefaultSchedulingPolicy, LoadAwareSchedulingPolicy,
        PrioritySchedulingPolicy,
    };
    json!({
        "fifo": measure("fifo", &DefaultSchedulingPolicy),
        "priority": measure("priority", &PrioritySchedulingPolicy::new()),
        "loadaware": measure("loadaware", &LoadAwareSchedulingPolicy::new()),
        "adaptive_with_aging": measure(
            "adaptive",
            &AdaptiveSchedulingPolicy::new(Some(Duration::from_millis(250))),
        ),
        "adaptive_without_aging": measure(
            "adaptive-no-aging",
            &AdaptiveSchedulingPolicy::new(None),
        ),
    })
}

/// Memory per queued task: one blocker holds the only slot; M no-op tasks
/// sit in the runtime's tracked queue; RSS delta / M is the entry cost.
async fn memory_per_queued(mode: SchedulerMode, queued: usize) -> Result<Value, String> {
    let runtime = Arc::new(
        MesuT::new(
            RuntimeConfig::new()
                .with_animations(false)
                .with_telemetry(false)
                .with_scheduling(mode.scheduling())
                .with_max_in_flight(1)
                .with_max_queue_size(1_000_000)
                .with_backpressure(mesut::BackpressurePolicy::Accept),
        )
        .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
        .with_compute_executor(Arc::new(
            RayonExecutor::new(
                mesut_rayon::RayonExecutorConfig::new().with_num_threads(2),
            )
            .unwrap(),
        ))
        .with_blocking_executor(Arc::new(mesut_blocking::BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::new().with_num_threads(2),
        ))),
    );
    let blocker_flag = mesut_benchkit::workload::cancel_flag();
    let hold_flag = blocker_flag.clone();
    let work = Work::new(mesut_core::WorkKind::Compute)
        .with_hint(mesut::RoutingHint::RequireCompute)
        .with_label("blocker")
        .with_job(move |_| {
            let _ = mesut_benchkit::workload::burn_cooperative(Duration::from_secs(30), &hold_flag);
            Ok(vec![])
        });
    runtime.submit(work).await.map_err(|e| e.to_string())?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rss_before = benv::rss_bytes().ok_or("no rss")?;
    let remaining = Arc::new(AtomicUsize::new(queued));
    for index in 0..queued {
        let remaining = remaining.clone();
        let work = Work::new(mesut_core::WorkKind::Compute)
            .with_hint(mesut::RoutingHint::RequireCompute)
            .with_label(format!("mem-{index}"))
            .with_job(move |_| Ok(vec![]))
            .on_complete(move |_| {
                remaining.fetch_sub(1, Ordering::AcqRel);
            });
        runtime.submit(work).await.map_err(|e| e.to_string())?;
    }
    // Let the queue settle, then measure. The tracked depth is queued+1
    // in-flight; RSS delta over `queued` entries is the metric.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let state = runtime.runtime_state();
    let rss_after = benv::rss_bytes().ok_or("no rss")?;
    blocker_flag.store(true, std::sync::atomic::Ordering::Relaxed);
    runtime.shutdown().await.map_err(|e| e.to_string())?;
    Ok(json!({
        "mode": mode.name(),
        "queued": queued,
        "tracked_at_peak": state.queued_tasks + state.in_flight_tasks,
        "rss_delta_bytes": rss_after.saturating_sub(rss_before),
        "bytes_per_queued_task": (rss_after.saturating_sub(rss_before)) as f64 / queued as f64,
    }))
}

pub fn cmd_overhead(argv: &[String]) -> i32 {
    if cfg!(debug_assertions) {
        eprintln!("overhead evidence requires a release build");
        return 2;
    }
    let args = match parse_args(argv) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("tokio: {e}");
            return 2;
        }
    };
    let result = rt.block_on(run_overhead(&args));
    drop(rt);
    match result {
        Ok(report) => {
            let gate_d = report["gate_d"]["passed"].as_bool().unwrap_or(false);
            let path = args.out.clone().unwrap_or_else(|| {
                std::path::PathBuf::from("docs/evidence/overhead.json")
            });
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            std::fs::write(&path, serde_json::to_string_pretty(&report).unwrap()).ok();
            eprintln!("wrote {}", path.display());
            if gate_d { 0 } else { 1 }
        }
        Err(e) => {
            eprintln!("overhead failed: {e}");
            1
        }
    }
}

async fn run_overhead(args: &OverheadArgs) -> Result<Value, String> {
    let burn: &'static CalibratedBurn = Box::leak(Box::new(CalibratedBurn::calibrate()));
    let mut cells = Vec::new();
    for (size_name, service, counts) in size_plan(args.quick) {
        for count in counts {
            // Skip cells whose service cost exceeds ~40s of worker time.
            if count as u128 * service.as_nanos() / (args.workers as u128 * 1_000_000_000) > 40 {
                continue;
            }
            for rep in 1..=args.reps {
                for (path, mode) in [
                    ("direct-rayon", None),
                    ("direct-tokio", None),
                    ("mesut", Some(SchedulerMode::Fifo)),
                    ("mesut", Some(SchedulerMode::Adaptive)),
                ] {
                    let (wall, cpu_ns) =
                        timed_cell(path, mode, service, count, args.workers, true, burn).await?;
                    let telemetry = true;
                    cells.push(json!({
                        "size": size_name,
                        "service_us": service.as_micros(),
                        "count": count,
                        "path": path,
                        "mode": mode.map(|m| m.name()),
                        "rep": rep,
                        "telemetry": telemetry,
                        "wall_s": wall,
                        "cpu_ns": cpu_ns,
                        "wall_us_per_task": wall * 1e6 / count as f64,
                        "cpu_ns_per_task": cpu_ns as f64 / count as f64,
                        "throughput_per_sec": count as f64 / wall,
                    }));
                    eprintln!(
                        "[{rep}] {size_name} x{count} {path}{}: wall {:.3}s cpu/task {:.0}ns",
                        mode.map(|m| format!("/{}", m.name())).unwrap_or_default(),
                        wall,
                        cpu_ns as f64 / count as f64,
                    );
                }
            }
        }
    }

    // Telemetry tax: fifo, 100us x 10k, telemetry off.
    let mut telemetry_off = Vec::new();
    for rep in 1..=args.reps {
        let (wall, cpu_ns) = timed_cell(
            "mesut",
            Some(SchedulerMode::Fifo),
            Duration::from_micros(100),
            10_000,
            args.workers,
            false,
            burn,
        )
        .await?;
        telemetry_off.push(json!({
            "rep": rep, "wall_s": wall, "cpu_ns": cpu_ns,
            "cpu_ns_per_task": cpu_ns as f64 / 10_000.0,
        }));
    }

    // Submission overhead (pure admission under full in-flight).
    let mut submissions = Vec::new();
    for mode in [SchedulerMode::Fifo, SchedulerMode::Adaptive] {
        submissions.push(submission_overhead(mode, 5_000, args.workers).await?);
    }

    // Selection cost.
    let selection = json!({
        "1k": selection_cost(1_000, 20_000),
        "10k": selection_cost(10_000, 2_000),
        "100k": selection_cost(100_000, 200),
        "1m": selection_cost(1_000_000, 20),
    });

    // Memory per queued task.
    let memory = memory_per_queued(SchedulerMode::Adaptive, 50_000).await?;

    // Gate D: 1ms cells — adaptive CPU/task vs best direct CPU/task, medians.
    let median = |values: &mut Vec<f64>| -> f64 {
        values.sort_by(f64::total_cmp);
        values[values.len() / 2]
    };
    let cell_median = |path_filter: &str, mode_name: Option<&str>| -> Option<f64> {
        let mut values: Vec<f64> = cells
            .iter()
            .filter(|c| {
                c["size"] == "1ms"
                    && c["path"] == path_filter
                    && c["mode"].as_str() == mode_name
                    && c["count"].as_u64() == Some(10_000)
            })
            .map(|c| c["cpu_ns_per_task"].as_f64().unwrap_or(f64::NAN))
            .filter(|v| v.is_finite())
            .collect();
        (!values.is_empty()).then(|| median(&mut values))
    };
    let direct_best = [
        cell_median("direct-rayon", None),
        cell_median("direct-tokio", None),
    ]
    .into_iter()
    .flatten()
    .fold(f64::MAX, f64::min);
    let adaptive = cell_median("mesut", Some("adaptive"));
    let fifo = cell_median("mesut", Some("fifo"));
    let service_ns = 1_000_000.0;
    let overhead_fraction = match (adaptive, direct_best.is_finite().then_some(direct_best)) {
        (Some(a), Some(b)) if service_ns > 0.0 => Some((a - b) / service_ns),
        _ => None,
    };
    let gate_d_passed = overhead_fraction.is_some_and(|f| f < 0.10);

    // Supported granularity: smallest size where adaptive overhead < 10%.
    let mut granularity = None;
    for (size_name, service, counts) in size_plan(false) {
        let Some(count) = counts.iter().find(|c| **c == 10_000) else { continue };
        let mode_cpu = |mode_name: Option<&str>| {
            let mut values: Vec<f64> = cells
                .iter()
                .filter(|c| {
                    c["size"] == size_name
                        && c["path"] == "mesut"
                        && c["mode"].as_str() == mode_name
                        && c["count"].as_u64() == Some(*count as u64)
                })
                .map(|c| c["cpu_ns_per_task"].as_f64().unwrap_or(f64::NAN))
                .filter(|v| v.is_finite())
                .collect();
            (!values.is_empty()).then(|| median(&mut values))
        };
        let direct: Vec<f64> = ["direct-rayon", "direct-tokio"]
            .iter()
            .filter_map(|p| {
                let mut values: Vec<f64> = cells
                    .iter()
                    .filter(|c| {
                        c["size"] == size_name
                            && c["path"] == *p
                            && c["count"].as_u64() == Some(*count as u64)
                    })
                    .map(|c| c["cpu_ns_per_task"].as_f64().unwrap_or(f64::NAN))
                    .filter(|v| v.is_finite())
                    .collect();
                (!values.is_empty()).then(|| median(&mut values))
            })
            .collect();
        let Some(direct_best_size) = direct.iter().copied().fold(None::<f64>, |acc, v| {
            acc.map_or(Some(v), |m| Some(m.min(v)))
        }) else { continue };
        let Some(adaptive_cpu) = mode_cpu(Some("adaptive")) else { continue };
        let fraction = (adaptive_cpu - direct_best_size) / service.as_nanos() as f64;
        if fraction < 0.10 {
            granularity = Some((size_name, service.as_micros(), fraction));
            break;
        }
    }

    Ok(json!({
        "schema_version": 1,
        "kind": "mesut-overhead",
        "machine": mesut_benchkit::env::machine_json(),
        "config": {
            "reps": args.reps,
            "workers": args.workers,
            "mesut_max_in_flight": args.workers.max(2),
        },
        "cells": cells,
        "telemetry_off_fifo_100us_10k": telemetry_off,
        "submission_overhead": submissions,
        "selection_cost": selection,
        "memory_per_queued": memory,
        "gate_d": {
            "name": "adaptive-overhead-below-10pct-for-1ms-tasks",
            "passed": gate_d_passed,
            "direct_best_cpu_ns_per_task": direct_best,
            "adaptive_cpu_ns_per_task": adaptive,
            "fifo_cpu_ns_per_task": fifo,
            "overhead_fraction_of_1ms": overhead_fraction,
            "detail": match overhead_fraction {
                Some(f) => format!("adaptive overhead {f:.3} of 1ms service (budget 0.10)"),
                None => "no comparable 1ms cells".into(),
            },
        },
        "supported_granularity": granularity.map(|(name, us, f)| json!({
            "size": name,
            "service_us": us,
            "overhead_fraction": f,
        })).unwrap_or(Value::Null),
    }))
}

// Keep the unused-import surface honest for the config module.
#[allow(unused)]
fn _config_shape(_: &RunConfig) {}
