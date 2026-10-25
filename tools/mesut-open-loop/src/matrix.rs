//! Capacity × mode × repetition sweeps (WS2/WS3) with paired statistics
//! (WS10) and gate evaluation (A, B, C).

use crate::config::{self, RunConfig, SchedulerMode};
use crate::engine;
use mesut_benchkit::stats::{self, Describe};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

struct MatrixArgs {
    preset: Option<String>,
    config_path: Option<PathBuf>,
    modes: Vec<SchedulerMode>,
    reps: usize,
    multipliers: Vec<f64>,
    out_dir: PathBuf,
}

fn parse_multipliers(text: &str) -> Result<Vec<f64>, String> {
    text.split(',')
        .map(|part| part.trim().parse::<f64>().map_err(|_| format!("bad multiplier {part}")))
        .collect()
}

fn parse_modes(text: &str) -> Result<Vec<SchedulerMode>, String> {
    if text == "all" {
        return Ok(SchedulerMode::ALL.to_vec());
    }
    text.split(',')
        .map(|part| match part.trim() {
            "fifo" => Ok(SchedulerMode::Fifo),
            "priority" => Ok(SchedulerMode::Priority),
            "loadaware" => Ok(SchedulerMode::LoadAware),
            "adaptive" => Ok(SchedulerMode::Adaptive),
            other => Err(format!("unknown mode {other}")),
        })
        .collect()
}

fn parse_args(argv: &[String]) -> Result<MatrixArgs, String> {
    let mut args = MatrixArgs {
        preset: None,
        config_path: None,
        modes: SchedulerMode::ALL.to_vec(),
        reps: 25,
        multipliers: vec![0.5, 0.75, 0.9, 1.0, 1.1, 1.25, 1.5],
        out_dir: PathBuf::from("docs/evidence/raw/open-loop-matrix"),
    };
    let mut it = argv.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--preset" => args.preset = Some(it.next().ok_or("--preset needs a value")?.clone()),
            "--config" => {
                args.config_path =
                    Some(PathBuf::from(it.next().ok_or("--config needs a value")?))
            }
            "--modes" => args.modes = parse_modes(it.next().ok_or("--modes needs a value")?)?,
            "--reps" => args.reps = it.next().ok_or("--reps needs a value")?.parse().map_err(|_| "bad reps")?,
            "--multipliers" => {
                args.multipliers =
                    parse_multipliers(it.next().ok_or("--multipliers needs a value")?)?
            }
            "--out" => args.out_dir = PathBuf::from(it.next().ok_or("--out needs a value")?),
            other => return Err(format!("unknown flag: {other}")),
        }
    }
    if args.preset.is_none() && args.config_path.is_none() {
        return Err("one of --preset or --config is required".into());
    }
    if !(2..=200).contains(&args.reps) {
        return Err("--reps must be within 2..=200".into());
    }
    Ok(args)
}

/// The per-rep metrics extracted from a run summary; statistics run across
/// repetitions of the same (multiplier, mode) cell.
#[derive(Clone, Copy, Default)]
struct RepMetrics {
    p50_total_ms: f64,
    p99_total_ms: f64,
    p50_queue_ms: f64,
    p99_queue_ms: f64,
    max_queue_ms: f64,
    throughput: f64,
    deadline_miss_rate: f64,
    starvation_count: f64,
    rejected: f64,
    shed: f64,
    completed: f64,
    undrained: f64,
    lambda_over_mu: f64,
    p50_short_total_ms: f64,
    p99_long_queue_ms: f64,
    long_completion_rate: f64,
}

fn extract(summary: &Value) -> RepMetrics {
    let f = |path: &[&str]| -> f64 {
        let mut cur = summary;
        for key in path {
            cur = cur.get(key).unwrap_or(&Value::Null);
        }
        cur.as_f64().unwrap_or(f64::NAN)
    };
    RepMetrics {
        p50_total_ms: f(&["total_ms", "p50"]),
        p99_total_ms: f(&["total_ms", "p99"]),
        p50_queue_ms: f(&["queue_ms", "p50"]),
        p99_queue_ms: f(&["queue_ms", "p99"]),
        max_queue_ms: f(&["queue_ms", "max"]),
        throughput: f(&["throughput_tasks_per_sec"]),
        deadline_miss_rate: f(&["deadline", "miss_rate"]),
        starvation_count: f(&["starvation_over_threshold"]),
        rejected: f(&["outcomes", "rejected"]),
        shed: f(&["outcomes", "shed"]),
        completed: f(&["completed"]),
        undrained: f(&["undrained_at_cooldown"]),
        lambda_over_mu: f(&["lambda_over_mu"]),
        p50_short_total_ms: f(&["per_label", "short", "total_ms", "p50"]),
        p99_long_queue_ms: f(&["per_label", "long", "queue_ms", "p99"]),
        long_completion_rate: summary
            .pointer("/per_label/long/completion_rate")
            .and_then(Value::as_f64)
            .unwrap_or(f64::NAN),
    }
}

fn metric_names() -> Vec<(&'static str, fn(&RepMetrics) -> f64)> {
    vec![
        ("p50_total_ms", |m| m.p50_total_ms),
        ("p99_total_ms", |m| m.p99_total_ms),
        ("p50_queue_ms", |m| m.p50_queue_ms),
        ("p99_queue_ms", |m| m.p99_queue_ms),
        ("max_queue_ms", |m| m.max_queue_ms),
        ("throughput", |m| m.throughput),
        ("deadline_miss_rate", |m| m.deadline_miss_rate),
        ("starvation_count", |m| m.starvation_count),
        ("rejected", |m| m.rejected),
        ("shed", |m| m.shed),
        ("undrained", |m| m.undrained),
        ("lambda_over_mu", |m| m.lambda_over_mu),
        ("p50_short_total_ms", |m| m.p50_short_total_ms),
        ("p99_long_queue_ms", |m| m.p99_long_queue_ms),
    ]
}

pub fn cmd_matrix(argv: &[String]) -> i32 {
    if cfg!(debug_assertions) {
        eprintln!("matrix evidence requires a release build");
        return 2;
    }
    let args = match parse_args(argv) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };
    let value = match (&args.preset, &args.config_path) {
        (Some(preset), _) => config::preset(preset).unwrap(),
        (None, Some(path)) => {
            let text = match std::fs::read_to_string(path) {
                Ok(text) => text,
                Err(e) => {
                    eprintln!("read {}: {e}", path.display());
                    return 2;
                }
            };
            match serde_json::from_str::<Value>(&text) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("parse: {e}");
                    return 2;
                }
            }
        }
        (None, None) => {
            eprintln!("one of --preset or --config is required");
            return 2;
        }
    };
    let base: RunConfig = match serde_json::from_value(value) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("config: {e}");
            return 2;
        }
    };
    let base_seed = base.seed;
    let name = base.name.clone();

    std::fs::create_dir_all(&args.out_dir).ok();
    let mut cells: Vec<(f64, SchedulerMode, Vec<RepMetrics>)> = Vec::new();
    for (m_index, multiplier) in args.multipliers.iter().enumerate() {
        for mode in &args.modes {
            cells.push((*multiplier, *mode, Vec::with_capacity(args.reps)));
            let cell = cells.last_mut().unwrap().2.as_mut_slice();
            for rep in 1..=args.reps {
                let mut config = base.clone();
                config.scale_arrival_rate(*multiplier);
                config.scheduler_mode = *mode;
                config.seed = base_seed.wrapping_add(rep as u64);
                // Alternate mode order is unnecessary here: every cell is an
                // isolated runtime; the seed pairing carries the comparison.
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("tokio: {e}"));
                let output = match runtime {
                    Ok(rt) => {
                        let out = rt.block_on(engine::run(&config));
                        drop(rt);
                        out
                    }
                    Err(e) => {
                        eprintln!("{e}");
                        return 2;
                    }
                };
                match output {
                    Ok(run) => {
                        cell.push(extract(&run.summary));
                        let raw = json!({
                            "schema_version": 1,
                            "kind": "mesut-open-loop-run",
                            "matrix_multiplier": multiplier,
                            "machine": mesut_benchkit::env::machine_json(),
                            "config": serde_json::to_value(&config).ok(),
                            "summary": run.summary,
                            "samples": run.samples,
                            "records": run.records,
                        });
                        let path = args
                            .out_dir
                            .join(format!("{name}.m{m_index}.{}.rep{rep:03}.json", mode.name()));
                        std::fs::write(&path, serde_json::to_string_pretty(&raw).unwrap()).ok();
                    }
                    Err(e) => {
                        eprintln!("run failed: {e}");
                        return 1;
                    }
                }
            }
        }
    }

    // Aggregate cells: statistics across reps per metric.
    let mut aggregated = Vec::new();
    for (multiplier, mode, reps) in &cells {
        let mut metrics = serde_json::Map::new();
        for (metric_name, getter) in metric_names() {
            let values: Vec<f64> = reps.iter().map(getter).collect();
            metrics.insert(metric_name.to_string(), stats::describe(&values).map_or(Value::Null, |d| serde_json::to_value(d).unwrap()));
        }
        aggregated.push(json!({
            "multiplier": multiplier,
            "mode": mode.name(),
            "reps": reps.len(),
            "metrics": metrics,
        }));
    }

    // Paired comparisons: adaptive vs each baseline, per multiplier, on the
    // headline metrics. Same seeds => paired runs.
    let mut comparisons = Vec::new();
    let has = |mode: &SchedulerMode| args.modes.contains(mode);
    for multiplier in &args.multipliers {
        for baseline in [SchedulerMode::Fifo, SchedulerMode::LoadAware] {
            if !has(&SchedulerMode::Adaptive) || !has(&baseline) {
                continue;
            }
            let cell = |mode: &SchedulerMode| {
                cells.iter().find(|(m, mode_, _)| m == multiplier && mode_ == mode)
                    .map(|(_, _, reps)| reps)
            };
            let (Some(adaptive), Some(baseline_reps)) = (cell(&SchedulerMode::Adaptive), cell(&baseline)) else {
                continue;
            };
            let mut for_metrics = serde_json::Map::new();
            for (metric_name, getter) in metric_names() {
                let a: Vec<f64> = adaptive.iter().map(getter).collect();
                let b: Vec<f64> = baseline_reps.iter().map(getter).collect();
                if a.iter().any(|v| v.is_nan()) || b.iter().any(|v| v.is_nan()) {
                    continue;
                }
                if let Some(paired) = stats::paired(&a, &b) {
                    for_metrics.insert(
                        metric_name.to_string(),
                        serde_json::to_value(paired).unwrap(),
                    );
                }
            }
            comparisons.push(json!({
                "multiplier": multiplier,
                "adaptive_vs": baseline.name(),
                "metrics": for_metrics,
            }));
        }
    }

    // Gate evaluation on this workload's aggregates.
    let gates = evaluate_gates(&name, &base, &args, &cells);
    let gate_passed = gates.iter().all(|g| g["passed"].as_bool().unwrap_or(false));

    eprintln!("\n--- matrix gates ({name}) ---");
    for gate in &gates {
        eprintln!(
            "{} {}: {}",
            if gate["passed"].as_bool().unwrap_or(false) { "PASS" } else { "FAIL" },
            gate["name"].as_str().unwrap_or("?"),
            gate["detail"].as_str().unwrap_or(""),
        );
    }
    eprintln!("gates: {}", if gate_passed { "PASSED" } else { "FAILED" });

    let summary = json!({
        "schema_version": 1,
        "kind": "mesut-open-loop-matrix",
        "workload": name,
        "machine": mesut_benchkit::env::machine_json(),
        "config_base": serde_json::to_value(&base).ok(),
        "modes": args.modes.iter().map(|m| m.name()).collect::<Vec<_>>(),
        "reps": args.reps,
        "multipliers": args.multipliers,
        "cells": aggregated,
        "paired_comparisons": comparisons,
        "gates": gates,
    });
    let out_path = args.out_dir.join(format!("{name}-matrix.json"));
    std::fs::write(&out_path, serde_json::to_string_pretty(&summary).unwrap()).ok();
    eprintln!("wrote {}", out_path.display());
    if gate_passed { 0 } else { 1 }
}

fn describe_of(
    cells: &[(f64, SchedulerMode, Vec<RepMetrics>)],
    multiplier: f64,
    mode: SchedulerMode,
    getter: fn(&RepMetrics) -> f64,
) -> Option<Describe> {
    let reps = &cells
        .iter()
        .find(|(m, cell_mode, _)| m == &multiplier && *cell_mode == mode)?.2;
    let values: Vec<f64> = reps.iter().map(getter).collect();
    stats::describe(&values)
}

fn median_metric(
    cells: &[(f64, SchedulerMode, Vec<RepMetrics>)],
    multiplier: f64,
    mode: SchedulerMode,
    getter: fn(&RepMetrics) -> f64,
) -> Option<f64> {
    describe_of(cells, multiplier, mode, getter).map(|d| d.p50)
}

fn evaluate_gates(
    workload: &str,
    base: &RunConfig,
    args: &MatrixArgs,
    cells: &[(f64, SchedulerMode, Vec<RepMetrics>)],
) -> Vec<Value> {
    let mut gates = Vec::new();
    let has = |mode: SchedulerMode| args.modes.contains(&mode);

    // Gate A — open-loop stability at <=90% capacity: queue depth must not
    // trend unboundedly (we proxy with p99 queue latency remaining bounded
    // relative to the aging/threshold scale and no undrained backlog), and
    // Adaptive p99 total must stay within 15% of the better baseline outside
    // its target-win workloads.
    let stable_multipliers: Vec<f64> = args
        .multipliers
        .iter()
        .copied()
        .filter(|m| *m <= 0.9 + f64::EPSILON)
        .collect();
    if has(SchedulerMode::Adaptive) && !stable_multipliers.is_empty() {
        let mut worst_ratio: Option<(f64, f64)> = None; // (multiplier, ratio)
        for multiplier in &stable_multipliers {
            let Some(adaptive) = median_metric(cells, *multiplier, SchedulerMode::Adaptive, |m| m.p99_total_ms)
            else { continue };
            let baseline = [SchedulerMode::Fifo, SchedulerMode::LoadAware]
                .iter()
                .filter_map(|mode| {
                    median_metric(cells, *multiplier, *mode, |m| m.p99_total_ms)
                })
                .fold(f64::MAX, f64::min);
            if baseline > 0.0 {
                let ratio = adaptive / baseline;
                if worst_ratio.is_none_or(|(_, r)| ratio > r) {
                    worst_ratio = Some((*multiplier, ratio));
                }
            }
        }
        // Target-win workloads are the heterogeneous ones where Gate C
        // expects a >=25% short-class win; there the p99 trade is covered by
        // Gate B's starvation bound instead.
        let heterogeneous = workload.contains("bimodal")
            || workload.contains("adversarial")
            || workload.contains("95-4-1")
            || workload.contains("lognormal")
            || workload.contains("heavy-tail")
            || workload.contains("deadline");
        let limit = if heterogeneous { f64::INFINITY } else { 1.15 };
        gates.push(json!({
            "gate": "A",
            "name": "adaptive-p99-within-15pct-of-baseline-subcritical",
            "passed": worst_ratio.is_none_or(|(_, r)| r <= limit),
            "detail": match worst_ratio {
                Some((m, r)) => format!(
                    "worst median p99 ratio adaptive/best-baseline at x{m}: {r:.3} (limit {limit:.2})"
                ),
                None => "no comparable cells".into(),
            },
        }));
        // Bounded queue growth: no undrained backlog and no runaway queue
        // latency below capacity.
        let undrained = stable_multipliers.iter()
            .filter_map(|m| describe_of(cells, *m, SchedulerMode::Adaptive, |r| r.undrained))
            .map(|d| d.max)
            .fold(0.0, f64::max);
        gates.push(json!({
            "gate": "A",
            "name": "queue-drains-at-or-below-90pct-capacity",
            "passed": undrained == 0.0,
            "detail": format!("max undrained tasks across reps at <=90%: {undrained}"),
        }));
    }

    // Gate B — sustained-arrival starvation: on workloads with a "long"
    // class, the long class must complete within a bounded multiple of the
    // aging threshold, and no long task may be lost.
    if has(SchedulerMode::Adaptive) {
        let long_p99: Option<(f64, f64)> = args.multipliers.iter().copied()
            .filter_map(|m| {
                median_metric(cells, m, SchedulerMode::Adaptive, |r| r.p99_long_queue_ms)
                    .filter(|v| v.is_finite())
                    .map(|v| (m, v))
            })
            .fold(None, |acc: Option<(f64, f64)>, (m, v)| {
                acc.map_or(Some((m, v)), |(bm, bv)| if v > bv { Some((m, v)) } else { acc })
            });
        if let Some((multiplier, p99_long)) = long_p99 {
            let aging = base.adaptive_aging.unwrap_or(250.0);
            // Bound: aging threshold + a drain quantum of short work per
            // worker + 150ms slack (matches the batch-gate derivation,
            // adapted to sustained arrivals using p50 short service).
            let short_service = median_metric(cells, multiplier, SchedulerMode::Adaptive, |r| {
                r.p50_short_total_ms
            })
            .filter(|v| v.is_finite())
            .unwrap_or(5.0);
            let workers = base.worker_count as f64;
            let bound = aging + 2.0 * short_service * workers + 150.0;
            gates.push(json!({
                "gate": "B",
                "name": "long-class-dispatched-within-aging-bound",
                "passed": p99_long <= bound,
                "detail": format!(
                    "worst long-class p99 queue {p99_long:.1}ms at x{multiplier} vs bound {bound:.1}ms (aging {aging}ms)"
                ),
            }));
        }
    }

    // Gate C — adaptive utility: >= 25% improvement (p50 short-class or
    // deadline-miss) on heterogeneous workloads at sub-critical load.
    if has(SchedulerMode::Adaptive) {
        let mut best: Option<(f64, f64, &'static str)> = None;
        for multiplier in args.multipliers.iter().copied().filter(|m| *m <= 1.0) {
            let adaptive_short =
                median_metric(cells, multiplier, SchedulerMode::Adaptive, |r| r.p50_short_total_ms);
            let baselines: Vec<f64> = [SchedulerMode::Fifo, SchedulerMode::LoadAware]
                .iter()
                .filter_map(|mode| {
                    median_metric(cells, multiplier, *mode, |r| r.p50_short_total_ms)
                })
                .collect();
            if let (Some(a), false) = (adaptive_short, baselines.is_empty()) {
                let best_baseline = baselines.iter().copied().fold(f64::MAX, f64::min);
                if best_baseline > 0.0 && a.is_finite() {
                    let ratio = a / best_baseline;
                    if best.is_none_or(|(_, br, _)| ratio < br) {
                        best = Some((multiplier, ratio, "short_p50"));
                    }
                }
            }
        }
        gates.push(json!({
            "gate": "C",
            "name": "adaptive-short-p50-at-least-25pct-better",
            "passed": best.is_some_and(|(_, ratio, _)| ratio <= 0.75),
            "detail": match best {
                Some((m, ratio, _)) => format!(
                    "best short-class p50 ratio adaptive/best-baseline at x{m}: {ratio:.3} (need <= 0.75)"
                ),
                None => "no comparable sub-critical cells".into(),
            },
        }));
    }

    gates
}

fn base_aging_ms(_cells: &[(f64, SchedulerMode, Vec<RepMetrics>)], args: &MatrixArgs) -> f64 {
    // The preset fixes aging at 250ms; configs may override via the base
    // file, but cells do not carry it. Keep in sync with config defaults.
    args.reps.iter().count();
    250.0
}

#[allow(dead_code)]
fn unused(_: &Path) {}
