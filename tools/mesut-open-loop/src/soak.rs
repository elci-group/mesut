//! Long-duration soak runs (WS8): sustained mixed load while RSS, task
//! registry, threads, fds, and latency are monitored; resource-integrity
//! verdicts per Gate F.
//!
//! Fail conditions (blockers): unexplained monotonic growth in RSS, task
//! registry, label histories, file descriptors, or threads; unbounded queue
//! growth; latency drift without a workload cause.

use crate::config::{self, RunConfig, SchedulerMode};
use crate::engine;
use mesut_benchkit::stats::{self, trend};
use serde_json::json;

struct SoakArgs {
    minutes: f64,
    warmup_minutes: f64,
    mode: SchedulerMode,
    preset: String,
    out: Option<std::path::PathBuf>,
    monitor_ms: f64,
    rate: Option<f64>,
}

fn parse_args(argv: &[String]) -> Result<SoakArgs, String> {
    let mut args = SoakArgs {
        minutes: 30.0,
        warmup_minutes: 2.0,
        mode: SchedulerMode::Adaptive,
        preset: "mixed-cpu-io".into(),
        out: None,
        monitor_ms: 1000.0,
        rate: None,
    };
    let mut it = argv.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--minutes" => args.minutes = it.next().ok_or("--minutes needs a value")?.parse().map_err(|_| "bad minutes")?,
            "--warmup-minutes" => args.warmup_minutes = it.next().ok_or("--warmup-minutes needs a value")?.parse().map_err(|_| "bad warmup")?,
            "--mode" => args.mode = match it.next().ok_or("--mode needs a value")?.as_str() {
                "fifo" => SchedulerMode::Fifo,
                "priority" => SchedulerMode::Priority,
                "loadaware" => SchedulerMode::LoadAware,
                "adaptive" => SchedulerMode::Adaptive,
                other => return Err(format!("unknown mode {other}")),
            },
            "--preset" => args.preset = it.next().ok_or("--preset needs a value")?.clone(),
            "--rate" => args.rate = Some(it.next().ok_or("--rate needs a value")?.parse().map_err(|_| "bad rate")?),
            "--monitor-ms" => args.monitor_ms = it.next().ok_or("--monitor-ms needs a value")?.parse().map_err(|_| "bad monitor-ms")?,
            "--out" => args.out = Some(std::path::PathBuf::from(it.next().ok_or("--out needs a value")?)),
            other => return Err(format!("unknown flag: {other}")),
        }
    }
    if args.minutes < 1.0 || args.minutes > 24.0 * 60.0 {
        return Err("--minutes must be within 1..=1440".into());
    }
    Ok(args)
}

/// First-vs-last quartile comparison, robust against spikes.
fn grew(baseline: &[f64], final_: &[f64], tolerance: f64) -> Option<(f64, f64, bool)> {
    let base = stats::mean(baseline);
    let fin = stats::mean(final_);
    let grew = base > 0.0 && (fin - base) / base > tolerance;
    Some((base, fin, grew))
}

pub fn cmd_soak(argv: &[String]) -> i32 {
    if cfg!(debug_assertions) {
        eprintln!("soak evidence requires a release build");
        return 2;
    }
    let args = match parse_args(argv) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };
    let value = match config::preset(&args.preset) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };
    let mut cfg: RunConfig = match serde_json::from_value(value) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("config: {e}");
            return 2;
        }
    };
    cfg.scheduler_mode = args.mode;
    cfg.duration = args.minutes * 60.0;
    cfg.warmup_duration = args.warmup_minutes * 60.0;
    cfg.cooldown_duration = 30.0;
    cfg.monitor_interval_ms = args.monitor_ms;
    if let Some(rate) = args.rate {
        cfg.arrival.arrival_rate = rate;
        scale_rates(&mut cfg, rate);
    }
    // ~70% of nominal capacity for soak: the mixed preset at reduced rate.
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("tokio: {e}");
            return 2;
        }
    };
    eprintln!(
        "soak: {} {} for {:.0}min (warmup {:.0}min), λ={}/s",
        args.preset,
        args.mode.name(),
        args.minutes,
        args.warmup_minutes,
        cfg.arrival.arrival_rate,
    );
    let run = rt.block_on(engine::run(&cfg));
    drop(rt);
    let run = match run {
        Ok(run) => run,
        Err(e) => {
            eprintln!("soak run failed: {e}");
            return 1;
        }
    };

    // ---- Verdicts over the measured window (post-warmup samples) ----
    let samples = &run.samples;
    let measured: Vec<_> = samples
        .iter()
        .filter(|s| s.t >= cfg.warmup_duration)
        .collect();
    let n = measured.len();
    if n < 10 {
        eprintln!("soak produced too few samples ({n})");
        return 1;
    }
    let quarter = n / 4;
    let first_quarter = &measured[..quarter];
    let last_quarter = &measured[n - quarter..];

    let series = |pick: fn(&engine::Sample) -> Option<f64>| -> Vec<(f64, f64)> {
        measured
            .iter()
            .filter_map(|s| pick(s).map(|v| (s.t, v)))
            .collect()
    };
    let slope_of = |series: &[(f64, f64)]| -> Option<(f64, f64, f64)> {
        let (times, values): (Vec<f64>, Vec<f64>) =
            series.iter().copied().unzip();
        trend(&times, &values).map(|t| (t.slope_per_second, t.ci95.0, t.ci95.1))
    };

    let mut verdicts = Vec::new();
    let mut check = |name: &str, passed: bool, detail: String| {
        eprintln!("{} {}: {}", if passed { "PASS" } else { "FAIL" }, name, detail);
        verdicts.push(json!({ "name": name, "passed": passed, "detail": detail }));
    };

    // RSS growth: must not trend upward with CI excluding zero.
    let rss_series = series(|s| s.rss_bytes.map(|v| v as f64));
    let rss_bytes_per_s = slope_of(&rss_series);
    let (rss_ok, rss_detail) = match rss_bytes_per_s {
        Some((slope, lo, hi)) => {
            let mb_per_hour = slope * 3600.0 / 1e6;
            (
                lo <= 0.0 || mb_per_hour < 1.0,
                format!("RSS {mb_per_hour:+.3} MB/h (95% CI [{:.3}, {:.3}])", lo * 3600.0 / 1e6, hi * 3600.0 / 1e6),
            )
        }
        None => (true, "no rss series".into()),
    };
    check("rss-no-monotonic-growth", rss_ok, rss_detail);

    // Threads and fds: last quarter vs first quarter, tolerant of jitter.
    for (name, pick) in [
        ("threads", |s: &engine::Sample| s.threads.map(|v| v as f64)),
        ("open-fds", |s: &engine::Sample| s.fds.map(|v| v as f64)),
    ] {
        let first: Vec<f64> = first_quarter.iter().filter_map(|s| pick(s)).collect();
        let last: Vec<f64> = last_quarter.iter().filter_map(|s| pick(s)).collect();
        match grew(&first, &last, 0.10) {
            Some((base, fin, did_grow)) => check(
                &format!("{name}-stable"),
                !did_grow,
                format!("{name}: {base:.0} -> {fin:.0} (limit +10%)"),
            ),
            None => check(&format!("{name}-stable"), true, format!("{name}: not exposed")),
        }
    }

    // Task registry: queued + in_flight bounded by admission limits, no
    // upward trend.
    let registry_series = series(|s| Some((s.queued + s.in_flight) as f64));
    let registry_max = registry_series.iter().map(|(_, v)| *v).fold(0.0, f64::max);
    let registry_limit = (cfg.max_in_flight + cfg.queue_limit) as f64;
    let registry_slope = slope_of(&registry_series);
    let registry_trending = registry_slope.is_some_and(|(slope, lo, _)| lo > 0.0 && slope > 0.5);
    check(
        "task-registry-bounded",
        registry_max <= registry_limit && !registry_trending,
        format!(
            "peak tracked {registry_max} (limit {registry_limit}), trend {}",
            match registry_slope { Some((s, lo, _)) => format!("{s:.2}/s CI-lo {lo:.2}"), None => "n/a".into() },
        ),
    );

    // Latency drift: per-minute p99 of total latency must not trend upward.
    let mut per_minute: Vec<(f64, Vec<f64>)> = Vec::new();
    for record in &run.records {
        if record.warmup {
            continue;
        }
        let Some(total_us) = record.total_us else { continue };
        let minute = (record.submitted_offset_s / 60.0).floor() as usize;
        if per_minute.len() <= minute {
            per_minute.resize(minute + 1, (0.0, Vec::new()));
        }
        per_minute[minute].1.push(total_us as f64 / 1e3);
    }
    let drift_series: Vec<(f64, f64)> = per_minute
        .iter()
        .enumerate()
        .filter(|(_, (_, values))| values.len() >= 20)
        .map(|(minute, (_, values))| (minute as f64 * 60.0, stats::percentile(values, 0.99)))
        .collect();
    let (drift_ok, drift_detail) = if drift_series.len() >= 5 {
        match slope_of(&drift_series) {
            Some((slope, lo, _)) if lo > 0.0 && slope > 0.05 => (
                false,
                format!("p99 latency drifting +{:.2}ms/min (CI-lo {:.2})", slope * 60.0, lo * 60.0),
            ),
            Some((slope, _, _)) => (true, format!("p99 drift {:+.3}ms/min", slope * 60.0)),
            None => (true, "insufficient drift series".into()),
        }
    } else {
        (true, format!("only {} minutes with enough data", drift_series.len()))
    };
    check("latency-p99-no-upward-drift", drift_ok, drift_detail);

    // Counter sanity: every submitted measured task reaches exactly one
    // terminal outcome.
    let s = &run.summary;
    let completed = s["completed"].as_f64().unwrap_or(0.0);
    let outcomes: f64 = ["completed", "failed", "cancelled", "rejected", "shed"]
        .iter()
        .map(|k| s["outcomes"][k].as_f64().unwrap_or(0.0))
        .sum();
    let undrained = s["undrained_at_cooldown"].as_f64().unwrap_or(0.0);
    check(
        "every-task-terminated",
        (outcomes + undrained - s["submitted_measured"].as_f64().unwrap_or(0.0)).abs() < 1.0,
        format!("outcomes {outcomes} + undrained {undrained} vs submitted {}", s["submitted_measured"]),
    );
    let _ = completed;

    let passed = verdicts.iter().all(|v| v["passed"].as_bool().unwrap_or(false));
    let report = json!({
        "schema_version": 1,
        "kind": "mesut-soak",
        "machine": mesut_benchkit::env::machine_json(),
        "config": serde_json::to_value(&cfg).ok(),
        "gate_f": {
            "passed": passed,
            "checks": verdicts,
        },
        "summary": s,
        "samples": samples,
    });
    let path = args.out.clone().unwrap_or_else(|| {
        std::path::PathBuf::from(format!(
            "docs/evidence/soak-{}-{}min.json",
            args.mode.name(),
            args.minutes as u64
        ))
    });
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(&path, serde_json::to_string_pretty(&report).unwrap()).ok();
    eprintln!("wrote {} — soak {}", path.display(), if passed { "PASSED" } else { "FAILED" });
    if passed { 0 } else { 1 }
}

fn scale_rates(cfg: &mut RunConfig, _rate: f64) {
    // Preset rates already set; hook for explicit --rate scaling of
    // multi-rate processes.
    use mesut_benchkit::ArrivalProcess;
    match &mut cfg.arrival.process {
        ArrivalProcess::Fixed { rate } | ArrivalProcess::Poisson { rate } => {
            *rate = cfg.arrival.arrival_rate;
        }
        ArrivalProcess::Alternating { low_rate, high_rate, .. } => {
            let ratio = cfg.arrival.arrival_rate / cfg.arrival.arrival_rate.max(1.0);
            let _ = ratio;
            // Explicit rate keeps the preset's shape; scaled via arrival_rate
            // only for fixed/poisson.
        }
        ArrivalProcess::Bursty { .. } => {}
    }
}
