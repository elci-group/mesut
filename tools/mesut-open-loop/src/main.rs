//! mesut-open-loop: open-loop load generation and adversarial scheduler
//! evidence for MESUT-AVP-01.
//!
//! Subcommands:
//!   env      — machine-readable environment metadata
//!   presets  — list built-in workload presets (WS1)
//!   run      — one open-loop run from --preset or --config
//!   matrix   — capacity × mode × repetition sweep with gates (WS2/WS3)
//!   overhead — scheduler-overhead microbenchmarks (WS5)
//!   soak     — long-duration resource-integrity soak (WS8)

mod config;
mod engine;
mod matrix;
mod overhead;
mod soak;

use config::{RunConfig, SchedulerMode};
use engine::RunOutput;
use serde_json::json;
use std::io::Write;
use std::path::PathBuf;

type Result<T> = std::result::Result<T, String>;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match args.first().map(String::as_str) {
        Some("env") => cmd_env(),
        Some("presets") => cmd_presets(),
        Some("run") => cmd_run(&args[1..]),
        Some("matrix") => crate::matrix::cmd_matrix(&args[1..]),
        Some("overhead") => crate::overhead::cmd_overhead(&args[1..]),
        Some("soak") => crate::soak::cmd_soak(&args[1..]),
        _ => {
            eprintln!("usage: mesut-open-loop <env|presets|run|matrix|overhead|soak> [options]");
            2
        }
    };
    std::process::exit(code);
}

fn cmd_env() -> i32 {
    println!("{}", serde_json::to_string_pretty(&mesut_benchkit::env::machine_json()).unwrap());
    0
}

fn cmd_presets() -> i32 {
    for name in [
        "homogeneous-short",
        "bimodal",
        "heavy-tail",
        "lognormal",
        "adversarial-95-4-1",
        "spiky",
        "priority-mix",
        "deadline-heavy",
        "bursty",
        "alternating",
        "mixed-cpu-io",
        "steady-overload",
    ] {
        println!("{name}");
    }
    0
}

struct RunArgs {
    preset: Option<String>,
    config_path: Option<PathBuf>,
    out: Option<PathBuf>,
    mode: Option<SchedulerMode>,
    seed: Option<u64>,
    quiet: bool,
}

fn parse_run_args(argv: &[String]) -> Result<RunArgs> {
    let mut parsed = RunArgs {
        preset: None,
        config_path: None,
        out: None,
        mode: None,
        seed: None,
        quiet: false,
    };
    let mut it = argv.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--preset" => parsed.preset = Some(it.next().ok_or("--preset needs a value")?.clone()),
            "--config" => {
                parsed.config_path = Some(PathBuf::from(it.next().ok_or("--config needs a value")?))
            }
            "--out" => parsed.out = Some(PathBuf::from(it.next().ok_or("--out needs a value")?)),
            "--mode" => parsed.mode = Some(parse_mode(&it.next().ok_or("--mode needs a value")?)?),
            "--seed" => parsed.seed = Some(it.next().ok_or("--seed needs a value")?.parse().map_err(|_| "bad seed")?),
            "--quiet" => parsed.quiet = true,
            other => return Err(format!("unknown flag: {other}")),
        }
    }
    if parsed.preset.is_none() && parsed.config_path.is_none() {
        return Err("one of --preset or --config is required".into());
    }
    Ok(parsed)
}

fn parse_mode(name: &str) -> Result<SchedulerMode> {
    match name {
        "fifo" => Ok(SchedulerMode::Fifo),
        "priority" => Ok(SchedulerMode::Priority),
        "loadaware" => Ok(SchedulerMode::LoadAware),
        "adaptive" => Ok(SchedulerMode::Adaptive),
        other => Err(format!("unknown mode {other}")),
    }
}

fn load_config(args: &RunArgs) -> Result<RunConfig> {
    let value = match (&args.preset, &args.config_path) {
        (Some(preset), _) => config::preset(preset)?,
        (None, Some(path)) => {
            let text = std::fs::read_to_string(path)
                .map_err(|e| format!("read {}: {e}", path.display()))?;
            serde_json::from_str::<serde_json::Value>(&text)
                .map_err(|e| format!("parse {}: {e}", path.display()))?
        }
        (None, None) => return Err("one of --preset or --config is required".into()),
    };
    let mut config: RunConfig = serde_json::from_value(value)
        .map_err(|e| format!("config is not a valid RunConfig: {e}"))?;
    if let Some(mode) = args.mode {
        config.scheduler_mode = mode;
    }
    if let Some(seed) = args.seed {
        config.seed = seed;
    }
    Ok(config)
}

fn cmd_run(argv: &[String]) -> i32 {
    if cfg!(debug_assertions) {
        eprintln!("open-loop evidence requires a release build");
        return 2;
    }
    let args = match parse_run_args(argv) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };
    let config = match load_config(&args) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))
        .unwrap();
    let output = runtime.block_on(engine::run(&config));
    drop(runtime);
    match output {
        Ok(run) => {
            report_run(&config, &run, args.quiet);
            let raw = json!({
                "schema_version": 1,
                "kind": "mesut-open-loop-run",
                "machine": mesut_benchkit::env::machine_json(),
                "config": serde_json::to_value(&config).ok(),
                "summary": run.summary,
                "samples": run.samples,
                "records": run.records,
            });
            emit(&raw, args.out.as_deref())
        }
        Err(e) => {
            eprintln!("run failed: {e}");
            1
        }
    }
}

fn report_run(config: &RunConfig, run: &RunOutput, quiet: bool) {
    if quiet {
        return;
    }
    let s = &run.summary;
    let get = |path: &[&str]| -> f64 {
        let mut cur = s;
        for key in path {
            cur = cur.get(key).unwrap_or(&serde_json::Value::Null);
        }
        cur.as_f64().unwrap_or(f64::NAN)
    };
    eprintln!(
        "[{}] {} λ={} {}: completed {} p50 {:.2}ms p99 {:.2}ms max-queue {:.2}ms throughput {:.0}/s rejected {} shed {} undrained {}",
        config.name,
        config.scheduler_mode.name(),
        get(&["lambda_over_mu"]),
        config.arrival.process.describe(),
        get(&["completed"]),
        get(&["total_ms", "p50"]),
        get(&["total_ms", "p99"]),
        get(&["queue_ms", "max"]),
        get(&["throughput_tasks_per_sec"]),
        get(&["outcomes", "rejected"]),
        get(&["outcomes", "shed"]),
        get(&["undrained_at_cooldown"]),
    );
}

fn emit(raw: &serde_json::Value, out: Option<&std::path::Path>) -> i32 {
    let text = serde_json::to_string_pretty(raw).unwrap_or_default();
    match out {
        Some(path) => {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match std::fs::write(path, &text) {
                Ok(()) => {
                    eprintln!("wrote {}", path.display());
                    0
                }
                Err(e) => {
                    eprintln!("write {}: {e}", path.display());
                    1
                }
            }
        }
        None => {
            let mut stdout = std::io::stdout().lock();
            let _ = writeln!(stdout, "{text}");
            0
        }
    }
}
