//! WS4 — real I/O benchmarks: filesystem and loopback network operations
//! through Mesut's explicit WorkKind routing, so scheduler conclusions are
//! tested against real executor behaviour rather than synthetic timers.
//!
//! Scenarios (each run per scheduler mode):
//!   fs-small-read     4KiB file reads (page-cached), async executor
//!   fs-large-read     8MiB sequential reads from a 64MiB file, async
//!   fs-write          1MiB sequential writes, blocking executor
//!   fs-metadata       stat sweeps over many small files, blocking
//!   net-echo          64B request/response over loopback, async
//!   net-stream        1MiB streamed response, async
//!   net-delayed       echo with 5ms server delay, async
//!   net-variable      echo with 0..10ms jittered delay, async
//!   net-failure       connect to a closed port (error handling), async
//!   mixed             interleaved fs + net + cpu + blocking, kinds explicit

use crate::config::SchedulerMode;
use mesut::prelude::*;
use mesut_core::RoutingHint;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct RealioArgs {
    reps: usize,
    ops: usize,
    modes: Vec<SchedulerMode>,
    out: Option<PathBuf>,
}

fn parse_args(argv: &[String]) -> Result<RealioArgs, String> {
    let mut args =
        RealioArgs { reps: 5, ops: 200, modes: SchedulerMode::ALL.to_vec(), out: None };
    let mut it = argv.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--reps" => args.reps = it.next().ok_or("--reps needs a value")?.parse().map_err(|_| "bad reps")?,
            "--ops" => args.ops = it.next().ok_or("--ops needs a value")?.parse().map_err(|_| "bad ops")?,
            "--modes" => {
                args.modes = it
                    .next()
                    .ok_or("--modes needs a value")?
                    .split(',')
                    .map(|part| match part.trim() {
                        "fifo" => Ok(SchedulerMode::Fifo),
                        "priority" => Ok(SchedulerMode::Priority),
                        "loadaware" => Ok(SchedulerMode::LoadAware),
                        "adaptive" => Ok(SchedulerMode::Adaptive),
                        other => Err(format!("unknown mode {other}")),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            }
            "--out" => args.out = Some(PathBuf::from(it.next().ok_or("--out needs a value")?)),
            other => return Err(format!("unknown flag: {other}")),
        }
    }
    if !(1..=50).contains(&args.reps) || !(10..=10_000).contains(&args.ops) {
        return Err("--reps must be 1..=50 and --ops 10..=10000".into());
    }
    Ok(args)
}

fn build(mode: SchedulerMode) -> Arc<MesuT> {
    Arc::new(
        MesuT::new(
            RuntimeConfig::new()
                .with_animations(false)
                .with_telemetry(false)
                .with_scheduling(mode.scheduling())
                .with_max_in_flight(16)
                .with_max_queue_size(10_000),
        )
        .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
        .with_compute_executor(Arc::new(
            RayonExecutor::new(mesut_rayon::RayonExecutorConfig::new().with_num_threads(8))
                .unwrap(),
        ))
        .with_blocking_executor(Arc::new(mesut_blocking::BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::new().with_num_threads(8),
        ))),
    )
}

struct Fixtures {
    dir: PathBuf,
    small: Vec<PathBuf>,
    large: PathBuf,
    port: u16,
}

/// Prepare the temp tree and the loopback echo server. The server honours a
/// tiny protocol: first byte selects delay behaviour, remaining bytes echo.
async fn fixtures() -> Result<Fixtures, String> {
    let dir = PathBuf::from(format!(
        "{}/mesut-realio-{}",
        std::env::temp_dir().display(),
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let mut small = Vec::new();
    let blob = vec![0xABu8; 4096];
    for index in 0..64 {
        let path = dir.join(format!("small-{index}.bin"));
        std::fs::write(&path, &blob).map_err(|e| e.to_string())?;
        small.push(path);
    }
    let large = dir.join("large.bin");
    std::fs::write(&large, vec![0xCDu8; 64 * 1024 * 1024]).map_err(|e| e.to_string())?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else { return };
            tokio::spawn(async move {
                let (mut reader, mut writer) = socket.split();
                let mut mode_byte = [0u8; 1];
                if tokio::io::AsyncReadExt::read_exact(&mut reader, &mut mode_byte)
                    .await
                    .is_err()
                {
                    return;
                }
                match mode_byte[0] {
                    // 1: echo; 2: delay 5ms then echo; 3: delay 0..10ms; 4: stream 1MiB.
                    1 => {
                        let _ = tokio::io::copy(&mut reader, &mut writer).await;
                    }
                    2 => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        let _ = tokio::io::copy(&mut reader, &mut writer).await;
                    }
                    3 => {
                        tokio::time::sleep(Duration::from_millis((rand_jitter() % 10) as u64))
                            .await;
                        let _ = tokio::io::copy(&mut reader, &mut writer).await;
                    }
                    _ => {
                        let chunk = vec![0u8; 64 * 1024];
                        for _ in 0..16 {
                            if tokio::io::AsyncWriteExt::write_all(&mut writer, &chunk)
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
                drop(writer);
            });
        }
    });
    Ok(Fixtures { dir, small, large, port })
}

/// xorshift for jitter; determinism is irrelevant for the delay shape.
fn rand_jitter() -> u64 {
    use std::sync::atomic::AtomicU64;
    static STATE: AtomicU64 = AtomicU64::new(0x2545F4914F6CDD1D);
    let mut s = STATE.load(Ordering::Relaxed);
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    STATE.store(s, Ordering::Relaxed);
    s % 11
}

/// Run `ops` tasks described by `make`, one at a time, collecting per-task
/// end-to-end latency in microseconds.
async fn run_ops<F, Fut>(runtime: &MesuT, label: &str, ops: usize, make: F) -> Vec<f64>
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = mesut::TaskResult<Vec<u8>>> + Send + 'static,
{
    let latencies = Arc::new(std::sync::Mutex::new(Vec::with_capacity(ops)));
    let completed = Arc::new(AtomicUsize::new(0));
    for index in 0..ops {
        let latencies = latencies.clone();
        let completed = completed.clone();
        let completed_for_cb = completed.clone();
        let timing = mesut::TaskTiming::new().shared();
        let timing_for_cb = timing.clone();
        let submitted = std::time::Instant::now();
        let work = Work::new(WorkKind::Io)
            .with_hint(RoutingHint::RequireAsync)
            .with_label(format!("{label}-{index}"))
            .with_timing(timing)
            .with_future(make(index))
            .on_complete(move |result| {
                if result.is_ok() {
                    latencies.lock().unwrap().push(
                        submitted.elapsed().as_secs_f64() * 1e6,
                    );
                }
                completed_for_cb.fetch_add(1, Ordering::AcqRel);
                let _ = timing_for_cb;
            });
        runtime.submit(work).await.unwrap();
        // Small in-flight window keeps the queue realistic for latency work.
        while completed.load(Ordering::Acquire) < index {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    runtime.join().await.unwrap();
    Arc::try_unwrap(latencies).unwrap().into_inner().unwrap()
}

fn describe(mut values: Vec<f64>) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_by(f64::total_cmp);
    let pick = |p: f64| values[((values.len() as f64 * p).ceil() as usize).saturating_sub(1)];
    json!({
        "count": values.len(),
        "mean_us": values.iter().sum::<f64>() / values.len() as f64,
        "p50_us": pick(0.5),
        "p95_us": pick(0.95),
        "p99_us": pick(0.99),
        "max_us": values[values.len() - 1],
    })
}

async fn one_scenario(
    runtime: &MesuT,
    name: &str,
    fx: &Fixtures,
    ops: usize,
) -> Result<Value, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let latencies = match name {
        "fs-small-read" => {
            let paths = fx.small.clone();
            run_ops(runtime, name, ops, |index| {
                let path = paths[index % paths.len()].clone();
                async move {
                    let data = tokio::fs::read(&path)
                        .await
                        .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                    if data.len() != 4096 {
                        return Err(mesut::TaskError::ExecutionFailed("short read".into()));
                    }
                    Ok(vec![])
                }
            })
            .await
        }
        "fs-large-read" => {
            let path = fx.large.clone();
            run_ops(runtime, name, ops.min(50), move |_| {
                let path = path.clone();
                async move {
                    let mut file = tokio::fs::File::open(&path)
                        .await
                        .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                    let mut buf = vec![0u8; 8 * 1024 * 1024];
                    file.read_exact(&mut buf)
                        .await
                        .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                    Ok(vec![])
                }
            })
            .await
        }
        "fs-write" | "fs-metadata" => {
            // These use std::fs on the blocking executor.
            let dir = fx.dir.clone();
            let latencies = Arc::new(std::sync::Mutex::new(Vec::new()));
            let completed = Arc::new(AtomicUsize::new(0));
            for index in 0..ops {
                let latencies = latencies.clone();
                let completed = completed.clone();
                let dir = dir.clone();
                let submitted = std::time::Instant::now();
                let work = match name {
                    "fs-write" => Work::new(WorkKind::Blocking)
                        .with_hint(RoutingHint::RequireBlocking)
                        .with_label(format!("{name}-{index}"))
                        .with_job(move |_| {
                            let path = dir.join(format!("write-{index}.bin"));
                            std::fs::write(&path, vec![0u8; 1024 * 1024])
                                .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                            let _ = std::fs::remove_file(&path);
                            Ok(vec![])
                        }),
                    _ => Work::new(WorkKind::Blocking)
                        .with_hint(RoutingHint::RequireBlocking)
                        .with_label(format!("{name}-{index}"))
                        .with_job(move |_| {
                            let mut count = 0usize;
                            for entry in std::fs::read_dir(&dir)
                                .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?
                            {
                                let Ok(entry) = entry else { continue };
                                let _ = entry.metadata();
                                count += 1;
                                if count >= 64 {
                                    break;
                                }
                            }
                            Ok(vec![])
                        }),
                };
                let completed_for_cb = completed.clone();
                let work = work.on_complete(move |result| {
                    if result.is_ok() {
                        latencies
                            .lock()
                            .unwrap()
                            .push(submitted.elapsed().as_secs_f64() * 1e6);
                    }
                    completed_for_cb.fetch_add(1, Ordering::AcqRel);
                });
                runtime.submit(work).await.unwrap();
                while completed.load(Ordering::Acquire) < index + 1 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            runtime.join().await.unwrap();
            Arc::try_unwrap(latencies).unwrap().into_inner().unwrap()
        }
        "net-echo" | "net-delayed" | "net-variable" => {
            let port = fx.port;
            let mode_byte = match name {
                "net-echo" => 1u8,
                "net-delayed" => 2,
                _ => 3,
            };
            run_ops(runtime, name, ops, move |_| {
                let port = port;
                async move {
                    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
                        .await
                        .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                    stream
                        .write_all(&[mode_byte, 0x42, 0x42])
                        .await
                        .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                    let mut buf = [0u8; 3];
                    stream
                        .read_exact(&mut buf)
                        .await
                        .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                    let _ = stream.shutdown().await;
                    Ok(vec![])
                }
            })
            .await
        }
        "net-stream" => {
            let port = fx.port;
            run_ops(runtime, name, ops.min(50), move |_| {
                let port = port;
                async move {
                    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
                        .await
                        .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                    stream
                        .write_all(&[9, 0])
                        .await
                        .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                    let mut remaining: usize = 1024 * 1024;
                    let mut buf = vec![0u8; 64 * 1024];
                    while remaining > 0 {
                        let read = stream
                            .read(&mut buf)
                            .await
                            .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                        if read == 0 {
                            return Err(mesut::TaskError::ExecutionFailed("short stream".into()));
                        }
                        remaining = remaining.saturating_sub(read);
                    }
                    Ok(vec![])
                }
            })
            .await
        }
        "net-failure" => {
            // Port 1 on loopback is closed by convention: connect fails fast.
            run_ops(runtime, name, ops, |_| async move {
                match tokio::net::TcpStream::connect(("127.0.0.1", 1)).await {
                    Ok(_) => Err(mesut::TaskError::ExecutionFailed(
                        "unexpectedly connected".into(),
                    )),
                    Err(_) => Ok(vec![]),
                }
            })
            .await
        }
        "mixed" => {
            let paths = fx.small.clone();
            let port = fx.port;
            run_ops(runtime, name, ops, |index| {
                let path = paths[index % paths.len()].clone();
                let port = port;
                async move {
                    match index % 4 {
                        0 => {
                            tokio::fs::read(&path)
                                .await
                                .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                        }
                        1 => {
                            let mut stream =
                                tokio::net::TcpStream::connect(("127.0.0.1", port))
                                    .await
                                    .map_err(|e| {
                                        mesut::TaskError::ExecutionFailed(e.to_string())
                                    })?;
                            stream
                                .write_all(&[1, 0])
                                .await
                                .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                            let mut buf = [0u8; 2];
                            stream
                                .read_exact(&mut buf)
                                .await
                                .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                        }
                        2 => {
                            let deadline = std::time::Instant::now()
                                + Duration::from_millis(2);
                            let mut n = 1u64;
                            while std::time::Instant::now() < deadline {
                                n = n.wrapping_mul(6364136223846793005).wrapping_add(1);
                            }
                            std::hint::black_box(n);
                        }
                        _ => {
                            tokio::time::sleep(Duration::from_millis(3)).await;
                        }
                    }
                    Ok(vec![])
                }
            })
            .await
        }
        other => return Err(format!("unknown scenario {other}")),
    };
    Ok(describe(latencies))
}

const SCENARIOS: [&str; 10] = [
    "fs-small-read",
    "fs-large-read",
    "fs-write",
    "fs-metadata",
    "net-echo",
    "net-stream",
    "net-delayed",
    "net-variable",
    "net-failure",
    "mixed",
];

pub fn cmd_realio(argv: &[String]) -> i32 {
    if cfg!(debug_assertions) {
        eprintln!("realio evidence requires a release build");
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
    let report = rt.block_on(async {
        let fx = match fixtures().await {
            Ok(fx) => fx,
            Err(e) => return Err(e),
        };
        let mut runs = Vec::new();
        for mode in &args.modes {
            for scenario in SCENARIOS {
                for rep in 1..=args.reps {
                    let runtime = build(*mode);
                    let result = one_scenario(&runtime, scenario, &fx, args.ops).await;
                    let _ = runtime.shutdown().await;
                    match result {
                        Ok(latency) => {
                            eprintln!(
                                "[{rep}] {scenario} {}: p50 {:.0}us p99 {:.0}us",
                                mode.name(),
                                latency["p50_us"].as_f64().unwrap_or(f64::NAN),
                                latency["p99_us"].as_f64().unwrap_or(f64::NAN),
                            );
                            runs.push(json!({
                                "scenario": scenario,
                                "mode": mode.name(),
                                "rep": rep,
                                "ops": args.ops,
                                "latency_us": latency,
                            }));
                        }
                        Err(e) => return Err(format!("{scenario}: {e}")),
                    }
                }
            }
        }
        let _ = std::fs::remove_dir_all(&fx.dir);
        Ok(runs)
    });
    drop(rt);
    match report {
        Ok(runs) => {
            let report = json!({
                "schema_version": 1,
                "kind": "mesut-realio",
                "machine": mesut_benchkit::env::machine_json(),
                "config": {"reps": args.reps, "ops": args.ops},
                "runs": runs,
            });
            let path = args.out.unwrap_or_else(|| {
                PathBuf::from("docs/evidence/realio.json")
            });
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            std::fs::write(&path, serde_json::to_string_pretty(&report).unwrap()).ok();
            eprintln!("wrote {}", path.display());
            0
        }
        Err(e) => {
            eprintln!("realio failed: {e}");
            1
        }
    }
}
