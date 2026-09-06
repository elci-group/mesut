use mesut::{prelude::*, RoutingHint};
use serde_json::{json, Value};
use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const CASES: [&str; 6] = [
    "direct-tokio",
    "mesut-tokio",
    "direct-rayon",
    "mesut-rayon",
    "direct-mixed",
    "mesut-mixed",
];

#[derive(Clone)]
struct Args {
    iterations: usize,
    concurrency: usize,
    threads: usize,
    rounds: usize,
    repeat: usize,
    warmup: usize,
    child: bool,
    case: Option<String>,
}
impl Args {
    fn parse() -> Result<Self> {
        let mut out = Self {
            iterations: 2000,
            concurrency: 32,
            threads: 4,
            rounds: 20000,
            repeat: 3,
            warmup: 256,
            child: false,
            case: None,
        };
        let mut args = std::env::args().skip(1);
        while let Some(key) = args.next() {
            if key == "--child" {
                out.child = true;
                continue;
            }
            if key == "--help" {
                println!("mesut-bench [--iterations 2000] [--concurrency 32] [--threads 4] [--rounds 20000] [--repeat 3] [--warmup 256] [--case direct-tokio|mesut-tokio|direct-rayon|mesut-rayon|direct-mixed|mesut-mixed]");
                std::process::exit(0);
            }
            let value = args.next().ok_or("missing argument value")?;
            match key.as_str() {
                "--iterations" => out.iterations = value.parse()?,
                "--concurrency" => out.concurrency = value.parse()?,
                "--threads" => out.threads = value.parse()?,
                "--rounds" => out.rounds = value.parse()?,
                "--repeat" => out.repeat = value.parse()?,
                "--warmup" => out.warmup = value.parse()?,
                "--case" if CASES.contains(&value.as_str()) => out.case = Some(value),
                _ => return Err(format!("unknown option or case: {key} {value}").into()),
            }
        }
        if !(1..=1_000_000).contains(&out.iterations)
            || !(1..=64).contains(&out.concurrency)
            || !(1..=64).contains(&out.threads)
            || !(1..=10_000_000).contains(&out.rounds)
            || !(3..=100).contains(&out.repeat)
            || !(64..=1_000_000).contains(&out.warmup)
            || (out.child && out.case.is_none())
        {
            return Err(
                "bounds: iterations 1..1000000, concurrency/threads 1..64, rounds 1..10000000, repeat 3..100, warmup 64..1000000; --child requires --case"
                    .into(),
            );
        }
        Ok(out)
    }
}

fn rss() -> Option<i64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|v| v.split_whitespace().next()?.parse().ok())
        })
}
fn delta(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    Some(a? - b?)
}
fn cpu(id: usize, rounds: usize) -> Vec<u8> {
    let mut n = id as u64 + 1;
    for _ in 0..rounds {
        n = n
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        n ^= n >> 23;
    }
    std::hint::black_box(n).to_le_bytes().to_vec()
}
async fn io(id: usize) -> Vec<u8> {
    tokio::time::sleep(Duration::from_millis(1)).await;
    (id as u64).to_le_bytes().to_vec()
}
fn is_cpu(case: &str, id: usize) -> bool {
    case.ends_with("rayon") || (case.ends_with("mixed") && id % 2 == 1)
}
fn percentiles(mut values: Vec<f64>) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_by(f64::total_cmp);
    let q = |p: f64| values[((values.len() as f64 * p).ceil() as usize).saturating_sub(1)];
    json!({"count": values.len(), "p50": q(0.50), "p95": q(0.95), "p99": q(0.99)})
}

async fn run(args: &Args, case: &str, baseline: Option<i64>) -> Result<Value> {
    let mesut = case.starts_with("mesut");
    let uses_cpu = !case.ends_with("tokio");
    let uses_io = !case.ends_with("rayon");
    let direct_pool = if !mesut && uses_cpu {
        Some(
            rayon::ThreadPoolBuilder::new()
                .num_threads(args.threads)
                .build()?,
        )
    } else {
        None
    };
    let io_executor = (mesut && uses_io).then(|| Arc::new(TokioExecutor::new(Default::default())));
    // Infer adapter config types through their public constructors.
    let compute_executor = if mesut && uses_cpu {
        let mut config = default_config_for(RayonExecutor::new);
        config.num_threads = Some(args.threads);
        Some(Arc::new(RayonExecutor::new(config)?))
    } else {
        None
    };
    let runtime = if mesut {
        let mut runtime = MesuT::new(RuntimeConfig::default().with_animations(false));
        if uses_io {
            runtime = runtime.with_async_executor(io_executor.as_ref().unwrap().clone());
        }
        if let Some(executor) = &compute_executor {
            runtime = runtime.with_compute_executor(executor.clone());
        }
        Some(Arc::new(runtime))
    } else {
        None
    };
    let after_setup = rss();
    // Touch reusable result storage before warmup and the measured RSS baseline.
    let capacity = args.iterations.max(args.warmup);
    // Nonzero fills force writes rather than leaving calloc's shared zero pages.
    let mut latencies = vec![1.0; capacity];
    let mut io_latencies = vec![1.0; capacity];
    let mut cpu_latencies = vec![1.0; capacity];
    let mut outputs = vec![Some(0_u64); capacity];
    std::hint::black_box((
        &mut latencies,
        &mut io_latencies,
        &mut cpu_latencies,
        &mut outputs,
    ));
    let mut measurements = Value::Null;
    let mut warmup = Value::Null;
    for warming in [true, false] {
        let iterations = if warming {
            args.warmup
        } else {
            args.iterations
        };
        latencies.clear();
        io_latencies.clear();
        cpu_latencies.clear();
        outputs.fill(None);
        let before = rss();
        let mut sampled_max = before;
        let mut checksum = 0_u64;
        let start = Instant::now();
        // Batches deliberately bound both admitted jobs and completion storage.
        for base in (0..iterations).step_by(args.concurrency) {
            let mut pending = tokio::task::JoinSet::new();
            for id in base..(base + args.concurrency).min(iterations) {
                let submitted = Instant::now();
                let compute = is_cpu(case, id);
                let rounds = args.rounds;
                let (tx, rx) = tokio::sync::oneshot::channel();
                if mesut {
                    let work = if compute {
                        Work::new(WorkKind::Compute)
                            .with_hint(RoutingHint::RequireCompute)
                            .with_job(move |_| Ok(cpu(id, rounds)))
                    } else {
                        Work::new(WorkKind::Io)
                            .with_hint(RoutingHint::RequireAsync)
                            .with_future(async move { Ok(io(id).await) })
                    }
                    .on_complete(move |result| {
                        let _ = tx.send((result.clone(), submitted.elapsed().as_secs_f64() * 1e6));
                    });
                    let runtime = runtime.as_ref().unwrap().clone();
                    pending.spawn(async move {
                        runtime.submit(work).await?;
                        let (result, latency) = rx
                            .await
                            .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                        Ok::<_, mesut::TaskError>((id, compute, result?, latency))
                    });
                } else {
                    if compute {
                        direct_pool.as_ref().unwrap().spawn(move || {
                            let result = Ok(cpu(id, rounds));
                            let _ = tx.send((result, submitted.elapsed().as_secs_f64() * 1e6));
                        });
                    } else {
                        tokio::spawn(async move {
                            let result = Ok(io(id).await);
                            let _ = tx.send((result, submitted.elapsed().as_secs_f64() * 1e6));
                        });
                    }
                    pending.spawn(async move {
                        let (result, latency) = rx
                            .await
                            .map_err(|e| mesut::TaskError::ExecutionFailed(e.to_string()))?;
                        Ok::<_, mesut::TaskError>((id, compute, result?, latency))
                    });
                }
            }
            while let Some(result) = pending.join_next().await {
                let (id, compute, bytes, latency) = result??;
                // Expected values are checked after the timed workload, below.
                let value =
                    u64::from_le_bytes(bytes.try_into().map_err(|_| "invalid output length")?);
                checksum = checksum.wrapping_add(value);
                if outputs[id].replace(value).is_some() {
                    return Err("duplicate completion".into());
                }
                latencies.push(latency);
                if compute {
                    cpu_latencies.push(latency);
                } else {
                    io_latencies.push(latency);
                }
            }
            sampled_max = sampled_max.into_iter().chain(rss()).max();
        }
        let seconds = start.elapsed().as_secs_f64();
        if let Some(runtime) = &runtime {
            runtime.join().await?;
        }
        let after = rss();
        sampled_max = sampled_max.into_iter().chain(after).max();
        for (id, value) in outputs.iter().take(iterations).enumerate() {
            let expected = if is_cpu(case, id) {
                u64::from_le_bytes(cpu(id, args.rounds).try_into().unwrap())
            } else {
                id as u64
            };
            if *value != Some(expected) {
                return Err(format!("incorrect or missing result for task {id}").into());
            }
        }
        if warming {
            warmup = json!({"completed": iterations, "elapsed_seconds": seconds,
            "rss_before_kib": before, "rss_after_kib": after});
            continue;
        }
        measurements = json!({"case": case, "completed": latencies.len(), "checksum": checksum, "elapsed_seconds": seconds,
        "tasks_per_second": iterations as f64 / seconds, "latency_us": percentiles(latencies.clone()),
        "io_latency_us": percentiles(io_latencies.clone()), "cpu_latency_us": percentiles(cpu_latencies.clone()),
        "warmup": warmup,
        "rss_kib": {"process_baseline": baseline, "before_work": before, "after_work": after,
            "after_setup": after_setup, "sampled_max": sampled_max,
            "setup_delta": delta(after_setup, baseline),
            "warmup_and_buffers_delta": delta(before, after_setup),
            "work_delta": delta(after, before),
            "sampled_max_delta_from_before_work": delta(sampled_max, before)}});
    }
    if let Some(runtime) = runtime {
        runtime.shutdown().await?;
    }
    Ok(measurements)
}

// The config type is not re-exported by mesut; infer it without another dependency.
fn default_config_for<T: Default, R>(_: fn(T) -> R) -> T {
    T::default()
}

// Aggregate only measurement fields, never integer checksums or task IDs.
fn median_values(values: &[&Value]) -> Value {
    if values.is_empty() || values.iter().any(|v| v.is_null()) {
        return Value::Null;
    }
    if let Some(object) = values[0].as_object() {
        return Value::Object(
            object
                .keys()
                .map(|key| {
                    let fields = values.iter().map(|v| &v[key]).collect::<Vec<_>>();
                    (key.clone(), median_values(&fields))
                })
                .collect(),
        );
    }
    let Some(mut numbers) = values
        .iter()
        .map(|v| v.as_f64())
        .collect::<Option<Vec<_>>>()
    else {
        return Value::Null;
    };
    numbers.sort_by(f64::total_cmp);
    let mid = numbers.len() / 2;
    json!(if numbers.len() % 2 == 0 {
        numbers[mid - 1] / 2.0 + numbers[mid] / 2.0
    } else {
        numbers[mid]
    })
}

fn main() -> Result<()> {
    let args = Args::parse()?;
    if cfg!(debug_assertions) {
        return Err("benchmark requires a release build: cargo run --release --manifest-path tools/mesut-bench/Cargo.toml".into());
    }
    if args.child {
        let case = args.case.as_ref().unwrap();
        let baseline = rss();
        let executor = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(args.threads)
            .enable_all()
            .build()?;
        let result = executor.block_on(async {
            tokio::time::timeout(Duration::from_secs(300), run(&args, case, baseline)).await
        });
        println!("{}", serde_json::to_string(&result??)?);
    } else {
        let mut results = Vec::new();
        let cases: Vec<&str> = match &args.case {
            Some(case) => vec![case.as_str()],
            None => CASES.to_vec(),
        };
        for repetition in 0..args.repeat {
            let mut order = cases.clone();
            // Alternate direction to reduce consistent first/last-case bias.
            if repetition % 2 == 1 {
                order.reverse();
            }
            for case in order {
                let output = std::process::Command::new(std::env::current_exe()?)
                    .args([
                        "--child",
                        "--case",
                        case,
                        "--iterations",
                        &args.iterations.to_string(),
                        "--concurrency",
                        &args.concurrency.to_string(),
                        "--threads",
                        &args.threads.to_string(),
                        "--rounds",
                        &args.rounds.to_string(),
                        "--warmup",
                        &args.warmup.to_string(),
                    ])
                    .output()?;
                if !output.status.success() {
                    return Err(
                        format!("{case}: {}", String::from_utf8_lossy(&output.stderr)).into(),
                    );
                }
                let mut sample = serde_json::from_slice::<Value>(&output.stdout)?;
                sample["repetition"] = json!(repetition + 1);
                results.push(sample);
            }
        }
        let mut medians = Vec::new();
        for case in cases {
            let samples = results
                .iter()
                .filter(|v| v["case"] == case)
                .collect::<Vec<_>>();
            if samples.len() != args.repeat
                || samples.iter().any(|v| {
                    v["completed"] != args.iterations || v["checksum"] != samples[0]["checksum"]
                })
            {
                return Err(format!("inconsistent repetitions for {case}").into());
            }
            let mut summary = json!({"case": case, "repetitions": samples.len()});
            for field in [
                "elapsed_seconds",
                "tasks_per_second",
                "latency_us",
                "io_latency_us",
                "cpu_latency_us",
                "rss_kib",
            ] {
                summary[field] =
                    median_values(&samples.iter().map(|v| &v[field]).collect::<Vec<_>>());
            }
            medians.push(summary);
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({"schema_version": 2,
            "config": {"iterations": args.iterations, "concurrency": args.concurrency, "threads_per_pool": args.threads, "cpu_rounds": args.rounds, "io_sleep_ms": 1,
                "repeat": args.repeat, "warmup_iterations": args.warmup, "debug_assertions": cfg!(debug_assertions)},
            "machine": {"os": std::env::consts::OS, "arch": std::env::consts::ARCH,
                "available_parallelism": std::thread::available_parallelism()?.get(),
                "cpu_model": std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|s| s.lines().find(|l| l.starts_with("model name")).map(str::to_owned)),
                "unix_seconds": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs()},
            "results": results, "medians": medians}))?
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_handles_odd_even_negative_and_missing_readings() {
        assert_eq!(
            median_values(&[&json!(100), &json!(-4), &json!(6)]),
            json!(6.0)
        );
        assert_eq!(median_values(&[&json!(-4), &json!(6)]), json!(1.0));
        assert_eq!(
            median_values(&[&json!(10), &Value::Null, &json!(20)]),
            Value::Null
        );
        assert_eq!(median_values(&[]), Value::Null);
    }

    #[test]
    fn medians_are_of_run_percentiles_not_pooled_samples() {
        let a = json!({"p50": 3, "p99": 100});
        let b = json!({"p50": 1, "p99": 400});
        let c = json!({"p50": 2, "p99": 200});
        assert_eq!(
            median_values(&[&a, &b, &c]),
            json!({"p50": 2.0, "p99": 200.0})
        );
    }

    #[test]
    fn nearest_rank_small_samples() {
        assert_eq!(percentiles(vec![]), Value::Null);
        assert_eq!(
            percentiles(vec![4.0, 1.0, 3.0, 2.0]),
            json!({"count": 4, "p50": 2.0, "p95": 4.0, "p99": 4.0})
        );
    }
}
