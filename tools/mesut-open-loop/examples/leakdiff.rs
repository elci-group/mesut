//! Differential leak probe: 30k no-op tasks through progressively more of
//! the stack, one scenario per process. RSS delta per task localizes the
//! growth.

use mesut::prelude::*;
use mesut::SchedulingMode;
use mesut_core::RoutingHint;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn rss_bytes() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status.lines().find(|l| l.starts_with("VmRSS:")).unwrap();
    line.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap() * 1024
}

const TASKS: usize = 30_000;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let scenario = std::env::args().nth(1).unwrap_or_default();
    let done = Arc::new(AtomicUsize::new(0));

    // Settle allocator with a warmup wave through the same path.
    async fn wave(scenario: &str, done: Arc<AtomicUsize>, count: usize, labeled: bool) {
        let start = done.load(Ordering::Acquire);
        let end = start + count;
        match scenario {
            "raw-rayon" => {
                let pool = rayon::ThreadPoolBuilder::new().num_threads(8).build().unwrap();
                for _ in 0..count {
                    let done = done.clone();
                    pool.spawn(move || {
                        done.fetch_add(1, Ordering::AcqRel);
                    });
                }
            }
            "rayon-executor" | "mesut" | "mesut-adaptive" | "mesut-unlabeled" | "mesut-telemetry" => {
                let build = |mode: SchedulingMode, telemetry: bool| {
                    MesuT::new(
                        RuntimeConfig::new()
                            .with_animations(false)
                            .with_telemetry(telemetry)
                            .with_scheduling(mode)
                            .with_max_in_flight(8)
                            .with_max_queue_size(1_000_000),
                    )
                    .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
                    .with_compute_executor(Arc::new(
                        RayonExecutor::new(
                            mesut_rayon::RayonExecutorConfig::new().with_num_threads(8),
                        )
                        .unwrap(),
                    ))
                    .with_blocking_executor(Arc::new(mesut_blocking::BlockingExecutor::new(
                        mesut_blocking::BlockingExecutorConfig::new().with_num_threads(8),
                    )))
                };
                match scenario {
                    "rayon-executor" => {
                        let executor: Arc<dyn mesut::Executor> = Arc::new(
                            RayonExecutor::new(
                                mesut_rayon::RayonExecutorConfig::new().with_num_threads(8),
                            )
                            .unwrap(),
                        );
                        for index in 0..count {
                            let done = done.clone();
                            let work = Work::new(WorkKind::Compute)
                                .with_hint(RoutingHint::RequireCompute)
                                .with_job(|_| Ok(vec![]))
                                .on_complete(move |_| {
                                    done.fetch_add(1, Ordering::AcqRel);
                                });
                            let _ = index;
                            executor.submit(work).await.unwrap();
                        }
                    }
                    _ => {
                        let mode = if scenario == "mesut-adaptive" {
                            SchedulingMode::Adaptive
                        } else {
                            SchedulingMode::Fifo
                        };
                        let telemetry = scenario != "mesut-telemetry";
                        let runtime = Arc::new(build(mode, telemetry));
                        for index in 0..count {
                            let done = done.clone();
                            let mut work = Work::new(WorkKind::Compute)
                                .with_hint(RoutingHint::RequireCompute)
                                .with_job(|_| Ok(vec![]))
                                .on_complete(move |_| {
                                    done.fetch_add(1, Ordering::AcqRel);
                                });
                            if labeled {
                                work = work.with_label(format!("l-{}", index % 4));
                            }
                            runtime.submit(work).await.unwrap();
                        }
                        // Hold the runtime alive until the wave drains so
                        // scenario teardown is comparable.
                        while done.load(Ordering::Acquire) < end {
                            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        }
                        let _ = runtime.runtime_state();
                    }
                }
            }
            other => panic!("unknown scenario {other}"),
        }
    }

    let labeled = scenario != "mesut-unlabeled";
    let target = |waves: usize| waves * TASKS;

    // Warmup.
    wave(&scenario, done.clone(), TASKS, labeled).await;
    while done.load(Ordering::Acquire) < target(1) {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let baseline = rss_bytes();

    // Measurement.
    wave(&scenario, done.clone(), TASKS, labeled).await;
    while done.load(Ordering::Acquire) < target(2) {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let after = rss_bytes();

    let per_task = (after.saturating_sub(baseline)) as f64 / TASKS as f64;
    println!(
        "{{\"scenario\":\"{scenario}\",\"baseline\":{baseline},\"after\":{after},\"bytes_per_task\":{per_task:.1}}}"
    );
}

