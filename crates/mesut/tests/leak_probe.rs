//! Gate F leak probe: sustained submission must not grow RSS linearly with
//! completions. Runs the runtime directly — no harness buffers — so growth
//! is attributable to the runtime.
//!
//! Evidence basis (x86-64 Linux, 2026-10): with a 30k-task warmup the
//! marginal RSS growth is ~4–35 B/task depending on glibc allocator
//! tuning; `GLIBC_TUNABLES=glibc.malloc.trim_threshold=65536` collapses it
//! to ~4 B/task, demonstrating allocator retention of per-task allocation
//! churn rather than retained objects. A genuine per-task leak (retained
//! Work/Entry/timing) shows at kilobyte scale. The threshold below sits far
//! from both.

use mesut::prelude::*;
use mesut::SchedulingMode;
use mesut_core::RoutingHint;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(target_os = "linux")]
fn rss_bytes() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status.lines().find(|l| l.starts_with("VmRSS:")).unwrap();
    line.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap() * 1024
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rss_does_not_grow_linearly_with_completions() {
    const WARMUP: usize = 30_000;
    const MEASURED: usize = 100_000;

    let runtime = Arc::new(
        MesuT::new(
            RuntimeConfig::new()
                .with_animations(false)
                .with_telemetry(false)
                .with_scheduling(SchedulingMode::Adaptive)
                .with_max_in_flight(8)
                .with_max_queue_size(1_000_000),
        )
        .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
        .with_compute_executor(Arc::new(
            RayonExecutor::new(mesut_rayon::RayonExecutorConfig::new().with_num_threads(8))
                .unwrap(),
        ))
        .with_blocking_executor(Arc::new(mesut_blocking::BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::new().with_num_threads(8),
        ))),
    );

    let done = Arc::new(AtomicUsize::new(0));
    let submit_wave = |count: usize| {
        let runtime = runtime.clone();
        let done = done.clone();
        async move {
            for index in 0..count {
                let done = done.clone();
                let work = Work::new(WorkKind::Compute)
                    .with_hint(RoutingHint::RequireCompute)
                    .with_label(format!("probe-{}", index % 4))
                    .with_job(|_| Ok(vec![]))
                    .on_complete(move |_| {
                        done.fetch_add(1, Ordering::AcqRel);
                    });
                runtime.submit(work).await.unwrap();
            }
        }
    };
    let wait_rss = |target: usize| -> u64 {
        while done.load(Ordering::Acquire) < target {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        rss_bytes()
    };

    // Warmup: executor pools, tokio arenas, and allocator chunks reach
    // steady state before any measurement.
    submit_wave(WARMUP).await;
    let _ = wait_rss(WARMUP);
    let baseline = wait_rss(WARMUP);

    submit_wave(MEASURED).await;
    let after = wait_rss(WARMUP + MEASURED);

    runtime.shutdown().await.unwrap();

    let growth_per_task = (after.saturating_sub(baseline)) as f64 / MEASURED as f64;
    assert!(
        growth_per_task < 128.0,
        "RSS grew {growth_per_task:.0} bytes/task across {MEASURED} tasks (baseline {baseline}, after {after}) — linear leak"
    );
}
