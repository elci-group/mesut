//! Gate F leak probe: sustained submission must not grow RSS linearly with
//! completions. Runs the runtime directly — no harness vectors, no sample
//! buffers — so growth here is attributable to the runtime itself.
//!
//! The assertion is generous (allocator retention, page granularity) but
//! linear-with-completions growth of kilobytes per task fails it.

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
    if cfg!(debug_assertions) {
        // Debug builds allocate differently; evidence soaks run release.
        // Still run a shortened sanity pass in debug CI.
    }
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
            RayonExecutor::new(
                mesut_rayon::RayonExecutorConfig::new().with_num_threads(8),
            )
            .unwrap(),
        ))
        .with_blocking_executor(Arc::new(mesut_blocking::BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::new().with_num_threads(8),
        ))),
    );

    let done = Arc::new(AtomicUsize::new(0));
    let rss_at = |target: usize| -> u64 {
        while done.load(Ordering::Acquire) < target {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
        rss_bytes()
    };

    let submit_wave = |runtime: Arc<MesuT>, done: Arc<AtomicUsize>, count: usize, label: usize| async move {
        for index in 0..count {
            let done = done.clone();
            let work = Work::new(WorkKind::Compute)
                .with_hint(RoutingHint::RequireCompute)
                .with_label(format!("probe-{label}-{}", index % 4))
                .with_job(|_| Ok(vec![]))
                .on_complete(move |_| {
                    done.fetch_add(1, Ordering::AcqRel);
                });
            runtime.submit(work).await.unwrap();
        }
    };

    // Warmup wave: allocator pools reach steady state.
    submit_wave(runtime.clone(), done.clone(), 5_000, 0).await;
    let _ = rss_at(5_000);
    let baseline = rss_at(5_000);

    // Measurement waves: 30k more tasks.
    submit_wave(runtime.clone(), done.clone(), 30_000, 1).await;
    let after = rss_at(35_000);

    runtime.shutdown().await.unwrap();

    let growth_per_task = (after.saturating_sub(baseline)) as f64 / 30_000.0;
    // Fail only on kilobyte-scale per-task growth (linear leak), not on
    // allocator retention noise (bounded pages).
    assert!(
        growth_per_task < 256.0,
        "RSS grew {growth_per_task:.0} bytes/task across 30k tasks (baseline {baseline}, after {after}) — linear leak"
    );
}
