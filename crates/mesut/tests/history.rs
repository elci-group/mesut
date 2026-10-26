//! WS6 — Adaptive history quality: regime changes, poisoned outliers,
//! cold labels, and label-cardinality caps, exercised through the runtime.

use mesut::prelude::*;
use mesut::SchedulingMode;
use mesut_core::RoutingHint;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn burn(service: Duration) {
    let deadline = std::time::Instant::now() + service;
    let mut n = 0xdeadbeef_u64;
    while std::time::Instant::now() < deadline {
        for _ in 0..512 {
            n = n.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
    }
    std::hint::black_box(n);
}

fn build(max_labels: usize) -> Arc<MesuT> {
    Arc::new(
        MesuT::new(
            RuntimeConfig::new()
                .with_animations(false)
                .with_telemetry(false)
                .with_scheduling(SchedulingMode::Adaptive)
                .with_max_in_flight(4)
                .with_max_queue_size(10_000)
                .with_adaptive_max_labels(max_labels),
        )
        .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
        .with_compute_executor(Arc::new(
            RayonExecutor::new(mesut_rayon::RayonExecutorConfig::new().with_num_threads(4))
                .unwrap(),
        ))
        .with_blocking_executor(Arc::new(mesut_blocking::BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::new().with_num_threads(4),
        ))),
    )
}

async fn run_labeled(runtime: &MesuT, label: &str, service: Duration) {
    let work = Work::new(WorkKind::Compute)
        .with_hint(RoutingHint::RequireCompute)
        .with_label(label.to_string())
        .with_job(move |_| {
            burn(service);
            Ok(vec![])
        });
    runtime.submit(work).await.unwrap();
    runtime.join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn profile_converges_after_regime_change() {
    let runtime = build(64);
    // Regime 1: "shifting" work is slow.
    for _ in 0..6 {
        run_labeled(&runtime, "shifting", Duration::from_millis(20)).await;
    }
    let before = runtime.workload_profile("shifting").unwrap();
    assert!(before.mean_service_time >= Duration::from_millis(15));

    // Regime 2: the same label is now fast. The rolling window (16) must
    // reflect the new shape after 16 fresh samples; we submit 16.
    for _ in 0..16 {
        run_labeled(&runtime, "shifting", Duration::from_millis(2)).await;
    }
    let after = runtime.workload_profile("shifting").unwrap();
    assert!(
        after.mean_service_time < before.mean_service_time,
        "profile did not move toward the new regime: {before:?} -> {after:?}"
    );
    assert!(after.mean_service_time <= Duration::from_millis(6));
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poisoned_outlier_is_bounded_by_the_window() {
    let runtime = build(64);
    // Fill the 16-sample window with clean work first.
    for _ in 0..16 {
        run_labeled(&runtime, "poisoned", Duration::from_millis(3)).await;
    }
    let clean = runtime.workload_profile("poisoned").unwrap();
    assert!(clean.mean_service_time < Duration::from_millis(10));

    // One 200ms poisoned sample lands in the full window: the mean moves by
    // at most outlier/16.
    run_labeled(&runtime, "poisoned", Duration::from_millis(200)).await;
    let poisoned = runtime.workload_profile("poisoned").unwrap();
    assert!(
        poisoned.mean_service_time > clean.mean_service_time,
        "outlier had no effect at all"
    );
    assert!(
        poisoned.mean_service_time < Duration::from_millis(20),
        "outlier dominated the window: {poisoned:?}"
    );

    // And it rolls out again after 16 clean samples.
    for _ in 0..16 {
        run_labeled(&runtime, "poisoned", Duration::from_millis(3)).await;
    }
    let recovered = runtime.workload_profile("poisoned").unwrap();
    assert!(recovered.mean_service_time < Duration::from_millis(10));
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_labels_schedule_without_starvation() {
    let runtime = build(64);
    // Established short-history label floods the queue.
    let completed = Arc::new(AtomicUsize::new(0));
    for index in 0..40 {
        let done = completed.clone();
        let work = Work::new(WorkKind::Compute)
            .with_hint(RoutingHint::RequireCompute)
            .with_label("warm".to_string())
            .with_job(move |_| {
                burn(Duration::from_millis(2));
                Ok(vec![])
            })
            .on_complete(move |_| {
                done.fetch_add(1, Ordering::AcqRel);
            });
        runtime.submit(work).await.unwrap();
        let _ = index;
    }
    // One cold-labelled task lands behind the flood: it must still complete
    // promptly (neutral bucket = FIFO within priority), not wait for the
    // whole flood.
    let (tx, rx) = tokio::sync::oneshot::channel();
    let cold = Work::new(WorkKind::Compute)
        .with_hint(RoutingHint::RequireCompute)
        .with_label("cold".to_string())
        .with_job(move |_| {
            burn(Duration::from_millis(2));
            Ok(vec![])
        })
        .on_complete(move |_| {
            let _ = tx.send(());
        });
    runtime.submit(cold).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("cold-labelled task starved behind warm flood")
        .unwrap();
    while completed.load(Ordering::Acquire) < 40 {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn label_cardinality_cap_bounds_history() {
    let runtime = build(8);
    for label in 0..20 {
        run_labeled(
            &runtime,
            &format!("cardinality-{label}"),
            Duration::from_millis(1),
        )
        .await;
    }
    assert!(
        runtime.workload_overflow() > 0,
        "expected overflow counts beyond the 8-label cap"
    );
    assert!(runtime.workload_profiles().len() <= 8);
    // Scheduling continues to work for capped labels (neutral bucket).
    run_labeled(&runtime, "cardinality-19", Duration::from_millis(1)).await;
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alternating_shapes_under_one_label_keep_fifo_fairness() {
    let runtime = build(64);
    // One label alternating fast/slow: the label mean sits between the two
    // shapes, so tasks of this label stay mutually FIFO-ish. Every task must
    // still complete and the mean must not collapse to either extreme.
    for index in 0..24 {
        let service = if index % 2 == 0 {
            Duration::from_millis(2)
        } else {
            Duration::from_millis(20)
        };
        run_labeled(&runtime, "alternating", service).await;
    }
    let profile = runtime.workload_profile("alternating").unwrap();
    assert!(profile.mean_service_time >= Duration::from_millis(5));
    assert!(profile.mean_service_time <= Duration::from_millis(14));
    runtime.shutdown().await.unwrap();
}
