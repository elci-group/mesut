use mesut::prelude::*;
use mesut::{BackpressurePolicy, SchedulingMode};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

fn mixed_runtime(config: RuntimeConfig) -> MesuT {
    MesuT::new(config.with_animations(false))
        .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
        .with_compute_executor(Arc::new(
            RayonExecutor::new(mesut_rayon::RayonExecutorConfig::new().with_num_threads(2))
                .unwrap(),
        ))
        .with_blocking_executor(Arc::new(BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::new().with_num_threads(2),
        )))
}

fn track(work: Work) -> (Work, oneshot::Receiver<Result<(), String>>) {
    let (tx, rx) = oneshot::channel();
    (
        work.on_complete(move |result| {
            let _ = tx.send(result.as_ref().map(|_| ()).map_err(|e| e.to_string()));
        }),
        rx,
    )
}

async fn wait(rx: oneshot::Receiver<Result<(), String>>) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(10), rx)
        .await
        .expect("task did not complete before timeout")
        .unwrap()
}

/// A bounded in-flight limit forces the coordinator to actually queue and
/// dispatch across all three adapters instead of handing every submission
/// straight to its executor; if one kind starved the others under that
/// pressure, this would hang or time out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_mixed_load_completes_without_starvation() {
    let runtime = Arc::new(mixed_runtime(
        RuntimeConfig::new()
            .with_max_in_flight(6)
            .with_max_queue_size(1000),
    ));
    const PER_KIND: usize = 40;
    let mut receivers = Vec::new();
    for kind in [WorkKind::Io, WorkKind::Compute, WorkKind::Blocking] {
        for _ in 0..PER_KIND {
            let work = match kind {
                WorkKind::Io => Work::new(kind).with_future(async {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    Ok(vec![])
                }),
                _ => Work::new(kind).with_job(|_| {
                    std::thread::sleep(Duration::from_millis(1));
                    Ok(vec![])
                }),
            };
            let (work, rx) = track(work);
            runtime.submit(work).await.unwrap();
            receivers.push(rx);
        }
    }
    for rx in receivers {
        wait(rx).await.unwrap();
    }
    assert_eq!(runtime.active_tasks(), 0);
    runtime.shutdown().await.unwrap();
}

/// With a single in-flight slot, dispatch order is entirely up to the
/// scheduling policy. Submitting low-priority work first and only then
/// critical-priority work distinguishes real priority scheduling from
/// plain FIFO: FIFO would finish the low-priority batch first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn priority_scheduling_reorders_queued_work_under_saturation() {
    let runtime = Arc::new(mixed_runtime(
        RuntimeConfig::new()
            .with_max_in_flight(1)
            .with_max_queue_size(100)
            .with_scheduling(SchedulingMode::Priority),
    ));

    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (started_tx, started_rx) = oneshot::channel();
    let occupier = Work::new(WorkKind::Compute).with_job(move |_| {
        let _ = started_tx.send(());
        release_rx.recv().unwrap();
        Ok(vec![])
    });
    let (occupier, occupier_done) = track(occupier);
    runtime.submit(occupier).await.unwrap();
    started_rx.await.unwrap();

    let order = Arc::new(Mutex::new(Vec::new()));
    let mut receivers = Vec::new();
    for priority in [Priority::Low; 10] {
        let order = order.clone();
        let work = Work::new(WorkKind::Compute)
            .with_priority(priority)
            .with_job(move |_| Ok(vec![]))
            .on_complete(move |_| order.lock().unwrap().push(priority));
        let (work, rx) = track(work);
        runtime.submit(work).await.unwrap();
        receivers.push(rx);
    }
    for priority in [Priority::Critical; 5] {
        let order = order.clone();
        let work = Work::new(WorkKind::Compute)
            .with_priority(priority)
            .with_job(move |_| Ok(vec![]))
            .on_complete(move |_| order.lock().unwrap().push(priority));
        let (work, rx) = track(work);
        runtime.submit(work).await.unwrap();
        receivers.push(rx);
    }

    release_tx.send(()).unwrap();
    wait(occupier_done).await.unwrap();
    for rx in receivers {
        wait(rx).await.unwrap();
    }

    let order = order.lock().unwrap().clone();
    assert_eq!(order.len(), 15);
    assert_eq!(&order[..5], [Priority::Critical; 5]);
    assert_eq!(&order[5..], [Priority::Low; 10]);
    runtime.shutdown().await.unwrap();
}

/// Adaptive scheduling learns per-label duration from real completions, then
/// uses that history to run historically-fast labeled work ahead of
/// historically-slow labeled work at the same priority, even when the slow
/// work was queued first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adaptive_scheduling_prefers_labels_with_shorter_learned_duration() {
    let runtime = Arc::new(mixed_runtime(
        RuntimeConfig::new()
            .with_max_in_flight(1)
            .with_max_queue_size(100)
            .with_scheduling(SchedulingMode::Adaptive),
    ));

    // Warm up: build distinct duration histories for "slow" and "fast".
    for _ in 0..3 {
        let work = Work::new(WorkKind::Compute)
            .with_label("slow")
            .with_job(|_| {
                std::thread::sleep(Duration::from_millis(30));
                Ok(vec![])
            });
        let (work, rx) = track(work);
        runtime.submit(work).await.unwrap();
        wait(rx).await.unwrap();
    }
    for _ in 0..3 {
        let work = Work::new(WorkKind::Compute)
            .with_label("fast")
            .with_job(|_| {
                std::thread::sleep(Duration::from_millis(1));
                Ok(vec![])
            });
        let (work, rx) = track(work);
        runtime.submit(work).await.unwrap();
        wait(rx).await.unwrap();
    }
    assert!(runtime.workload_profile("slow").unwrap().mean_duration >= Duration::from_millis(20));
    assert!(runtime.workload_profile("fast").unwrap().mean_duration < Duration::from_millis(10));

    // Saturate the single in-flight slot, then queue slow-labeled work
    // before fast-labeled work at the same priority.
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (started_tx, started_rx) = oneshot::channel();
    let occupier = Work::new(WorkKind::Compute).with_job(move |_| {
        let _ = started_tx.send(());
        release_rx.recv().unwrap();
        Ok(vec![])
    });
    let (occupier, occupier_done) = track(occupier);
    runtime.submit(occupier).await.unwrap();
    started_rx.await.unwrap();

    let order = Arc::new(Mutex::new(Vec::new()));
    let mut receivers = Vec::new();
    for label in ["slow", "slow", "slow", "fast", "fast", "fast"] {
        let order = order.clone();
        let work = Work::new(WorkKind::Compute)
            .with_label(label)
            .with_job(|_| Ok(vec![]))
            .on_complete(move |_| order.lock().unwrap().push(label));
        let (work, rx) = track(work);
        runtime.submit(work).await.unwrap();
        receivers.push(rx);
    }

    release_tx.send(()).unwrap();
    wait(occupier_done).await.unwrap();
    for rx in receivers {
        wait(rx).await.unwrap();
    }

    let order = order.lock().unwrap().clone();
    assert_eq!(order, ["fast", "fast", "fast", "slow", "slow", "slow"]);
    runtime.shutdown().await.unwrap();
}

/// `Shed` backpressure must evict queued low-priority work to admit
/// higher-priority work once the queue is full, rather than rejecting it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shed_backpressure_evicts_low_priority_work_to_admit_high_priority() {
    let runtime = Arc::new(mixed_runtime(
        RuntimeConfig::new()
            .with_max_in_flight(1)
            .with_max_queue_size(6)
            .with_backpressure(BackpressurePolicy::Shed)
            .with_scheduling(SchedulingMode::Priority),
    ));

    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (started_tx, started_rx) = oneshot::channel();
    let occupier = Work::new(WorkKind::Compute).with_job(move |_| {
        let _ = started_tx.send(());
        release_rx.recv().unwrap();
        Ok(vec![])
    });
    let (occupier, occupier_done) = track(occupier);
    runtime.submit(occupier).await.unwrap();
    started_rx.await.unwrap();

    // Fill the remaining 5 queue slots with low-priority filler.
    let mut fillers = Vec::new();
    for _ in 0..5 {
        let work = Work::new(WorkKind::Compute)
            .with_priority(Priority::Low)
            .with_job(|_| Ok(vec![]));
        let (work, rx) = track(work);
        runtime.submit(work).await.unwrap();
        fillers.push(rx);
    }

    // The queue is now full; admitting critical work must shed a filler.
    let critical = Work::new(WorkKind::Compute)
        .with_priority(Priority::Critical)
        .with_job(|_| Ok(vec![]));
    let (critical, critical_done) = track(critical);
    runtime.submit(critical).await.unwrap();

    release_tx.send(()).unwrap();
    wait(occupier_done).await.unwrap();
    wait(critical_done).await.unwrap();

    let mut shed = 0;
    let mut completed = 0;
    for rx in fillers {
        match wait(rx).await {
            Ok(()) => completed += 1,
            Err(message) => {
                assert!(message.contains("queue is full"));
                shed += 1;
            }
        }
    }
    assert_eq!(
        shed, 1,
        "exactly one filler must be shed to admit critical work"
    );
    assert_eq!(completed, 4);
    runtime.shutdown().await.unwrap();
}
