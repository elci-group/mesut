use mesut::prelude::*;
use mesut::SchedulingMode;
use mesut_observe::TaskEventType;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

fn runtime(config: RuntimeConfig) -> MesuT {
    MesuT::new(config.with_animations(false))
        .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
        .with_compute_executor(Arc::new(
            RayonExecutor::new(mesut_rayon::RayonExecutorConfig::new().with_num_threads(2))
                .unwrap(),
        ))
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

/// Queue, dispatch, execution, and completion are measured as separate
/// phases, and a task queued behind a blocker shows real queue time without
/// it contaminating the execution phase.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phases_split_queue_dispatch_execution_and_completion() {
    let collector = Arc::new(mesut_observe::TraceCollector::new());
    let runtime = runtime(
        RuntimeConfig::new()
            .with_max_in_flight(1)
            .with_scheduling(SchedulingMode::Fifo),
    )
    .with_observer(collector.clone());

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

    // With one in-flight slot taken, the runtime must report one dispatched
    // task and zero queued ones.
    let snapshot = runtime.runtime_state();
    assert_eq!(snapshot.in_flight_tasks, 1);
    assert_eq!(snapshot.queued_tasks, 0);

    let (timing, timing_rx) = oneshot::channel();
    let victim = Work::new(WorkKind::Compute)
        .with_job(|_| {
            std::thread::sleep(Duration::from_millis(30));
            Ok(vec![])
        })
        .with_timing(mesut::TaskTiming::new().shared())
        .on_complete(move |_| {
            let _ = timing.send(());
        });
    let (victim, victim_done) = track(victim);
    runtime.submit(victim).await.unwrap();

    let snapshot = runtime.runtime_state();
    assert_eq!(snapshot.in_flight_tasks, 1);
    assert_eq!(snapshot.queued_tasks, 1);

    // Hold the queue long enough that queue time is unambiguous.
    tokio::time::sleep(Duration::from_millis(60)).await;
    release_tx.send(()).unwrap();
    wait(occupier_done).await.unwrap();
    wait(victim_done).await.unwrap();
    timing_rx.await.unwrap();

    let completed = collector
        .events()
        .into_iter()
        .filter_map(|event| match event.event_type {
            TaskEventType::Completed { phases, .. } => phases,
            _ => None,
        })
        .next_back()
        .expect("completed event with phases");

    assert!(
        completed.queue_us >= 55_000,
        "queue phase must cover the held-back wait, got {}us",
        completed.queue_us
    );
    assert!(
        completed.execution_us >= 25_000 && completed.execution_us < 500_000,
        "execution phase must reflect the 30ms job, got {}us",
        completed.execution_us
    );
    assert!(
        completed.queue_us > completed.execution_us,
        "queue wait ({}) must not be folded into execution ({})",
        completed.queue_us,
        completed.execution_us
    );
    assert!(
        completed.dispatch_us < 100_000,
        "dispatch handoff should be fast, got {}us",
        completed.dispatch_us
    );
    assert!(
        completed.completion_us < 100_000,
        "completion bookkeeping should be fast, got {}us",
        completed.completion_us
    );
    runtime.shutdown().await.unwrap();
}

/// The learned adaptive profile is a service-time estimate: it must reflect
/// the job's own run, not the queue-inclusive total.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workload_profile_excludes_queue_wait() {
    let runtime = runtime(
        RuntimeConfig::new()
            .with_max_in_flight(1)
            .with_scheduling(SchedulingMode::Adaptive)
            .with_adaptive_aging(None),
    );

    // Seed history so the profile exists.
    let seeded = Work::new(WorkKind::Compute)
        .with_label("hash")
        .with_job(|_| {
            std::thread::sleep(Duration::from_millis(25));
            Ok(vec![])
        });
    let (seeded, seeded_done) = track(seeded);
    runtime.submit(seeded).await.unwrap();
    wait(seeded_done).await.unwrap();

    // Now queue an identical job behind a blocker: its queue wait must not
    // inflate what gets learned.
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

    let (submit_started_tx, submit_started_rx) = oneshot::channel();
    let queued = Work::new(WorkKind::Compute)
        .with_label("hash")
        .with_job(move |_| {
            std::thread::sleep(Duration::from_millis(25));
            Ok(vec![])
        })
        .on_complete(move |_| {
            let _ = submit_started_tx.send(());
        });
    let (queued, queued_done) = track(queued);
    runtime.submit(queued).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    release_tx.send(()).unwrap();
    wait(occupier_done).await.unwrap();
    wait(queued_done).await.unwrap();
    submit_started_rx.await.unwrap();

    let profile = runtime.workload_profile("hash").unwrap();
    assert_eq!(profile.samples, 2);
    assert!(
        profile.mean_service_time < Duration::from_millis(60),
        "learned service time {:?} must stay near the 25ms job despite 80ms of queue wait",
        profile.mean_service_time
    );
    runtime.shutdown().await.unwrap();
}

async fn wait(rx: oneshot::Receiver<Result<(), String>>) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(10), rx)
        .await
        .expect("task did not complete before timeout")
        .unwrap()
}
