use mesut::prelude::*;
use mesut::{Executor, ShutdownState, TaskError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

fn runtime_with_one_blocking_worker() -> MesuT {
    MesuT::new(RuntimeConfig::new().with_animations(false)).with_blocking_executor(Arc::new(
        BlockingExecutor::new(mesut_blocking::BlockingExecutorConfig::new().with_num_threads(1)),
    ))
}

/// A closure that refuses to cooperate cannot hang shutdown past the grace
/// period: the runtime force-terminates, clears its bookkeeping, and late
/// completions from the abandoned closure cannot resurrect tracked state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_force_terminates_when_closure_refuses_to_cooperate() {
    let runtime = Arc::new(runtime_with_one_blocking_worker());

    let (started_tx, started_rx) = oneshot::channel();
    let uncooperative = Work::new(WorkKind::Blocking).with_job(move |token| {
        let _ = started_tx.send(());
        // Ignores the token on purpose: the worst-case closure.
        let deadline = std::time::Instant::now() + Duration::from_millis(1500);
        while std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        if token.is_cancelled() {
            Err(TaskError::Cancelled)
        } else {
            Ok(vec![])
        }
    });
    let (uncooperative, uncooperative_done) = {
        let (tx, rx) = oneshot::channel();
        (
            uncooperative.on_complete(move |result| {
                let _ = tx.send(result.as_ref().map(|_| ()).map_err(|e| e.to_string()));
            }),
            rx,
        )
    };
    runtime.submit(uncooperative).await.unwrap();
    started_rx.await.unwrap();

    let queued = Work::new(WorkKind::Blocking).with_job(|_| panic!("force-terminated queued work must not run"));
    let (queued, queued_done) = {
        let (tx, rx) = oneshot::channel();
        (
            queued.on_complete(move |result| {
                let _ = tx.send(result.as_ref().map(|_| ()).map_err(|e| e.to_string()));
            }),
            rx,
        )
    };
    runtime.submit(queued).await.unwrap();
    assert_eq!(runtime.active_tasks(), 2);
    assert_eq!(runtime.shutdown_state(), ShutdownState::Running);

    let began = std::time::Instant::now();
    runtime
        .shutdown_with_grace(Some(Duration::from_millis(150)))
        .await
        .unwrap();
    let elapsed = began.elapsed();

    assert!(
        elapsed < Duration::from_millis(1200),
        "shutdown must be bounded by the grace period plus handoff, took {elapsed:?}"
    );
    assert_eq!(runtime.shutdown_state(), ShutdownState::ForceTerminated);
    assert_eq!(runtime.active_tasks(), 0, "no orphaned bookkeeping");
    // Queued work was finished by the graceful drain as a plain cancellation;
    // the force pass only reached the still-running closure.
    assert_eq!(
        queued_done.await.unwrap(),
        Err("task was cancelled".to_string())
    );

    // The abandoned closure eventually exits; its late completion must not
    // resurrect tracked state or double-count. Its own result is Cancelled
    // (the closure observes the token) under the Shutdown token reason.
    let late = tokio::time::timeout(Duration::from_secs(5), uncooperative_done)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(late, Err("task was cancelled".to_string()));
    assert_eq!(runtime.active_tasks(), 0);
    let state = runtime.runtime_state();
    assert_eq!(state.total_cancelled, 2);
    assert_eq!(state.total_completed, 0);
    assert_eq!(state.shutdown, ShutdownState::ForceTerminated);
}

/// With the grace period unset, shutdown waits unboundedly — and when
/// cooperative work does drain in time, the runtime terminates cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_cooperative_work_and_terminates_cleanly() {
    let runtime = Arc::new(runtime_with_one_blocking_worker());

    let (done_tx, done_rx) = oneshot::channel();
    let cooperative = Work::new(WorkKind::Blocking).with_job(|token| {
        // Cooperates: exits within ~100ms of cancellation being requested.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !token.is_cancelled() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let result = if token.is_cancelled() {
            Err(TaskError::Cancelled)
        } else {
            Ok(vec![])
        };
        let _ = done_tx.send(());
        result
    });
    runtime.submit(cooperative).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    runtime.shutdown_with_grace(None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), done_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(runtime.shutdown_state(), ShutdownState::Terminated);
    assert_eq!(runtime.active_tasks(), 0);
    let state = runtime.runtime_state();
    assert_eq!(state.total_cancelled, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_shutdown_cancels_queued_work_and_keeps_running_work_tracked() {
    let executor = Arc::new(BlockingExecutor::new(
        mesut_blocking::BlockingExecutorConfig::new().with_num_threads(1),
    ));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    executor
        .submit(
            Work::new(WorkKind::Blocking)
                .with_job(move |_| {
                    started_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(vec![])
                })
                .on_complete(move |result| {
                    let _ = done_tx.send(result.clone());
                }),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), started_rx)
        .await
        .unwrap()
        .unwrap();
    let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
    executor
        .submit(
            Work::new(WorkKind::Blocking)
                .with_job(|_| panic!("cancelled queued job must not run"))
                .on_complete(move |result| {
                    let _ = queued_tx.send(result.clone());
                }),
        )
        .await
        .unwrap();
    executor.shutdown().await.unwrap();
    assert_eq!(executor.active_tasks(), 2);
    assert!(matches!(
        executor.submit(Work::new(WorkKind::Blocking)).await,
        Err(TaskError::ExecutorUnavailable)
    ));
    release_tx.send(()).unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap(),
        Err(TaskError::Cancelled)
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), queued_rx)
            .await
            .unwrap()
            .unwrap(),
        Err(TaskError::Cancelled)
    ));
    assert_eq!(executor.active_tasks(), 0);
}
