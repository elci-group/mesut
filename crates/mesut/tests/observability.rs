use mesut::prelude::*;
use mesut_core::TaskError;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

fn runtime() -> MesuT {
    MesuT::new(RuntimeConfig::new().with_animations(false))
        .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
}

fn track(work: Work) -> (Work, oneshot::Receiver<()>) {
    let (tx, rx) = oneshot::channel();
    (
        work.on_complete(move |_| {
            let _ = tx.send(());
        }),
        rx,
    )
}

async fn wait(rx: oneshot::Receiver<()>) {
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("task did not complete before timeout")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_state_reflects_real_submissions_completions_and_failures() {
    let runtime = runtime();
    assert_eq!(runtime.runtime_state().total_submitted, 0);

    for _ in 0..3 {
        let (work, rx) = track(Work::new(WorkKind::Io).with_future(async { Ok(vec![]) }));
        runtime.submit(work).await.unwrap();
        wait(rx).await;
    }
    for _ in 0..2 {
        let (work, rx) = track(
            Work::new(WorkKind::Io)
                .with_future(async { Err(TaskError::ExecutionFailed("intentional".into())) }),
        );
        runtime.submit(work).await.unwrap();
        wait(rx).await;
    }

    // A pre-admission rejection (incompatible job/executor pairing) never
    // reaches the coordination queue but must still count as a failure.
    let (bad_work, bad_rx) = track(Work::new(WorkKind::Io).with_job(|_| Ok(vec![])));
    assert!(runtime.submit(bad_work).await.is_err());
    wait(bad_rx).await;

    // A real cancellation.
    let (started_tx, started_rx) = oneshot::channel();
    let cancel_work = Work::new(WorkKind::Io).with_future(async move {
        let _ = started_tx.send(());
        std::future::pending().await
    });
    let id = cancel_work.id;
    let (cancel_work, cancel_rx) = track(cancel_work);
    runtime.submit(cancel_work).await.unwrap();
    started_rx.await.unwrap();
    runtime.cancel(id).await.unwrap();
    wait(cancel_rx).await;

    let state = runtime.runtime_state();
    assert_eq!(state.total_submitted, 7);
    assert_eq!(state.total_completed, 3);
    assert_eq!(state.total_failed, 3);
    assert_eq!(state.total_cancelled, 1);

    // The executor stats are keyed by executor id; there is exactly one
    // registered executor in this runtime, so its cumulative counts should
    // cover every task that actually reached it (everything but the
    // pre-admission rejection).
    assert_eq!(state.executor_stats.len(), 1);
    let stats = state.executor_stats.values().next().unwrap();
    assert_eq!(stats.completed_tasks, 3);
    assert_eq!(stats.failed_tasks, 3); // 2 execution failures + 1 cancellation
    assert_eq!(stats.active_tasks, 0);
    assert_eq!(stats.queue_depth, 0);

    runtime.shutdown().await.unwrap();
}
