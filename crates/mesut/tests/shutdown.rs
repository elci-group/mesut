use mesut::prelude::*;
use mesut::{Executor, TaskError};
use std::sync::Arc;
use std::time::Duration;

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
