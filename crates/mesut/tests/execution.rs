use mesut::prelude::*;
use mesut::{ArcExecutor, Executor, TraceCollector};
use mesut_core::{TaskError, TaskResult};
use mesut_observe::TaskEventType;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

fn executors() -> Vec<(WorkKind, ArcExecutor)> {
    vec![
        (
            WorkKind::Io,
            Arc::new(TokioExecutor::new(Default::default())),
        ),
        (
            WorkKind::Compute,
            Arc::new(
                RayonExecutor::new(mesut_rayon::RayonExecutorConfig::new().with_num_threads(1))
                    .unwrap(),
            ),
        ),
        (
            WorkKind::Blocking,
            Arc::new(BlockingExecutor::new(
                mesut_blocking::BlockingExecutorConfig::new().with_num_threads(1),
            )),
        ),
    ]
}

fn runtime(executor: ArcExecutor, observer: Arc<TraceCollector>) -> MesuT {
    MesuT::new(RuntimeConfig::new().with_animations(false))
        .with_async_executor(executor.clone())
        .with_compute_executor(executor.clone())
        .with_blocking_executor(executor)
        .with_observer(observer)
}

fn result_channel(work: Work) -> (Work, oneshot::Receiver<Result<Vec<u8>, String>>) {
    let (tx, rx) = oneshot::channel();
    (
        work.on_complete(move |result| {
            let _ = tx.send(match result {
                Ok(bytes) => Ok(bytes.clone()),
                Err(error) => Err(error.to_string()),
            });
        }),
        rx,
    )
}

async fn receive(rx: oneshot::Receiver<Result<Vec<u8>, String>>) -> Result<Vec<u8>, String> {
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_results_close_each_runtime_and_executor_lifecycle() {
    for (kind, executor) in executors() {
        let observer = Arc::new(TraceCollector::new());
        let runtime = runtime(executor.clone(), observer.clone());
        // Immediate completion repeatedly exercises registration/cleanup races.
        for _ in 0..50 {
            let work = if kind == WorkKind::Io {
                Work::new(kind).with_future(async { Ok(b"async result".to_vec()) })
            } else {
                Work::new(kind).with_job(|_| Ok(b"worker result".to_vec()))
            };
            let id = work.id;
            let (work, result) = result_channel(work);
            runtime.submit(work).await.unwrap();
            let bytes = receive(result).await.unwrap();
            assert_eq!(
                bytes,
                if kind == WorkKind::Io {
                    b"async result".to_vec()
                } else {
                    b"worker result".to_vec()
                }
            );
            assert_eq!(runtime.active_tasks(), 0);
            assert_eq!(executor.active_tasks(), 0);
            let events: Vec<_> = observer
                .events()
                .into_iter()
                .filter(|event| event.task_id == id)
                .collect();
            assert_eq!(events.len(), 5);
            assert!(matches!(
                events[3].event_type,
                TaskEventType::Started { .. }
            ));
            assert!(matches!(
                events[4].event_type,
                TaskEventType::Completed { .. }
            ));
        }
        executor.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panics_and_errors_report_failure_and_release_capacity() {
    for (kind, executor) in executors() {
        let observer = Arc::new(TraceCollector::new());
        let runtime = runtime(executor.clone(), observer.clone());
        for panic in [false, true] {
            let fail = move || -> TaskResult<Vec<u8>> {
                if panic {
                    panic!("intentional test panic");
                }
                Err(TaskError::ExecutionFailed("intentional error".into()))
            };
            let work = if kind == WorkKind::Io {
                Work::new(kind).with_future(async move { fail() })
            } else {
                Work::new(kind).with_job(move |_| fail())
            };
            let (work, result) = result_channel(work);
            runtime.submit(work).await.unwrap();
            assert!(receive(result).await.unwrap_err().contains(if panic {
                "panicked"
            } else {
                "intentional error"
            }));
            assert_eq!(runtime.active_tasks(), 0);
            assert_eq!(executor.active_tasks(), 0);
            assert!(matches!(
                observer.events().last().unwrap().event_type,
                TaskEventType::Failed { .. }
            ));
        }
        executor.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_reaches_running_work_on_every_adapter() {
    for (kind, executor) in executors() {
        let runtime = runtime(executor.clone(), Arc::new(TraceCollector::new()));
        let (started_tx, started_rx) = oneshot::channel();
        let work = if kind == WorkKind::Io {
            Work::new(kind).with_future(async move {
                let _ = started_tx.send(());
                std::future::pending().await
            })
        } else {
            Work::new(kind).with_job(move |token| {
                let _ = started_tx.send(());
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while !token.is_cancelled() && std::time::Instant::now() < deadline {
                    std::thread::yield_now();
                }
                assert!(token.is_cancelled());
                Ok(vec![])
            })
        };
        let id = work.id;
        let (work, result) = result_channel(work);
        runtime.submit(work).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .unwrap()
            .unwrap();
        runtime.cancel(id).await.unwrap();
        assert!(receive(result).await.unwrap_err().contains("cancelled"));
        assert_eq!(runtime.active_tasks(), 0);
        assert_eq!(executor.active_tasks(), 0);
        executor.shutdown().await.unwrap();
        assert!(!executor.is_available());
        assert!(matches!(
            executor.submit(Work::new(kind)).await,
            Err(TaskError::ExecutorUnavailable)
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_capacity_is_reserved_atomically() {
    let executor = Arc::new(TokioExecutor::new(
        mesut_tokio::TokioExecutorConfig::new().with_max_queue_size(1),
    ));
    let mut submissions = Vec::new();
    for _ in 0..32 {
        let executor = executor.clone();
        submissions.push(tokio::spawn(async move {
            executor
                .submit(Work::new(WorkKind::Io).with_future(std::future::pending()))
                .await
        }));
    }
    let mut accepted = 0;
    for submission in submissions {
        match submission.await.unwrap() {
            Ok(_) => accepted += 1,
            Err(TaskError::QueueFull) => (),
            Err(error) => panic!("unexpected: {error}"),
        }
    }
    assert_eq!(accepted, 1);
    assert_eq!(executor.active_tasks(), 1);
    executor.shutdown().await.unwrap();
}

#[tokio::test]
async fn incompatible_work_fails_without_execution_or_stale_runtime_state() {
    for (kind, executor) in executors() {
        let observer = Arc::new(TraceCollector::new());
        let runtime = runtime(executor.clone(), observer.clone());
        let work = if kind == WorkKind::Io {
            Work::new(kind).with_job(|_| panic!("must not execute"))
        } else {
            Work::new(kind).with_future(async { panic!("must not execute") })
        };
        let (work, result) = result_channel(work);
        assert!(matches!(
            runtime.submit(work).await,
            Err(TaskError::InvalidConfiguration(_))
        ));
        assert!(receive(result).await.is_err());
        assert_eq!(runtime.active_tasks(), 0);
        assert_eq!(executor.active_tasks(), 0);
        let events = observer.events();
        assert_eq!(events.len(), 2);
        assert!(matches!(events[1].event_type, TaskEventType::Failed { .. }));
        executor.shutdown().await.unwrap();
    }
}
