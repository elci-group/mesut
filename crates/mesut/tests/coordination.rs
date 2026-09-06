use mesut::prelude::*;
use mesut::{BackpressurePolicy, SchedulingMode, TaskError, TaskResult, TaskScope};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

fn runtime(config: RuntimeConfig) -> Arc<MesuT> {
    Arc::new(
        MesuT::new(config.with_animations(false))
            .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
            .with_compute_executor(Arc::new(
                RayonExecutor::new(mesut_rayon::RayonExecutorConfig::new().with_num_threads(1))
                    .unwrap(),
            ))
            .with_blocking_executor(Arc::new(BlockingExecutor::new(
                mesut_blocking::BlockingExecutorConfig::new().with_num_threads(1),
            ))),
    )
}

fn result(work: Work) -> (Work, oneshot::Receiver<TaskResult<Vec<u8>>>) {
    let (tx, rx) = oneshot::channel();
    (
        work.on_complete(move |result| {
            let _ = tx.send(result.clone());
        }),
        rx,
    )
}

async fn receive(rx: oneshot::Receiver<TaskResult<Vec<u8>>>) -> TaskResult<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(3), rx)
        .await
        .unwrap()
        .unwrap()
}

async fn occupy(runtime: &MesuT) -> oneshot::Sender<()> {
    let (release, wait) = oneshot::channel();
    let (started, ready) = oneshot::channel();
    runtime
        .submit(Work::new(WorkKind::Io).with_future(async move {
            let _ = started.send(());
            let _ = wait.await;
            Ok(vec![])
        }))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), ready)
        .await
        .unwrap()
        .unwrap();
    release
}

#[tokio::test]
async fn priority_and_fifo_control_actual_dispatch_order() {
    for (mode, expected) in [
        (SchedulingMode::Priority, vec![2, 3, 1]),
        (SchedulingMode::Fifo, vec![1, 2, 3]),
    ] {
        let runtime = runtime(
            RuntimeConfig::new()
                .with_max_in_flight(1)
                .with_scheduling(mode),
        );
        let release = occupy(&runtime).await;
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        for (id, priority) in [(1, Priority::Low), (2, Priority::High), (3, Priority::High)] {
            let observed = observed.clone();
            runtime
                .submit(
                    Work::new(WorkKind::Io)
                        .with_priority(priority)
                        .with_future(async move {
                            observed.lock().unwrap().push(id);
                            Ok(vec![])
                        }),
                )
                .await
                .unwrap();
        }
        release.send(()).unwrap();
        runtime.join().await.unwrap();
        assert_eq!(*observed.lock().unwrap(), expected);
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn queue_is_bounded_and_reject_refuses_saturation() {
    for policy in [BackpressurePolicy::Queue, BackpressurePolicy::Reject] {
        let runtime = runtime(
            RuntimeConfig::new()
                .with_max_in_flight(1)
                .with_max_queue_size(2)
                .with_backpressure(policy),
        );
        let release = occupy(&runtime).await;
        let second = runtime.submit(Work::new(WorkKind::Io)).await;
        if policy == BackpressurePolicy::Queue {
            second.unwrap();
        } else {
            assert!(matches!(second, Err(TaskError::QueueFull)));
        }
        assert!(matches!(
            runtime.submit(Work::new(WorkKind::Io)).await,
            Err(TaskError::QueueFull)
        ));
        release.send(()).unwrap();
        runtime.shutdown().await.unwrap();
        assert_eq!(runtime.active_tasks(), 0);
    }
}

#[tokio::test]
async fn delay_waits_for_capacity_and_shutdown_wakes_waiters() {
    for close in [false, true] {
        let runtime = runtime(
            RuntimeConfig::new()
                .with_max_queue_size(1)
                .with_backpressure(BackpressurePolicy::Delay),
        );
        let release = occupy(&runtime).await;
        let submitter = runtime.clone();
        let pending = tokio::spawn(async move { submitter.submit(Work::new(WorkKind::Io)).await });
        tokio::task::yield_now().await;
        assert!(!pending.is_finished());
        if close {
            runtime.shutdown().await.unwrap();
            assert!(matches!(
                pending.await.unwrap(),
                Err(TaskError::ExecutorUnavailable)
            ));
        } else {
            release.send(()).unwrap();
            pending.await.unwrap().unwrap();
            runtime.join().await.unwrap();
            runtime.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn shedding_replaces_only_lower_priority_waiting_work() {
    let runtime = runtime(
        RuntimeConfig::new()
            .with_max_in_flight(1)
            .with_max_queue_size(2)
            .with_backpressure(BackpressurePolicy::Shed),
    );
    let release = occupy(&runtime).await;
    let (low, low_result) = result(Work::new(WorkKind::Io).with_priority(Priority::Low));
    runtime.submit(low).await.unwrap();
    let (high, high_result) = result(
        Work::new(WorkKind::Io)
            .with_priority(Priority::High)
            .with_payload(vec![7]),
    );
    runtime.submit(high).await.unwrap();
    assert!(matches!(
        receive(low_result).await,
        Err(TaskError::QueueFull)
    ));
    assert!(matches!(
        runtime
            .submit(Work::new(WorkKind::Io).with_priority(Priority::Low))
            .await,
        Err(TaskError::QueueFull)
    ));
    release.send(()).unwrap();
    assert_eq!(receive(high_result).await.unwrap(), vec![7]);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn accept_explicitly_allows_unbounded_admission() {
    let runtime = runtime(
        RuntimeConfig::new()
            .with_max_in_flight(1)
            .with_max_queue_size(1)
            .with_backpressure(BackpressurePolicy::Accept),
    );
    let _release = occupy(&runtime).await;
    for _ in 0..3 {
        runtime.submit(Work::new(WorkKind::Io)).await.unwrap();
    }
    assert_eq!(runtime.active_tasks(), 4);
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.active_tasks(), 0);
}

#[tokio::test]
async fn deadlines_cover_running_queued_and_waiting_for_admission() {
    let timeout = Duration::from_millis(15);
    let runtime = runtime(RuntimeConfig::new().with_max_in_flight(1));
    let (work, finished) = result(
        Work::new(WorkKind::Io)
            .with_timeout(timeout)
            .with_future(std::future::pending()),
    );
    runtime.submit(work).await.unwrap();
    assert!(matches!(receive(finished).await, Err(TaskError::Timeout(d)) if d == timeout));
    let _release = occupy(&runtime).await;
    let (queued, finished) = result(
        Work::new(WorkKind::Io)
            .with_timeout(timeout)
            .with_future(async { panic!("expired queued work ran") }),
    );
    runtime.submit(queued).await.unwrap();
    assert!(matches!(receive(finished).await, Err(TaskError::Timeout(d)) if d == timeout));
    runtime.shutdown().await.unwrap();
    let runtime = self::runtime(
        RuntimeConfig::new()
            .with_max_queue_size(1)
            .with_backpressure(BackpressurePolicy::Delay),
    );
    let _release = occupy(&runtime).await;
    let (waiting, finished) = result(Work::new(WorkKind::Io).with_timeout(timeout));
    assert!(matches!(runtime.submit(waiting).await, Err(TaskError::Timeout(d)) if d == timeout));
    assert!(matches!(
        receive(finished).await,
        Err(TaskError::Timeout(_))
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_default_deadline_and_work_override_are_enforced() {
    let runtime = runtime(RuntimeConfig::new().with_task_timeout(Duration::from_millis(10)));
    let (work, finished) = result(Work::new(WorkKind::Io).with_future(std::future::pending()));
    runtime.submit(work).await.unwrap();
    assert!(matches!(
        receive(finished).await,
        Err(TaskError::Timeout(_))
    ));
    let (work, finished) = result(
        Work::new(WorkKind::Io)
            .with_timeout(Duration::from_secs(1))
            .with_future(async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok(vec![1])
            }),
    );
    runtime.submit(work).await.unwrap();
    assert_eq!(receive(finished).await.unwrap(), vec![1]);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn awaitable_shutdown_waits_for_noncooperative_closure_exit() {
    let runtime = runtime(RuntimeConfig::new());
    let (started, ready) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let (work, finished) = result(Work::new(WorkKind::Blocking).with_job(move |_| {
        started.send(()).unwrap();
        wait.recv_timeout(Duration::from_secs(3)).unwrap();
        Ok(vec![])
    }));
    runtime.submit(work).await.unwrap();
    ready.await.unwrap();
    let closer = runtime.clone();
    let shutdown = tokio::spawn(async move { closer.shutdown().await });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    assert_eq!(runtime.active_tasks(), 1);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(receive(finished).await, Err(TaskError::Cancelled)));
    assert!(matches!(
        runtime.submit(Work::new(WorkKind::Io)).await,
        Err(TaskError::ExecutorUnavailable)
    ));
}

#[tokio::test]
async fn scope_waits_for_children_and_child_errors_cancel_siblings() {
    let runtime = runtime(RuntimeConfig::new());
    let value = runtime
        .scope(|scope| {
            let runtime = runtime.clone();
            async move {
                runtime
                    .submit_scoped(
                        &scope,
                        Work::new(WorkKind::Io).with_future(async {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                            Ok(vec![1])
                        }),
                    )
                    .await?;
                Ok(42)
            }
        })
        .await
        .unwrap();
    assert_eq!(value, 42);
    assert_eq!(runtime.active_tasks(), 0);
    let outcome: TaskResult<()> = runtime
        .scope(|scope| {
            let runtime = runtime.clone();
            async move {
                runtime
                    .submit_scoped(
                        &scope,
                        Work::new(WorkKind::Io).with_future(std::future::pending()),
                    )
                    .await?;
                runtime
                    .submit_scoped(
                        &scope,
                        Work::new(WorkKind::Io).with_future(async {
                            Err(TaskError::ExecutionFailed("child failed".into()))
                        }),
                    )
                    .await?;
                Ok(())
            }
        })
        .await;
    assert!(matches!(outcome, Err(TaskError::ExecutionFailed(_))));
    assert_eq!(runtime.active_tasks(), 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_scope_and_parent_cancellation_reach_children() {
    let runtime = runtime(RuntimeConfig::new());
    let (started, ready) = oneshot::channel();
    let runner = runtime.clone();
    let scope = tokio::spawn(async move {
        runner
            .scope(|scope| {
                let runner = runner.clone();
                async move {
                    runner
                        .submit_scoped(
                            &scope,
                            Work::new(WorkKind::Io).with_future(async move {
                                let _ = started.send(());
                                std::future::pending().await
                            }),
                        )
                        .await?;
                    std::future::pending::<TaskResult<()>>().await
                }
            })
            .await
    });
    ready.await.unwrap();
    scope.abort();
    let _ = scope.await;
    tokio::time::timeout(Duration::from_secs(3), runtime.join())
        .await
        .unwrap()
        .unwrap();
    let parent = TaskScope::new(TaskId::new());
    let child = TaskScope::with_parent(TaskId::new(), parent.cancellation_token());
    let (work, finished) = result(Work::new(WorkKind::Io).with_future(std::future::pending()));
    runtime.submit_scoped(&child, work).await.unwrap();
    parent.cancel();
    assert!(matches!(receive(finished).await, Err(TaskError::Cancelled)));
    assert!(child.is_empty());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn independent_pipeline_branches_overlap_and_join_outputs() {
    let runtime = runtime(RuntimeConfig::new().with_pipeline_concurrency(2));
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let a_gate = gate.clone();
    let a = PipelineStage::new("a".into(), WorkKind::Io).with_work(
        Work::new(WorkKind::Io).with_future(async move {
            a_gate.wait().await;
            Ok(vec![2])
        }),
    );
    let b = PipelineStage::new("b".into(), WorkKind::Io).with_work(
        Work::new(WorkKind::Io).with_future(async move {
            gate.wait().await;
            Ok(vec![3])
        }),
    );
    let (a_id, b_id) = (a.id, b.id);
    let join = PipelineStage::new("join".into(), WorkKind::Compute)
        .with_dependency(a_id)
        .with_dependency(b_id)
        .with_factory(move |inputs| {
            let sum = inputs[&a_id][0] + inputs[&b_id][0];
            Ok(Work::new(WorkKind::Compute).with_job(move |_| Ok(vec![sum])))
        });
    let join_id = join.id;
    let outputs = tokio::time::timeout(
        Duration::from_secs(3),
        runtime.execute_pipeline(Pipeline::new().add_stage(join).add_stage(a).add_stage(b)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(&**outputs.get(&join_id).unwrap(), &[5]);
    runtime.shutdown().await.unwrap();
}
