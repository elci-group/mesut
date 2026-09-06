use mesut::prelude::*;
use mesut::{TaskError, TraceCollector};
use mesut_observe::TaskEventType;
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

fn runtime() -> (Arc<MesuT>, Arc<TraceCollector>) {
    let trace = Arc::new(TraceCollector::new());
    let runtime = MesuT::new(RuntimeConfig::new().with_animations(false))
        .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
        .with_compute_executor(Arc::new(
            RayonExecutor::new(mesut_rayon::RayonExecutorConfig::new().with_num_threads(1))
                .unwrap(),
        ))
        .with_blocking_executor(Arc::new(BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::new().with_num_threads(1),
        )))
        .with_observer(trace.clone());
    (Arc::new(runtime), trace)
}

fn stage(name: &str) -> PipelineStage {
    PipelineStage::new(name.into(), WorkKind::Io)
        .with_work(Work::new(WorkKind::Io).with_future(async { Ok(vec![1]) }))
}

#[test]
fn readiness_requires_completion_and_order_is_topological() {
    let a = stage("a");
    let a_id = a.id;
    let b = stage("b").with_dependency(a_id);
    let b_id = b.id;
    let c = stage("c").with_dependency(a_id).with_dependency(b_id);
    let c_id = c.id;
    let graph = Pipeline::new().add_stage(c).add_stage(b).add_stage(a);
    assert_eq!(graph.execution_order().unwrap(), vec![a_id, b_id, c_id]);
    assert!(graph.is_ready_to_execute(a_id));
    assert!(!graph.is_ready_to_execute(b_id));
    assert!(!graph.is_ready_after(c_id, &HashSet::from([a_id])));
    assert!(graph.is_ready_after(c_id, &HashSet::from([a_id, b_id])));
    assert!(!graph.is_ready_after(c_id, &HashSet::from([a_id, b_id, c_id])));
}

#[test]
fn malformed_graphs_and_cycles_are_rejected() {
    let a = stage("a");
    let id = a.id;
    assert!(Pipeline::new()
        .add_stage(a.with_dependency(id))
        .validate()
        .is_err());
    let mut a = stage("a");
    let b = stage("b").with_dependency(a.id);
    a.dependencies.push(b.id);
    assert!(Pipeline::new()
        .add_stage(a)
        .add_stage(b)
        .validate()
        .is_err());
    assert!(Pipeline::new()
        .add_stage(stage("missing").with_dependency(TaskId::new()))
        .validate()
        .is_err());
    let a = stage("a");
    let b = stage("b").with_dependency(a.id).with_dependency(a.id);
    assert!(Pipeline::new()
        .add_stage(a)
        .add_stage(b)
        .validate()
        .is_err());
    let a = stage("a");
    let mut b = stage("b");
    b.id = a.id;
    assert!(Pipeline::new()
        .add_stage(a)
        .add_stage(b)
        .validate()
        .is_err());
    let a = stage("a");
    let id = a.id;
    let mut graph = Pipeline::new().add_stage(a);
    graph.stages.get_mut(&id).unwrap().id = TaskId::new();
    assert!(graph.validate().is_err());
    graph.stages.get_mut(&id).unwrap().id = id;
    graph.stage_order.clear();
    assert!(graph.validate().is_err());
}

#[test]
fn independent_stages_keep_insertion_order_before_fan_in() {
    let left = stage("left");
    let right = stage("right");
    let (left_id, right_id) = (left.id, right.id);
    let join = stage("join")
        .with_dependency(left_id)
        .with_dependency(right_id);
    let join_id = join.id;
    let pipeline = mesut::PipelineBuilder::new()
        .with_stage(join)
        .with_stage(right)
        .with_stage(left)
        .build()
        .unwrap();
    assert_eq!(
        pipeline.execution_order().unwrap(),
        vec![right_id, left_id, join_id]
    );
}

#[tokio::test]
async fn admission_failure_stops_the_pipeline_without_leaking_state() {
    let runtime = MesuT::default();
    assert!(matches!(
        runtime
            .execute_pipeline(Pipeline::new().add_stage(stage("no executor")))
            .await,
        Err(TaskError::NoExecutor)
    ));
    assert_eq!(runtime.active_tasks(), 0);
}

#[tokio::test]
async fn pipeline_cancellation_reaches_running_compute_jobs() {
    let (runtime, _) = runtime();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let stage = PipelineStage::new("cooperative".into(), WorkKind::Compute).with_work(
        Work::new(WorkKind::Compute)
            .with_job(move |token| {
                let _ = started_tx.send(());
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while !token.is_cancelled() && std::time::Instant::now() < deadline {
                    std::thread::yield_now();
                }
                assert!(token.is_cancelled());
                Ok(vec![])
            })
            .on_complete(move |result| {
                let _ = done_tx.send(result.clone());
            }),
    );
    let pipeline = Pipeline::new().add_stage(stage);
    let token = pipeline.cancellation_token();
    let runner_runtime = runtime.clone();
    let runner = tokio::spawn(async move { runner_runtime.execute_pipeline(pipeline).await });
    tokio::time::timeout(Duration::from_secs(5), started_rx)
        .await
        .unwrap()
        .unwrap();
    token.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), runner)
            .await
            .unwrap()
            .unwrap(),
        Err(TaskError::Cancelled)
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap(),
        Err(TaskError::Cancelled)
    ));
    assert_eq!(runtime.active_tasks(), 0);
}

#[tokio::test]
async fn mixed_executor_pipeline_passes_outputs_and_emits_stage_ids() {
    let (runtime, trace) = runtime();
    let fetch = PipelineStage::new("fetch".into(), WorkKind::Io)
        .with_work(Work::new(WorkKind::Io).with_future(async { Ok(vec![2, 3]) }));
    let fetch_id = fetch.id;
    let compute = PipelineStage::new("sum".into(), WorkKind::Compute)
        .with_dependency(fetch_id)
        .with_factory(move |inputs| {
            assert_eq!(inputs.len(), 1);
            let input = inputs[&fetch_id].clone();
            Ok(Work::new(WorkKind::Compute).with_job(move |_| Ok(vec![input.iter().sum()])))
        });
    let compute_id = compute.id;
    let save = PipelineStage::new("save".into(), WorkKind::Blocking)
        .with_dependency(fetch_id)
        .with_dependency(compute_id)
        .with_factory(move |inputs| {
            assert_eq!(inputs.len(), 2);
            let total = inputs[&compute_id][0];
            Ok(Work::new(WorkKind::Blocking).with_job(move |_| Ok(vec![total * 2])))
        });
    let save_id = save.id;
    let outputs = runtime
        .execute_pipeline(
            Pipeline::new()
                .add_stage(save)
                .add_stage(compute)
                .add_stage(fetch),
        )
        .await
        .unwrap();
    assert_eq!(&**outputs.get(&save_id).unwrap(), &[10]);
    assert_eq!(outputs.len(), 3);
    assert_eq!(runtime.active_tasks(), 0);
    let completed: Vec<_> = trace
        .events()
        .into_iter()
        .filter(|e| matches!(e.event_type, TaskEventType::Completed { .. }))
        .map(|e| e.task_id)
        .collect();
    assert_eq!(completed, vec![fetch_id, compute_id, save_id]);
}

#[tokio::test]
async fn preflight_rejects_missing_work_and_wrong_kind_before_any_submission() {
    let (runtime, trace) = runtime();
    for invalid in [
        PipelineStage::new("missing work".into(), WorkKind::Compute),
        PipelineStage::new("wrong kind".into(), WorkKind::Compute)
            .with_work(Work::new(WorkKind::Io)),
    ] {
        assert!(matches!(
            runtime
                .execute_pipeline(Pipeline::new().add_stage(stage("valid")).add_stage(invalid))
                .await,
            Err(TaskError::InvalidConfiguration(_))
        ));
        assert!(trace.is_empty());
    }
}

#[tokio::test]
async fn failure_and_factory_panic_stop_downstream_work() {
    for factory_panic in [false, true] {
        let (runtime, _) = runtime();
        let first = if factory_panic {
            stage("fail").with_factory(|_| panic!("intentional factory panic"))
        } else {
            stage("fail").with_work(Work::new(WorkKind::Io).with_future(async {
                Err(TaskError::ExecutionFailed(
                    "intentional work failure".into(),
                ))
            }))
        };
        let executed = Arc::new(AtomicBool::new(false));
        let flag = executed.clone();
        let second = stage("unreachable")
            .with_dependency(first.id)
            .with_factory(move |_| {
                flag.store(true, Ordering::SeqCst);
                Ok(Work::new(WorkKind::Io))
            });
        assert!(matches!(
            runtime
                .execute_pipeline(Pipeline::new().add_stage(first).add_stage(second))
                .await,
            Err(TaskError::ExecutionFailed(_))
        ));
        assert!(!executed.load(Ordering::SeqCst));
        assert_eq!(runtime.active_tasks(), 0);
    }
}

#[tokio::test]
async fn empty_and_pre_cancelled_pipelines_have_defined_results() {
    let (runtime, trace) = runtime();
    assert!(runtime
        .execute_pipeline(Pipeline::new())
        .await
        .unwrap()
        .is_empty());
    let pipeline = Pipeline::new().add_stage(stage("never"));
    pipeline.cancellation_token().cancel();
    assert!(matches!(
        runtime.execute_pipeline(pipeline).await,
        Err(TaskError::Cancelled)
    ));
    assert!(trace.is_empty());
}

#[tokio::test]
async fn cancellation_and_dropping_the_runner_abort_active_async_work() {
    for drop_runner in [false, true] {
        let (runtime, trace) = runtime();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let first = stage("pending").with_work(
            Work::new(WorkKind::Io)
                .with_future(async move {
                    let _ = started_tx.send(());
                    std::future::pending().await
                })
                .on_complete(move |result| {
                    let _ = done_tx.send(result.clone());
                }),
        );
        let second = stage("must not run").with_dependency(first.id);
        let pipeline = Pipeline::new().add_stage(first).add_stage(second);
        let token = pipeline.cancellation_token();
        let runner_runtime = runtime.clone();
        let runner = tokio::spawn(async move { runner_runtime.execute_pipeline(pipeline).await });
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .unwrap()
            .unwrap();
        if drop_runner {
            runner.abort();
            assert!(runner.await.unwrap_err().is_cancelled());
        } else {
            token.cancel();
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(5), runner)
                    .await
                    .unwrap()
                    .unwrap(),
                Err(TaskError::Cancelled)
            ));
        }
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), done_rx)
                .await
                .unwrap()
                .unwrap(),
            Err(TaskError::Cancelled)
        ));
        assert_eq!(runtime.active_tasks(), 0);
        assert_eq!(
            trace
                .events()
                .iter()
                .filter(|e| matches!(e.event_type, TaskEventType::Submitted { .. }))
                .count(),
            1
        );
    }
}
