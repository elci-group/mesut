use mesut::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = MesuT::default()
        .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
        .with_compute_executor(Arc::new(RayonExecutor::new(Default::default())?))
        .with_blocking_executor(Arc::new(BlockingExecutor::new(Default::default())));

    let fetch = PipelineStage::new("fetch".into(), WorkKind::Io)
        .with_work(Work::new(WorkKind::Io).with_future(async { Ok(vec![10, 20, 30]) }));
    let fetch_id = fetch.id;
    let sum = PipelineStage::new("sum".into(), WorkKind::Compute)
        .with_dependency(fetch_id)
        .with_factory(move |inputs| {
            let values = inputs[&fetch_id].clone();
            Ok(Work::new(WorkKind::Compute).with_job(move |_| {
                Ok(values
                    .iter()
                    .map(|value| u64::from(*value))
                    .sum::<u64>()
                    .to_string()
                    .into_bytes())
            }))
        });
    let sum_id = sum.id;
    let format = PipelineStage::new("format".into(), WorkKind::Blocking)
        .with_dependency(sum_id)
        .with_factory(move |inputs| {
            let total = inputs[&sum_id].clone();
            Ok(Work::new(WorkKind::Blocking).with_job(move |_| {
                Ok(format!("Pipeline result: {}", String::from_utf8_lossy(&total)).into_bytes())
            }))
        });
    let final_id = format.id;
    // Intentionally inserted out of order: prerequisites determine execution.
    let pipeline = Pipeline::new()
        .add_stage(format)
        .add_stage(sum)
        .add_stage(fetch);
    let outputs = runtime.execute_pipeline(pipeline).await?;
    println!("{}", String::from_utf8_lossy(&outputs[&final_id]));
    assert_eq!(runtime.active_tasks(), 0);
    Ok(())
}
