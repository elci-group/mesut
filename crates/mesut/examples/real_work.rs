use mesut::prelude::*;
use mesut::Executor;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let io = Arc::new(TokioExecutor::new(Default::default()));
    let compute = Arc::new(RayonExecutor::new(Default::default())?);
    let blocking = Arc::new(BlockingExecutor::new(Default::default()));
    let runtime = MesuT::default()
        .with_async_executor(io.clone())
        .with_compute_executor(compute.clone())
        .with_blocking_executor(blocking.clone());

    let jobs = [
        Work::new(WorkKind::Io).with_future(async {
            tokio::task::yield_now().await;
            Ok(b"async result".to_vec())
        }),
        Work::new(WorkKind::Compute)
            .with_job(|_| Ok((1..=100_u64).sum::<u64>().to_string().into_bytes())),
        Work::new(WorkKind::Blocking).with_job(|_| {
            std::env::current_dir()
                .map(|path| path.display().to_string().into_bytes())
                .map_err(|error| mesut::TaskError::ExecutionFailed(error.to_string()))
        }),
    ];
    let mut results = Vec::new();
    for work in jobs {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        runtime
            .submit(work.on_complete(move |result| {
                let _ = sender.send(result.clone());
            }))
            .await?;
        results.push(receiver);
    }
    for receiver in results {
        let bytes = receiver.await??;
        println!("{}", String::from_utf8(bytes)?);
    }
    assert_eq!(runtime.active_tasks(), 0);
    io.shutdown().await?;
    compute.shutdown().await?;
    blocking.shutdown().await?;
    Ok(())
}
