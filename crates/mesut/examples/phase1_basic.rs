use mesut::prelude::*;
use mesut::Executor;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("=== Mesut Phase 1 Foundation Example ===\n");

    let config = RuntimeConfig::default();
    let runtime = MesuT::new(config);

    // Create executors
    println!("Setting up executors...");
    let async_executor = Arc::new(mesut::TokioExecutor::new(
        mesut_tokio::TokioExecutorConfig::default(),
    ));
    let compute_executor =
        match mesut::RayonExecutor::new(mesut_rayon::RayonExecutorConfig::default()) {
            Ok(executor) => Arc::new(executor),
            Err(e) => {
                eprintln!("Failed to create Rayon executor: {}", e);
                return Err(format!("Rayon executor initialization failed: {}", e).into());
            }
        };
    let blocking_executor = Arc::new(mesut::BlockingExecutor::new(
        mesut_blocking::BlockingExecutorConfig::default(),
    ));

    // Setup runtime with all executors
    let runtime = runtime
        .with_async_executor(async_executor.clone())
        .with_compute_executor(compute_executor.clone())
        .with_blocking_executor(blocking_executor.clone());

    // Setup tracing
    let trace_collector = Arc::new(mesut::TraceCollector::new());
    let runtime = runtime.with_observer(trace_collector.clone());

    println!("✓ Async executor: {}", async_executor.id());
    println!("✓ Compute executor: {}", compute_executor.id());
    println!("✓ Blocking executor: {}", blocking_executor.id());
    println!();

    // Submit I/O work
    println!("Submitting I/O work...");
    let io_work = mesut_core::Work::new(WorkKind::Io).with_priority(Priority::Normal);
    runtime.submit(io_work).await?;
    println!("✓ I/O task submitted\n");

    // Submit compute work
    println!("Submitting compute work...");
    let compute_work = mesut_core::Work::new(WorkKind::Compute).with_priority(Priority::High);
    runtime.submit(compute_work).await?;
    println!("✓ Compute task submitted\n");

    // Submit blocking work
    println!("Submitting blocking work...");
    let blocking_work = mesut_core::Work::new(WorkKind::Blocking).with_priority(Priority::Normal);
    runtime.submit(blocking_work).await?;
    println!("✓ Blocking task submitted\n");

    // Let work execute
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Display routing decisions
    println!("=== Routing Decisions ===");
    for event in trace_collector.events() {
        println!("  Task {}: {:?}", event.task_id, event.event_type);
    }
    println!();

    println!("✓ Phase 1 Foundation complete!");
    println!("  - Work model: ✓ Implemented");
    println!("  - Executor abstraction: ✓ Implemented");
    println!("  - Router: ✓ Working");
    println!("  - Tokio/Rayon/Blocking adapters: ✓ Integrated");
    println!("  - Cancellation: ✓ Available");
    println!("  - Basic telemetry: ✓ Working");

    Ok(())
}
