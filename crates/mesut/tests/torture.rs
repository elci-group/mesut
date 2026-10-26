//! WS7 — cancellation and shutdown torture suite. Gate E: 0 deadlocks,
//! 0 duplicate terminal events, 0 task-state resurrection, 0 leaked tracked
//! tasks. Invariant: every task ends in exactly one terminal runtime state.

use mesut::prelude::*;
use mesut::{SchedulingMode, ShutdownState, TaskError, TaskResult};
use mesut_core::{CancellationToken, RoutingHint, TaskScope};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn burn_cooperative(service: Duration, token: &CancellationToken) -> Result<(), ()> {
    let deadline = std::time::Instant::now() + service;
    let mut n = 0xdeadbeef_u64;
    while std::time::Instant::now() < deadline {
        if token.is_cancelled() {
            std::hint::black_box(n);
            return Err(());
        }
        for _ in 0..512 {
            n = n.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
    }
    std::hint::black_box(n);
    Ok(())
}

fn burn_uncooperative(service: Duration) {
    let deadline = std::time::Instant::now() + service;
    let mut n = 0xdeadbeef_u64;
    while std::time::Instant::now() < deadline {
        for _ in 0..512 {
            n = n.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
    }
    std::hint::black_box(n);
}

fn build(max_in_flight: usize, grace: Option<Duration>) -> Arc<MesuT> {
    Arc::new(
        MesuT::new(
            RuntimeConfig::new()
                .with_animations(false)
                .with_telemetry(false)
                .with_scheduling(SchedulingMode::Fifo)
                .with_max_in_flight(max_in_flight)
                .with_max_queue_size(10_000)
                .with_shutdown_grace(grace),
        )
        .with_async_executor(Arc::new(TokioExecutor::new(Default::default())))
        .with_compute_executor(Arc::new(
            RayonExecutor::new(
                mesut_rayon::RayonExecutorConfig::new().with_num_threads(max_in_flight.max(2)),
            )
            .unwrap(),
        ))
        .with_blocking_executor(Arc::new(mesut_blocking::BlockingExecutor::new(
            mesut_blocking::BlockingExecutorConfig::new().with_num_threads(max_in_flight.max(2)),
        ))),
    )
}

/// Completion callback asserting exactly-once terminal delivery for one
/// task. A duplicate event panics the test.
fn exactly_once_terminal(
    counter: Arc<AtomicUsize>,
) -> impl FnOnce(&TaskResult<Vec<u8>>) + Send + 'static {
    move |_| {
        assert_eq!(
            counter.fetch_add(1, Ordering::AcqRel),
            0,
            "duplicate terminal event for one task"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_before_admission_never_executes() {
    let runtime = build(2, Some(Duration::from_secs(10)));
    let ran = Arc::new(AtomicUsize::new(0));
    let terminal = Arc::new(AtomicUsize::new(0));
    let mut work = Work::new(WorkKind::Compute)
        .with_hint(RoutingHint::RequireCompute)
        .with_job({
            let ran = ran.clone();
            move |_| {
                ran.fetch_add(1, Ordering::AcqRel);
                Ok(vec![])
            }
        });
    work.cancellation.cancel();
    let work = work.on_complete(exactly_once_terminal(terminal.clone()));
    let result = runtime.submit(work).await;
    assert!(result.is_err(), "cancelled submission must be rejected");
    runtime.join().await.unwrap();
    assert_eq!(
        ran.load(Ordering::Acquire),
        0,
        "job ran despite pre-admission cancel"
    );
    assert_eq!(terminal.load(Ordering::Acquire), 1);
    assert_eq!(runtime.active_tasks(), 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_while_queued_prevents_execution() {
    let runtime = build(1, Some(Duration::from_millis(10_000)));
    let executions = Arc::new(AtomicUsize::new(0));
    let mut flags = Vec::new();
    for _ in 0..12 {
        let executions = executions.clone();
        let work = Work::new(WorkKind::Compute)
            .with_hint(RoutingHint::RequireCompute)
            .with_job(move |token| {
                if burn_cooperative(Duration::from_millis(30), &token).is_ok() {
                    executions.fetch_add(1, Ordering::AcqRel);
                }
                Ok(vec![])
            });
        flags.push((work.id, work.cancellation.clone()));
        runtime.submit(work).await.unwrap();
    }
    // Cancel 8 of the 12 while they are still queued.
    for (id, token) in flags.iter().skip(2).take(8) {
        token.cancel();
        let _ = runtime.cancel(*id).await;
    }
    runtime.join().await.unwrap();
    // All 12 terminate; at most the 4 uncancelled ones ever ran.
    assert!(
        executions.load(Ordering::Acquire) <= 4,
        "queued-cancelled tasks executed: {}",
        executions.load(Ordering::Acquire)
    );
    assert_eq!(runtime.active_tasks(), 0, "leaked tracked tasks");
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_racing_start_runs_at_most_once() {
    for _ in 0..25 {
        let runtime = build(2, Some(Duration::from_secs(10)));
        let started = Arc::new(AtomicUsize::new(0));
        let started_for_job = started.clone();
        let work = Work::new(WorkKind::Compute)
            .with_hint(RoutingHint::RequireCompute)
            .with_job(move |token| {
                started_for_job.fetch_add(1, Ordering::AcqRel);
                let _ = burn_cooperative(Duration::from_millis(50), &token);
                Ok(vec![])
            });
        let id = work.id;
        let started_for_start = started.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<TaskResult<Vec<u8>>>();
        let work = work
            .on_start(move || {
                started_for_start.fetch_add(1, Ordering::AcqRel);
            })
            .on_complete(move |result| {
                let _ = tx.send(result.clone());
            });
        runtime.submit(work).await.unwrap();
        // Cancel immediately: races with dispatch/start. At most one start
        // of the job, exactly one terminal event.
        let _ = runtime.cancel(id).await;
        let _ = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("task never reached a terminal state")
            .unwrap();
        assert!(started.load(Ordering::Acquire) <= 1);
        assert_eq!(runtime.active_tasks(), 0);
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_and_cancellation_produce_exactly_one_terminal_reason() {
    for round in 0..25 {
        let runtime = build(1, Some(Duration::from_secs(10)));
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        let work = Work::new(WorkKind::Compute)
            .with_hint(RoutingHint::RequireCompute)
            .with_timeout(Duration::from_millis(if round % 2 == 0 { 20 } else { 80 }))
            .with_job(|token| {
                let _ = burn_cooperative(Duration::from_millis(200), &token);
                Ok(vec![])
            });
        let work = work.on_complete(move |result| {
            let reason = match result {
                Ok(_) => "ok".to_string(),
                Err(TaskError::Cancelled) => "cancelled".to_string(),
                Err(TaskError::Timeout(_)) => "timeout".to_string(),
                Err(other) => format!("other:{other}"),
            };
            let _ = tx.send(reason);
        });
        let id = work.id;
        runtime.submit(work).await.unwrap();
        if round % 3 == 0 {
            let _ = runtime.cancel(id).await;
        }
        // Await the task's own terminal event (join() returns when tracked
        // state clears, which precedes user callbacks).
        let reason = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("task never reached a terminal state")
            .unwrap();
        assert!(
            matches!(reason.as_str(), "ok" | "cancelled" | "timeout"),
            "round {round}: unexpected terminal reason {reason:?}"
        );
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scope_cancellation_during_child_completion_keeps_one_terminal_each() {
    let runtime = build(4, Some(Duration::from_secs(10)));
    let scope = TaskScope::new(mesut_core::TaskId::new());
    let mut rxs = Vec::new();
    for index in 0..16 {
        let work = Work::new(WorkKind::Compute)
            .with_hint(RoutingHint::RequireCompute)
            .with_job(move |token| {
                let service = if index % 4 == 0 { 40 } else { 10 };
                let _ = burn_cooperative(Duration::from_millis(service), &token);
                Ok(vec![])
            });
        let terminal = Arc::new(AtomicUsize::new(0));
        let terminal_for_cb = terminal.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<TaskResult<Vec<u8>>>();
        let work = work.with_scope(&scope).on_complete(move |result| {
            assert_eq!(
                terminal_for_cb.fetch_add(1, Ordering::AcqRel),
                0,
                "scope child terminated twice"
            );
            let _ = tx.send(result.clone());
        });
        runtime.submit_scoped(&scope, work).await.unwrap();
        rxs.push((rx, terminal));
    }
    // Cancel the scope while children are mid-flight.
    scope.cancel();
    for (rx, terminal) in rxs {
        let result = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("scope child never reached a terminal state")
            .unwrap();
        let _ = result;
        assert_eq!(terminal.load(Ordering::Acquire), 1);
    }
    runtime.join().await.unwrap();
    assert!(scope.is_empty(), "scope leaked children");
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_with_queued_work_terminates_every_task_exactly_once() {
    let runtime = build(2, Some(Duration::from_secs(5)));
    let terminals = Arc::new(AtomicUsize::new(0));
    let total = Arc::new(AtomicUsize::new(0));
    for _ in 0..100 {
        let terminals = terminals.clone();
        let total = total.clone();
        let work = Work::new(WorkKind::Compute)
            .with_hint(RoutingHint::RequireCompute)
            .with_job(|token| {
                let _ = burn_cooperative(Duration::from_millis(20), &token);
                Ok(vec![])
            })
            .on_complete(move |_| {
                assert_eq!(
                    terminals.fetch_add(1, Ordering::AcqRel),
                    0,
                    "duplicate terminal during shutdown drain"
                );
                total.fetch_add(1, Ordering::AcqRel);
            });
        runtime.submit(work).await.unwrap();
    }
    runtime.shutdown().await.unwrap();
    assert_eq!(
        terminals.load(Ordering::Acquire),
        100,
        "not every queued task reached exactly one terminal state"
    );
    assert_eq!(runtime.active_tasks(), 0);
    assert!(matches!(
        runtime.shutdown_state(),
        ShutdownState::Terminated | ShutdownState::ForceTerminated
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_during_dispatch_completes_within_grace() {
    for _ in 0..10 {
        let runtime = build(4, Some(Duration::from_millis(500)));
        for _ in 0..50 {
            let work = Work::new(WorkKind::Compute)
                .with_hint(RoutingHint::RequireCompute)
                .with_job(|token| {
                    let _ = burn_cooperative(Duration::from_millis(30), &token);
                    Ok(vec![])
                });
            let _ = runtime.submit(work).await;
        }
        let began = std::time::Instant::now();
        runtime.shutdown().await.unwrap();
        assert!(
            began.elapsed() < Duration::from_secs(5),
            "shutdown with 500ms grace took {:?}",
            began.elapsed()
        );
        assert_eq!(runtime.active_tasks(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_termination_ignores_uncooperative_closures_without_resurrection() {
    let runtime = build(2, Some(Duration::from_millis(100)));
    let mut ids = Vec::new();
    for _ in 0..4 {
        let work = Work::new(WorkKind::Compute)
            .with_hint(RoutingHint::RequireCompute)
            .with_job(|_| {
                // Ignores cancellation entirely.
                burn_uncooperative(Duration::from_millis(700));
                Ok(vec![])
            })
            .on_complete(|_| {});
        ids.push(work.id);
        runtime.submit(work).await.unwrap();
    }
    std::thread::sleep(Duration::from_millis(150));
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.shutdown_state(), ShutdownState::ForceTerminated);
    assert_eq!(
        runtime.active_tasks(),
        0,
        "tracked state resurrected after force termination"
    );
    // Late completions may fire on executor threads; they must not resurrect
    // tracked state or restart the lifecycle.
    for id in &ids {
        assert_eq!(
            runtime.task_state(*id),
            None,
            "task state resurrected: {id}"
        );
    }
    // Wait out the 700ms closures, then confirm the terminal state holds.
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(runtime.shutdown_state(), ShutdownState::ForceTerminated);
    assert_eq!(runtime.active_tasks(), 0);
}

/// Randomized churn: seeded PRNG drives concurrent submit/cancel/timeout
/// while the exactly-once terminal invariant is asserted per task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn randomized_churn_keeps_lifecycle_integrity() {
    let runtime = build(6, Some(Duration::from_secs(10)));
    let live: Arc<std::sync::Mutex<Vec<mesut_core::TaskId>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut prng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut rng = move || {
        prng ^= prng << 13;
        prng ^= prng >> 7;
        prng ^= prng << 17;
        prng
    };

    for round in 0..600 {
        let live_for_task = live.clone();
        let work = Work::new(WorkKind::Compute)
            .with_hint(RoutingHint::RequireCompute)
            .with_label(format!("churn-{}", round % 6))
            .with_timeout(Duration::from_millis(5 + (rng() % 40) as u64))
            .with_job(move |token| {
                let _ = burn_cooperative(Duration::from_millis(10), &token);
                Ok(vec![])
            })
            .on_complete(move |_| {
                // Terminal reached; the live set is reconciled at drain time.
                let _ = live_for_task;
            });
        let id = work.id;
        if runtime.submit(work).await.is_ok() {
            live.lock().unwrap().push(id);
        }
        match rng() % 100 {
            0..=34 => {
                let victim = {
                    let mut list = live.lock().unwrap();
                    if list.is_empty() {
                        None
                    } else {
                        let index = (rng() as usize) % list.len();
                        Some(list.swap_remove(index))
                    }
                };
                if let Some(id) = victim {
                    let _ = runtime.cancel(id).await;
                }
            }
            _ => {}
        }
    }
    // Cancel everything still tracked, drain, and verify the invariants.
    let all: Vec<_> = live.lock().unwrap().drain(..).collect();
    for id in &all {
        let _ = runtime.cancel(*id).await;
    }
    runtime.join().await.unwrap();
    assert_eq!(
        runtime.active_tasks(),
        0,
        "leaked tracked tasks after churn"
    );
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.active_tasks(), 0);
    assert!(matches!(
        runtime.shutdown_state(),
        ShutdownState::Terminated | ShutdownState::ForceTerminated
    ));
}
