# Mesut

A Rust-native unified execution orchestration layer for heterogeneous workloads.

Mesut operates as an execution router and scheduler, determining the appropriate execution substrate (Tokio for I/O, Rayon for compute, blocking workers) for application work.

Mesut's default terminal output follows each task with an adaptive
"scan → lane → assist" animation inspired by Özil's playmaking. It is
lifecycle-driven, adds no artificial delay, and automatically stays quiet in
CI or when stderr is redirected. Set `MESUT_ANIMATIONS=always` to force it,
`MESUT_ANIMATIONS=never` to suppress it, or use
`RuntimeConfig::new().with_animations(false)` for a programmatic opt-out.

## Architecture

```
                         APPLICATION
                              │
                              ▼
                         MESUT API
                              │
                              ▼
                    ┌──────────────────┐
                    │ Work Description │
                    └────────┬─────────┘
                             │
                    ┌────────▼────────┐
                    │ Classification   │
                    │ + Scheduling     │
                    │ + Policy         │
                    └────────┬─────────┘
                             │
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
       ASYNC              COMPUTE            BLOCKING
          │                  │                  │
       Tokio              Rayon             worker pool
          │                  │                  │
          └──────────────────┼──────────────────┘
                             ▼
                           RESULT
```

## Workspace Crates

- **mesut** — Public API
- **mesut-core** — Work model, identifiers, lifecycle
- **mesut-router** — Classification and routing
- **mesut-scheduler** — Scheduling policy
- **mesut-runtime** — Runtime coordination
- **mesut-executor** — Executor abstraction
- **mesut-tokio** — Tokio integration
- **mesut-rayon** — Rayon integration
- **mesut-blocking** — Blocking execution
- **mesut-observe** — Telemetry and diagnostics

## Current development status

Mesut is an early-stage runtime with real execution through Tokio futures and
Rayon/blocking closures. Run `cargo run -p mesut --example real_work` for an
end-to-end example that awaits results from all three adapters.

Construct jobs with `Work::new(WorkKind::Io).with_future(async { Ok(bytes) })`
or `Work::new(WorkKind::Compute).with_job(|token| Ok(bytes))`. Use
`on_complete` to receive `TaskResult<Vec<u8>>`; the example bridges this callback
to a Tokio oneshot receiver. Work without a job returns its payload unchanged.
Closures routed to Tokio, and futures routed to worker pools, are rejected.

Completion, failure, and cancellation remove runtime and executor task state.
`Started` is emitted when execution begins. Every completed task reports its
latency split into four measured phases — queue, dispatch, execution,
completion — via `TaskEvent::Completed` and `mesut_core::TaskTiming` (see
"Latency phases" below); the queue-inclusive total is still reported alongside
them, but nothing schedules on it. Callback panics are isolated; callbacks must
remain nonblocking and run in reverse registration order so internal cleanup
precedes user callbacks.

Runtime cancellation aborts Tokio futures. Compute and blocking closures must
check their cancellation token to stop promptly; running closures remain counted
until they exit. Queue depth counts both queued and running tasks, and configured
admission limits are checked under the same lock used to register tasks.

Submission now goes through a coordination layer (`mesut-runtime::coordination`)
that owns a priority-ordered admission queue instead of dispatching directly to
executors. `RuntimeConfig::with_scheduling` selects `Fifo`, `Priority`, or
`LoadAware` (picks the candidate whose target executor currently has the
fewest active tasks) dispatch order. `RuntimeConfig::with_backpressure` selects
`Accept`, `Queue`, `Reject`, `Delay` (wait for capacity under a threshold), or
`Shed` (evict a lower-priority queued task to admit a higher-priority one).
Per-work and runtime-level timeouts (`RuntimeConfig::with_task_timeout`,
`Work::with_timeout`) are enforced both at admission and while queued, failing
with `TaskError::Timeout`. `MesuT::scope`/`submit_scoped`/`join_scope` give
structured concurrency: a scope waits for all children, a child error or a
dropped scope cancels siblings, and cancellation propagates from parent scopes
to children.

`crates/mesut/tests/load.rs` exercises this under sustained mixed load
(bounded in-flight across all three adapters), priority reordering of queued
work, and `Shed` evicting low-priority queued work to admit high-priority
work.

## Latency phases

Every task carries a `mesut_core::TaskTiming` handle (created at admission,
or caller-attached via `Work::with_timing` for tooling that wants to read the
breakdown directly). At completion the runtime reports four measured phases:

- **queue** — submission until the dispatch decision (includes waiting for
  admission under backpressure);
- **dispatch** — the dispatch decision until execution begins, covering the
  executor handoff and executor-internal queueing;
- **execution** — the job's own run, from `Started` to job return;
- **completion** — job return until runtime bookkeeping and callbacks finish.

`TaskEvent::Completed` carries them as `Option<LatencyPhases>` (`None` when a
task is cancelled or fails before it ever starts). The old behavior — one
wall-clock number that mixed queue wait into "completion duration" — is gone;
the queue-inclusive total is still emitted next to the phases for continuity.

Where the platform exposes it (Linux `CLOCK_THREAD_CPUTIME_ID`), synchronous
compute/blocking jobs also record the thread CPU time they consumed, exposed
as `TaskTiming::job_cpu_ns`. Async futures deliberately report no CPU figure:
between awaits the worker runs unrelated tasks, so any per-future attribution
would be fiction. Process-wide byte counters (`/proc/self/io`) are likewise
not attributed per task — with parallel workers they cannot be attributed
honestly — and the runtime does not pretend otherwise.

## Adaptive scheduling

`Work::with_label("name")` marks a task as a recurring, named workload.
`mesut-scheduler::WorkloadObserver` keeps a bounded rolling window (default
16, `RuntimeConfig::with_adaptive_window`) of observed **execution** durations
per label — the job's own run time, never the queue-inclusive total, so the
learned estimate is a service-time estimate that does not grow just because
the system is busy. `RuntimeConfig::with_scheduling(SchedulingMode::Adaptive)`
uses that history at dispatch time: within the same priority tier, it prefers
the queued candidate with the shorter historical mean service time for its
label (bucketed on a log2 scale so one-off outliers don't dominate); unlabeled
work, or a label with no history yet, is scheduled FIFO among itself and never
starved or favored by the unknown duration. Priority ordering always wins over
duration — a `Critical` task with a long history still runs before a `Low`
task with a short one.

Shortest-service-time-first can hold long work behind any number of fresh
short tasks. `RuntimeConfig::with_adaptive_aging` (default 250ms) bounds that:
a candidate whose queue wait exceeds the threshold is promoted to FIFO order
ahead of fresh work in its tier, so long work waits roughly the threshold plus
the drain of other aged work — never indefinitely. `None` disables aging.
`MesuT::workload_profile(label)` exposes what has been learned so far.

This intentionally does not attempt to guess a task's `WorkKind`: `Work::new`
still requires an explicit, mandatory kind (used for sync/async validation).
Measurements improve ordering within the explicit kind; they do not replace
it. `crates/mesut/tests/load.rs` verifies the ordering end-to-end under real
load, including the aging rescue. See `docs/scheduler-evidence.md` and
`docs/scheduler-bench-results.json` for the measured comparison of FIFO,
Priority, LoadAware, and Adaptive across eight workload scenarios — including
the exit-gate result that Adaptive cuts bimodal short-job p50 by ~65% against
both FIFO and LoadAware while rescuing starved long work at the aging
threshold and staying within tail budgets everywhere else.

The historical end-to-end reports do not establish production readiness; see
`docs/benchmark-methodology.md` and `docs/benchmark-results.json` for
overhead-oriented throughput numbers (still predating the service-time change
— see that file for why).

## Shutdown lifecycle

`MesuT::shutdown()` closes admission, requests cancellation, and drains
cooperatively within `RuntimeConfig::with_shutdown_grace` (default 30s;
`shutdown_with_grace` overrides per call, `None` waits unboundedly). If the
grace expires with work still active, the runtime enters
`ShutdownState::ForceTerminated`: queued work is finished with
`TaskError::Shutdown`, dispatchable handles are cancelled, and tracked
bookkeeping is cleared — late completions from closures that ignored
cancellation update counters but cannot resurrect tracked state. Threads
cannot be killed, so an uncooperative closure may still be running on an
executor thread afterward; it is simply no longer the runtime's problem. A
clean drain ends in `ShutdownState::Terminated`. `MesuT::shutdown_state()`
and `RuntimeState.shutdown` report where the runtime is: `Running`,
`Draining`, `Terminated`, or `ForceTerminated`.

## Removed rather than fixed

Two more pieces turned out to be the same "plausible but disconnected" shape
as the CPU/IO adaptive-routing code retired earlier, and were removed instead
of left as silent no-ops:

- `TokioExecutorConfig::worker_threads` was stored but never read by
  anything. This adapter spawns onto the caller's ambient Tokio runtime
  (`tokio::task::spawn`), so its worker-thread count is controlled by
  whichever `#[tokio::main]`/`Runtime::Builder` started that runtime, not by
  Mesut — spinning up a second, separately-configured runtime just to honor
  this setting would be a larger and likely worse design (two runtimes
  competing for OS threads) than the library not offering the knob.
- `mesut-scheduler::PriorityQueue` was a complete, tested, four-tier priority
  queue that nothing used: `mesut-runtime::coordination` does its own
  priority-ordered admission and dispatch, and needs to re-evaluate every
  pending task on every dispatch decision (cancellation, deadlines,
  executor availability, and for `LoadAware`/`Adaptive` modes, a live score)
  in a way a static tiered queue can't represent. Unlike the removed CPU/IO
  code, `PriorityQueue` never claimed capability it didn't have — it was
  correct and simply superseded. Retired instead of kept as a second,
  unused priority mechanism alongside the real one.

## Observability

`MesuT::runtime_state()` returns an `mesut_scheduler::RuntimeState` snapshot:
cumulative `total_submitted`/`total_completed`/`total_failed`/`total_cancelled`
counters (every path that terminates a task updates these, including
pre-admission rejections and `Shed` evictions), the `shutdown` lifecycle
state, point-in-time `queued_tasks`/`in_flight_tasks` for the runtime as a
whole, and `executor_stats` keyed by executor ID. Per executor,
`active_tasks`/`queue_depth` are read fresh from the executor at snapshot
time; `completed_tasks`/`failed_tasks`/`total_wall_ms`/`busy_ms` accumulate
for the runtime's lifetime. `busy_ms` is the sum of measured execution time
of an executor's finished tasks — against a known worker count it yields
utilisation, which is how the scheduler benchmark computes it. Per-task CPU
time exists where attributable (synchronous jobs, via `TaskTiming::job_cpu_ns`)
and is deliberately absent where it is not (async futures);
`ExecutorStats` has no CPU field for the same reason.
`crates/mesut/tests/observability.rs` exercises this against a real mix of
successes, execution failures, a pre-admission rejection, and a cancellation.

## Pipelines

`MesuT::execute_pipeline(pipeline).await` runs a stage as soon as its declared
dependencies complete, up to `RuntimeConfig::with_pipeline_concurrency` stages
at once (default 1, which is effectively sequential); independent branches run
in parallel above that. Completion order is nondeterministic when stages run
in parallel. It returns outputs keyed by stage ID. Run
`cargo run -p mesut --example pipeline` for a three-stage async → compute → blocking
example. Each stage supplies `with_work(...)` or `with_factory(...)`; factories
receive only declared prerequisite outputs, shared through `Arc<Vec<u8>>`.
Factories must return promptly and put expensive work in the returned job.
Use `PipelineBuilder::with_stage` to build pipelines from fully configured stages.

Validation rejects cycles, missing/repeated dependencies, duplicate stage IDs,
inconsistent stage order, and mismatched IDs. Execution checks missing work and
declared work kinds before submitting any stage. Factories are validated when
invoked. Among ready stages, insertion order breaks ties. Stage IDs replace the
work IDs so runtime events and cancellation use the same identifiers.

A stage error stops the pipeline; later stages are not submitted, and no partial
output map is returned. Outputs remain in memory until the pipeline completes.
Clone `pipeline.cancellation_token()` before moving the pipeline into execution
to cancel it externally. Cancellation is checked between stages and at 10ms
intervals while awaiting submission/results. Dropping the execution future also
requests cancellation of its active stage. Running closures still require
cooperation and may outlive the pipeline future. Execution requires a running
Tokio runtime with timers enabled.

## End-to-end example

`cargo run -p mesut --example file_bundle` puts pipelines, parallel branches,
adaptive scheduling, and observability together against real (if synthetic)
data: it generates small and large files in a temp directory, reads each with
a real async file read, hashes its real contents with a real per-byte
compute loop (FNV-1a — a content fingerprint for the demo, not a security
hash), writes a manifest from a real blocking file write, then prints
`runtime_state()` and the learned `hash-small`/`hash-large` duration
profiles. The two labels typically differ by more than an order of
magnitude, which is the situation `SchedulingMode::Adaptive` is for.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
# mesut
