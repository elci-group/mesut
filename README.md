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
`Started` is emitted when execution begins. Completion duration currently includes
queue wait. Callback panics are isolated; callbacks must remain nonblocking and
run in reverse registration order so internal cleanup precedes user callbacks.

Runtime cancellation aborts Tokio futures. Compute and blocking closures must
check their cancellation token to stop promptly; running closures remain counted
until they exit. Shutdown rejects new submissions and requests cancellation,
but does not wait for running closures. Blocking workers exit after queued jobs
are drained. Queue depth counts both queued and running tasks, and configured
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

## Adaptive scheduling

`Work::with_label("name")` marks a task as a recurring, named workload.
`mesut-scheduler::WorkloadObserver` keeps a bounded rolling window (default
16, `RuntimeConfig::with_adaptive_window`) of observed wall-clock durations
per label, fed from durations the coordination layer already measures for
telemetry — there is no CPU/IO-time instrumentation, so this is duration
history only, not a resource-usage profile. `RuntimeConfig::with_scheduling
(SchedulingMode::Adaptive)` uses that history at dispatch time: within the
same priority tier, it prefers the queued candidate with the shorter
historical mean duration for its label (bucketed on a log2 scale so one-off
outliers don't dominate); unlabeled work, or a label with no history yet, is
scheduled FIFO among itself and never starved or favored by the unknown
duration. Priority ordering always wins over duration — a `Critical` task
with a long history still runs before a `Low` task with a short one.
`MesuT::workload_profile(label)` exposes what has been learned so far.

This intentionally does not attempt to guess a task's `WorkKind`: `Work::new`
still requires an explicit, mandatory kind (used for sync/async validation),
and there is no per-task CPU/IO measurement to base such a guess on. An
earlier, unused `mesut-router` module attempted CPU%/IO%-based kind
inference from data nothing ever populated; it has been removed in favor of
this duration-based scheduling signal, which the runtime actually feeds and
uses. See `crates/mesut/tests/load.rs` for an end-to-end test that builds
duration history for two labels under real load and confirms `Adaptive`
scheduling reorders queued work by it.

Tokio's worker-thread setting does not configure the caller's runtime. The
historical completion reports do not establish production readiness; see
`docs/benchmark-methodology.md` and `docs/benchmark-results.json` for
end-to-end throughput numbers (currently predating this change — see that
file for why).

## Observability

`MesuT::runtime_state()` returns an `mesut_scheduler::RuntimeState` snapshot:
cumulative `total_submitted`/`total_completed`/`total_failed`/`total_cancelled`
counters (every path that terminates a task updates these, including
pre-admission rejections and `Shed` evictions), plus `executor_stats` keyed by
executor ID. Per executor, `active_tasks`/`queue_depth` are read fresh from
the executor at snapshot time; `completed_tasks`/`failed_tasks`/
`total_wall_ms` accumulate for the runtime's lifetime. There is still no
CPU-time instrumentation, so `ExecutorStats` has no CPU field — an earlier
version did, always zero, which was more misleading than not having it.
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
