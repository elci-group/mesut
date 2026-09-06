# End-to-end benchmark methodology

`benchmark-results.json` predates the adaptive-routing change (`Work::label`,
`SchedulingMode::Adaptive`, the `WorkloadObserver`-backed coordination
changes). `recorded-source-sha256.txt` has been advanced to the current tree
so future drift is still caught, but it no longer matches these particular
results — treat `benchmark-results.json` as historical until it is
re-measured.

A same-day attempt to refresh it produced `mesut-rayon` throughput with much
higher run-to-run variance than the recorded run (27k-64k tasks/sec across 8
repetitions, vs. a tight cluster around 59-66k previously). This was traced to
host contention, not the code change: pinning the new
`SchedulingCandidate::expected_duration` field to `None` (functionally
reverting the change on this workload, which submits no labeled work) and
rerunning reproduced the same 36k-62k range, and `direct-rayon` — untouched by
the change — showed an 18% spread within a single batch. The host was running
a desktop session (browser, other agent sessions) with no CPU pinning or
governor control, which the methodology below already flags as a source of
noise. Rerun on a quiet host before trusting new numbers.

To reproduce, run from the repository root:

```sh
cargo run --release --offline --manifest-path tools/mesut-bench/Cargo.toml -- --iterations 2000 --concurrency 32 --threads 4 --rounds 20000 --warmup 256 --repeat 3
```

Drop `--offline` if dependencies are not cached. Do not add `--locked`: this
crate's lockfile is regenerated whenever the root workspace's path
dependencies change, so pinning it rejects perfectly valid rebuilds. The standalone crate has its own
`[workspace]`, manifest, lockfile, and target directory; it does not change the
root workspace or Mesut's dependencies. JSON goes to stdout; Cargo diagnostics
go to stderr. The executable rejects builds with debug assertions enabled;
use Cargo's standard release profile without environment/profile overrides.
This guard catches ordinary debug builds, but does not independently prove an
arbitrary custom profile's optimization settings. Cargo builds occur outside
measurement, and each child executes the same binary as its parent.
`--help` lists options; `--case` selects one case, still with repetitions. The
internal `--child --case ...` mode produces one sample for the parent.
Invalid options,
zero values, failed submissions, missing/incorrect outputs, and task failures
produce a nonzero exit. Each child (warmup plus measured work) has a cooperative 300-second Tokio timeout
(not an external watchdog for deadlocked worker threads).

## Work and completion

Six cases compare direct Tokio with Mesut's Tokio adapter, direct Rayon with
Mesut's Rayon adapter, and direct/Mesut mixed execution. Each case runs in a
fresh child process for each repetition. Case order reverses on alternate
repetitions to reduce consistent ordering bias. Each task ID has the same
input and output in its matched cases:

- Async: a one-millisecond Tokio timer followed by an eight-byte task ID.
  This is simulated waiting, not network or disk I/O; timer granularity can
  make observed waits longer than one millisecond.
- Compute: `rounds` dependent wrapping multiply/add/xor steps, seeded by task
  ID, returning eight bytes with `black_box` preventing discarded computation.
- Mixed: even task IDs run async work; odd IDs run compute work. With an odd
  iteration count, the async group has one extra task.

Concurrency is bounded by batches (default 32, allowed 1–64), with a barrier
after each batch. A slow task delays admission of the next batch. This is a
closed-loop batch benchmark, not sustained open-loop traffic or a saturation
test. There are at most `concurrency` workload jobs admitted at once, plus
bounded helper tasks. Iterations count total completed jobs per case, not
repetitions of the entire benchmark. `--repeat` defaults to 3 and accepts 3–100;
`--warmup` defaults to 256 and accepts 64–1,000,000 completed jobs per child.
`--rounds` remains CPU arithmetic steps per task, not measurement repetitions.

Schema-v2 JSON retains every raw sample in `results` with a one-based
`repetition`, and reports per-case `medians`. Each summary field is the median
of the corresponding run-level measurements (even counts average the two
middle values). Latency summaries are medians of each run's p50/p95/p99, not
percentiles of pooled tasks. RSS deltas are computed within each child first,
then summarized; a difference of median RSS readings need not equal the median
delta. Any missing RSS reading makes that field's median null. No confidence
intervals or statistical significance are inferred from three repetitions.

Both paths use an explicitly configured Tokio runtime with `threads` workers.
Each compute case also has its own Rayon pool with the same thread count;
mixed cases therefore have two pools, not a shared total worker budget. Mesut's
Tokio adapter uses the current runtime, so the actual Tokio builder controls
its worker count. Rayon config is inferred from its public constructor and its
`num_threads` field is set explicitly. Mesut uses
`RuntimeConfig::default().with_animations(false)` and required routing hints.
No tracing subscriber is installed. Direct cases construct neither MesuT nor
Mesut adapters.

Mesut submission runs in a Tokio helper task so an implementation that waits
inside `submit` cannot accidentally serialize the workload. Completion is
reported using `on_complete`, cloning the referenced result into a oneshot.
Direct work is spawned onto the target executor and sends its owned result
through a oneshot. Both paths collect through a bounded JoinSet. Thus measured
Mesut cost includes its wrapper, submission scheduling, bookkeeping, and
result clone; this is application-visible overhead, not isolated router cost.

Every completion is received, its ID checked for duplication, and its output
compared against the deterministic expected value after timing. Aggregate
checksums are also reported for easy paired comparisons. Submission acceptance
alone never counts as completion. The same execution loop runs first for
warmup, then for measurement, reusing executors and result storage. Warmup
results are validated and discarded from latency/throughput summaries.
Mesut `join().await` drains each phase before its final RSS reading;
`shutdown().await` closes the runtime after measurements and verification.
Completion callbacks remain the authority for individual results even when
`submit` returns before dispatch.

## Timing and memory

Elapsed time starts before the first batch and ends after the final batch is
collected. It includes work construction, scheduling, completion collection,
basic output decoding, batch barriers, and RSS sampling. It excludes executor
construction, buffer preparation, warmup, final runtime join, expected-output
recomputation, percentile sorting, shutdown, and JSON serialization.

Before warmup, all result/latency vectors are allocated to the larger of the
warmup and measured task counts and filled with nonzero values to touch their
pages. They are cleared and reused without capacity growth for measured work.
Completed warmup batches exercise submission, timer startup, worker execution,
callbacks, and RSS reading before the measured baseline. Setup and warmup
remain visible in separate memory fields and a raw `warmup` object. A finite
warmup reduces startup effects; it cannot prove all worker paths are resident
or that CPU frequency, allocator behavior, or RSS has fully stabilized. Increase
warmup and inspect repeated raw samples if that distinction matters.

Task latency starts before work/channel construction and ends at the worker's
completion notification. It includes helper scheduling, queueing, execution,
and Mesut cleanup preceding the callback, but excludes the collector's wakeup
delay. Percentiles use nearest rank (`ceil(p * count)`, one-based), reported in
microseconds, for all tasks and separately for async/compute subsets. Empty
subsets are JSON null. The pooled mixed p50 sits at the boundary of two very
different populations; use the separate distributions too.

RSS is Linux `/proc/self/status` `VmRSS`, in KiB, for the **entire child process**:

- `process_baseline`: before Tokio runtime and executor construction.
- `after_setup`: after executor construction, before buffer preparation/warmup.
- `before_work`: after warmup, validation, and reusable result-buffer reset.
- `after_work`: after timed work and runtime join, before verification/shutdown.
- `sampled_max`: maximum of `before_work`, batch-boundary samples, and `after_work`.
- `setup_delta`: `after_setup - process_baseline`.
- `warmup_and_buffers_delta`: `before_work - after_setup`.
- `work_delta`: `after_work - before_work`.
- `sampled_max_delta_from_before_work`: `sampled_max - before_work`.

Deltas are signed. Unavailable readings are null. Batch-boundary sampling can
miss transient peaks; `sampled_max` is not peak RSS or `VmHWM`. RSS includes
thread stacks, runtime/allocator caches, libraries, bookkeeping, benchmark
vectors, and workload data. Result storage scales with the larger of warmup
and iteration count, but is prepared before the measured baseline. A zero
warmed delta means no observed retained RSS growth, not zero workload memory.
Warmup's retained runtime/allocator memory is already in `before_work`. It is
**not per-task memory**, not allocation counts, and not proof of a leak.
No custom allocator or allocation counter is installed. Fresh processes avoid
retained allocations from earlier cases, but ASLR, demand paging, scheduling,
CPU frequency and other machine activity still affect results.

## Recorded run

`benchmark-results.json` contains unrounded stdout from the release run
described above the command: 2,000 iterations, concurrency 32, four threads,
20,000 CPU rounds, 256 warmup jobs per case, and 3 repetitions, reporting the
`medians` schema. The machine has an Intel Core 5 120U, x86_64 Linux, 12
available logical CPUs, kernel `6.18.7-76061807-generic`, and `rustc 1.98.0
(88d9e12ae 2026-08-18)`. This is a single local run without CPU pinning or
governor control; three repetitions with median reporting reduce but do not
eliminate noise, and no confidence intervals are computed. Do not treat small
differences as stable performance claims.

Mesut's adapters track their direct-executor baselines closely: `mesut-tokio`
runs at roughly 85% of `direct-tokio` throughput, `mesut-rayon` at roughly 90%
of `direct-rayon`, and `mesut-mixed` at roughly 79% of `direct-mixed`, with
higher tail latency on the async and mixed cases (Rayon dispatch overhead is
small; Tokio-side wrapping/bookkeeping is the larger share of the gap).

The local runtime is undergoing concurrent changes. Source hashes recorded
alongside the run identify the measured inputs more precisely than a dirty
repository's HEAD; rerun after runtime changes before comparing them.
