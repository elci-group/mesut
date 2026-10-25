# Scheduler evidence

This document records the measured comparison of Mesut's four scheduling
modes — `Fifo`, `Priority`, `LoadAware`, `Adaptive` — across eight defined
workload scenarios, and the exit gate that decides whether Adaptive earns its
complexity. Raw results: `docs/scheduler-bench-results.json`. Reproduce with:

```sh
cargo run --release --manifest-path tools/mesut-sched-bench/Cargo.toml -- --repeat 3
```

## What is measured

Every task carries a caller-attached `mesut_core::TaskTiming`, so per-task
latency comes from the runtime's own phase instrumentation, not sampling:

- **queue**: submission → dispatch decision (includes backpressure waiting);
- **dispatch**: dispatch decision → execution start (executor handoff plus
  executor-internal queueing);
- **execution**: job start → job return (the service time);
- **completion**: job return → bookkeeping and callbacks finished.

The learned history for `Adaptive` is execution time only. Metrics per run:
p50/p95/p99 of total and per-phase latency, per-label breakdowns, throughput,
deadline miss rate (per-job SLAs), Jain fairness over per-label mean slowdown
(latency / service time), executor utilisation (`busy_ms` against the compute
worker budget), and cancellation latency (8 cooperative 5-second jobs
cancelled 30ms after submission).

## Scenarios

| Scenario | Shape |
| --- | --- |
| homogeneous-short | 240 identical 3ms compute jobs, 4 in flight |
| homogeneous-long | 48 identical 30ms compute jobs, 4 in flight |
| bimodal | 160× 5ms + 40× 40ms compute jobs interleaved (every 5th long), 4 in flight, 250ms aging |
| burst | 3 bursts of 60× 3ms jobs separated by 250ms quiet gaps |
| mixed-cpu-io | 100× 5ms compute + 100× 5ms timer I/O alternating, 16 in flight |
| deadline-heavy | 30× 40ms jobs + 90× 5ms jobs with a 200ms SLA, 4 in flight, 250ms aging |
| priority-inversion | 20 low-priority 30ms jobs queued first, then 10 critical 2ms jobs, 1 in flight |
| starvation-stress | 60× 2ms jobs, one 200ms job, then 540× 2ms jobs, 2 in flight, 100ms aging |
| cancellation-latency | 8 cooperative 5s jobs cancelled 30ms after submission |

Worker counts per runtime match each scenario's in-flight limit so
executor-internal queueing does not mask the coordination signal being
measured. Identical label history is seeded for every mode before the
measured phase; only Adaptive consumes it. Mode order alternates per
repetition; each cell reports the median of 3 repetitions of per-run metrics.

## Exit gate

Adaptive must demonstrate a statistically meaningful win for at least one
defined workload without unacceptable starvation or tail-latency regression
elsewhere. Operationalized as three checks over the medians:

1. **Win** — bimodal short-job p50 ≤ 0.70× the same figure for *both* FIFO
   and LoadAware (≥30% improvement against each).
2. **No starvation** — in starvation-stress, the long job's maximum queue
   wait ≤ aging threshold + one dispatch quantum + 150ms slack (i.e. aging
   rescues it near the threshold instead of letting it wait out the short
   backlog).
3. **No tail regression elsewhere** — on every scenario except the win
   scenario, Adaptive's overall p99 ≤ 1.15× the better of FIFO/LoadAware.

**Recorded result (2026-10-25, all three checks PASS):**

1. Adaptive short p50 **102.0ms** vs FIFO 287.2ms / LoadAware 286.8ms —
   **ratio 0.356** (64% better than both; limit 0.70).
2. Starvation-stress long max queue wait **100.2ms** against a 100ms aging
   threshold (limit 258ms). Under pure shortest-service-time-first the same
   job would wait for the full 600-job short backlog.
3. Worst p99 ratio elsewhere: **deadline-heavy ×1.040** (limit 1.150).

## Full medians (ms unless noted)

| Scenario | Mode | p50 | p99 | throughput | Jain slowdown | util | SLA miss |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| homogeneous-short | fifo / loadaware / adaptive | 95.3 / 93.2 / 93.8 | 183.4 / 183.0 / 184.5 | 1273 / 1279 / 1277 | — | 1.0 | — |
| homogeneous-long | fifo / loadaware / adaptive | 180.5 / 180.7 / 180.5 | 360.8 / 360.9 / 360.7 | 132 / 132 / 132 | — | 1.0 | — |
| bimodal | fifo | 297.3 | 593.7 | 319.5 | 0.64 | 1.03 | — |
| bimodal | loadaware | 296.9 | 592.8 | 319.3 | 0.64 | 1.03 | — |
| bimodal | **adaptive** | **127.1** | 602.8 | 329.8 | **0.90** | 1.06 | — |
| burst | adaptive | 25.0 | 46.5 | 326.4 | — | 0.25 | — |
| mixed-cpu-io | adaptive | 38.3 | 72.8 | 2662.7 | 1.00 | 0.44 | — |
| deadline-heavy | fifo | 340.7 | 416.1 | 286.4 | 0.56 | 1.09 | **1.00** |
| deadline-heavy | **adaptive** | **111.6** | 432.6 | 276.5 | 0.80 | 1.03 | **0.00** |
| priority-inversion | fifo | 451.5 | 623.5 | 48.1 | 0.53 | 0.50 | — |
| priority-inversion | **adaptive** | 171.4 | 622.7 | 48.2 | 0.92 | 0.50 | — |
| starvation-stress | fifo / adaptive | 425.0 / 424.8 | 728.2 / 730.0 | 811.7 / 810.2 | 0.51 / 0.51 | 1.08 | — |
| cancellation-latency | fifo / adaptive (cancel p50 ms) | 4.1 / 4.2 | — | — | — | — | — |

Priority and LoadAware track FIFO closely wherever executor balancing has no
signal (single executor class); both are included in the JSON in full.

## Findings

- **The win is real and large where the workload has mixed service times.**
  Bimodal short-job p50 drops 57–65% (queue p50 282.5ms → 122.1ms) with
  execution-phase p50 constant at 5.0ms — the gain is pure queue-order, which
  is exactly the thing a scheduler controls. Jain fairness improves 0.64 →
  0.90 because short jobs no longer sit behind long ones. Deadline-heavy
  tells the same story as an SLA: FIFO misses 100% of short-job deadlines,
  Adaptive misses 0%.
- **Aging bounds the cost.** The bimodal long class trades tail for the
  short class's win (long max queue 563ms ≈ the short backlog it correctly
  waits behind; every long completes). Where long work could starve
  indefinitely (starvation-stress), the 100ms aging threshold rescued it at
  100.2ms median with short-job p50 unchanged versus FIFO. Aging is the
  difference between shortest-job-first and a scheduler you can run in
  production.
- **Priority semantics are preserved.** In priority-inversion, Adaptive
  matches Priority (critical p50 171ms vs FIFO's 452ms): duration ordering
  only ever breaks ties inside a priority tier.
- **No regression where there is no signal.** Homogeneous, burst, and mixed
  workloads are statistically indistinguishable across modes; cancellation
  latency is flat (~4ms) across all four.

## Caveats

- Single local host (12 logical CPUs, x86_64 Linux), no CPU pinning or
  governor control; background load was present during the recorded run
  (load average ~6–11). Absolute numbers are inflated by roughly 1.5–2×
  relative to a quiet host; mode-to-mode comparisons within a run are the
  evidence, and medians-of-3 with alternating mode order damp (not remove)
  drift. Busy-wait service times overshoot their targets under contention,
  which affects all modes equally.
- Closed-batch scenarios, not sustained open-loop traffic: every task is
  submitted at t≈0 and the queue drains to empty. The aging mechanism's
  behavior under arrivals that outpace drain forever is bounded by
  construction (aged work runs FIFO among itself) and probed by
  starvation-stress, but not measured under adversarial arrival patterns.
- Compute service is a busy-wait loop and I/O service is a Tokio timer —
  real file/network I/O would shift the mixed-cpu-io numbers.
- `busy_ms`-based utilisation can slightly exceed 1.0 when the host is
  oversubscribed (measured execution wall time of a preempted job exceeds
  its worker share); treat it as a sanity signal, not a precision metric.
- Three repetitions: no confidence intervals are inferred. The gate margins
  (ratio 0.356 vs 0.70 limit; 100.2ms vs 258ms bound; ×1.040 vs 1.150) are
  wide relative to the run-to-run spread visible in the raw `runs` array.

## Why the other modes are kept

FIFO is the baseline any mode must beat and the simplest thing that can
possibly work. Priority is the correct mode whenever priority tiers carry
real meaning — no duration history can override them. LoadAware is the only
mode that balances across executor classes (it differs from Priority in the
mixed-cpu-io cell) and is the honest fallback if a future workload shows the
Adaptive service-time estimate misbehaving. If a future change regresses the
gate, the recorded procedure above is what "simplify the scheduler" is
measured against.
