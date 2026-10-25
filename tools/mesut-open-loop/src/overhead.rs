//! Scheduler-overhead microbenchmarks (WS5): the cost of Mesut's own
//! orchestration vs direct Tokio/Rayon execution, plus policy-selection
//! cost and per-queued-task memory.
//!
//! Full implementation lands with the overhead evidence run; the command
//! exists so the CLI surface is complete.
pub fn cmd_overhead(_argv: &[String]) -> i32 {
    eprintln!("overhead: not yet implemented");
    2
}
