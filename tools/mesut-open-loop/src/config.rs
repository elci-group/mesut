//! Run configuration. Field names follow MESUT-AVP-01 WS1: arrival_rate,
//! duration, warmup/cooldown, service/priority/deadline/work-kind
//! distributions, scheduler_mode, adaptive_aging, worker_count, queue_limit,
//! backpressure_mode.

use mesut_benchkit::{ArrivalProcess, DeadlineDist, PriorityDist, ServiceDist, WorkKindDist};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerMode {
    Fifo,
    Priority,
    LoadAware,
    Adaptive,
}

impl SchedulerMode {
    pub fn name(self) -> &'static str {
        match self {
            SchedulerMode::Fifo => "fifo",
            SchedulerMode::Priority => "priority",
            SchedulerMode::LoadAware => "loadaware",
            SchedulerMode::Adaptive => "adaptive",
        }
    }

    pub fn scheduling(self) -> mesut::SchedulingMode {
        match self {
            SchedulerMode::Fifo => mesut::SchedulingMode::Fifo,
            SchedulerMode::Priority => mesut::SchedulingMode::Priority,
            SchedulerMode::LoadAware => mesut::SchedulingMode::LoadAware,
            SchedulerMode::Adaptive => mesut::SchedulingMode::Adaptive,
        }
    }

    pub const ALL: [SchedulerMode; 4] = [
        SchedulerMode::Fifo,
        SchedulerMode::Priority,
        SchedulerMode::LoadAware,
        SchedulerMode::Adaptive,
    ];
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ArrivalConfig {
    #[serde(flatten)]
    pub process: ArrivalProcess,
    /// Arrival rate used by fixed/poisson (arrivals per second). Also the
    /// nominal rate a matrix sweep scales.
    pub arrival_rate: f64,
}

impl Default for ArrivalConfig {
    fn default() -> Self {
        Self {
            process: ArrivalProcess::Poisson { rate: 100.0 },
            arrival_rate: 100.0,
        }
    }
}

/// Cancellation probe: after `after_s` in the measured window, submit
/// `count` long cooperative tasks, let them start, cancel them, and record
/// cancellation latency.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CancellationProbe {
    pub count: usize,
    pub service_s: f64,
    pub after_s: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RunConfig {
    pub name: String,
    pub seed: u64,
    #[serde(flatten)]
    pub arrival: ArrivalConfig,
    pub duration: f64,
    #[serde(default)]
    pub warmup_duration: f64,
    #[serde(default = "default_cooldown")]
    pub cooldown_duration: f64,
    pub service_distribution: ServiceDist,
    #[serde(default)]
    pub priority_distribution: PriorityDist,
    #[serde(default)]
    pub deadline_distribution: DeadlineDist,
    #[serde(default)]
    pub work_kind_distribution: WorkKindDist,
    pub scheduler_mode: SchedulerMode,
    /// Aging threshold in ms; null disables aging.
    #[serde(default = "default_aging")]
    pub adaptive_aging: Option<f64>,
    #[serde(default = "default_window")]
    pub adaptive_window: usize,
    pub worker_count: usize,
    pub queue_limit: usize,
    #[serde(default = "default_backpressure")]
    pub backpressure_mode: String,
    /// Maximum tasks dispatched to executors at once.
    #[serde(default = "default_in_flight")]
    pub max_in_flight: usize,
    #[serde(default)]
    pub cancellation_probe: Option<CancellationProbe>,
    #[serde(default = "default_monitor")]
    pub monitor_interval: f64,
    /// Tasks with queue wait above this are counted as starvation incidents.
    #[serde(default = "default_starvation_ms")]
    pub starvation_threshold: f64,
}

fn default_cooldown() -> f64 {
    5.0
}
fn default_aging() -> Option<f64> {
    Some(250.0)
}
fn default_window() -> usize {
    16
}
fn default_backpressure() -> String {
    "queue".into()
}
fn default_in_flight() -> usize {
    64
}
fn default_monitor() -> f64 {
    100.0
}
fn default_starvation_ms() -> f64 {
    250.0
}

impl RunConfig {
    pub fn aging_duration(&self) -> Option<Duration> {
        self.adaptive_aging.map(|ms| Duration::from_secs_f64(ms / 1e3))
    }

    pub fn parse_backpressure(&self) -> mesut::BackpressurePolicy {
        match self.backpressure_mode.as_str() {
            "accept" => mesut::BackpressurePolicy::Accept,
            "delay" => mesut::BackpressurePolicy::Delay,
            "reject" => mesut::BackpressurePolicy::Reject,
            "shed" => mesut::BackpressurePolicy::Shed,
            _ => mesut::BackpressurePolicy::Queue,
        }
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("read {}: {e}", path.display()))?;
        serde_json::from_str(&text).map_err(|e| format!("parse {}: {e}", path.display()))
    }

    /// Scale every rate in the arrival process by `factor` for capacity
    /// sweeps. Fixed/poisson scale `arrival_rate`; alternating scales both
    /// phase rates; bursty scales the inner rate.
    pub fn scale_arrival_rate(&mut self, factor: f64) {
        self.arrival.arrival_rate *= factor;
        match &mut self.arrival.process {
            mesut_benchkit::ArrivalProcess::Fixed { rate } | {
                let _ = rate;
            } => {}
            _ => {}
        }
        scale_process(&mut self.arrival.process, factor);
    }
}

fn scale_process(process: &mut mesut_benchkit::ArrivalProcess, factor: f64) {
    match process {
        mesut_benchkit::ArrivalProcess::Fixed { rate }
        | mesut_benchkit::ArrivalProcess::Poisson { rate } => *rate *= factor,
        mesut_benchkit::ArrivalProcess::Bursty { inner_rate, .. } => *inner_rate *= factor,
        mesut_benchkit::ArrivalProcess::Alternating { low_rate, high_rate, .. } => {
            *low_rate *= factor;
            *high_rate *= factor;
        }
    }
}

/// Standard workload presets from WS1. Returned as JSON so they round-trip
/// through the same config file format as bespoke runs.
pub fn preset(name: &str) -> Result<serde_json::Value, String> {
    let poisson = |rate: f64| serde_json::json!({ "kind": "poisson", "rate": rate });
    let v: serde_json::Value = match name {
        "homogeneous-short" => serde_json::json!({
            "name": "homogeneous-short",
            "arrival": { "kind": "poisson", "arrival_rate": 300.0, "rate": 300.0 },
            "service_distribution": { "kind": "constant", "mean_us": 3000.0 },
            "duration": 20.0, "warmup_duration": 5.0,
        }),
        "bimodal" => serde_json::json!({
            "name": "bimodal",
            "arrival": { "kind": "poisson", "arrival_rate": 250.0, "rate": 250.0 },
            "service_distribution": { "kind": "mixture", "components": [
                { "weight": 80.0, "label": "short", "kind": "constant", "mean_us": 5000.0 },
                { "weight": 20.0, "label": "long", "kind": "constant", "mean_us": 40000.0 },
            ]},
            "duration": 20.0, "warmup_duration": 5.0,
        }),
        "heavy-tail" => serde_json::json!({
            "name": "heavy-tail",
            "arrival": { "kind": "poisson", "arrival_rate": 200.0, "rate": 200.0 },
            "service_distribution": { "kind": "pareto", "xm_us": 500.0, "alpha": 1.4 },
            "duration": 20.0, "warmup_duration": 5.0,
        }),
        "lognormal" => serde_json::json!({
            "name": "lognormal",
            "arrival": { "kind": "poisson", "arrival_rate": 200.0, "rate": 200.0 },
            "service_distribution": { "kind": "lognormal", "mean_us": 5000.0, "sigma": 1.2 },
            "duration": 20.0, "warmup_duration": 5.0,
        }),
        "adversarial-95-4-1" => serde_json::json!({
            "name": "adversarial-95-4-1",
            "arrival": { "kind": "poisson", "arrival_rate": 400.0, "rate": 400.0 },
            "service_distribution": { "kind": "mixture", "components": [
                { "weight": 95.0, "label": "short", "kind": "constant", "mean_us": 2000.0 },
                { "weight": 4.0, "label": "medium", "kind": "constant", "mean_us": 20000.0 },
                { "weight": 1.0, "label": "long", "kind": "constant", "mean_us": 200000.0 },
            ]},
            "duration": 30.0, "warmup_duration": 5.0,
        }),
        "spiky" => serde_json::json!({
            "name": "spiky",
            "arrival": { "kind": "fixed", "arrival_rate": 200.0, "rate": 200.0 },
            "service_distribution": { "kind": "spiky", "base_us": 2000.0, "spike_us": 50000.0, "period_s": 2.0, "spike_fraction": 0.1 },
            "duration": 20.0, "warmup_duration": 5.0,
        }),
        "priority-mix" => serde_json::json!({
            "name": "priority-mix",
            "arrival": { "kind": "poisson", "arrival_rate": 250.0, "rate": 250.0 },
            "service_distribution": { "kind": "lognormal", "mean_us": 5000.0, "sigma": 1.0 },
            "priority_distribution": { "low": 2.0, "normal": 7.0, "high": 1.0, "critical": 0.1 },
            "duration": 20.0, "warmup_duration": 5.0,
        }),
        "deadline-heavy" => serde_json::json!({
            "name": "deadline-heavy",
            "arrival": { "kind": "poisson", "arrival_rate": 200.0, "rate": 200.0 },
            "service_distribution": { "kind": "mixture", "components": [
                { "weight": 75.0, "label": "short", "kind": "constant", "mean_us": 5000.0 },
                { "weight": 25.0, "label": "long", "kind": "constant", "mean_us": 40000.0 },
            ]},
            "deadline_distribution": { "kind": "fixed", "sla_ms": 200.0 },
            "duration": 20.0, "warmup_duration": 5.0,
        }),
        "bursty" => serde_json::json!({
            "name": "bursty",
            "arrival": { "kind": "bursty", "arrival_rate": 150.0, "burst_size": 60, "inner_rate": 500.0, "gap_s": 1.0 },
            "service_distribution": { "kind": "constant", "mean_us": 3000.0 },
            "duration": 20.0, "warmup_duration": 5.0,
        }),
        "alternating" => serde_json::json!({
            "name": "alternating",
            "arrival": { "kind": "alternating", "arrival_rate": 300.0, "low_rate": 50.0, "high_rate": 600.0, "phase_s": 4.0 },
            "service_distribution": { "kind": "constant", "mean_us": 5000.0 },
            "duration": 30.0, "warmup_duration": 5.0,
        }),
        "mixed-cpu-io" => serde_json::json!({
            "name": "mixed-cpu-io",
            "arrival": { "kind": "poisson", "arrival_rate": 300.0, "rate": 300.0 },
            "service_distribution": { "kind": "lognormal", "mean_us": 5000.0, "sigma": 0.8 },
            "work_kind_distribution": { "compute": 6.0, "io": 3.0, "blocking": 1.0 },
            "duration": 20.0, "warmup_duration": 5.0,
        }),
        "steady-overload" => serde_json::json!({
            "name": "steady-overload",
            "arrival": { "kind": "poisson", "arrival_rate": 400.0, "rate": 400.0 },
            "service_distribution": { "kind": "constant", "mean_us": 5000.0 },
            "duration": 30.0, "warmup_duration": 5.0,
            "backpressure_mode": "reject",
            "queue_limit": 2000,
        }),
        other => {
            // poisson(rate) and overload(rate) shortcuts.
            if let Some(rest) = other.strip_prefix("poisson:") {
                let rate: f64 = rest.parse().map_err(|_| format!("bad rate {rest}"))?;
                return Ok(serde_json::json!({
                    "name": format!("poisson-{rate}"),
                    "arrival": poisson(rate),
                    "service_distribution": { "kind": "constant", "mean_us": 5000.0 },
                    "duration": 20.0, "warmup_duration": 5.0,
                }));
            }
            return Err(format!("unknown preset: {other}"));
        }
    };
    Ok(with_defaults(v))
}

/// Fill a preset with the standard runtime settings shared by all runs.
fn with_defaults(mut v: serde_json::Value) -> serde_json::Value {
    let object = v.as_object_mut().expect("preset is an object");
    object
        .entry("seed")
        .or_insert(serde_json::json!(1));
    object
        .entry("scheduler_mode")
        .or_insert(serde_json::json!("adaptive"));
    object
        .entry("adaptive_aging")
        .or_insert(serde_json::json!(250.0));
    object
        .entry("worker_count")
        .or_insert(serde_json::json!(8));
    object
        .entry("queue_limit")
        .or_insert(serde_json::json!(10_000));
    object
        .entry("backpressure_mode")
        .or_insert(serde_json::json!("queue"));
    object
        .entry("max_in_flight")
        .or_insert(serde_json::json!(64));
    object
        .entry("cooldown_duration")
        .or_insert(serde_json::json!(5.0));
    object
        .entry("monitor_interval")
        .or_insert(serde_json::json!(100.0));
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presets_parse_into_configs() {
        for name in [
            "homogeneous-short",
            "bimodal",
            "heavy-tail",
            "lognormal",
            "adversarial-95-4-1",
            "spiky",
            "priority-mix",
            "deadline-heavy",
            "bursty",
            "alternating",
            "mixed-cpu-io",
            "steady-overload",
        ] {
            let value = preset(name).unwrap();
            let config: RunConfig = serde_json::from_value(value)
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(config.duration > config.warmup_duration, "{name}");
        }
    }

    #[test]
    fn rate_shortcut_parses() {
        let value = preset("poisson:350").unwrap();
        let config: RunConfig = serde_json::from_value(value).unwrap();
        assert!((config.arrival.arrival_rate - 350.0).abs() < f64::EPSILON);
    }
}
