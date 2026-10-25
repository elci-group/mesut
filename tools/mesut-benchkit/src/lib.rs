//! Shared benchmark infrastructure for Mesut evidence tooling.
//!
//! Everything here is deterministic from a `u64` seed: the RNG is a local
//! xoshiro256** so runs are reproducible across hosts and architectures
//! without depending on an external RNG crate's feature flags.

pub mod arrival;
pub mod dist;
pub mod env;
pub mod rng;
pub mod stats;
pub mod workload;

pub use arrival::ArrivalProcess;
pub use dist::{DeadlineDist, PriorityDist, ServiceDist, WorkKindDist};
pub use rng::Rng;
