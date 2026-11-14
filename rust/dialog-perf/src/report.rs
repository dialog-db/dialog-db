//! What one scenario run measured, serializable for baselines.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One scenario's measurements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    /// The scenario's name.
    pub scenario: String,
    /// The size it ran at.
    pub size: usize,
    /// The cargo profile the harness was built under: instruction counts
    /// compare only within one.
    pub profile: String,
    /// What the measured phase produced (rows read, facts committed): the
    /// workload's own record, which a baseline must match exactly.
    pub outcome: BTreeMap<String, u64>,
    /// The blocks moved and engine steps taken by the measured phase.
    pub counters: BTreeMap<String, u64>,
    /// Instructions the measured phase retired under callgrind; absent
    /// when the run was not under valgrind. Reported, not gated: about a
    /// quarter of them are the system allocator's, whose cost per call
    /// depends on the heap's layout, and the layout depends on the
    /// random seeds the process draws (hash keys among them). One binary
    /// measures up to 8% apart between runs (`commit-succeed`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<u64>,
    /// The instructions less every call into the allocator from outside
    /// it, callees included (see `sweep::allocator`): the engine's own
    /// work, which repeats between runs to a few tenths of a percent.
    /// Gated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions_outside_allocator: Option<u64>,
    /// Calls the measured phase made into the allocator from outside it,
    /// by kind (`alloc`, `realloc`, `free`). They vary between runs by a
    /// few dozen, with the order of hash maps. Gated with that slack.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub allocations: BTreeMap<String, u64>,
}

impl Report {
    /// The profile this binary was built under.
    pub fn profile() -> &'static str {
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
    }
}
