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
    /// when the run was not under valgrind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<u64>,
    /// Whether the instruction count is known to vary between runs
    /// past the gate's allowance (see [`Spec::volatile`]), in which
    /// case the gate holds the counters alone.
    ///
    /// [`Spec::volatile`]: crate::scenario::Spec::volatile
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub volatile: bool,
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
