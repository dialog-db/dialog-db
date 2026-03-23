//! IPLD parameter collection and UCAN argument conversion.

use crate::{Ability, PolicyBuilder};
use dialog_ucan::promise::Promised;
use ipld_core::ipld::Ipld;
use ipld_core::serde::to_ipld;
use serde::Serialize;
use std::collections::BTreeMap;

/// IPLD-based parameter map for UCAN invocations.
pub type Parameters = BTreeMap<String, Ipld>;

/// UCAN invocation arguments (Promised values for the invocation body).
pub type Args = BTreeMap<String, Promised>;

/// Builder that collects caveats as IPLD parameters.
struct ParametersBuilder(Parameters);

impl PolicyBuilder for ParametersBuilder {
    fn push<T: Serialize>(&mut self, caveat: &T) {
        if let Ok(Ipld::Map(map)) = to_ipld(caveat) {
            self.0.extend(map);
        }
    }
}

/// Collect parameters from a capability into an IPLD map.
///
/// Iterates over all caveats in the capability chain, serializes each
/// to IPLD, and merges their fields into a single map.
pub fn parameters<T: Ability>(capability: &T) -> Parameters {
    let mut builder = ParametersBuilder(Parameters::new());
    capability.constrain(&mut builder);
    builder.0
}

/// Convert an IPLD value to a Promised value (for UCAN invocation arguments).
fn ipld_to_promised(ipld: Ipld) -> Promised {
    match ipld {
        Ipld::Null => Promised::Null,
        Ipld::Bool(b) => Promised::Bool(b),
        Ipld::Integer(i) => Promised::Integer(i),
        Ipld::Float(f) => Promised::Float(f),
        Ipld::String(s) => Promised::String(s),
        Ipld::Bytes(b) => Promised::Bytes(b),
        Ipld::Link(c) => Promised::Link(c),
        Ipld::List(l) => Promised::List(l.into_iter().map(ipld_to_promised).collect()),
        Ipld::Map(m) => Promised::Map(
            m.into_iter()
                .map(|(k, v)| (k, ipld_to_promised(v)))
                .collect(),
        ),
    }
}

/// Convert IPLD parameters to UCAN invocation arguments.
pub fn parameters_to_args(parameters: Parameters) -> Args {
    parameters
        .into_iter()
        .map(|(k, v)| (k, ipld_to_promised(v)))
        .collect()
}
