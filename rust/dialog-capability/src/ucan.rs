//! UCAN bridge types.
//!
//! When the `ucan` feature is enabled this module provides IPLD parameter
//! collection utilities for UCAN invocations.
//!
//! The core bridging is automatic: any type implementing [`Authority`](crate::Authority)
//! automatically satisfies `ucan::Issuer<A::Signature>` because `Authority`
//! extends `varsig::Principal + varsig::Signer<Self::Signature>`, and
//! `ucan::Issuer<S>` has a blanket impl for `Signer<S> + Principal`.

use crate::{Ability, PolicyBuilder};
use ipld_core::ipld::Ipld;
use ipld_core::serde::to_ipld;
use serde::Serialize;
use std::collections::BTreeMap;

/// IPLD-based parameter map for UCAN invocations.
pub type Parameters = BTreeMap<String, Ipld>;

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
/// This function iterates over all caveats in the capability chain,
/// serializes each to IPLD, and merges their fields into a single map.
pub fn parameters<T: Ability>(capability: &T) -> Parameters {
    let mut builder = ParametersBuilder(Parameters::new());
    capability.constrain(&mut builder);
    builder.0
}
