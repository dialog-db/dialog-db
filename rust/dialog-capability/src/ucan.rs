//! UCAN-specific parameter collection using IPLD.

use crate::{Ability, Parameters};
use ipld_core::ipld::Ipld;
use ipld_core::serde::to_ipld;
use serde::Serialize;
use std::collections::BTreeMap;

/// IPLD-based parameter collector for UCAN invocations.
///
/// This type implements [`Parameters`] by serializing values to IPLD format.
pub type IpldParameters = BTreeMap<String, Ipld>;

impl Parameters for IpldParameters {
    fn set<V: Serialize + ?Sized>(&mut self, key: &str, value: &V) {
        if let Ok(ipld) = to_ipld(value) {
            self.insert(key.to_string(), ipld);
        }
    }
}

/// Collect parameters from a capability into an IPLD map.
///
/// This is a convenience function for UCAN invocations.
pub fn parameters<T: Ability>(capability: &T) -> IpldParameters {
    let mut params = IpldParameters::new();
    capability.parametrize(&mut params);
    params
}
