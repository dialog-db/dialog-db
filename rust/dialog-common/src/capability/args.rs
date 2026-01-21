//! IPLD argument serialization for capability policy validation.
//!
//! This module provides the [`ToIpldArgs`] trait for serializing capability
//! chains into IPLD format for policy predicate validation.
//!
//! # Policy Validation
//!
//! UCAN delegations can contain policy predicates that restrict what operations
//! are allowed. For example:
//!
//! ```text
//! Delegation: /storage with policy [.store == "index"]
//! ```
//!
//! When claiming a capability, the policy predicates are evaluated against the
//! capability's arguments (extracted via `ToIpldArgs`).

use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;

use super::capability::Policy;
use super::{Ability, Capability, Claim, Constrained, Subject};

/// Trait for serializing capability chains into IPLD arguments.
///
/// Used by policy validation to evaluate predicates against capability
/// parameters. Each layer in the capability chain contributes its fields
/// to the merged argument map.
pub trait ToIpldArgs {
    /// Serialize this capability chain into IPLD arguments.
    ///
    /// Returns a map of field name → value for all policy constraints
    /// in the chain.
    fn to_ipld_args(&self) -> Ipld;
}

/// Subject contributes no args (it's just the DID root).
impl ToIpldArgs for Subject {
    fn to_ipld_args(&self) -> Ipld {
        Ipld::Map(BTreeMap::new())
    }
}

/// Constrained chains collect args from all layers using serde.
///
/// Each policy layer that implements `serde::Serialize` will have its
/// fields merged into the argument map.
impl<P, Of> ToIpldArgs for Constrained<P, Of>
where
    P: Policy + serde::Serialize,
    Of: Ability + ToIpldArgs,
{
    fn to_ipld_args(&self) -> Ipld {
        // Get args from parent capability
        let mut map = match self.capability.to_ipld_args() {
            Ipld::Map(m) => m,
            _ => BTreeMap::new(),
        };

        // Serialize this constraint to IPLD and merge its fields
        if let Ok(Ipld::Map(constraint_map)) = ipld_core::serde::to_ipld(&self.constraint) {
            map.extend(constraint_map);
        }

        Ipld::Map(map)
    }
}

/// Capability<T> wrapper delegates to inner.
impl<T> ToIpldArgs for Capability<T>
where
    T: super::Constraint,
    T::Capability: Ability + ToIpldArgs,
{
    fn to_ipld_args(&self) -> Ipld {
        self.0.to_ipld_args()
    }
}

/// Extension trait for `Claim` to extract IPLD arguments for policy validation.
pub trait ClaimArgsExt {
    /// Get the IPLD arguments from the capability for policy validation.
    fn args(&self) -> Ipld;
}

impl<C: ToIpldArgs> ClaimArgsExt for Claim<C> {
    fn args(&self) -> Ipld {
        self.capability.to_ipld_args()
    }
}
