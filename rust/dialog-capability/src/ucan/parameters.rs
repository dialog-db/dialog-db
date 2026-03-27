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

use dialog_ucan::delegation::policy::predicate::Predicate;
use dialog_ucan::delegation::policy::selector::filter::Filter;
use dialog_ucan::delegation::policy::selector::select::Select;

/// Convert capability parameters to UCAN delegation policy predicates.
///
/// Each parameter becomes an equality constraint: `.{key} == value`.
pub fn parameters_to_policy(parameters: Parameters) -> Vec<Predicate> {
    parameters
        .into_iter()
        .map(|(key, value)| Predicate::Equal(Select::new(vec![Filter::Field(key)]), value))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Get, Set, Storage, Store};
    use crate::{Subject, did};

    #[test]
    fn parameters_from_empty_subject() {
        let cap = Subject::from(did!("key:z6MkTest"));
        let params = parameters(&cap);
        assert!(params.is_empty());
    }

    #[test]
    fn parameters_from_storage_store() {
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Storage)
            .attenuate(Store::new("index"));
        let params = parameters(&cap);
        assert_eq!(params.get("store"), Some(&Ipld::String("index".into())));
    }

    #[test]
    fn parameters_from_storage_get() {
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Storage)
            .attenuate(Store::new("data"))
            .invoke(Get::new(b"my-key"));
        let params = parameters(&cap);
        assert_eq!(params.get("store"), Some(&Ipld::String("data".into())));
        assert_eq!(params.get("key"), Some(&Ipld::Bytes(b"my-key".to_vec())));
    }

    #[test]
    fn parameters_to_policy_empty() {
        let policy = parameters_to_policy(Parameters::new());
        assert!(policy.is_empty());
    }

    #[test]
    fn parameters_to_policy_produces_equality_constraints() {
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Storage)
            .attenuate(Store::new("data"));
        let policy = parameters_to_policy(parameters(&cap));

        assert_eq!(policy.len(), 1);
        assert_eq!(
            policy[0],
            Predicate::Equal(
                Select::new(vec![Filter::Field("store".into())]),
                Ipld::String("data".into())
            )
        );
    }

    #[test]
    fn parameters_to_policy_multiple_constraints() {
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new(b"key1", b"val1"));
        let policy = parameters_to_policy(parameters(&cap));

        // Should have constraints for store, key, and value (via checksum)
        assert!(
            policy.len() >= 2,
            "expected at least 2 constraints, got {}",
            policy.len()
        );

        let has_store = policy.iter().any(|p| {
            matches!(
                p,
                Predicate::Equal(sel, Ipld::String(v))
                    if sel == &Select::new(vec![Filter::Field("store".into())])
                    && v == "index"
            )
        });
        assert!(has_store, "should have store equality constraint");

        let has_key = policy.iter().any(|p| {
            matches!(
                p,
                Predicate::Equal(sel, Ipld::Bytes(v))
                    if sel == &Select::new(vec![Filter::Field("key".into())])
                    && v == b"key1"
            )
        });
        assert!(has_key, "should have key equality constraint");
    }

    #[test]
    fn parameters_to_args_roundtrip() {
        let mut params = Parameters::new();
        params.insert("name".into(), Ipld::String("test".into()));
        params.insert("count".into(), Ipld::Integer(42));

        let args = parameters_to_args(params);
        assert_eq!(args.get("name"), Some(&Promised::String("test".into())));
        assert_eq!(args.get("count"), Some(&Promised::Integer(42)));
    }
}
