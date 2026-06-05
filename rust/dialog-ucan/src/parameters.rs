//! IPLD parameter collection and UCAN argument conversion.

use dialog_capability::{Ability, PolicyBuilder};
use dialog_ucan_core::promise::Promised;
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

use dialog_ucan_core::delegation::policy::predicate::Predicate;
use dialog_ucan_core::delegation::policy::selector::filter::Filter;
use dialog_ucan_core::delegation::policy::selector::select::Select;

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
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_capability::{Subject, did};
    use dialog_common::Blake3Hash;
    use dialog_effects::archive::{Archive, Catalog, Get, Put};

    #[dialog_common::test]
    fn it_returns_no_parameters_for_bare_subject() {
        let cap = Subject::from(did!("key:z6MkTest"));
        let params = parameters(&cap);
        assert!(params.is_empty());
    }

    #[dialog_common::test]
    fn it_collects_parameters_from_archive_catalog_chain() {
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("index"));
        let params = parameters(&cap);
        assert_eq!(params.get("catalog"), Some(&Ipld::String("index".into())));
    }

    #[dialog_common::test]
    fn it_collects_parameters_from_archive_get_invocation() {
        let digest = Blake3Hash::hash(b"my-content");
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("data"))
            .invoke(Get::new(digest));
        let params = parameters(&cap);
        assert_eq!(params.get("catalog"), Some(&Ipld::String("data".into())));
        assert!(
            params.contains_key("digest"),
            "should contain digest parameter"
        );
    }

    #[dialog_common::test]
    fn it_returns_empty_policy_for_empty_parameters() {
        let policy = parameters_to_policy(Parameters::new());
        assert!(policy.is_empty());
    }

    #[dialog_common::test]
    fn it_produces_equality_constraints_for_each_parameter() {
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("data"));
        let policy = parameters_to_policy(parameters(&cap));

        assert_eq!(policy.len(), 1);
        assert_eq!(
            policy[0],
            Predicate::Equal(
                Select::new(vec![Filter::Field("catalog".into())]),
                Ipld::String("data".into())
            )
        );
    }

    #[dialog_common::test]
    fn it_produces_multiple_constraints_for_chain_with_payload() {
        let content = b"hello world";
        let digest = Blake3Hash::hash(content);
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(digest, content.to_vec()));
        let policy = parameters_to_policy(parameters(&cap));

        // Should have constraints for catalog, digest, and content (via checksum)
        assert!(
            policy.len() >= 2,
            "expected at least 2 constraints, got {}",
            policy.len()
        );

        let has_catalog = policy.iter().any(|p| {
            matches!(
                p,
                Predicate::Equal(sel, Ipld::String(v))
                    if sel == &Select::new(vec![Filter::Field("catalog".into())])
                    && v == "index"
            )
        });
        assert!(has_catalog, "should have catalog equality constraint");

        let has_digest = policy.iter().any(|p| {
            matches!(
                p,
                Predicate::Equal(sel, Ipld::Bytes(_))
                    if sel == &Select::new(vec![Filter::Field("digest".into())])
            )
        });
        assert!(has_digest, "should have digest equality constraint");
    }

    #[dialog_common::test]
    fn it_converts_parameters_to_args() {
        let mut params = Parameters::new();
        params.insert("name".into(), Ipld::String("test".into()));
        params.insert("count".into(), Ipld::Integer(42));

        let args = parameters_to_args(params);
        assert_eq!(args.get("name"), Some(&Promised::String("test".into())));
        assert_eq!(args.get("count"), Some(&Promised::Integer(42)));
    }
}
