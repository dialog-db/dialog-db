//! Capability-derived scope for UCAN delegation and invocation.

use crate::{Ability, Capability, Constraint, Effect, Policy, Subject};
use dialog_ucan::command::Command;
use dialog_ucan::delegation::policy::predicate::Predicate;
use dialog_ucan::subject::Subject as UcanSubject;
use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;

use super::parameters::{self, parameters_to_args, parameters_to_policy};

pub use super::parameters::Args;

/// Parameters extracted from a capability chain.
#[derive(Debug, Clone, Default)]
pub struct Parameters(pub BTreeMap<String, Ipld>);

impl Parameters {
    /// Get the inner map.
    pub fn as_map(&self) -> &BTreeMap<String, Ipld> {
        &self.0
    }

    /// Convert to delegation policy predicates (equality constraints).
    pub fn policy(&self) -> Vec<Predicate> {
        parameters_to_policy(self.0.clone())
    }

    /// Convert to invocation arguments.
    pub fn args(&self) -> Args {
        parameters_to_args(self.0.clone())
    }
}

/// Scope extracted from a capability chain for UCAN operations.
pub struct Scope {
    /// The subject (specific DID or `Any` for powerline).
    pub subject: UcanSubject,
    /// The command.
    pub command: Command,
    /// Parameters from the capability's policy chain.
    pub parameters: Parameters,
}

impl Scope {
    /// Convert parameters to delegation policy predicates.
    pub fn policy(&self) -> Vec<Predicate> {
        self.parameters.policy()
    }

    /// Convert parameters to invocation arguments.
    pub fn args(&self) -> Args {
        self.parameters.args()
    }
}

fn ucan_subject(did: &crate::Did) -> UcanSubject {
    if Subject::from(did.clone()).is_any() {
        UcanSubject::Any
    } else {
        UcanSubject::Specific(did.clone())
    }
}

fn ability_to_command(ability: &str) -> Command {
    if ability == "/" {
        Command::new(vec![])
    } else {
        Command::new(
            ability
                .trim_start_matches('/')
                .split('/')
                .map(String::from)
                .collect(),
        )
    }
}

impl<T: Ability> From<&T> for Scope {
    fn from(capability: &T) -> Self {
        Self {
            subject: ucan_subject(capability.subject()),
            command: ability_to_command(&capability.ability()),
            parameters: Parameters(parameters::parameters(capability)),
        }
    }
}

impl Scope {
    /// Build a scope from an effect capability, projecting through Claim.
    #[allow(dead_code)]
    pub(crate) fn invoke<Fx>(capability: &Capability<Fx>) -> Self
    where
        Fx: Effect + Clone,
        Capability<Fx>: Ability,
    {
        // Collect parameters from the parent chain (excluding the leaf effect)
        let chain: &<Fx as Constraint>::Capability = capability.as_ref();
        let mut params = parameters::parameters(&chain.capability);

        // Add claim-projected parameters for the effect
        let claim = Policy::of(capability).clone().claim();
        if let Ok(Ipld::Map(map)) = ipld_core::serde::to_ipld(&claim) {
            params.extend(map);
        }

        Self {
            subject: ucan_subject(capability.subject()),
            command: ability_to_command(&capability.ability()),
            parameters: Parameters(params),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Get, Storage, Store};
    use crate::{Subject, did};
    use dialog_ucan::delegation::policy::predicate::Predicate;
    use dialog_ucan::delegation::policy::selector::filter::Filter;
    use dialog_ucan::delegation::policy::selector::select::Select;
    use dialog_ucan::promise::Promised;

    #[test]
    fn scope_from_subject() {
        let cap = Subject::from(did!("key:z6MkTest"));
        let scope = Scope::from(&cap);

        assert!(matches!(scope.subject, UcanSubject::Specific(_)));
        assert!(scope.command.segments().is_empty());
        assert!(scope.parameters.0.is_empty());
        assert!(scope.policy().is_empty());
        assert!(scope.args().is_empty());
    }

    #[test]
    fn scope_from_any_subject() {
        let cap = Subject::any();
        let scope = Scope::from(&cap);
        assert!(matches!(scope.subject, UcanSubject::Any));
    }

    #[test]
    fn scope_from_storage_store() {
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Storage)
            .attenuate(Store::new("data"));
        let scope = Scope::from(&cap);

        assert_eq!(scope.command, Command::parse("/storage").unwrap());

        let policy = scope.policy();
        assert_eq!(policy.len(), 1);
        assert_eq!(
            policy[0],
            Predicate::Equal(
                Select::new(vec![Filter::Field("store".into())]),
                Ipld::String("data".into())
            )
        );

        let args = scope.args();
        assert_eq!(args.get("store"), Some(&Promised::String("data".into())));
    }

    #[test]
    fn scope_from_storage_get() {
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Storage)
            .attenuate(Store::new("data"))
            .invoke(Get::new(b"my-key"));
        let scope = Scope::from(&cap);

        assert_eq!(scope.command, Command::parse("/storage/get").unwrap());

        let policy = scope.policy();
        assert!(
            policy.contains(&Predicate::Equal(
                Select::new(vec![Filter::Field("store".into())]),
                Ipld::String("data".into())
            )),
            "policy should contain store=data constraint"
        );
        assert!(
            policy.contains(&Predicate::Equal(
                Select::new(vec![Filter::Field("key".into())]),
                Ipld::Bytes(b"my-key".to_vec())
            )),
            "policy should contain key=my-key constraint"
        );

        let args = scope.args();
        assert_eq!(args.get("store"), Some(&Promised::String("data".into())));
        assert_eq!(args.get("key"), Some(&Promised::Bytes(b"my-key".to_vec())));
    }

    #[test]
    fn parameters_to_policy() {
        let mut map = BTreeMap::new();
        map.insert("store".into(), Ipld::String("data".into()));
        let params = Parameters(map);
        let policy = params.policy();

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
    fn parameters_to_args() {
        let mut map = BTreeMap::new();
        map.insert("name".into(), Ipld::String("test".into()));
        map.insert("count".into(), Ipld::Integer(42));
        let params = Parameters(map);
        let args = params.args();

        assert_eq!(args.get("name"), Some(&Promised::String("test".into())));
        assert_eq!(args.get("count"), Some(&Promised::Integer(42)));
    }
}
