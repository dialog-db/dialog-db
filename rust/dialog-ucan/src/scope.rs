//! Capability-derived scope for UCAN delegation and invocation.

use dialog_capability::{Ability, Capability, Constraint, Effect, Policy, Subject};
use dialog_ucan_core::command::Command;
use dialog_ucan_core::delegation::policy::predicate::Predicate;
use dialog_ucan_core::delegation::policy::selector::filter::Filter;
use dialog_ucan_core::delegation::policy::selector::select::Select;
use dialog_ucan_core::promise::Promised;
use dialog_ucan_core::subject::Subject as UcanSubject;
use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;

use super::parameters::parameters;

/// UCAN invocation arguments.
pub type Args = BTreeMap<String, Promised>;

/// Parameters extracted from a capability chain.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct Parameters(pub BTreeMap<String, Ipld>);

impl Parameters {
    /// Get the inner map.
    pub fn as_map(&self) -> &BTreeMap<String, Ipld> {
        &self.0
    }

    /// Convert to delegation policy predicates (equality constraints).
    pub fn policy(&self) -> Vec<Predicate> {
        self.into()
    }

    /// Convert to invocation arguments.
    pub fn args(&self) -> Args {
        self.into()
    }
}

impl From<&Parameters> for Vec<Predicate> {
    fn from(parameters: &Parameters) -> Self {
        parameters
            .0
            .iter()
            .map(|(key, value)| {
                Predicate::Equal(Select::new(vec![Filter::Field(key.clone())]), value.clone())
            })
            .collect()
    }
}

impl From<&Parameters> for Args {
    fn from(parameters: &Parameters) -> Self {
        parameters
            .0
            .iter()
            .map(|(k, v)| (k.clone(), ipld_to_promised(v.clone())))
            .collect()
    }
}

/// Scope extracted from a capability chain for UCAN operations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Scope {
    /// The subject (specific DID or `Any` for powerline).
    pub subject: UcanSubject,
    /// The command.
    pub command: Command,
    /// Parameters from the capability's policy chain.
    pub parameters: Parameters,
}

impl dialog_capability::access::Scope for Scope {
    fn subject(&self) -> &dialog_varsig::Did {
        use dialog_ucan_core::subject::Subject as UcanSubject;
        match &self.subject {
            UcanSubject::Specific(did) => did,
            UcanSubject::Any => {
                static ANY: std::sync::LazyLock<dialog_varsig::Did> =
                    std::sync::LazyLock::new(|| {
                        dialog_capability::ANY_SUBJECT.parse().expect("valid DID")
                    });
                &ANY
            }
        }
    }
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
        let ability = capability.ability();
        let subject_did = capability.subject();

        let subject = if Subject::from(subject_did.clone()).is_any() {
            UcanSubject::Any
        } else {
            UcanSubject::Specific(subject_did.clone())
        };

        Self {
            subject,
            command: ability_to_command(&ability),
            parameters: Parameters(parameters(capability)),
        }
    }
}

impl Scope {
    /// Build a scope from a delegation chain.
    ///
    /// Extracts subject, command, and an empty parameter set from the chain.
    pub fn from_chain(chain: &dialog_ucan_core::DelegationChain) -> Self {
        let subject = chain
            .subject()
            .map(|did| UcanSubject::Specific(did.clone()))
            .unwrap_or(UcanSubject::Any);

        let ability = chain.ability();
        let command = ability_to_command(&ability);

        Self {
            subject,
            command,
            parameters: Parameters::default(),
        }
    }

    /// Build a scope from an effect capability, projecting through Claim.
    ///
    /// Unlike `Scope::from`, this projects effect fields through their
    /// [`Claim`](dialog_capability::Claim) type, so payload fields like
    /// `content` become `checksum` in the scope parameters.
    pub fn invoke<Fx>(capability: &Capability<Fx>) -> Self
    where
        Fx: Effect + Clone,
        Capability<Fx>: Ability,
    {
        let ability = capability.ability();
        let subject_did = capability.subject();

        let subject = if Subject::from(subject_did.clone()).is_any() {
            UcanSubject::Any
        } else {
            UcanSubject::Specific(subject_did.clone())
        };

        // Collect parameters from the parent chain (excluding the leaf effect)
        let chain: &<Fx as Constraint>::Capability = capability.as_ref();
        let mut params = parameters(&chain.capability);

        // Add claim-projected parameters for the effect
        let claim = Policy::of(capability).clone().claim();
        if let Ok(Ipld::Map(map)) = ipld_core::serde::to_ipld(&claim) {
            params.extend(map);
        }

        Self {
            subject,
            command: ability_to_command(&ability),
            parameters: Parameters(params),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Subject, did};
    use dialog_common::Blake3Hash;
    use dialog_effects::archive::{Archive, Catalog, Get};

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
    fn scope_from_archive_catalog() {
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("index"));
        let scope = Scope::from(&cap);

        assert_eq!(scope.command, Command::parse("/archive").unwrap());

        let policy = scope.policy();
        assert_eq!(policy.len(), 1);
        assert_eq!(
            policy[0],
            Predicate::Equal(
                Select::new(vec![Filter::Field("catalog".into())]),
                Ipld::String("index".into())
            )
        );

        let args = scope.args();
        assert_eq!(args.get("catalog"), Some(&Promised::String("index".into())));
    }

    #[test]
    fn scope_from_archive_get() {
        let digest = Blake3Hash::hash(b"hello");
        let cap = Subject::from(did!("key:z6MkTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest.clone()));
        let scope = Scope::from(&cap);

        assert_eq!(scope.command, Command::parse("/archive/get").unwrap());

        let policy = scope.policy();
        assert!(
            policy.contains(&Predicate::Equal(
                Select::new(vec![Filter::Field("catalog".into())]),
                Ipld::String("index".into())
            )),
            "policy should contain catalog=index constraint"
        );
        assert!(
            policy
                .iter()
                .any(|p| matches!(p, Predicate::Equal(sel, Ipld::Bytes(_))
                    if sel == &Select::new(vec![Filter::Field("digest".into())])
                )),
            "policy should contain digest constraint"
        );

        let args = scope.args();
        assert_eq!(args.get("catalog"), Some(&Promised::String("index".into())));
        assert!(args.contains_key("digest"), "args should contain digest");
    }

    #[test]
    fn parameters_to_policy() {
        let mut map = BTreeMap::new();
        map.insert("catalog".into(), Ipld::String("index".into()));
        let params = Parameters(map);
        let policy = params.policy();

        assert_eq!(policy.len(), 1);
        assert_eq!(
            policy[0],
            Predicate::Equal(
                Select::new(vec![Filter::Field("catalog".into())]),
                Ipld::String("index".into())
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
