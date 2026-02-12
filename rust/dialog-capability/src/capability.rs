use crate::{
    Ability, Access, Authorized, Claim, Constrained, Constraint, Did, Effect, Policy,
    PolicyBuilder, Provider, Selector,
};
use dialog_common::ConditionalSend;
use dialog_varsig::Principal;

/// Newtype wrapper for describing a capability chain from the constraint type.
/// It enables defining convenience methods for working with that capability.
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct Capability<T: Constraint>(pub T::Capability);

impl<T: Constraint> Capability<T> {
    /// Create a new Capability wrapping the capability chain.
    pub fn new(capability: T::Capability) -> Self {
        Self(capability)
    }

    /// Get the inner capability chain.
    pub fn into_inner(self) -> T::Capability {
        self.0
    }

    /// Attenuate this capability with another policy/attinuation.
    pub fn attenuate<U>(self, value: U) -> Capability<U>
    where
        U: Policy<Of = T>,
        T::Capability: Ability,
    {
        Capability(Constrained {
            constraint: value,
            capability: self.0,
        })
    }

    /// Get the subject DID from the capability chain.
    pub fn subject(&self) -> &Did
    where
        T::Capability: Ability,
    {
        self.0.subject()
    }

    /// Get the ability path (e.g., `/storage/get`, `/memory/publish`).
    pub fn ability(&self) -> String
    where
        T::Capability: Ability,
    {
        self.0.ability()
    }

    /// Creates an invocation of the effect derived from this capability.
    /// Note: It is no difference from `attenuate` execpt it ensures that
    /// what is invoked is an effect as opposed to an ability.
    pub fn invoke<Fx>(self, fx: Fx) -> Capability<Fx>
    where
        Fx: Effect<Of = T>,
        T::Capability: Ability,
    {
        Capability(Constrained {
            constraint: fx,
            capability: self.0,
        })
    }

    /// Acquire authorization for this capability from an access provider.
    ///
    /// This method uses the `Access` trait to find authorization proofs for
    /// the capability claim, returning an `Authorized` bundle that pairs the
    /// capability with its authorization proof.
    pub async fn acquire<A: Access + Principal>(
        self,
        access: &mut A,
    ) -> Result<Authorized<T, A::Authorization>, A::Error>
    where
        Self: ConditionalSend + Clone + 'static,
    {
        let capability = self.clone();
        let authorization = access
            .claim(Claim {
                capability,
                audience: access.did(),
            })
            .await?;

        Ok(Authorized::new(self.clone(), authorization))
    }
}

impl<T: Policy + Constraint> Capability<T> {
    /// Extract a policy or ability from this chain.
    pub fn policy<U, Index>(&self) -> &U
    where
        T::Capability: Selector<U, Index>,
    {
        self.0.select()
    }
}

/// Implementation for effect capabilities.
///
/// When a Capability wraps an Effect, we can perform it directly in an
/// environment that provides unauthorized effects to be performed.
impl<Fx: Effect> Capability<Fx> {
    /// Perform the invocation directly without authorization verification.
    /// For operations that require authorization, use `acquire` first.
    pub async fn perform<Env>(self, env: &mut Env) -> Fx::Output
    where
        Env: Provider<Fx>,
    {
        env.execute(self).await
    }
}

impl<T: Constraint> Ability for Capability<T>
where
    T::Capability: Ability,
{
    fn subject(&self) -> &Did {
        self.0.subject()
    }

    fn ability(&self) -> String {
        self.0.ability()
    }

    fn constrain(&self, builder: &mut impl PolicyBuilder) {
        self.0.constrain(builder)
    }
}

impl<T: Constraint> std::ops::Deref for Capability<T> {
    type Target = T::Capability;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Constraint> AsRef<T::Capability> for Capability<T> {
    fn as_ref(&self) -> &T::Capability {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use crate::{Attenuation, Subject};
    use serde::{Deserialize, Serialize};

    // Test types for capability chains

    /// A root attenuation (attaches to Subject)
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Archive;

    impl Attenuation for Archive {
        type Of = Subject;
    }

    /// A policy that restricts Archive (no ability path contribution)
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Catalog {
        name: String,
    }

    impl Policy for Catalog {
        type Of = Archive;
        fn attenuation() -> Option<&'static str> {
            None
        }
    }

    /// An effect that operates on Catalog
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Get {
        digest: Vec<u8>,
    }

    impl Effect for Get {
        type Of = Catalog;
        type Output = Option<Vec<u8>>;
    }

    /// Another root attenuation for testing different chains
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Storage;

    impl Attenuation for Storage {
        type Of = Subject;
    }

    /// An attenuation under Storage (contributes to ability path)
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Store {
        name: String,
    }

    impl Attenuation for Store {
        type Of = Storage;
    }

    /// An effect under Store
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Lookup {
        key: Vec<u8>,
    }

    impl Effect for Lookup {
        type Of = Store;
        type Output = Option<Vec<u8>>;
    }

    #[test]
    fn it_returns_root_ability_for_subject() {
        let subject = Subject::from(did!("key:zSpace"));
        assert_eq!(subject.subject(), &did!("key:zSpace"));
        assert_eq!(subject.ability(), "/");
    }

    #[test]
    fn it_adds_attenuation_to_ability_path() {
        let cap = Subject::from(did!("key:zSpace")).attenuate(Archive);

        assert_eq!(cap.subject(), &did!("key:zSpace"));
        assert_eq!(cap.ability(), "/archive");
    }

    #[test]
    fn it_does_not_add_policy_to_ability_path() {
        let cap = Subject::from(did!("key:zSpace"))
            .attenuate(Archive)
            .attenuate(Catalog {
                name: "blobs".into(),
            });

        assert_eq!(cap.subject(), &did!("key:zSpace"));
        // Catalog is a Policy, not Attenuation, so ability stays /archive
        assert_eq!(cap.ability(), "/archive");
        assert_eq!(Catalog::of(&cap).name, "blobs");
    }

    #[test]
    fn it_adds_effect_to_ability_path() {
        let cap: Capability<Get> = Subject::from(did!("key:zSpace"))
            .attenuate(Archive)
            .attenuate(Catalog {
                name: "blobs".into(),
            })
            .invoke(Get {
                digest: vec![1, 2, 3],
            });

        assert_eq!(cap.subject(), &did!("key:zSpace"));
        assert_eq!(Catalog::of(&cap).name, "blobs");
        // Effect adds /get to the path
        assert_eq!(cap.ability(), "/archive/get");
        assert_eq!(Get::of(&cap).digest, vec![1, 2, 3]);
    }

    #[test]
    fn it_chains_multiple_attenuations() {
        let cap: Capability<Lookup> = Subject::from(did!("key:zSpace"))
            .attenuate(Storage)
            .attenuate(Store {
                name: "index".into(),
            })
            .invoke(Lookup {
                key: b"hello".to_vec(),
            });

        assert_eq!(cap.subject(), &did!("key:zSpace"));
        // Storage -> Store -> Lookup all contribute
        assert_eq!(cap.ability(), "/storage/store/lookup");
        assert_eq!(Store::of(&cap).name, "index");
        assert_eq!(Lookup::of(&cap).key, b"hello".to_vec());
    }

    #[test]
    fn it_extracts_policies_from_chain() {
        let cap: Capability<Get> = Subject::from(did!("key:zSpace"))
            .attenuate(Archive)
            .attenuate(Catalog {
                name: "blobs".into(),
            })
            .invoke(Get {
                digest: vec![1, 2, 3],
            });

        // Extract various policies from the chain
        let get: &Get = cap.policy();
        assert_eq!(get.digest, vec![1, 2, 3]);

        let catalog: &Catalog = cap.policy();
        assert_eq!(catalog.name, "blobs");

        let archive: &Archive = cap.policy();
        assert_eq!(archive, &Archive);
    }

    #[cfg(feature = "ucan")]
    mod parameters_tests {
        use super::*;
        use crate::ucan::parameters;
        use ipld_core::ipld::Ipld;

        #[test]
        fn it_collects_parameters_from_chain() {
            let cap: Capability<Get> = Subject::from(did!("key:zSpace"))
                .attenuate(Archive)
                .attenuate(Catalog {
                    name: "blobs".into(),
                })
                .invoke(Get {
                    digest: vec![1, 2, 3],
                });

            let params = parameters(&cap);

            // Catalog should contribute "name" parameter
            assert_eq!(params.get("name"), Some(&Ipld::String("blobs".into())));
            // Get should contribute "digest" parameter (serialized as list of integers)
            assert_eq!(
                params.get("digest"),
                Some(&Ipld::List(vec![
                    Ipld::Integer(1),
                    Ipld::Integer(2),
                    Ipld::Integer(3)
                ]))
            );
        }

        #[test]
        fn it_collects_parameters_from_attenuations() {
            let cap: Capability<Lookup> = Subject::from(did!("key:zSpace"))
                .attenuate(Storage)
                .attenuate(Store {
                    name: "index".into(),
                })
                .invoke(Lookup {
                    key: b"hello".to_vec(),
                });

            let params = parameters(&cap);

            // Store should contribute "name" parameter
            assert_eq!(params.get("name"), Some(&Ipld::String("index".into())));
            // Lookup should contribute "key" parameter (serialized as list of integers)
            let hello_bytes: Vec<Ipld> =
                b"hello".iter().map(|&b| Ipld::Integer(b as i128)).collect();
            assert_eq!(params.get("key"), Some(&Ipld::List(hello_bytes)));
        }
    }
}
