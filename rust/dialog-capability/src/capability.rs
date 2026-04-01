use crate::access::{Authorization, AuthorizeError, Protocol};
use crate::authority::{Identify, Sign};
use crate::fork::Fork;
use crate::site::{Site, SiteAddress};
use crate::storage::{Get, List};
use crate::{
    Ability, Constrained, Constraint, Did, Effect, Policy, PolicyBuilder, Provider, Selector,
};
use dialog_common::{ConditionalSend, ConditionalSync};
use std::fmt::{Debug, Formatter};

/// Capability chain — wraps a fully-typed constraint chain.
///
/// `Capability<T>` carries the chain from `Subject` through attenuations,
/// policies, and effects down to `T`. Use `.perform(&env)` to execute
/// effect capabilities locally.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound(deserialize = ""))]
pub struct Capability<T: Constraint> {
    can: T::Capability,
}

impl<T: Constraint> Clone for Capability<T>
where
    T::Capability: Clone,
{
    fn clone(&self) -> Self {
        Self {
            can: self.can.clone(),
        }
    }
}

impl<T: Constraint> Debug for Capability<T>
where
    T::Capability: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Capability")
            .field("can", &self.can)
            .finish()
    }
}

impl<T: Constraint> Capability<T> {
    /// Create a new Capability wrapping the capability chain.
    pub fn new(capability: T::Capability) -> Self {
        Self { can: capability }
    }

    /// Get the inner capability chain.
    pub fn into_inner(self) -> T::Capability {
        self.can
    }

    /// Attenuate this capability with another policy/attenuation.
    pub fn attenuate<U>(self, value: U) -> Capability<U>
    where
        U: Policy<Of = T>,
        T::Capability: Ability,
    {
        Capability {
            can: Constrained {
                constraint: value,
                capability: self.can,
            },
        }
    }

    /// Creates an invocation of the effect derived from this capability.
    pub fn invoke<Fx>(self, fx: Fx) -> Capability<Fx>
    where
        Fx: Effect<Of = T>,
        T::Capability: Ability,
    {
        Capability {
            can: Constrained {
                constraint: fx,
                capability: self.can,
            },
        }
    }

    /// Get the subject DID from the capability chain.
    pub fn subject(&self) -> &Did
    where
        T::Capability: Ability,
    {
        self.can.subject()
    }

    /// Get the ability path (e.g., `/storage/get`, `/memory/publish`).
    pub fn ability(&self) -> String
    where
        T::Capability: Ability,
    {
        self.can.ability()
    }
}

impl<T: Policy + Constraint> Capability<T> {
    /// Extract a policy or ability from this chain.
    pub fn policy<U, Index>(&self) -> &U
    where
        T::Capability: Selector<U, Index>,
    {
        self.can.select()
    }
}

/// Perform — only for effect capabilities.
impl<Fx: Effect> Capability<Fx> {
    /// Perform the invocation directly against a provider.
    pub async fn perform<Env>(self, env: &Env) -> Fx::Output
    where
        Env: Provider<Fx>,
    {
        env.execute(self).await
    }

    /// Authorize this capability for a specific site's authorization format.
    ///
    /// Delegates to the site's [`Protocol::authorize`](access::Protocol::authorize).
    pub async fn acquire<S, Env>(
        self,
        env: &Env,
    ) -> Result<Authorization<Fx, S::Protocol>, AuthorizeError>
    where
        Fx: Clone + ConditionalSend + 'static,
        S: Site,
        S::Protocol: Protocol,
        Self: Ability + ConditionalSend + ConditionalSync,
        Env: Provider<Identify> + Provider<Sign> + Provider<List> + Provider<Get> + ConditionalSync,
    {
        S::Protocol::authorize(env, self).await
    }

    /// Attach a site address to this capability for remote execution.
    ///
    /// Returns a [`Fork`] that can be authorized (`.acquire()`) or
    /// authorized and executed in one step (`.perform()`).
    ///
    /// The site type is inferred from the address via [`SiteAddress`].
    pub fn fork<A: SiteAddress>(self, address: &A) -> Fork<A::Site, Fx>
    where
        Fx::Of: Constraint,
    {
        Fork::new(self, address.clone())
    }
}

impl<T: Constraint> Ability for Capability<T> {
    fn subject(&self) -> &Did {
        self.can.subject()
    }

    fn ability(&self) -> String {
        self.can.ability()
    }

    fn constrain(&self, builder: &mut impl PolicyBuilder) {
        self.can.constrain(builder)
    }
}

impl<T: Constraint> std::ops::Deref for Capability<T> {
    type Target = T::Capability;

    fn deref(&self) -> &Self::Target {
        &self.can
    }
}

impl<T: Constraint> AsRef<T::Capability> for Capability<T> {
    fn as_ref(&self) -> &T::Capability {
        &self.can
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use crate::{Attenuation, Subject};
    use serde::{Deserialize, Serialize};

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
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, crate::Claim)]
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
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, crate::Claim)]
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
}
