//! Capability traits and types.
//!
//! This module defines the core capability system:
//! - `Policy` - trait for types that restrict capabilities
//! - `Attenuation` - trait for types that contribute to command path
//! - `Effect` - trait for types that can be performed
//! - `Constraint` - trait that computes full chain type

use super::ability::Ability;
use super::access::Access;
use super::authority::Authority;
use super::authorization::{Authorization, AuthorizationError, Authorized};
use super::claim::Claim;
use super::constrained::Constrained;
use super::provider::Provider;
use super::selector::Selector;
use super::subject::{Did, Subject};
use crate::{ConditionalSend, ConditionalSync};

/// Trait for policy types that restrict capabilities.
///
/// `Policy` is for types that represent restrictions on what can be done
/// with a capability. Implement this for types that don't contribute to
/// the command path.
///
/// For types that contribute to the command path, implement `Attenuation`
/// instead (which provides `Policy` via blanket impl).
pub trait Policy: Sized {
    /// The capability this policy restricts.
    /// Must implement `Constraint` so we can compute the full chain type.
    type Of: Constraint;

    /// Get the attenuation segment for this type, if it contributes to the
    /// command path. Default returns None (policies don't attenuate the
    /// command path by default). Attenuation types override this to return
    /// Some(name).
    fn attenuation() -> Option<&'static str> {
        None
    }

    /// Extract this type from a capability chain. Type parameters allow
    /// compiler to infer where in the constrain chain desired policy type
    /// is.
    fn of<Head, Tail, Index>(capability: &Constrained<Head, Tail>) -> &Self
    where
        Head: Policy,
        Tail: Ability,
        Constrained<Head, Tail>: Selector<Self, Index>,
    {
        capability.select()
    }
}

/// Marker trait for policies that also constrain a command path.
///
/// Attenuation implies `Policy` via blanket impl. The `attenuation()` method
/// provides the path segment for the command path.
///
/// Note: `Effect` types automatically implement `Attenuation` via blanket impl.
pub trait Attenuation: Sized {
    /// The capability this type constrains.
    /// Must implement `Constraint` so the blanket `Policy` impl works.
    type Of: Constraint;

    /// Get the attenuation segment for this type.
    /// Attenuation types contribute to the command path.
    fn attenuation() -> &'static str {
        let full = std::any::type_name::<Self>();
        full.rsplit("::").next().unwrap_or(full)
    }
}

// Attenuation implies Policy (with attenuation override)
impl<T: Attenuation> Policy for T {
    type Of = <T as Attenuation>::Of;

    fn attenuation() -> Option<&'static str> {
        Some(<T as Attenuation>::attenuation())
    }
}

/// Trait for effect types that can be performed.
///
/// Effects are capabilities that can be invoked and therefor require their
/// output type. Implementing `Effect` automatically makes the type an
/// `Attenuation` (and thus a `Policy`) via blanket impls.
pub trait Effect: Sized {
    /// The capability this effect requires (the parent in the chain).
    type Of: Constraint;
    /// The output type produced by the invoaction of this effect when performed.
    type Output: ConditionalSend;
}

// Effect implies Attenuation
impl<T: Effect> Attenuation for T {
    type Of = <T as Effect>::Of;
}

/// Trait for deriving capability constrain chain type from an individual
/// constraints of the chain.
pub trait Constraint {
    /// The full capability chain type.
    type Capability: Ability;
}

/// For the Subject capabilty is the Subject itself.
impl Constraint for Subject {
    type Capability = Subject;
}

/// For any `Policy` or `Subject`, `Constraint::Capability` gives the full
/// `Constrained<...>` chain type, which implements the `Ability` trait.
impl<T: Policy> Constraint for T {
    type Capability = Constrained<T, <T::Of as Constraint>::Capability>;
}

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

    /// Get the command path (abilities covered by this capability).
    pub fn ability(&self) -> String
    where
        T::Capability: Ability,
    {
        self.0.command()
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

impl<C: Constraint> Capability<C>
where
    C::Capability: Ability,
    Self: Ability + Clone + ConditionalSend + 'static,
{
    /// Acquire authorization for this capability or fail if operator is not
    /// authorized.
    ///
    /// Obtains authorization, usually a delegation chain stemming from the resource
    /// owner and leading to an operating authority (provided by env). If resource
    /// matches operator DID, authorization is self-issued by the operator;
    /// otherwise it is claimed via `Access` API through `env`.
    ///
    /// This method allows consumers to fail early before invocation if
    /// authorization for a desired capability cannot be obtained.
    pub async fn acquire<Env>(
        self,
        env: &Env,
    ) -> Result<Authorized<Self, Env::Authorization<Self>>, Env::Error>
    where
        Env: Access + Authority + ConditionalSync,
        Env::Error: From<AuthorizationError>,
    {
        let authorization = if env.did() == self.subject() {
            // Self-issue: we own the resource
            Env::Authorization::<Self>::issue(self.clone(), env).map_err(Env::Error::from)?
        } else {
            // Need delegation chain
            let claim = Claim::new(self.clone(), env.did().clone());
            env.claim(claim).await?
        };
        Ok(Authorized::new(self, authorization))
    }
}

/// Implementation for effect capabilities.
///
/// When a Capability wraps an Effect, we can perform it directly in an
/// environment that provides unauthorized effects to be performed.
impl<Fx: Effect + Constraint> Capability<Fx> {
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

    fn command(&self) -> String {
        self.0.command()
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
    use super::*;
    use serde::{Deserialize, Serialize};

    // Test types for capability chains

    /// A root attenuation (attaches to Subject)
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Archive;

    impl Attenuation for Archive {
        type Of = Subject;
    }

    /// A policy that restricts Archive (no command path contribution)
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Catalog {
        name: String,
    }

    impl Policy for Catalog {
        type Of = Archive;
        fn attenuation() -> Option<&'static str> {
            None // Policy, not attenuation
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

    /// An attenuation under Storage (contributes to command path)
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
    fn it_returns_root_command_for_subject() {
        let subject = Subject::from("did:key:zSpace");
        assert_eq!(subject.subject(), "did:key:zSpace");
        assert_eq!(subject.command(), "/");
    }

    #[test]
    fn it_adds_attenuation_to_command_path() {
        let cap = Subject::from("did:key:zSpace").attenuate(Archive);

        assert_eq!(cap.subject(), "did:key:zSpace");
        assert_eq!(cap.ability(), "/archive");
    }

    #[test]
    fn it_does_not_add_policy_to_command_path() {
        let cap = Subject::from("did:key:zSpace")
            .attenuate(Archive)
            .attenuate(Catalog {
                name: "blobs".into(),
            });

        assert_eq!(cap.subject(), "did:key:zSpace");
        // Catalog is a Policy, not Attenuation, so command stays /archive
        assert_eq!(cap.ability(), "/archive");
        assert_eq!(Catalog::of(&cap).name, "blobs");
    }

    #[test]
    fn it_adds_effect_to_command_path() {
        let cap: Capability<Get> = Subject::from("did:key:zSpace")
            .attenuate(Archive)
            .attenuate(Catalog {
                name: "blobs".into(),
            })
            .invoke(Get {
                digest: vec![1, 2, 3],
            });

        assert_eq!(cap.subject(), "did:key:zSpace");
        assert_eq!(Catalog::of(&cap).name, "blobs");
        // Effect adds /get to the path
        assert_eq!(cap.ability(), "/archive/get");
        assert_eq!(Get::of(&cap).digest, vec![1, 2, 3]);
    }

    #[test]
    fn it_chains_multiple_attenuations() {
        let cap: Capability<Lookup> = Subject::from("did:key:zSpace")
            .attenuate(Storage)
            .attenuate(Store {
                name: "index".into(),
            })
            .invoke(Lookup {
                key: b"hello".to_vec(),
            });

        assert_eq!(cap.subject(), "did:key:zSpace");
        // Storage -> Store -> Lookup all contribute
        assert_eq!(cap.ability(), "/storage/store/lookup");
        assert_eq!(Store::of(&cap).name, "index");
        assert_eq!(Lookup::of(&cap).key, b"hello".to_vec());
    }

    #[test]
    fn it_extracts_policies_from_chain() {
        let cap: Capability<Get> = Subject::from("did:key:zSpace")
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
