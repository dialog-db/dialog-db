use crate::{
    Ability, Constrained, Constraint, Did, Effect, Policy, PolicyBuilder, Provider, Selector,
    Subject,
    credential::{Authorize, AuthorizeError, Credential, Profile},
    site::{Local, Site},
};
use dialog_common::{ConditionalSend, ConditionalSync};
use std::fmt::{Debug, Formatter};

/// Capability chain with an optional site parameter.
///
/// When `At = Local` (default), this behaves like a bare capability.
/// When `At` is a remote site, `.acquire()` produces site-specific invocations.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound(deserialize = ""))]
pub struct Capability<T: Constraint, At: Site = Local> {
    can: T::Capability,
    at: At,
}

impl<T: Constraint, At: Site> Clone for Capability<T, At>
where
    T::Capability: Clone,
    At: Clone,
{
    fn clone(&self) -> Self {
        Self {
            can: self.can.clone(),
            at: self.at.clone(),
        }
    }
}

impl<T: Constraint, At: Site> Debug for Capability<T, At>
where
    T::Capability: Debug,
    At: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Capability")
            .field("can", &self.can)
            .field("at", &self.at)
            .finish()
    }
}

impl<T: Constraint, At: Site> Capability<T, At> {
    /// Get the inner capability chain.
    pub fn into_inner(self) -> T::Capability {
        self.can
    }

    /// Get a reference to the site.
    pub fn site(&self) -> &At {
        &self.at
    }

    /// Consume and return both parts.
    pub fn into_parts(self) -> (Capability<T>, At) {
        (
            Capability {
                can: self.can,
                at: Local,
            },
            self.at,
        )
    }

    /// Attenuate this capability with another policy/attenuation.
    pub fn attenuate<U>(self, value: U) -> Capability<U, At>
    where
        U: Policy<Of = T>,
        T::Capability: Ability,
    {
        Capability {
            can: Constrained {
                constraint: value,
                capability: self.can,
            },
            at: self.at,
        }
    }

    /// Creates an invocation of the effect derived from this capability.
    pub fn invoke<Fx>(self, fx: Fx) -> Capability<Fx, At>
    where
        Fx: Effect<Of = T>,
        T::Capability: Ability,
    {
        Capability {
            can: Constrained {
                constraint: fx,
                capability: self.can,
            },
            at: self.at,
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

impl<T: Constraint> Capability<T, Local> {
    /// Create a new Capability wrapping the capability chain.
    pub fn new(capability: T::Capability) -> Self {
        Self {
            can: capability,
            at: Local,
        }
    }

    /// Attach a site to this capability.
    pub fn at<S: Site>(self, site: &S) -> Capability<T, S> {
        Capability {
            can: self.can,
            at: site.clone(),
        }
    }
}

impl<T: Policy + Constraint, At: Site> Capability<T, At> {
    /// Extract a policy or ability from this chain.
    pub fn policy<U, Index>(&self) -> &U
    where
        T::Capability: Selector<U, Index>,
    {
        self.can.select()
    }
}

/// Implementation for effect capabilities — perform directly (local only).
impl<Fx: Effect> Capability<Fx, Local> {
    /// Perform the invocation directly without authorization verification.
    /// For operations that require authorization, use `acquire` first.
    pub async fn perform<Env>(self, env: &Env) -> Fx::Output
    where
        Env: Provider<Fx>,
    {
        env.execute(self).await
    }
}

/// acquire — only for effects, produces site-specific invocation.
impl<Fx: Effect, At: Site> Capability<Fx, At> {
    /// Authorize the capability and produce a site-specific invocation.
    pub async fn acquire<Env>(self, env: &Env) -> Result<At::Invocation<Fx>, AuthorizeError>
    where
        Env: Provider<Authorize<Fx, At::Access>> + ConditionalSync,
        Capability<Fx>: ConditionalSend,
        At::Access: ConditionalSend,
        Authorize<Fx, At::Access>: ConditionalSend + 'static,
    {
        let subject = self.can.subject().clone();
        let access = self.at.access();
        let capability = Capability {
            can: self.can,
            at: Local,
        };
        let authorize = Subject::from(subject)
            .attenuate(Credential)
            .attenuate(Profile::default())
            .invoke(Authorize { capability, access });
        let authorized =
            <Env as Provider<Authorize<Fx, At::Access>>>::execute(env, authorize).await?;
        Ok(authorized.into())
    }
}

impl<T: Constraint, At: Site> Ability for Capability<T, At> {
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

impl<T: Constraint, At: Site> std::ops::Deref for Capability<T, At> {
    type Target = T::Capability;

    fn deref(&self) -> &Self::Target {
        &self.can
    }
}

impl<T: Constraint, At: Site> AsRef<T::Capability> for Capability<T, At> {
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
