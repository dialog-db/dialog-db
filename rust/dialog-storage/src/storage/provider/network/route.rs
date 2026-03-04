//! Generic route that binds an issuer to a resource pool.
//!
//! Callers only pass an address in their invocations. The [`Route`]
//! combines the held issuer with the incoming address to open the
//! resource via [`Resource::open`], then caches the connection by
//! address in a [`Pool`].

use std::convert::Infallible;
use std::hash::Hash;

use crate::resource::{Pool, Resource};
use dialog_capability::{Capability, Constraint, Effect, Provider, ProviderRoute};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::remote::RemoteInvocation;

/// A route that holds an issuer and caches connections by address.
///
/// Implements [`ProviderRoute`] with `Address = Address` so callers
/// just pass their address/credentials in [`RemoteInvocation`]s — the
/// issuer is injected internally when opening new connections.
///
/// The `Connection` type must implement `Resource<(Address, Issuer)>` so
/// that new connections can be opened from the (address, issuer) pair.
///
/// [`RemoteInvocation`]: dialog_effects::remote::RemoteInvocation
pub struct Route<Issuer, Address, Connection> {
    issuer: Issuer,
    connections: Pool<Address, Connection>,
}

impl<Issuer, Address, Connection> Route<Issuer, Address, Connection> {
    /// Create a new route with the given issuer.
    pub fn new(issuer: Issuer) -> Self {
        Self {
            issuer,
            connections: Pool::new(),
        }
    }
}

impl<Issuer, Address, Connection> ProviderRoute for Route<Issuer, Address, Connection> {
    type Address = Address;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Issuer, Address, Connection, Fx> Provider<RemoteInvocation<Fx, Address>>
    for Route<Issuer, Address, Connection>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Issuer: Clone + ConditionalSend + ConditionalSync + 'static,
    Address: Clone + Eq + Hash + ConditionalSend + ConditionalSync + 'static,
    Connection:
        Resource<(Address, Issuer), Error = Infallible> + Provider<Fx> + ConditionalSend + 'static,
{
    async fn execute(&mut self, input: RemoteInvocation<Fx, Address>) -> Fx::Output {
        let (capability, address) = input.into_parts();
        if self.connections.get_mut(&address).is_none() {
            let connection = match Connection::open(&(address.clone(), self.issuer.clone())).await {
                Ok(c) => c,
                Err(error) => match error {},
            };
            self.connections.insert(address.clone(), connection);
        }
        let connection = self.connections.get_mut(&address).unwrap();
        <Connection as Provider<Fx>>::execute(connection, capability).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Archive;
    impl dialog_capability::Attenuation for Archive {
        type Of = dialog_capability::Subject;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Get {
        key: String,
    }
    impl Effect for Get {
        type Of = Archive;
        type Output = Option<Vec<u8>>;
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestCredentials(String);

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestIssuer(String);

    struct MockConnection {
        data: HashMap<String, Vec<u8>>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<Get> for MockConnection {
        async fn execute(&mut self, effect: Capability<Get>) -> Option<Vec<u8>> {
            let get: &Get = effect.policy();
            self.data.get(&get.key).cloned()
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Resource<(TestCredentials, TestIssuer)> for MockConnection {
        type Error = Infallible;

        async fn open(_address: &(TestCredentials, TestIssuer)) -> Result<Self, Self::Error> {
            Ok(MockConnection {
                data: HashMap::new(),
            })
        }
    }

    #[dialog_common::test]
    async fn it_routes_and_caches_connections() {
        let mut route: Route<TestIssuer, TestCredentials, MockConnection> =
            Route::new(TestIssuer("issuer-1".into()));

        let creds = TestCredentials("site-a".into());

        let did: dialog_capability::Did = "did:key:z6Mk...".parse().unwrap();
        let capability = dialog_capability::Subject::from(did.clone())
            .attenuate(Archive)
            .invoke(Get {
                key: "my-key".into(),
            });

        // First invocation opens and caches the connection
        let remote = RemoteInvocation::new(capability, creds.clone());
        let result = remote.perform(&mut route).await;
        assert_eq!(result, None); // empty connection, no data

        // Insert data into the cached connection directly
        route
            .connections
            .get_mut(&creds)
            .unwrap()
            .data
            .insert("my-key".into(), b"hello".to_vec());

        // Second invocation reuses the cached connection
        let capability = dialog_capability::Subject::from(did)
            .attenuate(Archive)
            .invoke(Get {
                key: "my-key".into(),
            });
        let remote = RemoteInvocation::new(capability, creds);
        let result = remote.perform(&mut route).await;
        assert_eq!(result, Some(b"hello".to_vec()));
    }

    mod derive_router {
        use dialog_capability::{Attenuation, Did, Effect, Provider, ProviderRoute, Subject};
        use dialog_effects::remote::RemoteInvocation;
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct Storage;
        impl Attenuation for Storage {
            type Of = Subject;
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct Fetch {
            key: String,
        }
        impl Effect for Fetch {
            type Of = Storage;
            type Output = String;
        }

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct AlphaAddr(String);

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct BetaAddr(String);

        struct AlphaBackend;

        impl ProviderRoute for AlphaBackend {
            type Address = AlphaAddr;
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl Provider<RemoteInvocation<Fetch, AlphaAddr>> for AlphaBackend {
            async fn execute(&mut self, input: RemoteInvocation<Fetch, AlphaAddr>) -> String {
                let (cap, addr) = input.into_parts();
                let fetch: &Fetch = cap.policy();
                format!("alpha:{}:{}", addr.0, fetch.key)
            }
        }

        struct BetaBackend;

        impl ProviderRoute for BetaBackend {
            type Address = BetaAddr;
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl Provider<RemoteInvocation<Fetch, BetaAddr>> for BetaBackend {
            async fn execute(&mut self, input: RemoteInvocation<Fetch, BetaAddr>) -> String {
                let (cap, addr) = input.into_parts();
                let fetch: &Fetch = cap.policy();
                format!("beta:{}:{}", addr.0, fetch.key)
            }
        }

        #[derive(dialog_capability::Router)]
        struct CompositeRouter {
            alpha: AlphaBackend,
            beta: BetaBackend,
        }

        #[dialog_common::test]
        async fn it_routes_via_derive_macro() {
            let mut router = CompositeRouter {
                alpha: AlphaBackend,
                beta: BetaBackend,
            };

            let cap_a = Subject::from("did:key:zAlpha".parse::<Did>().unwrap())
                .attenuate(Storage)
                .invoke(Fetch {
                    key: "doc-1".into(),
                });
            let remote_a = RemoteInvocation::new(cap_a, AlphaAddr("site-a".into()));
            let result_a = remote_a.perform(&mut router).await;
            assert_eq!(result_a, "alpha:site-a:doc-1");

            let cap_b = Subject::from("did:key:zBeta".parse::<Did>().unwrap())
                .attenuate(Storage)
                .invoke(Fetch {
                    key: "doc-2".into(),
                });
            let remote_b = RemoteInvocation::new(cap_b, BetaAddr("site-b".into()));
            let result_b = remote_b.perform(&mut router).await;
            assert_eq!(result_b, "beta:site-b:doc-2");
        }
    }
}
