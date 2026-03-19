//! Generic route that caches connections by address.
//!
//! Callers pass an address in their invocations. The [`Route`]
//! opens a connection via [`Resource::open`], then caches it by
//! address in a [`Pool`].

use std::convert::Infallible;
use std::hash::Hash;
use std::sync::Arc;

use crate::resource::{Pool, Resource};
use dialog_capability::{Capability, Constraint, Effect, Provider, ProviderRoute};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::remote::RemoteInvocation;

/// A route that caches connections by address.
///
/// Implements [`ProviderRoute`] with `Address = Address` so callers
/// just pass their address/credentials in [`RemoteInvocation`]s.
///
/// The `Connection` type must implement `Resource<Address>` so
/// that new connections can be opened from the address.
///
/// Connections are cached in `Arc` wrappers so they can be cloned out of the
/// pool's `RwLock` before any `.await` points.
///
/// [`RemoteInvocation`]: dialog_effects::remote::RemoteInvocation
pub struct Route<Address, Connection> {
    connections: Pool<Address, Arc<Connection>>,
}

impl<Address, Connection> Route<Address, Connection> {
    /// Create a new route.
    pub fn new() -> Self {
        Self {
            connections: Pool::new(),
        }
    }
}

impl<Address, Connection> Default for Route<Address, Connection> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Address, Connection> ProviderRoute for Route<Address, Connection> {
    type Address = Address;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Address, Connection, Fx> Provider<RemoteInvocation<Fx, Address>> for Route<Address, Connection>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Address: Clone + Eq + Hash + ConditionalSend + ConditionalSync + 'static,
    Connection: Resource<Address, Error = Infallible>
        + Provider<Fx>
        + ConditionalSend
        + ConditionalSync
        + 'static,
{
    async fn execute(&self, input: RemoteInvocation<Fx, Address>) -> Fx::Output {
        let (capability, address) = input.into_parts();

        // Check the pool (read lock, dropped immediately).
        let connection = match self.connections.get(&address) {
            Some(conn) => conn,
            None => {
                // Open a new connection with no lock held during the await.
                let new_conn = match Connection::open(&address).await {
                    Ok(c) => Arc::new(c),
                    Err(error) => match error {},
                };
                // Insert into pool (write lock, dropped immediately).
                self.connections.insert(address.clone(), new_conn.clone());
                new_conn
            }
        };

        // Execute on the Arc'd connection — no lock held.
        <Connection as Provider<Fx>>::execute(&connection, capability).await
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

    struct MockConnection {
        data: HashMap<String, Vec<u8>>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<Get> for MockConnection {
        async fn execute(&self, effect: Capability<Get>) -> Option<Vec<u8>> {
            let get: &Get = effect.policy();
            self.data.get(&get.key).cloned()
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Resource<TestCredentials> for MockConnection {
        type Error = Infallible;

        async fn open(_address: &TestCredentials) -> Result<Self, Self::Error> {
            Ok(MockConnection {
                data: HashMap::new(),
            })
        }
    }

    #[dialog_common::test]
    async fn it_routes_and_caches_connections() {
        let route: Route<TestCredentials, MockConnection> = Route::new();

        let creds = TestCredentials("site-a".into());

        let did: dialog_capability::Did = "did:key:z6Mk...".parse().unwrap();
        let capability = dialog_capability::Subject::from(did.clone())
            .attenuate(Archive)
            .invoke(Get {
                key: "my-key".into(),
            });

        // First invocation opens and caches the connection
        let remote = RemoteInvocation::new(capability, creds.clone());
        let result = remote.perform(&route).await;
        assert_eq!(result, None); // empty connection, no data

        // Pre-populate a connection with data and insert it
        let mut conn = MockConnection {
            data: HashMap::new(),
        };
        conn.data.insert("my-key".into(), b"hello".to_vec());
        route.connections.insert(creds.clone(), Arc::new(conn));

        // Second invocation uses the newly inserted connection
        let capability = dialog_capability::Subject::from(did)
            .attenuate(Archive)
            .invoke(Get {
                key: "my-key".into(),
            });
        let remote = RemoteInvocation::new(capability, creds);
        let result = remote.perform(&route).await;
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

        #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        struct AlphaAddr(String);

        #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        struct BetaAddr(String);

        struct AlphaBackend;

        impl ProviderRoute for AlphaBackend {
            type Address = AlphaAddr;
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl Provider<RemoteInvocation<Fetch, AlphaAddr>> for AlphaBackend {
            async fn execute(&self, input: RemoteInvocation<Fetch, AlphaAddr>) -> String {
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
            async fn execute(&self, input: RemoteInvocation<Fetch, BetaAddr>) -> String {
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
            let router = CompositeRouter {
                alpha: AlphaBackend,
                beta: BetaBackend,
            };

            let cap_a = Subject::from("did:key:zAlpha".parse::<Did>().unwrap())
                .attenuate(Storage)
                .invoke(Fetch {
                    key: "doc-1".into(),
                });
            let remote_a = RemoteInvocation::new(cap_a, AlphaAddr("site-a".into()));
            let result_a = remote_a.perform(&router).await;
            assert_eq!(result_a, "alpha:site-a:doc-1");

            let cap_b = Subject::from("did:key:zBeta".parse::<Did>().unwrap())
                .attenuate(Storage)
                .invoke(Fetch {
                    key: "doc-2".into(),
                });
            let remote_b = RemoteInvocation::new(cap_b, BetaAddr("site-b".into()));
            let result_b = remote_b.perform(&router).await;
            assert_eq!(result_b, "beta:site-b:doc-2");
        }

        #[dialog_common::test]
        async fn it_dispatches_via_unified_address() {
            let router = CompositeRouter {
                alpha: AlphaBackend,
                beta: BetaBackend,
            };

            let cap_a = Subject::from("did:key:zAlpha".parse::<Did>().unwrap())
                .attenuate(Storage)
                .invoke(Fetch {
                    key: "doc-1".into(),
                });
            let addr_a: CompositeRouterAddress = AlphaAddr("site-a".into()).into();
            let result_a = RemoteInvocation::new(cap_a, addr_a).perform(&router).await;
            assert_eq!(result_a, "alpha:site-a:doc-1");

            let cap_b = Subject::from("did:key:zBeta".parse::<Did>().unwrap())
                .attenuate(Storage)
                .invoke(Fetch {
                    key: "doc-2".into(),
                });
            let addr_b: CompositeRouterAddress = BetaAddr("site-b".into()).into();
            let result_b = RemoteInvocation::new(cap_b, addr_b).perform(&router).await;
            assert_eq!(result_b, "beta:site-b:doc-2");
        }

        #[test]
        fn it_generates_from_impls_for_unified_address() {
            let alpha: CompositeRouterAddress = AlphaAddr("a".into()).into();
            let beta: CompositeRouterAddress = BetaAddr("b".into()).into();

            assert_eq!(alpha, CompositeRouterAddress::Alpha(AlphaAddr("a".into())));
            assert_eq!(beta, CompositeRouterAddress::Beta(BetaAddr("b".into())));
        }

        #[test]
        fn it_implements_provider_route_via_router() {
            use dialog_capability::ProviderRoute;

            // CompositeRouter implements ProviderRoute via the Router blanket
            fn _assert_provider_route<T: ProviderRoute>() {}
            _assert_provider_route::<CompositeRouter>();
        }
    }
}
