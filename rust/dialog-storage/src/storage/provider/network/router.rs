//! Connection cache that routes remote invocations by address.
//!
//! [`Router`] manages a cache of connections keyed by address. When a
//! [`RemoteInvocation`] arrives, it uses the [`Connector`] trait to resolve the
//! address into a connection, then delegates the capability to that connection's
//! [`Provider`] implementation.
//!
//! [`RemoteInvocation`]: dialog_effects::remote::RemoteInvocation
//! [`Connector`]: super::connector::Connector
//! [`Provider`]: dialog_capability::Provider

use std::collections::HashMap;
use std::hash::Hash;

use dialog_capability::{Constraint, Effect, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::remote::RemoteInvocation;

use super::connector::Connector;

/// A connection cache that routes remote invocations by address.
///
/// When an effect arrives as a [`RemoteInvocation`], the router resolves
/// the address via the [`Connector`] trait and delegates execution to the
/// resulting connection.
///
/// [`RemoteInvocation`]: dialog_effects::remote::RemoteInvocation
pub struct Router<Address, Connection> {
    connections: HashMap<Address, Connection>,
}

impl<Address, Connection> Router<Address, Connection> {
    /// Create a new router with no cached connections.
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }

    /// Get the number of cached connections.
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Check if there are no cached connections.
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}

impl<Address, Connection> Default for Router<Address, Connection> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Address: Eq + Hash, Connection> Router<Address, Connection> {
    /// Get a mutable reference to a cached connection, if it exists.
    pub fn get_mut(&mut self, address: &Address) -> Option<&mut Connection> {
        self.connections.get_mut(address)
    }

    /// Insert a connection for the given address, returning the old one if present.
    pub fn insert(&mut self, address: Address, connection: Connection) -> Option<Connection> {
        self.connections.insert(address, connection)
    }
}

/// [`Provider`] impl for [`Router`]: when `Router` implements [`Connector`]
/// for the address type and the connection implements [`Provider`] for the
/// effect, `Router` can handle [`RemoteInvocation`]s by resolving the address
/// and delegating to the connection.
///
/// [`RemoteInvocation`]: dialog_effects::remote::RemoteInvocation
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx, Address, Connection> Provider<RemoteInvocation<Fx, Address>>
    for Router<Address, Connection>
where
    Fx: Effect + ConditionalSend + 'static,
    Fx::Of: Constraint,
    <Fx::Of as Constraint>::Capability: ConditionalSend + 'static,
    Address: ConditionalSend + ConditionalSync + 'static,
    Connection: Provider<Fx> + ConditionalSend + 'static,
    Self: Connector<Address, Connection = Connection>,
    <Self as Connector<Address>>::Error: Into<Fx::Output> + 'static,
{
    async fn execute(&mut self, input: RemoteInvocation<Fx, Address>) -> Fx::Output {
        let (capability, address) = input.into_parts();
        let connection = match self.open(&address).await {
            Ok(connection) => connection,
            Err(error) => return error.into(),
        };
        connection.execute(capability).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Attenuation, Capability, Did, Subject};
    use serde::{Deserialize, Serialize};

    fn test_subject() -> Subject {
        Subject::from("did:key:z6MkTest".parse::<Did>().unwrap())
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Archive;
    impl Attenuation for Archive {
        type Of = Subject;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Get {
        key: String,
    }
    impl Effect for Get {
        type Of = Archive;
        type Output = Result<Option<Vec<u8>>, TestError>;
    }

    #[derive(Debug, thiserror::Error)]
    #[error("{0}")]
    struct TestError(String);

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestAddress(String);

    struct MockConnection {
        data: HashMap<String, Vec<u8>>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<Get> for MockConnection {
        async fn execute(&mut self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, TestError> {
            let get: &Get = effect.policy();
            Ok(self.data.get(&get.key).cloned())
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Connector<TestAddress> for Router<TestAddress, MockConnection> {
        type Connection = MockConnection;
        type Error = TestError;

        async fn open(&mut self, address: &TestAddress) -> Result<&mut MockConnection, TestError> {
            if !self.connections.contains_key(address) {
                self.connections.insert(
                    address.clone(),
                    MockConnection {
                        data: HashMap::new(),
                    },
                );
            }
            self.connections
                .get_mut(address)
                .ok_or_else(|| TestError("connection not found".into()))
        }
    }

    impl From<TestError> for Result<Option<Vec<u8>>, TestError> {
        fn from(e: TestError) -> Self {
            Err(e)
        }
    }

    #[dialog_common::test]
    async fn it_routes_to_correct_connection() -> anyhow::Result<()> {
        let mut provider: Router<TestAddress, MockConnection> = Router::new();

        let addr = TestAddress("site-a".into());

        // Seed connection with data
        provider.insert(
            addr.clone(),
            MockConnection {
                data: HashMap::from([("my-key".into(), b"hello".to_vec())]),
            },
        );

        let capability = test_subject().attenuate(Archive).invoke(Get {
            key: "my-key".into(),
        });

        let remote = RemoteInvocation::new(capability, addr);
        let result = remote.perform(&mut provider).await?;
        assert_eq!(result, Some(b"hello".to_vec()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_key() -> anyhow::Result<()> {
        let mut provider: Router<TestAddress, MockConnection> = Router::new();

        let addr = TestAddress("site-b".into());
        provider.insert(
            addr.clone(),
            MockConnection {
                data: HashMap::new(),
            },
        );

        let capability = test_subject().attenuate(Archive).invoke(Get {
            key: "missing".into(),
        });

        let remote = RemoteInvocation::new(capability, addr);
        let result = remote.perform(&mut provider).await?;
        assert_eq!(result, None);

        Ok(())
    }

    #[dialog_common::test]
    fn it_tracks_cached_connections() {
        let mut provider: Router<TestAddress, MockConnection> = Router::new();
        assert!(provider.is_empty());
        assert_eq!(provider.len(), 0);

        provider.insert(
            TestAddress("a".into()),
            MockConnection {
                data: HashMap::new(),
            },
        );
        assert!(!provider.is_empty());
        assert_eq!(provider.len(), 1);
    }
}
