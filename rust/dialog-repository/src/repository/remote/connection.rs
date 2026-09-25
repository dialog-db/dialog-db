//! A replica on a connected peer, as an environment.
//!
//! A [`Connection`] is the peer's replica paired with the environment
//! that reaches it. Performing an effect against it performs that effect
//! at the peer: the capability is forked to one of the peer's addresses,
//! starting from the one that last answered, and passed to the next when
//! an address cannot be reached. Code working with a remote replica then
//! performs its effects as it would locally, and never names a site.

use std::fmt;

use dialog_capability::{Capability, Constraint, Effect, Fork, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};

use super::repository::Unreachable;
use crate::{ConnectedReplica, RemoteSite};

/// A replica on a connected peer, reached through `env`.
pub struct Connection<'a, Env> {
    replica: ConnectedReplica,
    env: &'a Env,
}

impl<Env> Clone for Connection<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            replica: self.replica.clone(),
            env: self.env,
        }
    }
}

impl<Env> fmt::Debug for Connection<'_, Env> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("replica", &self.replica)
            .finish_non_exhaustive()
    }
}

impl ConnectedReplica {
    /// This replica, reached through `env`: an environment whose effects
    /// are performed at the peer.
    pub fn connection<'a, Env>(&self, env: &'a Env) -> Connection<'a, Env> {
        Connection {
            replica: self.clone(),
            env,
        }
    }
}

impl<'a, Env> Connection<'a, Env> {
    /// The replica this connection reaches.
    pub fn replica(&self) -> &ConnectedReplica {
        &self.replica
    }

    /// The environment the connection reaches the peer through.
    pub fn env(&self) -> &'a Env {
        self.env
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env, E, T, Error> Provider<E> for Connection<'_, Env>
where
    E: Effect<Output = Result<T, Error>> + Clone + ConditionalSend + ConditionalSync + 'static,
    E::Of: Constraint,
    Capability<E>: Clone + ConditionalSend + ConditionalSync,
    Env: Provider<Fork<RemoteSite, E>> + ConditionalSync,
    T: ConditionalSend,
    Error: Unreachable + ConditionalSend,
{
    async fn execute(&self, input: Capability<E>) -> Result<T, Error> {
        self.replica
            .reach(|address| {
                let input = input.clone();
                async move { input.fork(address.site()).perform(self.env).await }
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use std::sync::atomic::{AtomicUsize, Ordering};

    use dialog_artifacts::Entity;
    use dialog_capability::{Fork, Provider, Subject};
    use dialog_effects::memory::prelude::CellScope;
    use dialog_effects::memory::{MemoryError, Publish, Version};
    use dialog_remote_s3::Address as S3Address;
    use dialog_varsig::did;

    use crate::{ConnectedReplica, RemoteSite, SiteAddress};

    /// An environment whose every publish at a peer fails on the wire,
    /// counting how many were sent.
    #[derive(Default)]
    struct Timeouts {
        sent: AtomicUsize,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<Fork<RemoteSite, Publish>> for Timeouts {
        async fn execute(&self, _input: Fork<RemoteSite, Publish>) -> Result<Version, MemoryError> {
            self.sent.fetch_add(1, Ordering::Relaxed);
            Err(MemoryError::Storage("timed out".into()))
        }
    }

    fn site(endpoint: &str) -> SiteAddress {
        S3Address::builder(endpoint)
            .region("us-east-1")
            .bucket("bucket")
            .build()
            .unwrap()
            .into()
    }

    /// A publish is a conditional write: one that failed on the wire may
    /// have landed, so a connection does not send it to another address.
    #[dialog_common::test]
    async fn it_does_not_resend_a_publish_that_may_have_landed() {
        let subject = did!("key:z6MkkZfZmshVFcBYo9RS6ZyUstxYdjjStQaFaL2TSTVdsiJh");
        let replica = ConnectedReplica::new(
            Subject::from(subject.clone()),
            Entity::new().unwrap(),
            None,
            vec![site("https://a.example"), site("https://b.example")],
            subject.clone(),
        );
        let env = Timeouts::default();

        let published = CellScope::new(Subject::from(subject), "branch/main", "revision")
            .publish(vec![1], None)
            .perform(&replica.connection(&env))
            .await;
        assert!(matches!(published, Err(MemoryError::Storage(_))));
        assert_eq!(env.sent.load(Ordering::Relaxed), 1);
    }
}
