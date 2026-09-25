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
