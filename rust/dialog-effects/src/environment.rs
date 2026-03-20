//! Generic composable environment for executing capability effects.
//!
//! [`Environment`] composes three providers:
//! - `Local` — storage effects (archive, memory)
//! - `Credentials` — credential effects (identify, sign, authorize)
//! - `Remote` — remote invocations (S3, UCAN, etc.)
//!
//! Local and credential effects are routed via `#[derive(Provider)]`.
//! [`Authorize`] is routed to `Credentials` via a blanket impl.
//! Remote invocations (site-specific types like `S3Invocation<Fx>`) are
//! routed to `Remote` via blanket impls defined alongside each site type.
//!
//! [`Authorize`]: dialog_capability::credential::Authorize

use dialog_capability::Provider;
use dialog_capability::access::Access;
use dialog_capability::authorization::Authorized;
use dialog_capability::credential::{Authorize, AuthorizeError};
use dialog_capability::{Constraint, Effect};

/// Generic environment that delegates:
/// - Storage effects to `Local`
/// - Credential effects (including authorization) to `Credentials`
/// - Remote invocations to `Remote`
#[derive(Provider)]
pub struct Environment<Local, Credentials = (), Remote = ()> {
    #[provide(crate::credential::Identify, crate::credential::Sign)]
    /// Provider for credential effects.
    pub credentials: Credentials,
    #[provide(
        crate::archive::Get,
        crate::archive::Put,
        crate::memory::Resolve,
        crate::memory::Publish,
        crate::memory::Retract
    )]
    /// Provider for local storage effects.
    pub local: Local,
    /// Provider for remote invocations.
    pub remote: Remote,
}

// Route Authorize<Fx, A> to self.credentials for any access format.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Local, Credentials, Remote, Fx, A> Provider<Authorize<Fx, A>>
    for Environment<Local, Credentials, Remote>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    A: Access,
    Authorize<Fx, A>: dialog_common::ConditionalSend,
    Credentials: Provider<Authorize<Fx, A>> + dialog_common::ConditionalSync,
    Self: dialog_common::ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: Authorize<Fx, A>) -> Result<Authorized<Fx, A>, AuthorizeError> {
        self.credentials.execute(input).await
    }
}

impl<Local> Environment<Local> {
    /// Create a new environment from a local provider (no credentials).
    pub fn new(local: Local) -> Self {
        Self {
            local,
            credentials: (),
            remote: (),
        }
    }
}

impl<Local, Credentials> Environment<Local, Credentials> {
    /// Create a new environment from local and credential providers.
    pub fn with_credentials(local: Local, credentials: Credentials) -> Self {
        Self {
            local,
            credentials,
            remote: (),
        }
    }
}

impl<Local, Credentials, Remote> Environment<Local, Credentials, Remote> {
    /// Create a new environment from local, credential, and remote providers.
    pub fn with_remote(local: Local, credentials: Credentials, remote: Remote) -> Self {
        Self {
            local,
            credentials,
            remote,
        }
    }
}
