use crate::{Authorized, Capability, Claim, Constraint, Provider, Subject, credential};
use dialog_common::ConditionalSync;

/// The `Access` trait abstracts over credential stores that can authorize
/// capabilities.
///
/// Implementors provide `authorize` to produce authorized capabilities.
///
/// `C` is the constraint (capability) type being authorized. Making `Access`
/// generic over `C` lets implementations add bounds on the capability
/// (e.g., `Capability<C>: S3Request` for S3 credentials).
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Access<C: Constraint> {
    /// The authorization type produced by this access provider.
    type Authorization;

    /// Error type for authorization failures.
    type Error: std::error::Error;

    /// Grant authorization for a capability.
    ///
    /// Takes a capability and an environment providing credential effects.
    /// Returns an [`Authorized`] capability ready for execution.
    async fn authorize<Env>(
        &self,
        capability: Capability<C>,
        env: &Env,
    ) -> Result<Authorized<C, Self::Authorization>, Self::Error>
    where
        Env: Provider<credential::Identify> + Provider<credential::Sign> + ConditionalSync;

    /// Start building a capability chain with these credentials.
    ///
    /// Returns a [`Claim`] rooted at the given subject. Use `.attenuate()`
    /// and `.invoke()` to build the chain, then `.acquire()` to authorize.
    fn claim(&self, subject: impl Into<Subject>) -> Claim<'_, Self, Subject>
    where
        Self: Sized,
    {
        Claim::new(self, Capability::new(subject.into()))
    }
}
