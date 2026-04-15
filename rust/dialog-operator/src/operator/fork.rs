//! Fork dispatch providers for Operator.
//!
//! Uses [`Authorizer`] parameterized by scheme marker to provide
//! disjoint implementations for credential-based and capability-based
//! sites, with a single blanket `Provider<Fork<At, Fx>>` that delegates.

use crate::Operator;
use crate::network::Network;

use dialog_capability::{
    Ability, Capability, Effect, Provider, SiteAddress, SiteAuthorization, Subject,
};
use dialog_capability::{
    Access, Authorization as _, AuthorizeError, Capabilities, Credentials, Fork, ForkInvocation,
    FromCapability, Proof as _, Protocol, Prove, Retain, Site,
};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::credential::Secret;
use dialog_effects::credential::prelude::*;
use dialog_storage::provider::space::SpaceProvider;

/// Helper trait for effect outputs that can absorb authorization errors.
///
/// All our effects return `Result<T, E>` where `E: From<AuthorizeError>`.
/// Enables converting authorization failures into effect-specific errors
/// (e.g., `AuthorizeError` -> `ArchiveError::Authorization`).
trait FromAuthError {
    fn from_auth_error(e: AuthorizeError) -> Self;
}

impl<T, E: From<AuthorizeError>> FromAuthError for Result<T, E> {
    fn from_auth_error(e: AuthorizeError) -> Self {
        Err(E::from(e))
    }
}

/// Scheme-parameterized authorization.
///
/// Produces a [`ForkInvocation`] from a [`Fork`] by building the
/// appropriate authorization material. Separate impls for
/// [`Credentials`] and [`Capabilities`] don't conflict because
/// they're different instantiations of this trait.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
trait Authorizer<Scheme, At: Site, Fx: Effect> {
    async fn authorize(
        &self,
        input: Fork<At, Fx>,
    ) -> Result<ForkInvocation<At, Fx>, AuthorizeError>;
}

/// Single blanket `Provider<Fork<At, Fx>>` impl that authorizes via
/// the scheme-specific [`Authorizer`], then performs the invocation.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<A, At, Fx> Provider<Fork<At, Fx>> for Operator<A>
where
    // Operator's storage provider type
    A: Clone + ConditionalSend + ConditionalSync + 'static,
    At: Site,
    Fx: Effect + 'static,
    // Needed to flatten AuthorizeError into effect error via FromAuthError
    Fx::Output: FromAuthError,
    // Required by async_trait for Send futures
    Fork<At, Fx>: ConditionalSend,
    ForkInvocation<At, Fx>: ConditionalSend,
    // Network dispatches the authorized invocation to the site provider
    Network: Provider<ForkInvocation<At, Fx>> + ConditionalSync,
    // Selects the right Authorizer impl based on the site's scheme
    Self: Authorizer<<At::Authorization as SiteAuthorization>::Scheme, At, Fx>
        + ConditionalSend
        + ConditionalSync,
{
    async fn execute(&self, input: Fork<At, Fx>) -> Fx::Output {
        match self.authorize(input).await {
            Ok(invocation) => invocation.perform(&self.network).await,
            Err(e) => FromAuthError::from_auth_error(e),
        }
    }
}

// Credential-based authorization.
//
// Looks up credentials from the secret store using a blake3 hash of
// the serialized address as the key.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<A, At, Fx> Authorizer<Credentials, At, Fx> for Operator<A>
where
    // SpaceProvider gives access to the credential store
    A: SpaceProvider + Clone + ConditionalSend + ConditionalSync + 'static,
    At: Site,
    At::Address: SiteAddress,
    // Site must use credential-based scheme and be deserializable from Secret
    At::Authorization: SiteAuthorization<Scheme = Credentials> + TryFrom<Secret>,
    // Deserialization errors must convert to AuthorizeError for ? to work
    <At::Authorization as TryFrom<Secret>>::Error: Into<AuthorizeError>,
    Fx: Effect + 'static,
    // Required by async_trait: Fork is held before the await point
    Fork<At, Fx>: ConditionalSend,
    Self: ConditionalSend + ConditionalSync,
{
    async fn authorize(
        &self,
        input: Fork<At, Fx>,
    ) -> Result<ForkInvocation<At, Fx>, AuthorizeError> {
        // Look up credentials before taking ownership of the fork,
        // to avoid holding the non-Send capability across the await.
        let address = input.address().clone();
        let credential = {
            self.profile_did()
                .credential()
                .site(&address)
                .load()
                .perform(self)
                .await?
                .try_into()
                .map_err(Into::into)?
        };

        let (capability, _) = input.into_parts();
        Ok(ForkInvocation::new(capability, address, credential))
    }
}

// Capability-based authorization.
//
// Builds a delegation proof chain from the certificate store, signs it,
// and wraps the invocation in the site's authorization type.

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<A, At, Fx> Authorizer<Capabilities, At, Fx> for Operator<A>
where
    // SpaceProvider gives access to the certificate store (Prove/Retain)
    A: SpaceProvider + Clone + 'static,
    At: Site,
    // Site must use capability-based scheme
    At::Authorization: SiteAuthorization<Scheme = Capabilities>,
    // Protocol defines the proof chain, certificate, and invocation types
    <At::Authorization as SiteAuthorization>::Protocol: Protocol,
    // The Operator uses Ed25519 signing; protocol signer must accept it
    <<At::Authorization as SiteAuthorization>::Protocol as Protocol>::Signer:
        From<dialog_credentials::Ed25519Signer>,
    // Site authorization must be constructable from the protocol's invocation
    At::Authorization:
        From<<<At::Authorization as SiteAuthorization>::Protocol as Protocol>::Invocation>,
    // Certificate store providers for the protocol
    A: Provider<Prove<<At::Authorization as SiteAuthorization>::Protocol>>
        + Provider<Retain<<At::Authorization as SiteAuthorization>::Protocol>>,
    // Clone needed to pass capability to both scope derivation and ForkInvocation
    Fx: Effect + Clone + ConditionalSend + 'static,
    // Ability needed for FromCapability scope derivation
    Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn authorize(
        &self,
        input: Fork<At, Fx>,
    ) -> Result<ForkInvocation<At, Fx>, AuthorizeError> {
        type P<Auth> = <Auth as SiteAuthorization>::Protocol;

        let (capability, address) = input.into_parts();

        let scope = <P<At::Authorization> as Protocol>::Access::from_capability(&capability);

        let proof = Subject::from(self.profile_did())
            .attenuate(Access)
            .invoke(Prove::<P<At::Authorization>>::new(self.did(), scope))
            .perform(self)
            .await?;

        let authorization = proof.claim(self.authority.operator_signer().clone().into())?;
        let invocation = authorization.invoke().await?;

        Ok(ForkInvocation::new(
            capability,
            address,
            At::Authorization::from(invocation),
        ))
    }
}
