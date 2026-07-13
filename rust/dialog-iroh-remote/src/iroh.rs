//! Iroh site type and fork plumbing.
//!
//! Mirrors the shape of `dialog_remote_s3::s3` and `dialog_remote_fs::fs`.
//! The site marker is [`Iroh`]; the site-bound fork is [`IrohFork<Fx>`].
//! Authorization builds a signed UCAN invocation (the same artifact the
//! UCAN-S3 site produces for its access service); the transport that
//! redeems it at a peer lives in the native-only submodules.

mod address;
mod authorization;
mod error;
pub mod protocol;

pub use address::IrohAddress;
pub use authorization::IrohAuthorization;
pub use error::IrohRemoteError;

#[cfg(not(target_arch = "wasm32"))]
mod transport;
#[cfg(not(target_arch = "wasm32"))]
pub use transport::*;

pub mod provider;

use dialog_capability::access::{
    Access, Authorization as _, Authorize as AuthorizeEffect, AuthorizeError, FromCapability,
    Protocol,
};
use dialog_capability::{
    Ability, Capability, Constraint, Effect, Fork, ForkInvocation, Provider, Site, SiteFork,
    Subject,
};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::authority::{self, OperatorExt};
use dialog_ucan::Ucan;

/// Iroh peer-to-peer site.
///
/// Marker for fork dispatch. Effects forked to an [`IrohAddress`] are
/// authorized as UCAN invocations and executed at the peer over the
/// `dialog-db/remote/0` QUIC protocol. The live endpoint state (connection
/// pool, joined swarms) is process-global — see the `node` module — so the
/// marker stays `Copy` and composes into `dialog_network::Network`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Iroh;

/// Site-owned fork wrapper for [`Iroh`].
///
/// Thin newtype around [`Fork<Iroh, Fx>`] that carries the site-specific
/// [`SiteFork`] impl: fetch session identity from the env, invoke UCAN's
/// `Authorize` on that identity, and bundle the resulting signed invocation
/// into a [`ForkInvocation`]. This is the same authorization the UCAN-S3
/// site performs — only the redemption transport differs.
pub struct IrohFork<Fx: Effect>(Fork<Iroh, Fx>);

impl<Fx: Effect> From<Fork<Iroh, Fx>> for IrohFork<Fx> {
    fn from(fork: Fork<Iroh, Fx>) -> Self {
        Self(fork)
    }
}

impl Site for Iroh {
    type Authorization = IrohAuthorization;
    type Address = IrohAddress;
    type Fork<Fx: Effect> = IrohFork<Fx>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx, Env> SiteFork<Env> for IrohFork<Fx>
where
    Fx: Effect + Clone + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
    Env: Provider<AuthorizeEffect<Ucan>> + Provider<authority::Identify> + ConditionalSync,
{
    type Site = Iroh;
    type Effect = Fx;

    async fn authorize(self, env: &Env) -> Result<ForkInvocation<Iroh, Fx>, AuthorizeError> {
        let identity = authority::Identify
            .perform(env)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;
        let profile = identity.profile().clone();
        let operator = identity.did();

        let scope = <Ucan as Protocol>::Access::from_capability(self.0.capability());

        let authorization = Subject::from(profile)
            .attenuate(Access)
            .invoke(AuthorizeEffect::<Ucan>::new(operator, scope))
            .perform(env)
            .await?;

        let invocation = authorization.invoke().await?;
        Ok(self.0.attest(IrohAuthorization::new(invocation)))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    #[dialog_common::test]
    fn it_builds_an_address_from_an_endpoint_id() {
        let address: IrohAddress = "ku3zxvamvqjrmnrkiknl3d2gzwsdcbgcpqfhjbjjbmscqvlgoeoa"
            .parse()
            .unwrap();
        assert_eq!(
            address.endpoint(),
            "ku3zxvamvqjrmnrkiknl3d2gzwsdcbgcpqfhjbjjbmscqvlgoeoa"
        );
        assert!(address.relay_url().is_none());
        assert!(address.direct_addresses().next().is_none());
    }

    #[dialog_common::test]
    fn it_roundtrips_address_through_serde() {
        let address = IrohAddress::new("ku3zxvamvqjrmnrkiknl3d2gzwsdcbgcpqfhjbjjbmscqvlgoeoa")
            .with_relay_url("https://relay.example.com./")
            .with_direct_address("192.168.1.10:4433");
        let json = serde_json::to_string(&address).unwrap();
        let parsed: IrohAddress = serde_json::from_str(&json).unwrap();
        assert_eq!(address, parsed);
    }
}
