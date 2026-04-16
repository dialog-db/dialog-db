//! Network dispatch — routes `ForkInvocation<RemoteSite, Fx>` to the
//! appropriate site provider.
//!
//! The Operator builds the authorization (converting `Fork` to `ForkInvocation`)
//! before delegating here. The [`RemoteSite`](crate::site::RemoteSite) enum
//! dispatches to S3 or UCAN based on the address/authorization variant.

use crate::site::{RemoteAuthorization, RemoteSite, SiteAddress};
use async_trait::async_trait;
use dialog_capability::fork::ForkInvocation;
use dialog_capability::{Constraint, Effect, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_remote_s3::S3;
use dialog_remote_ucan_s3::UcanSite;

/// Network dispatch — routes fork invocations to the appropriate site.
///
/// Stateless dispatcher. The Operator routes `ForkInvocation<RemoteSite, Fx>`
/// here after building the protocol-specific authorization. The match on
/// `RemoteAuthorization`/`SiteAddress` variants delegates to `S3` or
/// `UcanSite` directly.
#[derive(Debug, Clone, Copy, Default)]
pub struct Network;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Fx> Provider<ForkInvocation<RemoteSite, Fx>> for Network
where
    Fx: Effect + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    ForkInvocation<S3, Fx>: ConditionalSend,
    ForkInvocation<UcanSite, Fx>: ConditionalSend,
    S3: Provider<ForkInvocation<S3, Fx>>,
    UcanSite: Provider<ForkInvocation<UcanSite, Fx>>,
{
    async fn execute(&self, input: ForkInvocation<RemoteSite, Fx>) -> Fx::Output {
        let ForkInvocation {
            capability,
            address,
            authorization,
        } = input;
        match (authorization, address) {
            (RemoteAuthorization::S3(auth), SiteAddress::S3(addr)) => {
                ForkInvocation::new(capability, addr, auth)
                    .perform(&S3)
                    .await
            }
            (RemoteAuthorization::Ucan(auth), SiteAddress::Ucan(addr)) => {
                ForkInvocation::new(capability, addr, auth)
                    .perform(&UcanSite)
                    .await
            }
            _ => unreachable!("authorization/address type mismatch"),
        }
    }
}
