//! Operator — an operating environment built from a Profile.
//!
//! Build one via `Profile::operator()`.

mod builder;
#[cfg(test)]
mod test;

pub use builder::{NetworkBuilder, OperatorBuilder, OperatorError};

use crate::Authority;
use crate::remote::Remote;
use crate::storage::Storage;
use dialog_capability::Capability;
use dialog_capability::Provider;
use dialog_capability::authority::{Identify, Operator as AuthOperator};
use dialog_capability::storage::{Load, Mount, Save};
use dialog_credentials::credential::Credential;
use dialog_effects::{archive, memory, storage as fx_storage};
use dialog_storage::provider::Address;
use dialog_varsig::{Did, Principal};

use dialog_capability::access::Claim as AccessClaim;

use dialog_capability::access::Save as AccessSave;

use dialog_capability_ucan::Ucan;

type ClaimUcan = AccessClaim<Ucan>;

type SaveUcan = AccessSave<Ucan>;

/// An operating environment built from a [`Profile`](crate::profile::Profile).
///
/// Composes:
/// - Authority credentials (identity)
/// - [`Storage`] for addressed Load/Save/Mount and DID-routed effects
/// - Remote for fork invocations
#[derive(Provider)]
pub struct Operator {
    #[provide(Identify)]
    /// Provider for authority effects (identity).
    authority: Authority,

    #[provide(
        archive::Get,
        archive::Put,
        memory::Resolve,
        memory::Publish,
        memory::Retract,
        fx_storage::Get,
        fx_storage::Set,
        fx_storage::Delete,
        fx_storage::List,
        Load<Vec<u8>, Address>,
        Save<Vec<u8>, Address>,
        Load<Credential, Address>,
        Save<Credential, Address>,
        Mount<Address>,
        ClaimUcan,
        SaveUcan
    )]
    /// Storage — routes DID-based effects and addressed Load/Save/Mount.
    storage: Storage,

    /// Provider for remote invocations.
    remote: Remote,
}

impl Operator {
    /// The operator's DID (the ephemeral/derived session key).
    pub fn did(&self) -> Did {
        self.authority.operator_did()
    }

    /// The profile's DID (the long-lived identity).
    pub fn profile_did(&self) -> Did {
        self.authority.profile_did()
    }

    /// Build the authority chain for a given subject DID.
    pub fn build_authority(&self, subject: Did) -> Capability<AuthOperator> {
        self.authority.build_authority(subject)
    }

    /// The storage for this operator.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }
}

impl Principal for Operator {
    fn did(&self) -> Did {
        self.authority.operator_did()
    }
}

use dialog_capability::access::AuthorizeError;
use dialog_capability::fork::{Fork, ForkInvocation};
use dialog_capability::{Constraint, Effect};
use dialog_common::{ConditionalSend, ConditionalSync};

#[cfg(feature = "s3")]
mod s3_fork {
    use super::*;
    use dialog_remote_s3::S3;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl<Fx> Provider<Fork<S3, Fx>> for Operator
    where
        Fx: Effect + 'static,
        Fx::Of: Constraint,
        Fork<S3, Fx>: ConditionalSend,
        ForkInvocation<S3, Fx>: ConditionalSend,
        Remote: Provider<ForkInvocation<S3, Fx>> + ConditionalSync,
        Self: ConditionalSend + ConditionalSync,
    {
        async fn execute(&self, input: Fork<S3, Fx>) -> Result<Fx::Output, AuthorizeError> {
            let (capability, address) = input.into_parts();
            let invocation = ForkInvocation::new(capability, address, ());
            Ok(self.remote.execute(invocation).await)
        }
    }
}

mod ucan_fork {
    use super::*;
    use dialog_capability::Ability;
    use dialog_capability::access::{self, Authorization as _, ProofChain as _};
    use dialog_capability_ucan::scope::Scope as UcanScope;
    use dialog_capability_ucan::{Ucan, UcanProofChain};
    use dialog_remote_ucan_s3::UcanSite;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl<Fx> Provider<Fork<UcanSite, Fx>> for Operator
    where
        Fx: Effect + Clone + ConditionalSend + 'static,
        Fx::Of: Constraint,
        Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
        Fork<UcanSite, Fx>: ConditionalSend,
        ForkInvocation<UcanSite, Fx>: ConditionalSend,
        Remote: Provider<ForkInvocation<UcanSite, Fx>> + ConditionalSync,
        Self: ConditionalSend + ConditionalSync,
    {
        async fn execute(&self, input: Fork<UcanSite, Fx>) -> Result<Fx::Output, AuthorizeError> {
            let (capability, address) = input.into_parts();

            // Build access scope with claim projection (content -> checksum etc.)
            let scope = UcanScope::invoke(&capability);

            // Claim authorization via capability chain
            // Subject is the profile DID (where delegations are stored)
            let proof_chain: UcanProofChain = dialog_capability::Subject::from(self.profile_did())
                .attenuate(access::Permit)
                .invoke(access::Claim::<Ucan>::new(self.did(), scope))
                .perform(self)
                .await?;

            // Bind signer and build invocation
            let authorization = proof_chain.claim(self.authority.operator_signer().clone())?;
            let ucan_invocation = authorization.invoke().await?;

            let invocation = ForkInvocation::new(capability, address, ucan_invocation);
            Ok(self.remote.execute(invocation).await)
        }
    }
}
