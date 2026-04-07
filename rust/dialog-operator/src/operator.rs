//! Operator — an operating environment built from a Profile.
//!
//! Build one via `Profile::derive()`.

mod builder;
#[cfg(test)]
mod test;

pub use builder::{NetworkBuilder, OperatorBuilder, OperatorError};

use crate::Authority;
use crate::remote::Remote;
use dialog_capability::Capability;
use dialog_capability::Provider;
use dialog_capability::authority::{Identify, Operator as AuthOperator};
use dialog_effects::space as space_fx;
use dialog_effects::storage as storage_fx;
use dialog_effects::{archive, credential, memory};
use dialog_storage::provider::environment::Environment;
use dialog_storage::provider::space::SpaceProvider;
use dialog_varsig::{Did, Principal};

use dialog_capability::access::Prove as AccessProve;
use dialog_capability::access::Retain as AccessRetain;
use dialog_ucan::Ucan;

type ProveUcan = AccessProve<Ucan>;
type RetainUcan = AccessRetain<Ucan>;

/// An operating environment built from a [`Profile`](crate::profile::Profile).
///
/// Composes:
/// - Authority credentials (identity)
/// - [`Environment`] for DID-routed effects and space load/create
/// - Base directory for resolving space names to storage locations
/// - Remote for fork invocations
#[derive(Provider)]
pub struct Operator<S: Clone> {
    #[provide(Identify)]
    /// Provider for authority effects (identity).
    authority: Authority,

    #[provide(
        archive::Get,
        archive::Put,
        credential::Load,
        credential::Save,
        memory::Resolve,
        memory::Publish,
        memory::Retract
    )]
    /// Environment — routes DID-based effects.
    env: Environment<S>,

    /// Base directory for resolving space names.
    directory: storage_fx::Directory,

    /// Provider for remote invocations.
    remote: Remote,
}

impl<S: Clone> Operator<S> {
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
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<ProveUcan> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    S: Provider<ProveUcan>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<ProveUcan>,
    ) -> Result<dialog_ucan::UcanProof, AuthorizeError> {
        self.env.execute(input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<RetainUcan> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    S: Provider<RetainUcan>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<RetainUcan>) -> Result<(), AuthorizeError> {
        self.env.execute(input).await
    }
}

impl<S: Clone> Principal for Operator<S> {
    fn did(&self) -> Did {
        self.authority.operator_did()
    }
}

use dialog_capability::{Policy, Subject};
use dialog_effects::storage::LocationExt as _;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<space_fx::Load> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Environment<S>: Provider<storage_fx::Load>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<space_fx::Load>,
    ) -> Result<dialog_credentials::Credential, storage_fx::StorageError> {
        let subject = input.subject();
        if *subject != self.profile_did() {
            return Err(storage_fx::StorageError::Storage(format!(
                "space load denied: subject {subject} does not match profile {}",
                self.profile_did()
            )));
        }

        let name = &space_fx::Space::of(&input).name;
        let location = storage_fx::Location::new(self.directory.clone(), name);
        Subject::from(dialog_capability::did!("local:storage"))
            .attenuate(storage_fx::Storage)
            .attenuate(location)
            .load()
            .perform(&self.env)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<space_fx::Create> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Environment<S>: Provider<storage_fx::Create>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<space_fx::Create>,
    ) -> Result<dialog_credentials::Credential, storage_fx::StorageError> {
        let subject = input.subject();
        if *subject != self.profile_did() {
            return Err(storage_fx::StorageError::Storage(format!(
                "space create denied: subject {subject} does not match profile {}",
                self.profile_did()
            )));
        }

        let name = &space_fx::Space::of(&input).name;
        let credential = space_fx::Create::of(&input).credential.clone();
        let location = storage_fx::Location::new(self.directory.clone(), name);
        Subject::from(dialog_capability::did!("local:storage"))
            .attenuate(storage_fx::Storage)
            .attenuate(location)
            .create(credential)
            .perform(&self.env)
            .await
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
    impl<S, Fx> Provider<Fork<S3, Fx>> for Operator<S>
    where
        S: Clone + ConditionalSend + ConditionalSync + 'static,
        Fx: Effect + 'static,
        Fx::Of: Constraint,
        Fx::Output: ConditionalSend,
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
    use dialog_capability::access::{self, Authorization as _, Proof as _};
    use dialog_remote_ucan_s3::UcanSite;
    use dialog_ucan::scope::Scope as UcanScope;
    use dialog_ucan::{Ucan, UcanProofChain};

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl<S, Fx> Provider<Fork<UcanSite, Fx>> for Operator<S>
    where
        S: SpaceProvider + Clone + 'static + Provider<ProveUcan> + Provider<RetainUcan>,
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

            let scope = UcanScope::invoke(&capability);

            let proof_chain: UcanProofChain = dialog_capability::Subject::from(self.profile_did())
                .attenuate(access::Access)
                .invoke(access::Prove::<Ucan>::new(self.did(), scope))
                .perform(self)
                .await?;

            let authorization = proof_chain.claim(self.authority.operator_signer().clone())?;
            let ucan_invocation = authorization.invoke().await?;

            let invocation = ForkInvocation::new(capability, address, ucan_invocation);
            Ok(self.remote.execute(invocation).await)
        }
    }
}
