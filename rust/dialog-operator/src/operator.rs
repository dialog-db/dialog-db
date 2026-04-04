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
use dialog_capability::authority::{Identify, Operator as AuthOperator, Sign};
use dialog_capability::storage::{Load, Mount, Save};
use dialog_credentials::credential::Credential;
use dialog_effects::{archive, memory, storage as fx_storage};
use dialog_storage::provider::Address;
use dialog_varsig::{Did, Principal};

/// An operating environment built from a [`Profile`](crate::profile::Profile).
///
/// Composes:
/// - Authority credentials (identify, sign)
/// - [`Storage`] for addressed Load/Save/Mount and DID-routed effects
/// - Remote for fork invocations
#[derive(Provider)]
pub struct Operator {
    #[provide(Identify, Sign)]
    /// Provider for authority effects (identity + signing).
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
        Mount<Address>
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
use dialog_capability::fork::Fork;
use dialog_capability::site::Site;
use dialog_capability::{Constraint, Effect};
use dialog_common::{ConditionalSend, ConditionalSync};

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, Fx> Provider<Fork<S, Fx>> for Operator
where
    S: Site,
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Fork<S, Fx>: ConditionalSend,
    Remote: Provider<Fork<S, Fx>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Fork<S, Fx>) -> Result<Fx::Output, AuthorizeError> {
        self.remote.execute(input).await
    }
}
