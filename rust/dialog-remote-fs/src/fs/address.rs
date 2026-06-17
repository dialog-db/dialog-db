//! FS-remote address types.
//!
//! An [`FsAddress`] names a granted directory by opaque [`SiteId`]. The grant
//! itself — a path on native, a `FileSystemDirectoryHandle` on the web — is
//! stored as a site credential and loaded at authorize time. Consumers
//! typically use the vault's subject DID as the id, but the crate doesn't care.

use super::{Fs, FsAuthorization, FsFork};
use dialog_capability::access::AuthorizeError;
use dialog_capability::{
    Constraint, Effect, ForkInvocation, Provider, SiteAddress, SiteFork, SiteId,
};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::authority::{self, OperatorExt};
use dialog_effects::credential::prelude::*;
use dialog_effects::credential::{Grant, Load};
use dialog_storage::provider::FileSystem;
use serde::{Deserialize, Serialize};

/// Address for a local-filesystem-backed remote.
///
/// Carries an opaque identifier naming the directory grant. Serializable for
/// storage in the repository's remote configuration; the grant it names is
/// resolved from the credential store at authorize time.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsAddress {
    id: String,
}

impl FsAddress {
    /// Construct an address with the given identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    /// The opaque identifier for this address.
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl SiteAddress for FsAddress {
    type Site = Fs;
}

impl From<FsAddress> for SiteId {
    fn from(address: FsAddress) -> Self {
        address.id.into()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx, Env> SiteFork<Env> for FsFork<Fx>
where
    Fx: Effect + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    Env: Provider<Load<Grant>> + Provider<authority::Identify> + ConditionalSync,
    FsFork<Fx>: ConditionalSend,
{
    type Site = Fs;
    type Effect = Fx;

    async fn authorize(self, env: &Env) -> Result<ForkInvocation<Fs, Fx>, AuthorizeError> {
        // FS-remote has no over-the-wire authorization. Load the directory grant
        // the host saved for this site, resolve it into a FileSystem, and attest
        // that — the provider delegates the capability to it. Mirrors the S3
        // site's credential resolution.
        let profile = authority::Identify
            .perform(env)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?
            .profile()
            .clone();

        let address = self.0.address().clone();

        let grant = profile
            .credential()
            .site(address.clone())
            .load_grant()
            .perform(env)
            .await?;

        let filesystem = FileSystem::from_grant(address.id(), &grant)
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

        Ok(self.0.attest(FsAuthorization::new(filesystem)))
    }
}
