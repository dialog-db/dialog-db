//! FS-remote address types.
//!
//! An [`FsAddress`] names a registered directory handle by opaque string
//! identifier. The actual `FileSystemDirectoryHandle` (browser) or path
//! (native) is resolved through the provider's handle registry at
//! invocation time. Consumers typically use the vault's subject DID as the
//! handle id, but the crate doesn't care.

use super::{Fs, FsAuthorization, FsFork};
use dialog_capability::access::AuthorizeError;
use dialog_capability::{Constraint, Effect, ForkInvocation, SiteAddress, SiteFork, SiteId};
use dialog_common::{ConditionalSend, ConditionalSync};
use serde::{Deserialize, Serialize};

/// Address for a local-filesystem-backed remote.
///
/// Carries an opaque identifier used to look up the actual directory
/// handle in the provider's registry. Serializable for storage in the
/// repository's remote configuration.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsAddress {
    id: String,
}

impl FsAddress {
    /// Construct an address with the given handle identifier.
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
    Env: ConditionalSync,
    FsFork<Fx>: ConditionalSend,
{
    type Site = Fs;
    type Effect = Fx;

    async fn authorize(self, _env: &Env) -> Result<ForkInvocation<Fs, Fx>, AuthorizeError> {
        // FS-remote has no over-the-wire authorization step: the host already
        // granted access by registering the directory. Attest a unit
        // authorization; the provider resolves the address and delegates the
        // capability to the registered directory when the invocation fires.
        Ok(self.0.attest(FsAuthorization))
    }
}
