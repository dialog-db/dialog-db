//! FS-remote address types.
//!
//! An [`FsAddress`] names the local directory that backs a space. Authorization
//! is the local analogue of the UCAN site's presign endpoint:
//! [`authorize`](FsFork::authorize) opens the directory, verifies it really is
//! the space for the invocation's subject (its stored `credential/key/self` DID
//! matches), and hands the resolved [`FileSystem`] to the provider to act on.
//!
//! The address resolves to a directory per target:
//! - **native**: a `file:` URL, opened directly.
//! - **web**: an IndexedDB database name holding the directory's
//!   `FileSystemDirectoryHandle` (saved once via
//!   [`register_web_directory`](dialog_storage::provider::register_web_directory)).

use super::{Fs, FsAuthorization, FsFork};
use dialog_capability::access::AuthorizeError;
use dialog_capability::{Ability, Capability, Constraint, Effect, ForkInvocation, Principal};
use dialog_capability::{SiteAddress, SiteFork, SiteId, did};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::credential::SELF;
use dialog_effects::credential::prelude::*;
use dialog_storage::provider::FileSystem;
use serde::{Deserialize, Serialize};

/// Address for a local-filesystem-backed remote.
///
/// Serializable for storage in the repository's remote configuration. The
/// string is a `file:` URL on native and an IndexedDB database name on the web
/// (see the [module docs](self)).
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsAddress {
    target: String,
}

impl FsAddress {
    /// Construct an address naming the directory: a `file:` URL on native, an
    /// IndexedDB database name on the web.
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
        }
    }

    /// The directory locator (a `file:` URL on native, an IndexedDB database
    /// name on the web).
    pub fn target(&self) -> &str {
        &self.target
    }
}

impl SiteAddress for FsAddress {
    type Site = Fs;
}

impl From<FsAddress> for SiteId {
    fn from(address: FsAddress) -> Self {
        address.target.into()
    }
}

/// Resolve an address to the [`FileSystem`] rooted at the directory it names.
#[cfg(not(target_arch = "wasm32"))]
async fn open(address: &FsAddress) -> Result<FileSystem, AuthorizeError> {
    // Native: the file: URL is self-contained — open the directory directly.
    let handle = dialog_storage::provider::FileSystemHandle::try_from(address.target())
        .map_err(|e| AuthorizeError::Configuration(format!("invalid directory URL: {e}")))?;
    Ok(FileSystem::from(handle))
}

/// Resolve an address to the [`FileSystem`] rooted at the directory it names.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
async fn open(address: &FsAddress) -> Result<FileSystem, AuthorizeError> {
    // Web: the address is an IndexedDB database holding the directory handle
    // (a file: URL can't carry a live FileSystemDirectoryHandle).
    FileSystem::open_web(address.target())
        .await
        .map_err(|e| AuthorizeError::Configuration(format!("opening directory: {e}")))
}

/// Verify the resolved directory is the space for the invocation's subject:
/// its stored `credential/key/self` DID must match. This is the local stand-in
/// for the UCAN access service's check before it hands back a presigned
/// operation.
async fn verify_subject<Fx>(
    filesystem: &FileSystem,
    capability: &Capability<Fx>,
) -> Result<(), AuthorizeError>
where
    Fx: Effect,
    Fx::Of: Constraint,
    Capability<Fx>: Ability,
{
    // Read the directory's own identity, the same way Repository load derives a
    // space's subject from its stored credential. The chain subject is a
    // placeholder — key("self") reads credential/key/self regardless.
    //
    // A missing/unreadable credential is a *precondition* failure, not a denial:
    // the directory isn't a space yet (it must be created with
    // `Repository::create` first), so it surfaces as a configuration error.
    let credential = did!("local:storage")
        .credential()
        .key(SELF)
        .load()
        .perform(filesystem)
        .await
        .map_err(|e| {
            AuthorizeError::Configuration(format!(
                "directory is not an initialized space (no readable credential/key/self): {e}"
            ))
        })?;

    // A subject the directory is not the space for IS a denial.
    let expected = capability.subject();
    let actual = credential.did();
    if &actual != expected {
        return Err(AuthorizeError::Denied(format!(
            "directory is the space for {actual}, not the invocation subject {expected}",
        )));
    }
    Ok(())
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx, Env> SiteFork<Env> for FsFork<Fx>
where
    Fx: Effect + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    Capability<Fx>: Ability,
    Env: ConditionalSync,
    FsFork<Fx>: ConditionalSend,
{
    type Site = Fs;
    type Effect = Fx;

    async fn authorize(self, _env: &Env) -> Result<ForkInvocation<Fs, Fx>, AuthorizeError> {
        let filesystem = open(self.0.address()).await?;
        verify_subject(&filesystem, self.0.capability()).await?;
        Ok(self.0.attest(FsAuthorization::new(filesystem)))
    }
}
