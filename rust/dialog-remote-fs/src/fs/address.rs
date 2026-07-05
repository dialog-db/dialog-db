//! FS-remote address types.
//!
//! An [`FsAddress`] names the local directory that backs a space, as a
//! [`Location`] — the same target-agnostic locator the rest of the system uses
//! to open storage. Authorization is the local analogue of the UCAN site's
//! presign endpoint: [`authorize`](FsFork::authorize) proves the operator holds
//! a delegation for the requested effect, opens the directory, verifies it
//! really is the space for the invocation's subject (its stored
//! `credential/key/self` DID matches), and hands the resolved [`FileSystem`] to
//! the provider to act on.
//!
//! Because the address is a [`Location`], the directory is opened the same way
//! on every target via [`FileSystem::open`] (a path under the platform layout on
//! native, an OPFS subdirectory in the browser) — no platform-specific locator.

use super::{Fs, FsAuthorization, FsFork};
use dialog_capability::access::{Access, AuthorizeError, FromCapability, Protocol, Prove};
use dialog_capability::{Ability, Capability, Constraint, Effect, ForkInvocation, Principal};
use dialog_capability::{Provider, SiteAddress, SiteFork, SiteId, Subject, did};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::authority::{self, OperatorExt};
use dialog_effects::credential::SELF;
use dialog_effects::credential::prelude::*;
use dialog_effects::storage::Location;
use dialog_storage::provider::FileSystem;
use dialog_storage::resource::Resource;
use dialog_ucan::Ucan;
use serde::{Deserialize, Serialize};

/// Address for a local-filesystem-backed remote: the [`Location`] of the
/// directory that backs the space.
///
/// Serializable for storage in the repository's remote configuration. The
/// [`Location`] resolves to a platform directory through [`FileSystem::open`],
/// so the same address works on native and the web.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FsAddress {
    location: Location,
}

impl FsAddress {
    /// Construct an address naming the directory at `location`.
    pub fn new(location: Location) -> Self {
        Self { location }
    }

    /// The [`Location`] of the directory.
    pub fn location(&self) -> &Location {
        &self.location
    }
}

impl SiteAddress for FsAddress {
    type Site = Fs;
}

impl From<FsAddress> for SiteId {
    fn from(address: FsAddress) -> Self {
        let Location { directory, name } = address.location;
        format!("{directory:?}/{name}").into()
    }
}

/// Resolve an address to the [`FileSystem`] rooted at the directory it names.
///
/// The [`Location`] opens to a platform directory the same way on every target.
async fn open(address: &FsAddress) -> Result<FileSystem, AuthorizeError> {
    FileSystem::open(address.location())
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

/// Prove the operator holds a delegation for the requested effect.
///
/// This is the local analogue of the UCAN site's authorization step: rather
/// than sign an invocation for a remote access service, we prove against the
/// operator's own stored delegations (the same `profile.access().save(chain)`
/// certificates [`Prove`] walks). The scope is derived from the capability, so
/// it carries the exact ability path (`/archive/get` vs `/archive/put`, etc.):
/// a delegation granting only read fails to prove a write by command-prefix
/// mismatch, gating per-effect access for free. A self-owned operator (it *is*
/// the subject) proves instantly.
async fn prove_effect<Fx, Env>(env: &Env, capability: &Capability<Fx>) -> Result<(), AuthorizeError>
where
    Fx: Effect + Clone + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: Ability,
    Env: Provider<authority::Identify> + Provider<Prove<Ucan>> + ConditionalSync,
{
    let identity = authority::Identify
        .perform(env)
        .await
        .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;
    let profile = identity.profile().clone();
    let operator = identity.did();

    let scope = <Ucan as Protocol>::Access::from_capability(capability);

    Subject::from(profile)
        .attenuate(Access)
        .invoke(Prove::<Ucan>::new(operator, scope))
        .perform(env)
        .await
        .map(|_proof| ())
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx, Env> SiteFork<Env> for FsFork<Fx>
where
    Fx: Effect + Clone + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    Capability<Fx>: Ability,
    Env: Provider<authority::Identify> + Provider<Prove<Ucan>> + ConditionalSync,
    FsFork<Fx>: ConditionalSend,
{
    type Site = Fs;
    type Effect = Fx;

    async fn authorize(self, env: &Env) -> Result<ForkInvocation<Fs, Fx>, AuthorizeError> {
        // Prove the operator may perform this effect for the subject, gating
        // read vs write by the delegation it holds.
        prove_effect(env, self.0.capability()).await?;
        // Resolve the directory and verify it really is that subject's space.
        let filesystem = open(self.0.address()).await?;
        verify_subject(&filesystem, self.0.capability()).await?;
        Ok(self.0.attest(FsAuthorization::new(filesystem)))
    }
}
