//! Shared, cross-target setup for the FS-remote provider integration tests.
//!
//! Builds a directory that is a valid space for a generated signer (its
//! `credential/key/self` holds that signer's credential), the [`FsAddress`]
//! naming it, the subject the directory belongs to, and a [`FileSystem`] rooted
//! at the same directory for byte-compat cross-checks.
//!
//! The vault is a [`Location`] opened through [`FileSystem::open`], the same way
//! the rest of the system opens storage — so the tests run on native (a path
//! under the platform temp dir) and on the web (an OPFS subdirectory) alike,
//! with no platform-specific setup.

#![allow(dead_code)]

use dialog_capability::{
    Ability, Capability, Constraint, Effect, Fork, ForkInvocation, Provider, Subject,
};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
use dialog_effects::credential::prelude::*;
use dialog_effects::storage::Location;
use dialog_remote_fs::{Fs, FsAddress, FsAuthorization};
use dialog_storage::provider::FileSystem;
use dialog_storage::resource::Resource;
use dialog_storage::unique_name;
use dialog_varsig::Principal;

/// A vault directory: the FS-remote address naming it, the subject it is the
/// space for, and a [`FileSystem`] rooted at the same directory (so tests can
/// read/write it directly to assert byte-compatibility).
pub struct Setup {
    pub address: FsAddress,
    pub subject: Subject,
    pub filesystem: FileSystem,
}

/// Open a fresh FS-remote test environment over a directory that is the space
/// for a freshly generated signer.
pub async fn setup() -> Setup {
    let location = Location::temp(unique_name("fs-remote"));
    let filesystem = FileSystem::open(&location).await.unwrap();

    // Make the directory the space for `signer`: store its credential at
    // credential/key/self, exactly as Repository::create would. On the web a
    // signer can't persist its non-extractable key, so this stores the public
    // identity, which is what the subject check reads.
    let signer = Ed25519Signer::generate().await.unwrap();
    let did = Principal::did(&signer);
    let credential = Credential::Signer(SignerCredential::from(signer));
    did.clone()
        .credential()
        .key("self")
        .save(credential)
        .perform(&filesystem)
        .await
        .unwrap();

    Setup {
        address: FsAddress::new(location),
        subject: Subject::from(did),
        filesystem,
    }
}

/// Run a forked capability against the directory its address names, exercising
/// the [`Fs`] provider directly.
///
/// Resolves the directory (its [`Location`] opens the same way on every target)
/// and hands it to the provider, standing in for the attested authorization an
/// Operator would produce. The env-bound `authorize`/`prove` path is covered by
/// the Operator-driven `e2e` tests.
pub async fn perform<Fx>(fork: Fork<Fs, Fx>) -> anyhow::Result<Fx::Output>
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: Ability,
    ForkInvocation<Fs, Fx>: ConditionalSend,
    Fs: Provider<ForkInvocation<Fs, Fx>> + ConditionalSync,
{
    let filesystem = FileSystem::open(fork.address().location()).await?;
    let invocation = fork.attest(FsAuthorization::new(filesystem));
    Ok(invocation.perform(&Fs).await)
}
