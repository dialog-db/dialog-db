//! Shared setup for the FS-remote integration tests.
//!
//! Builds a tempdir that is a valid space for a generated signer (its
//! `credential/key/self` holds that signer's credential), the matching
//! `file:`-URL [`FsAddress`], and an [`FsNetwork`] that drives the real
//! `authorize` path (including the subject-verification). Native-only: the
//! integration tests it serves use a tempdir-backed vault.

#![cfg(not(target_arch = "wasm32"))]
#![allow(dead_code)]

use dialog_capability::Subject;
use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
use dialog_effects::credential::prelude::*;
use dialog_effects::storage::{Directory, Location};
use dialog_remote_fs::FsAddress;
use dialog_remote_fs::helpers::FsNetwork;
use dialog_storage::provider::FileSystem;
use dialog_storage::resource::Resource;
use dialog_varsig::Principal;
use tempfile::TempDir;

/// A tempdir vault, the FS-remote network, the `file:` URL address naming it,
/// and the subject the vault belongs to (whose credential is stored in it).
pub struct Setup {
    pub tmp: TempDir,
    pub network: FsNetwork,
    pub address: FsAddress,
    pub subject: Subject,
}

/// Open a fresh FS-remote test environment over a tempdir that is the space for
/// a freshly generated signer.
pub async fn setup() -> Setup {
    let tmp = tempfile::tempdir().unwrap();
    let filesystem = open_at(tmp.path()).await;

    // Make the directory the space for `signer`: store its credential at
    // credential/key/self, exactly as Repository::create would.
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
        address: FsAddress::new(file_url(tmp.path())),
        network: FsNetwork::new(),
        subject: Subject::from(did),
        tmp,
    }
}

/// Open a `dialog_storage::FileSystem` rooted at the given directory.
pub async fn open_at(path: &std::path::Path) -> FileSystem {
    let location = Location::new(Directory::At(path.to_string_lossy().into_owned()), "");
    FileSystem::open(&location).await.unwrap()
}

/// The `file:` URL for a directory path.
pub fn file_url(path: &std::path::Path) -> String {
    url::Url::from_file_path(path)
        .expect("tempdir path is absolute")
        .to_string()
}
