//! Browser (wasm) tests for the FS-remote site's authorization path.
//!
//! Exercises the web-specific machinery end to end: a directory handle is
//! obtained from OPFS, given an on-disk identity, persisted in IndexedDB, then
//! reached through an [`FsAddress`] naming that database. Running a capability
//! drives the real `authorize` — opening the directory via `open_web`, reading
//! its `credential/key/self` DID, and verifying it matches the invocation
//! subject — which is the surface the native tests can't cover.

#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

use dialog_capability::Subject;
use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
use dialog_effects::archive::prelude::*;
use dialog_remote_fs::FsAddress;
use dialog_remote_fs::helpers::FsNetwork;
use dialog_storage::provider::{WebRoot, register_web_directory};
use dialog_storage::unique_name;
use dialog_varsig::Principal;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Set up a directory in OPFS that is the space for a fresh signer, persist its
/// handle in IndexedDB, and return the address naming it plus the subject.
async fn setup() -> (FsNetwork, FsAddress, Subject) {
    // OPFS gives a real FileSystemDirectoryHandle without a user gesture.
    let root = WebRoot::opfs(&unique_name("web-fs-authorize"))
        .await
        .expect("OPFS root available in the worker test runner");
    let handle = root.handle();
    let filesystem = root.provider();

    // Make the directory the space for `signer` by writing its identity to
    // credential/key/self, in the byte-compatible storage form.
    let signer = Ed25519Signer::generate().await.unwrap();
    let did = Principal::did(&signer);
    let credential = Credential::Signer(SignerCredential::from(signer));
    filesystem
        .resolve("credential")
        .and_then(|c| c.resolve("key"))
        .and_then(|c| c.resolve("self"))
        .expect("resolve credential/key/self")
        .write(&credential.to_identity_bytes())
        .await
        .expect("write directory identity");

    // Persist the handle under an IndexedDB database; that database name is the
    // address. open_web reads the handle back from it.
    let db = unique_name("web-fs-db");
    register_web_directory(&db, handle)
        .await
        .expect("register directory handle");

    (FsNetwork::new(), FsAddress::new(db), Subject::from(did))
}

#[dialog_common::test]
async fn it_writes_and_reads_back_a_blob() -> anyhow::Result<()> {
    let (network, address, subject) = setup().await;
    let content = b"hello web fs-remote".to_vec();
    let digest = dialog_common::Blake3Hash::hash(&content);

    subject
        .clone()
        .archive()
        .catalog("index")
        .put(content.clone())
        .fork(&address)
        .perform(&network)
        .await?;

    let result = subject
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&address)
        .perform(&network)
        .await?;
    assert_eq!(result, Some(content));
    Ok(())
}

#[dialog_common::test]
async fn it_returns_none_for_missing_blob() -> anyhow::Result<()> {
    let (network, address, subject) = setup().await;
    let digest = dialog_common::Blake3Hash::hash(b"never written");

    let result = subject
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&address)
        .perform(&network)
        .await?;
    assert_eq!(result, None);
    Ok(())
}

#[dialog_common::test]
async fn it_denies_when_subject_is_not_the_directory() -> anyhow::Result<()> {
    let (network, address, _subject) = setup().await;
    // A different subject than the one the directory is the space for.
    let stranger = Subject::from(Principal::did(&Ed25519Signer::generate().await.unwrap()));
    let digest = dialog_common::Blake3Hash::hash(b"anything");

    let result = stranger
        .archive()
        .catalog("index")
        .get(digest)
        .fork(&address)
        .perform(&network)
        .await;
    assert!(result.is_err(), "mismatched subject must be denied");
    Ok(())
}
