//! Web environment — IndexedDb storage with profile credentials.

use dialog_credentials::{Ed25519Signer, key::KeyExport};
use dialog_storage::provider::IndexedDb;

use super::{Environment, OpenError, Remote};
use crate::credentials::open::Open;
use crate::{Credentials, Operator};

/// Web environment with opened profile credentials and remote dispatch.
pub type WebEnvironment = Environment<Credentials, IndexedDb, Remote>;

/// Open a fully-configured web environment from a profile descriptor.
pub async fn open(profile: crate::Profile) -> Result<WebEnvironment, OpenError> {
    let storage = IndexedDb::new();

    let profile_signer = Open::new(&profile.name)
        .perform(&storage)
        .await
        .map_err(|e| OpenError::Key(e.to_string()))?;

    let operator = derive_operator(profile_signer.signer(), &profile.operator).await?;
    let credentials = Credentials::new(&profile.name, profile_signer.into_signer(), operator);

    Ok(Environment::new(credentials, storage, Remote))
}

async fn derive_operator(
    _profile: &Ed25519Signer,
    strategy: &Operator,
) -> Result<Ed25519Signer, OpenError> {
    match strategy {
        Operator::Unique => Ed25519Signer::generate()
            .await
            .map_err(|e| OpenError::Key(e.to_string())),
        Operator::Derived(_) => Err(OpenError::Key(
            "derived operators not yet supported on web (non-extractable keys)".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Profile;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    #[dialog_common::test]
    async fn profile_open_creates_key() {
        let storage = IndexedDb::new();

        let profile = Open::new("test-create").perform(&storage).await.unwrap();
        assert!(
            !profile.did().to_string().is_empty(),
            "should produce a valid DID"
        );
    }

    #[dialog_common::test]
    async fn profile_open_returns_same_key_on_reload() {
        let storage = IndexedDb::new();

        let first = Open::new("test-reload").perform(&storage).await.unwrap();
        let second = Open::new("test-reload").perform(&storage).await.unwrap();

        assert_eq!(
            first.did(),
            second.did(),
            "same profile should produce same DID"
        );
    }

    #[dialog_common::test]
    async fn profile_open_different_names_produce_different_keys() {
        let storage = IndexedDb::new();

        let work = Open::new("test-work").perform(&storage).await.unwrap();
        let personal = Open::new("test-personal").perform(&storage).await.unwrap();

        assert_ne!(
            work.did(),
            personal.did(),
            "different profiles should have different keys"
        );
    }
}
