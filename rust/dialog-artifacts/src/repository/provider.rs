//! Provider implementations for repository Load/Save capabilities.
//!
//! Uses a [`Storage`] newtype wrapper around the platform storage backend
//! to satisfy orphan rules.

use std::ops::Deref;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod web;

/// Newtype wrapper around a storage backend reference that implements
/// repository capabilities (`Provider<Load>`, `Provider<Save>`).
///
/// Wraps a reference so it can be constructed from a borrow without ownership.
pub struct Storage<'a, T>(pub &'a T);

impl<T> Deref for Storage<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.0
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::Storage;
    use dialog_capability::Subject;
    use dialog_capability::did;
    use dialog_credentials::Ed25519Signer;
    use dialog_effects::repository::{self, Credential};
    use dialog_storage::provider::FileSystem;
    use dialog_varsig::Principal;

    fn repo_load(name: &str) -> dialog_capability::Capability<repository::Load> {
        Subject::from(did!("key:z6MkTest"))
            .attenuate(repository::Repository)
            .attenuate(repository::Name::new(name))
            .invoke(repository::Load)
    }

    fn repo_save(
        name: &str,
        credential: Credential,
    ) -> dialog_capability::Capability<repository::Save> {
        Subject::from(did!("key:z6MkTest"))
            .attenuate(repository::Repository)
            .attenuate(repository::Name::new(name))
            .invoke(repository::Save::new(credential))
    }

    #[dialog_common::test]
    async fn load_returns_none_for_missing_repo() {
        let dir = tempfile::tempdir().unwrap();
        let fs = FileSystem::mount(dir.path().to_path_buf()).unwrap();

        let result = repo_load("nonexistent")
            .perform(&Storage(&fs))
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[dialog_common::test]
    async fn save_and_load_signer_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let fs = FileSystem::mount(dir.path().to_path_buf()).unwrap();

        let signer = Ed25519Signer::generate().await.unwrap();
        let did = signer.did();

        repo_save("home", signer.into())
            .perform(&Storage(&fs))
            .await
            .unwrap();

        let loaded = repo_load("home")
            .perform(&Storage(&fs))
            .await
            .unwrap()
            .expect("should find saved repo");

        assert_eq!(loaded.did(), did);
        assert!(matches!(loaded, Credential::Signer(_)));
    }

    #[dialog_common::test]
    async fn save_and_load_verifier_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let fs = FileSystem::mount(dir.path().to_path_buf()).unwrap();

        let signer = Ed25519Signer::generate().await.unwrap();
        let verifier = signer.ed25519_did().clone();
        let did = verifier.did();

        repo_save("guest", verifier.into())
            .perform(&Storage(&fs))
            .await
            .unwrap();

        let loaded = repo_load("guest")
            .perform(&Storage(&fs))
            .await
            .unwrap()
            .expect("should find saved repo");

        assert_eq!(loaded.did(), did);
        assert!(matches!(loaded, Credential::Verifier(_)));
    }

    #[dialog_common::test]
    async fn different_names_are_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let fs = FileSystem::mount(dir.path().to_path_buf()).unwrap();

        let signer1 = Ed25519Signer::generate().await.unwrap();
        let signer2 = Ed25519Signer::generate().await.unwrap();

        repo_save("repo1", signer1.clone().into())
            .perform(&Storage(&fs))
            .await
            .unwrap();
        repo_save("repo2", signer2.clone().into())
            .perform(&Storage(&fs))
            .await
            .unwrap();

        let loaded1 = repo_load("repo1")
            .perform(&Storage(&fs))
            .await
            .unwrap()
            .unwrap();
        let loaded2 = repo_load("repo2")
            .perform(&Storage(&fs))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(loaded1.did(), signer1.did());
        assert_eq!(loaded2.did(), signer2.did());
        assert_ne!(loaded1.did(), loaded2.did());
    }
}
