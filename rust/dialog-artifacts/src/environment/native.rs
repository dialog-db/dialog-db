//! Native environment — filesystem storage with profile credentials.

use std::path::Path;

use dialog_credentials::{Ed25519Signer, key::KeyExport};
use dialog_storage::provider::FileSystem;

use super::{Environment, OpenError, Remote};
use crate::credentials::open::Open;
use crate::{Credentials, Operator};

/// Domain separation context for deriving operator keys from profile keys.
const OPERATOR_DERIVATION_CONTEXT: &str = "dialog-db operator derivation";

/// Native environment with opened profile credentials and remote dispatch.
pub type NativeEnvironment = Environment<Credentials, FileSystem, Remote>;

/// Open a fully-configured native environment from a profile descriptor.
///
/// Uses `dirs::data_dir()/dialog` for storage. Opens or creates the profile
/// keypair, derives the operator, and assembles the environment.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use dialog_artifacts::{Profile, Operator, environment};
///
/// let env = environment::open(Profile::default()).await?;
/// # Ok(())
/// # }
/// ```
pub async fn open(profile: crate::Profile) -> Result<NativeEnvironment, OpenError> {
    let data_dir = dirs::data_dir()
        .ok_or_else(|| OpenError::Storage("could not determine data directory".into()))?;
    let dialog_dir = data_dir.join("dialog");
    open_at(profile, &dialog_dir).await
}

async fn open_at(profile: crate::Profile, root: &Path) -> Result<NativeEnvironment, OpenError> {
    let storage =
        FileSystem::mount(root.to_path_buf()).map_err(|e| OpenError::Storage(e.to_string()))?;

    let profile_signer = Open::new(&profile.name)
        .perform(&storage)
        .await
        .map_err(|e| OpenError::Key(e.to_string()))?;

    let operator = derive_operator(profile_signer.signer(), &profile.operator).await?;
    let credentials = Credentials::new(&profile.name, profile_signer.into_signer(), operator);

    Ok(Environment::new(credentials, storage, Remote))
}

/// Derive or generate an operator key from a profile signer.
///
/// - `Operator::Unique` — generates a random ephemeral keypair
/// - `Operator::Derived(context)` — derives deterministically using
///   `blake3::derive_key` with the profile seed and context
async fn derive_operator(
    profile: &Ed25519Signer,
    strategy: &Operator,
) -> Result<Ed25519Signer, OpenError> {
    match strategy {
        Operator::Unique => Ed25519Signer::generate()
            .await
            .map_err(|e| OpenError::Key(e.to_string())),
        Operator::Derived(context) => {
            let KeyExport::Extractable(ref seed) = profile
                .export()
                .await
                .map_err(|e| OpenError::Key(e.to_string()))?;

            // Concatenate profile seed + context for the key material
            let mut key_material = seed.clone();
            key_material.extend_from_slice(context);

            let derived = blake3::derive_key(OPERATOR_DERIVATION_CONTEXT, &key_material);
            Ed25519Signer::import(&derived)
                .await
                .map_err(|e| OpenError::Key(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Profile;

    #[dialog_common::test]
    async fn profile_open_creates_key_on_first_run() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileSystem::mount(dir.path().to_path_buf()).unwrap();

        let profile = Open::new("default").perform(&storage).await.unwrap();
        assert!(
            !profile.did().to_string().is_empty(),
            "should produce a valid DID"
        );
    }

    #[dialog_common::test]
    async fn profile_open_returns_same_key_on_reload() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileSystem::mount(dir.path().to_path_buf()).unwrap();

        let first = Open::new("default").perform(&storage).await.unwrap();
        let second = Open::new("default").perform(&storage).await.unwrap();

        assert_eq!(
            first.did(),
            second.did(),
            "same profile should produce same DID"
        );
    }

    #[dialog_common::test]
    async fn profile_open_different_names_produce_different_keys() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileSystem::mount(dir.path().to_path_buf()).unwrap();

        let work = Open::new("work").perform(&storage).await.unwrap();
        let personal = Open::new("personal").perform(&storage).await.unwrap();

        assert_ne!(
            work.did(),
            personal.did(),
            "different profiles should have different keys"
        );
    }

    #[dialog_common::test]
    async fn environment_open_produces_different_profile_and_operator() {
        let dir = tempfile::tempdir().unwrap();
        let env = open_at(Profile::default(), dir.path()).await.unwrap();

        assert_ne!(
            env.credentials.profile_did(),
            env.credentials.operator_did(),
            "profile and operator should be different keys (Operator::Unique)"
        );
    }

    #[dialog_common::test]
    async fn environment_open_preserves_profile_across_restarts() {
        let dir = tempfile::tempdir().unwrap();

        let env1 = open_at(Profile::default(), dir.path()).await.unwrap();
        let env2 = open_at(Profile::default(), dir.path()).await.unwrap();

        assert_eq!(
            env1.credentials.profile_did(),
            env2.credentials.profile_did(),
            "profile DID should persist"
        );
    }

    #[dialog_common::test]
    async fn environment_open_unique_operator_differs_each_time() {
        let dir = tempfile::tempdir().unwrap();

        let env1 = open_at(Profile::default(), dir.path()).await.unwrap();
        let env2 = open_at(Profile::default(), dir.path()).await.unwrap();

        assert_ne!(
            env1.credentials.operator_did(),
            env2.credentials.operator_did(),
            "Operator::Unique should differ each time"
        );
    }

    #[dialog_common::test]
    async fn environment_open_derived_operator_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();

        let env1 = open_at(
            Profile::named("default").operated_by(Operator::derived(b"alice")),
            dir.path(),
        )
        .await
        .unwrap();

        let env2 = open_at(
            Profile::named("default").operated_by(Operator::derived(b"alice")),
            dir.path(),
        )
        .await
        .unwrap();

        assert_eq!(
            env1.credentials.operator_did(),
            env2.credentials.operator_did(),
            "same context should produce same operator"
        );
    }

    #[dialog_common::test]
    async fn environment_open_different_contexts_produce_different_operators() {
        let dir = tempfile::tempdir().unwrap();

        let env1 = open_at(
            Profile::named("default").operated_by(Operator::derived(b"alice")),
            dir.path(),
        )
        .await
        .unwrap();

        let env2 = open_at(
            Profile::named("default").operated_by(Operator::derived(b"bob")),
            dir.path(),
        )
        .await
        .unwrap();

        assert_ne!(
            env1.credentials.operator_did(),
            env2.credentials.operator_did(),
            "different contexts should produce different operators"
        );
    }
}
