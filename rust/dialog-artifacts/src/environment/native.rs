//! Native environment — filesystem storage with profile credentials.

use std::io::ErrorKind;
use std::path::Path;

use dialog_credentials::{Ed25519Signer, key::KeyExport};

use crate::Credentials;
use dialog_storage::provider::FileSystem;
use tokio::fs;

use super::{Environment, OpenError, Remote};
use crate::Operator;

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

    let storage =
        FileSystem::mount(dialog_dir.clone()).map_err(|e| OpenError::Storage(e.to_string()))?;

    let profile_signer = load_or_create_profile_key(&profile.name, &dialog_dir).await?;
    let operator = derive_operator(&profile_signer, &profile.operator).await?;
    let credentials = Credentials::new(&profile.name, profile_signer, operator);

    Ok(Environment::new(credentials, storage, Remote))
}

async fn load_or_create_profile_key(name: &str, root: &Path) -> Result<Ed25519Signer, OpenError> {
    let key_path = root.join("profiles").join(name).join("key");

    match fs::read(&key_path).await {
        Ok(data) if data.len() == 32 => {
            let seed: [u8; 32] = data
                .try_into()
                .map_err(|_| OpenError::Key("invalid seed length".into()))?;
            Ed25519Signer::import(&seed)
                .await
                .map_err(|e| OpenError::Key(e.to_string()))
        }
        Ok(data) => Err(OpenError::Key(format!(
            "profile key has invalid length: {} (expected 32)",
            data.len()
        ))),
        Err(e) if e.kind() == ErrorKind::NotFound => {
            let signer = Ed25519Signer::generate()
                .await
                .map_err(|e| OpenError::Key(e.to_string()))?;

            let KeyExport::Extractable(ref bytes) = signer
                .export()
                .await
                .map_err(|e| OpenError::Key(e.to_string()))?;

            if let Some(parent) = key_path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|e| OpenError::Storage(e.to_string()))?;
            }
            fs::write(&key_path, bytes)
                .await
                .map_err(|e| OpenError::Storage(e.to_string()))?;

            Ok(signer)
        }
        Err(e) => Err(OpenError::Storage(e.to_string())),
    }
}

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

            let derived = blake3::keyed_hash(
                &<[u8; 32]>::try_from(seed.as_slice())
                    .map_err(|_| OpenError::Key("invalid profile seed".into()))?,
                context,
            );
            Ed25519Signer::import(derived.as_bytes())
                .await
                .map_err(|e| OpenError::Key(e.to_string()))
        }
    }
}
