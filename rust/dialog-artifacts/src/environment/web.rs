//! Web environment — IndexedDb storage with profile credentials.

use dialog_credentials::{Ed25519Signer, OpenedProfile};
use dialog_storage::provider::IndexedDb;

use super::{Environment, OpenError, Remote};
use crate::Operator;

/// Web environment with opened profile credentials and remote dispatch.
pub type WebEnvironment = Environment<OpenedProfile, IndexedDb, Remote>;

/// Open a fully-configured web environment from a profile descriptor.
pub async fn open(profile: crate::Profile) -> Result<WebEnvironment, OpenError> {
    let storage = IndexedDb::new();

    // On web, profile keys are generated fresh (persistence via IndexedDb TBD)
    let profile_key = Ed25519Signer::generate()
        .await
        .map_err(|e| OpenError::Key(e.to_string()))?;

    let operator = match &profile.operator {
        Operator::Unique => Ed25519Signer::generate()
            .await
            .map_err(|e| OpenError::Key(e.to_string()))?,
        Operator::Derived(_) => {
            return Err(OpenError::Key(
                "derived operators not yet supported on web".into(),
            ));
        }
    };

    let credentials = OpenedProfile::new(&profile.name, profile_key, operator);

    Ok(Environment::new(credentials, storage, Remote))
}
