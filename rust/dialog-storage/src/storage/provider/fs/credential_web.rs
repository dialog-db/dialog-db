//! Web `Load<Credential>` for the FS provider: read a directory's identity.
//!
//! A web signer credential is a non-extractable WebCrypto key, so it can't be
//! saved or fully imported from the byte-compatible on-disk form. But verifying
//! that a directory IS the space for a given subject only needs the directory's
//! DID, which is derivable from the public-key bytes alone. This provider reads
//! `credential/key/{address}` and returns a DID-only verifier credential via
//! [`Credential::identity`]. `Save<Credential>` and `Secret` remain the
//! IndexedDb provider's job on the web.

use super::FileSystem;
use dialog_capability::{Capability, Provider};
use dialog_credentials::Credential;
use dialog_effects::credential::prelude::LoadCredentialExt;
use dialog_effects::credential::{CredentialError, Load};

const CREDENTIAL: &str = "credential";
const KEY: &str = "key";

#[async_trait::async_trait(?Send)]
impl Provider<Load<Credential>> for FileSystem {
    async fn execute(
        &self,
        input: Capability<Load<Credential>>,
    ) -> Result<Credential, CredentialError> {
        let handle = self
            .resolve(CREDENTIAL)
            .and_then(|c| c.resolve(KEY))
            .and_then(|c| c.resolve(input.address()))
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        let bytes = handle
            .read()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        Credential::identity(&bytes).map_err(|e| CredentialError::Corrupted(e.to_string()))
    }
}
