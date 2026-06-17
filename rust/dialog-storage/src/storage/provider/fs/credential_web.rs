//! Web credential identity for the FS provider.
//!
//! A web signer credential is a non-extractable WebCrypto key, so its *private*
//! material can't be written to or recovered from the byte-compatible on-disk
//! form. But a directory's **identity** — its public key, hence its
//! [`Did`](dialog_varsig::Did) — is byte-serializable on every target. These
//! providers persist and read just that identity at `credential/key/{address}`,
//! which is all a subject check (and space mounting) needs:
//!
//! - [`Save<Credential>`] writes the credential's public identity via
//!   [`Credential::to_identity_bytes`]. Saving a signer on the web therefore
//!   stores only its verifier — the private key is not persisted (it can't be).
//! - [`Load<Credential>`] reads those bytes back as a DID-only verifier via
//!   [`Credential::identity`].
//!
//! `Secret` storage remains the IndexedDb provider's job on the web.

use super::FileSystem;
use dialog_capability::{Capability, Provider};
use dialog_credentials::Credential;
use dialog_effects::credential::prelude::{LoadCredentialExt, SaveCredentialExt};
use dialog_effects::credential::{CredentialError, Load, Save};

const CREDENTIAL: &str = "credential";
const KEY: &str = "key";

impl FileSystem {
    /// Handle for the credential key at `credential/key/{address}`.
    fn credential_key(&self, address: &str) -> Result<super::FileSystemHandle, CredentialError> {
        self.resolve(CREDENTIAL)
            .and_then(|c| c.resolve(KEY))
            .and_then(|c| c.resolve(address))
            .map_err(|e| CredentialError::Storage(e.to_string()))
    }
}

#[async_trait::async_trait(?Send)]
impl Provider<Load<Credential>> for FileSystem {
    async fn execute(
        &self,
        input: Capability<Load<Credential>>,
    ) -> Result<Credential, CredentialError> {
        let bytes = self
            .credential_key(input.address())?
            .read()
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))?;
        Credential::identity(&bytes).map_err(|e| CredentialError::Corrupted(e.to_string()))
    }
}

#[async_trait::async_trait(?Send)]
impl Provider<Save<Credential>> for FileSystem {
    async fn execute(&self, input: Capability<Save<Credential>>) -> Result<(), CredentialError> {
        // Only the public identity is persisted; a non-extractable web signer
        // cannot be stored, and the identity is all the FS layout needs.
        let bytes = input.credential().to_identity_bytes();
        self.credential_key(input.address())?
            .write(&bytes)
            .await
            .map_err(|e| CredentialError::Storage(e.to_string()))
    }
}
