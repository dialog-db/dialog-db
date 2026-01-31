//! Archive capability types and Provider implementations for S3 backend.
//!
//! Re-exports archive types from [`dialog_effects`] and implements
//! `Provider<Get>` and `Provider<Put>` for [`S3`].

pub use dialog_effects::archive::*;

use async_trait::async_trait;
use dialog_capability::{Authority, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_s3_credentials::capability::archive::{Get as AuthorizeGet, Put as AuthorizePut};

use super::{Hasher, RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<Get> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(&mut self, input: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(Archive)
            .attenuate(Catalog::of(&input).clone())
            .invoke(AuthorizeGet {
                digest: Get::of(&input).digest.clone(),
            });

        // Acquire authorization and perform using self (which implements Access + Authority)
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| ArchiveError::AuthorizationError(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| ArchiveError::ExecutionError(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client);
        let response = builder
            .send()
            .await
            .map_err(|e| ArchiveError::Io(e.to_string()))?;

        if response.status().is_success() {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| ArchiveError::Io(e.to_string()))?;
            Ok(Some(bytes.to_vec().into()))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(ArchiveError::Storage(format!(
                "Failed to get value: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<Put> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(&mut self, input: Capability<Put>) -> Result<(), ArchiveError> {
        let Put { content, digest } = Put::of(&input);
        let checksum = Hasher::Sha256.checksum(content);

        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(Archive)
            .attenuate(Catalog::of(&input).clone())
            .invoke(AuthorizePut {
                digest: digest.clone(),
                checksum,
            });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| ArchiveError::AuthorizationError(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| ArchiveError::ExecutionError(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client).body(content.to_vec());
        let response = builder
            .send()
            .await
            .map_err(|e| ArchiveError::Io(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(ArchiveError::Storage(format!(
                "Failed to put value: {}",
                response.status()
            )))
        }
    }
}
