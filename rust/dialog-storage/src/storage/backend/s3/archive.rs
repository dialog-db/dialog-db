//! Archive capability types and Provider implementations for S3 backend.
//!
//! Re-exports archive types from [`dialog_effects`] and implements
//! `Provider<Authorization<Fx, AuthorizedRequest>>` for [`S3`].

pub use dialog_effects::archive::*;

use async_trait::async_trait;
use dialog_capability::{Authorization, Provider};
use dialog_s3_credentials::AuthorizedRequest;

use super::{RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorization<Get, AuthorizedRequest>> for S3 {
    async fn execute(
        &self,
        authorized: Authorization<Get, AuthorizedRequest>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        let request = authorized.into_site();

        let client = reqwest::Client::new();
        let response = request
            .into_request(&client)
            .send()
            .await
            .map_err(|e| ArchiveError::Io(e.to_string()))?;

        if response.status().is_success() {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| ArchiveError::Io(e.to_string()))?;
            Ok(Some(bytes.to_vec()))
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
impl Provider<Authorization<Put, AuthorizedRequest>> for S3 {
    async fn execute(
        &self,
        authorized: Authorization<Put, AuthorizedRequest>,
    ) -> Result<(), ArchiveError> {
        let content = Put::of(authorized.capability()).content.clone();
        let request = authorized.into_site();

        let client = reqwest::Client::new();
        let response = request
            .into_request(&client)
            .body(content)
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
