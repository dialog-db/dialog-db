//! Memory capability types and Provider implementations for S3 backend.
//!
//! Re-exports memory types from [`dialog_effects`] and implements
//! `Provider<Authorization<Fx, AuthorizedRequest>>` for [`S3`].

pub use dialog_effects::memory::*;

use async_trait::async_trait;
use dialog_capability::{Authorization, Provider};
use dialog_s3_credentials::AuthorizedRequest;

use super::{RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorization<Resolve, AuthorizedRequest>> for S3 {
    async fn execute(
        &self,
        authorized: Authorization<Resolve, AuthorizedRequest>,
    ) -> Result<Option<Publication>, MemoryError> {
        let request = authorized.into_site();

        let client = reqwest::Client::new();
        let response = request
            .into_request(&client)
            .send()
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        if response.status().is_success() {
            let edition = response
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim_matches('"').to_string())
                .ok_or_else(|| MemoryError::Storage("Response missing ETag header".to_string()))?;

            let bytes = response
                .bytes()
                .await
                .map_err(|e| MemoryError::Storage(e.to_string()))?;

            Ok(Some(Publication {
                content: bytes.to_vec(),
                edition: edition.into_bytes(),
            }))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(MemoryError::Storage(format!(
                "Failed to resolve value: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorization<Publish, AuthorizedRequest>> for S3 {
    async fn execute(
        &self,
        authorized: Authorization<Publish, AuthorizedRequest>,
    ) -> Result<Vec<u8>, MemoryError> {
        let content = Publish::of(authorized.capability()).content.clone();
        let when = Publish::of(authorized.capability())
            .when
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).to_string());
        let request = authorized.into_site();

        let client = reqwest::Client::new();
        let response = request
            .into_request(&client)
            .body(content)
            .send()
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        match response.status() {
            status if status.is_success() => {
                let new_edition = response
                    .headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.trim_matches('"').to_string())
                    .ok_or_else(|| {
                        MemoryError::Storage("Response missing ETag header".to_string())
                    })?;
                Ok(new_edition.into_bytes())
            }
            reqwest::StatusCode::PRECONDITION_FAILED => Err(MemoryError::EditionMismatch {
                expected: when,
                actual: None,
            }),
            status => Err(MemoryError::Storage(format!(
                "Failed to publish value: {}",
                status
            ))),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorization<Retract, AuthorizedRequest>> for S3 {
    async fn execute(
        &self,
        authorized: Authorization<Retract, AuthorizedRequest>,
    ) -> Result<(), MemoryError> {
        let when = String::from_utf8_lossy(&Retract::of(authorized.capability()).when).to_string();
        let request = authorized.into_site();

        let client = reqwest::Client::new();
        let response = request
            .into_request(&client)
            .send()
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        match response.status() {
            status if status.is_success() => Ok(()),
            reqwest::StatusCode::PRECONDITION_FAILED => Err(MemoryError::EditionMismatch {
                expected: Some(when),
                actual: None,
            }),
            status => Err(MemoryError::Storage(format!(
                "Failed to retract value: {}",
                status
            ))),
        }
    }
}
