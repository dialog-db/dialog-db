//! Memory capability types and Provider implementations for S3 backend.
//!
//! Re-exports memory types from [`dialog_effects`] and implements
//! `Provider<Fork<S3, Fx>>` for [`S3`].

pub use dialog_effects::memory::*;

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::fork::{Fork, ForkInvocation};

use crate::s3::{RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<S3, Resolve>> for S3 {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, Resolve>,
    ) -> Result<Option<Publication>, MemoryError> {
        let request = invocation
            .address
            .authorize(
                &invocation.authorization.capability,
                invocation.credentials.as_ref(),
            )
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

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
impl Provider<Fork<S3, Publish>> for S3 {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, Publish>,
    ) -> Result<Vec<u8>, MemoryError> {
        let content = Publish::of(&invocation.authorization.capability)
            .content
            .clone();
        let when = Publish::of(&invocation.authorization.capability)
            .when
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).to_string());

        let request = invocation
            .address
            .authorize(
                &invocation.authorization.capability,
                invocation.credentials.as_ref(),
            )
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

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
impl Provider<Fork<S3, Retract>> for S3 {
    async fn execute(&self, invocation: ForkInvocation<S3, Retract>) -> Result<(), MemoryError> {
        let when = String::from_utf8_lossy(&Retract::of(&invocation.authorization.capability).when)
            .to_string();

        let request = invocation
            .address
            .authorize(
                &invocation.authorization.capability,
                invocation.credentials.as_ref(),
            )
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

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
