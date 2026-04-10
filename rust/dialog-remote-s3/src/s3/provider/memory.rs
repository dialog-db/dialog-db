//! Memory providers for S3.

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::fork::ForkInvocation;
use dialog_effects::memory::*;
use reqwest::StatusCode;

use crate::s3::{S3, S3Invocation};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<S3, Resolve>> for S3 {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, Resolve>,
    ) -> Result<Option<Publication>, MemoryError> {
        invocation
            .authorization
            .permit(&invocation.capability, &invocation.address)
            .await?
            .invoke(invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Resolve>> for S3 {
    async fn execute(
        &self,
        input: S3Invocation<Resolve>,
    ) -> Result<Option<Publication>, MemoryError> {
        let response = input
            .permit
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
        } else if response.status() == StatusCode::NOT_FOUND {
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
impl Provider<ForkInvocation<S3, Publish>> for S3 {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, Publish>,
    ) -> Result<Vec<u8>, MemoryError> {
        invocation
            .authorization
            .permit(&invocation.capability, &invocation.address)
            .await?
            .invoke(invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Publish>> for S3 {
    async fn execute(&self, input: S3Invocation<Publish>) -> Result<Vec<u8>, MemoryError> {
        let publish = input.capability.into_effect();
        let when = publish
            .when
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).to_string());

        let response = input
            .permit
            .upload(publish.content)
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
            StatusCode::PRECONDITION_FAILED => Err(MemoryError::EditionMismatch {
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
impl Provider<ForkInvocation<S3, Retract>> for S3 {
    async fn execute(&self, invocation: ForkInvocation<S3, Retract>) -> Result<(), MemoryError> {
        invocation
            .authorization
            .permit(&invocation.capability, &invocation.address)
            .await?
            .invoke(invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Retract>> for S3 {
    async fn execute(&self, input: S3Invocation<Retract>) -> Result<(), MemoryError> {
        let retract = input.capability.into_effect();
        let when = String::from_utf8_lossy(&retract.when).to_string();

        let response = input
            .permit
            .send()
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        match response.status() {
            status if status.is_success() => Ok(()),
            StatusCode::PRECONDITION_FAILED => Err(MemoryError::EditionMismatch {
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
