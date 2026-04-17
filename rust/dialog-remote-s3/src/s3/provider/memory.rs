//! Memory providers for S3.

use crate::{S3, S3Error, S3Invocation};
use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::fork::ForkInvocation;
use dialog_effects::memory::*;
use reqwest::StatusCode;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<S3, Resolve>> for S3 {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        invocation
            .authorization
            .redeem(&invocation.capability, &invocation.address)
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
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        let response = input.permit.send().await?;

        if response.status().is_success() {
            let edition = response
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim_matches('"').to_string())
                .ok_or_else(|| MemoryError::Storage("Response missing ETag header".to_string()))?;

            let bytes = response.bytes().await.map_err(S3Error::from)?;

            Ok(Some(Edition {
                content: bytes.to_vec(),
                version: Version::from(edition),
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
    ) -> Result<Version, MemoryError> {
        invocation
            .authorization
            .redeem(&invocation.capability, &invocation.address)
            .await?
            .invoke(invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Publish>> for S3 {
    async fn execute(&self, input: S3Invocation<Publish>) -> Result<Version, MemoryError> {
        let publish = input.capability.into_effect();
        let when = publish.when.clone();

        let response = input.permit.upload(publish.content).await?;

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
                Ok(Version::from(new_edition))
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
            .redeem(&invocation.capability, &invocation.address)
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
        let when = retract.when.clone();

        let response = input.permit.send().await?;

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
