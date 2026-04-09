//! Memory capability providers for S3.
//!
//! Each effect is paired: `Provider<ForkInvocation<S3, Fx>>` authorizes via SigV4,
//! then delegates to `Provider<Authorized<Fx>>` for HTTP execution.

use async_trait::async_trait;
use dialog_capability::fork::ForkInvocation;
use dialog_capability::{Policy, Provider};
use dialog_effects::memory::*;

use crate::Authorized;
use crate::s3::{RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<S3, Resolve>> for S3 {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, Resolve>,
    ) -> Result<Option<Publication>, MemoryError> {
        let permit = invocation
            .address
            .authorize(&invocation.capability)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        Authorized::new(permit, invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Resolve>> for S3 {
    async fn execute(
        &self,
        input: Authorized<Resolve>,
    ) -> Result<Option<Publication>, MemoryError> {
        let client = reqwest::Client::new();
        let response = input
            .permit
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
impl Provider<ForkInvocation<S3, Publish>> for S3 {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, Publish>,
    ) -> Result<Vec<u8>, MemoryError> {
        let permit = invocation
            .address
            .authorize(&invocation.capability)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        Authorized::new(permit, invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Publish>> for S3 {
    async fn execute(&self, input: Authorized<Publish>) -> Result<Vec<u8>, MemoryError> {
        let content = Publish::of(&input.capability).content.clone();
        let when = Publish::of(&input.capability)
            .when
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).to_string());

        let client = reqwest::Client::new();
        let response = input
            .permit
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
impl Provider<ForkInvocation<S3, Retract>> for S3 {
    async fn execute(&self, invocation: ForkInvocation<S3, Retract>) -> Result<(), MemoryError> {
        let permit = invocation
            .address
            .authorize(&invocation.capability)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        Authorized::new(permit, invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Retract>> for S3 {
    async fn execute(&self, input: Authorized<Retract>) -> Result<(), MemoryError> {
        let when = String::from_utf8_lossy(&Retract::of(&input.capability).when).to_string();

        let client = reqwest::Client::new();
        let response = input
            .permit
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
