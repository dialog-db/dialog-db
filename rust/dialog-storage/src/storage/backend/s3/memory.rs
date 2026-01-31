//! Memory capability types and Provider implementations for S3 backend.
//!
//! Re-exports memory types from [`dialog_effects`] and implements
//! `Provider<Resolve>`, `Provider<Publish>`, and `Provider<Retract>` for [`S3`].

pub use dialog_effects::memory::*;

use async_trait::async_trait;
use dialog_capability::{Authority, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_s3_credentials::capability::memory::{
    Publish as AuthorizePublish, Resolve as AuthorizeResolve, Retract as AuthorizeRetract,
};

use super::{Hasher, RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<Resolve> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &mut self,
        input: Capability<Resolve>,
    ) -> Result<Option<Publication>, MemoryError> {
        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(Memory)
            .attenuate(Space::of(&input).clone())
            .attenuate(Cell::of(&input).clone())
            .invoke(AuthorizeResolve);

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| MemoryError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client);
        let response = builder
            .send()
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        if response.status().is_success() {
            // Extract ETag from response headers as the edition
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
                content: bytes.to_vec().into(),
                edition: edition.into_bytes().into(),
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
impl<Issuer> Provider<Publish> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(&mut self, input: Capability<Publish>) -> Result<Vec<u8>, MemoryError> {
        let Publish { content, when } = Publish::of(&input);
        let when = when
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).to_string());
        let checksum = Hasher::Sha256.checksum(content);

        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(Memory)
            .attenuate(Space::of(&input).clone())
            .attenuate(Cell::of(&input).clone())
            .invoke(AuthorizePublish {
                checksum,
                when: when.clone(),
            });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| MemoryError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client).body(content.to_vec());
        let response = builder
            .send()
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        match response.status() {
            status if status.is_success() => {
                // Extract new ETag from response as the new edition
                let new_edition = response
                    .headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.trim_matches('"').to_string())
                    .ok_or_else(|| {
                        MemoryError::Storage("Response missing ETag header".to_string())
                    })?;
                Ok(new_edition.into_bytes().into())
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
impl<Issuer> Provider<Retract> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(&mut self, input: Capability<Retract>) -> Result<(), MemoryError> {
        let Retract { when } = Retract::of(&input);
        let when = String::from_utf8_lossy(when).to_string();

        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(Memory)
            .attenuate(Space::of(&input).clone())
            .attenuate(Cell::of(&input).clone())
            .invoke(AuthorizeRetract { when: when.clone() });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| MemoryError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client);
        let response = builder
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
