//! Blob providers for S3.
//!
//! Mirrors the archive providers, but for the streaming blob effects:
//! `Read` streams the (optionally ranged) GET response body without buffering;
//! `Import` (single-part) verifies the streamed bytes hash to the declared
//! digest, then PUTs them. Multipart upload is not yet implemented, so a large
//! `Import` is a single PUT (bounded by S3's 5 GiB single-object limit).

use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::{ForkInvocation, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::blob::prelude::{BlobImportExt as _, BlobReadExt as _};
use dialog_effects::blob::{BlobError, BlobReader, BlobSink, BlobSource, BlobWriter, Import, Read};
use futures_util::{Stream, StreamExt};
use reqwest::StatusCode;
use std::pin::Pin;

use crate::s3::{Permit, S3, S3Invocation};

/// A boxed stream of decoded byte chunks, `Send` off the web.
#[cfg(not(target_arch = "wasm32"))]
type ByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, BlobError>> + Send>>;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
type ByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, BlobError>>>>;

// --- Fork entry points: redeem the authorization, then execute over S3 -----

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<S3, Read>> for S3 {
    async fn execute(&self, input: ForkInvocation<S3, Read>) -> Result<BlobReader, BlobError> {
        input
            .authorization
            .redeem(&input.address)
            .await?
            .invoke(input.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<S3, Import>> for S3 {
    async fn execute(&self, input: ForkInvocation<S3, Import>) -> Result<BlobWriter, BlobError> {
        input
            .authorization
            .redeem(&input.address)
            .await?
            .invoke(input.capability)
            .perform(self)
            .await
    }
}

// --- HTTP execution ---------------------------------------------------------

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Read>> for S3 {
    async fn execute(&self, input: S3Invocation<Read>) -> Result<BlobReader, BlobError> {
        let mut permit = input.permit;
        if let Some(range) = input.capability.range() {
            let value = match range.length {
                Some(length) => {
                    format!(
                        "bytes={}-{}",
                        range.offset,
                        range.offset + length.max(1) - 1
                    )
                }
                None => format!("bytes={}-", range.offset),
            };
            permit.headers.push(("range".to_string(), value));
        }

        let response = permit
            .send()
            .await
            .map_err(|e| BlobError::Storage(e.to_string()))?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Err(BlobError::NotFound(
                input.capability.digest().as_bytes().to_base58(),
            ));
        }
        if !status.is_success() {
            return Err(BlobError::Storage(format!("blob read failed: {status}")));
        }

        Ok(Box::new(S3BlobSource::from_response(response)))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Import>> for S3 {
    async fn execute(&self, input: S3Invocation<Import>) -> Result<BlobWriter, BlobError> {
        Ok(Box::new(S3BlobSink {
            permit: input.permit,
            expected: input.capability.digest().clone(),
            buffer: Vec::new(),
            hasher: blake3::Hasher::new(),
        }))
    }
}

/// Streams a GET response body as decoded chunks. On native this is reqwest's
/// chunked `bytes_stream`; on the web (where the fetch backend has no streaming
/// body) it degrades to the whole body as one chunk.
struct S3BlobSource {
    stream: ByteStream,
}

impl S3BlobSource {
    fn from_response(response: reqwest::Response) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let stream: ByteStream = Box::pin(response.bytes_stream().map(|chunk| {
            chunk
                .map(|b| b.to_vec())
                .map_err(|e| BlobError::Io(e.to_string()))
        }));
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let stream: ByteStream = Box::pin(futures_util::stream::once(async move {
            response
                .bytes()
                .await
                .map(|b| b.to_vec())
                .map_err(|e| BlobError::Io(e.to_string()))
        }));
        Self { stream }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSource for S3BlobSource {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        self.stream.next().await.transpose()
    }
}

/// Buffers the streamed bytes, verifies they hash to the declared digest, then
/// PUTs them in one request on `finish`. Single-part only for now.
struct S3BlobSink {
    permit: Permit,
    expected: Blake3Hash,
    buffer: Vec<u8>,
    hasher: blake3::Hasher,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSink for S3BlobSink {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        self.hasher.update(bytes);
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<Blake3Hash, BlobError> {
        let S3BlobSink {
            permit,
            expected,
            buffer,
            hasher,
        } = *self;

        let hash = Blake3Hash::from(*hasher.finalize().as_bytes());
        if hash != expected {
            return Err(BlobError::DigestMismatch {
                expected: expected.as_bytes().to_base58(),
                actual: hash.as_bytes().to_base58(),
            });
        }

        let response = permit
            .upload(buffer)
            .await
            .map_err(|e| BlobError::Storage(e.to_string()))?;
        if response.status().is_success() {
            Ok(hash)
        } else {
            Err(BlobError::Storage(format!(
                "blob import failed: {}",
                response.status()
            )))
        }
    }
}

#[cfg(all(test, feature = "helpers", not(target_arch = "wasm32")))]
mod tests {
    use crate::Address;
    use crate::helpers::{LocalS3, S3Network};
    use dialog_capability::{Subject, did};
    use dialog_common::Blake3Hash;
    use dialog_effects::blob::BlobReader;
    use dialog_effects::prelude::*;

    async fn drain(mut reader: BlobReader) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            out.extend(chunk);
        }
        out
    }

    #[tokio::test]
    async fn it_imports_then_reads_a_blob_over_s3() -> anyhow::Result<()> {
        let server = LocalS3::start(&["dialog"]).await?;
        let address = Address::builder(&server.endpoint)
            .region("auto")
            .bucket("dialog")
            .build()?;
        let network = S3Network::new();
        let subject = Subject::from(did!("key:zBlobOverS3"));

        let payload: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
        let digest = Blake3Hash::from(*blake3::hash(&payload).as_bytes());

        // Import: stream the bytes in, get the verified digest back.
        let mut sink = subject
            .clone()
            .archive()
            .blob()
            .import(digest.clone(), payload.len() as u64)
            .fork(&address)
            .perform(&network)
            .await?;
        for chunk in payload.chunks(4096) {
            sink.write_all(chunk).await?;
        }
        assert_eq!(sink.finish().await?, digest);

        // Read the whole blob back.
        let reader = subject
            .clone()
            .archive()
            .blob()
            .read(digest.clone())
            .fork(&address)
            .perform(&network)
            .await?;
        assert_eq!(drain(reader).await, payload);

        // A missing blob reports NotFound.
        let missing = subject
            .archive()
            .blob()
            .read([0u8; 32])
            .fork(&address)
            .perform(&network)
            .await;
        assert!(matches!(
            missing,
            Err(dialog_effects::blob::BlobError::NotFound(_))
        ));

        Ok(())
    }
}
