//! Sealed assets: an asset's sealed copy written as its bytes stream in,
//! and read back opened, whole or in ranges.
//!
//! The format is `dialog-keyring`'s ([`layered::asset`]). The sealed copy
//! is an ordinary blob, stored under the hash of its own bytes, so the blob
//! store verifies, imports and serves it as it does any other; only what
//! goes in and what comes out differ.

use std::mem::replace;

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_effects::blob::{BlobError, BlobReader, BlobSource, BlobWriter, Write as BlobWrite};
use dialog_keyring::KeyringError;
use dialog_keyring::layered::asset::{Span, sealed_len};
use dialog_keyring::layered::{AssetOpener, AssetSealer};

use super::TreeSpace;
use crate::CommitError;
use crate::repository::source::SourceRef;

/// Writes an asset's sealed copy into a line's blob store as the asset's
/// bytes arrive, hashing the plaintext on the way.
pub(crate) struct SealingSink {
    sink: BlobWriter,
    sealer: AssetSealer,
    hasher: blake3::Hasher,
    size: u64,
}

/// What a [`SealingSink`] wrote.
pub(crate) struct Sealed {
    /// The plaintext's hash, which names the asset.
    pub(crate) hash: Blake3Hash,
    /// The plaintext's length.
    pub(crate) size: u64,
    /// Where the sealed copy is stored.
    pub(crate) address: Blake3Hash,
}

impl SealingSink {
    /// Start a sealed copy in `source`'s blob store. With the plaintext's
    /// `reference` in hand the copy is sealed convergently, so sealing the
    /// same content again gives the same copy; without it, freshly.
    pub(crate) async fn open<Env>(
        source: SourceRef<'_>,
        space: &TreeSpace,
        reference: Option<&Blake3Hash>,
        env: &Env,
    ) -> Result<Self, CommitError>
    where
        Env: Provider<BlobWrite> + ConditionalSync + 'static,
    {
        let sealer = space.asset_sealer(reference)?;
        let mut sink = source.archive().blob().write().perform(env).await?;
        sink.write_all(sealer.header()).await?;
        Ok(Self {
            sink,
            sealer,
            hasher: blake3::Hasher::new(),
            size: 0,
        })
    }

    /// Take the next plaintext bytes.
    pub(crate) async fn write(&mut self, bytes: &[u8]) -> Result<(), CommitError> {
        self.hasher.update(bytes);
        self.size += bytes.len() as u64;
        let sealed = self.sealer.update(bytes)?;
        if !sealed.is_empty() {
            self.sink.write_all(&sealed).await?;
        }
        Ok(())
    }

    /// Seal the last piece and commit the sealed copy.
    pub(crate) async fn finish(self) -> Result<Sealed, CommitError> {
        let Self {
            mut sink,
            sealer,
            hasher,
            size,
        } = self;
        sink.write_all(&sealer.finish()?).await?;
        let address = sink.finish().await?;
        Ok(Sealed {
            hash: Blake3Hash::from(*hasher.finalize().as_bytes()),
            size,
            address,
        })
    }
}

/// Seal `content`, whose plaintext hashes to `reference`, whole and
/// convergently: for an asset already held in memory.
pub(crate) fn seal_whole(
    space: &TreeSpace,
    reference: &Blake3Hash,
    content: &[u8],
) -> Result<Vec<u8>, KeyringError> {
    let mut sealer = space.asset_sealer(Some(reference))?;
    let mut sealed = Vec::with_capacity(sealed_len(content.len() as u64) as usize);
    sealed.extend_from_slice(sealer.header());
    sealed.extend(sealer.update(content)?);
    sealed.extend(sealer.finish()?);
    Ok(sealed)
}

/// A sealed copy's pieces, read from the blob store and opened: the
/// plaintext of one range of the asset.
pub(crate) struct OpenedAsset {
    inner: Option<BlobReader>,
    opener: AssetOpener,
    index: u64,
    end: u64,
    buffer: Vec<u8>,
    skip: usize,
    take: u64,
}

impl OpenedAsset {
    /// Open the pieces `span` names, read from `inner`, which yields the
    /// sealed bytes `span` covers; `None` when it covers none.
    pub(crate) fn new(inner: Option<BlobReader>, opener: AssetOpener, span: Span) -> Self {
        Self {
            inner,
            opener,
            index: span.first,
            end: span.first + span.count,
            buffer: Vec::new(),
            skip: span.skip as usize,
            take: span.take,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl BlobSource for OpenedAsset {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        while self.take > 0 && self.index < self.end {
            let length = self.opener.piece_len(self.index);
            let Some(inner) = self.inner.as_mut() else {
                break;
            };
            while self.buffer.len() < length {
                match inner.next().await? {
                    Some(bytes) => self.buffer.extend_from_slice(&bytes),
                    None => {
                        return Err(BlobError::Storage(format!(
                            "sealed asset ended inside piece {}",
                            self.index
                        )));
                    }
                }
            }
            let rest = self.buffer.split_off(length);
            let piece = replace(&mut self.buffer, rest);
            let mut plain = self.opener.open(self.index, &piece).map_err(|error| {
                BlobError::Storage(format!("sealed asset piece {}: {error}", self.index))
            })?;
            self.index += 1;
            plain.drain(..self.skip.min(plain.len()));
            self.skip = 0;
            plain.truncate(usize::try_from(self.take).unwrap_or(usize::MAX));
            self.take -= plain.len() as u64;
            if !plain.is_empty() {
                return Ok(Some(plain));
            }
        }
        Ok(None)
    }
}
