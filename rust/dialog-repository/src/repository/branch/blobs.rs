//! Entity-addressed blob API.
//!
//! A thin facade over the hash-addressed [`write_blob`](Branch::write_blob) /
//! [`read_blob`](Branch::read_blob) / [`blob_size`](Branch::blob_size) /
//! [`list_blobs`](Branch::list_blobs) commands. A blob is referenced by its
//! content-derived entity `blob:<hash>` (see [`Entity::from_blob`]), so `add`
//! hands back an [`Entity`] and `get` takes one — matching the design record,
//! where a blob is a first-class resource other facts can reference and attach
//! metadata to. The hash-addressed commands remain as the lower layer.
//!
//! ```ignore
//! let blob = branch.blobs().add(source).perform(env).await?;   // -> Entity
//! let view = branch.blobs().get(blob)?;
//! let size = view.size().perform(env).await?;                  // from the index
//! let all  = view.read().perform(env).await?;                  // streaming reader
//! let head = view.slice(0, Some(1024)).perform(env).await?;    // ranged reader
//! ```

use crate::{BlobSize, Branch, CommitError, ListBlobs, ReadBlob, RemoteSite, WriteBlob};
use dialog_artifacts::Entity;
use dialog_capability::{Fork, Provider};
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::Identify;
use dialog_effects::blob::{BlobError, ByteRange, Write as BlobWrite};
use dialog_effects::memory::{Publish, Resolve};
use futures_util::Stream;

/// Entity-addressed blob accessor, created by [`Branch::blobs`].
pub struct Blobs<'a> {
    branch: &'a Branch,
}

impl Branch {
    /// Access this branch's blobs through the entity-addressed API.
    pub fn blobs(&self) -> Blobs<'_> {
        Blobs { branch: self }
    }
}

impl<'a> Blobs<'a> {
    /// Ingest a blob from a stream of byte chunks and record it in the blob
    /// index, returning the blob's content-derived [`Entity`] (`blob:<hash>`).
    pub fn add<S>(&self, source: S) -> AddBlob<'a, S>
    where
        S: Stream<Item = Result<Vec<u8>, BlobError>> + ConditionalSend + Unpin,
    {
        AddBlob {
            inner: self.branch.write_blob(source),
        }
    }

    /// View a blob by its entity. Errors if `entity` is not a `blob:<hash>`
    /// entity.
    pub fn get(&self, entity: impl Into<Entity>) -> Result<BlobView<'a>, BlobError> {
        let entity = entity.into();
        let hash = entity
            .blob_hash()
            .ok_or_else(|| BlobError::NotFound(format!("not a blob entity: {entity}")))?;
        Ok(BlobView {
            branch: self.branch,
            hash: hash.into(),
        })
    }

    /// List the entities of every blob referenced by the branch's current tree,
    /// in hash order. Answered from the blob index alone — no bytes are fetched.
    pub fn list(&self) -> ListBlobEntities<'a> {
        ListBlobEntities {
            inner: self.branch.list_blobs(),
        }
    }
}

/// A handle to a stored blob, created by [`Blobs::get`]. Read the whole blob, a
/// byte range, or just its size.
pub struct BlobView<'a> {
    branch: &'a Branch,
    hash: Blake3Hash,
}

impl<'a> BlobView<'a> {
    /// The blob's entity (`blob:<hash>`).
    pub fn entity(&self) -> Entity {
        // The hash came from a valid blob entity (or a `write_blob` digest), so
        // the `blob:<base58(32)>` form always parses.
        Entity::from_blob(self.hash.as_bytes()).expect("blob entity is always valid")
    }

    /// The blob's content hash.
    pub fn hash(&self) -> &Blake3Hash {
        &self.hash
    }

    /// Read the whole blob as a stream (local first, remote hydration on a miss).
    pub fn read(&self) -> ReadBlob<'a> {
        self.branch.read_blob(&self.hash, None)
    }

    /// Read a byte range of the blob: `length` bytes from `offset`, or to the
    /// end when `length` is `None`. Mirrors `Blob.slice`.
    pub fn slice(&self, offset: u64, length: Option<u64>) -> ReadBlob<'a> {
        self.branch
            .read_blob(&self.hash, Some(ByteRange { offset, length }))
    }

    /// The blob's size in bytes, answered from the blob index without fetching
    /// the bytes. `None` if the branch's current tree does not reference it.
    pub fn size(&self) -> BlobSize<'a> {
        self.branch.blob_size(&self.hash)
    }
}

/// Command that ingests a blob and returns its entity; see [`Blobs::add`].
///
/// Wraps [`WriteBlob`] and maps its discovered hash to the blob's entity.
pub struct AddBlob<'a, S> {
    inner: WriteBlob<'a, S>,
}

impl<S> AddBlob<'_, S>
where
    S: Stream<Item = Result<Vec<u8>, BlobError>> + ConditionalSend + Unpin,
{
    /// Execute the ingest, returning the blob's entity.
    pub async fn perform<Env>(self, env: &Env) -> Result<Entity, CommitError>
    where
        Env: Provider<BlobWrite>
            + Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let hash = self.inner.perform(env).await?;
        Ok(Entity::from_blob(hash.as_bytes())?)
    }
}

/// Command that lists referenced blobs as entities; see [`Blobs::list`].
pub struct ListBlobEntities<'a> {
    inner: ListBlobs<'a>,
}

impl ListBlobEntities<'_> {
    /// Execute the listing, returning one [`Entity`] per referenced blob in
    /// hash order.
    pub async fn perform<Env>(self, env: &Env) -> Result<Vec<Entity>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + ConditionalSync
            + 'static,
    {
        self.inner
            .perform(env)
            .await?
            .into_iter()
            .map(|(hash, _record)| Entity::from_blob(&hash).map_err(CommitError::from))
            .collect()
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::RepositoryExt as _;
    use crate::helpers::unique_name;
    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_effects::blob::{BlobError, BlobReader};
    use dialog_network::Network;
    use dialog_operator::Profile;
    use dialog_storage::provider::storage::Storage;
    use futures_util::stream;

    async fn drain(mut reader: BlobReader) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = reader.next().await.unwrap() {
            out.extend(chunk);
        }
        out
    }

    // Blob effects stream through a real filesystem, so this builds the operator
    // over a temp-dir native space (`Storage::temp()`); the volatile space has
    // no blob provider. Mirrors `write_blob`/`read_blob`'s tests.
    #[dialog_common::test]
    async fn it_adds_a_blob_and_reads_it_back_by_entity() -> Result<()> {
        let storage = Storage::temp();
        let profile = Profile::open(unique_name("blob-entity"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"test")
            .allow(Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;
        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let payload: Vec<u8> = (0..50_000u32).map(|i| (i % 251) as u8).collect();
        let chunks: Vec<Result<Vec<u8>, BlobError>> =
            payload.chunks(8192).map(|c| Ok(c.to_vec())).collect();

        // add -> Entity
        let entity = branch
            .blobs()
            .add(stream::iter(chunks))
            .perform(&operator)
            .await?;
        assert!(entity.as_str().starts_with("blob:"));

        let view = branch.blobs().get(entity.clone())?;
        assert_eq!(view.entity(), entity);

        // size from the index, no fetch
        assert_eq!(
            view.size().perform(&operator).await?,
            Some(payload.len() as u64)
        );

        // whole read
        let reader = view.read().perform(&operator).await?;
        assert_eq!(drain(reader).await, payload);

        // ranged read (slice)
        let reader = view.slice(10, Some(9)).perform(&operator).await?;
        assert_eq!(drain(reader).await, payload[10..19]);

        // slice to end
        let reader = view.slice(49_000, None).perform(&operator).await?;
        assert_eq!(drain(reader).await, payload[49_000..]);

        // list returns the entity
        let listed = branch.blobs().list().perform(&operator).await?;
        assert_eq!(listed, vec![entity]);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_a_non_blob_entity() -> Result<()> {
        let storage = Storage::temp();
        let profile = Profile::open(unique_name("blob-entity-reject"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"test")
            .allow(Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;
        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let entity: dialog_artifacts::Entity = "user:alice".parse()?;
        assert!(matches!(
            branch.blobs().get(entity),
            Err(BlobError::NotFound(_))
        ));

        Ok(())
    }
}
