//! DCAA archive capability provider: one append-only `.dialog` file per
//! catalog instead of one file per block.
//!
//! This is the capability-provider face of the single-file
//! content-addressed archive specified in `notes/dcaa.md` (see [`cas`]
//! for the format engine). DCAA is just another member of the archive
//! provider family — a peer of the file-per-blob [`FileSystem`] archive
//! on native and of the IndexedDB/OPFS archive providers on the web. It
//! serves the same `archive::{Get, Put, Import}` effect surface, opens
//! from a [`Location`] via [`Resource`] exactly like the others, and a
//! [`Space`](crate::provider::Space) selects it by simply using it as
//! its archive field; nothing else in the stack knows or cares which
//! archive implementation backs a space.
//!
//! Layout: `{space_root}/archive/{catalog}.dialog`.
//!
//! Commit granularity maps one-to-one onto effect granularity: a `Put` is
//! a single-blob transaction and an `Import` persists its whole batch as
//! ONE transaction — records, index delta, and footer appended together
//! and made durable with a single fsync. Since the repository layer sends
//! one `Import` per branch commit, that is one fsync per branch commit.
//! Duplicate blocks (content addressing makes them byte-identical) are
//! dropped at insert time; a fully-duplicate transaction appends nothing.
//!
//! Native-only: the engine needs real file offsets, `set_len`, and
//! `fdatasync`, none of which the browser filesystem API provides.

pub mod cas;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use dialog_capability::{Capability, Policy, Provider};
use dialog_effects::archive::prelude::{GetExt, ImportExt, PutExt};
use dialog_effects::archive::{ArchiveError, Get, Import, Put};
use dialog_effects::storage::Location;
use parking_lot::Mutex;

use crate::provider::{FileSystem, FileSystemError};
use crate::resource::Resource;

use cas::{Address, CasError, CasFile, DEFAULT_FOLD_THRESHOLD};

/// Environment variable overriding the delta-chain fold threshold
/// (benchmark plumbing: `0` folds every commit, i.e. a complete merged
/// index per commit — the pre-amendment spec behavior).
pub const FOLD_THRESHOLD_ENV: &str = "DIALOG_DCAA_FOLD";

/// Read the fold threshold from [`FOLD_THRESHOLD_ENV`], defaulting to
/// [`DEFAULT_FOLD_THRESHOLD`].
pub fn fold_threshold_from_env() -> usize {
    std::env::var(FOLD_THRESHOLD_ENV)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(DEFAULT_FOLD_THRESHOLD)
}

/// DCAA archive provider for one space: each catalog is a single
/// append-only `.dialog` file under `{root}/archive/`.
#[derive(Clone, Debug)]
pub struct Dcaa {
    root: PathBuf,
    fold_threshold: usize,
    catalogs: Arc<Mutex<HashMap<String, Arc<Mutex<CasFile>>>>>,
}

impl Dcaa {
    /// A provider rooted at `root`. Files are created lazily on first
    /// effect, not here.
    pub fn at(root: impl Into<PathBuf>, fold_threshold: usize) -> Self {
        Self {
            root: root.into(),
            fold_threshold,
            catalogs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// This space's root directory.
    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    /// The archive file backing `catalog`, opened (and created) on first
    /// use and cached for the provider's lifetime.
    fn catalog(&self, name: &str) -> Result<Arc<Mutex<CasFile>>, CasError> {
        let mut catalogs = self.catalogs.lock();
        if let Some(existing) = catalogs.get(name) {
            return Ok(Arc::clone(existing));
        }
        let path = self.root.join("archive").join(format!("{name}.dialog"));
        let store = Arc::new(Mutex::new(CasFile::open(path, self.fold_threshold)?));
        catalogs.insert(name.to_string(), Arc::clone(&store));
        Ok(store)
    }
}

impl From<CasError> for ArchiveError {
    fn from(e: CasError) -> Self {
        ArchiveError::Storage(e.to_string())
    }
}

/// Run a blocking archive operation off the async executor.
async fn blocking<T, F>(op: F) -> Result<T, ArchiveError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, ArchiveError> + Send + 'static,
{
    tokio::task::spawn_blocking(op)
        .await
        .map_err(|e| ArchiveError::ExecutionError(e.to_string()))?
}

#[async_trait::async_trait]
impl Provider<Get> for Dcaa {
    async fn execute(&self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        let digest: Address = *effect.digest().as_bytes();
        let store = self.catalog(effect.catalog())?;
        blocking(move || {
            let mut store = store.lock();
            match store.read(&digest) {
                Ok(bytes) => Ok(Some(bytes)),
                // Redacted content is absent for readers, per the spec.
                Err(CasError::NotFound(_)) | Err(CasError::Redacted(_)) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await
    }
}

#[async_trait::async_trait]
impl Provider<Put> for Dcaa {
    async fn execute(&self, effect: Capability<Put>) -> Result<(), ArchiveError> {
        let block = Put::of(&effect).block.clone();
        let store = self.catalog(effect.catalog())?;
        blocking(move || {
            let mut store = store.lock();
            let mut tx = store.begin();
            tx.insert(block.as_ref())?;
            tx.commit()?;
            Ok(())
        })
        .await
    }
}

#[async_trait::async_trait]
impl Provider<Import> for Dcaa {
    async fn execute(&self, effect: Capability<Import>) -> Result<(), ArchiveError> {
        let blocks = effect.blocks().to_vec();
        if blocks.is_empty() {
            return Ok(());
        }
        let store = self.catalog(effect.catalog())?;
        blocking(move || {
            // The whole batch is one transaction: one appended run of
            // records + index delta + footer, one fsync.
            let mut store = store.lock();
            let mut tx = store.begin();
            for block in &blocks {
                tx.insert(block.as_ref())?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }
}

/// Opening from a [`Location`] reuses the filesystem provider's directory
/// resolution so a DCAA-backed space lands where a `FileSystem`-backed
/// one would. Construction is lazy (no file is created), so the default
/// `load` delegation is correct.
#[async_trait::async_trait]
impl Resource<Location> for Dcaa {
    type Error = FileSystemError;

    async fn open(location: &Location) -> Result<Self, FileSystemError> {
        let fs = FileSystem::open(location).await?;
        let root: PathBuf = fs.handle().try_into()?;
        Ok(Self::at(root, fold_threshold_from_env()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::unique_subject;
    use dialog_common::{Blake3Hash, Buffer};
    use dialog_effects::archive::{Archive, Catalog};

    fn temp_provider(name: &str) -> Dcaa {
        let root = std::env::temp_dir().join(format!(
            "dcaa-provider-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        Dcaa::at(root, DEFAULT_FOLD_THRESHOLD)
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_content() -> anyhow::Result<()> {
        let provider = temp_provider("get-none");
        let subject = unique_subject("dcaa-get-none");
        let digest = Blake3Hash::hash(b"nonexistent");

        let result = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&provider)
            .await?;
        assert!(result.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_stores_and_retrieves_content() -> anyhow::Result<()> {
        let provider = temp_provider("put-get");
        let subject = unique_subject("dcaa-put-get");
        let content = b"hello single-file archive".to_vec();
        let digest = Blake3Hash::hash(&content);

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(Buffer::from(content.clone())))
            .perform(&provider)
            .await?;

        let result = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&provider)
            .await?;
        assert_eq!(result, Some(content));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_catalogs_in_separate_files() -> anyhow::Result<()> {
        let provider = temp_provider("catalogs");
        let subject = unique_subject("dcaa-catalogs");
        let content = b"catalog one only".to_vec();
        let digest = Blake3Hash::hash(&content);

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("one"))
            .invoke(Put::new(Buffer::from(content.clone())))
            .perform(&provider)
            .await?;

        let cross = subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("two"))
            .invoke(Get::new(digest.clone()))
            .perform(&provider)
            .await?;
        assert!(cross.is_none());

        assert!(provider.root().join("archive").join("one.dialog").is_file());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_imports_a_batch_and_dedups_repeats() -> anyhow::Result<()> {
        let provider = temp_provider("import");
        let subject = unique_subject("dcaa-import");

        let blocks: Vec<Buffer> = (0..8u8).map(|i| Buffer::from(vec![i; 64])).collect();
        let digests: Vec<_> = blocks
            .iter()
            .map(|buffer| buffer.blake3_hash().clone())
            .collect();

        for _ in 0..2 {
            subject
                .clone()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Import::new(blocks.clone()))
                .perform(&provider)
                .await?;
        }

        for (i, digest) in digests.into_iter().enumerate() {
            let content = subject
                .clone()
                .attenuate(Archive)
                .attenuate(Catalog::new("index"))
                .invoke(Get::new(digest))
                .perform(&provider)
                .await?;
            assert_eq!(content, Some(vec![i as u8; 64]));
        }

        let store = provider.catalog("index").expect("catalog");
        let store = store.lock();
        assert_eq!(store.len(), 8, "repeat import must not add entries");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_persists_across_provider_instances() -> anyhow::Result<()> {
        let provider = temp_provider("persist");
        let subject = unique_subject("dcaa-persist");
        let content = b"durable bytes".to_vec();
        let digest = Blake3Hash::hash(&content);

        subject
            .clone()
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Put::new(Buffer::from(content.clone())))
            .perform(&provider)
            .await?;

        let reopened = Dcaa::at(provider.root().clone(), DEFAULT_FOLD_THRESHOLD);
        let result = subject
            .attenuate(Archive)
            .attenuate(Catalog::new("index"))
            .invoke(Get::new(digest))
            .perform(&reopened)
            .await?;
        assert_eq!(result, Some(content));
        Ok(())
    }
}
