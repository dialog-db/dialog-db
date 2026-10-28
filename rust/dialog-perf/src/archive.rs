//! Opt-in disk archive comparisons under the existing scenario matrix.
//!
//! Compile with `--features single-file-archive`, then select
//! `DIALOG_PERF_ARCHIVE=fs` or `DIALOG_PERF_ARCHIVE=single-file`. Compare those
//! two sweeps, not their disk-I/O instructions against volatile baselines.
//! Without this feature the original operator is compiled unchanged.
use dialog_artifacts::{Datum, Key, State};
use dialog_capability::{Capability, Command, Provider};
use dialog_common::{ConditionalSend, ConditionalSync, Held, Holds};
use dialog_effects::{
    archive::{
        ArchiveError, Get, Import, Put,
        prelude::{GetExt, ImportExt, PutExt},
    },
    storage::{Directory, Location},
};
use dialog_search_tree::TreeFrameCodec;
use dialog_storage::provider::single_file::{BlockFrame, FrameCodec};
use dialog_storage::{
    provider::{FileSystem, SingleFile},
    resource::Resource,
};
use std::{any::Any, sync::Arc};

// Stable uninlined attribution boundaries around the real codec, only in this
// opt-in measurement build. Inclusive Callgrind costs include all codec work.
#[derive(Clone)]
struct ProfiledCodec<C>(C);
impl<C: FrameCodec> FrameCodec for ProfiledCodec<C> {
    fn identifier(&self) -> &'static str {
        self.0.identifier()
    }
    #[inline(never)]
    fn encode(&self, bytes: &[u8]) -> std::io::Result<BlockFrame> {
        self.0.encode(bytes)
    }
    #[inline(never)]
    fn decode(&self, metadata: &[u8], tokens: &[&[u8]]) -> std::io::Result<Vec<u8>> {
        self.0.decode(metadata, tokens)
    }
}

#[derive(Clone)]
enum Archive {
    FileSystem(FileSystem),
    SingleFile(SingleFile<ProfiledCodec<TreeFrameCodec<Key, State<Datum>>>>),
}
/// One temporary disk archive, shared across a scenario's sessions.
#[derive(Clone)]
pub struct DiskArchive {
    archive: Archive,
    // Keep the directory until every adopter has released its archive.
    _directory: Arc<tempfile::TempDir>,
}
impl DiskArchive {
    /// Open `DIALOG_PERF_ARCHIVE`; an unset selection uses the filesystem.
    pub async fn open() -> anyhow::Result<Self> {
        let directory = Arc::new(tempfile::tempdir()?);
        let archive = match std::env::var("DIALOG_PERF_ARCHIVE").as_deref() {
            Ok("fs") | Err(std::env::VarError::NotPresent) => Archive::FileSystem(
                FileSystem::open(&Location::new(
                    Directory::At(directory.path().display().to_string()),
                    "archive",
                ))
                .await?,
            ),
            Ok("single-file") => Archive::SingleFile(
                SingleFile::open(
                    directory.path().join("archive"),
                    ProfiledCodec(TreeFrameCodec::new()),
                )
                .await?,
            ),
            _ => anyhow::bail!("DIALOG_PERF_ARCHIVE must be fs or single-file"),
        };
        Ok(Self {
            archive,
            _directory: directory,
        })
    }
}
// Callgrind's measurement marker only toggles the thread that enters it.
// Keep encoding/decoding, hashing and dictionary operations on that thread.
// Ordinary asynchronous SingleFile providers still offload to spawn_blocking;
// this adapter excludes worker scheduling overhead, not archive CPU work.
#[async_trait::async_trait]
impl Provider<Get> for DiskArchive {
    async fn execute(&self, effect: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        match &self.archive {
            Archive::FileSystem(archive) => {
                <FileSystem as Provider<Get>>::execute(archive, effect).await
            }
            Archive::SingleFile(archive) => archive
                .get_blocking(effect.catalog(), *effect.digest().as_bytes())
                .map_err(|e| ArchiveError::Storage(e.to_string())),
        }
    }
}
#[async_trait::async_trait]
impl Provider<Put> for DiskArchive {
    async fn execute(&self, effect: Capability<Put>) -> Result<(), ArchiveError> {
        match &self.archive {
            Archive::FileSystem(archive) => {
                <FileSystem as Provider<Put>>::execute(archive, effect).await
            }
            Archive::SingleFile(archive) => archive
                .put_blocking(effect.catalog(), effect.content())
                .map(|_| ())
                .map_err(|e| ArchiveError::Storage(e.to_string())),
        }
    }
}
#[async_trait::async_trait]
impl Provider<Import> for DiskArchive {
    async fn execute(&self, effect: Capability<Import>) -> Result<(), ArchiveError> {
        match &self.archive {
            Archive::FileSystem(archive) => {
                <FileSystem as Provider<Import>>::execute(archive, effect).await
            }
            Archive::SingleFile(archive) => {
                for block in effect.blocks() {
                    archive
                        .put_blocking(effect.catalog(), block.as_ref())
                        .map_err(|e| ArchiveError::Storage(e.to_string()))?;
                }
                Ok(())
            }
        }
    }
}

/// A measurement-only archive override; all other commands use the normal peer.
#[derive(Clone)]
pub struct ArchiveOperator<E> {
    inner: E,
    archive: DiskArchive,
}
impl<E> ArchiveOperator<E> {
    /// Route this peer's archive traffic to the chosen disk provider.
    pub fn new(inner: E, archive: DiskArchive) -> Self {
        Self { inner, archive }
    }
}
impl<E: Holds> Holds for ArchiveOperator<E> {
    fn held(&self, key: &str) -> Option<Held> {
        self.inner.held(key)
    }
    fn hold(&self, key: String, value: Held) {
        self.inner.hold(key, value)
    }
    fn held_or(&self, key: &str, make: &dyn Fn() -> Held) -> Held {
        self.inner.held_or(key, make)
    }
}
#[async_trait::async_trait]
impl<C, E> Provider<C> for ArchiveOperator<E>
where
    C: Command + 'static,
    C::Input: ConditionalSend + 'static,
    C::Output: 'static,
    E: Provider<C> + ConditionalSync,
{
    async fn execute(&self, input: C::Input) -> C::Output {
        fn output<T: 'static, U: 'static>(value: T) -> U {
            *(Box::new(value) as Box<dyn Any>)
                .downcast::<U>()
                .expect("matching archive output")
        }
        let (get, put, import) = {
            let any = &input as &dyn Any;
            (
                any.downcast_ref::<Capability<Get>>().cloned(),
                any.downcast_ref::<Capability<Put>>().cloned(),
                any.downcast_ref::<Capability<Import>>().cloned(),
            )
        };
        if let Some(effect) = get {
            return output(<DiskArchive as Provider<Get>>::execute(&self.archive, effect).await);
        }
        if let Some(effect) = put {
            return output(<DiskArchive as Provider<Put>>::execute(&self.archive, effect).await);
        }
        if let Some(effect) = import {
            return output(<DiskArchive as Provider<Import>>::execute(&self.archive, effect).await);
        }
        self.inner.execute(input).await
    }
}
