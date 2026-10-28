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
    archive::{ArchiveError, Get, Import, Put},
    storage::{Directory, Location},
};
use dialog_search_tree::TreeFrameCodec;
use dialog_storage::{
    provider::{FileSystem, SingleFile},
    resource::Resource,
};
use std::{any::Any, sync::Arc};

#[derive(Clone)]
enum Archive {
    FileSystem(FileSystem),
    SingleFile(SingleFile<TreeFrameCodec<Key, State<Datum>>>),
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
                SingleFile::open(directory.path().join("archive"), TreeFrameCodec::new()).await?,
            ),
            _ => anyhow::bail!("DIALOG_PERF_ARCHIVE must be fs or single-file"),
        };
        Ok(Self {
            archive,
            _directory: directory,
        })
    }
}
macro_rules! provide {
    ($effect:ty, $output:ty) => {
        #[async_trait::async_trait]
        impl Provider<$effect> for DiskArchive {
            async fn execute(&self, effect: Capability<$effect>) -> Result<$output, ArchiveError> {
                match &self.archive {
                    Archive::FileSystem(archive) => <FileSystem as Provider<$effect>>::execute(archive, effect).await,
                    Archive::SingleFile(archive) => <SingleFile<TreeFrameCodec<Key, State<Datum>>> as Provider<$effect>>::execute(archive, effect).await,
                }
            }
        }
    };
}
provide!(Get, Option<Vec<u8>>);
provide!(Put, ());
provide!(Import, ());

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
