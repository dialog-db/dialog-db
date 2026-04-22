//! Temp-dir-redirecting FileSystem wrapper for tests.
//!
//! This module is intentionally test-flavoured. It is available only
//! when the `helpers` feature is enabled (or under `cfg(test)` inside
//! this crate) and is not intended for production use.

use std::env;
use std::ops::Deref;

use async_trait::async_trait;
use dialog_capability::{Command, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::storage::{Directory, Location};

use crate::provider::{FileSystem, FileSystemError, Space, Storage};
use crate::resource::Resource;

/// Subdirectory under the platform temp directory used to namespace
/// every [`TempFileSystem`]-rooted path.
const STORAGE_NAMESPACE: &str = "dialog";

/// A [`FileSystem`] provider that redirects every [`Location`] into a
/// disambiguated subdirectory of the platform temp directory.
///
/// **This is not a general-purpose abstraction.** It exists so tests
/// that bootstrap through the real `Storage`/`Space` machinery can do
/// so without touching real user profile or working directories. If
/// you need ephemeral storage outside a test, reach for
/// [`tempfile::tempdir`] or the `Volatile` provider instead.
///
/// Each input location is rewritten to a unique path under
/// `{env::temp_dir()}/dialog/`. The original [`Directory`] variant
/// becomes a top-level segment so distinct inputs never collide; the
/// [`Location::name`] is preserved on the rewritten location:
///
/// | Input directory          | Rewritten base under `{temp}/dialog/` |
/// |--------------------------|---------------------------------------|
/// | [`Directory::Profile`]   | `profile`                             |
/// | [`Directory::Current`]   | `current`                             |
/// | [`Directory::Temp`]      | `temp`                                |
/// | [`Directory::At("p")`]   | `at/p`                                |
/// | [`Directory::At("/p")`]  | `at/./p`                              |
///
/// [`Directory::At`] paths are treated as URI-ish segments: a leading
/// `/` is prefixed with `.` so `PathBuf::join` doesn't discard the
/// temp base. Further containment (e.g. `..` escapes) is enforced by
/// [`FileSystemHandle`](crate::provider::FileSystemHandle) at open
/// time.
///
/// After resolution, all further filesystem operations delegate to the
/// wrapped [`FileSystem`] — the only difference is where the root
/// lives.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct TempFileSystem {
    inner: FileSystem,
}

impl Deref for TempFileSystem {
    type Target = FileSystem;

    fn deref(&self) -> &FileSystem {
        &self.inner
    }
}

/// Forward every command the wrapped [`FileSystem`] can handle. Keeps
/// [`TempFileSystem`] in sync as the inner [`FileSystem`] gains (or
/// loses) [`Provider`] impls without touching this file.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<C> Provider<C> for TempFileSystem
where
    C: Command,
    C::Input: ConditionalSync + 'static,
    FileSystem: Provider<C> + ConditionalSync,
{
    async fn execute(&self, input: C::Input) -> C::Output {
        self.inner.execute(input).await
    }
}

#[async_trait]
impl Resource<Location> for TempFileSystem {
    type Error = FileSystemError;

    async fn open(location: &Location) -> Result<Self, FileSystemError> {
        let path = match &location.directory {
            Directory::Profile => "profile".to_string(),
            Directory::Current => "current".to_string(),
            Directory::Temp => "temp".to_string(),
            Directory::At(path) => {
                // Prefix `.` to a leading `/` so PathBuf::join does not
                // discard our temp base; containment of `..` etc. is
                // enforced downstream by FileSystemHandle.
                let relative = if let Some(rest) = path.strip_prefix('/') {
                    format!("./{rest}")
                } else {
                    path.clone()
                };
                format!("at/{relative}")
            }
        };
        let root = env::temp_dir().join(STORAGE_NAMESPACE).join(path);
        let rewritten = Location::new(
            Directory::At(root.to_string_lossy().into_owned()),
            location.name.clone(),
        );
        Ok(Self {
            inner: FileSystem::open(&rewritten).await?,
        })
    }
}

/// A [`Space`] that routes every field through [`TempFileSystem`].
///
/// Use via [`Storage::temp`] to get a filesystem-backed storage whose
/// roots live entirely under the platform temp directory.
pub type NativeTempSpace = Space<TempFileSystem, TempFileSystem, TempFileSystem, TempFileSystem>;

impl Storage<NativeTempSpace> {
    /// Create a filesystem-backed storage whose roots live under the
    /// platform temp directory, via [`TempFileSystem`] redirection.
    ///
    /// This is only available when the `helpers` feature is enabled
    /// (or inside the crate's own tests) and is intended exclusively
    /// for tests that need to exercise the real `Storage` / `Space`
    /// pipeline without touching real user profile or working
    /// directories.
    pub fn temp() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    async fn open_root(location: Location) -> PathBuf {
        TempFileSystem::open(&location)
            .await
            .unwrap()
            .handle()
            .clone()
            .try_into()
            .unwrap()
    }

    fn temp_base() -> PathBuf {
        env::temp_dir().join(STORAGE_NAMESPACE)
    }

    #[tokio::test]
    async fn it_opens_profile_under_temp() {
        assert_eq!(
            open_root(Location::new(Directory::Profile, "alice")).await,
            temp_base().join("profile").join("alice"),
        );
    }

    #[tokio::test]
    async fn it_opens_current_under_temp() {
        assert_eq!(
            open_root(Location::new(Directory::Current, "app")).await,
            temp_base().join("current").join("app"),
        );
    }

    #[tokio::test]
    async fn it_opens_temp_under_namespaced_subdir() {
        assert_eq!(
            open_root(Location::new(Directory::Temp, "scratch")).await,
            temp_base().join("temp").join("scratch"),
        );
    }

    #[tokio::test]
    async fn it_opens_at_under_temp() {
        assert_eq!(
            open_root(Location::new(Directory::At("foo/bar".into()), "runtime")).await,
            temp_base().join("at/foo/bar").join("runtime"),
        );
    }

    #[tokio::test]
    async fn it_neutralizes_leading_slash_in_at_path() {
        // Absolute `At("/etc")` becomes `at/./etc`; after normalization
        // by PathBuf / the OS this is equivalent to `at/etc`, still
        // under temp_dir() rather than escaping to the real `/etc`.
        let root = open_root(Location::new(Directory::At("/etc".into()), "runtime")).await;
        assert!(
            root.starts_with(temp_base()),
            "{root:?} should live under {:?}",
            temp_base()
        );
    }

    #[tokio::test]
    async fn distinct_at_paths_produce_distinct_roots() {
        let a = open_root(Location::new(Directory::At("foo/bar".into()), "")).await;
        let b = open_root(Location::new(Directory::At("whatever".into()), "")).await;
        assert_ne!(a, b);
    }
}
