use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::io::AsyncWrite as FuturesAsyncWrite;
use js_sys::Uint8Array;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemWritableFileStream,
};

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::DialogBlobsError;

/// Extracts a human-readable message from a [`JsValue`] error.
///
/// Tries the `message` property of a JS `Error` first, then a bare string
/// value, and falls back to `Debug` formatting for anything else.
fn display_js_value(value: &JsValue) -> String {
    if let Some(error) = value.dyn_ref::<js_sys::Error>() {
        return String::from(error.message());
    }
    if let Some(s) = value.as_string() {
        return s;
    }
    format!("{value:?}")
}

/// Returns `true` if the given [`JsValue`] is a `NotFoundError` DOMException.
fn is_not_found_error(value: &JsValue) -> bool {
    value
        .dyn_ref::<js_sys::Error>()
        .map(|e| String::from(e.name()) == "NotFoundError")
        .unwrap_or(false)
}

#[wasm_bindgen]
extern "C" {
    /// Extension type for `FileSystemFileHandle` that exposes the
    /// `move()` method. Since `move` is a Rust keyword, the Rust-side
    /// name is `move_to`.
    ///
    /// The `move(parent, name)` overload is supported by all major browsers:
    /// - Chromium: `move()` on `FileSystemFileHandle`
    /// - Firefox: `move()` on `FileSystemHandle` (the base class)
    /// - Safari/WebKit: `move(parent, name)` on `FileSystemHandle`
    ///
    /// We use the two-argument form (`parent` + `name`) on `FileSystemFileHandle`,
    /// which is the intersection that all three engines support.
    ///
    /// See <https://github.com/nicolo-ribaudo/tc39-proposal-structs/issues/180>
    /// for browser compatibility details.
    #[wasm_bindgen(extends = web_sys::FileSystemFileHandle)]
    type FileSystemFileHandleExt;

    /// `fileHandle.move(destinationDir, newName)` — atomically moves
    /// the file into `destination` with the given name. Returns a Promise.
    #[wasm_bindgen(method, structural, catch, js_name = move)]
    fn move_to(
        this: &FileSystemFileHandleExt,
        destination: &web_sys::FileSystemDirectoryHandle,
        name: &str,
    ) -> Result<js_sys::Promise, JsValue>;
}

/// Returns the `LockManager` from `navigator.locks`.
fn lock_manager() -> Result<web_sys::LockManager, DialogBlobsError> {
    let global = js_sys::global();
    let navigator = js_sys::Reflect::get(&global, &"navigator".into()).map_err(|error| {
        DialogBlobsError::Put(format!("No navigator: {}", display_js_value(&error)))
    })?;
    js_sys::Reflect::get(&navigator, &"locks".into())
        .map_err(|error| {
            DialogBlobsError::Put(format!("No lock manager: {}", display_js_value(&error)))
        })?
        .dyn_into()
        .map_err(|_| DialogBlobsError::Put("Expected LockManager".into()))
}

/// Executes `f` while holding a Web Lock with the given `name`.
///
/// Uses the Web Locks API (`navigator.locks.request`) to coordinate
/// across browser tabs, workers, and service workers. The lock is
/// automatically released when `f` completes or when the execution
/// context terminates.
async fn with_web_lock<F, Fut, T>(name: &str, f: F) -> Result<T, DialogBlobsError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, DialogBlobsError>>,
{
    let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

    // The JS callback is invoked once the browser grants the lock. It
    // signals acquisition via `acquired_tx`, then returns a Promise that
    // stays pending until we send on `release_tx` — keeping the Web Lock
    // held for the duration of the critical section.
    let callback = Closure::once_into_js(move |_lock: JsValue| -> JsValue {
        let _ = acquired_tx.send(());
        wasm_bindgen_futures::future_to_promise(async move {
            let _ = release_rx.await;
            Ok(JsValue::UNDEFINED)
        })
        .into()
    });

    let mgr = lock_manager()?;
    let outer_promise = mgr.request_with_callback(name, callback.unchecked_ref());

    // Yield until the browser grants the lock and our callback fires.
    acquired_rx
        .await
        .map_err(|_| DialogBlobsError::Put("Web Lock acquisition cancelled".into()))?;

    // Critical section.
    let result = f().await;

    // Release the Web Lock (signal the inner promise to resolve).
    // If `release_tx` is dropped without sending (e.g. on panic), the
    // channel closes and `release_rx.await` returns Err — which we
    // ignore in the callback, so the lock is still released.
    let _ = release_tx.send(());
    let outer = JsFuture::from(outer_promise).await;

    // Prefer the critical-section error (more actionable); otherwise
    // propagate any lock-manager rejection.
    let value = result?;
    outer.map_err(|error| {
        DialogBlobsError::Put(format!(
            "Web Lock request failed: {}",
            display_js_value(&error)
        ))
    })?;
    Ok(value)
}

/// A sandboxed virtual filesystem backed by the Origin Private File System (OPFS).
///
/// Every path provided to [`Vfs`] methods is resolved relative to a virtual root
/// directory within OPFS. Path components like `..` are rejected to prevent
/// traversal outside the root.
pub struct Vfs {
    root: String,
}

impl From<String> for Vfs {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl Vfs {
    /// Creates a new [`Vfs`] with the given path as its root within OPFS.
    ///
    /// The root is a `/`-separated virtual path (e.g. `"dialog/blobs"`) that will
    /// be created as nested directories inside the OPFS root.
    pub fn new(root: String) -> Self {
        Self { root }
    }

    /// Creates a [`Vfs`] rooted at `dialog/blobs` within OPFS.
    pub fn with_default_root() -> Result<Self, DialogBlobsError> {
        Ok(Self::new("dialog/blobs".into()))
    }

    /// Obtains the OPFS root directory handle via `navigator.storage.getDirectory()`.
    async fn opfs_root() -> Result<FileSystemDirectoryHandle, DialogBlobsError> {
        let global = js_sys::global();
        let navigator = js_sys::Reflect::get(&global, &"navigator".into()).map_err(|error| {
            DialogBlobsError::Initialization(format!("No navigator: {}", display_js_value(&error)))
        })?;
        let storage: web_sys::StorageManager = js_sys::Reflect::get(&navigator, &"storage".into())
            .map_err(|error| {
                DialogBlobsError::Initialization(format!(
                    "No storage manager: {}",
                    display_js_value(&error)
                ))
            })?
            .dyn_into()
            .map_err(|_| DialogBlobsError::Initialization("Expected StorageManager".to_string()))?;
        JsFuture::from(storage.get_directory())
            .await
            .map_err(|error| {
                DialogBlobsError::Initialization(format!(
                    "OPFS unavailable: {}",
                    display_js_value(&error)
                ))
            })?
            .dyn_into()
            .map_err(|_| {
                DialogBlobsError::Initialization("Expected FileSystemDirectoryHandle".to_string())
            })
    }

    /// Splits a caller-supplied path into `(directory_segments, file_name)`.
    ///
    /// Rejects `..` and `.` components to prevent traversal.
    fn split_path(path: &str) -> Result<(Vec<&str>, &str), DialogBlobsError> {
        let segments: Vec<&str> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if segments.is_empty() {
            return Err(DialogBlobsError::Path("Empty path".into()));
        }

        if segments.iter().any(|s| *s == ".." || *s == ".") {
            return Err(DialogBlobsError::Path(
                "Resolved path does not fall inside of configured root".into(),
            ));
        }

        let (dirs, file) = segments.split_at(segments.len() - 1);
        Ok((dirs.to_vec(), file[0]))
    }

    /// Navigates from a starting directory through a chain of subdirectories,
    /// optionally creating them along the way.
    ///
    /// When `create` is `false`, a missing directory produces
    /// [`DialogBlobsError::NotFound`]; other OPFS errors produce
    /// [`DialogBlobsError::Path`].
    async fn navigate(
        from: &FileSystemDirectoryHandle,
        segments: &[&str],
        create: bool,
    ) -> Result<FileSystemDirectoryHandle, DialogBlobsError> {
        let opts = FileSystemGetDirectoryOptions::new();
        opts.set_create(create);

        let mut current = from.clone();
        for segment in segments {
            current = JsFuture::from(current.get_directory_handle_with_options(segment, &opts))
                .await
                .map_err(|error| {
                    if !create && is_not_found_error(&error) {
                        DialogBlobsError::NotFound(format!("Directory '{segment}' not found"))
                    } else {
                        DialogBlobsError::Path(format!(
                            "Directory '{segment}': {}",
                            display_js_value(&error)
                        ))
                    }
                })?
                .dyn_into()
                .map_err(|_| {
                    DialogBlobsError::Path(format!(
                        "Failed to cast directory handle for '{segment}'"
                    ))
                })?;
        }

        Ok(current)
    }

    /// Resolves the virtual root plus additional path segments into a directory handle.
    async fn resolve_directory(
        &self,
        extra: &[&str],
        create: bool,
    ) -> Result<FileSystemDirectoryHandle, DialogBlobsError> {
        let opfs = Self::opfs_root().await?;
        let root_segments: Vec<&str> = self.root.split('/').filter(|s| !s.is_empty()).collect();
        let all: Vec<&str> = root_segments.iter().chain(extra.iter()).copied().collect();
        Self::navigate(&opfs, &all, create).await
    }

    /// Removes a file by name from the given parent directory.
    async fn remove_entry(
        dir: &FileSystemDirectoryHandle,
        name: &str,
    ) -> Result<(), DialogBlobsError> {
        JsFuture::from(dir.remove_entry(name))
            .await
            .map_err(|error| {
                DialogBlobsError::Put(format!(
                    "Could not delete '{name}': {}",
                    display_js_value(&error)
                ))
            })?;
        Ok(())
    }

    /// Opens the file at `path` (relative to root) for reading and returns a
    /// [`FileReader`] that streams its contents as [`bytes::Bytes`] chunks.
    ///
    /// Returns [`DialogBlobsError::NotFound`] if the file does not exist, or
    /// [`DialogBlobsError::Get`] for other I/O failures.
    pub async fn read_from(&self, path: &str) -> Result<FileReader, DialogBlobsError> {
        let (dirs, name) = Self::split_path(path)?;
        let dir = self.resolve_directory(&dirs, false).await?;

        let opts = FileSystemGetFileOptions::new();
        opts.set_create(false);

        let handle: FileSystemFileHandle =
            JsFuture::from(dir.get_file_handle_with_options(name, &opts))
                .await
                .map_err(|error| {
                    if is_not_found_error(&error) {
                        DialogBlobsError::NotFound(format!(
                            "File not found: {}",
                            display_js_value(&error)
                        ))
                    } else {
                        DialogBlobsError::Get(format!(
                            "Could not open file: {}",
                            display_js_value(&error)
                        ))
                    }
                })?
                .dyn_into()
                .map_err(|_| DialogBlobsError::Get("Expected FileSystemFileHandle".into()))?;

        let file: web_sys::File = JsFuture::from(handle.get_file())
            .await
            .map_err(|error| {
                DialogBlobsError::Get(format!("Could not get file: {}", display_js_value(&error)))
            })?
            .dyn_into()
            .map_err(|_| DialogBlobsError::Get("Expected File".into()))?;

        Ok(FileReader::new(file.stream()))
    }

    /// Creates a file at the given path (relative to root) and returns a handle for writing.
    /// Intermediate directories are created as needed.
    pub async fn write_to(&self, path: &str) -> Result<FileWriter, DialogBlobsError> {
        let (dirs, name) = Self::split_path(path)?;
        let dir = self.resolve_directory(&dirs, true).await?;

        let opts = FileSystemGetFileOptions::new();
        opts.set_create(true);

        let handle: FileSystemFileHandle =
            JsFuture::from(dir.get_file_handle_with_options(name, &opts))
                .await
                .map_err(|error| {
                    DialogBlobsError::Put(format!(
                        "Could not create file: {}",
                        display_js_value(&error)
                    ))
                })?
                .dyn_into()
                .map_err(|_| DialogBlobsError::Put("Expected FileSystemFileHandle".into()))?;

        let writable: FileSystemWritableFileStream = JsFuture::from(handle.create_writable())
            .await
            .map_err(|error| {
                DialogBlobsError::Put(format!(
                    "Could not create writable stream: {}",
                    display_js_value(&error)
                ))
            })?
            .dyn_into()
            .map_err(|_| DialogBlobsError::Put("Expected FileSystemWritableFileStream".into()))?;

        Ok(FileWriter::new(writable))
    }

    /// Moves a file from one rooted path to another.
    ///
    /// Intermediate directories for the destination are created as needed.
    /// Both `from` and `to` are resolved relative to root. The data is
    /// streamed from source to destination so that large files do not need
    /// to be buffered entirely in memory.
    pub async fn move_file(&self, from: &str, to: &str) -> Result<(), DialogBlobsError> {
        let (from_dirs, from_name) = Self::split_path(from)?;
        let (to_dirs, to_name) = Self::split_path(to)?;

        // Resolve the source file handle
        let from_dir = self.resolve_directory(&from_dirs, false).await?;
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(false);
        let handle: FileSystemFileHandle =
            JsFuture::from(from_dir.get_file_handle_with_options(from_name, &opts))
                .await
                .map_err(|error| {
                    DialogBlobsError::Get(format!(
                        "Source file not found: {}",
                        display_js_value(&error)
                    ))
                })?
                .dyn_into()
                .map_err(|_| DialogBlobsError::Get("Expected FileSystemFileHandle".into()))?;

        // Resolve (or create) the destination directory
        let to_dir = self.resolve_directory(&to_dirs, true).await?;

        // Primary path: atomic move via FileSystemFileHandle.move(parent, name).
        //
        // If the call throws (method absent) or the promise rejects (e.g.
        // destination locked by a concurrent writer), we fall through to the
        // stream-copy fallback.
        let ext: &FileSystemFileHandleExt = handle.unchecked_ref();
        if let Ok(promise) = ext.move_to(&to_dir, to_name)
            && JsFuture::from(promise).await.is_ok()
        {
            return Ok(());
        }

        // Fallback: stream copy + delete under a Web Lock.
        //
        // This path should rarely be reached on modern browsers, but provides
        // resilience if the native move is unavailable or rejects (e.g. older
        // browser versions, non-standard contexts). The copy is not atomic, so
        // a Web Lock keyed on the destination path serializes concurrent moves
        // to prevent interleaved writes.
        let lock_name = format!("dialog-blob-move:{to}");
        with_web_lock(&lock_name, || async {
            let mut reader = self.read_from(from).await?;
            let mut writer = self.write_to(to).await?;

            while let Some(chunk) = reader.next().await {
                let bytes =
                    chunk.map_err(|error| DialogBlobsError::Put(format!("Read error: {error}")))?;
                writer
                    .write_all(&bytes)
                    .await
                    .map_err(|error| DialogBlobsError::Put(format!("Write error: {error}")))?;
            }

            writer
                .shutdown()
                .await
                .map_err(|error| DialogBlobsError::Put(format!("Failed to finalize: {error}")))?;

            Self::remove_entry(&from_dir, from_name).await
        })
        .await
    }
}

/// A streaming reader for a file managed by [`Vfs`].
///
/// Implements [`Stream`]`<Item = Result<`[`bytes::Bytes`]`, `[`std::io::Error`]`>>`,
/// yielding the file contents in chunks suitable for forwarding over a
/// network or piping into another async sink.
#[derive(Debug)]
pub struct FileReader {
    inner: wasm_streams::readable::IntoStream<'static>,
}

impl FileReader {
    fn new(readable: web_sys::ReadableStream) -> Self {
        let stream = wasm_streams::ReadableStream::from_raw(readable.unchecked_into());
        Self {
            inner: stream.into_stream(),
        }
    }
}

impl Stream for FileReader {
    type Item = Result<bytes::Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(js_value))) => {
                let array = Uint8Array::new(&js_value);
                Poll::Ready(Some(Ok(bytes::Bytes::from(array.to_vec()))))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Stream error: {}", display_js_value(&error)),
            )))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// An async writer for a file managed by [`Vfs`].
///
/// Implements [`AsyncWrite`] by delegating to the [`wasm_streams`] async
/// writer, which bridges tokio's poll-based interface with the Promise-based
/// OPFS [`FileSystemWritableFileStream`].
pub struct FileWriter {
    inner: wasm_streams::writable::IntoAsyncWrite<'static>,
}

impl FileWriter {
    fn new(writable: FileSystemWritableFileStream) -> Self {
        let stream = wasm_streams::WritableStream::from_raw(writable.unchecked_into());
        Self {
            inner: stream.into_async_write(),
        }
    }
}

impl AsyncWrite for FileWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    use wasm_bindgen_test::wasm_bindgen_test_configure;
    wasm_bindgen_test_configure!(run_in_service_worker);

    #[wasm_bindgen_test]
    fn it_resolves_a_simple_filename_under_root() {
        let (dirs, name) = Vfs::split_path("foo.txt").unwrap();
        assert!(dirs.is_empty());
        assert_eq!(name, "foo.txt");
    }

    #[wasm_bindgen_test]
    fn it_strips_a_leading_slash_from_the_given_path() {
        let (dirs, name) = Vfs::split_path("/sub/file.bin").unwrap();
        assert_eq!(dirs, vec!["sub"]);
        assert_eq!(name, "file.bin");
    }

    #[wasm_bindgen_test]
    fn it_resolves_nested_paths() {
        let (dirs, name) = Vfs::split_path("a/b/c.txt").unwrap();
        assert_eq!(dirs, vec!["a", "b"]);
        assert_eq!(name, "c.txt");
    }

    #[wasm_bindgen_test]
    fn it_rejects_traversal_above_root() {
        assert!(Vfs::split_path("../../etc/passwd").is_err());
    }

    #[wasm_bindgen_test]
    fn it_rejects_parent_traversal_even_within_root() {
        // Web VFS is stricter than native: all ".." components are rejected
        // because OPFS does not support path normalization.
        assert!(Vfs::split_path("a/b/../c.txt").is_err());
    }

    #[wasm_bindgen_test]
    fn it_rejects_a_path_that_escapes_to_roots_sibling() {
        assert!(Vfs::split_path("../secret").is_err());
    }

    #[wasm_bindgen_test]
    fn it_rejects_dot_segments() {
        assert!(Vfs::split_path("./foo.txt").is_err());
    }

    #[wasm_bindgen_test]
    fn it_rejects_empty_path() {
        assert!(Vfs::split_path("").is_err());
    }
}
