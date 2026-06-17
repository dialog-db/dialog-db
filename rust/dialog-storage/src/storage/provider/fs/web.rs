//! Web filesystem backend for [`FileSystemHandle`], backed by the
//! [File System Access API][fsapi].
//!
//! A [`WebRoot`] wraps a host-supplied [`web_sys::FileSystemDirectoryHandle`]
//! (typically from `showDirectoryPicker()` or, in tests,
//! `navigator.storage.getDirectory()`). The handle's `file:` URL is the source
//! of truth for layout and containment, exactly as on native; the path
//! relative to the root's base URL gives the segment list to walk through the
//! FS Access API. Writes commit atomically via `createWritable().close()` — no
//! temp+rename dance — and the temp+rename the providers still issue is a
//! harmless copy through the same atomic write.
//!
//! On-disk format matches the native backend byte-for-byte, so a vault written
//! from the browser can be read by any native `dialog-storage` consumer and
//! vice versa.
//!
//! [fsapi]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API

use super::{FileSystem, FileSystemError, FileSystemHandle};
use js_sys::Uint8Array;
use std::rc::Rc;
use url::Url;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemWritableFileStream,
};

/// A user-picked directory exposed through the File System Access API,
/// together with the base `file:` URL the layout is anchored at.
///
/// Cloning is cheap: the JS handle is reference-counted by the engine and the
/// base URL is shared behind an [`Rc`].
#[derive(Clone)]
pub struct WebRoot {
    /// The user-picked root directory handle.
    handle: FileSystemDirectoryHandle,
    /// The `file:` URL the root is anchored at (always ends with `/`). Path
    /// segments under this prefix are what we walk through the FS Access API.
    base: Rc<Url>,
}

impl std::fmt::Debug for WebRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The JS directory handle has no meaningful Debug; the base URL is the
        // identifying part.
        f.debug_struct("WebRoot").field("base", &self.base).finish()
    }
}

impl WebRoot {
    /// Wrap a host-supplied directory handle as a storage root.
    ///
    /// `id` is used only to synthesize a stable base URL (and, through it,
    /// Web Locks identifiers); it does not need to correspond to anything on
    /// disk. Typically the subject DID or vault id.
    pub fn new(id: &str, handle: FileSystemDirectoryHandle) -> Self {
        // Synthesize an opaque but stable base URL for this root. The path is
        // never touched on disk — only its suffix relative to this prefix is
        // walked through the JS handle — so any unique, well-formed file: URL
        // works. Percent-encode the id so arbitrary ids stay valid path bytes.
        let encoded: String = id
            .bytes()
            .flat_map(|b| {
                if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.') {
                    vec![b as char]
                } else {
                    format!("%{b:02X}").chars().collect()
                }
            })
            .collect();
        let base = Url::parse(&format!("file:///{encoded}/"))
            .expect("synthesized file: URL is always valid");
        Self {
            handle,
            base: Rc::new(base),
        }
    }

    /// The root's base URL (always a directory URL ending in `/`).
    pub(super) fn base(&self) -> &Url {
        &self.base
    }

    /// A [`FileSystem`] provider rooted at this directory.
    pub fn provider(&self) -> FileSystem {
        FileSystem::from(self.handle_at_base())
    }

    /// The underlying directory handle (cheap clone — JS handles are
    /// reference-counted). Useful for [`register`](Self::register)ing an
    /// OPFS-obtained root for later [`open`](Self::open).
    pub fn handle(&self) -> FileSystemDirectoryHandle {
        self.handle.clone()
    }

    /// A root backed by the [Origin Private File System][opfs] subdirectory
    /// named `id`, obtained via `navigator.storage.getDirectory()`.
    ///
    /// Unlike a directory picked through `showDirectoryPicker()`, OPFS needs no
    /// user gesture and is available in workers, which makes it the backing
    /// store for headless browser tests of this provider. It is also a valid
    /// production root for browsers that prefer private, origin-scoped storage.
    ///
    /// [opfs]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system
    pub async fn opfs(id: &str) -> Result<Self, FileSystemError> {
        Self::opfs_path(id, std::slice::from_ref(&id.to_string())).await
    }

    /// A root at the OPFS subdirectory reached by walking `segments` (creating
    /// missing levels). `id` anchors the synthetic base URL the layout uses.
    async fn opfs_path(id: &str, segments: &[String]) -> Result<Self, FileSystemError> {
        let global = js_sys::global();
        let navigator = js_sys::Reflect::get(&global, &JsValue::from_str("navigator"))
            .map_err(|e| js_io_error("reading navigator", e))?;
        let storage: web_sys::StorageManager =
            js_sys::Reflect::get(&navigator, &JsValue::from_str("storage"))
                .map_err(|e| js_io_error("reading navigator.storage", e))?
                .dyn_into()
                .map_err(|_| FileSystemError::Io("navigator.storage is unavailable".into()))?;
        let root: FileSystemDirectoryHandle = JsFuture::from(storage.get_directory())
            .await
            .map_err(|e| js_io_error("opening OPFS root", e))?
            .dyn_into()
            .map_err(|_| FileSystemError::Io("expected FileSystemDirectoryHandle".into()))?;

        // Scope this root to a subdirectory (creating missing levels) so
        // independent consumers don't collide in the shared OPFS namespace.
        let scoped = navigate_directory(&root, segments, true)
            .await?
            .ok_or_else(|| FileSystemError::Io("OPFS subdirectory navigation failed".into()))?;

        Ok(Self::new(id, scoped))
    }

    /// A root backed by a directory handle persisted in IndexedDB under the
    /// database named `name`.
    ///
    /// This is the durable web equivalent of a native `file:` path: the
    /// IndexedDB database is the address, and it stores the
    /// `FileSystemDirectoryHandle` (structured-cloneable, so it survives a
    /// reload). The handle must have been saved once via
    /// [`register`](Self::register) — typically right after the user picked the
    /// directory through `showDirectoryPicker()`.
    pub async fn open(name: &str) -> Result<Self, FileSystemError> {
        let db = open_directory_db(name).await?;
        let tx = db
            .transaction(&[DIRECTORY_STORE], rexie::TransactionMode::ReadOnly)
            .map_err(|e| FileSystemError::Io(format!("opening directory transaction: {e}")))?;
        let store = tx
            .store(DIRECTORY_STORE)
            .map_err(|e| FileSystemError::Io(format!("opening directory store: {e}")))?;
        let value = store
            .get(JsValue::from_str(HANDLE_KEY))
            .await
            .map_err(|e| FileSystemError::Io(format!("reading directory handle: {e}")))?;
        tx.done()
            .await
            .map_err(|e| FileSystemError::Io(format!("closing directory transaction: {e}")))?;

        let handle: FileSystemDirectoryHandle = value
            .filter(|v| !v.is_undefined() && !v.is_null())
            .ok_or_else(|| FileSystemError::Io(format!("no directory registered for '{name}'")))?
            .dyn_into()
            .map_err(|_| {
                FileSystemError::Io("stored value is not a FileSystemDirectoryHandle".into())
            })?;
        Ok(Self::new(name, handle))
    }

    /// Persist a directory handle under the IndexedDB database named `name`, so
    /// it can later be reopened with [`open`](Self::open). Replaces any handle
    /// previously registered under the same name.
    pub async fn register(
        name: &str,
        handle: FileSystemDirectoryHandle,
    ) -> Result<(), FileSystemError> {
        let db = open_directory_db(name).await?;
        let tx = db
            .transaction(&[DIRECTORY_STORE], rexie::TransactionMode::ReadWrite)
            .map_err(|e| FileSystemError::Io(format!("opening directory transaction: {e}")))?;
        let store = tx
            .store(DIRECTORY_STORE)
            .map_err(|e| FileSystemError::Io(format!("opening directory store: {e}")))?;
        store
            .put(handle.as_ref(), Some(&JsValue::from_str(HANDLE_KEY)))
            .await
            .map_err(|e| FileSystemError::Io(format!("storing directory handle: {e}")))?;
        tx.done()
            .await
            .map_err(|e| FileSystemError::Io(format!("committing directory handle: {e}")))?;
        Ok(())
    }

    /// The [`FileSystemHandle`] for this root's base URL.
    fn handle_at_base(&self) -> FileSystemHandle {
        FileSystemHandle::from_root(self.base.as_ref().clone(), self.clone())
    }
}

use crate::resource::Resource;
use dialog_effects::storage::{Directory, Location};

/// Resolve a [`Location`] to a web [`FileSystem`], mirroring the native
/// `Resource<Location>` impl so `Storage`/`Space` compose identically.
///
/// `Profile`/`Current`/`Temp` map to OPFS subdirectories (`{category}/{name}`);
/// `At` looks up a directory handle previously registered (e.g. from
/// `showDirectoryPicker()`) in IndexedDB under the given key.
#[async_trait::async_trait(?Send)]
impl Resource<Location> for FileSystem {
    type Error = FileSystemError;

    async fn open(location: &Location) -> Result<Self, FileSystemError> {
        let root = match &location.directory {
            Directory::Profile => opfs_category("profile", &location.name).await?,
            Directory::Current => opfs_category("current", &location.name).await?,
            Directory::Temp => opfs_category("temp", &location.name).await?,
            // `At` names a directory the host granted and registered in IDB.
            Directory::At(key) => WebRoot::open(key).await?,
        };
        Ok(root.provider())
    }
}

/// A web root at OPFS `{category}/{name}`. `id` (the joined path) anchors the
/// synthetic base URL so distinct locations don't collide.
async fn opfs_category(category: &str, name: &str) -> Result<WebRoot, FileSystemError> {
    let id = format!("{category}/{name}");
    let segments = vec![category.to_string(), name.to_string()];
    WebRoot::opfs_path(&id, &segments).await
}

/// IndexedDB store name and key under which a directory handle is persisted.
const DIRECTORY_STORE: &str = "directory";
const HANDLE_KEY: &str = "handle";

/// Open (creating if needed) the IndexedDB database that backs an FS-remote
/// directory, ensuring its single object store exists.
async fn open_directory_db(name: &str) -> Result<rexie::Rexie, FileSystemError> {
    rexie::Rexie::builder(name)
        .version(1)
        .add_object_store(rexie::ObjectStore::new(DIRECTORY_STORE).auto_increment(false))
        .build()
        .await
        .map_err(|e| FileSystemError::Io(format!("opening directory database '{name}': {e}")))
}

impl FileSystemHandle {
    /// Construct a handle from an already-anchored URL and its web root.
    pub(super) fn from_root(url: Url, root: WebRoot) -> Self {
        Self { url, root }
    }

    /// The web root this handle navigates from.
    fn root(&self) -> &WebRoot {
        &self.root
    }

    /// The path segments of this handle relative to its root's base URL.
    /// Empty segments (e.g. from a trailing slash) are dropped.
    fn segments(&self) -> Result<Vec<String>, FileSystemError> {
        let base_path = self.root().base().path();
        let full_path = self.url().path();
        let relative = full_path.strip_prefix(base_path).ok_or_else(|| {
            FileSystemError::Containment(format!(
                "handle path '{full_path}' escapes root '{base_path}'"
            ))
        })?;
        Ok(relative
            .split('/')
            .filter(|s| !s.is_empty())
            .map(percent_decode_segment)
            .collect())
    }

    /// Walk all but the last segment as directories, returning the parent
    /// directory handle and the leaf file name.
    async fn navigate_parent(
        &self,
        create: bool,
    ) -> Result<Option<(FileSystemDirectoryHandle, String)>, FileSystemError> {
        let segments = self.segments()?;
        let Some((name, parents)) = segments.split_last() else {
            return Err(FileSystemError::Io(
                "cannot operate on the root directory as a file".into(),
            ));
        };
        match navigate_directory(&self.root().handle, parents, create).await? {
            Some(parent) => Ok(Some((parent, name.clone()))),
            None => Ok(None),
        }
    }
}

/// Decode a single percent-encoded path segment back to its original string.
fn percent_decode_segment(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (
                (bytes[i + 1] as char).to_digit(16),
                (bytes[i + 2] as char).to_digit(16),
            )
        {
            out.push((hi * 16 + lo) as u8);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn display_js_value(value: &JsValue) -> String {
    if let Some(error) = value.dyn_ref::<js_sys::Error>() {
        return String::from(error.message());
    }
    if let Some(s) = value.as_string() {
        return s;
    }
    format!("{value:?}")
}

fn is_not_found_error(value: &JsValue) -> bool {
    value
        .dyn_ref::<js_sys::Error>()
        .map(|e| e.name() == "NotFoundError")
        .unwrap_or(false)
}

fn js_io_error(op: &str, error: JsValue) -> FileSystemError {
    FileSystemError::Io(format!("{op}: {}", display_js_value(&error)))
}

async fn navigate_directory(
    from: &FileSystemDirectoryHandle,
    segments: &[String],
    create: bool,
) -> Result<Option<FileSystemDirectoryHandle>, FileSystemError> {
    let opts = FileSystemGetDirectoryOptions::new();
    opts.set_create(create);

    let mut current = from.clone();
    for segment in segments {
        match JsFuture::from(current.get_directory_handle_with_options(segment, &opts)).await {
            Ok(value) => {
                current = value.dyn_into().map_err(|_| {
                    FileSystemError::Io(format!(
                        "expected FileSystemDirectoryHandle while resolving '{segment}'"
                    ))
                })?;
            }
            Err(error) if !create && is_not_found_error(&error) => return Ok(None),
            Err(error) => {
                return Err(js_io_error(
                    &format!("resolving directory '{segment}'"),
                    error,
                ));
            }
        }
    }
    Ok(Some(current))
}

async fn get_file_handle(
    parent: &FileSystemDirectoryHandle,
    name: &str,
    create: bool,
) -> Result<Option<FileSystemFileHandle>, FileSystemError> {
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(create);
    match JsFuture::from(parent.get_file_handle_with_options(name, &opts)).await {
        Ok(value) => value
            .dyn_into::<FileSystemFileHandle>()
            .map(Some)
            .map_err(|_| {
                FileSystemError::Io(format!("expected FileSystemFileHandle for '{name}'"))
            }),
        Err(error) if !create && is_not_found_error(&error) => Ok(None),
        Err(error) => Err(js_io_error(&format!("opening file '{name}'"), error)),
    }
}

pub(super) async fn ensure_dir(handle: &FileSystemHandle) -> Result<(), FileSystemError> {
    let segments = handle.segments()?;
    navigate_directory(&handle.root().handle, &segments, true)
        .await
        .map(|_| ())
}

pub(super) async fn read(handle: &FileSystemHandle) -> Result<Vec<u8>, FileSystemError> {
    read_optional(handle)
        .await?
        .ok_or_else(|| FileSystemError::Io("file not found".into()))
}

pub(super) async fn read_optional(
    handle: &FileSystemHandle,
) -> Result<Option<Vec<u8>>, FileSystemError> {
    let Some((parent, name)) = handle.navigate_parent(false).await? else {
        return Ok(None);
    };
    let Some(file_handle) = get_file_handle(&parent, &name, false).await? else {
        return Ok(None);
    };
    let file: web_sys::File = JsFuture::from(file_handle.get_file())
        .await
        .map_err(|e| js_io_error("getting file", e))?
        .dyn_into()
        .map_err(|_| FileSystemError::Io("expected File".into()))?;
    let buffer: js_sys::ArrayBuffer = JsFuture::from(file.array_buffer())
        .await
        .map_err(|e| js_io_error("reading file contents", e))?
        .dyn_into()
        .map_err(|_| FileSystemError::Io("expected ArrayBuffer".into()))?;
    Ok(Some(Uint8Array::new(&buffer).to_vec()))
}

pub(super) async fn write(
    handle: &FileSystemHandle,
    contents: &[u8],
) -> Result<(), FileSystemError> {
    let Some((parent, name)) = handle.navigate_parent(true).await? else {
        return Err(FileSystemError::Io(
            "parent directory navigation failed".into(),
        ));
    };
    let file_handle = get_file_handle(&parent, &name, true)
        .await?
        .ok_or_else(|| FileSystemError::Io(format!("could not create file '{name}'")))?;

    let writable: FileSystemWritableFileStream = JsFuture::from(file_handle.create_writable())
        .await
        .map_err(|e| js_io_error("creating writable stream", e))?
        .dyn_into()
        .map_err(|_| FileSystemError::Io("expected FileSystemWritableFileStream".into()))?;

    let chunk = Uint8Array::from(contents);
    let write_promise = writable
        .write_with_buffer_source(&chunk)
        .map_err(|e| js_io_error("scheduling write", e))?;
    JsFuture::from(write_promise)
        .await
        .map_err(|e| js_io_error("writing file contents", e))?;
    JsFuture::from(writable.close())
        .await
        .map_err(|e| js_io_error("closing writable stream", e))?;
    Ok(())
}

pub(super) async fn rename(
    from: &FileSystemHandle,
    to: &FileSystemHandle,
) -> Result<(), FileSystemError> {
    // `FileSystemHandle.move(parent, name)` exists in modern browsers but
    // web_sys doesn't expose it yet. Fall back to copy + delete; the caller's
    // lock prevents racing, and create_writable() makes each step atomic.
    let contents = read(from).await?;
    write(to, &contents).await?;
    remove(from).await
}

pub(super) async fn remove(handle: &FileSystemHandle) -> Result<(), FileSystemError> {
    let Some((parent, name)) = handle.navigate_parent(false).await? else {
        return Ok(());
    };
    match JsFuture::from(parent.remove_entry(&name)).await {
        Ok(_) => Ok(()),
        Err(error) if is_not_found_error(&error) => Ok(()),
        Err(error) => Err(js_io_error(&format!("removing entry '{name}'"), error)),
    }
}

pub(super) async fn list(handle: &FileSystemHandle) -> Result<Vec<String>, FileSystemError> {
    let segments = handle.segments()?;
    let Some(dir) = navigate_directory(&handle.root().handle, &segments, false).await? else {
        return Ok(Vec::new());
    };
    // `entries()` returns an async iterator of [name, handle] pairs. Drive it
    // through the JS async-iterator protocol.
    let iterator = js_sys::Reflect::get(&dir, &JsValue::from_str("entries"))
        .ok()
        .and_then(|entries| entries.dyn_into::<js_sys::Function>().ok())
        .and_then(|entries| entries.call0(&dir).ok())
        .ok_or_else(|| FileSystemError::Io("directory entries() is unavailable".into()))?;
    let next = js_sys::Reflect::get(&iterator, &JsValue::from_str("next"))
        .ok()
        .and_then(|next| next.dyn_into::<js_sys::Function>().ok())
        .ok_or_else(|| FileSystemError::Io("entries iterator has no next()".into()))?;

    let mut names = Vec::new();
    loop {
        let promise = next
            .call0(&iterator)
            .map_err(|e| js_io_error("advancing directory iterator", e))?;
        let result = JsFuture::from(js_sys::Promise::from(promise))
            .await
            .map_err(|e| js_io_error("reading directory entry", e))?;
        let done = js_sys::Reflect::get(&result, &JsValue::from_str("done"))
            .ok()
            .and_then(|d| d.as_bool())
            .unwrap_or(true);
        if done {
            break;
        }
        // value is a [name, handle] pair.
        if let Ok(value) = js_sys::Reflect::get(&result, &JsValue::from_str("value"))
            && let Ok(name) = js_sys::Reflect::get(&value, &JsValue::from_f64(0.0))
            && let Some(name) = name.as_string()
        {
            names.push(name);
        }
    }
    Ok(names)
}

pub(super) async fn exists(handle: &FileSystemHandle) -> bool {
    let Ok(Some((parent, name))) = handle.navigate_parent(false).await else {
        // Either the parent directory is missing, or this is the root handle.
        // A root handle exists by construction; segments() erroring on the
        // root is treated as "missing" here only for non-root handles, but the
        // providers never call exists() on the root.
        return handle.segments().map(|s| s.is_empty()).unwrap_or(false);
    };
    if get_file_handle(&parent, &name, false)
        .await
        .ok()
        .flatten()
        .is_some()
    {
        return true;
    }
    navigate_directory(&parent, std::slice::from_ref(&name), false)
        .await
        .ok()
        .flatten()
        .is_some()
}

/// Acquire a [Web Locks API][weblocks] lock for the given handle's CAS
/// critical section, keyed by the handle's URL so distinct cells don't
/// contend. The returned guard releases the lock when dropped.
///
/// [weblocks]: https://developer.mozilla.org/en-US/docs/Web/API/Web_Locks_API
pub(crate) async fn lock(handle: &FileSystemHandle) -> Result<LockGuard, FileSystemError> {
    let name = format!("dialog-storage:fs:{}", handle.url().path());
    LockGuard::acquire(&name).await
}

/// RAII guard holding a Web Locks API lock for the duration of a CAS critical
/// section. Dropping it resolves the lock callback's promise, releasing the
/// lock.
pub(crate) struct LockGuard {
    release: Option<tokio::sync::oneshot::Sender<()>>,
    outer: Option<js_sys::Promise>,
}

impl LockGuard {
    async fn acquire(name: &str) -> Result<Self, FileSystemError> {
        use wasm_bindgen::closure::Closure;

        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        // The browser invokes this callback once the lock is granted. We signal
        // acquisition and return a Promise that stays pending until `release_rx`
        // fires (on guard drop) — keeping the lock held for the critical
        // section. `once_into_js` lets the closure own the channels by move.
        let callback = Closure::once_into_js(move |_lock: JsValue| -> JsValue {
            let _ = acquired_tx.send(());
            wasm_bindgen_futures::future_to_promise(async move {
                let _ = release_rx.await;
                Ok(JsValue::UNDEFINED)
            })
            .into()
        });

        let manager = lock_manager()?;
        let outer = manager.request_with_callback(name, callback.unchecked_ref());

        acquired_rx
            .await
            .map_err(|_| FileSystemError::Lock("lock acquisition was cancelled".into()))?;

        Ok(Self {
            release: Some(release_tx),
            outer: Some(outer),
        })
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // Signal the callback's promise to resolve, releasing the lock.
        if let Some(tx) = self.release.take() {
            let _ = tx.send(());
        }
        // Drain the outer request promise so a late rejection doesn't surface
        // as an unhandled promise rejection.
        if let Some(outer) = self.outer.take() {
            wasm_bindgen_futures::spawn_local(async move {
                let _ = JsFuture::from(outer).await;
            });
        }
    }
}

/// Reach `navigator.locks` from either a window or a worker global scope.
fn lock_manager() -> Result<web_sys::LockManager, FileSystemError> {
    let global = js_sys::global();
    let navigator = js_sys::Reflect::get(&global, &JsValue::from_str("navigator"))
        .map_err(|e| js_io_error("reading navigator", e))?;
    let locks = js_sys::Reflect::get(&navigator, &JsValue::from_str("locks"))
        .map_err(|e| js_io_error("reading navigator.locks", e))?;
    locks
        .dyn_into::<web_sys::LockManager>()
        .map_err(|_| FileSystemError::Lock("navigator.locks is unavailable".into()))
}

#[cfg(test)]
mod tests {
    use super::WebRoot;
    use crate::helpers::{unique_did, unique_name};
    use dialog_effects::archive::prelude::*;
    use dialog_effects::memory::prelude::*;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A `FileSystem` provider backed by a fresh OPFS subdirectory. OPFS is
    /// available in the dedicated-worker test runner without a user gesture and
    /// exercises the real web (File System Access API) code path.
    async fn opfs_provider(label: &str) -> crate::provider::FileSystem {
        WebRoot::opfs(&unique_name(label))
            .await
            .expect("OPFS root should be available in the worker test runner")
            .provider()
    }

    #[dialog_common::test]
    async fn it_returns_none_for_missing_blob() -> anyhow::Result<()> {
        let provider = opfs_provider("web-archive-missing").await;
        let did = unique_did().await;
        let digest = dialog_common::Blake3Hash::hash(b"never written");

        let result = did
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&provider)
            .await?;
        assert!(result.is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_writes_and_reads_back_a_blob() -> anyhow::Result<()> {
        let provider = opfs_provider("web-archive-roundtrip").await;
        let did = unique_did().await;
        let content = b"hello opfs".to_vec();
        let digest = dialog_common::Blake3Hash::hash(&content);

        did.clone()
            .archive()
            .catalog("index")
            .put(content.clone())
            .perform(&provider)
            .await?;

        let result = did
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&provider)
            .await?;
        assert_eq!(result, Some(content));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_publishes_and_resolves_a_cell() -> anyhow::Result<()> {
        let provider = opfs_provider("web-memory-roundtrip").await;
        let did = unique_did().await;
        let content = b"first revision".to_vec();

        let version = did
            .clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(content.clone(), None)
            .perform(&provider)
            .await?;
        assert!(!version.is_empty());

        let resolved = did
            .memory()
            .space("local")
            .cell("head")
            .resolve()
            .perform(&provider)
            .await?;
        let edition = resolved.expect("cell should resolve to the published edition");
        assert_eq!(edition.content, content);
        assert_eq!(edition.version, version);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_enforces_cas_on_publish() -> anyhow::Result<()> {
        use dialog_effects::memory::MemoryError;

        let provider = opfs_provider("web-memory-cas").await;
        let did = unique_did().await;

        did.clone()
            .memory()
            .space("local")
            .cell("head")
            .publish(b"first".to_vec(), None)
            .perform(&provider)
            .await?;

        // A second IfNoneMatch publish must fail: the cell already exists.
        let result = did
            .memory()
            .space("local")
            .cell("head")
            .publish(b"second".to_vec(), None)
            .perform(&provider)
            .await;
        assert!(matches!(result, Err(MemoryError::VersionMismatch { .. })));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_writes_a_nested_cell_path() -> anyhow::Result<()> {
        let provider = opfs_provider("web-memory-nested").await;
        let did = unique_did().await;
        let content = b"branch head".to_vec();

        did.clone()
            .memory()
            .space("local")
            .cell("branch/main")
            .publish(content.clone(), None)
            .perform(&provider)
            .await?;

        let resolved = did
            .memory()
            .space("local")
            .cell("branch/main")
            .resolve()
            .perform(&provider)
            .await?;
        let edition = resolved.expect("nested cell should resolve");
        assert_eq!(edition.content, content);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_opens_a_location_under_opfs() -> anyhow::Result<()> {
        use crate::resource::Resource;
        use dialog_effects::storage::{Directory, Location};

        // FileSystem::open(Location) resolves to an OPFS directory on web, the
        // same entry point as native, so a round-trip works through it.
        let location = Location::new(Directory::Temp, unique_name("web-location"));
        let provider = crate::provider::FileSystem::open(&location).await?;
        let did = unique_did().await;
        let content = b"opened via Location".to_vec();
        let digest = dialog_common::Blake3Hash::hash(&content);

        did.clone()
            .archive()
            .catalog("index")
            .put(content.clone())
            .perform(&provider)
            .await?;
        let result = did
            .clone()
            .archive()
            .catalog("index")
            .get(digest.clone())
            .perform(&provider)
            .await?;
        assert_eq!(result, Some(content));

        // Re-opening the same Location must reach the same directory.
        let reopened = crate::provider::FileSystem::open(&location).await?;
        let again = did
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&reopened)
            .await?;
        assert!(again.is_some(), "same Location should reopen the same dir");
        Ok(())
    }
}
