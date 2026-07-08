//! Web filesystem backend for [`FileSystemHandle`], backed by the
//! [File System Access API][fsapi].
//!
//! A [`MountedDirectory`] wraps a host-supplied [`web_sys::FileSystemDirectoryHandle`]
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

use super::{FileReader, FileSystem, FileSystemError, FileSystemHandle};
use futures_util::StreamExt;
use js_sys::Uint8Array;
use std::rc::Rc;
use url::Url;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemWritableFileStream,
};

/// A directory mounted through the File System Access API — its
/// [`web_sys::FileSystemDirectoryHandle`] together with the base `file:` URL
/// the layout is anchored at. Capabilities operate *within* it.
///
/// The directory may come from the user picker (`showDirectoryPicker()`), OPFS
/// (`navigator.storage.getDirectory()`), or the directory registry. Cloning is
/// cheap: the JS handle is reference-counted by the engine and the base URL is
/// shared behind an [`Rc`].
#[derive(Clone)]
pub struct MountedDirectory {
    /// The mounted directory's handle.
    handle: FileSystemDirectoryHandle,
    /// The `file:` URL the directory is anchored at (always ends with `/`).
    /// Path segments under this prefix are what we walk through the FS Access
    /// API.
    base: Rc<Url>,
}

impl std::fmt::Debug for MountedDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The JS directory handle has no meaningful Debug; the base URL is the
        // identifying part.
        f.debug_struct("MountedDirectory")
            .field("base", &self.base)
            .finish()
    }
}

impl MountedDirectory {
    /// Mount a host-supplied directory handle.
    ///
    /// `id` is used only to synthesize a stable base URL (and, through it,
    /// Web Locks identifiers); it does not need to correspond to anything on
    /// disk. It is the directory's reproducible identity: its OPFS logical path,
    /// or its registry UUID for a picked directory.
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
    /// reference-counted). Test-only: production roots come from OPFS
    /// ([`opfs_category`]) or the IndexedDB registry ([`open`](Self::open)),
    /// neither of which needs to hand the raw handle back out.
    #[cfg(test)]
    pub fn handle(&self) -> FileSystemDirectoryHandle {
        self.handle.clone()
    }

    /// A root backed by the [Origin Private File System][opfs] subdirectory
    /// named `id`, obtained via `navigator.storage.getDirectory()`.
    ///
    /// Test-only single-segment helper: OPFS needs no user gesture and is
    /// available in workers, which makes it the backing store for headless
    /// browser tests. Production OPFS roots go through [`opfs_category`], which
    /// calls [`opfs_path`](Self::opfs_path) directly.
    ///
    /// [opfs]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system
    #[cfg(test)]
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

    /// A root for the mounted directory identified by `id` in the directory
    /// registry (see [`FileSystemDirectoryHandleExt::mount`]).
    ///
    /// `id` is a stable, registry-assigned identifier (a UUID) for one physical
    /// directory — the durable web equivalent of a native `file:` path. The
    /// registry stores the directory's `FileSystemDirectoryHandle`
    /// (structured-cloneable, so it survives a reload) under that id.
    pub async fn open(id: &str) -> Result<Self, FileSystemError> {
        let handle = registry::handle(id)
            .await?
            .ok_or_else(|| FileSystemError::Io(format!("no directory registered for '{id}'")))?;
        Ok(Self::new(id, handle))
    }

    /// The [`FileSystemHandle`] for this root's base URL.
    fn handle_at_base(&self) -> FileSystemHandle {
        FileSystemHandle::from_root(self.base.as_ref().clone(), self.clone())
    }
}

/// Mount and unmount a host-supplied directory through the directory registry.
///
/// The host obtains a [`FileSystemDirectoryHandle`] however it can —
/// `showDirectoryPicker()`, drag-and-drop's
/// `DataTransferItem.getAsFileSystemHandle()`, or a PWA launch handler (all
/// require a user gesture and a main-thread context, none of which this crate
/// can or should drive). This trait turns such a handle into a durable,
/// serializable [`Location`] that [`FileSystem::open`] reopens, and back again.
///
/// Only the *real-directory* case needs this: OPFS directories are addressed by
/// their deterministic [`Location`] path directly, with no registry.
#[async_trait::async_trait(?Send)]
pub trait FileSystemDirectoryHandleExt {
    /// Persist this directory in the registry and return the [`Location`] that
    /// reopens it via [`FileSystem::open`].
    ///
    /// The registry is deduplicated by physical directory (compared with
    /// [`FileSystemHandle.isSameEntry`][isSameEntry]): mounting the same
    /// directory again — re-picked, or restored from a different handle —
    /// returns the same [`Location`]. That stable identity is what the Web Locks
    /// CAS name derives from, so independent handles to one directory mutually
    /// exclude. The File System Access API exposes no identity string of its
    /// own, so the registry is the authority.
    ///
    /// [isSameEntry]: https://developer.mozilla.org/en-US/docs/Web/API/FileSystemHandle/isSameEntry
    async fn mount(&self) -> Result<Location, FileSystemError>;

    /// Drop a mounted directory from the registry. After this, [`FileSystem::open`]
    /// on the same [`Location`] fails until it is mounted again. A `Location`
    /// that names no registry entry (e.g. an OPFS path) is a no-op.
    async fn unmount(location: &Location) -> Result<(), FileSystemError>;
}

#[async_trait::async_trait(?Send)]
impl FileSystemDirectoryHandleExt for FileSystemDirectoryHandle {
    async fn mount(&self) -> Result<Location, FileSystemError> {
        let id = registry::register(self.clone()).await?;
        Ok(Location::at(id))
    }

    async fn unmount(location: &Location) -> Result<(), FileSystemError> {
        if let Directory::At(id) = &location.directory {
            registry::unmount(id).await?;
        }
        Ok(())
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
            Directory::At(key) => MountedDirectory::open(key).await?,
        };
        Ok(root.provider())
    }
}

/// A web root at OPFS `{category}/{name}`. `id` (the joined path) anchors the
/// synthetic base URL so distinct locations don't collide.
async fn opfs_category(category: &str, name: &str) -> Result<MountedDirectory, FileSystemError> {
    let id = format!("{category}/{name}");
    let segments = vec![category.to_string(), name.to_string()];
    MountedDirectory::opfs_path(&id, &segments).await
}

/// The directory registry: a single IndexedDB database mapping a stable id (a
/// UUID) to a picked directory's `FileSystemDirectoryHandle`, deduplicated by
/// physical directory via [`FileSystemHandle.isSameEntry`][isSameEntry].
///
/// This is the identity authority for picked directories. The File System
/// Access API gives no readable identity string for a handle (only async
/// pairwise `isSameEntry`), so two handles for the same folder can't agree on a
/// derived name on their own. By assigning each physical directory one UUID
/// here — reused on any subsequent handle that `isSameEntry`-matches — the
/// registry produces an identity both can resolve to, which the Web Locks name
/// keys on for cross-tab CAS.
///
/// OPFS roots need none of this: their logical path (`{category}/{name}`) is
/// already a reproducible identity, so they are anchored directly on it.
///
/// [isSameEntry]: https://developer.mozilla.org/en-US/docs/Web/API/FileSystemHandle/isSameEntry
mod registry {
    use super::{FileSystemDirectoryHandle, FileSystemError, js_io_error, random_uuid};
    use wasm_bindgen::{JsCast, JsValue};
    use wasm_bindgen_futures::JsFuture;

    const DB: &str = "dialog-fs-directories";
    const STORE: &str = "directories";

    async fn open_db() -> Result<rexie::Rexie, FileSystemError> {
        rexie::Rexie::builder(DB)
            .version(1)
            .add_object_store(rexie::ObjectStore::new(STORE).auto_increment(false))
            .build()
            .await
            .map_err(|e| FileSystemError::Io(format!("opening directory registry: {e}")))
    }

    /// Whether two directory handles refer to the same physical directory.
    async fn is_same_entry(
        a: &FileSystemDirectoryHandle,
        b: &FileSystemDirectoryHandle,
    ) -> Result<bool, FileSystemError> {
        let same = JsFuture::from(a.is_same_entry(b))
            .await
            .map_err(|e| js_io_error("comparing directory handles", e))?;
        Ok(same.is_truthy())
    }

    /// Read the handle registered under `id`, if any.
    pub(super) async fn handle(
        id: &str,
    ) -> Result<Option<FileSystemDirectoryHandle>, FileSystemError> {
        let db = open_db().await?;
        let tx = db
            .transaction(&[STORE], rexie::TransactionMode::ReadOnly)
            .map_err(|e| FileSystemError::Io(format!("opening registry transaction: {e}")))?;
        let store = tx
            .store(STORE)
            .map_err(|e| FileSystemError::Io(format!("opening registry store: {e}")))?;
        let value = store
            .get(JsValue::from_str(id))
            .await
            .map_err(|e| FileSystemError::Io(format!("reading registry entry: {e}")))?;
        tx.done()
            .await
            .map_err(|e| FileSystemError::Io(format!("closing registry transaction: {e}")))?;

        value
            .filter(|v| !v.is_undefined() && !v.is_null())
            .map(|v| {
                v.dyn_into::<FileSystemDirectoryHandle>().map_err(|_| {
                    FileSystemError::Io("registry value is not a FileSystemDirectoryHandle".into())
                })
            })
            .transpose()
    }

    /// Register `handle`, returning the id of the physical directory it refers
    /// to: an existing id if a registered handle `isSameEntry`-matches, else a
    /// freshly minted UUID stored under the handle.
    pub(super) async fn register(
        handle: FileSystemDirectoryHandle,
    ) -> Result<String, FileSystemError> {
        let db = open_db().await?;

        // Scan existing entries for a handle pointing at the same directory.
        let tx = db
            .transaction(&[STORE], rexie::TransactionMode::ReadOnly)
            .map_err(|e| FileSystemError::Io(format!("opening registry transaction: {e}")))?;
        let store = tx
            .store(STORE)
            .map_err(|e| FileSystemError::Io(format!("opening registry store: {e}")))?;
        let keys = store
            .get_all_keys(None, None)
            .await
            .map_err(|e| FileSystemError::Io(format!("listing registry keys: {e}")))?;
        let values = store
            .get_all(None, None)
            .await
            .map_err(|e| FileSystemError::Io(format!("listing registry entries: {e}")))?;
        tx.done()
            .await
            .map_err(|e| FileSystemError::Io(format!("closing registry transaction: {e}")))?;

        for (key, value) in keys.into_iter().zip(values) {
            let Ok(stored) = value.dyn_into::<FileSystemDirectoryHandle>() else {
                continue;
            };
            if is_same_entry(&handle, &stored).await? {
                return key
                    .as_string()
                    .ok_or_else(|| FileSystemError::Io("registry key is not a string".into()));
            }
        }

        // New directory: mint an id and store the handle under it.
        let id = random_uuid()?;
        let tx = db
            .transaction(&[STORE], rexie::TransactionMode::ReadWrite)
            .map_err(|e| FileSystemError::Io(format!("opening registry transaction: {e}")))?;
        let store = tx
            .store(STORE)
            .map_err(|e| FileSystemError::Io(format!("opening registry store: {e}")))?;
        store
            .put(handle.as_ref(), Some(&JsValue::from_str(&id)))
            .await
            .map_err(|e| FileSystemError::Io(format!("storing registry entry: {e}")))?;
        tx.done()
            .await
            .map_err(|e| FileSystemError::Io(format!("committing registry entry: {e}")))?;
        Ok(id)
    }

    /// Drop the registry entry for `id`. Absent ids are a no-op.
    pub(super) async fn unmount(id: &str) -> Result<(), FileSystemError> {
        let db = open_db().await?;
        let tx = db
            .transaction(&[STORE], rexie::TransactionMode::ReadWrite)
            .map_err(|e| FileSystemError::Io(format!("opening registry transaction: {e}")))?;
        let store = tx
            .store(STORE)
            .map_err(|e| FileSystemError::Io(format!("opening registry store: {e}")))?;
        store
            .delete(JsValue::from_str(id))
            .await
            .map_err(|e| FileSystemError::Io(format!("deleting registry entry: {e}")))?;
        tx.done()
            .await
            .map_err(|e| FileSystemError::Io(format!("committing registry deletion: {e}")))?;
        Ok(())
    }
}

/// A v4-shaped UUID string from the platform's `crypto.randomUUID()`, reached
/// through `js_sys::Reflect` on the global so it works in both window and
/// worker scopes (where `crypto` is a global) without the `web_sys::Crypto`
/// feature.
fn random_uuid() -> Result<String, FileSystemError> {
    let global = js_sys::global();
    let crypto = js_sys::Reflect::get(&global, &JsValue::from_str("crypto"))
        .map_err(|e| js_io_error("reading crypto", e))?;
    let func: js_sys::Function = js_sys::Reflect::get(&crypto, &JsValue::from_str("randomUUID"))
        .map_err(|e| js_io_error("reading crypto.randomUUID", e))?
        .dyn_into()
        .map_err(|_| FileSystemError::Io("crypto.randomUUID is unavailable".into()))?;
    func.call0(&crypto)
        .map_err(|e| js_io_error("calling crypto.randomUUID", e))?
        .as_string()
        .ok_or_else(|| FileSystemError::Io("crypto.randomUUID did not return a string".into()))
}

impl FileSystemHandle {
    /// Construct a handle from an already-anchored URL and its web root.
    pub(super) fn from_root(url: Url, root: MountedDirectory) -> Self {
        Self { url, root }
    }

    /// The web root this handle navigates from.
    fn root(&self) -> &MountedDirectory {
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

/// Whether a JS object exposes `name` as a callable method.
fn has_method(object: &JsValue, name: &str) -> bool {
    js_sys::Reflect::get(object, &JsValue::from_str(name))
        .map(|v| v.is_function())
        .unwrap_or(false)
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

/// On the web, a plain `write` is already atomic: `createWritable().close()`
/// stages the data and swaps it into place on close (and the sync-access path
/// truncates+writes a single handle), so a reader never observes a partial
/// file. No temp+rename is needed — that would just add a full read, a second
/// write, and a delete.
pub(super) async fn write_atomic(
    handle: &FileSystemHandle,
    contents: &[u8],
) -> Result<(), FileSystemError> {
    write(handle, contents).await
}

/// Streaming reader. Slices to the requested range (a `Blob.slice`), then
/// streams the result incrementally via `Blob.stream()` — nothing buffers the
/// whole file.
pub(super) async fn open_reader(
    handle: &FileSystemHandle,
    offset: u64,
    len: Option<u64>,
) -> Result<FileReader, FileSystemError> {
    let Some((parent, name)) = handle.navigate_parent(false).await? else {
        return Err(FileSystemError::Io("file not found".into()));
    };
    let Some(file_handle) = get_file_handle(&parent, &name, false).await? else {
        return Err(FileSystemError::Io("file not found".into()));
    };
    let file: web_sys::File = JsFuture::from(file_handle.get_file())
        .await
        .map_err(|e| js_io_error("getting file", e))?
        .dyn_into()
        .map_err(|_| FileSystemError::Io("expected File".into()))?;

    let readable: web_sys::ReadableStream = if offset > 0 || len.is_some() {
        let start = offset as f64;
        let end = match len {
            Some(len) => (offset + len) as f64,
            None => file.size(),
        };
        let blob = file
            .slice_with_f64_and_f64(start, end)
            .map_err(|e| js_io_error("slicing file", e))?;
        blob.stream()
    } else {
        file.stream()
    };

    let stream = wasm_streams::ReadableStream::from_raw(readable.unchecked_into())
        .into_stream()
        .map(|chunk| {
            chunk
                .map(|value| Uint8Array::new(&value).to_vec())
                .map_err(|e| js_io_error("reading stream", e))
        });
    Ok(Box::pin(stream))
}

/// Process-local counter for unique staging file names.
fn unique_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// A uniquely-named staging sibling of `handle` in the same directory.
fn staging_handle(handle: &FileSystemHandle) -> FileSystemHandle {
    let mut url = handle.url().clone();
    let path = url.path().trim_end_matches('/').to_string();
    url.set_path(&format!("{}.{}.tmp", path, unique_suffix()));
    handle.with_url(url)
}

/// Streaming writer. Writes each chunk straight to a staging file through the
/// async `createWritable` stream (no buffering), then commits by closing the
/// stream and renaming the staging file into place. Safari ≤ 18 has no
/// streaming writable, so it falls back to buffering + an atomic sync-access
/// write.
pub(super) async fn open_writer(handle: &FileSystemHandle) -> Result<FileWriter, FileSystemError> {
    let staging = staging_handle(handle);
    let Some((parent, name)) = staging.navigate_parent(true).await? else {
        return Err(FileSystemError::Io(
            "parent directory navigation failed".into(),
        ));
    };
    let file_handle = get_file_handle(&parent, &name, true)
        .await?
        .ok_or_else(|| FileSystemError::Io(format!("could not create staging '{name}'")))?;

    let inner = if has_method(&file_handle, "createWritable") {
        let writable: FileSystemWritableFileStream = JsFuture::from(file_handle.create_writable())
            .await
            .map_err(|e| js_io_error("creating writable stream", e))?
            .dyn_into()
            .map_err(|_| FileSystemError::Io("expected FileSystemWritableFileStream".into()))?;
        WriterInner::Streaming(writable)
    } else {
        // No streaming writable: drop the empty staging file and buffer instead.
        let _ = remove(&staging).await;
        WriterInner::Buffered(Vec::new())
    };

    Ok(FileWriter {
        target: handle.clone(),
        staging,
        inner,
    })
}

/// A streaming, atomically-committed file writer (web).
pub struct FileWriter {
    /// The handle this writer was opened on; `finish` commits here.
    target: FileSystemHandle,
    /// The staging file actually written; renamed into place on commit.
    staging: FileSystemHandle,
    inner: WriterInner,
}

enum WriterInner {
    /// Chunks streamed straight to the staging file's writable.
    Streaming(FileSystemWritableFileStream),
    /// Safari ≤ 18 fallback: accumulate, then one atomic write at commit.
    Buffered(Vec<u8>),
}

impl FileWriter {
    /// Append a chunk, streaming it to the staging file when possible.
    pub async fn write_all(&mut self, bytes: &[u8]) -> Result<(), FileSystemError> {
        match &mut self.inner {
            WriterInner::Streaming(writable) => {
                let chunk = Uint8Array::from(bytes);
                let promise = writable
                    .write_with_buffer_source(&chunk)
                    .map_err(|e| js_io_error("scheduling write", e))?;
                JsFuture::from(promise)
                    .await
                    .map_err(|e| js_io_error("writing chunk", e))?;
                Ok(())
            }
            WriterInner::Buffered(buffer) => {
                buffer.extend_from_slice(bytes);
                Ok(())
            }
        }
    }

    /// Commit to the handle this writer was opened on.
    pub async fn finish(self) -> Result<(), FileSystemError> {
        let target = self.target.clone();
        self.commit_to(&target).await
    }

    /// Commit to `dest` instead — lets a content-addressed writer pick the
    /// final path (the hash) only after the content has streamed.
    pub async fn finish_to(self, dest: &FileSystemHandle) -> Result<(), FileSystemError> {
        self.commit_to(dest).await
    }

    async fn commit_to(self, dest: &FileSystemHandle) -> Result<(), FileSystemError> {
        match self.inner {
            WriterInner::Streaming(writable) => {
                JsFuture::from(writable.close())
                    .await
                    .map_err(|e| js_io_error("closing writable stream", e))?;
                rename(&self.staging, dest).await
            }
            WriterInner::Buffered(buffer) => write_atomic(dest, &buffer).await,
        }
    }

    /// Discard the staged content without committing it.
    pub async fn discard(self) -> Result<(), FileSystemError> {
        if let WriterInner::Streaming(writable) = self.inner {
            let _ = JsFuture::from(writable.close()).await;
            let _ = remove(&self.staging).await;
        }
        Ok(())
    }
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

    // `createWritable` is the async writable-stream path (Chrome, Firefox,
    // Safari 26+). Where it is absent — Safari ≤ 18, whose OPFS only exposes the
    // synchronous, worker-scoped `createSyncAccessHandle` — fall back to that.
    if has_method(&file_handle, "createWritable") {
        write_via_writable(&file_handle, contents).await
    } else {
        write_via_sync_access(&file_handle, contents).await
    }
}

/// Write through `FileSystemWritableFileStream` (async, main-thread-capable).
async fn write_via_writable(
    file_handle: &FileSystemFileHandle,
    contents: &[u8],
) -> Result<(), FileSystemError> {
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

/// Write through `FileSystemSyncAccessHandle` (synchronous, OPFS-only, and
/// available only in a worker — the only OPFS write path on Safari ≤ 18). The
/// handle is exclusive, so it is acquired, used, and closed within this call;
/// truncating to the content length drops any stale tail from a shorter
/// rewrite.
async fn write_via_sync_access(
    file_handle: &FileSystemFileHandle,
    contents: &[u8],
) -> Result<(), FileSystemError> {
    let access: web_sys::FileSystemSyncAccessHandle =
        JsFuture::from(file_handle.create_sync_access_handle())
            .await
            .map_err(|e| js_io_error("creating sync access handle", e))?
            .dyn_into()
            .map_err(|_| FileSystemError::Io("expected FileSystemSyncAccessHandle".into()))?;

    let result = (|| {
        let options = web_sys::FileSystemReadWriteOptions::new();
        options.set_at(0.0);
        // `write` auto-extends, but a shorter rewrite would leave stale bytes;
        // truncate to exactly the content length.
        access
            .truncate_with_f64(contents.len() as f64)
            .map_err(|e| js_io_error("truncating file", e))?;
        access
            .write_with_u8_array_and_options(contents, &options)
            .map_err(|e| js_io_error("writing file contents", e))?;
        access
            .flush()
            .map_err(|e| js_io_error("flushing file", e))?;
        Ok(())
    })();
    // Always release the exclusive lock, even on error.
    access.close();
    result
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
    use super::MountedDirectory;
    use crate::helpers::{unique_did, unique_name};
    use dialog_effects::archive::prelude::*;
    use dialog_effects::memory::prelude::*;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A `FileSystem` provider backed by a fresh OPFS subdirectory. OPFS is
    /// available in the dedicated-worker test runner without a user gesture and
    /// exercises the real web (File System Access API) code path.
    async fn opfs_provider(label: &str) -> crate::provider::FileSystem {
        MountedDirectory::opfs(&unique_name(label))
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

    #[dialog_common::test]
    async fn it_opens_a_mounted_location() -> anyhow::Result<()> {
        use super::FileSystemDirectoryHandleExt;
        use crate::resource::Resource;

        // The host obtains a directory handle (showDirectoryPicker / drag-drop /
        // PWA launch — all gesture-bound, untestable headlessly) and mounts it.
        // We stand in with an OPFS handle, mount it, then open the Location it
        // yields.
        let granted = MountedDirectory::opfs(&unique_name("web-mount-source")).await?;
        let location = granted.handle().mount().await?;

        let provider = crate::provider::FileSystem::open(&location).await?;
        let did = unique_did().await;
        let content = b"opened via mounted Location".to_vec();
        let digest = dialog_common::Blake3Hash::hash(&content);

        did.clone()
            .archive()
            .catalog("index")
            .put(content.clone())
            .perform(&provider)
            .await?;

        // Re-opening the same Location must reach the same directory.
        let reopened = crate::provider::FileSystem::open(&location).await?;
        let result = did
            .archive()
            .catalog("index")
            .get(digest)
            .perform(&reopened)
            .await?;
        assert_eq!(result, Some(content));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_deduplicates_the_same_directory_on_mount() -> anyhow::Result<()> {
        use super::FileSystemDirectoryHandleExt;

        // Mounting two handles for the same physical directory must return the
        // same Location, so a Web Lock derived from it mutually excludes them.
        // Distinct directories must get distinct Locations.
        let dir = MountedDirectory::opfs(&unique_name("web-dedup")).await?;
        let first = dir.handle().mount().await?;
        let again = dir.handle().mount().await?;
        assert_eq!(
            first, again,
            "same directory must reuse its mounted Location"
        );

        let other = MountedDirectory::opfs(&unique_name("web-dedup-other")).await?;
        let other_loc = other.handle().mount().await?;
        assert_ne!(
            first, other_loc,
            "distinct directories must get distinct Locations"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_unmounts_a_directory() -> anyhow::Result<()> {
        use super::FileSystemDirectoryHandleExt;
        use crate::resource::Resource;

        let dir = MountedDirectory::opfs(&unique_name("web-unmount")).await?;
        let location = dir.handle().mount().await?;

        // Mounted: opens fine.
        crate::provider::FileSystem::open(&location).await?;

        // Unmounted: the Location no longer resolves.
        <web_sys::FileSystemDirectoryHandle as FileSystemDirectoryHandleExt>::unmount(&location)
            .await?;
        let reopened = crate::provider::FileSystem::open(&location).await;
        assert!(
            reopened.is_err(),
            "an unmounted Location must no longer resolve"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_writes_via_sync_access_handle() -> anyhow::Result<()> {
        // The Safari ≤ 18 write path: createSyncAccessHandle. Force it directly
        // (regardless of whether createWritable is also present, as on Chrome)
        // and read the bytes back through the normal read path — proving the two
        // write paths are byte-compatible. Sync access handles are worker-only,
        // which the dedicated-worker test runner provides.
        let provider = opfs_provider("web-sync-access").await;
        let handle = provider.resolve("archive")?.resolve("cell.bin")?;

        let (parent, name) = handle
            .navigate_parent(true)
            .await?
            .expect("file path has a parent");
        let file_handle = super::get_file_handle(&parent, &name, true)
            .await?
            .expect("file handle is created");

        // First write.
        let first = b"sync access write".to_vec();
        super::write_via_sync_access(&file_handle, &first).await?;
        assert_eq!(super::read_optional(&handle).await?, Some(first));

        // Shorter rewrite must truncate the stale tail, not leave old bytes.
        let second = b"short".to_vec();
        super::write_via_sync_access(&file_handle, &second).await?;
        assert_eq!(super::read_optional(&handle).await?, Some(second));
        Ok(())
    }
}
