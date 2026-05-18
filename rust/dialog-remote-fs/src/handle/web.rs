//! Web filesystem handle backed by the File System Access API.
//!
//! Wraps a [`web_sys::FileSystemDirectoryHandle`] supplied by the host
//! (typically obtained from `showDirectoryPicker()`). Path segments
//! accumulated via `resolve()` are pure Rust-side bookkeeping; the
//! actual JS-side navigation happens lazily on I/O.
//!
//! On-disk format matches `dialog-storage`'s native FS provider
//! byte-for-byte, so a directory written from the browser can be read
//! by any native `dialog-storage` consumer and vice versa. Writes go
//! through `createWritable()`, which commits atomically on close — no
//! temp+rename dance is needed.

use super::FsHandle;
use crate::FsError;
use async_trait::async_trait;
use js_sys::Uint8Array;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemWritableFileStream,
};

/// Extract a human-readable message from a JS error value.
fn display_js_value(value: &JsValue) -> String {
    if let Some(error) = value.dyn_ref::<js_sys::Error>() {
        return String::from(error.message());
    }
    if let Some(s) = value.as_string() {
        return s;
    }
    format!("{value:?}")
}

/// Returns true if the given JS error is a `NotFoundError` DOMException.
fn is_not_found_error(value: &JsValue) -> bool {
    value
        .dyn_ref::<js_sys::Error>()
        .map(|e| String::from(e.name()) == "NotFoundError")
        .unwrap_or(false)
}

fn js_io_error(op: &str, error: JsValue) -> FsError {
    FsError::Io(format!("{op}: {}", display_js_value(&error)))
}

fn validate_segment(segment: &str) -> Result<(), FsError> {
    if segment.is_empty() {
        return Err(FsError::Containment(
            "empty path segment is not allowed".into(),
        ));
    }
    if segment == "." || segment == ".." {
        return Err(FsError::Containment(format!(
            "navigation segment '{segment}' is not allowed",
        )));
    }
    if segment.contains('/') || segment.contains('\\') || segment.contains('\0') {
        return Err(FsError::Containment(format!(
            "path separator or NUL in segment '{segment}'",
        )));
    }
    Ok(())
}

/// A handle into a registered FS Access API directory tree.
///
/// `root` is the user-picked `FileSystemDirectoryHandle`; `segments` are
/// the relative path under that root. Both `Clone` impls are cheap — JS
/// handles are reference-counted by the engine.
#[derive(Clone)]
pub(crate) struct WebHandle {
    /// Vault id this handle was looked up under. Used to build a stable
    /// Web Locks identifier for CAS critical sections.
    pub(crate) handle_id: String,
    /// The user-picked root directory.
    pub(crate) root: FileSystemDirectoryHandle,
    /// Path segments under the root.
    pub(crate) segments: Vec<String>,
}

impl WebHandle {
    pub(crate) fn new(handle_id: String, root: FileSystemDirectoryHandle) -> Self {
        Self {
            handle_id,
            root,
            segments: Vec::new(),
        }
    }

    /// Walk all but the last segment as directories.
    ///
    /// When `create` is true, missing directories are created. When false,
    /// a missing intermediate returns `Ok(None)` so callers can implement
    /// "absent target" semantics without a separate exists() round-trip.
    async fn navigate_parent(
        &self,
        create: bool,
    ) -> Result<Option<FileSystemDirectoryHandle>, FsError> {
        if self.segments.is_empty() {
            return Ok(Some(self.root.clone()));
        }
        let parent_segments = &self.segments[..self.segments.len() - 1];
        navigate_directory(&self.root, parent_segments, create).await
    }

    /// The leaf path segment, if any.
    fn file_name(&self) -> Option<&str> {
        self.segments.last().map(|s| s.as_str())
    }
}

async fn navigate_directory(
    from: &FileSystemDirectoryHandle,
    segments: &[String],
    create: bool,
) -> Result<Option<FileSystemDirectoryHandle>, FsError> {
    let opts = FileSystemGetDirectoryOptions::new();
    opts.set_create(create);

    let mut current = from.clone();
    for segment in segments {
        match JsFuture::from(current.get_directory_handle_with_options(segment, &opts)).await {
            Ok(value) => {
                current = value.dyn_into().map_err(|_| {
                    FsError::Io(format!(
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
) -> Result<Option<FileSystemFileHandle>, FsError> {
    let opts = FileSystemGetFileOptions::new();
    opts.set_create(create);
    match JsFuture::from(parent.get_file_handle_with_options(name, &opts)).await {
        Ok(value) => value
            .dyn_into::<FileSystemFileHandle>()
            .map(Some)
            .map_err(|_| FsError::Io(format!("expected FileSystemFileHandle for '{name}'"))),
        Err(error) if !create && is_not_found_error(&error) => Ok(None),
        Err(error) => Err(js_io_error(&format!("opening file '{name}'"), error)),
    }
}

#[async_trait(?Send)]
impl FsHandle for WebHandle {
    async fn resolve(&self, segment: &str) -> Result<Self, FsError> {
        validate_segment(segment)?;
        let mut segments = self.segments.clone();
        segments.push(segment.to_string());
        Ok(Self {
            handle_id: self.handle_id.clone(),
            root: self.root.clone(),
            segments,
        })
    }

    async fn read_optional(&self) -> Result<Option<Vec<u8>>, FsError> {
        let Some(parent) = self.navigate_parent(false).await? else {
            return Ok(None);
        };
        let Some(name) = self.file_name() else {
            return Err(FsError::Io(
                "cannot read root directory as a file".into(),
            ));
        };
        let Some(file_handle) = get_file_handle(&parent, name, false).await? else {
            return Ok(None);
        };
        let file: web_sys::File = JsFuture::from(file_handle.get_file())
            .await
            .map_err(|e| js_io_error("getting file", e))?
            .dyn_into()
            .map_err(|_| FsError::Io("expected File".into()))?;
        let buffer: js_sys::ArrayBuffer = JsFuture::from(file.array_buffer())
            .await
            .map_err(|e| js_io_error("reading file contents", e))?
            .dyn_into()
            .map_err(|_| FsError::Io("expected ArrayBuffer".into()))?;
        let bytes = Uint8Array::new(&buffer).to_vec();
        Ok(Some(bytes))
    }

    async fn write(&self, contents: &[u8]) -> Result<(), FsError> {
        let Some(parent) = self.navigate_parent(true).await? else {
            return Err(FsError::Io("parent directory navigation failed".into()));
        };
        let Some(name) = self.file_name() else {
            return Err(FsError::Io(
                "cannot write to root directory as a file".into(),
            ));
        };
        let file_handle = get_file_handle(&parent, name, true)
            .await?
            .ok_or_else(|| FsError::Io(format!("could not create file '{name}'")))?;

        let writable: FileSystemWritableFileStream = JsFuture::from(file_handle.create_writable())
            .await
            .map_err(|e| js_io_error("creating writable stream", e))?
            .dyn_into()
            .map_err(|_| FsError::Io("expected FileSystemWritableFileStream".into()))?;

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

    async fn rename(&self, to: &Self) -> Result<(), FsError> {
        // File System Access API has `FileSystemHandle.move(parent, name)`
        // in modern browsers, but web_sys doesn't currently expose it.
        // Fall back to copy + delete; the caller's lock prevents racing.
        let contents = self
            .read_optional()
            .await?
            .ok_or_else(|| FsError::Io("rename source does not exist".into()))?;
        to.write(&contents).await?;
        self.remove().await
    }

    async fn remove(&self) -> Result<(), FsError> {
        let Some(parent) = self.navigate_parent(false).await? else {
            // Parent directory missing — target can't exist either.
            return Ok(());
        };
        let Some(name) = self.file_name() else {
            return Err(FsError::Io(
                "cannot remove root directory through FsHandle".into(),
            ));
        };
        match JsFuture::from(parent.remove_entry(name)).await {
            Ok(_) => Ok(()),
            Err(error) if is_not_found_error(&error) => Ok(()),
            Err(error) => Err(js_io_error(&format!("removing entry '{name}'"), error)),
        }
    }

    async fn list(&self) -> Result<Vec<String>, FsError> {
        // TODO: iterate `FileSystemDirectoryHandle.entries()`. None of the
        // current providers exercise this; landing the iterator wiring
        // alongside the first consumer will get it right faster than
        // building it blind.
        Err(FsError::Io(
            "WebHandle::list is not yet implemented".into(),
        ))
    }

    async fn exists(&self) -> bool {
        let Some(parent) = self.navigate_parent(false).await.ok().flatten() else {
            return false;
        };
        let Some(name) = self.file_name() else {
            // Handle for the root itself — it exists by construction.
            return true;
        };
        // Probe as a file first; if NotFound, try directory.
        if get_file_handle(&parent, name, false)
            .await
            .ok()
            .flatten()
            .is_some()
        {
            return true;
        }
        navigate_directory(&parent, std::slice::from_ref(&name.to_string()), false)
            .await
            .ok()
            .flatten()
            .is_some()
    }

    async fn ensure_dir(&self) -> Result<(), FsError> {
        navigate_directory(&self.root, &self.segments, true)
            .await
            .map(|_| ())
    }
}
