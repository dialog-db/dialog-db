//! Native filesystem backend for [`FileSystemHandle`], backed by [`tokio::fs`].
//!
//! The handle's `file:` URL is converted to a [`PathBuf`] for each operation.
//! Path layout and containment live in the shared [`super`] module; this file
//! only performs I/O.

use super::{FileReader, FileSystem, FileSystemError, FileSystemHandle};
use crate::resource::Resource;
use async_stream::try_stream;
use dialog_effects::storage::{Directory, Location};
use std::env;
use std::io;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use url::Url;

/// Chunk size for streaming file reads.
const STREAM_CHUNK_SIZE: usize = 64 * 1024;

/// Subdirectory name used to namespace dialog storage inside the
/// platform's data and temp directories.
pub(super) const STORAGE_NAMESPACE: &str = "dialog";

/// The characters that cannot appear in a Windows path component, as a
/// [`percent_encoding::AsciiSet`]: `< > : " \ | ? *` and controls, per
/// <https://learn.microsoft.com/windows/win32/fileio/naming-a-file>, plus
/// `%` itself so decoding is unambiguous.
///
/// DIDs (`did:key:z6Mk...`) are used directly as directory names by the
/// certificate/credential layout; writing e.g. `certificate/{did}/...` fails
/// with ERROR_INVALID_NAME (os error 123) unless the colon is escaped.
///
/// One might expect `Url::to_file_path` to take care of this, but it does
/// the opposite: it percent-*decodes* every path segment (see
/// `file_url_segments_to_pathbuf_windows` in the `url` crate) and escapes
/// nothing, so a reserved character reaches the `PathBuf` raw — and one
/// escaped at the URL layer gets unescaped on the way through. The escape
/// therefore has to be (re)applied after that conversion, at the OS-path
/// boundary. The encoding itself is the standard percent-encoding the URL
/// stack already uses — only the *set* of characters is ours.
///
/// Applied per `Normal` path component, so the drive-letter colon (a
/// `Prefix` component) is never touched. Windows-only; other targets keep
/// their exact on-disk layout.
#[cfg(windows)]
const WINDOWS_RESERVED: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
    .add(b'<')
    .add(b'>')
    .add(b':')
    .add(b'"')
    .add(b'\\')
    .add(b'|')
    .add(b'?')
    .add(b'*')
    .add(b'%');

/// Percent-encode the characters of [`WINDOWS_RESERVED`] in one path
/// component. Reversible (`:` -> `%3A`), so distinct names never collide
/// and [`decode_reserved`] recovers the original.
#[cfg(windows)]
fn encode_reserved(component: &str) -> String {
    percent_encoding::utf8_percent_encode(component, WINDOWS_RESERVED).to_string()
}

/// Inverse of [`encode_reserved`]. A name that decodes to non-UTF-8 (only
/// possible for files created out-of-band) is returned as-is.
#[cfg(windows)]
fn decode_reserved(name: &str) -> String {
    percent_encoding::percent_decode_str(name)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .unwrap_or_else(|_| name.to_string())
}

/// Render an OS path, percent-encoding reserved characters in every `Normal`
/// component. Rebuilding from `components()` also drops any trailing separator
/// (which on Windows otherwise yields os error 267).
#[cfg(windows)]
fn encode_windows_path(path: PathBuf) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::Normal(os) => match os.to_str() {
                Some(s) => out.push(encode_reserved(s)),
                None => out.push(os),
            },
            other => out.push(other.as_os_str()),
        }
    }
    out
}

#[async_trait::async_trait]
impl Resource<Location> for FileSystem {
    type Error = FileSystemError;

    async fn open(location: &Location) -> Result<Self, FileSystemError> {
        Ok(Self(FileSystemHandle::try_from(location)?))
    }
}

/// Resolve a `Location` (Directory + name) to a filesystem handle.
impl TryFrom<&Location> for FileSystemHandle {
    type Error = FileSystemError;

    fn try_from(location: &Location) -> Result<Self, FileSystemError> {
        let base = match &location.directory {
            Directory::Profile => {
                let data_dir = dirs::data_dir().ok_or_else(|| {
                    FileSystemError::Io("could not determine platform data directory".into())
                })?;
                data_dir.join(STORAGE_NAMESPACE)
            }
            Directory::Current => {
                env::current_dir().map_err(|e| FileSystemError::Io(e.to_string()))?
            }
            Directory::Temp => env::temp_dir(),
            Directory::At(path) => PathBuf::from(path),
        };

        let path = if location.name.is_empty() {
            base
        } else {
            base.join(&location.name)
        };

        path.try_into()
    }
}

impl TryFrom<PathBuf> for FileSystemHandle {
    type Error = FileSystemError;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        // Ensure the path is absolute for proper URL conversion
        let absolute = if path.is_absolute() {
            path
        } else {
            env::current_dir()
                .map_err(|e| FileSystemError::Io(e.to_string()))?
                .join(path)
        };

        // Convert to file: URL, ensuring trailing slash for directory
        let url = Url::from_file_path(&absolute)
            .map_err(|_| FileSystemError::Io("Invalid path for URL conversion".to_string()))?;

        url.try_into()
    }
}

impl TryFrom<FileSystemHandle> for PathBuf {
    type Error = FileSystemError;

    fn try_from(location: FileSystemHandle) -> Result<Self, Self::Error> {
        PathBuf::try_from(&location)
    }
}

/// `Url::to_file_path` percent-decodes every segment, so a `%2F` in the
/// URL becomes a separator in the OS path and `%2E%2E` a parent: the
/// path can have more, or other, components than the URL has segments,
/// and land outside the name that was resolved. `FileSystemHandle::resolve`
/// encodes `%` so a handle it produced cannot carry one, but a handle
/// can also be built from a raw URL; this is the check at the OS-path
/// boundary. Every decoded segment must be one plain component, and the
/// last of them must be the path's final component.
fn verify_segments(url: &Url, path: &std::path::Path) -> Result<(), FileSystemError> {
    use std::path::Component;

    let mut last = None;
    for segment in url.path_segments().into_iter().flatten() {
        if segment.is_empty() {
            continue;
        }
        let decoded = percent_encoding::percent_decode_str(segment).decode_utf8_lossy();
        if matches!(decoded.as_ref(), "." | "..") || decoded.chars().any(std::path::is_separator) {
            return Err(FileSystemError::Containment(format!(
                "URL segment '{segment}' does not decode to one path component"
            )));
        }
        last = Some(decoded);
    }

    if let (Some(expected), Some(Component::Normal(actual))) = (last, path.components().next_back())
        && actual.to_str() != Some(expected.as_ref())
    {
        return Err(FileSystemError::Containment(format!(
            "URL segment '{expected}' is not the path's final component {actual:?}"
        )));
    }
    Ok(())
}

impl TryFrom<&FileSystemHandle> for PathBuf {
    type Error = FileSystemError;

    fn try_from(location: &FileSystemHandle) -> Result<Self, Self::Error> {
        let path = location
            .url()
            .to_file_path()
            .map_err(|_| FileSystemError::Io("Failed to convert URL to path".to_string()))?;
        verify_segments(location.url(), &path)?;

        // On Windows, escape characters illegal in filenames (DIDs carry
        // colons) per path component, and drop the trailing separator that
        // `components()` rebuilding naturally removes. See `encode_windows_path`.
        #[cfg(windows)]
        {
            Ok(encode_windows_path(path))
        }

        // Strip trailing slash added by FileSystemHandle for URL semantics.
        // Filesystem operations (read, write, rename) need clean file paths.
        // Use `to_str` (not `to_string_lossy`) so non-UTF-8 paths don't
        // collide via lossy substitutions; require UTF-8 for the trim.
        #[cfg(not(windows))]
        {
            let s = path.to_str().ok_or_else(|| {
                FileSystemError::Io("Path is not valid UTF-8 and cannot be normalized".to_string())
            })?;
            if s.ends_with('/') && s.len() > 1 {
                Ok(PathBuf::from(s.trim_end_matches('/')))
            } else {
                Ok(path)
            }
        }
    }
}

pub(super) async fn ensure_dir(handle: &FileSystemHandle) -> Result<(), FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    fs::create_dir_all(&path)
        .await
        .map_err(|e| FileSystemError::Io(e.to_string()))
}

pub(super) async fn read(handle: &FileSystemHandle) -> Result<Vec<u8>, FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    match fs::read(&path).await {
        Ok(bytes) => Ok(bytes),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            Err(FileSystemError::NotFound(path.display().to_string()))
        }
        Err(e) => Err(FileSystemError::Io(e.to_string())),
    }
}

pub(super) async fn read_optional(
    handle: &FileSystemHandle,
) -> Result<Option<Vec<u8>>, FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    match fs::read(&path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(FileSystemError::Io(e.to_string())),
    }
}

pub(super) async fn files(handle: &FileSystemHandle) -> Result<Vec<String>, FileSystemError> {
    let io = |error: io::Error| FileSystemError::Io(error.to_string());
    let mut files = Vec::new();
    let mut pending: Vec<(PathBuf, String)> = vec![(handle.try_into()?, String::new())];

    while let Some((directory, prefix)) = pending.pop() {
        let mut entries = match fs::read_dir(&directory).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io(error)),
        };
        while let Some(entry) = entries.next_entry().await.map_err(io)? {
            let name = entry.file_name().into_string().map_err(|name| {
                FileSystemError::Io(format!("file name {name:?} is not valid UTF-8"))
            })?;
            #[cfg(windows)]
            let name = decode_reserved(&name);
            let path = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            if entry.file_type().await.map_err(io)?.is_dir() {
                pending.push((entry.path(), path));
            } else {
                files.push(path);
            }
        }
    }

    Ok(files)
}

pub(super) async fn write(
    handle: &FileSystemHandle,
    contents: &[u8],
) -> Result<(), FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))?;
    }
    fs::write(&path, contents)
        .await
        .map_err(|e| FileSystemError::Io(e.to_string()))
}

/// Write `contents` to `handle` atomically: stage into a sibling temp file,
/// then `fs::rename` it into place (one atomic syscall on the same filesystem),
/// so a concurrent reader never sees a partial write. A plain `fs::write` is
/// not atomic, which is why the temp+rename is kept on native.
pub(super) async fn write_atomic(
    handle: &FileSystemHandle,
    contents: &[u8],
) -> Result<(), FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))?;
    }

    // A sibling temp path so the rename stays on the same filesystem (atomic).
    // The unique suffix keeps concurrent writers of the same target from
    // clobbering each other's staging file.
    let tmp = path.with_extension(format!("{}.tmp", unique_suffix()));
    fs::write(&tmp, contents)
        .await
        .map_err(|e| FileSystemError::Io(e.to_string()))?;
    match fs::rename(&tmp, &path).await {
        Ok(()) => Ok(()),
        Err(e) => {
            // Don't leak the staging file if the rename failed.
            let _ = fs::remove_file(&tmp).await;
            Err(FileSystemError::Io(e.to_string()))
        }
    }
}

/// A process-unique suffix for staging temp files.
fn unique_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{n}", std::process::id())
}

pub(super) async fn rename(
    from: &FileSystemHandle,
    to: &FileSystemHandle,
) -> Result<(), FileSystemError> {
    let from_path: PathBuf = from.try_into()?;
    let to_path: PathBuf = to.try_into()?;
    fs::rename(&from_path, &to_path)
        .await
        .map_err(|e| FileSystemError::Io(e.to_string()))
}

pub(super) async fn remove(handle: &FileSystemHandle) -> Result<(), FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    match fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(FileSystemError::Io(e.to_string())),
    }
}

pub(super) async fn list(handle: &FileSystemHandle) -> Result<Vec<String>, FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    let mut entries = match fs::read_dir(&path).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(FileSystemError::Io(e.to_string())),
    };

    let mut names = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| FileSystemError::Io(e.to_string()))?
    {
        if let Some(name) = entry.file_name().to_str() {
            // On-disk names are escaped (see `encode_windows_path`); decode
            // so callers always see logical names and re-encode correctly on
            // the next resolve.
            #[cfg(windows)]
            names.push(decode_reserved(name));
            #[cfg(not(windows))]
            names.push(name.to_string());
        }
    }
    Ok(names)
}

pub(super) async fn size(handle: &FileSystemHandle) -> Result<Option<u64>, FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    match fs::metadata(&path).await {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
        Ok(_) => Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(FileSystemError::Io(error.to_string())),
    }
}

pub(super) async fn exists(handle: &FileSystemHandle) -> bool {
    let Ok(path) = PathBuf::try_from(handle) else {
        return false;
    };
    path.exists()
}

/// Open a streaming reader over the file, starting at `offset` and yielding at
/// most `len` bytes (all of it when `len` is `None`). Chunks are owned `Vec`s
/// so nothing buffers the whole file.
pub(super) async fn open_reader(
    handle: &FileSystemHandle,
    offset: u64,
    len: Option<u64>,
) -> Result<FileReader, FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    let mut file = fs::File::open(&path)
        .await
        .map_err(|e| FileSystemError::Io(e.to_string()))?;
    if offset > 0 {
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))?;
    }
    let stream = try_stream! {
        let mut file = file;
        let mut remaining = len;
        let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
        loop {
            let cap = match remaining {
                Some(0) => break,
                Some(r) => (r as usize).min(STREAM_CHUNK_SIZE),
                None => STREAM_CHUNK_SIZE,
            };
            let read = file
                .read(&mut buf[..cap])
                .await
                .map_err(|e| FileSystemError::Io(e.to_string()))?;
            if read == 0 {
                break;
            }
            if let Some(r) = remaining.as_mut() {
                *r -= read as u64;
            }
            yield buf[..read].to_vec();
        }
    };
    Ok(Box::pin(stream))
}

/// Open a streaming writer that stages into a sibling temp file and renames it
/// into place on [`FileWriter::finish`] — the same atomic temp+rename
/// `write_atomic` uses, but fed incrementally so multi-gigabyte content never
/// lands in a single buffer.
pub(super) async fn open_writer(handle: &FileSystemHandle) -> Result<FileWriter, FileSystemError> {
    let target: PathBuf = handle.try_into()?;
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))?;
    }
    let tmp = target.with_extension(format!("{}.tmp", unique_suffix()));
    let file = fs::File::create(&tmp)
        .await
        .map_err(|e| FileSystemError::Io(e.to_string()))?;
    Ok(FileWriter { file, tmp, target })
}

/// A streaming, atomically-committed file writer. Bytes go to a temp file;
/// [`finish`](Self::finish) flushes and renames it into place. Dropping without
/// `finish` leaves the temp file (cleaned up best-effort).
pub struct FileWriter {
    file: fs::File,
    tmp: PathBuf,
    target: PathBuf,
}

impl FileWriter {
    /// Append a chunk to the staged file.
    pub async fn write_all(&mut self, bytes: &[u8]) -> Result<(), FileSystemError> {
        self.file
            .write_all(bytes)
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))
    }

    /// Flush and atomically move the staged file into place.
    pub async fn finish(self) -> Result<(), FileSystemError> {
        let target = self.target.clone();
        self.finish_to_path(&target).await
    }

    /// Flush and atomically move the staged file to `dest` instead of the
    /// handle it was opened on. Lets a content-addressed writer pick the final
    /// path (the hash) only once the content has been streamed and hashed.
    pub async fn finish_to(self, dest: &FileSystemHandle) -> Result<(), FileSystemError> {
        let dest_path: PathBuf = dest.try_into()?;
        self.finish_to_path(&dest_path).await
    }

    async fn finish_to_path(mut self, dest: &PathBuf) -> Result<(), FileSystemError> {
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| FileSystemError::Io(e.to_string()))?;
        }
        self.file
            .flush()
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))?;
        self.file
            .sync_all()
            .await
            .map_err(|e| FileSystemError::Io(e.to_string()))?;
        match fs::rename(&self.tmp, dest).await {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = fs::remove_file(&self.tmp).await;
                Err(FileSystemError::Io(e.to_string()))
            }
        }
    }

    /// Discard the staged content without committing it.
    pub async fn discard(self) -> Result<(), FileSystemError> {
        let _ = fs::remove_file(&self.tmp).await;
        Ok(())
    }
}

/// Acquire a cross-process write lock for the given handle's CAS critical
/// section. The lock file lives at `{path}.lock`; see [`LockGuard`].
pub(crate) async fn lock(handle: &FileSystemHandle) -> Result<LockGuard, FileSystemError> {
    let path: PathBuf = handle.try_into()?;
    let lock_path = lock_path(&path);
    // Ensure parent directory exists for the lock file
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| FileSystemError::Io(e.to_string()))?;
    }
    LockGuard::acquire(lock_path)
}

/// The lock file for the cell at `path`: `{path}.lock`. The suffix is
/// appended rather than set as the extension, so cells `a.x` and `a.y` get
/// locks of their own instead of sharing `a.lock`.
pub(crate) fn lock_path(path: &Path) -> PathBuf {
    sibling(path, ".lock")
}

/// `path` with `suffix` appended to its last component.
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// How many times [`LockGuard::acquire`] tries to claim the lock: once, and
/// once more when the lock file it found was released, or was recovered
/// from a holder that is gone, before it could read it.
const CLAIM_ATTEMPTS: usize = 2;

/// A cell's write lock: the lock file `{cell}.lock`, holding `"{pid} {token}"`
/// for the process that holds it and a random token naming this one hold.
///
/// The lock is a plain file rather than an OS lock (`flock`), so it works on
/// network and FUSE filesystems, where an OS lock errors or is silently not
/// enforced. It follows four rules:
///
/// - **A lock file is held unless proven otherwise.** One that is empty or
///   does not read as `pid token` is held, and is never deleted: it may be a
///   writer that has created it and not yet written it. The one exception is
///   a bare decimal `pid`, the lock file the previous release wrote, which
///   reads as `pid 0`: held while that process runs, recovered like any
///   other once it is gone.
/// - **A claim never shows an empty lock file.** The content is written to a
///   temporary file `{lock}.{pid}.{token}.tmp` first and hard-linked to the
///   lock path, which fails atomically if a lock file is there. Where hard
///   links are not supported, the lock file is created with `create_new` and
///   written after; by the first rule, a writer that arrives in between is
///   refused. (A half-written `pid token` there can read as a bare PID, but
///   recovering it needs a hard link, which such a filesystem refuses.)
/// - **A lock whose holder is gone is recovered by one writer.** A writer that
///   reads `pid token` naming a process that is not running links the lock
///   file to `{lock}.stale.{token}`, a name only one writer can create. It
///   then checks that the file it linked still reads what it read (the lock
///   may have been recovered and claimed again in between), removes the lock
///   file, removes its link, and claims again.
/// - **A guard releases only its own hold.** On drop it removes the lock file
///   if the file still reads this guard's `pid token`, and leaves it
///   otherwise. No running holder's lock is ever recovered, so nothing
///   replaces the file between that read and the removal.
///
/// A writer that finds the lock held is refused at once with
/// [`FileSystemError::Lock`]; nothing waits here. A lock naming this process
/// is held, so two tasks of one process exclude each other.
///
/// Whether a holder is running is asked of the OS, in this process's PID
/// namespace: writers sharing a store must share one (a writer in another
/// container sees a PID that means nothing here). If a gone holder's PID has
/// since been given to another process, its lock looks held until the lock
/// file is deleted by hand. So does a gone holder's lock on a filesystem
/// without hard links, and one whose recovery was interrupted by a crash
/// (delete its `{lock}.stale.{token}` too).
pub(crate) struct LockGuard {
    path: PathBuf,
    content: String,
}

/// What a writer that found the lock file learned by reading it.
enum Holder {
    /// The lock file was gone by the time it was read.
    Released,
    /// Held: by a running process, by this one, or by a writer whose
    /// content does not read as `pid token`.
    Held,
    /// Names a process that is not running.
    Gone {
        /// The content read, which recovery checks the file still holds.
        content: Vec<u8>,
        /// The gone holder's token, which names the recovery link.
        token: u64,
    },
}

impl LockGuard {
    /// Take the lock at the given path.
    ///
    /// If the lock is held, returns [`FileSystemError::Lock`] immediately
    /// rather than waiting; the caller decides whether to try again.
    fn acquire(path: PathBuf) -> Result<Self, FileSystemError> {
        let pid = std::process::id();
        for _ in 0..CLAIM_ATTEMPTS {
            if let Some(guard) = Self::claim(&path, pid)? {
                return Ok(guard);
            }
            match holder(&path, pid)? {
                Holder::Released => {}
                Holder::Held => break,
                Holder::Gone { content, token } => recover(&path, &content, token)?,
            }
        }
        Err(FileSystemError::Lock(format!(
            "Concurrent write in progress (lock {} held)",
            path.display()
        )))
    }

    /// Create the lock file with this hold's content already in it, or
    /// return `None` if a lock file is there.
    fn claim(path: &Path, pid: u32) -> Result<Option<Self>, FileSystemError> {
        let token = token();
        let content = format!("{pid} {token}");
        let tmp = sibling(path, &format!(".{pid}.{token}.tmp"));
        write_new(&tmp, content.as_bytes()).map_err(|e| {
            FileSystemError::Lock(format!("Failed to create lock file {}: {e}", tmp.display()))
        })?;
        let linked = std::fs::hard_link(&tmp, path);
        let _ = std::fs::remove_file(&tmp);
        let created = match linked {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => return Ok(None),
            // No hard links here: create the file and write it after.
            Err(_) => write_new(path, content.as_bytes()),
        };
        match created {
            Ok(()) => Ok(Some(Self {
                path: path.to_path_buf(),
                content,
            })),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(None),
            Err(e) => Err(FileSystemError::Lock(format!(
                "Failed to acquire lock {}: {e}",
                path.display()
            ))),
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if std::fs::read(&self.path).is_ok_and(|now| now == self.content.as_bytes()) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Create the file at `path`, which must not exist, holding `content`. A
/// file that was created but could not be written is removed.
fn write_new(path: &Path, content: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    if let Err(e) = file.write_all(content) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(e);
    }
    Ok(())
}

/// Read the lock file at `path`, which a claim found there, and say who
/// holds it.
fn holder(path: &Path, pid: u32) -> Result<Holder, FileSystemError> {
    let content = match std::fs::read(path) {
        Ok(content) => content,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Holder::Released),
        Err(e) => {
            return Err(FileSystemError::Lock(format!(
                "Failed to read lock {}: {e}",
                path.display()
            )));
        }
    };
    match parse_holder(&content) {
        Some((holder, token)) if holder != pid && !is_running(holder) => {
            Ok(Holder::Gone { content, token })
        }
        _ => Ok(Holder::Held),
    }
}

/// Remove the lock file at `path` if it still holds `content`, the lock of
/// a holder that is gone, unless another writer is recovering it.
fn recover(path: &Path, content: &[u8], token: u64) -> Result<(), FileSystemError> {
    let link = sibling(path, &format!(".stale.{token}"));
    match std::fs::hard_link(path, &link) {
        Ok(()) => {}
        // Another writer is recovering it, or already has.
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::AlreadyExists | io::ErrorKind::NotFound
            ) =>
        {
            return Ok(());
        }
        Err(e) => {
            return Err(FileSystemError::Lock(format!(
                "Cannot recover lock {} from a holder that is gone ({e}); \
                 delete it by hand if no writer is running",
                path.display()
            )));
        }
    }
    // The link names the file that was at the lock path when it was made.
    // If that is still the gone holder's, nobody else can remove it while
    // the link exists, so removing the lock path removes that file.
    let unchanged = std::fs::read(&link).is_ok_and(|now| now == content);
    let removed = if unchanged {
        match std::fs::remove_file(path) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    } else {
        Ok(())
    };
    let _ = std::fs::remove_file(&link);
    removed.map_err(|e| {
        FileSystemError::Lock(format!("Failed to recover lock {}: {e}", path.display()))
    })
}

/// Read a lock file's content as `"{pid} {token}"`, both decimal, or as a
/// bare decimal `"{pid}"`, the lock file the previous release (`pidlock`)
/// wrote, which reads as token `0`. A PID no process can have (zero, or past
/// `i32::MAX`) does not read.
fn parse_holder(content: &[u8]) -> Option<(u32, u64)> {
    let text = std::str::from_utf8(content).ok()?;
    let (pid, token) = text.split_once(' ').unwrap_or((text, "0"));
    let decimal = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !decimal(pid) || !decimal(token) {
        return None;
    }
    let pid: u32 = pid.parse().ok()?;
    if pid == 0 || i32::try_from(pid).is_err() {
        return None;
    }
    Some((pid, token.parse().ok()?))
}

/// A token for one hold: random, so two holds by one process, or by two
/// processes given the same PID, do not share one.
fn token() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    // `RandomState` is seeded from the OS's random source.
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    hasher.write_u128(now);
    hasher.finish()
}

/// Whether the process `pid` is running, in this process's PID namespace.
/// Anything short of proof that it is not counts as running.
#[cfg(unix)]
fn is_running(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return true;
    };
    // SAFETY: signal 0 delivers nothing; `kill` only checks that `pid`
    // exists and could be signalled, and `pid` is positive, so it names one
    // process rather than a group.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    // EPERM: it exists, and belongs to another user.
    io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

/// Whether the process `pid` is running. Anything short of proof that it is
/// not counts as running.
#[cfg(windows)]
fn is_running(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_INVALID_PARAMETER, GetLastError, STILL_ACTIVE,
    };
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    // SAFETY: `OpenProcess` takes no pointers; the handle it returns is
    // checked for null, passed only to `GetExitCodeProcess` with a valid
    // out-pointer, and closed once.
    unsafe {
        let process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if process.is_null() {
            // No process has this id; access denied means one does.
            return GetLastError() != ERROR_INVALID_PARAMETER;
        }
        let mut code = 0u32;
        let read = GetExitCodeProcess(process, &mut code);
        CloseHandle(process);
        read == 0 || code == STILL_ACTIVE as u32
    }
}

/// No way to ask on this platform: every holder counts as running.
#[cfg(not(any(unix, windows)))]
fn is_running(_pid: u32) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    /// A PID no process has: Linux's `pid_max` is at most 2^22.
    const GONE_PID: u32 = 999_999_999;

    /// The names in `dir`, sorted.
    fn names(dir: &Path) -> anyhow::Result<Vec<String>> {
        let mut names = std::fs::read_dir(dir)?
            .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
            .collect::<anyhow::Result<Vec<_>>>()?;
        names.sort();
        Ok(names)
    }

    #[dialog_common::test]
    async fn it_fails_lock_when_held_by_same_process() -> anyhow::Result<()> {
        // Verifies that when our own process holds the lock, acquire returns
        // an error immediately (not a spin). This matters because all tests,
        // and every task of one peer, take their locks in one process.
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        let _guard = LockGuard::acquire(lock_path.clone())?;

        // Second acquire from same process should fail immediately
        let result = LockGuard::acquire(lock_path);
        let err = match result {
            Ok(_) => panic!("expected lock to fail when held by same process"),
            Err(e) => e,
        };
        assert!(
            matches!(err, FileSystemError::Lock(_)),
            "expected Lock error, got: {err:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_lock_with_trailing_slash_path() -> anyhow::Result<()> {
        // Reproduces the bug where FileSystemHandle's trailing-slash URLs
        // produced PathBufs like "/tmp/.../test.lock/", which can never be
        // opened as a file. That once caused an infinite busy loop in the
        // old unbounded retry code.
        let dir = tempfile::tempdir()?;
        let bad_path = dir.path().join("cell.lock/"); // trailing slash
        let result = LockGuard::acquire(bad_path);

        // Should fail with a Lock error, not spin forever
        let err = match result {
            Ok(_) => panic!("expected lock to fail with trailing-slash path"),
            Err(e) => e,
        };
        assert!(
            matches!(err, FileSystemError::Lock(_)),
            "expected Lock error, got: {err:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_lock_when_directory_exists_at_lock_path() -> anyhow::Result<()> {
        // If a directory exists where the lock file should be (e.g. from
        // a previous buggy run), acquire should fail, not spin or panic.
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        std::fs::create_dir_all(&lock_path)?;
        assert!(lock_path.is_dir());

        let err = match LockGuard::acquire(lock_path) {
            Ok(_) => panic!("expected lock to fail when a directory exists at the lock path"),
            Err(e) => e,
        };
        assert!(
            matches!(err, FileSystemError::Lock(_)),
            "expected Lock error, got: {err:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_takes_a_lock_again_once_released() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        drop(LockGuard::acquire(lock_path.clone())?);
        let _again = LockGuard::acquire(lock_path)?;
        Ok(())
    }

    #[dialog_common::test]
    async fn it_writes_the_holder_into_the_lock_file() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        let _guard = LockGuard::acquire(lock_path.clone())?;
        let content = std::fs::read(&lock_path)?;
        let (pid, _token) = parse_holder(&content).expect("lock file reads as `pid token`");
        assert_eq!(pid, std::process::id());
        // The claim's temporary file is gone.
        assert_eq!(names(dir.path())?, ["cell.lock"]);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_removes_a_released_lock_file() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        drop(LockGuard::acquire(lock_path.clone())?);
        assert!(!lock_path.exists(), "a released lock file is removed");
        assert!(names(dir.path())?.is_empty());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_holds_a_lock_whose_file_is_empty() -> anyhow::Result<()> {
        // The PID-file lock this replaced deleted a lock file it found empty
        // as corrupt, which let a second writer in while the first had
        // created the file but not yet written its PID. An empty lock file is
        // held, and nobody deletes it.
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        std::fs::write(&lock_path, b"")?;
        assert!(matches!(
            LockGuard::acquire(lock_path.clone()),
            Err(FileSystemError::Lock(_))
        ));
        assert_eq!(std::fs::read(&lock_path)?, b"");
        // The refused claim left no temporary file behind.
        assert_eq!(names(dir.path())?, ["cell.lock"]);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_holds_a_lock_whose_file_does_not_read_as_pid_and_token() -> anyhow::Result<()> {
        // A half-written claim, a PID with anything but a token after it,
        // and garbage are all held, whatever process they name.
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        for content in [
            format!("{GONE_PID}\n"),
            format!(" {GONE_PID}"),
            format!("{GONE_PID} "),
            format!("{GONE_PID} x"),
            format!("{GONE_PID} 7\n"),
            "0 7".to_string(),
            "not a lock".to_string(),
        ] {
            std::fs::write(&lock_path, &content)?;
            assert!(
                matches!(
                    LockGuard::acquire(lock_path.clone()),
                    Err(FileSystemError::Lock(_))
                ),
                "{content:?} is held"
            );
            assert_eq!(std::fs::read(&lock_path)?, content.as_bytes());
        }
        Ok(())
    }

    #[dialog_common::test]
    async fn it_recovers_a_bare_pid_lock_whose_holder_is_gone() -> anyhow::Result<()> {
        // The previous release's lock file is a bare PID. One left by a
        // writer that crashed is recovered, through `{lock}.stale.0`.
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        std::fs::write(&lock_path, GONE_PID.to_string())?;
        let _guard = LockGuard::acquire(lock_path.clone())?;
        let content = std::fs::read(&lock_path)?;
        assert_eq!(
            parse_holder(&content).map(|(pid, _)| pid),
            Some(std::process::id())
        );
        assert_eq!(names(dir.path())?, ["cell.lock"]);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_refuses_a_bare_pid_lock_naming_a_running_process() -> anyhow::Result<()> {
        // A previous release's lock file naming a running process (this
        // one, written by hand) is held and left alone.
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        let content = std::process::id().to_string();
        std::fs::write(&lock_path, &content)?;
        assert!(matches!(
            LockGuard::acquire(lock_path.clone()),
            Err(FileSystemError::Lock(_))
        ));
        assert_eq!(std::fs::read_to_string(&lock_path)?, content);
        assert_eq!(names(dir.path())?, ["cell.lock"]);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_refuses_a_lock_naming_a_running_process() -> anyhow::Result<()> {
        // A lock naming this process, written by hand: running, so held.
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        let content = format!("{} 7", std::process::id());
        std::fs::write(&lock_path, &content)?;
        assert!(matches!(
            LockGuard::acquire(lock_path.clone()),
            Err(FileSystemError::Lock(_))
        ));
        assert_eq!(std::fs::read_to_string(&lock_path)?, content);

        // A running process other than this one, which the liveness check
        // has to answer for: the test runner that started this process.
        #[cfg(unix)]
        {
            let content = format!("{} 7", std::os::unix::process::parent_id());
            std::fs::write(&lock_path, &content)?;
            assert!(matches!(
                LockGuard::acquire(lock_path.clone()),
                Err(FileSystemError::Lock(_))
            ));
            assert_eq!(std::fs::read_to_string(&lock_path)?, content);
        }
        Ok(())
    }

    #[dialog_common::test]
    async fn it_takes_a_lock_left_by_a_holder_that_is_gone() -> anyhow::Result<()> {
        // A lock file left behind by a holder that crashed is recovered.
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        std::fs::write(&lock_path, format!("{GONE_PID} 7"))?;
        let _guard = LockGuard::acquire(lock_path.clone())?;
        let content = std::fs::read(&lock_path)?;
        assert_eq!(
            parse_holder(&content).map(|(pid, _)| pid),
            Some(std::process::id())
        );
        // Neither the recovery link nor a temporary file is left behind.
        assert_eq!(names(dir.path())?, ["cell.lock"]);
        Ok(())
    }

    #[dialog_common::test]
    fn it_recovers_a_lock_whose_holder_is_gone_exactly_once() -> anyhow::Result<()> {
        // Writers that all find the same gone holder must not all take its
        // lock: exactly one of them holds it.
        const WRITERS: usize = 8;
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        for round in 0..100 {
            std::fs::write(&lock_path, format!("{GONE_PID} {round}"))?;
            let start = Arc::new(Barrier::new(WRITERS));
            let done = Arc::new(Barrier::new(WRITERS));
            let threads: Vec<_> = (0..WRITERS)
                .map(|_| {
                    let lock_path = lock_path.clone();
                    let start = start.clone();
                    let done = done.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        let result = LockGuard::acquire(lock_path);
                        // Hold until everyone has tried.
                        done.wait();
                        match result {
                            Ok(_guard) => true,
                            Err(FileSystemError::Lock(_)) => false,
                            Err(other) => panic!("expected a Lock refusal, got {other:?}"),
                        }
                    })
                })
                .collect();
            let held = threads
                .into_iter()
                .map(|thread| thread.join().expect("a writer panicked"))
                .filter(|held| *held)
                .count();
            assert_eq!(held, 1, "round {round}: {held} writers held the lock");
            assert!(names(dir.path())?.is_empty(), "round {round}");
        }
        Ok(())
    }

    #[dialog_common::test]
    async fn it_leaves_a_lock_file_another_holder_replaced() -> anyhow::Result<()> {
        // A guard releases only its own hold: if its file now names another
        // holder, dropping the guard leaves that holder's file in place.
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        let guard = LockGuard::acquire(lock_path.clone())?;
        let other = format!("{GONE_PID} 7");
        std::fs::write(&lock_path, &other)?;
        drop(guard);
        assert_eq!(std::fs::read_to_string(&lock_path)?, other);
        Ok(())
    }

    #[dialog_common::test]
    fn it_never_lets_two_writers_hold_one_lock() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = tempfile::tempdir()?;
        let lock_path = dir.path().join("cell.lock");
        let inside = Arc::new(AtomicUsize::new(0));
        let taken = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let lock_path = lock_path.clone();
                let inside = inside.clone();
                let taken = taken.clone();
                std::thread::spawn(move || {
                    for _ in 0..2000 {
                        if let Ok(_guard) = LockGuard::acquire(lock_path.clone()) {
                            assert_eq!(inside.fetch_add(1, Ordering::SeqCst), 0);
                            taken.fetch_add(1, Ordering::SeqCst);
                            std::thread::yield_now();
                            inside.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("a writer saw the lock held twice");
        }
        // Passing must mean the lock was contended, not that it was never taken.
        assert!(
            taken.load(Ordering::SeqCst) > 0,
            "no writer ever took the lock"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_gives_cells_that_differ_only_in_extension_their_own_locks() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let x = lock_path(&dir.path().join("a.x"));
        let y = lock_path(&dir.path().join("a.y"));
        assert_eq!(x, dir.path().join("a.x.lock"));
        assert_ne!(x, y);
        let _x = LockGuard::acquire(x)?;
        let _y = LockGuard::acquire(y)?;
        Ok(())
    }
}
