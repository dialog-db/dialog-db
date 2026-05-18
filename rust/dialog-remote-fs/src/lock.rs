//! Cross-process write lock for memory-cell CAS.
//!
//! `dialog-storage::fs::memory` uses `pidlock` for native filesystems, so
//! a native FS-remote consumer (running alongside `slide`) needs to use
//! the same primitive to actually coordinate. The WASM variant will land
//! in a follow-up commit with a `.lock` file + UUIDv7 + heartbeat
//! protocol since browser tabs have no PIDs.
//!
//! The guard is RAII: drop releases the lock. If a stale lock is found
//! (PID no longer alive), `pidlock` cleans it up and we retry once.

use crate::FsError;

/// Held while a CAS critical section is in progress.
///
/// Dropping the guard releases the lock. On native, the lock is a
/// `pidlock`-managed `.lock` file adjacent to the cell.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct LockGuard {
    inner: pidlock::Pidlock,
}

#[cfg(not(target_arch = "wasm32"))]
impl LockGuard {
    /// Acquire a lock for the given cell target. The lock file lives at
    /// `{cell_path}.lock`. Returns immediately if the lock is contended;
    /// the caller's CAS retry loop is responsible for re-attempting.
    pub(crate) fn acquire(target: &crate::handle::Handle) -> Result<Self, FsError> {
        let path = target.path().with_extension("lock");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| FsError::Io(e.to_string()))?;
        }
        // pidlock panics if the lock path is a directory — fail fast.
        if path.exists() && !path.is_file() {
            return Err(FsError::Lock(format!(
                "Lock path is not a regular file: {}",
                path.display()
            )));
        }

        let path_str = path
            .to_str()
            .ok_or_else(|| FsError::Lock("Lock path is not valid UTF-8".into()))?;
        let mut lock = pidlock::Pidlock::new(path_str);
        match lock.acquire() {
            Ok(()) => Ok(Self { inner: lock }),
            Err(pidlock::PidlockError::LockExists) => {
                // `get_owner()` checks whether the PID in the lock file
                // is still alive; if not, it removes the stale file so a
                // retry can succeed.
                match lock.get_owner() {
                    Some(pid) => Err(FsError::Lock(format!(
                        "Concurrent write in progress (lock held by pid {pid})",
                    ))),
                    None => lock
                        .acquire()
                        .map(|()| Self { inner: lock })
                        .map_err(|e| FsError::Lock(format!("Failed to acquire lock: {e:?}"))),
                }
            }
            Err(e) => Err(FsError::Lock(format!("Failed to acquire lock: {e:?}"))),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = self.inner.release();
    }
}
