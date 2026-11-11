//! Locks on single bytes of the file, far past its data: the writer's
//! turn, and each extent's owner (see the archive's `extent` module).
//!
//! A byte is locked shared or exclusive, waiting for it or only trying.
//! Locks past a file's end are allowed on every platform, and hold no data.
//!
//! On Unix these are POSIX record locks (`fcntl`), which belong to the
//! process, not the handle: two threads of one process never exclude each
//! other with them, and closing any handle of the file releases them. The
//! archive opens a file once per process and shares it (`FileArchive`'s
//! table of open files), closes it only when the last handle goes, and
//! keeps threads apart with its own mutexes, so both hold. On Windows they
//! are `LockFileEx` locks, which belong to the handle.

use std::fs::File;
use std::io;

/// Where the lock bytes start: past any file this archive will grow to.
const BASE: u64 = 1 << 62;

/// The byte of the writer's turn.
pub(crate) const TURN: u64 = BASE;

/// The byte every process holds shared while it has the file open: one
/// that takes it exclusive is the last.
pub(crate) const PRESENT: u64 = BASE - 1;

/// The byte a process holds exclusive while it opens the file: one that
/// repairs it holds it throughout, so no other opens from what it repairs.
pub(crate) const RECOVERY: u64 = BASE - 2;

/// The byte a process holds exclusive while it makes a sync point: the
/// durable slot is written by one at a time, without the writer's turn.
/// It is taken before the turn, never while holding it.
pub(crate) const SYNC: u64 = BASE - 3;

/// The byte an extent's owner holds while the extent is its own, by the
/// extent's record offset.
pub(crate) fn owner(extent: u64) -> u64 {
    BASE + 1 + extent
}

/// Takes the lock on `byte` of `file`, exclusive or shared; waits for it
/// when `wait`, else returns whether it was taken.
pub(crate) fn lock(file: &File, byte: u64, exclusive: bool, wait: bool) -> io::Result<bool> {
    #[cfg(unix)]
    {
        let taken = unix::set(
            file,
            byte,
            if exclusive { Kind::Write } else { Kind::Read },
            wait,
        );
        #[cfg(all(feature = "trace", unix))]
        if let Ok(taken) = &taken {
            crate::trace::lock(file, byte, exclusive, wait, *taken);
        }
        taken
    }
    #[cfg(windows)]
    {
        windows::lock(file, byte, exclusive, wait)
    }
}

/// Releases the lock on `byte` of `file`.
pub(crate) fn unlock(file: &File, byte: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        #[cfg(feature = "trace")]
        crate::trace::unlock(file, byte);
        unix::set(file, byte, Kind::Unlock, true).map(|_| ())
    }
    #[cfg(windows)]
    {
        windows::unlock(file, byte)
    }
}

#[cfg(unix)]
enum Kind {
    Read,
    Write,
    Unlock,
}

#[cfg(unix)]
mod unix {
    use std::fs::File;
    use std::io;
    use std::os::fd::AsRawFd;

    use super::Kind;

    pub(super) fn set(file: &File, byte: u64, kind: Kind, wait: bool) -> io::Result<bool> {
        // SAFETY: a zeroed `flock` is a valid value of the C struct, whose
        // fields are then set; `fcntl` reads it and writes nothing else.
        let mut region: libc::flock = unsafe { std::mem::zeroed() };
        region.l_type = match kind {
            Kind::Read => libc::F_RDLCK,
            Kind::Write => libc::F_WRLCK,
            Kind::Unlock => libc::F_UNLCK,
        } as _;
        region.l_whence = libc::SEEK_SET as _;
        region.l_start = byte as _;
        region.l_len = 1;
        let command = if wait { libc::F_SETLKW } else { libc::F_SETLK };
        loop {
            // SAFETY: the descriptor is the file's, open while `file` is
            // borrowed, and `region` is a valid `flock`.
            let result = unsafe { libc::fcntl(file.as_raw_fd(), command, &region) };
            if result != -1 {
                return Ok(true);
            }
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(libc::EINTR) => continue,
                // The system refuses a wait it takes to close a cycle of
                // processes waiting on one another. The archive's own order
                // closes none (the turn's holder waits on no lock), so the
                // cycle runs through another file's locks, held by another
                // thread of a process waiting here, as POSIX locks belong to
                // the process: no deadlock, as that thread goes on. Wait
                // again shortly.
                Some(libc::EDEADLK) if wait => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    continue;
                }
                Some(libc::EACCES) | Some(libc::EAGAIN) if !wait => return Ok(false),
                _ => return Err(error),
            }
        }
    }
}

#[cfg(windows)]
mod windows {
    use std::fs::File;
    use std::io;
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, ERROR_LOCK_VIOLATION, HANDLE};
    use windows_sys::Win32::Storage::FileSystem::{
        LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx, UnlockFileEx,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    fn at(byte: u64) -> OVERLAPPED {
        // SAFETY: a zeroed OVERLAPPED is valid; only the offset is set.
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        overlapped.Anonymous.Anonymous.Offset = byte as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (byte >> 32) as u32;
        overlapped
    }

    pub(super) fn lock(file: &File, byte: u64, exclusive: bool, wait: bool) -> io::Result<bool> {
        let mut flags = 0;
        if exclusive {
            flags |= LOCKFILE_EXCLUSIVE_LOCK;
        }
        if !wait {
            flags |= LOCKFILE_FAIL_IMMEDIATELY;
        }
        let mut overlapped = at(byte);
        // SAFETY: the handle is the file's, open while `file` is borrowed;
        // the file is opened for synchronous I/O, so the call completes
        // before returning and `overlapped` outlives it.
        let done = unsafe {
            LockFileEx(
                file.as_raw_handle() as HANDLE,
                flags,
                0,
                1,
                0,
                &mut overlapped,
            )
        };
        if done != 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error().map(|code| code as u32) {
            Some(ERROR_LOCK_VIOLATION) | Some(ERROR_IO_PENDING) if !wait => Ok(false),
            _ => Err(error),
        }
    }

    pub(super) fn unlock(file: &File, byte: u64) -> io::Result<()> {
        let mut overlapped = at(byte);
        // SAFETY: as for `lock`.
        let done =
            unsafe { UnlockFileEx(file.as_raw_handle() as HANDLE, 0, 1, 0, &mut overlapped) };
        if done != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Set in a child run of this test: the path whose turn byte the child
    /// holds until its input closes.
    const HOLD: &str = "DIALOG_ARCHIVE_LOCK_HOLD";

    /// What the child prints once it holds the turn. The test harness
    /// prints a test's name before its output on the same line, so the
    /// parent looks for this anywhere in a line.
    const HELD: &str = "DIALOG-ARCHIVE-TURN-HELD";

    /// Another process holding the writer's turn of the file at `path`,
    /// until the returned child's input is closed. The test binary runs
    /// itself as that process.
    pub(crate) fn hold_turn(path: &std::path::Path) -> anyhow::Result<std::process::Child> {
        use std::io::{BufRead, BufReader};
        use std::process::{Command, Stdio};
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "locks::tests::it_reports_a_byte_another_process_holds",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(HOLD, path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut out = BufReader::new(child.stdout.take().expect("piped"));
        let mut line = String::new();
        while !line.contains(HELD) {
            line.clear();
            anyhow::ensure!(
                out.read_line(&mut line)? > 0,
                "the child never held the lock"
            );
        }
        Ok(child)
    }

    /// Another process's lock is what a try reports as taken; a process's
    /// own locks never exclude each other (POSIX), which the archive's
    /// mutexes cover. The test binary runs itself as the other process.
    #[test]
    fn it_reports_a_byte_another_process_holds() -> anyhow::Result<()> {
        use std::io::{BufRead, BufReader, Read, Write};
        use std::process::{Command, Stdio};

        if let Some(path) = std::env::var_os(HOLD) {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)?;
            assert!(lock(&file, TURN, true, true)?);
            println!("{HELD}");
            std::io::stdout().flush()?;
            let mut rest = Vec::new();
            std::io::stdin().read_to_end(&mut rest)?;
            return Ok(());
        }

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("locked");
        std::fs::write(&path, b"")?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;

        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "locks::tests::it_reports_a_byte_another_process_holds",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(HOLD, &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut out = BufReader::new(child.stdout.take().expect("piped"));
        let mut line = String::new();
        while !line.contains(HELD) {
            line.clear();
            anyhow::ensure!(
                out.read_line(&mut line)? > 0,
                "the child never held the lock"
            );
        }

        assert!(
            !lock(&file, TURN, true, false)?,
            "the other process holds it"
        );
        assert!(!lock(&file, TURN, false, false)?, "not even shared");
        assert!(lock(&file, owner(64), true, false)?, "another byte is free");
        unlock(&file, owner(64))?;

        drop(child.stdin.take());
        child.wait()?;
        assert!(lock(&file, TURN, true, false)?, "free once it exits");
        unlock(&file, TURN)?;
        Ok(())
    }
}
