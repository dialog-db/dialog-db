//! Every concurrent case the file's writers, extents, integration and
//! cells are designed to handle, each pinned by a test that fails if the
//! case goes wrong (design/file-writers.md).
//!
//! Other processes are this test binary run again in a role (the `child`
//! test below, which does nothing in an ordinary run): holding a lock,
//! appending blocks and closing or dying, counting a cell up, creating,
//! swapping or retracting one. Children open the file, say they are
//! ready, and start together on a line from the parent, so their work
//! overlaps as much as it can. Threads share one process's handle. A crash
//! is a process that ends without closing the file
//! (`std::process::exit`), or the archive's `dies` and
//! `stop_before_retire` hooks in this process.
//!
//! What each test checks is what a broken case gets wrong: a block lost
//! or filed twice, a cell's update lost, a value read torn, a writer held
//! up that should not be, an extent written by two writers, a file that
//! does not reopen whole. A child that never says what it should fails
//! the test after [`PROMPTLY`] instead of hanging it.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use dialog_common::{Blake3Hash, Buffer, ConditionalSend};
use dialog_effects::memory::{MemoryError, Version};

use super::{FileArchive, Options, extent};
use crate::locks;

/// The role a child run of this binary plays, its arguments joined by
/// [`SEPARATOR`].
const ROLE: &str = "DIALOG_ARCHIVE_ROLE";
const SEPARATOR: char = '\u{1f}';
/// What a child prints once it is ready, and once it is done.
const READY: &str = "DIALOG-CHILD-READY";
const DONE: &str = "DIALOG-CHILD-DONE";
/// What a child racing for a cell prints when it won, and when it lost.
const WON: &str = "DIALOG-CHILD-WON";
const LOST: &str = "DIALOG-CHILD-LOST";

/// Long enough for anything that does not wait to have finished.
const PROMPTLY: Duration = Duration::from_secs(20);

/// Long enough for anything that is not waiting to have finished, on a
/// test that asserts something is waiting.
const A_WHILE: Duration = Duration::from_millis(500);

fn block(text: &str) -> Buffer {
    Buffer::from(text.as_bytes().to_vec())
}

fn version(content: &[u8]) -> Version {
    Version::from(Blake3Hash::hash(content).as_bytes())
}

fn say(marker: &str) {
    println!("{marker}");
    let _ = std::io::stdout().flush();
}

fn small(extent: u64) -> Options {
    Options {
        extent,
        ..Options::default()
    }
}

fn archive_in(directory: &tempfile::TempDir) -> PathBuf {
    directory.path().join("archive")
}

fn names(prefix: &str, count: usize) -> Vec<String> {
    (0..count).map(|n| format!("{prefix}-{n}")).collect()
}

/// A child run of this binary in a role: its input, and the lines it
/// prints, read on a thread of their own so a wait on them can end.
pub(super) struct Process {
    child: Child,
    lines: mpsc::Receiver<String>,
    input: Option<ChildStdin>,
}

impl Process {
    fn spawn(role: &[&str]) -> anyhow::Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "archive::concurrency::child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(ROLE, role.join(&SEPARATOR.to_string()))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let out = BufReader::new(child.stdout.take().expect("piped"));
        let err = BufReader::new(child.stderr.take().expect("piped"));
        let (send, lines) = mpsc::channel();
        // What it says on its error output comes too, marked, so a child
        // that fails says why in the parent's failure.
        let errors = send.clone();
        std::thread::spawn(move || {
            for line in err.lines() {
                let Ok(line) = line else {
                    break;
                };
                if errors.send(format!("stderr: {line}")).is_err() {
                    break;
                }
            }
        });
        std::thread::spawn(move || {
            for line in out.lines() {
                let Ok(line) = line else {
                    break;
                };
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        let input = child.stdin.take();
        Ok(Self {
            child,
            lines,
            input,
        })
    }

    /// Spawns the child in `role` and waits until it is ready.
    fn ready(role: &[&str]) -> anyhow::Result<Self> {
        let mut process = Self::spawn(role)?;
        process.expect(READY)?;
        Ok(process)
    }

    /// The lines the child prints up to the first holding `marker` (the
    /// harness prints a test's name ahead of its output on one line), if
    /// it prints one within `within`.
    fn said_within(
        &mut self,
        marker: &str,
        within: Duration,
    ) -> anyhow::Result<Option<Vec<String>>> {
        let deadline = std::time::Instant::now() + within;
        let mut said = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            match self.lines.recv_timeout(left) {
                Ok(line) => {
                    let found = line.contains(marker);
                    said.push(line);
                    if found {
                        return Ok(Some(said));
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => return Ok(None),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    anyhow::bail!("the child ended before saying {marker}: {said:?}")
                }
            }
        }
    }

    /// The lines the child prints up to the first holding `marker`; an
    /// error if it does not say it [promptly](PROMPTLY).
    fn expect(&mut self, marker: &str) -> anyhow::Result<Vec<String>> {
        self.said_within(marker, PROMPTLY)?
            .ok_or_else(|| anyhow::anyhow!("the child did not say {marker} in {PROMPTLY:?}"))
    }

    /// Whether the child said it won the race it ran.
    fn won(&mut self) -> anyhow::Result<bool> {
        let said = self.expect(DONE)?;
        let won = said.iter().any(|line| line.contains(WON));
        let lost = said.iter().any(|line| line.contains(LOST));
        anyhow::ensure!(won != lost, "the child said neither or both: {said:?}");
        Ok(won)
    }

    /// Has a reader child read `names` back, each as itself.
    fn read(&mut self, names: &[String]) -> anyhow::Result<()> {
        let input = self.input.as_mut().expect("the child's input is open");
        writeln!(input, "{}", names.join(&SEPARATOR.to_string()))?;
        input.flush()?;
        self.expect(DONE)?;
        Ok(())
    }

    fn go(&mut self) -> anyhow::Result<()> {
        let input = self.input.as_mut().expect("the child's input is open");
        input.write_all(b"go\n")?;
        input.flush()?;
        Ok(())
    }

    /// Closes the child's input, which ends a child that waits on it, and
    /// waits for it to exit well.
    pub(super) fn finish(mut self) -> anyhow::Result<()> {
        drop(self.input.take());
        let status = self.child.wait()?;
        anyhow::ensure!(status.success(), "the child failed: {status}");
        Ok(())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        drop(self.input.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts every child at once.
fn go(processes: &mut [Process]) -> anyhow::Result<()> {
    for process in processes {
        process.go()?;
    }
    Ok(())
}

/// A process holding `byte` of the file at `path`, shared or exclusive,
/// until it is finished.
pub(super) fn hold(path: &Path, byte: u64, exclusive: bool) -> anyhow::Result<Process> {
    Process::ready(&[
        "hold",
        &path.to_string_lossy(),
        &byte.to_string(),
        if exclusive { "1" } else { "0" },
    ])
}

/// A process that appends `count` blocks named from `prefix` to the file
/// at `path`, with extents of `extent` bytes, once told to go; then closes
/// the file, or dies without closing it.
fn appender(
    path: &Path,
    prefix: &str,
    count: usize,
    extent: u64,
    dies: bool,
) -> anyhow::Result<Process> {
    Process::ready(&[
        "append",
        &path.to_string_lossy(),
        prefix,
        &count.to_string(),
        &extent.to_string(),
        if dies { "die" } else { "close" },
    ])
}

/// Runs `work` on another thread: its result if it finishes within
/// `within`, else `None`, with the channel its result comes on when it
/// does finish.
fn within<T: ConditionalSend + 'static>(
    within: Duration,
    work: impl FnOnce() -> T + ConditionalSend + 'static,
) -> (Option<T>, mpsc::Receiver<T>) {
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = send.send(work());
    });
    match receive.recv_timeout(within) {
        Ok(done) => (Some(done), receive),
        Err(_) => (None, receive),
    }
}

/// Every block named in `names` reads back from `archive` as itself, and
/// the index files none of them twice.
fn all_read_back(archive: &FileArchive, names: &[String]) -> anyhow::Result<()> {
    let mut filed: HashMap<Blake3Hash, usize> = HashMap::new();
    for (_, hash, _) in archive.current().blocks()? {
        *filed.entry(hash).or_default() += 1;
    }
    for name in names {
        let block = block(name);
        assert_eq!(
            archive.get("index", block.blake3_hash())?.as_deref(),
            Some(block.as_ref()),
            "{name} reads back"
        );
        if let Some(times) = filed.get(block.blake3_hash()) {
            assert_eq!(*times, 1, "{name} is filed once in the index");
        }
    }
    Ok(())
}

/// The extents the newest slot lists that hold blocks.
fn holding(archive: &FileArchive) -> Vec<u64> {
    let snapshot = archive.current();
    snapshot
        .slot
        .extents
        .iter()
        .copied()
        .filter(|&record| extent::used(snapshot.bytes(), record).is_some_and(|used| used > 0))
        .collect()
}

/// The count a counting cell holds: its value's first word. The rest names
/// who wrote it, so no two increments write the same value. A cell's
/// edition is its content's hash, and publishing what a cell already
/// holds succeeds whatever edition the writer expected, as on every
/// memory provider: equal values would hide a lost update.
fn count_of(value: &[u8]) -> anyhow::Result<u64> {
    let text = std::str::from_utf8(value)?;
    Ok(text.split(' ').next().unwrap_or_default().parse()?)
}

/// Counts the cell up `times`, each a compare-and-swap retried until it
/// lands.
fn count_up(archive: &FileArchive, cell: &str, who: &str, times: usize) -> anyhow::Result<()> {
    for _ in 0..times {
        loop {
            let current = archive.resolve_cell("local", cell)?;
            let count = match &current {
                Some(value) => count_of(value)?,
                None => 0,
            };
            let when = current.as_deref().map(version);
            match archive.publish_cell(
                "local",
                cell,
                format!("{} {who}", count + 1).as_bytes(),
                when.as_ref(),
            ) {
                Ok(_) => break,
                Err(MemoryError::VersionMismatch { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

/// Says whether a race for a cell was won, or fails on any error but
/// losing it.
fn race<T>(outcome: Result<T, MemoryError>) -> anyhow::Result<()> {
    match outcome {
        Ok(_) => say(WON),
        Err(MemoryError::VersionMismatch { .. }) => say(LOST),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

/// A child's part, when this binary runs as one; nothing in an ordinary
/// run.
#[dialog_common::test]
fn child() -> anyhow::Result<()> {
    let Ok(role) = std::env::var(ROLE) else {
        return Ok(());
    };
    let args: Vec<&str> = role.split(SEPARATOR).collect();
    let go = || -> anyhow::Result<()> {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        Ok(())
    };
    match args[0] {
        // hold <path> <byte> <exclusive>: until the input closes.
        "hold" => {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(args[1])?;
            let byte = args[2].parse()?;
            assert!(locks::lock(&file, byte, args[3] == "1", true)?);
            say(READY);
            let mut rest = Vec::new();
            std::io::stdin().read_to_end(&mut rest)?;
            // Let go before exiting: Windows lets go of a closed handle's
            // or an ended process's locks only when it gets to it, so a
            // test that goes on once this exits could find the byte still
            // held.
            locks::unlock(&file, byte)?;
        }
        // append <path> <prefix> <count> <extent> <close|die>
        "append" => {
            let archive = FileArchive::open_with(args[1], small(args[4].parse()?))?;
            say(READY);
            go()?;
            for name in names(args[2], args[3].parse()?) {
                archive.append("index", &[block(&name)])?;
            }
            say(DONE);
            if args[5] == "die" {
                // Ends without closing the file, as a crash would.
                std::process::exit(0);
            }
            drop(archive);
        }
        // open <path>: holds the file open until the input closes.
        "open" => {
            let archive = FileArchive::open(args[1])?;
            say(READY);
            let mut rest = Vec::new();
            std::io::stdin().read_to_end(&mut rest)?;
            drop(archive);
        }
        // commits <path> <cell> <times>: each time 20 blocks of 4 KiB, then
        // the cell; says how long publishing the cell took, in microseconds.
        "commits" => {
            let archive = FileArchive::open(args[1])?;
            say(READY);
            go()?;
            let mut took = Vec::new();
            let mut waited = Vec::new();
            let mut current: Option<Version> = None;
            for commit in 0..args[3].parse::<usize>()? {
                let blocks: Vec<Buffer> = (0..20)
                    .map(|n| {
                        let mut bytes = format!("{} {commit} {n} ", args[2]).into_bytes();
                        bytes.resize(4096, b'.');
                        Buffer::from(bytes)
                    })
                    .collect();
                archive.append("index", &blocks)?;
                let started = std::time::Instant::now();
                current = Some(archive.publish_cell(
                    "local",
                    args[2],
                    format!("{commit}").as_bytes(),
                    current.as_ref(),
                )?);
                took.push(started.elapsed().as_micros() as u64);
                waited.push(archive.inner.turn_waited.load(Ordering::Relaxed));
            }
            let quantiles = |mut values: Vec<u64>| {
                values.sort_unstable();
                let at = |q: f64| values[((values.len() - 1) as f64 * q) as usize];
                let max = values[values.len() - 1];
                format!("{} {} {} {max}", at(0.5), at(0.9), at(0.99))
            };
            say(&format!("LATENCY {}", quantiles(took)));
            say(&format!("WAITED {}", quantiles(waited)));
            say(DONE);
        }
        // count <path> <cell> <times>
        "count" => {
            let archive = FileArchive::open(args[1])?;
            say(READY);
            go()?;
            let who = format!("process {}", std::process::id());
            count_up(&archive, args[2], &who, args[3].parse()?)?;
            say(DONE);
        }
        // create <path> <cell> <value>
        "create" => {
            let archive = FileArchive::open(args[1])?;
            say(READY);
            go()?;
            race(archive.publish_cell("local", args[2], args[3].as_bytes(), None))?;
            say(DONE);
        }
        // swap <path> <cell> <expected> <value>
        "swap" => {
            let archive = FileArchive::open(args[1])?;
            say(READY);
            go()?;
            let when = version(args[3].as_bytes());
            race(archive.publish_cell("local", args[2], args[4].as_bytes(), Some(&when)))?;
            say(DONE);
        }
        // retract <path> <cell> <expected>
        "retract" => {
            let archive = FileArchive::open(args[1])?;
            say(READY);
            go()?;
            let when = version(args[3].as_bytes());
            race(archive.retract_cell("local", args[2], &when))?;
            say(DONE);
        }
        // try <path> <byte>: whether it takes the byte exclusive at once.
        "try" => {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(args[1])?;
            let byte = args[2].parse()?;
            let won = locks::lock(&file, byte, true, false)?;
            say(if won { WON } else { LOST });
            if won {
                // Let go before exiting, as `hold` does.
                locks::unlock(&file, byte)?;
            }
            say(DONE);
        }
        // read <path>: then, for each line of names separated by the
        // separator, reads them all back and says so.
        "read" => {
            let archive = FileArchive::open(args[1])?;
            say(READY);
            for line in std::io::stdin().lines() {
                let names: Vec<String> = line?.split(SEPARATOR).map(String::from).collect();
                all_read_back(&archive, &names)?;
                say(DONE);
            }
        }
        // write <path> <cell> <values...>: publishes each over the last.
        "write" => {
            let archive = FileArchive::open(args[1])?;
            say(READY);
            go()?;
            for value in &args[3..] {
                let current = archive.resolve_cell("local", args[2])?;
                let when = current.as_deref().map(version);
                archive.publish_cell("local", args[2], value.as_bytes(), when.as_ref())?;
            }
            say(DONE);
        }
        other => anyhow::bail!("no role {other}"),
    }
    Ok(())
}

/// A process writing alone writes as it always did, in the turn: nothing
/// goes into an extent. It keeps one extent for its own writers and one
/// for a process that has none, both empty.
#[dialog_common::test]
fn a_lone_writer_writes_nothing_into_extents() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let archive = FileArchive::open(archive_in(&directory))?;
    let names = names("alone", 50);
    for name in &names {
        archive.append("index", &[block(name)])?;
    }
    let snapshot = archive.current();
    assert_eq!(snapshot.slot.extents.len(), 2, "a spare and one to adopt");
    for &record in &snapshot.slot.extents {
        assert_eq!(
            extent::used(snapshot.bytes(), record),
            Some(0),
            "both empty"
        );
    }
    drop(snapshot);
    all_read_back(&archive, &names)
}

/// Processes appending to one file at once all get through, and every
/// block of every one reads back, is filed once, and is there after
/// reopening. Small extents make them fill extents and claim more.
#[dialog_common::test]
fn processes_append_to_one_file_at_once() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    FileArchive::open(&path)?.append("index", &[block("first")])?;
    let mut writers = (0..4)
        .map(|writer| appender(&path, &format!("writer {writer}"), 40, 4096, false))
        .collect::<anyhow::Result<Vec<_>>>()?;
    go(&mut writers)?;
    for mut writer in writers {
        writer.expect(DONE)?;
        writer.finish()?;
    }
    let mut all: Vec<String> = (0..4)
        .flat_map(|writer| names(&format!("writer {writer}"), 40))
        .collect();
    all.push("first".into());
    let archive = FileArchive::open(&path)?;
    all_read_back(&archive, &all)?;
    drop(archive);
    all_read_back(&FileArchive::open(&path)?, &all)
}

/// Threads of one process appending at once all get through: the ones
/// that find the turn taken write into the process's extents, never two
/// into one at once (two would write over each other's blocks).
#[dialog_common::test]
fn threads_append_to_one_file_at_once() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open_with(&path, small(4096))?;
    archive.append("index", &[block("first")])?;
    let threads: Vec<_> = (0..8)
        .map(|thread| {
            let archive = archive.clone();
            std::thread::spawn(move || -> anyhow::Result<()> {
                for name in names(&format!("thread {thread}"), 40) {
                    archive.append("index", &[block(&name)])?;
                }
                Ok(())
            })
        })
        .collect();
    for thread in threads {
        thread.join().expect("a writer thread")?;
    }
    let all: Vec<String> = (0..8)
        .flat_map(|thread| names(&format!("thread {thread}"), 40))
        .collect();
    all_read_back(&archive, &all)?;
    drop(archive);
    all_read_back(&FileArchive::open(&path)?, &all)
}

/// A file reached by two names in one process (a hard link here) is one
/// handle: two would both hold the writer's turn, as the turn's lock
/// belongs to the process, and closing either would let go of the other's
/// locks.
#[dialog_common::test]
fn a_file_reached_by_two_names_is_one_handle() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    let other = path.with_extension("other");
    std::fs::hard_link(&path, &other)?;
    let again = FileArchive::open(&other)?;
    assert!(std::sync::Arc::ptr_eq(&archive.inner, &again.inner));
    drop(again);
    // Still open here: another process does not find itself the last.
    let present = locks::PRESENT.to_string();
    let mut probe = Process::spawn(&["try", &path.to_string_lossy(), &present])?;
    assert!(!probe.won()?, "this process still holds the file present");
    drop(archive);
    let mut probe = Process::spawn(&["try", &path.to_string_lossy(), &present])?;
    assert!(probe.won()?, "and lets go once it closes");
    Ok(())
}

/// A process that has never held the turn writes without waiting for it:
/// it takes the extent a turn holder left for one, while another process
/// holds the turn throughout. Another process reads its blocks from the
/// extent, and its close folds them in once the turn is free.
#[dialog_common::test]
fn a_process_that_never_held_the_turn_writes_without_waiting() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    let holder = hold(&path, locks::TURN, true)?;
    let mut writer = appender(&path, "fresh", 5, 1 << 20, false)?;
    writer.go()?;
    writer.expect(DONE)?;
    // Its close waits for the turn, to fold its extent in: it is still
    // open, and its blocks are still in the extent.
    let fresh = names("fresh", 5);
    archive.refresh()?;
    assert_eq!(holding(&archive).len(), 1, "they are in one extent");
    all_read_back(&archive, &fresh)?;
    holder.finish()?;
    writer.finish()?;
    archive.refresh()?;
    assert!(holding(&archive).is_empty(), "folded in at its close");
    all_read_back(&archive, &fresh)
}

/// Processes that have never held the turn, arriving together while
/// another holds it throughout, all write without waiting: every process
/// that opened the file with the turn free left an extent for one.
#[dialog_common::test]
fn processes_arriving_together_while_the_turn_is_held_do_not_wait() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    let path_text = path.to_string_lossy().into_owned();
    let open = (0..3)
        .map(|_| Process::ready(&["open", &path_text]))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let holder = hold(&path, locks::TURN, true)?;
    let mut writers = (0..4)
        .map(|writer| appender(&path, &format!("arriving {writer}"), 5, 1 << 20, false))
        .collect::<anyhow::Result<Vec<_>>>()?;
    go(&mut writers)?;
    for writer in &mut writers {
        writer.expect(DONE)?;
    }
    holder.finish()?;
    for writer in writers {
        writer.finish()?;
    }
    for process in open {
        process.finish()?;
    }
    let all: Vec<String> = (0..4)
        .flat_map(|writer| names(&format!("arriving {writer}"), 5))
        .collect();
    archive.refresh()?;
    all_read_back(&archive, &all)
}

/// The extents processes leave as they open are taken back as they close:
/// processes opening and closing the file one after another, while
/// another keeps it open, leave no more of them than were there.
#[dialog_common::test]
fn extents_left_for_others_do_not_pile_up() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    let path_text = path.to_string_lossy().into_owned();
    let empty = |archive: &FileArchive| -> anyhow::Result<usize> {
        archive.refresh()?;
        let snapshot = archive.current();
        Ok(snapshot
            .slot
            .extents
            .iter()
            .filter(|&&record| extent::used(snapshot.bytes(), record) == Some(0))
            .count())
    };
    let before = empty(&archive)?;
    for _ in 0..10 {
        Process::ready(&["open", &path_text])?.finish()?;
    }
    // A block appended makes the file grow, so this process reads the
    // slots again.
    archive.append("index", &[block("last")])?;
    assert!(
        empty(&archive)? <= before,
        "{} empty extents after ten opens and closes, {before} before",
        empty(&archive)?
    );
    Ok(())
}

/// A writer that finds the turn held by another process does not wait,
/// and a reader in another process reads its blocks from the extent before
/// they are folded in.
#[dialog_common::test]
fn another_process_reads_blocks_still_in_an_extent() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    let holder = hold(&path, locks::TURN, true)?;
    let loose = names("loose", 5);
    let (done, _) = within(PROMPTLY, {
        let archive = archive.clone();
        let loose = loose.clone();
        move || -> anyhow::Result<()> {
            for name in &loose {
                archive.append("index", &[block(name)])?;
            }
            Ok(())
        }
    });
    done.expect("the writer did not wait for the turn")?;
    assert_eq!(holding(&archive).len(), 1, "the blocks are in an extent");
    let mut reader = Process::ready(&["read", &path.to_string_lossy()])?;
    reader.read(&loose)?;
    assert_eq!(holding(&archive).len(), 1, "read before they are folded in");
    holder.finish()?;
    reader.finish()
}

/// A block another process holds only in its extent is written again
/// into this process's own: a head this process commits naming it is
/// appended after its own extents are folded in, and another's are folded
/// only when that one commits, so the head could otherwise reach the file
/// before the block.
#[dialog_common::test]
fn a_block_only_another_process_holds_loose_is_written_again() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    let holder = hold(&path, locks::TURN, true)?;
    let mut other = appender(&path, "shared", 1, 1 << 20, false)?;
    other.go()?;
    other.expect(DONE)?;
    archive.refresh()?;
    let theirs = holding(&archive);
    assert_eq!(theirs.len(), 1, "the other process holds it loose");
    archive.append("index", &[block("shared-0")])?;
    let held = holding(&archive);
    assert_eq!(held.len(), 2, "and this process wrote its own copy");
    let ours = archive
        .inner
        .extents
        .lock()
        .idle
        .iter()
        .map(|owned| owned.record)
        .collect::<Vec<_>>();
    assert!(
        held.iter()
            .any(|record| ours.contains(record) && !theirs.contains(record))
    );
    holder.finish()?;
    other.finish()?;
    all_read_back(&archive, &["shared-0".into()])
}

/// A sync point falling due as a writer writes without the turn, because
/// another process holds it, is made without waiting for the turn either:
/// what it frees is taken up by the next commit in the turn.
#[dialog_common::test]
fn a_sync_point_due_while_another_holds_the_turn_does_not_wait() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    let holder = hold(&path, locks::TURN, true)?;
    *archive.inner.synced.lock() = std::time::Instant::now() - std::time::Duration::from_secs(60);
    archive.inner.sync_due.store(true, Ordering::Relaxed);
    let (done, _) = within(PROMPTLY, {
        let archive = archive.clone();
        move || archive.append("index", &[block("while held")])
    });
    done.expect("the append and its sync point did not wait for the turn")?;
    let durable = super::slot::Slot::read_durable(&archive.inner.file)?.expect("a durable slot");
    assert!(
        durable.sequence > 0 && durable.sequence <= archive.current().slot.sequence,
        "the sync point was made"
    );
    holder.finish()?;
    archive.append("index", &[block("after")])?;
    assert_eq!(
        archive.current().slot.durable,
        durable.sequence,
        "the next commit took it up"
    );
    all_read_back(
        &archive,
        &["first".into(), "while held".into(), "after".into()],
    )
}

/// A writer that died with blocks in an extent leaves them readable, and
/// the next writer to take the turn folds them in, filing each once.
#[dialog_common::test]
fn a_dead_writers_extent_is_folded_in_by_the_next() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    let holder = hold(&path, locks::TURN, true)?;
    let mut dying = appender(&path, "dying", 5, 1 << 20, true)?;
    dying.go()?;
    dying.expect(DONE)?;
    dying.finish()?;
    holder.finish()?;

    let dead = names("dying", 5);
    archive.refresh()?;
    let left = holding(&archive);
    assert_eq!(left.len(), 1, "its extent is still listed");
    all_read_back(&archive, &dead)?;
    archive.append("index", &[block("next")])?;
    assert!(holding(&archive).is_empty(), "folded in");
    for record in &left {
        assert!(
            !archive.current().slot.extents.contains(record),
            "and retired"
        );
    }
    all_read_back(&archive, &dead)?;
    drop(archive);
    all_read_back(&FileArchive::open(&path)?, &dead)
}

/// A process that takes a dead writer's extent for its own writes still
/// reads the blocks the dead writer left in it, and so does every other
/// process: what it writes follows them, and they are folded in with it.
#[dialog_common::test]
fn a_dead_writers_extent_taken_over_keeps_its_blocks() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    // Another process leaves one extent to take, and the one it writes in
    // its turn.
    let mut setup = appender(&path, "setup", 1, 1 << 20, false)?;
    // This process is open throughout, so no close is the last.
    let archive = FileArchive::open(&path)?;
    setup.go()?;
    setup.expect(DONE)?;
    setup.finish()?;
    // A writer takes the one left, writes, and dies.
    let holder = hold(&path, locks::TURN, true)?;
    let mut dying = appender(&path, "dying", 5, 1 << 20, true)?;
    dying.go()?;
    dying.expect(DONE)?;
    dying.finish()?;
    // This process has no extent: it takes the dead writer's, without the
    // turn, and writes after its blocks.
    archive.refresh()?;
    let dead = holding(&archive);
    assert_eq!(dead.len(), 1);
    archive.append("index", &[block("mine")])?;
    assert_eq!(
        holding(&archive),
        dead,
        "written into the dead writer's extent"
    );
    let mut all = names("dying", 5);
    all.push("mine".into());
    all_read_back(&archive, &all)?;
    let mut reader = Process::ready(&["read", &path.to_string_lossy()])?;
    reader.read(&all)?;
    holder.finish()?;
    archive.append("index", &[block("folds")])?;
    assert!(holding(&archive).is_empty());
    all_read_back(&archive, &all)?;
    reader.read(&all)?;
    reader.finish()
}

/// A process never takes an extent its snapshot lists after an
/// integration retired it: the integration let go of the extent only
/// after writing the slot that retires it, and a process that takes an
/// extent checks that the newest slot still lists it.
#[dialog_common::test]
fn an_extent_retired_under_a_stale_snapshot_is_not_taken() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    // Another process leaves an extent to take; a third takes it with the
    // turn held, and dies with blocks in it.
    let mut setup = appender(&path, "setup", 1, 1 << 20, false)?;
    setup.go()?;
    setup.expect(DONE)?;
    setup.finish()?;
    let holder = hold(&path, locks::TURN, true)?;
    let mut dying = appender(&path, "dying", 2, 1 << 20, true)?;
    dying.go()?;
    dying.expect(DONE)?;
    dying.finish()?;
    holder.finish()?;
    archive.refresh()?;
    let retired = holding(&archive);
    assert_eq!(retired.len(), 1);
    // A fourth folds it in and retires it; this process's snapshot still
    // lists it.
    let mut folding = appender(&path, "folding", 1, 1 << 20, false)?;
    folding.go()?;
    folding.expect(DONE)?;
    folding.finish()?;
    assert!(archive.current().slot.extents.contains(&retired[0]));
    assert!(
        !super::newest(&archive.inner.file, false)?
            .extents
            .contains(&retired[0])
    );

    let taken = archive.adopt()?;
    assert!(
        taken
            .as_ref()
            .is_none_or(|owned| owned.record != retired[0]),
        "the retired extent is not taken"
    );
    assert!(
        !archive.inner.extents.lock().busy.contains(&retired[0]),
        "nor kept busy"
    );
    Ok(())
}

/// A crash between appending an extent's blocks as frames and retiring
/// the extent leaves both: reads stay right, and the next integration
/// retires it without filing anything twice.
#[dialog_common::test]
fn a_crash_between_folding_and_retiring_is_finished_by_the_next() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    let holder = hold(&path, locks::TURN, true)?;
    let loose = names("loose", 5);
    for name in &loose {
        archive.append("index", &[block(name)])?;
    }
    holder.finish()?;
    let listed = holding(&archive);
    assert_eq!(listed.len(), 1);
    archive
        .inner
        .stop_before_retire
        .store(true, Ordering::Relaxed);
    assert!(
        archive.append("index", &[block("stops")]).is_err(),
        "the integration stops before retiring"
    );
    archive.inner.dies.store(true, Ordering::Relaxed);
    drop(archive);

    let archive = FileArchive::open(&path)?;
    assert_eq!(holding(&archive), listed, "the extent is still listed");
    all_read_back(&archive, &loose)?;
    archive.append("index", &[block("after")])?;
    assert!(holding(&archive).is_empty(), "retired by the next");
    all_read_back(&archive, &loose)?;
    drop(archive);
    all_read_back(&FileArchive::open(&path)?, &loose)
}

/// An extent integrated is claimed again only after a sync point, and a
/// reader that read blocks from it before it was claimed again still
/// reads every block right after: its old ones from the index, the new
/// ones from the extent.
#[dialog_common::test]
fn an_extent_is_reused_only_after_a_sync_point_and_stale_readers_stay_right() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    let mut reader = Process::ready(&["read", &path.to_string_lossy()])?;

    let holder = hold(&path, locks::TURN, true)?;
    let before = names("before", 3);
    for name in &before {
        archive.append("index", &[block(name)])?;
    }
    holder.finish()?;
    let extent = holding(&archive)[0];
    // The reader reads them from the extent, and keeps where they were.
    reader.read(&before)?;

    // Folded in: retired, not free until a sync point, and not claimed
    // again before one.
    archive.append("index", &[block("folds")])?;
    let slot = archive.current().slot.clone();
    assert!(slot.extents_pending.contains(&extent));
    assert!(!slot.extents_free.contains(&extent));
    assert!(!slot.extents.contains(&extent), "not claimed again yet");
    archive.sync()?;
    assert!(archive.current().slot.extents_free.contains(&extent));

    // Claimed again once free, first in line for this process's writers.
    {
        let turn = archive.turn()?;
        let claimed = archive.claim_in_turn(&turn)?;
        assert_eq!(claimed.record, extent, "the freed extent is claimed again");
        let mut extents = archive.inner.extents.lock();
        extents.busy.remove(&claimed.record);
        extents.idle.push(claimed);
    }
    // Longer than what the reader read of it before: a reader that took
    // the extent for the one it read would look past them.
    let holder = hold(&path, locks::TURN, true)?;
    let after = names(&"after".repeat(20), 3);
    for name in &after {
        archive.append("index", &[block(name)])?;
    }
    holder.finish()?;
    assert!(holding(&archive).contains(&extent), "written with others");

    // The reader's map names the old blocks in the reused extent: the
    // bytes there fail their hash, and it finds them in the index.
    reader.read(&before)?;
    reader.read(&after)?;
    reader.finish()
}

/// A frame in an extent not whole is not read; the frames before it are.
/// One whose bytes arrive later (read before they all had) is read then,
/// from where the walk stopped; one cut short by a crash never is, and the
/// rest are folded in.
#[dialog_common::test]
fn a_torn_frame_in_an_extent_ends_what_is_read_of_it() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    let holder = hold(&path, locks::TURN, true)?;
    let kept = names("kept", 3);
    for name in &kept {
        archive.append("index", &[block(name)])?;
    }
    archive.append("index", &[block("torn")])?;
    holder.finish()?;
    let record = holding(&archive)[0];
    let last = {
        let snapshot = archive.current();
        let used = extent::used(snapshot.bytes(), record).expect("a whole count");
        let (held, _) = extent::held(snapshot.bytes(), record, 0, used)?;
        assert_eq!(held.len(), 4);
        held.last().expect("a frame").at as u64 * crate::tokens::ALIGN
    };
    archive.inner.dies.store(true, Ordering::Relaxed);
    drop(archive);

    // The last frame's bytes have not all arrived.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let mut arrived = [0u8; 4];
    crate::file::read_at(&file, last + 8, &mut arrived)?;
    crate::file::write_at(&file, last + 8, &[0xEE; 4])?;

    let archive = FileArchive::open(&path)?;
    all_read_back(&archive, &kept)?;
    assert_eq!(archive.get("index", block("torn").blake3_hash())?, None);
    // They arrive: the frame is read now.
    crate::file::write_at(&file, last + 8, &arrived)?;
    all_read_back(&archive, &["torn".into()])?;
    // They never do: a crash cut it short.
    crate::file::write_at(&file, last + 8, &[0xEE; 4])?;
    drop(file);
    archive.inner.dies.store(true, Ordering::Relaxed);
    drop(archive);
    let archive = FileArchive::open(&path)?;
    all_read_back(&archive, &kept)?;
    assert_eq!(archive.get("index", block("torn").blake3_hash())?, None);
    archive.append("index", &[block("next")])?;
    assert!(holding(&archive).is_empty());
    all_read_back(&archive, &kept)?;
    assert_eq!(archive.get("index", block("torn").blake3_hash())?, None);
    Ok(())
}

/// A writer that needs a catalog the file has never numbered waits for
/// the turn: it cannot number one without it.
#[dialog_common::test]
fn a_new_catalog_waits_for_the_turn() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    let holder = hold(&path, locks::TURN, true)?;
    let (done, waiting) = within(A_WHILE, {
        let archive = archive.clone();
        move || archive.append("another", &[block("in another catalog")])
    });
    assert!(done.is_none(), "it waits for the turn");
    holder.finish()?;
    waiting.recv_timeout(PROMPTLY)??;
    assert!(
        archive
            .get("another", block("in another catalog").blake3_hash())?
            .is_some()
    );
    Ok(())
}

/// A writer whose extents are full, and that finds no other to take,
/// claims one, which takes the turn: it waits for it, then writes, and
/// nothing is lost.
#[dialog_common::test]
fn a_full_extent_waits_for_the_turn_to_claim_another() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open_with(&path, small(512))?;
    archive.append("index", &[block("first")])?;
    let holder = hold(&path, locks::TURN, true)?;
    let many = names(&"x".repeat(100), 12);
    let (done, waiting) = within(A_WHILE, {
        let archive = archive.clone();
        let many = many.clone();
        move || -> anyhow::Result<()> {
            for name in &many {
                archive.append("index", &[block(name)])?;
            }
            Ok(())
        }
    });
    assert!(
        done.is_none(),
        "the extents filled, and claiming another waits"
    );
    holder.finish()?;
    waiting.recv_timeout(PROMPTLY)??;
    all_read_back(&archive, &many)
}

/// The file is cut at a close only by the last process to close it: one
/// that closes while another has it open leaves its length alone.
#[dialog_common::test]
fn only_the_last_process_to_close_cuts_the_file() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let present = {
        let archive = FileArchive::open(&path)?;
        archive.append("index", &[block("first")])?;
        let present = hold(&path, locks::PRESENT, false)?;
        let open = std::fs::metadata(&path)?.len();
        drop(archive);
        assert_eq!(
            std::fs::metadata(&path)?.len(),
            open,
            "another process has it open: not cut"
        );
        present
    };
    present.finish()?;
    let before = std::fs::metadata(&path)?.len();
    drop(FileArchive::open(&path)?);
    assert!(
        std::fs::metadata(&path)?.len() < before,
        "the last close cuts it"
    );
    let reopened = FileArchive::open_with(
        &path,
        Options {
            leave_extent: false,
            ..Options::default()
        },
    )?;
    assert!(
        reopened.current().slot.extents.is_empty(),
        "every extent retired"
    );
    all_read_back(&reopened, &["first".into()])
}

/// Processes creating one cell at once: exactly one creates it, the
/// others are told it is taken, and it holds the one's value.
#[dialog_common::test]
fn one_of_many_creating_a_cell_wins() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    FileArchive::open(&path)?.append("index", &[block("first")])?;
    let mut creators = (0..4)
        .map(|creator| {
            Process::ready(&[
                "create",
                &path.to_string_lossy(),
                "main",
                &format!("creator {creator}"),
            ])
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    go(&mut creators)?;
    let mut won = Vec::new();
    for (at, creator) in creators.iter_mut().enumerate() {
        if creator.won()? {
            won.push(at);
        }
    }
    drop(creators);
    assert_eq!(won.len(), 1, "exactly one creates it: {won:?}");
    let archive = FileArchive::open(&path)?;
    assert_eq!(
        archive.resolve_cell("local", "main")?.as_deref(),
        Some(format!("creator {}", won[0]).as_bytes())
    );
    Ok(())
}

/// Processes and threads counting one cell up with compare-and-swap lose
/// no update: the count is every increment.
#[dialog_common::test]
fn counting_one_cell_up_from_many_writers_loses_no_update() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.publish_cell("local", "count", b"0 start", None)?;
    let mut counters = (0..3)
        .map(|_| Process::ready(&["count", &path.to_string_lossy(), "count", "30"]))
        .collect::<anyhow::Result<Vec<_>>>()?;
    go(&mut counters)?;
    let threads: Vec<_> = (0..3)
        .map(|thread| {
            let archive = archive.clone();
            std::thread::spawn(move || count_up(&archive, "count", &format!("thread {thread}"), 30))
        })
        .collect();
    for thread in threads {
        thread.join().expect("a counting thread")?;
    }
    for mut counter in counters {
        counter.expect(DONE)?;
        counter.finish()?;
    }
    assert_eq!(
        archive
            .resolve_cell("local", "count")?
            .map(|value| count_of(&value))
            .transpose()?,
        Some(180),
        "3 processes and 3 threads, 30 increments each"
    );
    Ok(())
}

/// A swap and a retraction racing from the same edition: exactly one
/// lands, and the cell holds what it left, in every round.
#[dialog_common::test]
fn a_swap_and_a_retraction_racing_land_one() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    let path = path.to_string_lossy();
    for round in 0..8 {
        let base = format!("base {round}");
        let next = format!("next {round}");
        let current = archive.resolve_cell("local", "main")?;
        archive.publish_cell(
            "local",
            "main",
            base.as_bytes(),
            current.as_deref().map(version).as_ref(),
        )?;
        let mut racers = vec![
            Process::ready(&["swap", &path, "main", &base, &next])?,
            Process::ready(&["retract", &path, "main", &base])?,
        ];
        go(&mut racers)?;
        let swapped = racers[0].won()?;
        let retracted = racers[1].won()?;
        assert!(swapped != retracted, "round {round}: exactly one lands");
        let held = archive.resolve_cell("local", "main")?;
        if swapped {
            assert_eq!(held.as_deref(), Some(next.as_bytes()), "round {round}");
        } else {
            assert_eq!(held, None, "round {round}");
        }
    }
    Ok(())
}

/// A cell is updated in the writer's turn, as SQLite commits under its
/// write lock: an update waits while another process holds the turn, and
/// lands once it is let go. A read never waits.
#[dialog_common::test]
fn a_cell_update_waits_for_the_turn_and_a_read_does_not() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    let first = archive.publish_cell("local", "main", b"first", None)?;
    let holder = hold(&path, locks::TURN, true)?;
    let (read, _) = within(PROMPTLY, {
        let archive = archive.clone();
        move || archive.resolve_cell("local", "main")
    });
    assert_eq!(
        read.expect("a read does not wait")?.as_deref(),
        Some(&b"first"[..])
    );
    let (done, waiting) = within(A_WHILE, {
        let archive = archive.clone();
        move || archive.publish_cell("local", "main", b"second", Some(&first))
    });
    assert!(done.is_none(), "the update waits for the turn");
    holder.finish()?;
    waiting.recv_timeout(PROMPTLY)??;
    assert_eq!(
        archive.resolve_cell("local", "main")?.as_deref(),
        Some(&b"second"[..])
    );
    Ok(())
}

/// A reader reading a cell while another process writes it, small values
/// and large in turn, reads only values that were written, whole, in the
/// order they were written.
#[dialog_common::test]
fn a_cell_read_while_written_is_always_a_written_value() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let archive = FileArchive::open(&path)?;
    archive.publish_cell("local", "main", b"0000", None)?;
    let values: Vec<String> = (1..=200)
        .map(|n| {
            if n % 3 == 0 {
                format!("{n:04}{}", "-".repeat(400))
            } else {
                format!("{n:04}")
            }
        })
        .collect();
    let path_text = path.to_string_lossy().into_owned();
    let mut role = vec!["write", &path_text, "main"];
    role.extend(values.iter().map(String::as_str));
    let mut writer = Process::ready(&role)?;
    writer.go()?;
    let mut last = 0u32;
    let mut seen = std::collections::HashSet::new();
    loop {
        let value = archive.resolve_cell("local", "main")?.expect("never empty");
        let text = std::str::from_utf8(&value)?;
        assert!(
            text == "0000" || values.iter().any(|written| written == text),
            "read a value never written: {text:?}"
        );
        let n: u32 = text[..4].parse()?;
        assert!(n >= last, "read {n} after {last}");
        last = n;
        seen.insert(n);
        if n == 200 {
            break;
        }
    }
    writer.expect(DONE)?;
    writer.finish()?;
    assert!(seen.len() > 2, "the reads overlapped the writes: {seen:?}");
    Ok(())
}

/// Processes creating different cells at once all create theirs, and a
/// listing names every one.
#[dialog_common::test]
fn cells_created_at_once_are_all_listed() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    FileArchive::open(&path)?.append("index", &[block("first")])?;
    let mut creators = (0..4)
        .map(|creator| {
            Process::ready(&[
                "create",
                &path.to_string_lossy(),
                &format!("cell {creator}"),
                "value",
            ])
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    go(&mut creators)?;
    for mut creator in creators {
        assert!(creator.won()?, "each creates its own");
        creator.finish()?;
    }
    assert_eq!(
        FileArchive::open(&path)?.list_cells("local")?,
        (0..4).map(|n| format!("cell {n}")).collect::<Vec<_>>()
    );
    Ok(())
}

/// A retraction naming an edition the cell is no longer at is refused
/// and changes nothing; one naming its edition empties it.
#[dialog_common::test]
fn a_stale_retraction_is_refused() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let archive = FileArchive::open(archive_in(&directory))?;
    let first = archive.publish_cell("local", "main", b"first", None)?;
    let second = archive.publish_cell("local", "main", b"second", Some(&first))?;
    assert!(matches!(
        archive.retract_cell("local", "main", &first),
        Err(MemoryError::VersionMismatch { .. })
    ));
    assert_eq!(
        archive.resolve_cell("local", "main")?.as_deref(),
        Some(&b"second"[..])
    );
    archive.retract_cell("local", "main", &second)?;
    assert_eq!(archive.resolve_cell("local", "main")?, None);
    Ok(())
}

/// How long a cell's update waits, with one process or four committing
/// to cells of their own at once. A measurement, not a check: run it by
/// name, with `--ignored --nocapture`.
#[dialog_common::test]
#[ignore = "a measurement, run by name"]
fn measure_how_long_a_cell_update_waits() -> anyhow::Result<()> {
    for processes in [1, 4] {
        let directory = tempfile::tempdir()?;
        let path = archive_in(&directory);
        FileArchive::open(&path)?.append("index", &[block("first")])?;
        let path_text = path.to_string_lossy().into_owned();
        let mut writers = (0..processes)
            .map(|n| Process::ready(&["commits", &path_text, &format!("branch {n}"), "200"]))
            .collect::<anyhow::Result<Vec<_>>>()?;
        go(&mut writers)?;
        for (n, writer) in writers.iter_mut().enumerate() {
            let said = writer.expect(DONE)?;
            for (marker, what) in [("LATENCY", "publish"), ("WAITED", "waited for the turn")] {
                let line = said
                    .iter()
                    .find(|line| line.contains(marker))
                    .cloned()
                    .unwrap_or_default();
                let numbers: Vec<&str> = line.split_whitespace().rev().take(4).collect();
                println!(
                    "{processes} processes, writer {n}: {what} p50 {} us, p90 {} us, p99 {} us, max {} us",
                    numbers[3], numbers[2], numbers[1], numbers[0]
                );
            }
        }
        for writer in writers {
            writer.finish()?;
        }
    }
    Ok(())
}

/// A handle opened while this process's last handle is closing keeps the
/// file present: no other process can take the present byte exclusive,
/// so none takes itself for the last to close and cuts or retires what
/// this one uses. Today the closing handle lets go of the byte after the
/// new handle has taken over its state, so another process takes it.
#[dialog_common::test]
fn it_keeps_the_file_present_for_a_handle_opened_while_the_last_closes() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let path_text = path.to_string_lossy().into_owned();
    let archive = FileArchive::open(&path)?;
    archive.append("index", &[block("first")])?;
    // The close waits for the turn another process holds, after it found
    // itself the last handle.
    let holder = hold(&path, locks::TURN, true)?;
    let closing = std::thread::spawn(move || drop(archive));
    std::thread::sleep(A_WHILE);
    anyhow::ensure!(!closing.is_finished(), "the close waits for the turn");
    let reopened = FileArchive::open(&path)?;
    holder.finish()?;
    closing
        .join()
        .map_err(|_| anyhow::anyhow!("the close panicked"))?;
    let mut other = Process::spawn(&["try", &path_text, &locks::PRESENT.to_string()])?;
    assert!(
        !other.won()?,
        "another process took the present byte exclusive while this one has the file open"
    );
    other.finish()?;
    all_read_back(&reopened, &["first".into()])
}

/// Processes that end without closing the file (a crash, an exit without
/// drop) do not make it grow without bound: the empty extents a dead
/// process left listed are taken again by the next, so their count and
/// the file's length stay level however many die. Today each open claims
/// two new extents of 1 MiB whatever is left, so both grow with every
/// death, and the list heads for the slot's 64 KiB limit.
#[dialog_common::test]
fn it_does_not_pile_up_extents_left_by_processes_that_die() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = archive_in(&directory);
    let look = || -> anyhow::Result<(usize, u64)> {
        let archive = FileArchive::open_with(
            &path,
            Options {
                leave_extent: false,
                ..Options::default()
            },
        )?;
        let snapshot = archive.current();
        let empty = snapshot
            .slot
            .extents
            .iter()
            .filter(|&&record| extent::used(snapshot.bytes(), record) == Some(0))
            .count();
        drop(snapshot);
        // Looked at, not closed: a close would retire them.
        archive.inner.dies.store(true, Ordering::Relaxed);
        Ok((empty, std::fs::metadata(&path)?.len()))
    };
    let mut seen = Vec::new();
    for run in 0..12 {
        let mut dying = appender(&path, &format!("run {run}"), 1, 1 << 20, true)?;
        dying.go()?;
        dying.expect(DONE)?;
        dying.finish()?;
        seen.push(look()?);
    }
    let (first_empty, first_length) = seen[0];
    let (last_empty, last_length) = seen[seen.len() - 1];
    assert!(
        last_empty <= first_empty + 2 && last_length <= first_length + (4 << 20),
        "(empty extents listed, file length) after each of 12 deaths: {seen:?}"
    );
    Ok(())
}
