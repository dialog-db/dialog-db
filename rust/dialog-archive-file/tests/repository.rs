//! A repository whose space keeps its blocks in a [`FileArchive`]: commits
//! and queries go through it as they do through any archive.

#![cfg(not(target_arch = "wasm32"))]

use std::path::Path;
use std::str::FromStr;

use dialog_archive_file::{FILE_NAME, FileArchive, Options};
use dialog_artifacts::{
    Artifact, ArtifactSelector, Changes, Entity, Instruction, Pick, Relation, Value,
};
use dialog_capability::Subject;
use dialog_common::{Blake3Hash, Buffer};
use dialog_effects::storage::{Directory, Location};
use dialog_peer::helpers::{open_peer, test_owned, unique_name};
use dialog_repository::RepositoryExt as _;
use dialog_storage::provider::{FileSystem, Space, Storage};
use futures_util::TryStreamExt as _;

/// A space that keeps its blocks in one file and everything else on the
/// filesystem.
type FileArchiveSpace = Space<FileArchive, FileSystem, FileSystem, FileSystem, FileSystem>;

fn fact(entity: &str, name: &str) -> anyhow::Result<Artifact> {
    Ok(Artifact {
        the: Relation::from_str("person/name")?,
        of: Entity::from_str(entity)?,
        is: Value::String(name.to_string()),
        cause: None,
    })
}

/// Opens the `main` branch of repository `name` in the peer at `peer`, with
/// an operator allowed to act on it.
macro_rules! open {
    ($at:expr, $peer:expr, $name:expr) => {{
        let storage = test_owned(Storage::<FileArchiveSpace>::new()).await;
        let profile = open_peer(storage, Location::new($at.clone(), $peer.clone())).await?;
        let operator = profile
            .session(b"file-archive")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let repository = profile
            .space($name.clone())
            .open()
            .perform(&operator)
            .await?;
        let branch = repository.branch("main").open().perform(&operator).await?;
        (operator, branch)
    }};
}

/// Ten commits of ten facts each to the `main` branch of repository `name`,
/// then a check that all hundred read back.
async fn commit_facts(at: &Directory, peer: &str, name: &str) -> anyhow::Result<()> {
    let (at, peer, name) = (at.clone(), peer.to_string(), name.to_string());
    let (operator, branch) = open!(at, peer, name);
    for commit in 0..10 {
        let changes: Changes = (0..10)
            .map(|row| {
                let entity = format!("did:test:person-{commit}-{row}");
                Ok(Instruction::Assert(
                    fact(&entity, &format!("person {commit}.{row}"))?,
                    Pick::All,
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        branch
            .transaction()
            .integrate(changes)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    let rows: Vec<Artifact> = branch
        .claims()
        .select(ArtifactSelector::new().the(Relation::from_str("person/name")?))
        .to_owned()
        .perform(&operator)
        .await?
        .try_collect()
        .await?;
    assert_eq!(rows.len(), 100);
    Ok(())
}

#[tokio::test]
async fn it_commits_and_selects_through_a_file_archive() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let at = Directory::At(directory.path().to_string_lossy().into_owned());
    let peer = unique_name("peer");
    let name = unique_name("repo");
    let selector = || -> anyhow::Result<_> {
        Ok(ArtifactSelector::new().the(Relation::from_str("person/name")?))
    };

    commit_facts(&at, &peer, &name).await?;

    // Everything above is dropped: the repository reopens from disk and its
    // facts read back out of the archive file.
    let (operator, branch) = open!(at, peer, name);
    let rows: Vec<Artifact> = branch
        .claims()
        .select(selector()?)
        .to_owned()
        .perform(&operator)
        .await?
        .try_collect()
        .await?;
    assert_eq!(rows.len(), 100);
    assert!(
        rows.iter()
            .any(|row| row.is == Value::String("person 7.3".into()))
    );

    // The repository's blocks are in its archive file, not in a directory
    // of blocks.
    let space = directory.path().join(&name);
    let archive = FileArchive::open(space.join(FILE_NAME))?;
    assert!(
        !archive.is_empty(),
        "the archive file holds the commits' blocks"
    );
    assert!(!space.join("archive").exists(), "no directory of blocks");

    // The tree's nodes are frames, most written against the version before,
    // and every block reads back as the bytes its hash names.
    let stats = archive.stats();
    if let Ok(keep) = std::env::var("KEEP_ARCHIVE") {
        std::fs::copy(space.join(FILE_NAME), keep)?;
    }
    assert!(stats.frames > 0, "tree nodes are kept as frames: {stats:?}");
    assert!(
        stats.frames_against > 0,
        "a rewritten node is written against its predecessor: {stats:?}"
    );
    let hashes = archive.hashes();
    for (catalog, hash) in &hashes {
        let block = archive.get(catalog, hash)?.expect("a block it lists");
        assert_eq!(&Blake3Hash::hash(&block), hash, "{catalog}");
    }

    // Copied into another file a few blocks at a time, with a checkpoint
    // every few appends, the blocks read back the same after a reopen,
    // whether the open starts from a checkpoint or from the records after it.
    let copy = directory.path().join("copy.dialog");
    {
        let target = FileArchive::open_with(
            &copy,
            Options {
                checkpoint_every: 4096,
                chain: 4,
                ..Options::default()
            },
        )?;
        for chunk in hashes.chunks(5) {
            for (catalog, hash) in chunk {
                let block = archive.get(catalog, hash)?.expect("a block it lists");
                target.append(catalog, &[Buffer::from(block)])?;
            }
        }
        assert_eq!(target.stats().frames, stats.frames);
    }
    let reopened = FileArchive::open(&copy)?;
    assert_eq!(reopened.len(), hashes.len());
    for (catalog, hash) in &hashes {
        let block = reopened.get(catalog, hash)?.expect("a block it copied");
        assert_eq!(&Blake3Hash::hash(&block), hash, "{catalog}");
    }
    Ok(())
}

/// The variables a copying process reads its work from.
const SOURCE: &str = "DIALOG_ARCHIVE_FILE_SOURCE";
const TARGET: &str = "DIALOG_ARCHIVE_FILE_TARGET";
const SHARE: &str = "DIALOG_ARCHIVE_FILE_SHARE";

/// Every block of the archive at `source`, in an order every process
/// agrees on.
fn sorted_blocks(source: &FileArchive) -> Vec<(String, Blake3Hash)> {
    let mut hashes = source.hashes();
    hashes.sort_by(|a, b| (&a.0, a.1.as_bytes()).cmp(&(&b.0, b.1.as_bytes())));
    hashes
}

/// Appends every other block of `source` to `target`, starting at `share`,
/// one append per block, once `go` exists. Pauses between appends, so two
/// processes copying at once take turns many times over.
fn copy_share(source: &Path, target: &Path, share: usize, go: &Path) -> anyhow::Result<()> {
    let source = FileArchive::open(source)?;
    let target = FileArchive::open(target)?;
    let blocks = sorted_blocks(&source);
    while !go.exists() {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    for (catalog, hash) in blocks.into_iter().skip(share).step_by(2) {
        let block = source.get(&catalog, &hash)?.expect("a block it lists");
        target.append(&catalog, &[Buffer::from(block)])?;
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    Ok(())
}

/// The other process of [`it_takes_turns_with_another_process`]: copies its
/// share of the blocks. Does nothing unless that test started it.
#[test]
#[ignore = "run by it_takes_turns_with_another_process in its own process"]
fn copying_process() -> anyhow::Result<()> {
    let (Ok(source), Ok(target), Ok(share)) = (
        std::env::var(SOURCE),
        std::env::var(TARGET),
        std::env::var(SHARE),
    ) else {
        return Ok(());
    };
    let target: &Path = target.as_ref();
    std::fs::write(target.with_extension("ready"), b"")?;
    copy_share(
        source.as_ref(),
        target,
        share.parse()?,
        &target.with_extension("go"),
    )
}

/// Two processes append to one file at once, block by block: each takes
/// its turn on the file's lock and first reads what the other appended,
/// so the frames either writes name the other's tokens and nodes by the
/// ids the file gives them. Every block reads back from a fresh open, and
/// a process that holds the file open sees the other's blocks.
#[tokio::test]
async fn it_takes_turns_with_another_process() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let at = Directory::At(directory.path().to_string_lossy().into_owned());
    let name = unique_name("repo");
    commit_facts(&at, &unique_name("peer"), &name).await?;
    let source = directory.path().join(&name).join(FILE_NAME);
    let target = directory.path().join("shared.dialog");

    let mut other = std::process::Command::new(std::env::current_exe()?)
        .args(["copying_process", "--exact", "--ignored", "--nocapture"])
        .env(SOURCE, &source)
        .env(TARGET, &target)
        .env(SHARE, "1")
        .spawn()?;
    // Start both copies at once, once the other process is ready.
    while !target.with_extension("ready").exists() {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    std::fs::write(target.with_extension("go"), b"")?;
    copy_share(&source, &target, 0, &target.with_extension("go"))?;
    assert!(
        other.wait()?.success(),
        "the other process copied its share"
    );

    let hashes = sorted_blocks(&FileArchive::open(&source)?);
    {
        // This process's handle, open all along, reads the other's blocks.
        let held = FileArchive::open(&target)?;
        for (catalog, hash) in &hashes {
            let block = held.get(catalog, hash)?.expect("a block either copied");
            assert_eq!(&Blake3Hash::hash(&block), hash, "{catalog}");
        }
    }
    let reopened = FileArchive::open(&target)?;
    assert_eq!(reopened.len(), hashes.len());
    assert!(
        reopened.stats().frames > 0,
        "the copies kept nodes as frames"
    );
    for (catalog, hash) in &hashes {
        let block = reopened.get(catalog, hash)?.expect("a block either copied");
        assert_eq!(&Blake3Hash::hash(&block), hash, "{catalog}");
    }
    Ok(())
}
