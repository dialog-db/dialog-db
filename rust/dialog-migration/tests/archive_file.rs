//! A replica written by the `fs` provider, transcoded into the single file:
//! every block reads back as the bytes it was, and a repository opened over
//! the file reads what the replica committed.

#![cfg(not(target_arch = "wasm32"))]
// Exercises the one-shot `.commit().publish().perform(..)`, which keeps
// failing on a lost race as it did; nothing here races.
#![allow(deprecated)]

use std::str::FromStr;

use dialog_archive_file::{FILE_NAME, FileArchive};
use dialog_capability::Subject;
use dialog_corpus::{Changes, Entity, Fact, Instruction, Pick, Relation, Selector, Value};
use dialog_effects::storage::{Directory as At, Location};
use dialog_migration::archive_file::{BlockSource, Directory, transcode_spaces};
use dialog_peer::helpers::{Seed, open_peer_as, test_owned};
use dialog_repository::RepositoryExt as _;
use dialog_storage::provider::storage::NativeSpace;
use dialog_storage::provider::{FileSystem, Space, Storage};
use futures_util::TryStreamExt as _;

type FileArchiveSpace = Space<FileArchive, FileSystem, FileSystem, FileSystem, FileSystem>;

/// Opens the `main` branch of repository `name` in a space of `$space`
/// under `at`, with the same keys every time.
macro_rules! open {
    ($space:ty, $at:expr, $create:expr) => {{
        let seed = Seed::new(733);
        let storage = test_owned(Storage::<$space>::new()).await;
        let profile = open_peer_as(
            storage,
            Location::new($at.clone(), "peer".to_string()),
            seed.credential("profile").await?,
        )
        .await?;
        let operator = profile
            .session(b"transcode")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let space = profile.space("repo".to_string());
        let branch = if $create {
            space
                .create()
                .with_credential(seed.signer("space").await?)
                .perform(&operator)
                .await?
                .branch("main")
                .open()
                .perform(&operator)
                .await?
        } else {
            space
                .open()
                .perform(&operator)
                .await?
                .branch("main")
                .open()
                .perform(&operator)
                .await?
        };
        (operator, branch)
    }};
}

macro_rules! select_all {
    ($operator:expr, $branch:expr) => {{
        let mut rows: Vec<Fact> = $branch
            .claims()
            .select(Selector::new().the(Relation::from_str("person/name")?))
            .to_owned()
            .perform(&$operator)
            .await?
            .try_collect()
            .await?;
        rows.sort_by(|a, b| a.of.to_string().cmp(&b.of.to_string()));
        rows
    }};
}

#[tokio::test]
async fn it_transcodes_a_replica_the_fs_provider_wrote() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let at = At::At(directory.path().to_string_lossy().into_owned());

    // A replica on the `fs` provider: twenty commits, some rewriting what
    // earlier ones wrote, so nodes have versions to be written against.
    let before = {
        let _entropy = Seed::new(733).entropy();
        let (operator, branch) = open!(NativeSpace, at, true);
        for commit in 0..20 {
            let changes: Changes = (0..10)
                .map(|row| {
                    let entity = format!("did:test:person-{}", (commit * 7 + row) % 60);
                    Ok(Instruction::Assert(
                        Fact {
                            the: Relation::from_str("person/name")?,
                            of: Entity::from_str(&entity)?,
                            is: Value::String(format!("name {commit}.{row}")),
                            cause: None,
                        },
                        Pick::Last,
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
        select_all!(operator, branch)
    };
    assert_eq!(before.len(), 60);

    // Every space the profile directory holds (the peer's own and the
    // repository's), transcoded into the file a file-archive space reads,
    // beside the rest of the space.
    let reports = transcode_spaces(directory.path())?;
    assert_eq!(reports.len(), 2, "the peer's space and the repository's");
    let space = directory.path().join("repo");
    let (_, report) = reports
        .iter()
        .find(|(at, _)| at == &space)
        .expect("the repository's space");
    let catalog = report
        .catalogs
        .iter()
        .find(|catalog| catalog.catalog == "index")
        .expect("the index catalog");
    assert!(catalog.frames > 0, "{report:?}");
    assert!(catalog.corrupt.is_empty());
    let source = Directory::space(&space);
    let archive = FileArchive::open(space.join(FILE_NAME))?;

    // Every block reads back as the bytes the replica held.
    let mut blocks = 0;
    for catalog in source.catalogs()? {
        for hash in source.hashes(&catalog)? {
            let held = source.read(&catalog, &hash)?.expect("a block it lists");
            let read = archive.get(&catalog, &hash)?.expect("a block it copied");
            assert_eq!(read.as_slice(), held.as_ref());
            blocks += 1;
        }
    }
    assert_eq!(archive.len(), blocks);
    let stats = archive.stats();
    assert!(
        stats.frames_against > 0,
        "versions written against the one before: {stats:?}"
    );
    drop(archive);

    // The repository, opened over the file, reads what it committed.
    let (operator, branch) = open!(FileArchiveSpace, at, false);
    let after = select_all!(operator, branch);
    assert_eq!(after, before);
    Ok(())
}

/// An application's migration, as a library: a plan that changes nothing,
/// a run that moves every space, and a second run with nothing left to do;
/// a run interrupted mid-way picks up where it stopped; and the `fs`
/// archives go once asked to.
#[tokio::test]
async fn it_migrates_a_replica_as_an_application_would() -> anyhow::Result<()> {
    use dialog_migration::Migrations;
    use dialog_migration::archive_file::{ArchiveFile, MIGRATING};

    let directory = tempfile::tempdir()?;
    let at = At::At(directory.path().to_string_lossy().into_owned());
    let before = {
        let _entropy = Seed::new(733).entropy();
        let (operator, branch) = open!(NativeSpace, at, true);
        for commit in 0..8 {
            let changes: Changes = (0..10)
                .map(|row| {
                    Ok(Instruction::Assert(
                        Fact {
                            the: Relation::from_str("person/name")?,
                            of: Entity::from_str(&format!(
                                "did:test:person-{}",
                                (commit * 3 + row) % 30
                            ))?,
                            is: Value::String(format!("name {commit}.{row}")),
                            cause: None,
                        },
                        Pick::Last,
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
        select_all!(operator, branch)
    };

    let repo = directory.path().join("repo");
    let migrations = Migrations::new().with(ArchiveFile {
        remove_source: true,
    });

    // The plan names both spaces, and changes nothing.
    let plan = migrations.plan(directory.path())?;
    assert_eq!(plan.len(), 2, "{plan:?}");
    assert!(!repo.join(FILE_NAME).exists());

    // A run interrupted mid-way: part of the repository's blocks are in
    // the file being written, none has its final name yet.
    {
        let partial = FileArchive::open(repo.join(MIGRATING))?;
        let source = Directory::space(&repo);
        let hashes = source.hashes("index")?;
        for hash in hashes.iter().take(hashes.len() / 2) {
            let block = source.read("index", hash)?.expect("a block it lists");
            partial.append("index", &[block])?;
        }
    }
    assert_eq!(migrations.plan(directory.path())?.len(), 2);

    // The run finishes it, every space moved and its `fs` archive gone.
    let applied = migrations.run(directory.path())?;
    assert_eq!(applied.len(), 2, "{applied:?}");
    for (space, _) in [(directory.path().join("peer"), ()), (repo.clone(), ())] {
        assert!(space.join(FILE_NAME).exists(), "{}", space.display());
        assert!(!space.join(MIGRATING).exists());
        assert!(!space.join("archive").exists(), "the fs archive is removed");
    }

    // Nothing is left to do.
    assert!(migrations.plan(directory.path())?.is_empty());
    assert!(migrations.run(directory.path())?.is_empty());

    let (operator, branch) = open!(FileArchiveSpace, at, false);
    assert_eq!(select_all!(operator, branch), before);
    Ok(())
}

/// Writes `bytes` into the space's `fs` archive, in `catalog`, under the
/// base58 of the hash of `named` (the block's own bytes, unless a test
/// wants a file whose name does not match what it holds).
fn put_block(
    space: &std::path::Path,
    catalog: &str,
    named: &[u8],
    bytes: &[u8],
) -> anyhow::Result<()> {
    use base58::ToBase58 as _;

    let directory = space.join("archive").join(catalog);
    std::fs::create_dir_all(&directory)?;
    let name = dialog_common::Blake3Hash::hash(named)
        .as_bytes()
        .to_base58();
    std::fs::write(directory.join(name), bytes)?;
    Ok(())
}

fn buffer(text: &str) -> dialog_common::Buffer {
    dialog_common::Buffer::from(text.as_bytes().to_vec())
}

/// Migrating a space that already has an `archive.dialog` keeps what that
/// file holds: blocks a file-archive space wrote are not dropped for the
/// `fs` archive's. Today the transcode writes a new file and renames it
/// over the existing one, so a block only the existing file held is gone.
#[test]
fn it_keeps_what_an_existing_archive_file_holds_when_migrating() -> anyhow::Result<()> {
    use dialog_migration::archive_file::migrate_space;

    let directory = tempfile::tempdir()?;
    let space = directory.path().join("repo");
    let (old, new) = (buffer("only in the fs archive"), buffer("only in the file"));
    put_block(&space, "index", old.as_ref(), old.as_ref())?;
    {
        let archive = FileArchive::open(space.join(FILE_NAME))?;
        archive.append("index", std::slice::from_ref(&new))?;
        archive.sync()?;
    }
    migrate_space(&space, false)?;
    let archive = FileArchive::open(space.join(FILE_NAME))?;
    assert_eq!(
        archive.get("index", old.blake3_hash())?.as_deref(),
        Some(old.as_ref()),
        "the fs archive's block is migrated"
    );
    assert_eq!(
        archive.get("index", new.blake3_hash())?.as_deref(),
        Some(new.as_ref()),
        "the block the file already held is still there"
    );
    Ok(())
}

/// A space whose `archive.dialog` holds nothing (opening a path creates
/// it) but whose `fs` archive holds blocks still needs migrating. Today a
/// space with any `archive.dialog` counts as migrated, so its blocks are
/// never moved and a run reports nothing to do.
#[test]
fn it_counts_a_space_with_an_empty_archive_file_as_unmigrated() -> anyhow::Result<()> {
    use dialog_migration::Migrations;
    use dialog_migration::archive_file::{ArchiveFile, unmigrated};

    let directory = tempfile::tempdir()?;
    let space = directory.path().join("repo");
    let block = buffer("a block");
    put_block(&space, "index", block.as_ref(), block.as_ref())?;
    drop(FileArchive::open(space.join(FILE_NAME))?);
    assert_eq!(
        unmigrated(directory.path())?,
        vec![space.clone()],
        "the space is still to migrate"
    );
    let plan = Migrations::new()
        .with(ArchiveFile {
            remove_source: false,
        })
        .plan(directory.path())?;
    assert_eq!(plan.len(), 1, "{plan:?}");
    Ok(())
}

/// A migration that removes the `fs` archive does so only when every file
/// in it went into the archive file: a block whose bytes no longer match
/// its name, or a file not named as a block, is reported and kept (or the
/// run fails). Today they are left out silently and then deleted with the
/// rest of the `fs` archive.
#[test]
fn it_does_not_remove_files_the_migration_left_out() -> anyhow::Result<()> {
    use dialog_migration::Migrations;
    use dialog_migration::archive_file::ArchiveFile;

    let directory = tempfile::tempdir()?;
    let space = directory.path().join("repo");
    let good = buffer("a good block");
    put_block(&space, "index", good.as_ref(), good.as_ref())?;
    let whole = buffer("a block cut short on the disk");
    put_block(&space, "index", whole.as_ref(), &whole.as_ref()[..10])?;
    std::fs::write(
        space.join("archive").join("index").join("stray"),
        b"not a block",
    )?;

    let run = Migrations::new()
        .with(ArchiveFile {
            remove_source: true,
        })
        .run(directory.path());
    assert!(
        run.is_err() || space.join("archive").exists(),
        "the run succeeded ({run:?}) and removed the fs archive, with a corrupt block and a stray file in it"
    );
    Ok(())
}
