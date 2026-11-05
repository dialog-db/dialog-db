//! A replica written by the `fs` provider, transcoded into the single file:
//! every block reads back as the bytes it was, and a repository opened over
//! the file reads what the replica committed.

#![cfg(not(target_arch = "wasm32"))]

use std::str::FromStr;

use dialog_archive_file::{FILE_NAME, FileArchive};
use dialog_artifacts::{
    Artifact, ArtifactSelector, Changes, Entity, Instruction, Pick, Relation, Value,
};
use dialog_capability::Subject;
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
        let mut rows: Vec<Artifact> = $branch
            .claims()
            .select(ArtifactSelector::new().the(Relation::from_str("person/name")?))
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
                        Artifact {
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
