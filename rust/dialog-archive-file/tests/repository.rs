//! A repository whose space keeps its blocks in a [`FileArchive`]: commits
//! and queries go through it as they do through any archive.

#![cfg(not(target_arch = "wasm32"))]

use std::str::FromStr;

use dialog_archive_file::{FILE_NAME, FileArchive};
use dialog_artifacts::{
    Artifact, ArtifactSelector, Changes, Entity, Instruction, Pick, Relation, Value,
};
use dialog_capability::Subject;
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

#[tokio::test]
async fn it_commits_and_selects_through_a_file_archive() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let at = Directory::At(directory.path().to_string_lossy().into_owned());
    let peer = unique_name("peer");
    let name = unique_name("repo");
    let selector = || -> anyhow::Result<_> {
        Ok(ArtifactSelector::new().the(Relation::from_str("person/name")?))
    };

    {
        let (operator, branch) = open!(at, peer, name);
        // Ten commits of ten facts each.
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
            .select(selector()?)
            .to_owned()
            .perform(&operator)
            .await?
            .try_collect()
            .await?;
        assert_eq!(rows.len(), 100);
    }

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
    Ok(())
}
