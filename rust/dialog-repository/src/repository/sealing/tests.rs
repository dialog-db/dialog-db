//! Sealed lines, end to end through the repository.
//!
//! Every claim runs on the volatile store and on the filesystem one. The
//! filesystem variants are native only, because `Storage::temp` lays its
//! roots under the platform temp directory; on the web the volatile
//! variants run, and `dialog-keyring`'s `tests/layered_archive.rs` covers
//! envelopes on the filesystem provider (OPFS there).

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_artifacts::{Artifact, ArtifactSelector, Asset, Instruction, Value};
use dialog_capability::Subject;
use dialog_common::Blake3Hash;
use dialog_effects::blob::BlobError;
use dialog_effects::storage::Location;
use dialog_keyring::layered::{Access, Level, LevelSecret, Writer};
use dialog_keyring::{EpochId, KeyringError};
use dialog_peer::helpers::{open_peer, test_storage, unique_name};
use dialog_search_tree::Manifest;
use futures_util::{StreamExt, stream};
#[cfg(not(target_arch = "wasm32"))]
use {
    dialog_peer::helpers::test_owned,
    dialog_storage::provider::storage::Storage,
    dialog_storage::temp_storage_base,
    std::{fs, io, path::Path},
};

use super::{SealedReadError, TreeSpace, admit, reader_space, writer_space};
use crate::{CommitError, LocalIndex, RepositoryExt as _};

fn secret(tag: u8) -> LevelSecret {
    LevelSecret::new(EpochId::from([tag; 32]), [tag.wrapping_mul(31); 32])
}

fn range() -> LevelSecret {
    secret(1)
}

fn content() -> LevelSecret {
    secret(2)
}

fn member() -> Access {
    Access::content(Level::new().with(range()), Level::new().with(content()))
}

/// A member's space, sealing under the first generations.
fn writer() -> TreeSpace {
    writer_space(Writer::new(range(), content()), member())
}

/// A value long enough to spill out of its leaf.
fn spilling(marker: &str) -> String {
    let inline = Manifest::default().inline_n as usize;
    format!("{marker}{}", "x".repeat(inline + 16))
}

/// One fact stored inline, one spilled, both carrying `marker`.
fn facts(marker: &str) -> Result<Vec<Instruction>> {
    Ok(vec![
        Instruction::Assert(Artifact {
            the: "note/title".parse()?,
            of: "note:1".parse()?,
            is: Value::String(format!("{marker}-inline")),
            cause: None,
        }),
        Instruction::Assert(Artifact {
            the: "note/body".parse()?,
            of: "note:1".parse()?,
            is: Value::String(spilling(marker)),
            cause: None,
        }),
    ])
}

/// The value of each fact, by attribute.
fn values(facts: &[Artifact]) -> Vec<(String, Value)> {
    let mut values: Vec<_> = facts
        .iter()
        .map(|fact| (fact.the.to_string(), fact.is.clone()))
        .collect();
    values.sort_by(|a, b| a.0.cmp(&b.0));
    values
}

/// What [`facts`] reads back as.
fn expected(marker: &str) -> Vec<(String, Value)> {
    vec![
        ("note/body".to_string(), Value::String(spilling(marker))),
        (
            "note/title".to_string(),
            Value::String(format!("{marker}-inline")),
        ),
    ]
}

/// Every fact about `note:1` on `$branch`.
macro_rules! read {
    ($branch:expr, $operator:expr) => {{
        let results: Vec<_> = $branch
            .claims()
            .select(ArtifactSelector::new().of("note:1".parse()?))
            .to_owned()
            .perform($operator)
            .await?
            .collect()
            .await;
        results.into_iter().collect::<Result<Vec<Artifact>, _>>()
    }};
}

/// A session over a fresh peer on `$storage`, and a repository it created.
macro_rules! rig {
    ($storage:expr) => {{
        let profile = open_peer($storage, Location::profile(unique_name("sealer"))).await?;
        let operator = profile
            .session(b"test")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let repo = profile
            .space(unique_name("sealed"))
            .create()
            .perform(&operator)
            .await?;
        (operator, repo)
    }};
}

/// Stamp `$body` out as a test on the volatile store, and one on the
/// filesystem store natively.
macro_rules! on_both_stores {
    ($(#[$doc:meta])* $name:ident, $fs:ident, |$operator:ident, $repo:ident| $body:block) => {
        $(#[$doc])*
        #[dialog_common::test]
        async fn $name() -> Result<()> {
            let (operator, repo) = rig!(test_storage().await);
            let ($operator, $repo) = (&operator, &repo);
            $body
        }

        $(#[$doc])*
        #[cfg(not(target_arch = "wasm32"))]
        #[dialog_common::test]
        async fn $fs() -> Result<()> {
            let (operator, repo) =
                rig!(test_owned(Storage::temp()).await);
            let ($operator, $repo) = (&operator, &repo);
            $body
        }
    };
}

on_both_stores!(
    /// A sealed branch reads back what it committed, inline and spilled;
    /// so does a handle that has learned nothing, from the head alone, and
    /// a commit through that handle builds on the tree.
    it_reads_back_a_sealed_commit,
    it_reads_back_a_sealed_commit_on_the_filesystem,
    |operator, repo| {
        let marker = unique_name("marker");
        let branch = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        branch
            .commit(stream::iter(facts(&marker)?))
            .perform(operator)
            .await?;
        let head = branch.revision().expect("a head");
        assert!(head.sealed.is_some(), "a sealed commit names its sealed root");
        assert_eq!(values(&read!(branch, operator)?), expected(&marker));

        let fresh = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        assert_eq!(values(&read!(fresh, operator)?), expected(&marker));
        fresh
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "note/tag".parse()?,
                of: "note:1".parse()?,
                is: Value::String(spilling("tag")),
                cause: None,
            })]))
            .perform(operator)
            .await?;

        let again = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        assert_eq!(read!(again, operator)?.len(), 3);
        Ok(())
    }
);

on_both_stores!(
    /// A party holding only structure keys reaches the root's envelope
    /// but is refused its content for want of the range generation; one
    /// holding ranges, for want of the content generation.
    it_refuses_a_sealed_root_to_a_party_without_content,
    it_refuses_a_sealed_root_to_a_party_without_content_on_the_filesystem,
    |operator, repo| {
        let branch = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        branch
            .commit(stream::iter(facts("refused")?))
            .perform(operator)
            .await?;
        let head = branch.revision().expect("a head");
        let root = Blake3Hash::from(*head.tree.hash());

        for (access, missing) in [
            (Access::structure(), range()),
            (Access::ranges(Level::new().with(range())), content()),
        ] {
            let space = reader_space(access);
            admit(Some(&space), &head);
            let index = LocalIndex::new(operator, branch.archive().index()).sealed(Some(space));
            let refused = index.load_node(&root).await;
            assert!(
                matches!(
                    &refused,
                    Err(SealedReadError::Keyring(KeyringError::MissingGeneration(generation)))
                        if generation == missing.generation()
                ),
                "a party without the generation opened the root: {refused:?}"
            );
        }
        Ok(())
    }
);

on_both_stores!(
    /// A handle that can read a sealed line but holds no writer is
    /// refused a commit with `ReadOnly`, and the head does not move.
    it_refuses_a_commit_without_a_writer,
    it_refuses_a_commit_without_a_writer_on_the_filesystem,
    |operator, repo| {
        let branch = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        branch
            .commit(stream::iter(facts("first")?))
            .perform(operator)
            .await?;
        let before = branch.revision();

        let reader = repo
            .branch("main")
            .open()
            .sealed(reader_space(member()))
            .perform(operator)
            .await?;
        let refused = reader
            .commit(stream::iter(facts("second")?))
            .perform(operator)
            .await;
        assert!(
            matches!(refused, Err(CommitError::Sealing(KeyringError::ReadOnly))),
            "a reader committed: {refused:?}"
        );
        assert_eq!(reader.revision(), before);
        assert_eq!(values(&read!(reader, operator)?), expected("first"));
        Ok(())
    }
);

on_both_stores!(
    /// A handle opened without the space reads a sealed line's head, but
    /// none of its tree: the archive holds no node under any identity.
    it_reads_nothing_of_a_sealed_tree_without_the_space,
    it_reads_nothing_of_a_sealed_tree_without_the_space_on_the_filesystem,
    |operator, repo| {
        let branch = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        branch
            .commit(stream::iter(facts("plain")?))
            .perform(operator)
            .await?;
        let head = branch.revision().expect("a head");
        let plain = repo.branch("main").open().perform(operator).await?;
        assert_eq!(plain.revision(), Some(head.clone()));

        let index = LocalIndex::new(operator, plain.archive().index());
        let root = Blake3Hash::from(*head.tree.hash());
        assert!(
            matches!(index.load_node(&root).await, Ok(None)),
            "the archive holds the root under its plaintext identity"
        );
        Ok(())
    }
);

/// Whether any file under `root` holds `marker`.
#[cfg(not(target_arch = "wasm32"))]
fn on_disk(root: &Path, marker: &str) -> io::Result<bool> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            for entry in fs::read_dir(&path)? {
                pending.push(entry?.path());
            }
        } else if fs::read(&path)?
            .windows(marker.len())
            .any(|window| window == marker.as_bytes())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Nothing a sealed branch commits reaches the disk in the clear, inline
/// or spilled, while the same values committed on a plain branch of the
/// same repository do (which is what shows the scan can see them).
///
/// Native only: it reads the files `Storage::temp` lays out under the
/// platform temp directory. On the web there is no such directory to
/// read; `it_reads_nothing_of_a_sealed_tree_without_the_space` and the
/// remote tests' walk of every envelope and sealed value cover what the
/// store holds there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_writes_no_value_in_the_clear_to_disk() -> Result<()> {
    let (operator, repo) = rig!(test_owned(Storage::temp()).await);
    let sealed = unique_name("sealed-on-disk");
    let plain = unique_name("plain-on-disk");

    repo.branch("main")
        .open()
        .sealed(writer())
        .perform(&operator)
        .await?
        .commit(stream::iter(facts(&sealed)?))
        .perform(&operator)
        .await?;
    repo.branch("other")
        .open()
        .perform(&operator)
        .await?
        .commit(stream::iter(facts(&plain)?))
        .perform(&operator)
        .await?;

    let root = temp_storage_base();
    assert!(
        on_disk(&root, &plain)?,
        "the scan sees a plain commit's values"
    );
    assert!(
        !on_disk(&root, &sealed)?,
        "a sealed commit's values reached the disk in the clear"
    );
    Ok(())
}

on_both_stores!(
    /// A sealed line refuses to store an asset, by import or by a
    /// transaction asserting one, with `SealedAsset`: the bytes would land
    /// in the blob store in the clear. Nothing is written, and the head
    /// does not move.
    it_refuses_an_asset_on_a_sealed_line,
    it_refuses_an_asset_on_a_sealed_line_on_the_filesystem,
    |operator, repo| {
        let branch = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        let payload = unique_name("asset").into_bytes();
        let asset = Asset::from(payload.clone());

        let imported = branch
            .asset(stream::iter(vec![Ok(payload.clone())]))
            .import()
            .perform(operator)
            .await;
        assert!(
            matches!(imported, Err(CommitError::SealedAsset)),
            "a sealed line imported an asset: {imported:?}"
        );

        let asserted = branch
            .transaction()
            .assert(asset.clone())
            .commit()
            .publish()
            .perform(operator)
            .await;
        assert!(
            matches!(asserted, Err(CommitError::SealedAsset)),
            "a sealed line recorded an asset: {asserted:?}"
        );

        assert_eq!(branch.revision(), None, "the head moved");
        let stored = branch
            .archive()
            .blob()
            .read(Blake3Hash::from(*asset.hash()))
            .perform(operator)
            .await;
        assert!(
            matches!(stored, Err(BlobError::NotFound(_))),
            "the asset's bytes reached the blob store"
        );
        Ok(())
    }
);
