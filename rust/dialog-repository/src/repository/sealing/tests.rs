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
use dialog_artifacts::{Artifact, ArtifactSelector, Asset, AssetSealing, Instruction, Value};
use dialog_capability::Subject;
use dialog_common::Blake3Hash;
use dialog_effects::blob::{BlobError, ByteRange};
use dialog_effects::storage::Location;
use dialog_keyring::layered::asset::{CHUNK, sealed_len};
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
use crate::recorded_sealed;
use crate::repository::archive::local::read_all;
use crate::repository::source::SourceRef;
use crate::{Blob, CommitError, LocalIndex, RepositoryExt as _};

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

/// Nothing a sealed branch commits reaches the disk in the clear, inline,
/// spilled, or as an asset asserted or streamed in, while the same values
/// committed on a plain branch of the same repository do, and so does an
/// asset the sealed branch was told to keep in the clear (which is what
/// shows the scan can see them).
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

    // Assets on the sealed line: one sealed by assertion, one by streamed
    // import, and one marked plaintext, which is the one that may show.
    let branch = repo
        .branch("main")
        .open()
        .sealed(writer())
        .perform(&operator)
        .await?;
    let asserted = unique_name("sealed-asset-on-disk");
    let streamed = unique_name("streamed-asset-on-disk");
    let public = unique_name("public-asset-on-disk");
    let imported = branch
        .asset(stream::iter(chunked(&payload(&streamed))))
        .import()
        .perform(&operator)
        .await?;
    branch
        .transaction()
        .assert(Asset::new(payload(&asserted)))
        .assert(imported)
        .assert(Asset::new(payload(&public)).plaintext())
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let root = temp_storage_base();
    assert!(
        on_disk(&root, &plain)?,
        "the scan sees a plain commit's values"
    );
    assert!(on_disk(&root, &public)?, "the scan sees a plaintext asset");
    assert!(
        !on_disk(&root, &sealed)?,
        "a sealed commit's values reached the disk in the clear"
    );
    for marker in [&asserted, &streamed] {
        assert!(
            !on_disk(&root, marker)?,
            "a sealed asset reached the disk in the clear"
        );
    }
    Ok(())
}

/// Asset bytes carrying `marker` throughout, long enough to seal into
/// several pieces.
fn payload(marker: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    while bytes.len() < 2 * CHUNK + 1000 {
        bytes.extend_from_slice(marker.as_bytes());
        bytes.push(b' ');
    }
    bytes
}

fn chunked(bytes: &[u8]) -> Vec<Result<Vec<u8>, BlobError>> {
    bytes
        .chunks(10_000)
        .map(|chunk| Ok(chunk.to_vec()))
        .collect()
}

fn holds(bytes: &[u8], marker: &str) -> bool {
    bytes
        .windows(marker.len())
        .any(|window| window == marker.as_bytes())
}

/// `range` of the asset `entity` names, read through `$branch`.
macro_rules! read_asset {
    ($branch:expr, $operator:expr, $entity:expr, $range:expr) => {{
        let blob = Blob::from($entity);
        let blob = match $range {
            Some(range) => blob.slice(range),
            None => blob,
        };
        read_all(blob.read((&$branch).into()).perform($operator).await?).await?
    }};
}

/// The bytes `$branch`'s own blob store holds under `$digest`, or `None`.
macro_rules! stored {
    ($branch:expr, $operator:expr, $digest:expr) => {{
        match $branch
            .archive()
            .blob()
            .read(Blake3Hash::from($digest))
            .perform($operator)
            .await
        {
            Ok(reader) => Some(read_all(reader).await?),
            Err(BlobError::NotFound(_)) => None,
            Err(error) => return Err(error.into()),
        }
    }};
}

on_both_stores!(
    /// An asset asserted on a sealed line is stored only as its sealed
    /// copy: it reads back whole and in ranges across piece boundaries,
    /// its size is known, the blob store holds nothing under its plaintext
    /// hash, and the copy carries none of the plaintext. Asserting it again
    /// seals to the same copy and mints nothing.
    it_seals_an_asserted_asset,
    it_seals_an_asserted_asset_on_the_filesystem,
    |operator, repo| {
        let branch = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        let marker = unique_name("asserted-asset");
        let bytes = payload(&marker);
        let asset = Asset::new(bytes.clone());
        let entity = asset.entity()?;
        branch
            .transaction()
            .assert(asset.clone())
            .commit()
            .publish()
            .perform(operator)
            .await?;

        assert_eq!(read_asset!(branch, operator, entity.clone(), None), bytes);
        for (offset, length) in [
            (0, Some(10)),
            (CHUNK as u64 - 5, Some(10)),
            (CHUNK as u64, None),
            (bytes.len() as u64 - 3, Some(100)),
        ] {
            let end = length.map_or(bytes.len(), |length| {
                (offset as usize + length as usize).min(bytes.len())
            });
            assert_eq!(
                read_asset!(
                    branch,
                    operator,
                    entity.clone(),
                    Some(ByteRange { offset, length })
                ),
                &bytes[offset as usize..end],
                "range {offset}+{length:?}"
            );
        }
        assert_eq!(
            Blob::from(entity.clone())
                .size((&branch).into())
                .perform(operator)
                .await?,
            Some(bytes.len() as u64)
        );

        assert_eq!(stored!(branch, operator, *asset.hash()), None);
        let (copy, size) = recorded_sealed(SourceRef::from(&branch), asset.hash(), operator)
            .await?
            .expect("a sealed copy");
        assert_eq!(size, bytes.len() as u64);
        let sealed = stored!(branch, operator, copy.address).expect("the sealed copy");
        assert_eq!(sealed.len() as u64, sealed_len(size));
        assert_eq!(copy.length, sealed_len(size));
        assert!(!holds(&sealed, &marker), "the sealed copy holds the plaintext");

        let before = branch.revision();
        branch
            .transaction()
            .assert(asset)
            .commit()
            .publish()
            .perform(operator)
            .await?;
        assert_eq!(branch.revision(), before, "re-asserting minted a revision");

        // An empty asset seals to one empty piece and reads back empty,
        // as does a range past the end of a full one.
        let empty = Asset::new(Vec::new());
        branch
            .transaction()
            .assert(empty.clone())
            .commit()
            .publish()
            .perform(operator)
            .await?;
        assert!(read_asset!(branch, operator, empty.entity()?, None).is_empty());
        let past = Some(ByteRange {
            offset: bytes.len() as u64 + 5,
            length: Some(10),
        });
        assert!(read_asset!(branch, operator, entity, past).is_empty());
        Ok(())
    }
);

on_both_stores!(
    /// A streamed import on a sealed line seals as it writes and returns
    /// the sealed copy; asserting it records the asset, which reads back.
    /// `Blob::import` does the same in one step. Neither leaves the
    /// plaintext in the blob store.
    it_seals_a_streamed_import,
    it_seals_a_streamed_import_on_the_filesystem,
    |operator, repo| {
        let branch = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        let marker = unique_name("streamed-asset");
        let bytes = payload(&marker);

        let asset = branch
            .asset(stream::iter(chunked(&bytes)))
            .import()
            .perform(operator)
            .await?;
        let AssetSealing::Sealed(copy) = *asset.sealing() else {
            panic!("a sealed line's import returned {asset:?}");
        };
        assert_eq!(asset.size(), bytes.len() as u64);
        assert_eq!(copy.length, sealed_len(asset.size()));
        branch
            .transaction()
            .assert(asset.clone())
            .commit()
            .publish()
            .perform(operator)
            .await?;
        assert_eq!(read_asset!(branch, operator, asset.entity()?, None), bytes);
        assert_eq!(stored!(branch, operator, *asset.hash()), None);
        let sealed = stored!(branch, operator, copy.address).expect("the sealed copy");
        assert!(!holds(&sealed, &marker), "the sealed copy holds the plaintext");

        let other = payload(&unique_name("written-blob"));
        let entity = Blob::import(stream::iter(chunked(&other)))
            .write(branch.blobs())
            .perform(operator)
            .await?;
        assert_eq!(read_asset!(branch, operator, entity.clone(), None), other);
        let hash = entity.blob_hash().expect("an asset entity");
        assert_eq!(stored!(branch, operator, hash), None);
        assert!(
            recorded_sealed(SourceRef::from(&branch), &hash, operator)
                .await?
                .is_some()
        );
        Ok(())
    }
);

on_both_stores!(
    /// An asset marked plaintext stays in the clear on a sealed line, by
    /// assertion and by streamed import alike: the blob store holds it
    /// under its plaintext hash, no sealed copy is recorded, and it reads
    /// back.
    it_keeps_an_asset_marked_plaintext_in_the_clear,
    it_keeps_an_asset_marked_plaintext_in_the_clear_on_the_filesystem,
    |operator, repo| {
        let branch = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        let asserted = Asset::new(payload(&unique_name("public-asserted"))).plaintext();
        let streamed_bytes = payload(&unique_name("public-streamed"));
        let streamed = branch
            .asset(stream::iter(chunked(&streamed_bytes)))
            .plaintext()
            .import()
            .perform(operator)
            .await?;
        assert_eq!(streamed.sealing(), &AssetSealing::Plaintext);
        branch
            .transaction()
            .assert(asserted.clone())
            .assert(streamed.clone())
            .commit()
            .publish()
            .perform(operator)
            .await?;

        for (asset, bytes) in [
            (&asserted, asserted.content().expect("carried").to_vec()),
            (&streamed, streamed_bytes.clone()),
        ] {
            assert_eq!(stored!(branch, operator, *asset.hash()), Some(bytes.clone()));
            assert!(
                recorded_sealed(SourceRef::from(&branch), asset.hash(), operator)
                    .await?
                    .is_none()
            );
            assert_eq!(read_asset!(branch, operator, asset.entity()?, None), bytes);
        }
        Ok(())
    }
);

on_both_stores!(
    /// A sealed line refuses a stored asset naming plaintext bytes that
    /// was not marked plaintext, with `PlaintextAsset`, and the head does
    /// not move.
    it_refuses_an_unmarked_plaintext_asset_on_a_sealed_line,
    it_refuses_an_unmarked_plaintext_asset_on_a_sealed_line_on_the_filesystem,
    |operator, repo| {
        let branch = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        let bytes = payload(&unique_name("unmarked"));
        let imported = branch
            .asset(stream::iter(chunked(&bytes)))
            .plaintext()
            .import()
            .perform(operator)
            .await?;
        let unmarked = Asset::stored(*imported.hash(), imported.size());

        let refused = branch
            .transaction()
            .assert(unmarked)
            .commit()
            .publish()
            .perform(operator)
            .await;
        assert!(
            matches!(refused, Err(CommitError::PlaintextAsset)),
            "a sealed line recorded an unmarked plaintext asset: {refused:?}"
        );
        assert_eq!(branch.revision(), None, "the head moved");
        Ok(())
    }
);

on_both_stores!(
    /// A line that is not sealed refuses a sealed asset with
    /// `SealedAssetOnPlainLine`, and its head does not move.
    it_refuses_a_sealed_asset_on_a_plain_line,
    it_refuses_a_sealed_asset_on_a_plain_line_on_the_filesystem,
    |operator, repo| {
        let sealed = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        let asset = sealed
            .asset(stream::iter(chunked(&payload(&unique_name("misplaced")))))
            .import()
            .perform(operator)
            .await?;

        let plain = repo.branch("other").open().perform(operator).await?;
        let refused = plain
            .transaction()
            .assert(asset)
            .commit()
            .publish()
            .perform(operator)
            .await;
        assert!(
            matches!(refused, Err(CommitError::SealedAssetOnPlainLine)),
            "a plain line recorded a sealed asset: {refused:?}"
        );
        assert_eq!(plain.revision(), None, "the head moved");
        Ok(())
    }
);

on_both_stores!(
    /// A handle that can read a sealed line but holds no writer is refused
    /// sealing an asset, by import and by assertion, with `ReadOnly`; the
    /// head does not move and nothing is recorded or stored.
    it_refuses_to_seal_an_asset_without_a_writer,
    it_refuses_to_seal_an_asset_without_a_writer_on_the_filesystem,
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
        let bytes = payload(&unique_name("unwritten"));

        let imported = reader
            .asset(stream::iter(chunked(&bytes)))
            .import()
            .perform(operator)
            .await;
        assert!(
            matches!(imported, Err(CommitError::Sealing(KeyringError::ReadOnly))),
            "a reader sealed an asset: {imported:?}"
        );

        let asset = Asset::new(bytes);
        let asserted = reader
            .transaction()
            .assert(asset.clone())
            .commit()
            .publish()
            .perform(operator)
            .await;
        assert!(
            matches!(asserted, Err(CommitError::Sealing(KeyringError::ReadOnly))),
            "a reader recorded an asset: {asserted:?}"
        );
        assert_eq!(reader.revision(), before, "the head moved");
        assert!(
            recorded_sealed(SourceRef::from(&reader), asset.hash(), operator)
                .await?
                .is_none()
        );
        assert_eq!(stored!(reader, operator, *asset.hash()), None);
        Ok(())
    }
);

on_both_stores!(
    /// Retracting a sealed asset retracts its sealed fact: the line no
    /// longer vouches for it.
    it_retracts_a_sealed_asset,
    it_retracts_a_sealed_asset_on_the_filesystem,
    |operator, repo| {
        let branch = repo
            .branch("main")
            .open()
            .sealed(writer())
            .perform(operator)
            .await?;
        let asset = Asset::new(payload(&unique_name("retracted")));
        branch
            .transaction()
            .assert(asset.clone())
            .commit()
            .publish()
            .perform(operator)
            .await?;
        branch
            .transaction()
            .retract(asset.clone())
            .commit()
            .publish()
            .perform(operator)
            .await?;
        assert_eq!(
            Blob::from(asset.entity()?)
                .size((&branch).into())
                .perform(operator)
                .await?,
            None
        );
        assert!(
            recorded_sealed(SourceRef::from(&branch), asset.hash(), operator)
                .await?
                .is_none()
        );
        Ok(())
    }
);
