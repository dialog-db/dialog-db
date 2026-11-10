//! A repository whose space keeps its blocks in a [`FileArchive`]: commits
//! and queries go through it as they do through any archive.

#![cfg(not(target_arch = "wasm32"))]

use std::path::Path;
use std::str::FromStr;

use dialog_archive_file::{FILE_NAME, FileArchive, Options};
use dialog_artifacts::{
    Artifact, ArtifactSelector, Changes, Entity, Instruction, Pick, Relation, Update as _, Value,
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

    // Copied into another file a block at a time, short chains of frames
    // written against their predecessors, the blocks read back the same
    // after a reopen, which reads the newest slot and nothing else.
    let copy = directory.path().join("copy.dialog");
    {
        let target = FileArchive::open_with(
            &copy,
            Options {
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

/// Reads `node` both ways, from its block and as the archive serves it, and
/// checks every answer the tree reads a node for agrees.
fn compare(archive: &FileArchive, catalog: &str, hash: &Blake3Hash) -> anyhow::Result<bool> {
    use dialog_artifacts::{Datum, Key, State};
    use dialog_search_tree::nodes::Read;
    use dialog_search_tree::{NodeBody, PersistentNode, into_owned};
    type TreeNode = PersistentNode<Key, State<Datum>>;

    let block = Buffer::from(archive.get(catalog, hash)?.expect("a block it lists"));
    let Ok(stored) = TreeNode::try_from(block.clone()) else {
        return Ok(false);
    };
    let scope = dialog_effects::archive::prelude::ArchiveScope::new(Subject::from(
        "did:test:served".parse::<dialog_capability::Did>()?,
    ))
    .catalog(catalog);
    // The archive answers a node read at once, from its mapping.
    let Some(served) = Read::<TreeNode>::new(scope, hash.clone())
        .perform(archive)
        .into_now()
        .unwrap_or_else(|_| panic!("the archive answers a read at once"))?
    else {
        return Ok(false);
    };
    if !served.is_rows() {
        return Ok(false);
    }
    assert_eq!(served.hash(), stored.hash());
    assert_eq!(served.manifest()?, stored.manifest()?);
    assert_eq!(served.scale(), stored.scale());
    assert_eq!(served.upper_bound()?, stored.upper_bound()?);
    match (stored.body(), served.body()) {
        (NodeBody::Segment(stored), NodeBody::Segment(served)) => {
            assert_eq!(served.len(), stored.len());
            let (mut ours, mut theirs) = (stored.keys::<Key>()?, served.keys::<Key>()?);
            while let Some((at, key)) = ours.next_key()? {
                let (other, served_key) = theirs.next_key()?.expect("as many keys");
                assert_eq!((at, key), (other, served_key));
                let (a, b): (State<Datum>, State<Datum>) = (
                    into_owned(stored.value_at(at)?)?,
                    into_owned(served.value_at(at)?)?,
                );
                assert_eq!(a, b);
                assert_eq!(served.find::<Key>(key)?, Some(at));
                let mut past = key.to_vec();
                past.push(0);
                assert_eq!(
                    served.find::<Key>(&past)?,
                    stored.find::<Key>(&past)?,
                    "a key between entries"
                );
            }
            assert!(theirs.next_key()?.is_none());
            assert_eq!(served.first_key::<Key>()?, stored.first_key::<Key>()?);
            assert_eq!(served.last_key::<Key>()?, stored.last_key::<Key>()?);
        }
        (NodeBody::Index(stored), NodeBody::Index(served)) => {
            assert_eq!(served.len(), stored.len());
            for at in 0..stored.len() {
                assert_eq!(served.separator(at)?, stored.separator(at)?);
                assert_eq!(served.hash_at(at)?, stored.hash_at(at)?);
                assert_eq!(served.scale_at(at)?, stored.scale_at(at)?);
            }
            assert_eq!(served.novelty_len(), stored.novelty_len());
            for at in 0..stored.len() {
                let separator = stored.separator(at)?;
                let mut probes = vec![separator.clone(), vec![]];
                let mut past = separator.clone();
                past.push(0xFF);
                probes.push(past);
                if let Some(last) = separator.last() {
                    let mut before = separator.clone();
                    *before.last_mut().expect("a byte") = last.saturating_sub(1);
                    probes.push(before);
                }
                for probe in &probes {
                    assert_eq!(served.route(probe)?, stored.route(probe)?);
                    for upper in &probes {
                        assert_eq!(
                            served.children_spanning(probe, upper)?,
                            stored.children_spanning(probe, upper)?
                        );
                    }
                    for bound in [
                        std::ops::Bound::Included(probe.as_slice()),
                        std::ops::Bound::Excluded(probe.as_slice()),
                    ] {
                        assert_eq!(
                            served.children_within(bound)?,
                            stored.children_within(bound)?
                        );
                    }
                }
                let (ours, theirs) = (stored.buffer_for(at), served.buffer_for(at));
                assert_eq!(ours.is_some(), theirs.is_some());
                if let (Some(ours), Some(theirs)) = (ours, theirs) {
                    assert_eq!(theirs.child(), ours.child());
                    assert_eq!(theirs.count(), ours.count());
                    let entries = ours.entries::<Key>()?;
                    assert_eq!(theirs.entries::<Key>()?, entries);
                    for (op, entry) in entries.iter().enumerate() {
                        assert_eq!(theirs.is_assert(op), ours.is_assert(op));
                        assert_eq!(
                            theirs.resolve::<Key>(&entry.key)?,
                            ours.resolve::<Key>(&entry.key)?
                        );
                    }
                }
            }
            assert_eq!(served.all_novelty::<Key>()?, stored.all_novelty::<Key>()?);
        }
        _ => panic!("a node served as another kind"),
    }
    // Its bytes, built on asking, are the very block.
    assert_eq!(served.buffer()?.as_ref(), block.as_ref());
    Ok(true)
}

/// Every node the archive keeps as a frame reads the same served in place
/// as it does from its block, accessor by accessor.
#[tokio::test]
async fn it_serves_every_node_as_its_block_reads() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let at = Directory::At(directory.path().to_string_lossy().into_owned());
    let name = unique_name("repo");
    commit_facts(&at, &unique_name("peer"), &name).await?;
    let archive = FileArchive::open(directory.path().join(&name).join(FILE_NAME))?;
    let mut served = 0;
    for (catalog, hash) in sorted_blocks(&archive) {
        served += compare(&archive, &catalog, &hash)? as usize;
    }
    assert_eq!(served, archive.stats().frames, "every frame is served");
    Ok(())
}

/// The same, over an archive file named by `DIALOG_ARCHIVE_SERVED`: run by
/// hand over a large replica.
#[test]
#[ignore = "compares every node of a file named by DIALOG_ARCHIVE_SERVED"]
fn it_serves_every_node_of_a_file_as_its_block_reads() -> anyhow::Result<()> {
    let Ok(path) = std::env::var("DIALOG_ARCHIVE_SERVED") else {
        return Ok(());
    };
    let archive = FileArchive::open(path)?;
    let mut served = 0;
    for (catalog, hash) in sorted_blocks(&archive) {
        served += compare(&archive, &catalog, &hash)? as usize;
    }
    println!("{served} nodes compared");
    assert_eq!(served, archive.stats().frames);
    Ok(())
}

/// A node rewritten by a commit takes the rows of the entries it keeps
/// unchanged from the node it supersedes, and only those: facts written,
/// replaced and retracted, then claimed again, so the same keys come back
/// with other data. Frames are verified in a debug build, so a row taken
/// for an entry whose data changed would fail to rebuild its node's block
/// and count as unverified; none does, and every fact reads back.
#[tokio::test]
async fn it_takes_unchanged_rows_from_the_node_a_commit_supersedes() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let at = Directory::At(directory.path().to_string_lossy().into_owned());
    let (peer, name) = (unique_name("peer"), unique_name("repo"));
    commit_facts(&at, &peer, &name).await?;

    let (operator, branch) = open!(at, peer, name);
    for commit in 0..10 {
        let mut changes = Changes::new();
        for row in 0..10 {
            let entity = format!("did:test:person-{commit}-{row}");
            if row % 3 == 0 {
                let named = fact(&entity, &format!("person {commit}.{row}"))?;
                changes.dissociate(named.the, named.of, named.is);
            } else {
                let renamed = fact(&entity, &format!("renamed {commit}.{row}"))?;
                changes.associate(renamed.the, renamed.of, renamed.is, Pick::Last);
            }
        }
        branch
            .transaction()
            .integrate(changes)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }

    // A second claim of a fact already held lands on the same key, with a
    // datum that now names both claims: the one case where a kept key's
    // row must not be taken.
    for commit in 0..10 {
        let changes: Changes = (1..10)
            .filter(|row| row % 3 != 0)
            .map(|row| {
                let entity = format!("did:test:person-{commit}-{row}");
                Ok(Instruction::Assert(
                    fact(&entity, &format!("renamed {commit}.{row}"))?,
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

    let archive = FileArchive::open(directory.path().join(&name).join(FILE_NAME))?;
    let stats = archive.stats();
    assert!(stats.rows_taken > 0, "rewritten nodes took rows: {stats:?}");
    if cfg!(debug_assertions) {
        assert_eq!(stats.unverified, 0, "every frame rebuilt its block");
    }
    let rows: Vec<Artifact> = branch
        .claims()
        .select(ArtifactSelector::new().the(Relation::from_str("person/name")?))
        .to_owned()
        .perform(&operator)
        .await?
        .try_collect()
        .await?;
    let renamed = rows
        .iter()
        .filter(|row| matches!(&row.is, Value::String(name) if name.starts_with("renamed")))
        .count();
    assert_eq!(
        (rows.len(), renamed),
        (60, 60),
        "every third person retracted, the rest renamed"
    );
    Ok(())
}

/// A repository whose space keeps its blocks in the archive reads its tree
/// from the frames in place, the archive answering the tree's node reads,
/// and selects what it committed.
#[tokio::test]
async fn it_selects_through_nodes_served_in_place() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let at = Directory::At(directory.path().to_string_lossy().into_owned());
    let (peer, name) = (unique_name("peer"), unique_name("repo"));
    commit_facts(&at, &peer, &name).await?;

    let (operator, branch) = open!(at, peer, name);
    let archive = FileArchive::open(directory.path().join(&name).join(FILE_NAME))?;
    // A second handle on the file shares the space's: its counts are the
    // space's.
    let before = archive.stats().served;
    let rows: Vec<Artifact> = branch
        .claims()
        .select(ArtifactSelector::new().the(Relation::from_str("person/name")?))
        .to_owned()
        .perform(&operator)
        .await?
        .try_collect()
        .await?;
    assert_eq!(rows.len(), 100);
    assert!(
        rows.iter()
            .any(|row| row.is == Value::String("person 4.2".into()))
    );
    assert!(
        archive.stats().served > before,
        "the tree read nodes the archive served in place"
    );
    Ok(())
}

/// A node in an older layout is kept whole even when frames are not checked
/// by rebuilding their blocks (`Options::verify` off, as in a release
/// build): its frame would rebuild it in the current layout, other bytes
/// than were stored. The node is a real segment from a committed tree,
/// archived again under the older layout's header.
#[tokio::test]
async fn it_keeps_a_node_of_another_layout_whole_unverified() -> anyhow::Result<()> {
    use dialog_artifacts::{Datum, Key, State};
    use dialog_search_tree::{
        LEGACY_LAYOUT, LegacyManifest, LegacyNodeBody, LegacySegment, NodeBody, PersistentNode,
        PersistentSegment, SegmentView,
    };
    let directory = tempfile::tempdir()?;
    let at = Directory::At(directory.path().to_string_lossy().into_owned());
    let (peer, name) = (unique_name("peer"), unique_name("repo"));
    commit_facts(&at, &peer, &name).await?;
    let source = FileArchive::open(directory.path().join(&name).join(FILE_NAME))?;

    let mut legacy = None;
    for (catalog, hash) in source.hashes() {
        let Some(bytes) = source.get(&catalog, &hash)? else {
            continue;
        };
        let Ok(node) = PersistentNode::<Key, State<Datum>>::try_from(Buffer::from(bytes)) else {
            continue;
        };
        let NodeBody::Segment(SegmentView::Archived(segment)) = node.body() else {
            continue;
        };
        if segment.count.to_native() == 0 {
            continue;
        }
        let segment: PersistentSegment<State<Datum>> =
            rkyv::deserialize::<_, rkyv::rancor::Error>(segment)?;
        let manifest = node.manifest()?;
        let body = LegacyNodeBody::Segment(LegacySegment {
            header: LegacyManifest {
                version: 1,
                fanout_n: manifest.fanout_n,
                max_separator: manifest.max_separator,
                inline_n: manifest.inline_n,
                spill_prefix: manifest.spill_prefix,
                max_segment: manifest.max_segment,
                frame_ceiling_factor: manifest.frame_ceiling_factor,
                anchor_selector: manifest.anchor_selector,
            },
            count: segment.count,
            layout: segment.layout,
            columns: segment.columns,
            values: segment.values,
        });
        legacy = Some((
            catalog,
            Buffer::from(rkyv::to_bytes::<rkyv::rancor::Error>(&body)?.to_vec()),
        ));
        break;
    }
    let (catalog, bytes) = legacy.ok_or_else(|| anyhow::anyhow!("no segment with entries"))?;
    let node = PersistentNode::<Key, State<Datum>>::try_from(bytes.clone())?;
    assert_eq!(node.layout_version(), LEGACY_LAYOUT);

    let target = FileArchive::open_with(
        directory.path().join("legacy.dialog"),
        Options {
            verify: false,
            ..Options::default()
        },
    )?;
    target.append(&catalog, std::slice::from_ref(&bytes))?;
    assert_eq!(target.stats().frames, 0, "the older node is not framed");
    assert_eq!(
        target.get(&catalog, bytes.blake3_hash())?.as_deref(),
        Some(bytes.as_ref()),
        "it reads back as the bytes stored"
    );
    Ok(())
}
