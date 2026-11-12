//! A repository whose space keeps its blocks in a [`Chronicle`]: commits
//! and queries go through it as they do through any archive.

#![cfg(not(target_arch = "wasm32"))]

use std::path::Path;
use std::str::FromStr;

use dialog_capability::Subject;
use dialog_chronicle::{Chronicle, FILE_NAME, Options};
use dialog_common::{Blake3Hash, Buffer};
use dialog_corpus::{
    Changes, Entity, Fact, Instruction, Pick, Relation, Selector, Update as _, Value,
};
use dialog_effects::storage::{Directory, Location};
use dialog_peer::helpers::{Seed, open_peer_as, test_owned, unique_name};
use dialog_repository::RepositoryExt as _;
use dialog_storage::provider::{FileSystem, Space, Storage};
use futures_util::TryStreamExt as _;

/// A space that keeps its blocks in one file and everything else on the
/// filesystem.
type ChronicleSpace = Space<Chronicle, FileSystem, FileSystem, FileSystem, FileSystem>;

/// A space that keeps its blocks and its memory cells (branch heads among
/// them) in one file.
type FileSpace = Space<Chronicle, Chronicle, FileSystem, FileSystem, FileSystem>;

fn fact(entity: &str, name: &str) -> anyhow::Result<Fact> {
    Ok(Fact {
        the: Relation::from_str("person/name")?,
        of: Entity::from_str(entity)?,
        is: Value::String(name.to_string()),
        cause: None,
    })
}

/// Commits `changes` to `branch`, built again over the head that won when
/// another writer moves it first.
macro_rules! commit {
    ($branch:expr, $operator:expr, $changes:expr) => {{
        let changes: Changes = $changes;
        $branch
            .transact(|transaction| {
                let changes = changes.clone();
                async move { Ok::<_, dialog_repository::CommitError>(transaction.integrate(changes)) }
            })
            .perform($operator)
            .await
    }};
}

/// Opens the `main` branch of repository `name` in the peer at `peer`, with
/// an operator allowed to act on it.
macro_rules! open {
    ($at:expr, $peer:expr, $name:expr) => {
        open!(ChronicleSpace, $at, $peer, $name)
    };
    ($space:ty, $at:expr, $peer:expr, $name:expr) => {
        open!($space, $at, $peer, $name, "main")
    };
    ($space:ty, $at:expr, $peer:expr, $name:expr, $branch:expr) => {{
        let storage = test_owned(Storage::<$space>::new()).await;
        // A key of its own per process would make another process opening
        // the peer a different peer: every process derives the same one
        // from the peer's name.
        let credential = Seed::new(0).credential(&$peer).await?;
        let profile = open_peer_as(
            storage,
            Location::new($at.clone(), $peer.clone()),
            credential,
        )
        .await?;
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
        let branch = repository.branch($branch).open().perform(&operator).await?;
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
        commit!(branch, &operator, changes)?;
    }
    let rows: Vec<Fact> = branch
        .claims()
        .select(Selector::new().the(Relation::from_str("person/name")?))
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
    let selector =
        || -> anyhow::Result<_> { Ok(Selector::new().the(Relation::from_str("person/name")?)) };

    commit_facts(&at, &peer, &name).await?;

    // Everything above is dropped: the repository reopens from disk and its
    // facts read back out of the archive file.
    let (operator, branch) = open!(at, peer, name);
    let rows: Vec<Fact> = branch
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
    let archive = Chronicle::open(space.join(FILE_NAME))?;
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
        let target = Chronicle::open_with(
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
    let reopened = Chronicle::open(&copy)?;
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
fn sorted_blocks(source: &Chronicle) -> Vec<(String, Blake3Hash)> {
    let mut hashes = source.hashes();
    hashes.sort_by(|a, b| (&a.0, a.1.as_bytes()).cmp(&(&b.0, b.1.as_bytes())));
    hashes
}

/// Appends every other block of `source` to `target`, starting at `share`,
/// one append per block, once `go` exists. Pauses between appends, so two
/// processes copying at once take turns many times over.
fn copy_share(source: &Path, target: &Path, share: usize, go: &Path) -> anyhow::Result<()> {
    let source = Chronicle::open(source)?;
    let target = Chronicle::open(target)?;
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

    let hashes = sorted_blocks(&Chronicle::open(&source)?);
    {
        // This process's handle, open all along, reads the other's blocks.
        let held = Chronicle::open(&target)?;
        for (catalog, hash) in &hashes {
            let block = held.get(catalog, hash)?.expect("a block either copied");
            assert_eq!(&Blake3Hash::hash(&block), hash, "{catalog}");
        }
    }
    let reopened = Chronicle::open(&target)?;
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
fn compare(archive: &Chronicle, catalog: &str, hash: &Blake3Hash) -> anyhow::Result<bool> {
    use dialog_corpus::{Datum, Key, State};
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
    let archive = Chronicle::open(directory.path().join(&name).join(FILE_NAME))?;
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
    let archive = Chronicle::open(path)?;
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
        commit!(branch, &operator, changes)?;
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
        commit!(branch, &operator, changes)?;
    }

    let archive = Chronicle::open(directory.path().join(&name).join(FILE_NAME))?;
    let stats = archive.stats();
    assert!(stats.rows_taken > 0, "rewritten nodes took rows: {stats:?}");
    if cfg!(debug_assertions) {
        assert_eq!(stats.unverified, 0, "every frame rebuilt its block");
    }
    let rows: Vec<Fact> = branch
        .claims()
        .select(Selector::new().the(Relation::from_str("person/name")?))
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
    let archive = Chronicle::open(directory.path().join(&name).join(FILE_NAME))?;
    // A second handle on the file shares the space's: its counts are the
    // space's.
    let before = archive.stats().served;
    let rows: Vec<Fact> = branch
        .claims()
        .select(Selector::new().the(Relation::from_str("person/name")?))
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
    use dialog_corpus::{Datum, Key, State};
    use dialog_search_tree::{
        LEGACY_LAYOUT, LegacyManifest, LegacyNodeBody, LegacySegment, NodeBody, PersistentNode,
        PersistentSegment, SegmentView,
    };
    let directory = tempfile::tempdir()?;
    let at = Directory::At(directory.path().to_string_lossy().into_owned());
    let (peer, name) = (unique_name("peer"), unique_name("repo"));
    commit_facts(&at, &peer, &name).await?;
    let source = Chronicle::open(directory.path().join(&name).join(FILE_NAME))?;

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

    let target = Chronicle::open_with(
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

/// With its cells in the file too, a repository commits, reopens and reads
/// back, and no space keeps a memory directory: branch heads are in the
/// file with the blocks they name.
#[tokio::test]
async fn it_keeps_branch_heads_in_the_file() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let at = Directory::At(directory.path().to_string_lossy().into_owned());
    let (peer, name) = (unique_name("peer"), unique_name("repo"));
    {
        let (operator, branch) = open!(FileSpace, at, peer, name);
        for commit in 0..5 {
            let changes: Changes = (0..10)
                .map(|row| {
                    let entity = format!("did:test:person-{commit}-{row}");
                    Ok(Instruction::Assert(fact(&entity, "someone")?, Pick::All))
                })
                .collect::<anyhow::Result<_>>()?;
            commit!(branch, &operator, changes)?;
        }
    }
    let (operator, branch) = open!(FileSpace, at, peer, name);
    let rows: Vec<Fact> = branch
        .claims()
        .select(Selector::new().the(Relation::from_str("person/name")?))
        .to_owned()
        .perform(&operator)
        .await?
        .try_collect()
        .await?;
    assert_eq!(
        rows.len(),
        50,
        "the head read back from the file names them all"
    );

    let mut stack = vec![directory.path().to_path_buf()];
    let mut files = 0;
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                assert_ne!(
                    path.file_name().and_then(|name| name.to_str()),
                    Some("memory"),
                    "a memory directory at {}",
                    path.display()
                );
                stack.push(path);
            } else if path.file_name().and_then(|name| name.to_str()) == Some(FILE_NAME) {
                files += 1;
            }
        }
    }
    assert!(files > 0, "the spaces keep archive files");
    Ok(())
}

/// What a [`committing_process`] commits to, and how it is told to start:
/// the peer's directory, the peer, the repository, the branch, a tag its
/// facts are named by, how many commits, and the directory its signals
/// go in.
const COMMIT: &str = "DIALOG_COMMIT";

/// The facts of commit `commit` from the writer tagged `tag`: ten, each
/// its own entity.
fn tagged(tag: &str, commit: usize) -> anyhow::Result<Changes> {
    (0..10)
        .map(|row| {
            let entity = format!("did:test:{}-{commit}-{row}", tag.replace(' ', "-"));
            Ok(Instruction::Assert(
                fact(&entity, &format!("{tag} {commit}.{row}"))?,
                Pick::All,
            ))
        })
        .collect()
}

/// The names of the facts on `branch`, as its head has them.
async fn names_on(
    at: &Directory,
    peer: &str,
    name: &str,
    branch: &str,
) -> anyhow::Result<std::collections::HashSet<String>> {
    let (at, peer, name) = (at.clone(), peer.to_string(), name.to_string());
    let (operator, branch) = open!(FileSpace, at, peer, name, branch);
    let rows: Vec<Fact> = branch
        .claims()
        .select(Selector::new().the(Relation::from_str("person/name")?))
        .to_owned()
        .perform(&operator)
        .await?
        .try_collect()
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| match row.is {
            Value::String(name) => Some(name),
            _ => None,
        })
        .collect())
}

/// Waits for `signal` to appear, up to twenty seconds; whether it did.
fn signalled(signal: &Path) -> bool {
    let until = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while std::time::Instant::now() < until {
        if signal.exists() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    false
}

/// The other processes of the branch tests below: open the repository's
/// branch, commit once, say ready, wait to be told to go, make their
/// commits, say done, and close. Does nothing unless a test started it.
#[tokio::test]
#[ignore = "run by the branch tests in processes of its own"]
async fn committing_process() -> anyhow::Result<()> {
    let Ok(setting) = std::env::var(COMMIT) else {
        return Ok(());
    };
    let parts: Vec<&str> = setting.split('\u{1f}').collect();
    let [at, peer, name, branch, tag, commits, signals] = parts[..] else {
        anyhow::bail!("a setting of seven parts: {setting:?}");
    };
    let at = Directory::At(at.to_string());
    let (peer, name) = (peer.to_string(), name.to_string());
    let signals = Path::new(signals);
    let (operator, branch) = open!(FileSpace, at, peer, name, branch);
    commit!(branch, &operator, tagged(&format!("{tag} warm"), 0)?)?;
    std::fs::write(signals.join(format!("{tag}.ready")), b"")?;
    anyhow::ensure!(signalled(&signals.join("go")), "never told to go");
    for commit in 0..commits.parse()? {
        commit!(branch, &operator, tagged(tag, commit)?)?;
    }
    std::fs::write(signals.join(format!("{tag}.done")), b"")?;
    Ok(())
}

/// Starts a [`committing_process`] and waits until it is ready.
fn committer(
    at: &str,
    peer: &str,
    name: &str,
    branch: &str,
    tag: &str,
    commits: usize,
    signals: &Path,
) -> anyhow::Result<std::process::Child> {
    let setting = [
        at,
        peer,
        name,
        branch,
        tag,
        &commits.to_string(),
        &signals.to_string_lossy(),
    ]
    .join("\u{1f}");
    let child = std::process::Command::new(std::env::current_exe()?)
        .args(["committing_process", "--exact", "--ignored", "--nocapture"])
        .env(COMMIT, setting)
        .spawn()?;
    anyhow::ensure!(
        signalled(&signals.join(format!("{tag}.ready"))),
        "{tag} never got ready"
    );
    Ok(child)
}

/// The writer's turn of the file at `path`, held through a descriptor of
/// this process's own, until the returned file is closed: as a process
/// folding a long integration in holds it.
#[cfg(unix)]
fn hold_turn(path: &Path) -> anyhow::Result<std::fs::File> {
    use std::os::fd::AsRawFd;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    // SAFETY: a zeroed flock is valid; fcntl reads it and the descriptor
    // is open while `file` is.
    let mut region: libc::flock = unsafe { std::mem::zeroed() };
    region.l_type = libc::F_WRLCK as _;
    region.l_whence = libc::SEEK_SET as _;
    region.l_start = (1u64 << 62) as _;
    region.l_len = 1;
    // SAFETY: as above.
    let locked = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLKW, &region) };
    anyhow::ensure!(locked != -1, "{}", std::io::Error::last_os_error());
    Ok(file)
}

/// Two processes commit to two branches of one repository at once. A
/// head is updated in the file's writer's turn, as SQLite commits under its
/// write lock, so while a third process holds the turn their commits wait;
/// once it lets go both go on, taking turns with each other only to append
/// each head. Every commit of each lands on its own branch.
///
/// Unix only: the turn is held here with a POSIX lock. On Windows the
/// archive's own tests hold it (`archive::concurrency`, through the
/// `locks` module), and `it_commits_concurrently_to_one_branch_from_many_processes`
/// runs the commits.
#[cfg(unix)]
#[tokio::test]
async fn it_commits_to_two_branches_from_two_processes_at_once() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let at_text = directory.path().to_string_lossy().into_owned();
    let at = Directory::At(at_text.clone());
    let (peer, name) = (unique_name("peer"), unique_name("repo"));
    let signals = directory.path().join("signals");
    std::fs::create_dir(&signals)?;
    for branch in ["alpha", "beta"] {
        let (operator, branch_handle) = open!(FileSpace, at, peer, name, branch);
        commit!(
            branch_handle,
            &operator,
            tagged(&format!("{branch} first"), 0)?
        )?;
    }
    let file = directory.path().join(&name).join(FILE_NAME);
    let mut alpha = committer(&at_text, &peer, &name, "alpha", "alpha", 10, &signals)?;
    let mut beta = committer(&at_text, &peer, &name, "beta", "beta", 10, &signals)?;

    let turn = hold_turn(&file)?;
    std::fs::write(signals.join("go"), b"")?;
    std::thread::sleep(std::time::Duration::from_secs(1));
    let while_held = (
        signals.join("alpha.done").exists(),
        signals.join("beta.done").exists(),
    );
    drop(turn);
    let finished = (
        signalled(&signals.join("alpha.done")),
        signalled(&signals.join("beta.done")),
    );
    assert!(alpha.wait()?.success());
    assert!(beta.wait()?.success());
    assert_eq!(while_held, (false, false), "a head waits for the turn");
    assert_eq!(finished, (true, true), "both commit once it is let go");

    for (branch, other) in [("alpha", "beta"), ("beta", "alpha")] {
        let names = names_on(&at, &peer, &name, branch).await?;
        assert_eq!(names.len(), 10 * (1 + 1 + 10), "{branch}: every commit");
        for commit in 0..10 {
            for row in 0..10 {
                assert!(names.contains(&format!("{branch} {commit}.{row}")));
            }
        }
        assert!(
            !names.iter().any(|name| name.starts_with(other)),
            "{branch} holds none of {other}'s"
        );
    }
    Ok(())
}

/// Processes committing to one branch at once all land: a transaction that
/// loses the race for the head is staged again over the one that won, so
/// no commit's facts are lost.
#[tokio::test]
async fn it_commits_concurrently_to_one_branch_from_many_processes() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let at_text = directory.path().to_string_lossy().into_owned();
    let at = Directory::At(at_text.clone());
    let (peer, name) = (unique_name("peer"), unique_name("repo"));
    let signals = directory.path().join("signals");
    std::fs::create_dir(&signals)?;
    {
        let (operator, branch) = open!(FileSpace, at, peer, name);
        commit!(branch, &operator, tagged("first", 0)?)?;
    }
    let tags = ["one", "two", "three"];
    let mut writers = tags
        .iter()
        .map(|tag| committer(&at_text, &peer, &name, "main", tag, 5, &signals))
        .collect::<anyhow::Result<Vec<_>>>()?;
    std::fs::write(signals.join("go"), b"")?;
    for writer in &mut writers {
        assert!(writer.wait()?.success());
    }
    let names = names_on(&at, &peer, &name, "main").await?;
    assert_eq!(names.len(), 10 * (1 + tags.len() * (1 + 5)), "every commit");
    for tag in tags {
        for commit in 0..5 {
            for row in 0..10 {
                assert!(
                    names.contains(&format!("{tag} {commit}.{row}")),
                    "{tag} {commit}.{row}"
                );
            }
        }
    }
    Ok(())
}
