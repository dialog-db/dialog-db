//! Nodes written in the legacy (untagged) layout stay readable: every tree in
//! the fixture opens from its root, reads back every key, and reports the
//! manifest it was written under.

use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{
    Delta, HitchhikerTree, LEGACY_LAYOUT, Manifest, MemoryBlocks, PersistentNode, PersistentTree,
    TAGGED_LAYOUT,
};

type Tree = PersistentTree<[u8; 4], Vec<u8>>;
type Node = PersistentNode<[u8; 4], Vec<u8>>;

const FIXTURE: &str = include_str!("fixtures/legacy-nodes.txt");

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex"))
        .collect()
}

struct Fixture {
    name: String,
    root: Blake3Hash,
    keys: u32,
    buffered: bool,
    manifest: Manifest,
    nodes: Vec<Vec<u8>>,
}

async fn load(fixture: &Fixture) -> anyhow::Result<MemoryBlocks> {
    let storage = MemoryBlocks::new();
    for node in &fixture.nodes {
        storage.store(Buffer::from(node.clone()));
    }
    Ok(storage)
}

fn field<'a>(line: &'a str, name: &str) -> &'a str {
    line.split_whitespace()
        .find_map(|part| part.strip_prefix(&format!("{name}=")))
        .expect("field present")
}

fn fixtures() -> Vec<Fixture> {
    let mut out: Vec<Fixture> = Vec::new();
    for line in FIXTURE.lines() {
        if let Some(rest) = line.strip_prefix("tree ") {
            let root = Blake3Hash::try_from(unhex(field(line, "root"))).expect("hash");
            out.push(Fixture {
                name: rest.split_whitespace().next().expect("name").to_string(),
                root,
                keys: field(line, "keys").parse().expect("keys"),
                buffered: field(line, "buffered") == "true",
                manifest: Manifest {
                    fanout_n: field(line, "fanout_n").parse().expect("fanout_n"),
                    max_segment: field(line, "max_segment").parse().expect("max_segment"),
                    frame_ceiling_factor: field(line, "frame_ceiling_factor")
                        .parse()
                        .expect("frame_ceiling_factor"),
                    inline_n: field(line, "inline_n").parse().expect("inline_n"),
                    ..Manifest::default()
                },
                nodes: Vec::new(),
            });
        } else if let Some(hex) = line.strip_prefix("node ") {
            out.last_mut().expect("a tree first").nodes.push(unhex(hex));
        }
    }
    out
}

#[tokio::test]
async fn it_reads_trees_written_in_the_legacy_layout() -> anyhow::Result<()> {
    let fixtures = fixtures();
    assert_eq!(fixtures.len(), 4);
    for fixture in fixtures {
        let storage = load(&fixture).await?;
        for node in &fixture.nodes {
            assert_eq!(
                Node::try_from(Buffer::from(node.clone()))?.layout_version(),
                LEGACY_LAYOUT,
                "{}: a legacy node reads as one",
                fixture.name
            );
        }
        let tree = Tree::from_hash(fixture.root.clone());
        assert_eq!(
            tree.manifest(&storage).await?,
            fixture.manifest,
            "{}: the manifest the tree was written under",
            fixture.name
        );
        for key in &keys(&fixture) {
            assert_eq!(
                tree.get(&key.to_be_bytes(), &storage).await?,
                Some(key.to_be_bytes().to_vec()),
                "{}: key {key}",
                fixture.name
            );
        }
        assert_eq!(
            tree.get(&u32::MAX.to_be_bytes(), &storage).await?,
            None,
            "{}: an absent key",
            fixture.name
        );
    }
    Ok(())
}

/// The keys a fixture's tree holds.
fn keys(fixture: &Fixture) -> Vec<u32> {
    let mut keys: Vec<u32> = (0..fixture.keys).collect();
    if fixture.buffered {
        keys.extend((300..340).step_by(2));
    }
    keys
}

/// Edits to a legacy tree, canonical or buffered, write only tagged nodes
/// under the tree's own manifest, and the result reads every key: the edited
/// path is tagged and the untouched legacy subtrees it still links to read as
/// before.
#[tokio::test]
async fn it_edits_trees_written_in_the_legacy_layout() -> anyhow::Result<()> {
    for fixture in fixtures() {
        for buffered in [false, true] {
            let storage = load(&fixture).await?;
            let tree = Tree::from_hash(fixture.root.clone());
            let added = [1000u32, 1001, 1002];
            let mut delta = Delta::zero();
            let root = if buffered {
                let mut edit = HitchhikerTree::open(&tree);
                for key in added {
                    edit = edit
                        .insert(key.to_be_bytes(), key.to_be_bytes().to_vec(), &storage)
                        .await?;
                }
                edit.persist(&mut delta)?
            } else {
                let mut edit = tree.edit();
                for key in added {
                    edit = edit
                        .insert(key.to_be_bytes(), key.to_be_bytes().to_vec(), &storage)
                        .await?;
                }
                edit.persist(&mut delta)?.root().clone()
            };
            for (_hash, buffer) in delta.flush() {
                assert_eq!(
                    Node::try_from(buffer.clone())?.layout_version(),
                    TAGGED_LAYOUT,
                    "{}: an edit writes tagged nodes",
                    fixture.name
                );
                storage.store(buffer);
            }

            let edited = Tree::from_hash(root);
            assert_eq!(
                edited.manifest(&storage).await?,
                fixture.manifest,
                "{}: the edit keeps the tree's manifest",
                fixture.name
            );
            let mut expected = keys(&fixture);
            expected.extend(added);
            for key in &expected {
                assert_eq!(
                    edited.get(&key.to_be_bytes(), &storage).await?,
                    Some(key.to_be_bytes().to_vec()),
                    "{} (buffered: {buffered}): key {key}",
                    fixture.name
                );
            }
        }
    }
    Ok(())
}
