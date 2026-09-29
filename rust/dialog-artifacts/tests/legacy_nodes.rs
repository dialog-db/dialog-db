//! An artifact tree written in the legacy (untagged) node layout stays
//! readable: every fact scans back, including one whose value spilled.

#![cfg(not(target_arch = "wasm32"))]

use std::str::FromStr as _;

use dialog_artifacts::tree::{ArtifactTree, ArtifactTreeExt as _, spill_cache};
use dialog_artifacts::{Artifact, ArtifactSelector, Attribute, Entity, Value};
use dialog_common::Blake3Hash;
use dialog_search_tree::MemoryBlocks;
use futures_util::TryStreamExt as _;

const FIXTURE: &str = include_str!("fixtures/legacy-artifact-nodes.txt");

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex"))
        .collect()
}

fn expected(n: u32) -> Artifact {
    Artifact {
        the: Attribute::from_str(if n.is_multiple_of(2) {
            "fixture/name"
        } else {
            "fixture/bio"
        })
        .expect("attribute"),
        of: Entity::from_str(&format!("fixture:entity-{n:03}")).expect("entity"),
        is: if n == 7 {
            Value::String("x".repeat(5000))
        } else {
            Value::String(format!("value {n}"))
        },
        cause: None,
    }
}

#[tokio::test]
async fn it_reads_an_artifact_tree_written_in_the_legacy_layout() -> anyhow::Result<()> {
    let store = MemoryBlocks::new();
    let mut root = None;
    for line in FIXTURE.lines() {
        if let Some(hex) = line.strip_prefix("tree root=") {
            root = Some(Blake3Hash::try_from(unhex(hex)).expect("hash"));
        } else if let Some(hex) = line.strip_prefix("node ") {
            let node = unhex(hex);
            store.store(node.into());
        }
    }
    let spill = "x".repeat(5000).into_bytes();
    store.store(spill.into());

    let tree = ArtifactTree::from_hash(root.expect("a root line"));
    for attribute in ["fixture/name", "fixture/bio"] {
        let mut rows: Vec<Artifact> = tree
            .clone()
            .scan(
                store.clone(),
                spill_cache(),
                ArtifactSelector::new().the(Attribute::from_str(attribute)?),
            )
            .and_then(|row| async move { row.to_owned() })
            .try_collect()
            .await?;
        rows.sort_by(|left, right| left.of.as_str().cmp(right.of.as_str()));
        let want: Vec<Artifact> = (0..40u32)
            .filter(|n| (n % 2 == 0) == (attribute == "fixture/name"))
            .map(expected)
            .collect();
        assert_eq!(rows, want, "{attribute}");
    }
    Ok(())
}
