//! Invariants the policy-write design claims of the tree, pinned as
//! tests. A failing test here is a place where a batch does not land
//! as the design says it does.

use crate::ArchiveDelta;
use crate::tree::{ArtifactTree, ArtifactTreeExt as _, SpillCache};
use crate::{Artifact, Attribute, Changes, Entity, Instruction, Policy, Update as _, Value};
use anyhow::Result;
use dialog_search_tree::MemoryBlocks;
use futures_util::{TryStreamExt as _, stream};

use super::{Edition, HistorySelector, Origin, TreeHistory, Version};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

fn version(edition: u64) -> Version {
    Version::new(Origin::from([7u8; 32]), Edition::new(edition))
}

async fn apply(
    tree: &mut ArtifactTree,
    store: &MemoryBlocks,
    version: Version,
    instructions: Vec<Instruction>,
) -> Result<bool> {
    let mut delta = ArchiveDelta::zero();
    let changed = tree
        .apply_versioned(store, &mut delta, Some(version), stream::iter(instructions))
        .await?;
    delta.flush_into(store);
    Ok(changed)
}

async fn held(
    tree: &ArtifactTree,
    store: &MemoryBlocks,
    of: &Entity,
    the: &Attribute,
) -> Result<Vec<u128>> {
    let selector = crate::ArtifactSelector::new()
        .of(of.clone())
        .the(the.clone());
    let rows: Vec<Artifact> = tree
        .clone()
        .scan_owned(store.clone(), SpillCache::with_budget(0), selector)
        .try_collect()
        .await?;
    let mut values: Vec<u128> = rows
        .into_iter()
        .filter_map(|artifact| match artifact.is {
            Value::UnsignedInt(value) => Some(value),
            _ => None,
        })
        .collect();
    values.sort();
    Ok(values)
}

/// A batch "replays a cell's writes in the order the transaction made
/// them": `last` 200 succeeds the stored 100, `all` 300 stands beside
/// it, `last` 400 succeeds the newest of the two. The line comes to
/// `{200, 400}` whichever way the three writes were recorded. Recorded
/// through `Changes::associate`, the earlier `last` write is dropped
/// and the later one is moved past the `all` write, so the batch reads
/// `[all 300, last 400]` and the stored 100 is never succeeded.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn a_batch_lands_a_cells_writes_in_the_order_they_were_recorded() -> Result<()> {
    let store = MemoryBlocks::new();
    let entity = Entity::new()?;
    let the: Attribute = "org/salary".parse()?;
    let salary = |value: u32| Artifact {
        the: the.clone(),
        of: entity.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
    };
    let mut tree = ArtifactTree::empty();
    apply(
        &mut tree,
        &store,
        version(0),
        vec![Instruction::Assert(salary(100), Policy::All)],
    )
    .await?;

    // The same three writes, in order, as instructions the tree
    // replays one by one.
    let mut by_instruction = tree.clone();
    apply(
        &mut by_instruction,
        &store,
        version(1),
        vec![
            Instruction::Assert(salary(200), Policy::Last),
            Instruction::Assert(salary(300), Policy::All),
            Instruction::Assert(salary(400), Policy::Last),
        ],
    )
    .await?;
    let expected = held(&by_instruction, &store, &entity, &the).await?;
    assert!(
        !expected.contains(&100),
        "the first `last` write succeeded the stored claim: {expected:?}"
    );

    // Recorded through a `Changes` batch, as a statement or an
    // integrated batch records them.
    let mut changes = Changes::new();
    changes.associate(
        the.clone(),
        entity.clone(),
        Value::UnsignedInt(200),
        Policy::Last,
    );
    changes.associate(
        the.clone(),
        entity.clone(),
        Value::UnsignedInt(300),
        Policy::All,
    );
    changes.associate(
        the.clone(),
        entity.clone(),
        Value::UnsignedInt(400),
        Policy::Last,
    );
    let mut by_batch = tree.clone();
    let mut delta = ArchiveDelta::zero();
    by_batch
        .apply_versioned(&store, &mut delta, Some(version(1)), changes.into_stream())
        .await?;
    delta.flush_into(&store);
    assert_eq!(
        held(&by_batch, &store, &entity, &the).await?,
        expected,
        "a batch lands the same claims as its writes replayed in order"
    );
    Ok(())
}

/// A claim a later write of the same batch succeeds never stood: the
/// history records a retraction for it (as a same-batch assert and
/// retract collapse), or records nothing. The tree erases the elected
/// claim's index keys and buffers no retraction record, so the history
/// keeps an assertion of a value no index ever exposed, with nothing
/// saying it was withdrawn.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn a_claim_succeeded_within_its_own_batch_leaves_no_standing_record() -> Result<()> {
    let store = MemoryBlocks::new();
    let entity = Entity::new()?;
    let the: Attribute = "org/salary".parse()?;
    let salary = |value: u32| Artifact {
        the: the.clone(),
        of: entity.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
    };
    let mut tree = ArtifactTree::empty();
    apply(
        &mut tree,
        &store,
        version(0),
        vec![Instruction::Assert(salary(100), Policy::All)],
    )
    .await?;
    apply(
        &mut tree,
        &store,
        version(1),
        vec![
            Instruction::Assert(salary(150), Policy::Max),
            Instruction::Assert(salary(120), Policy::Max),
        ],
    )
    .await?;
    assert_eq!(held(&tree, &store, &entity, &the).await?, vec![120]);

    let history = TreeHistory::new(tree.clone(), store.clone());
    let records = history
        .select(HistorySelector::All)
        .try_collect::<Vec<_>>()
        .await?;
    let standing: Vec<u128> = records
        .iter()
        .filter(|(_, record)| record.is_assertion())
        .filter_map(|(_, record)| match record.claim().is {
            Value::UnsignedInt(value) => Some(value),
            _ => None,
        })
        .collect();
    let retracted: Vec<u128> = records
        .iter()
        .filter(|(_, record)| !record.is_assertion())
        .filter_map(|(_, record)| match record.claim().is {
            Value::UnsignedInt(value) => Some(value),
            _ => None,
        })
        .collect();
    assert!(
        !standing.contains(&150) || retracted.contains(&150),
        "150 was succeeded by 120 in the same batch; it is retracted or unrecorded, not standing: asserted {standing:?}, retracted {retracted:?}"
    );
    Ok(())
}
