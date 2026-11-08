//! Invariants the pick-write design claims of the tree, pinned as
//! tests. A failing test here is a place where a batch does not land
//! as the design says it does.

use crate::ArchiveDelta;
use crate::tree::{ArtifactTree, ArtifactTreeExt as _, SpillCache};
use crate::{Artifact, Changes, Entity, Instruction, Pick, Relation, Update as _, Value};
use anyhow::Result;
use dialog_search_tree::helpers::Blocks;
use dialog_search_tree::helpers::test_catalog;
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
    store: &Blocks,
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
    store: &Blocks,
    of: &Entity,
    the: &Relation,
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
    let store = Blocks::new();
    let entity = Entity::new()?;
    let the: Relation = "org/salary".parse()?;
    let salary = |value: u32| Artifact {
        the: the.clone(),
        of: entity.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
    };
    let mut tree = ArtifactTree::empty_in(test_catalog());
    apply(
        &mut tree,
        &store,
        version(0),
        vec![Instruction::Assert(salary(100), Pick::All)],
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
            Instruction::Assert(salary(200), Pick::Last),
            Instruction::Assert(salary(300), Pick::All),
            Instruction::Assert(salary(400), Pick::Last),
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
        Pick::Last,
    );
    changes.associate(
        the.clone(),
        entity.clone(),
        Value::UnsignedInt(300),
        Pick::All,
    );
    changes.associate(
        the.clone(),
        entity.clone(),
        Value::UnsignedInt(400),
        Pick::Last,
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
    let store = Blocks::new();
    let entity = Entity::new()?;
    let the: Relation = "org/salary".parse()?;
    let salary = |value: u32| Artifact {
        the: the.clone(),
        of: entity.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
    };
    let mut tree = ArtifactTree::empty_in(test_catalog());
    apply(
        &mut tree,
        &store,
        version(0),
        vec![Instruction::Assert(salary(100), Pick::All)],
    )
    .await?;
    apply(
        &mut tree,
        &store,
        version(1),
        vec![
            Instruction::Assert(salary(150), Pick::Max),
            Instruction::Assert(salary(120), Pick::Max),
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
    assert!(
        records
            .iter()
            .any(|(_, record)| record.claim().cause.contains(&version(0))),
        "the batch superseded the stored 100, and some record of it still says so, so a peer holding 100 retires it"
    );
    Ok(())
}

/// A batch carrying n identical copies of a head's instructions lands
/// the same tree, root hash and history included, as a batch carrying
/// one copy. Covers the three shapes an induced head takes after
/// succession is resolved: an `all` assert, a retract followed by an
/// `all` assert, and a lone retract.
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn copies_of_a_head_land_the_same_tree() -> Result<()> {
    let store = Blocks::new();
    let entity = Entity::new()?;
    let the: Relation = "issue/assignee".parse()?;
    let fact = |value: u32| Artifact {
        the: the.clone(),
        of: entity.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
    };
    let assert_all = |value: u32| Instruction::Assert(fact(value), Pick::All);
    let retract = |value: u32| Instruction::Retract(fact(value));
    async fn records(tree: &ArtifactTree, store: &Blocks) -> Result<usize> {
        let history = TreeHistory::new(tree.clone(), store.clone());
        Ok(history
            .select(HistorySelector::All)
            .try_collect::<Vec<_>>()
            .await?
            .len())
    }

    let mut base = ArtifactTree::empty_in(test_catalog());
    apply(&mut base, &store, version(0), vec![assert_all(1)]).await?;

    // A set head over a cell holding nothing of the value.
    let mut one = base.clone();
    let mut many = base.clone();
    apply(&mut one, &store, version(1), vec![assert_all(2)]).await?;
    apply(
        &mut many,
        &store,
        version(1),
        vec![assert_all(2), assert_all(2), assert_all(2)],
    )
    .await?;
    assert_eq!(one.root(), many.root(), "set head: three copies, one tree");
    assert_eq!(
        records(&one, &store).await?,
        records(&many, &store).await?,
        "set head: same history records"
    );

    // A `last` head resolved against the view: retract the elected
    // claim, assert the value under `all`, once vs. twice.
    let mut one = base.clone();
    let mut many = base.clone();
    apply(
        &mut one,
        &store,
        version(1),
        vec![retract(1), assert_all(3)],
    )
    .await?;
    apply(
        &mut many,
        &store,
        version(1),
        vec![retract(1), assert_all(3), retract(1), assert_all(3)],
    )
    .await?;
    assert_eq!(
        one.root(),
        many.root(),
        "succession head: two copies, one tree"
    );
    assert_eq!(held(&one, &store, &entity, &the).await?, vec![3]);
    assert_eq!(
        records(&one, &store).await?,
        records(&many, &store).await?,
        "succession head: same history records"
    );

    // A retracting head.
    let mut one = base.clone();
    let mut many = base.clone();
    apply(&mut one, &store, version(1), vec![retract(1)]).await?;
    apply(&mut many, &store, version(1), vec![retract(1), retract(1)]).await?;
    assert_eq!(
        one.root(),
        many.root(),
        "retract head: two copies, one tree"
    );
    Ok(())
}
