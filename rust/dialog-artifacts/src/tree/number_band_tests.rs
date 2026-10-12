//! A tree whose manifest sets `value_encoding` to 1 keys every number in
//! one band; one that leaves it at 0 keys each number type in its own.

#![allow(unexpected_cfgs)]

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use dialog_search_tree::{Cache, Manifest, MemoryBlocks, PersistentTree};
use futures_util::{TryStreamExt as _, stream};

use super::{ArtifactTree, ArtifactTreeExt as _, SpillCache};
use crate::key::artifact_index_keys;
use crate::key::varkey::parse_key_ref;
use crate::selector::Constrained;
use crate::{
    ArchiveDelta, Artifact, ArtifactSelector, Entity, Instruction, NUMBER_BAND, Pick, Relation,
    Value, ValueDataType,
};

/// A tree written under `value_encoding`, holding `facts`.
async fn tree(
    value_encoding: u32,
    facts: Vec<Artifact>,
) -> anyhow::Result<(ArtifactTree, MemoryBlocks)> {
    let store = MemoryBlocks::new();
    let manifest = Manifest {
        value_encoding,
        ..Manifest::default()
    };
    let mut tree: ArtifactTree = PersistentTree::empty_with_manifest(manifest, Cache::new());
    let mut delta = ArchiveDelta::zero();
    let instructions = facts
        .into_iter()
        .map(|fact| Instruction::Assert(fact, Pick::All));
    tree.apply(&store, &mut delta, stream::iter(instructions))
        .await?;
    delta.flush_into(&store);
    Ok((tree, store))
}

fn size() -> Relation {
    "item/size".parse().expect("parses")
}

/// One item per value, each with its size.
fn sizes(values: &[Value]) -> anyhow::Result<Vec<Artifact>> {
    values
        .iter()
        .map(|value| {
            Ok(Artifact {
                the: size(),
                of: Entity::new()?,
                is: value.clone(),
                cause: None,
            })
        })
        .collect()
}

async fn values(
    tree: &ArtifactTree,
    store: &MemoryBlocks,
    selector: ArtifactSelector<Constrained>,
) -> anyhow::Result<Vec<Value>> {
    let rows: Vec<Artifact> = tree
        .clone()
        .scan_owned(store.clone(), SpillCache::with_budget(0), selector)
        .try_collect()
        .await?;
    let mut values: Vec<Value> = rows.into_iter().map(|row| row.is).collect();
    values.sort_by(|a, b| number(a).total_cmp(&number(b)));
    Ok(values)
}

fn number(value: &Value) -> f64 {
    match value {
        Value::UnsignedInt(value) => *value as f64,
        Value::SignedInt(value) => *value as f64,
        Value::Float(value) => *value,
        _ => f64::NAN,
    }
}

fn mixed() -> Vec<Value> {
    vec![
        Value::UnsignedInt(3),
        Value::SignedInt(-2),
        Value::Float(2.5),
        Value::Float(5.0),
    ]
}

/// In the band a number does not record the type that wrote it: a whole
/// number reads back as a natural when it is not negative, as an integer
/// when it is, and every key carries the band's type byte.
#[dialog_common::test]
async fn it_keys_every_number_in_one_band() -> anyhow::Result<()> {
    let (tree, store) = tree(NUMBER_BAND, sizes(&mixed())?).await?;
    let all = values(&tree, &store, ArtifactSelector::new().the(size())).await?;
    assert_eq!(
        all,
        vec![
            Value::SignedInt(-2),
            Value::Float(2.5),
            Value::UnsignedInt(3),
            Value::UnsignedInt(5),
        ]
    );

    let manifest = Manifest {
        value_encoding: NUMBER_BAND,
        ..Manifest::default()
    };
    for fact in sizes(&mixed())? {
        for key in <[_; 3]>::from(artifact_index_keys(&fact, &manifest)) {
            let parts = parse_key_ref(key.as_ref()).expect("parses");
            assert_eq!(parts.value_type, ValueDataType::Number);
        }
    }
    Ok(())
}

/// A bound across number types scans the band's range: `>= 2.5` finds the
/// float, the natural and the whole float, not the negative integer.
#[dialog_common::test]
async fn it_scans_a_range_across_number_types_in_the_band() -> anyhow::Result<()> {
    let (tree, store) = tree(NUMBER_BAND, sizes(&mixed())?).await?;
    let selector = ArtifactSelector::new()
        .the(size())
        .is_at_least(Value::Float(2.5))
        .across_numbers();
    assert_eq!(
        values(&tree, &store, selector).await?,
        vec![
            Value::Float(2.5),
            Value::UnsignedInt(3),
            Value::UnsignedInt(5)
        ]
    );
    Ok(())
}

/// A tree keying each type in its own band cannot bound one range across
/// them, so a bound across number types scans without it and leaves the
/// comparison to the caller: every number comes back, as written.
#[dialog_common::test]
async fn it_drops_bounds_across_number_types_in_per_type_bands() -> anyhow::Result<()> {
    let (tree, store) = tree(0, sizes(&mixed())?).await?;
    let selector = ArtifactSelector::new()
        .the(size())
        .is_at_least(Value::Float(2.5))
        .across_numbers();
    assert_eq!(values(&tree, &store, selector).await?, {
        let mut all = mixed();
        all.sort_by(|a, b| number(a).total_cmp(&number(b)));
        all
    });

    // A bound of one type still bounds its own band.
    let selector = ArtifactSelector::new()
        .the(size())
        .is_at_least(Value::Float(2.5));
    assert_eq!(
        values(&tree, &store, selector).await?,
        vec![Value::Float(2.5), Value::Float(5.0)]
    );
    Ok(())
}

/// 5 and 5.0 are one number in the band, so asserting both of one item
/// holds one value; in per-type bands they are two.
#[dialog_common::test]
async fn it_holds_a_number_once_whatever_type_wrote_it() -> anyhow::Result<()> {
    let item = Entity::new()?;
    let facts = [Value::UnsignedInt(5), Value::Float(5.0)]
        .into_iter()
        .map(|is| Artifact {
            the: size(),
            of: item.clone(),
            is,
            cause: None,
        })
        .collect::<Vec<_>>();
    let (band, store) = tree(NUMBER_BAND, facts.clone()).await?;
    assert_eq!(
        values(&band, &store, ArtifactSelector::new().of(item.clone())).await?,
        vec![Value::UnsignedInt(5)]
    );
    let (per_type, store) = tree(0, facts).await?;
    assert_eq!(
        values(&per_type, &store, ArtifactSelector::new().of(item))
            .await?
            .len(),
        2
    );
    Ok(())
}
