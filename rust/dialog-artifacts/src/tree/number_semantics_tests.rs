//! Number semantics in a tree that keys every number in one band: what a
//! write reads back as, how numbers order across types, what an exact
//! selector and a bound of each number type find, and what a rewrite
//! between the band and the per-type bands keeps.

#![allow(unexpected_cfgs)]

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use dialog_search_tree::{Cache, Manifest, MemoryBlocks, PersistentTree};
use futures_util::{TryStreamExt as _, stream};

use super::rekey::rekey_tree;
use super::{ArtifactTree, ArtifactTreeExt as _, SpillCache};
use crate::selector::Constrained;
use crate::{
    ArchiveDelta, Artifact, ArtifactSelector, Entity, Instruction, NUMBER_BAND, Pick, Relation,
    Value, encode_key_value_owned,
};

const TWO_POW_53: u128 = 1 << 53;
const TWO_POW_64: u128 = 1 << 64;
const TEN_POW_30: u128 = 1_000_000_000_000_000_000_000_000_000_000;

fn natural(n: u128) -> Value {
    Value::UnsignedInt(n)
}

fn integer(n: i128) -> Value {
    Value::SignedInt(n)
}

fn float(f: f64) -> Value {
    Value::Float(f)
}

/// A tree written under `value_encoding`, holding `instructions`.
async fn tree(
    value_encoding: u32,
    instructions: Vec<Instruction>,
) -> anyhow::Result<(ArtifactTree, MemoryBlocks)> {
    let store = MemoryBlocks::new();
    let manifest = Manifest {
        value_encoding,
        ..Manifest::default()
    };
    let mut tree: ArtifactTree = PersistentTree::empty_with_manifest(manifest, Cache::new());
    let mut delta = ArchiveDelta::zero();
    tree.apply(&store, &mut delta, stream::iter(instructions))
        .await?;
    delta.flush_into(&store);
    Ok((tree, store))
}

fn size() -> Relation {
    "item/size".parse().expect("parses")
}

/// One item per value, each with its size.
fn sizes(values: &[Value]) -> anyhow::Result<Vec<Instruction>> {
    values
        .iter()
        .map(|value| {
            Ok(Instruction::Assert(
                Artifact {
                    the: size(),
                    of: Entity::new()?,
                    is: value.clone(),
                    cause: None,
                },
                Pick::All,
            ))
        })
        .collect()
}

/// The sizes `selector` finds, in the order the scan yields them: a scan
/// bounded on the value walks the value ordering, so that is key order.
async fn scan(
    tree: &ArtifactTree,
    store: &MemoryBlocks,
    selector: ArtifactSelector<Constrained>,
) -> anyhow::Result<Vec<Value>> {
    let rows: Vec<Artifact> = tree
        .clone()
        .scan_owned(store.clone(), SpillCache::with_budget(0), selector)
        .try_collect()
        .await?;
    Ok(rows.into_iter().map(|row| row.is).collect())
}

/// The band key of `value`: the identity the band gives a number.
fn key(value: &Value) -> Vec<u8> {
    encode_key_value_owned(value, NUMBER_BAND)
}

/// `values` in band order.
fn in_band_order(mut values: Vec<Value>) -> Vec<Value> {
    values.sort_by_key(key);
    values
}

fn the_size() -> ArtifactSelector<Constrained> {
    ArtifactSelector::new().the(size())
}

/// Every edge number, written as its narrowest type, in numeric order.
fn edges() -> Vec<Value> {
    vec![
        float(f64::NEG_INFINITY),
        integer(i128::MIN),
        integer(-(TEN_POW_30 as i128)),
        integer(i64::MIN as i128),
        integer(-3),
        float(-2.5),
        integer(-1),
        float(-0.5),
        natural(0),
        float(5e-324),
        float(f64::MIN_POSITIVE),
        float(0.1),
        float(0.5),
        natural(1),
        float(2.5),
        natural(TWO_POW_53 + 1),
        natural(u64::MAX as u128),
        natural(TWO_POW_64),
        natural(TEN_POW_30),
        natural(u128::MAX),
        float(1e39),
        float(f64::MAX),
        float(f64::INFINITY),
    ]
}

/// S2: a number written under any type reads back as the narrowest type
/// that holds it, exactly: no float detour for `u128::MAX`, `i128::MIN` or
/// `2^53 + 1`, bit-exact fractions, infinities and whole floats past
/// `u128` as floats, and a whole float as the natural or integer it
/// prints as.
#[dialog_common::test]
async fn it_reads_back_the_narrowest_type_exactly() -> anyhow::Result<()> {
    let written = vec![
        natural(0),
        integer(0),
        float(0.0),
        float(-0.0),
        natural(1),
        integer(1),
        float(1.0),
        integer(-1),
        float(-1.0),
        float(2.5),
        float(-2.5),
        float(0.5),
        float(0.1),
        float(5e-324),
        float(f64::MIN_POSITIVE),
        float(f64::MAX),
        float(1e39),
        float(1e30),
        float(-1e30),
        float(f64::INFINITY),
        float(f64::NEG_INFINITY),
        natural(u64::MAX as u128),
        natural(TWO_POW_64),
        natural(TWO_POW_53 + 1),
        integer(TWO_POW_53 as i128 + 1),
        natural(u128::MAX),
        integer(i128::MIN),
        integer(i64::MIN as i128),
        integer(i128::MAX),
    ];
    let expected = in_band_order(vec![
        natural(0),
        natural(0),
        natural(0),
        natural(0),
        natural(1),
        natural(1),
        natural(1),
        integer(-1),
        integer(-1),
        float(2.5),
        float(-2.5),
        float(0.5),
        float(0.1),
        float(5e-324),
        float(f64::MIN_POSITIVE),
        float(f64::MAX),
        float(1e39),
        natural(TEN_POW_30),
        integer(-(TEN_POW_30 as i128)),
        float(f64::INFINITY),
        float(f64::NEG_INFINITY),
        natural(u64::MAX as u128),
        natural(TWO_POW_64),
        natural(TWO_POW_53 + 1),
        natural(TWO_POW_53 + 1),
        natural(u128::MAX),
        integer(i128::MIN),
        integer(i64::MIN as i128),
        natural(i128::MAX as u128),
    ]);
    let (tree, store) = tree(NUMBER_BAND, sizes(&written)?).await?;
    let read = in_band_order(scan(&tree, &store, the_size()).await?);
    assert_eq!(read, expected);
    Ok(())
}

/// S3: a scan over the whole band yields every number in numeric order,
/// across natural, integer and float, each as its narrowest type: a float
/// bound sees every number as it is, and a bound across number types
/// compares every number as it is.
#[dialog_common::test]
async fn it_orders_numbers_across_types() -> anyhow::Result<()> {
    let mut shuffled = edges();
    shuffled.reverse();
    shuffled.swap(2, 11);
    shuffled.swap(5, 17);
    let (tree, store) = tree(NUMBER_BAND, sizes(&shuffled)?).await?;
    assert_eq!(
        scan(
            &tree,
            &store,
            the_size().is_at_least(float(f64::NEG_INFINITY))
        )
        .await?,
        edges(),
        "a float bound from -inf walks every number in order"
    );
    assert_eq!(
        scan(&tree, &store, the_size().is_at_most(float(f64::INFINITY))).await?,
        edges(),
        "a float bound to +inf walks every number in order"
    );
    assert_eq!(
        scan(
            &tree,
            &store,
            the_size().is_at_least(integer(i128::MIN)).across_numbers()
        )
        .await?,
        edges()[1..].to_vec(),
        "across number types an integer bound compares every number as it is"
    );
    assert_eq!(
        in_band_order(scan(&tree, &store, the_size()).await?),
        edges(),
        "the band keys order as the numbers do"
    );
    Ok(())
}

/// The numbers the lens tests read: one item per value, fractions on both
/// sides of each whole bound, both signs and both infinities.
fn lens_values() -> Vec<Value> {
    vec![
        float(f64::NEG_INFINITY),
        integer(i128::MIN),
        integer(-(TEN_POW_30 as i128)),
        float(-3.5),
        integer(-3),
        float(-2.5),
        float(-0.5),
        natural(0),
        float(0.5),
        float(1.5),
        natural(2),
        float(2.5),
        natural(3),
        natural(TWO_POW_64),
        natural(TEN_POW_30),
        natural(u128::MAX),
        float(1e39),
        float(f64::INFINITY),
    ]
}

/// `lens_values` from `from` to `to`, both included, by position.
fn slice(from: usize, to: usize) -> Vec<Value> {
    lens_values()[from..=to].to_vec()
}

/// S4: in the band an exact number is the range of that one number read
/// through the lens of its type. A float is the number as it is, so
/// `is(3.0)` finds the one number 3 whatever type wrote it, `is(2.5)`
/// finds 2.5 alone and `is(-41)` never finds 41. An integer or a natural
/// is a lens, so `is(2)` as a natural finds 2 and 2.5, which it reads as
/// 2, and `is(-3)` as an integer finds -3 and -2.5.
#[dialog_common::test]
async fn it_finds_an_exact_number_through_the_lens_of_its_type() -> anyhow::Result<()> {
    let (tree, store) = tree(NUMBER_BAND, sizes(&lens_values())?).await?;
    let is = |value: Value| the_size().is(value);
    for three in [float(3.0), integer(3), natural(3)] {
        assert_eq!(
            scan(&tree, &store, is(three.clone())).await?,
            vec![natural(3)],
            "{three:?} finds the one 3"
        );
    }
    assert_eq!(scan(&tree, &store, is(float(2.5))).await?, vec![float(2.5)]);
    assert_eq!(
        scan(&tree, &store, is(float(2.0))).await?,
        vec![natural(2)],
        "a float is the number as it is: 2.0 is not 2.5"
    );
    assert_eq!(
        scan(&tree, &store, is(natural(2))).await?,
        vec![natural(2), float(2.5)],
        "a natural 2 reads 2.5 as 2"
    );
    assert_eq!(
        scan(&tree, &store, is(integer(2))).await?,
        vec![natural(2), float(2.5)],
        "an integer 2 reads 2.5 as 2"
    );
    assert_eq!(
        scan(&tree, &store, is(integer(-3))).await?,
        vec![integer(-3), float(-2.5)],
        "an integer -3 reads -2.5 as -3"
    );
    assert_eq!(
        scan(&tree, &store, is(float(-3.0))).await?,
        vec![integer(-3)]
    );
    assert_eq!(scan(&tree, &store, is(integer(-41))).await?, vec![]);
    assert_eq!(scan(&tree, &store, is(natural(41))).await?, vec![]);
    assert_eq!(
        scan(&tree, &store, is(float(-0.0))).await?,
        vec![natural(0)]
    );
    assert_eq!(
        scan(&tree, &store, is(natural(u128::MAX))).await?,
        vec![natural(u128::MAX)]
    );
    assert_eq!(
        scan(&tree, &store, is(integer(i128::MIN))).await?,
        vec![integer(i128::MIN)]
    );
    assert_eq!(
        scan(&tree, &store, is(float(1e30))).await?,
        vec![natural(TEN_POW_30)],
        "a float is the decimal it prints as"
    );
    assert_eq!(
        scan(&tree, &store, is(float(f64::INFINITY))).await?,
        vec![float(f64::INFINITY)]
    );
    assert_eq!(
        scan(&tree, &store, is(float(f64::NEG_INFINITY))).await?,
        vec![float(f64::NEG_INFINITY)]
    );
    Ok(())
}

/// S5: a natural bound reads every number through the natural lens: it
/// sees a number that is not negative and that a natural holds, floored,
/// and nothing else, so no negative number, no number past `u128::MAX`
/// and no infinity. `>= 2` and `<= 2` both admit 2.5, which the lens reads
/// as 2; `> 2` and `< 2` both leave it out.
#[dialog_common::test]
async fn it_reads_numbers_through_a_natural_bound() -> anyhow::Result<()> {
    let (tree, store) = tree(NUMBER_BAND, sizes(&lens_values())?).await?;
    let cases: Vec<(ArtifactSelector<Constrained>, Vec<Value>, &str)> = vec![
        (the_size().is_at_least(natural(2)), slice(10, 15), ">= 2"),
        (the_size().is_greater_than(natural(2)), slice(12, 15), "> 2"),
        (the_size().is_at_most(natural(2)), slice(7, 11), "<= 2"),
        (the_size().is_less_than(natural(2)), slice(7, 9), "< 2"),
        (the_size().is_at_least(natural(0)), slice(7, 15), ">= 0"),
        (the_size().is_greater_than(natural(0)), slice(9, 15), "> 0"),
        (the_size().is_at_most(natural(0)), slice(7, 8), "<= 0"),
        (the_size().is_less_than(natural(0)), vec![], "< 0"),
        (
            the_size().is_between(natural(1), natural(2)),
            slice(9, 11),
            "1..=2",
        ),
        (
            the_size().is_at_least(natural(TWO_POW_64)),
            slice(13, 15),
            ">= 2^64",
        ),
        (
            the_size().is_at_most(natural(u128::MAX)),
            slice(7, 15),
            "<= u128::MAX",
        ),
        (
            the_size().is_greater_than(natural(u128::MAX)),
            vec![],
            "> u128::MAX sees no number past it",
        ),
    ];
    for (selector, expected, claim) in cases {
        assert_eq!(scan(&tree, &store, selector).await?, expected, "{claim}");
    }
    Ok(())
}

/// S5: an integer bound reads every number through the integer lens: it
/// sees any number an integer holds, floored, so `>= -3` admits -3 and
/// -2.5 and leaves -3.5 out, `<= -3` admits -2.5, and `< -3` leaves both
/// -3 and -2.5 out. It does not see `u128::MAX`, a whole float past
/// `i128` or an infinity.
#[dialog_common::test]
async fn it_reads_numbers_through_an_integer_bound() -> anyhow::Result<()> {
    let (tree, store) = tree(NUMBER_BAND, sizes(&lens_values())?).await?;
    let cases: Vec<(ArtifactSelector<Constrained>, Vec<Value>, &str)> = vec![
        (the_size().is_at_least(integer(-3)), slice(4, 14), ">= -3"),
        (
            the_size().is_greater_than(integer(-3)),
            slice(6, 14),
            "> -3",
        ),
        (the_size().is_at_most(integer(-3)), slice(1, 5), "<= -3"),
        (the_size().is_less_than(integer(-3)), slice(1, 3), "< -3"),
        (the_size().is_at_least(integer(0)), slice(7, 14), ">= 0"),
        (the_size().is_at_most(integer(0)), slice(1, 8), "<= 0"),
        (the_size().is_less_than(integer(0)), slice(1, 6), "< 0"),
        (the_size().is_greater_than(integer(0)), slice(9, 14), "> 0"),
        (
            the_size().is_between(integer(-3), integer(2)),
            slice(4, 11),
            "-3..=2",
        ),
        (
            the_size().is_between(integer(-1), integer(0)),
            slice(6, 8),
            "-1..=0 admits -0.5 and 0.5",
        ),
        (
            the_size().is_at_least(integer(i128::MIN)),
            slice(1, 14),
            ">= i128::MIN",
        ),
        (
            the_size().is_at_most(integer(i128::MAX)),
            slice(1, 14),
            "<= i128::MAX",
        ),
        (
            the_size().is_less_than(integer(i128::MIN)),
            vec![],
            "< i128::MIN sees no number below it",
        ),
    ];
    for (selector, expected, claim) in cases {
        assert_eq!(scan(&tree, &store, selector).await?, expected, "{claim}");
    }
    Ok(())
}

/// S5: a float bound sees every number as it is, infinities included.
#[dialog_common::test]
async fn it_reads_numbers_as_they_are_through_a_float_bound() -> anyhow::Result<()> {
    let (tree, store) = tree(NUMBER_BAND, sizes(&lens_values())?).await?;
    let all = lens_values();
    let cases: Vec<(ArtifactSelector<Constrained>, Vec<Value>, &str)> = vec![
        (the_size().is_at_least(float(2.5)), slice(11, 17), ">= 2.5"),
        (
            the_size().is_greater_than(float(2.5)),
            slice(12, 17),
            "> 2.5",
        ),
        (the_size().is_at_most(float(2.5)), slice(0, 11), "<= 2.5"),
        (the_size().is_less_than(float(2.5)), slice(0, 10), "< 2.5"),
        (the_size().is_at_least(float(2.0)), slice(10, 17), ">= 2.0"),
        (
            the_size().is_greater_than(float(2.0)),
            slice(11, 17),
            "> 2.0",
        ),
        (the_size().is_at_most(float(-2.5)), slice(0, 5), "<= -2.5"),
        (the_size().is_less_than(float(-2.5)), slice(0, 4), "< -2.5"),
        (the_size().is_at_least(float(-0.0)), slice(7, 17), ">= -0.0"),
        (the_size().is_less_than(float(0.0)), slice(0, 6), "< 0.0"),
        (
            the_size().is_between(float(-2.5), float(2.5)),
            slice(5, 11),
            "-2.5..=2.5",
        ),
        (
            the_size().is_at_least(float(f64::NEG_INFINITY)),
            all.clone(),
            ">= -inf",
        ),
        (
            the_size().is_greater_than(float(f64::NEG_INFINITY)),
            slice(1, 17),
            "> -inf",
        ),
        (
            the_size().is_at_most(float(f64::INFINITY)),
            all.clone(),
            "<= inf",
        ),
        (
            the_size().is_less_than(float(f64::INFINITY)),
            slice(0, 16),
            "< inf",
        ),
        (
            the_size().is_at_least(float(f64::INFINITY)),
            slice(17, 17),
            ">= inf",
        ),
        (
            the_size().is_greater_than(float(f64::MAX)),
            slice(17, 17),
            "> f64::MAX is +inf alone",
        ),
        (
            the_size().is_at_least(float(1e39)),
            slice(16, 17),
            ">= 1e39",
        ),
    ];
    for (selector, expected, claim) in cases {
        assert_eq!(scan(&tree, &store, selector).await?, expected, "{claim}");
    }
    Ok(())
}

/// S5: `across_numbers` compares every number as it is whatever the
/// bound's type: a natural `<= 2` then leaves 2.5 out and admits every
/// negative number and -inf, an integer `>= -3` admits `u128::MAX`, `1e39`
/// and +inf, and a natural `> 2` admits 2.5.
#[dialog_common::test]
async fn it_compares_numbers_as_they_are_across_number_types() -> anyhow::Result<()> {
    let (tree, store) = tree(NUMBER_BAND, sizes(&lens_values())?).await?;
    let cases: Vec<(ArtifactSelector<Constrained>, Vec<Value>, &str)> = vec![
        (
            the_size().is_at_most(natural(2)).across_numbers(),
            slice(0, 10),
            "<= 2",
        ),
        (
            the_size().is_less_than(natural(2)).across_numbers(),
            slice(0, 9),
            "< 2",
        ),
        (
            the_size().is_at_least(natural(2)).across_numbers(),
            slice(10, 17),
            ">= 2",
        ),
        (
            the_size().is_greater_than(natural(2)).across_numbers(),
            slice(11, 17),
            "> 2",
        ),
        (
            the_size().is_at_least(integer(-3)).across_numbers(),
            slice(4, 17),
            ">= -3",
        ),
        (
            the_size().is_greater_than(integer(-3)).across_numbers(),
            slice(5, 17),
            "> -3",
        ),
        (
            the_size().is_at_most(integer(-3)).across_numbers(),
            slice(0, 4),
            "<= -3",
        ),
        (
            the_size().is_less_than(integer(-3)).across_numbers(),
            slice(0, 3),
            "< -3",
        ),
        (
            the_size()
                .is_between(integer(-3), natural(2))
                .across_numbers(),
            slice(4, 10),
            "-3..=2",
        ),
        (
            the_size().is_at_least(float(2.5)).across_numbers(),
            slice(11, 17),
            ">= 2.5 is the same with or without",
        ),
    ];
    for (selector, expected, claim) in cases {
        assert_eq!(scan(&tree, &store, selector).await?, expected, "{claim}");
    }
    Ok(())
}

/// S5: a range whose two edges are of different number types reads each
/// edge through its own lens: between the integer -1 and the natural 1,
/// -0.5 is -1 to the lower edge but unseen by the upper, so it is out,
/// and 1.5 is 1 to the upper edge, so it is in.
#[dialog_common::test]
async fn it_reads_each_edge_of_a_mixed_range_through_its_own_lens() -> anyhow::Result<()> {
    let (tree, store) = tree(NUMBER_BAND, sizes(&lens_values())?).await?;
    assert_eq!(
        scan(
            &tree,
            &store,
            the_size().is_between(integer(-1), natural(1))
        )
        .await?,
        slice(7, 9),
        "an integer lower edge and a natural upper edge"
    );
    assert_eq!(
        scan(
            &tree,
            &store,
            the_size().is_between(integer(-1), float(1.0))
        )
        .await?,
        slice(6, 8),
        "an integer lower edge admits -0.5 as -1; a float upper edge leaves 1.5 out"
    );
    assert_eq!(
        scan(
            &tree,
            &store,
            the_size().is_between(float(-1.0), integer(1))
        )
        .await?,
        slice(6, 9),
        "a float lower edge leaves -3.5 out; an integer upper edge admits 1.5 as 1"
    );
    Ok(())
}

/// Items holding every edge value under every number type that holds it
/// exactly, each item labelled, so the tree holds keys a rewrite moves
/// next to keys it leaves alone. A float form is written only for a whole
/// number that is its own shortest decimal.
fn every_type_items() -> anyhow::Result<Vec<Instruction>> {
    let mut values = Vec::new();
    for n in [0u128, 1, 2, 255, 256, TWO_POW_53] {
        values.push(natural(n));
        values.push(integer(n as i128));
        values.push(float(n as f64));
        if n != 0 {
            values.push(integer(-(n as i128)));
            values.push(float(-(n as f64)));
        }
    }
    for n in [
        TWO_POW_53 + 1,
        u64::MAX as u128,
        TWO_POW_64,
        TEN_POW_30,
        i128::MAX as u128,
    ] {
        values.push(natural(n));
        values.push(integer(n as i128));
        values.push(integer(-(n as i128)));
    }
    values.push(natural(i128::MAX as u128 + 1));
    values.push(natural(u128::MAX));
    values.push(integer(i128::MIN));
    values.push(integer(i64::MIN as i128));
    values.extend(
        [
            2.5,
            -2.5,
            0.5,
            -0.5,
            0.1,
            -0.0,
            1e30,
            -1e30,
            1e39,
            -1e39,
            f64::MAX,
            -f64::MAX,
            f64::MIN_POSITIVE,
            5e-324,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ]
        .map(float),
    );
    let mut instructions = Vec::new();
    for value in values {
        let of = Entity::new()?;
        instructions.push(Instruction::Assert(
            Artifact {
                the: size(),
                of: of.clone(),
                is: value.clone(),
                cause: None,
            },
            Pick::All,
        ));
        instructions.push(Instruction::Assert(
            Artifact {
                the: "item/label".parse()?,
                of,
                is: Value::String(format!("{value:?}")),
                cause: None,
            },
            Pick::All,
        ));
    }
    Ok(instructions)
}

/// `tree`, rewritten under `value_encoding`.
async fn rekeyed(
    tree: &ArtifactTree,
    store: &MemoryBlocks,
    value_encoding: u32,
) -> anyhow::Result<ArtifactTree> {
    let manifest = Manifest {
        value_encoding,
        ..tree.manifest(store).await?
    };
    let mut delta = ArchiveDelta::zero();
    let rewritten = rekey_tree(tree, store, &mut delta, manifest, Cache::new()).await?;
    delta.flush_into(store);
    Ok(rewritten)
}

/// The band keys of every size in `tree`, sorted: the numbers it holds, by
/// the identity the band gives them.
async fn band_keys(tree: &ArtifactTree, store: &MemoryBlocks) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut keys: Vec<Vec<u8>> = scan(tree, store, the_size())
        .await?
        .iter()
        .map(key)
        .collect();
    keys.sort();
    Ok(keys)
}

/// S7: a band tree holding every edge value under every number type,
/// rewritten under the per-type bands and back, is the tree it started
/// from, root for root, and both rewrites are deterministic. Under the
/// per-type bands the same numbers read back.
#[dialog_common::test]
async fn it_rewrites_every_number_type_losslessly_from_the_band() -> anyhow::Result<()> {
    let (original, store) = tree(NUMBER_BAND, every_type_items()?).await?;
    assert_eq!(original.manifest(&store).await?.value_encoding, NUMBER_BAND);
    let numbers = band_keys(&original, &store).await?;

    let legacy = rekeyed(&original, &store, 0).await?;
    assert_eq!(legacy.manifest(&store).await?.value_encoding, 0);
    assert_ne!(legacy.root(), original.root());
    assert_eq!(
        band_keys(&legacy, &store).await?,
        numbers,
        "the per-type tree holds the same numbers"
    );
    assert_eq!(
        rekeyed(&original, &store, 0).await?.root(),
        legacy.root(),
        "the rewrite to per-type bands is deterministic"
    );

    let restored = rekeyed(&legacy, &store, NUMBER_BAND).await?;
    assert_eq!(restored.root(), original.root(), "the round trip is exact");
    assert_eq!(
        rekeyed(&legacy, &store, NUMBER_BAND).await?.root(),
        restored.root(),
        "the rewrite to the band is deterministic"
    );
    Ok(())
}

/// S7: a per-type tree whose every number is already held by its
/// narrowest type rewrites to the band and back root for root: nothing
/// but the key layout changes.
#[dialog_common::test]
async fn it_rewrites_narrowest_typed_per_type_bands_losslessly() -> anyhow::Result<()> {
    let (original, store) = tree(0, sizes(&edges())?).await?;
    let band = rekeyed(&original, &store, NUMBER_BAND).await?;
    assert_eq!(
        scan(
            &band,
            &store,
            the_size().is_at_least(float(f64::NEG_INFINITY))
        )
        .await?,
        edges(),
        "the band reads every number in order"
    );
    let back = rekeyed(&band, &store, 0).await?;
    assert_eq!(back.root(), original.root(), "the round trip is exact");
    assert_eq!(
        in_band_order(scan(&back, &store, the_size()).await?),
        edges()
    );
    Ok(())
}

/// S7: after a rewrite to the band, the ordering, exact and lens reads of
/// S3, S4 and S5 hold as they do on a tree written in the band.
#[dialog_common::test]
async fn it_reads_a_rekeyed_band_as_a_native_one() -> anyhow::Result<()> {
    let (native, store) = tree(NUMBER_BAND, sizes(&lens_values())?).await?;
    let (legacy, legacy_store) = tree(0, sizes(&lens_values())?).await?;
    let rekeyed = rekeyed(&legacy, &legacy_store, NUMBER_BAND).await?;
    assert_eq!(
        scan(
            &rekeyed,
            &legacy_store,
            the_size().is_at_least(float(f64::NEG_INFINITY))
        )
        .await?,
        lens_values(),
        "S3: numeric order across types"
    );
    let selectors: Vec<(ArtifactSelector<Constrained>, &str)> = vec![
        (the_size().is(float(3.0)), "is 3.0"),
        (the_size().is(integer(3)), "is integer 3"),
        (the_size().is(natural(2)), "is natural 2"),
        (the_size().is(float(2.5)), "is 2.5"),
        (the_size().is(integer(-41)), "is -41"),
        (the_size().is_at_least(natural(2)), "natural >= 2"),
        (the_size().is_at_most(natural(2)), "natural <= 2"),
        (the_size().is_greater_than(natural(2)), "natural > 2"),
        (the_size().is_at_least(integer(-3)), "integer >= -3"),
        (the_size().is_less_than(integer(-3)), "integer < -3"),
        (
            the_size().is_between(integer(-3), integer(2)),
            "integer -3..=2",
        ),
        (the_size().is_at_least(float(2.5)), "float >= 2.5"),
        (the_size().is_at_most(float(-2.5)), "float <= -2.5"),
        (the_size().is_at_least(float(f64::INFINITY)), "float >= inf"),
        (
            the_size().is_at_most(natural(2)).across_numbers(),
            "across <= 2",
        ),
        (
            the_size().is_between(integer(-1), natural(1)),
            "mixed -1..=1",
        ),
    ];
    for (selector, claim) in selectors {
        let expected = scan(&native, &store, selector.clone()).await?;
        assert!(
            !expected.is_empty() || claim == "is -41",
            "{claim} reads something"
        );
        assert_eq!(
            scan(&rekeyed, &legacy_store, selector).await?,
            expected,
            "{claim}"
        );
    }
    Ok(())
}
