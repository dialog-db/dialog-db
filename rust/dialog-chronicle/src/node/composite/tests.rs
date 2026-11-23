use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use dialog_common::Buffer;
use dialog_corpus::{
    AttributeKey, Composite, Entity, EntityKey, Fact, Key, Relation, Value, ValueKey, coverage_key,
    encode_key_value, history_key,
};
use dialog_search_tree::{Manifest, NodeRows};

use super::*;
use crate::Chronicle;
use crate::node::tests::{segment_block, spilling, version};
use crate::node::{entry_of, entry_row, key_tokens, rows_of, tree_node};
use crate::rows::Row;

fn text(text: &str) -> Value {
    Value::String(text.into())
}

fn attribute(name: &str) -> Relation {
    Relation::from_str(name).expect("an attribute")
}

fn entity(name: &str) -> Entity {
    Entity::from_str(&format!("test:{name}")).expect("an entity")
}

/// `{x: "hello world", y: "hello world"}`.
fn hello() -> Composite {
    Composite::map([
        (text("x"), text("hello world")),
        (text("y"), text("hello world")),
    ])
    .expect("a map")
}

fn fact(of: &str, the: &str, is: Value) -> Fact {
    Fact {
        the: attribute(the),
        of: entity(of),
        is,
        cause: None,
    }
}

/// The five forms a fact's key takes.
#[derive(Clone, Copy, Debug)]
enum Form {
    Entity,
    Attribute,
    Value,
    History,
    Coverage,
}

const FORMS: [Form; 5] = [
    Form::Entity,
    Form::Attribute,
    Form::Value,
    Form::History,
    Form::Coverage,
];

fn key_of(form: Form, fact: &Fact, manifest: &Manifest) -> Key {
    match form {
        Form::Entity => EntityKey::from_fact(fact, manifest).0,
        Form::Attribute => AttributeKey::from_fact(fact, manifest).0,
        Form::Value => ValueKey::from_fact(fact, manifest).0,
        Form::History => history_key(&version(1), &fact.of, &fact.the, &fact.is, manifest),
        Form::Coverage => coverage_key(&version(1), &fact.of, &fact.the, &fact.is),
    }
}

/// A key's value token as the draft holds it, by the row it was interned
/// as.
fn value_token<'a>(draft: &'a Draft<'_>, key: &[u8], row: &Row) -> &'a [u8] {
    let (form, _, count) = key_tokens(key);
    let slot = value_slot(form, count).expect("a key with a value");
    draft.get(row[1 + slot]).expect("an interned token")
}

#[dialog_common::test]
fn a_composite_in_a_key_is_split_into_shared_tokens() {
    let composite = hello();
    let mut value = vec![ValueDataType::Composite as u8];
    encode_key_value(&Value::Composite(composite), 1, &mut value);
    let mut draft = Draft::detached();
    let id = intern_value(&mut draft, &value, false);
    let token = draft.get(id).unwrap();
    assert!(
        token[1..].starts_with(MAGIC),
        "the composite was interned whole, not split"
    );
    // Its one atom, "hello world", and the keys "x" and "y": three atoms.
    assert_eq!(draft.pending(), 4);
    let mut back = Vec::new();
    realize(token, &draft, &mut back).unwrap();
    assert_eq!(back, value, "the packed token realizes to the very value");
}

/// The value is the last of a split key's tokens in every form, and the
/// row interns it there: a composite's packed, every other token as it is,
/// and the row gives back the very key.
#[dialog_common::test]
fn it_finds_the_value_last_in_every_key_form() -> anyhow::Result<()> {
    let manifest = Manifest::default();
    let fact = fact("alice", "test/profile", Value::Composite(hello()));
    for form in FORMS {
        let key = key_of(form, &fact, &manifest);
        let mut draft = Draft::detached();
        let row = entry_row(&mut draft, key.as_ref(), None)?;
        let (code, parts, count) = key_tokens(key.as_ref());
        let slot = value_slot(code, count).expect("a key with a value");
        for (at, part) in parts[..count].iter().enumerate() {
            let token = draft.get(row[1 + at]).unwrap();
            if at == slot {
                // Coverage keys hold the value's hash, never the value.
                let packed = !matches!(form, Form::Coverage);
                assert_eq!(is_packed(token), packed, "{form:?}: the value");
            } else {
                assert!(!is_packed(token), "{form:?}: token {at} is not the value");
                assert_eq!(token, *part, "{form:?}: token {at} is interned as it is");
            }
        }
        let (back, _) = entry_of(&row, &draft)?;
        assert_eq!(back, key.as_ref(), "{form:?}: the key comes back");
    }
    Ok(())
}

/// No composite a key holds whole or spilled starts, after its type byte,
/// with `FF`: the first byte of a packed token's magic. So a packed token
/// is never taken for a value kept whole, nor the other way round.
#[dialog_common::test]
fn no_composite_value_starts_as_a_packed_token_does() -> anyhow::Result<()> {
    let mut composites = vec![
        Composite::map([])?,
        Composite::list([])?,
        hello(),
        // More than eight atoms: the table has offsets, and its head
        // another width code.
        Composite::list((0..300u32).map(|n| Value::UnsignedInt(n.into())))?,
        Composite::list((0..70_000u32).map(|n| Value::UnsignedInt(n.into())))?,
    ];
    let mut random = Random(7);
    composites.extend((0..200).map(|_| random.composite(3)));
    let narrow = Manifest {
        inline_n: 16,
        spill_prefix: 4,
        ..Manifest::default()
    };
    let no_prefix = Manifest {
        inline_n: 1,
        spill_prefix: 0,
        ..Manifest::default()
    };
    for composite in composites {
        let mut value = vec![ValueDataType::Composite as u8];
        encode_key_value(&Value::Composite(composite.clone()), 1, &mut value);
        assert_ne!(value[1], 0xFF, "{composite:?} whole");
        for manifest in [Manifest::default(), narrow.clone(), no_prefix.clone()] {
            for form in [Form::Entity, Form::Value] {
                let key = key_of(
                    form,
                    &fact("a", "test/v", Value::Composite(composite.clone())),
                    &manifest,
                );
                let (code, parts, count) = key_tokens(key.as_ref());
                let token = parts[value_slot(code, count).unwrap()];
                assert!(
                    token.len() > 1 && token[1] != 0xFF,
                    "{composite:?} in {key:?}"
                );
                assert!(!is_packed(token));
            }
        }
    }
    Ok(())
}

/// A spilled composite's key holds only a prefix of its bytes: its value
/// token is kept whole, told by the key, not by a parse that fails.
#[dialog_common::test]
fn it_keeps_a_spilled_composite_whole() -> anyhow::Result<()> {
    let manifest = spilling();
    let long = Composite::map([(text("body"), text(&"long text ".repeat(40)))])?;
    let fact = fact("a", "test/v", Value::Composite(long));
    let before = kept_whole();
    for form in [Form::Entity, Form::Attribute, Form::Value, Form::History] {
        let key = key_of(form, &fact, &manifest);
        assert!(
            dialog_corpus::key_value_is_spilled(key.as_ref()),
            "{form:?}"
        );
        let mut draft = Draft::detached();
        let row = entry_row(&mut draft, key.as_ref(), None)?;
        let token = value_token(&draft, key.as_ref(), &row);
        assert!(!is_packed(token), "{form:?}: a spilled value is kept whole");
        assert_eq!(entry_of(&row, &draft)?.0, key.as_ref());
    }
    assert_eq!(
        kept_whole(),
        before,
        "kept whole on purpose, not by the net"
    );
    Ok(())
}

/// A value whose bytes split but would not come back the same is interned
/// whole: the net catches it, drops the atoms it interned, and the key
/// comes back as given.
#[dialog_common::test]
fn it_interns_whole_a_composite_that_would_not_come_back() -> anyhow::Result<()> {
    // `hello()`'s bytes with its table of three atoms written with offsets
    // (one byte wide), which a table of so few never has: they split, and
    // join again without the offsets, into other bytes.
    let canonical = hello().to_bytes().into_owned();
    let (count, size) = (canonical[1] as usize, canonical[2] as usize);
    assert_eq!(
        (canonical[0], count),
        (0x00, 3),
        "a table of three, no offsets"
    );
    let items = &canonical[3..3 + size];
    let first = 1 + items[1..].iter().position(|byte| *byte == 0).unwrap() + 1;
    let mut odd = vec![
        0x04,
        count as u8,
        (size + 2) as u8,
        first as u8,
        (first + 3) as u8,
    ];
    odd.extend_from_slice(items);
    odd.extend_from_slice(&canonical[3 + size..]);
    let (atoms, _) = Composite::split_atoms(&odd).expect("the odd bytes split");
    assert_eq!(atoms.len(), 3);
    let mut value = vec![ValueDataType::Composite as u8];
    dialog_corpus::encode_bytes(&odd, &mut value);

    let before = kept_whole();
    let mut draft = Draft::detached();
    let id = intern_value(&mut draft, &value, false);
    assert_eq!(draft.get(id), Some(value.as_slice()));
    assert_eq!(draft.pending(), 1, "no atom is left behind");
    assert_eq!(kept_whole(), before + 1);
    Ok(())
}

/// A packed token that does not read is an error of a named kind, never
/// a panic: a count past its ids, an id the file lacks, bytes cut short.
#[dialog_common::test]
fn it_refuses_a_corrupted_packed_token() -> anyhow::Result<()> {
    let mut value = vec![ValueDataType::Composite as u8];
    encode_key_value(&Value::Composite(hello()), 1, &mut value);
    let mut draft = Draft::detached();
    let id = intern_value(&mut draft, &value, false);
    let packed = draft.get(id).unwrap().to_vec();
    let count_at = 1 + MAGIC.len();
    assert_eq!(packed[count_at], 3, "three atoms");
    let kind = |token: &[u8]| -> Option<PackedError> {
        let error = realize(token, &draft, &mut Vec::new()).err()?;
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        error.get_ref()?.downcast_ref::<PackedError>().copied()
    };

    let mut counted = packed.clone();
    counted[count_at] = 100;
    assert!(matches!(kind(&counted), Some(PackedError::Truncated)));

    let mut huge = packed.clone();
    huge.splice(count_at..count_at + 1, [0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    assert!(matches!(kind(&huge), Some(PackedError::Truncated)));

    let mut missing = packed.clone();
    missing[count_at + 1..count_at + 5].copy_from_slice(&9_999u32.to_le_bytes());
    assert!(matches!(
        kind(&missing),
        Some(PackedError::MissingAtom(9_999))
    ));

    for cut in [count_at, count_at + 1, count_at + 6] {
        assert!(
            matches!(kind(&packed[..cut]), Some(PackedError::Truncated)),
            "cut at {cut}"
        );
    }

    // A row naming the corrupted token gives the error back, too.
    let key = key_of(
        Form::Entity,
        &fact("a", "test/v", Value::Composite(hello())),
        &Manifest::default(),
    );
    let mut draft = Draft::detached();
    let mut row = entry_row(&mut draft, key.as_ref(), None)?;
    row[3] = draft.intern(&missing);
    let error = entry_of(&row, &draft).expect_err("a token the file lacks");
    assert!(matches!(
        error
            .get_ref()
            .and_then(|e| e.downcast_ref::<PackedError>()),
        Some(PackedError::MissingAtom(9_999))
    ));

    // Naming drafted atoms no token took.
    let mut unresolved = packed.clone();
    let error = remap(&mut unresolved, |id| Err(PackedError::Unresolved(id))).unwrap_err();
    assert!(matches!(
        error
            .get_ref()
            .and_then(|e| e.downcast_ref::<PackedError>()),
        Some(PackedError::Unresolved(_))
    ));
    Ok(())
}

/// Random composites, in keys of every form, are interned as rows and
/// read back as the same key bytes, and none is kept whole by the net.
#[dialog_common::test]
fn random_composites_come_back_byte_for_byte() -> anyhow::Result<()> {
    let manifest = Manifest::default();
    let mut random = Random(0x5eed);
    let before = kept_whole();
    let mut draft = Draft::detached();
    let mut packed = 0;
    for round in 0..500 {
        let composite = random.composite(4);
        let fact = fact(
            &format!("e{}", round % 7),
            "test/v",
            Value::Composite(composite),
        );
        for form in FORMS {
            let key = key_of(form, &fact, &manifest);
            let row = entry_row(&mut draft, key.as_ref(), None)?;
            packed += is_packed(value_token(&draft, key.as_ref(), &row)) as usize;
            assert_eq!(entry_of(&row, &draft)?.0, key.as_ref(), "{fact:?} {form:?}");
        }
    }
    assert_eq!(
        kept_whole(),
        before,
        "the net caught a well formed composite"
    );
    assert!(packed >= 500 * 4 * 9 / 10, "most are packed: {packed}");
    Ok(())
}

/// Facts holding composites, across every form and shape: their segment
/// splits into rows whose packed values realize to the very block.
#[dialog_common::test]
fn it_realizes_a_segment_of_composites_from_its_tokens() -> anyhow::Result<()> {
    let manifest = spilling();
    let values = shapes()?;
    for form in FORMS {
        let keys: Vec<Key> = values
            .iter()
            .enumerate()
            .map(|(at, value)| {
                key_of(
                    form,
                    &fact(&format!("e{at}"), "test/v", value.clone()),
                    &manifest,
                )
            })
            .collect();
        let block = segment_block(keys, &manifest)?;
        let node = tree_node(&Buffer::from(block.clone())).expect("a tree node");
        let mut tokens = Draft::detached();
        let rows = rows_of(&node, &mut tokens, |_| None)?;
        assert_eq!(
            crate::node::realize(&rows, &tokens, &manifest, |_| None)?,
            block,
            "{form:?}"
        );
    }
    Ok(())
}

/// The shapes a composite takes: nested, a list, empty, and one that
/// spills under [`spilling`].
fn shapes() -> anyhow::Result<Vec<Value>> {
    let inner = Composite::map([
        (text("street"), text("hello world")),
        (text("n"), Value::UnsignedInt(7)),
    ])?;
    Ok(vec![
        Value::Composite(hello()),
        Value::Composite(Composite::map([(
            text("at"),
            Value::Composite(inner.clone()),
        )])?),
        Value::Composite(Composite::list([
            text("hello world"),
            Value::Composite(inner),
            Value::Boolean(true),
        ])?),
        Value::Composite(Composite::map([])?),
        Value::Composite(Composite::list([])?),
        Value::Composite(Composite::map([(
            text("body"),
            text(&"long text ".repeat(40)),
        )])?),
        text("hello world"),
    ])
}

/// Every key of `block` as the archive serves the node in place, joined.
fn served_keys(archive: &Chronicle, block: &Buffer) -> Vec<Vec<u8>> {
    let served = archive
        .served("index", block.blake3_hash())
        .expect("the node is served in place");
    (0..served.len())
        .map(|at| served.key(at).parts().concat())
        .collect()
}

/// Every key of the segment `block`, as the tree reads them from it.
fn block_keys(block: &Buffer) -> anyhow::Result<Vec<Vec<u8>>> {
    let node = tree_node(block).expect("a tree node");
    let dialog_search_tree::NodeBody::Segment(segment) = node.body() else {
        anyhow::bail!("a segment");
    };
    let mut keys = segment.keys::<Key>()?;
    let mut all = Vec::new();
    while let Some((_, key)) = keys.next_key()? {
        all.push(key.to_vec());
    }
    Ok(all)
}

/// Asserts `archive` holds each of `blocks` as a frame and reads it back
/// byte for byte: whole, and key by key as it serves the node in place.
fn assert_reads_back(archive: &Chronicle, blocks: &[Buffer]) -> anyhow::Result<()> {
    for block in blocks {
        let hash = block.blake3_hash();
        assert!(archive.is_frame("index", hash), "kept as a frame");
        assert_eq!(archive.get("index", hash)?.as_deref(), Some(block.as_ref()));
        assert_eq!(served_keys(archive, block), block_keys(block)?);
        let served = archive.served("index", hash).expect("served");
        let tree = crate::node::TreeNode::from_rows(Arc::new(served)).expect("a tree node");
        assert_eq!(tree.buffer()?.as_ref(), block.as_ref());
    }
    assert_eq!(archive.stats().unverified, 0, "no node failed to realize");
    Ok(())
}

/// The block of a segment of one form, a fact for each value.
fn segment(
    form: Form,
    name: &str,
    values: &[Value],
    manifest: &Manifest,
) -> anyhow::Result<Buffer> {
    let keys = values
        .iter()
        .enumerate()
        .map(|(at, value)| {
            key_of(
                form,
                &fact(
                    &format!("{name}-{at}"),
                    &format!("test/a{at}"),
                    value.clone(),
                ),
                manifest,
            )
        })
        .collect();
    Ok(Buffer::from(segment_block(keys, manifest)?))
}

/// Composites holding "hello world" and a fact holding it alone: after a
/// commit the file holds the atom once, and the fact's value token is that
/// very token, since an atom is a scalar as a key holds it.
#[dialog_common::test]
fn it_shares_a_composite_atom_with_every_value_that_holds_it() -> anyhow::Result<()> {
    let manifest = Manifest::default();
    let values = [
        Value::Composite(hello()),
        Value::Composite(Composite::map([
            (text("greeting"), text("hello world")),
            (text("n"), Value::UnsignedInt(1)),
        ])?),
        text("hello world"),
    ];
    let block = segment(Form::Entity, "share", &values, &manifest)?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("archive");
    let before = kept_whole();
    {
        let archive = Chronicle::open(&path)?;
        archive.append("index", std::slice::from_ref(&block))?;
    }
    let archive = Chronicle::open(&path)?;
    assert_reads_back(&archive, std::slice::from_ref(&block))?;
    assert_eq!(kept_whole(), before);

    let held = archive.held_tokens()?;
    let mut atom = vec![ValueDataType::String as u8];
    encode_key_value(&text("hello world"), 1, &mut atom);
    let atoms: Vec<u32> = held
        .iter()
        .filter(|(_, token)| *token == atom)
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(atoms.len(), 1, "the atom is held once");
    let spelled = held
        .iter()
        .filter(|(_, token)| token.windows(11).any(|w| w == b"hello world"))
        .count();
    assert_eq!(spelled, 1, "no other token spells it out");
    let packed: Vec<&Vec<u8>> = held
        .iter()
        .filter(|(_, token)| is_packed(token))
        .map(|(_, token)| token)
        .collect();
    assert_eq!(packed.len(), 2, "each composite is one packed token");
    for token in packed {
        let (count, start) = header(token)?;
        let names: Vec<u32> = (0..count).map(|n| id_at(token, start + 4 * n)).collect();
        assert!(names.contains(&atoms[0]), "each names the one atom");
    }
    Ok(())
}

/// Composites round trip byte for byte through commit, close, reopen and
/// read, in every form and shape: atoms new in the commit that packs the
/// composite (named by their drafted ids, then by their places), and
/// atoms the file held from an earlier commit (named by their places from
/// the start).
#[dialog_common::test]
fn it_reads_back_composites_after_reopening() -> anyhow::Result<()> {
    let manifest = spilling();
    let shapes = shapes()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("archive");
    let before = kept_whole();
    let first: Vec<Buffer> = FORMS
        .iter()
        .map(|form| segment(*form, "first", &shapes, &manifest))
        .collect::<anyhow::Result<_>>()?;
    {
        let archive = Chronicle::open(&path)?;
        archive.append("index", &first)?;
        assert_reads_back(&archive, &first)?;
    }
    // The same atoms in other composites, and some new ones.
    let second_values = vec![
        Value::Composite(Composite::map([
            (text("x"), text("hello world")),
            (text("z"), text("only in the second")),
        ])?),
        Value::Composite(Composite::list([
            text("hello world"),
            Value::UnsignedInt(7),
        ])?),
    ];
    let second: Vec<Buffer> = FORMS
        .iter()
        .map(|form| segment(*form, "second", &second_values, &manifest))
        .collect::<anyhow::Result<_>>()?;
    {
        let archive = Chronicle::open(&path)?;
        let tokens = archive.held_tokens()?.len();
        archive.append("index", &second)?;
        assert_reads_back(&archive, &second)?;
        let new = archive.held_tokens()?.len() - tokens;
        assert!(new > 0);
    }
    let archive = Chronicle::open(&path)?;
    let all: Vec<Buffer> = first.into_iter().chain(second).collect();
    assert_reads_back(&archive, &all)?;
    assert_eq!(kept_whole(), before, "only the spilled composite is whole");
    let held = archive.held_tokens()?;
    assert!(
        held.iter().any(
            |(_, token)| token.first() == Some(&(ValueDataType::Composite as u8))
                && !is_packed(token)
        ),
        "the spilled composite is held whole"
    );
    Ok(())
}

/// A node rewritten with one entry more takes its predecessor's rows for
/// the entries it keeps, packed composites among them, and reads back.
#[dialog_common::test]
fn it_reuses_a_predecessors_rows_of_composites() -> anyhow::Result<()> {
    let manifest = Manifest::default();
    let shapes = shapes()?;
    let keys = |count: usize| -> Vec<Key> {
        (0..count)
            .map(|at| {
                key_of(
                    Form::Entity,
                    &fact(
                        "same",
                        &format!("test/a{at:02}"),
                        shapes[at % shapes.len()].clone(),
                    ),
                    &manifest,
                )
            })
            .collect()
    };
    let before = Buffer::from(segment_block(keys(6), &manifest)?);
    let after = Buffer::from(segment_block(keys(7), &manifest)?);
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("archive");
    {
        let archive = Chronicle::open(&path)?;
        archive.append("index", std::slice::from_ref(&before))?;
        let taken = archive.stats().rows_taken;
        archive.append("index", std::slice::from_ref(&after))?;
        // Every entry but the first, which is interned to find the
        // predecessor by.
        assert_eq!(
            archive.stats().rows_taken,
            taken + 5,
            "the rewritten node takes its predecessor's rows"
        );
        assert_reads_back(&archive, &[before.clone(), after.clone()])?;
    }
    let archive = Chronicle::open(&path)?;
    assert_reads_back(&archive, &[before, after])?;
    Ok(())
}

/// A form of the notation as a syntax tree, shaped as the census of the
/// notation's documents reports them (`design/composite-value.md`): every
/// node a map that says its `type`, two fields a map at the median, a
/// call's parts a list, nested 7 to 19 deep, its names drawn from a small
/// vocabulary as a program's are.
fn syntax_form(random: &mut Random, depth: usize) -> Value {
    const KINDS: [&str; 12] = [
        "application",
        "variable",
        "symbol",
        "keyword",
        "text",
        "number",
        "tuple",
        "struct",
        "set",
        "rule",
        "concept",
        "field",
    ];
    let name = |random: &mut Random| {
        let at = random.below(150);
        match random.below(4) {
            0 => format!("?v{at}"),
            1 => format!(":k{at}"),
            _ => format!("name-{at}"),
        }
    };
    let kind = KINDS[random.below(KINDS.len() as u64) as usize];
    let mut fields = vec![(text("type"), text(kind))];
    if depth == 0 {
        fields.push((text("name"), text(&name(random))));
    } else {
        match random.below(3) {
            0 => {
                let items = (0..1 + random.below(3)).map(|_| syntax_form(random, depth - 1));
                fields.push((
                    text("terms"),
                    Value::Composite(Composite::list(items).unwrap()),
                ));
            }
            _ => {
                fields.push((text("operand"), syntax_form(random, depth - 1)));
                if random.below(2) == 0 {
                    fields.push((text("name"), text(&name(random))));
                }
            }
        }
    }
    Value::Composite(Composite::map(fields).unwrap())
}

/// The size of a file holding `forms`, one fact each, in the entity
/// ordering, with composites packed or kept whole.
fn archive_of(
    forms: &[Value],
    packing: bool,
) -> anyhow::Result<(std::collections::HashMap<String, u64>, usize, u64)> {
    PACKING.store(packing, Ordering::Relaxed);
    let manifest = Manifest::default();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("archive");
    let archive = Chronicle::open(&path)?;
    for (at, chunk) in forms.chunks(16).enumerate() {
        let block = segment(Form::Entity, &format!("doc{at}"), chunk, &manifest)?;
        archive.append("index", std::slice::from_ref(&block))?;
    }
    let held = archive.held_tokens()?;
    let token_bytes = held.iter().map(|(_, token)| token.len() as u64).sum();
    let records = archive.record_bytes()?;
    PACKING.store(true, Ordering::Relaxed);
    Ok((records, held.len(), token_bytes))
}

/// The file a set of syntax trees takes with their atoms shared as tokens
/// against each composite kept whole. Generated trees of the census's
/// shape, not the census's own trees. Run by hand:
/// `cargo test --release -p dialog-chronicle --lib -- --ignored --nocapture
/// measure_an_archive_of_syntax_trees`.
#[dialog_common::test]
#[ignore = "a measurement, run by hand"]
fn measure_an_archive_of_syntax_trees() -> anyhow::Result<()> {
    let mut random = Random(0x5ca1ab1e);
    let forms: Vec<Value> = (0..133)
        .map(|_| {
            let depth = 7 + random.below(5) as usize;
            syntax_form(&mut random, depth)
        })
        .collect();
    let canonical: usize = forms
        .iter()
        .map(|form| match form {
            Value::Composite(composite) => composite.to_bytes().len(),
            _ => 0,
        })
        .sum();
    let (shared, shared_tokens, shared_bytes) = archive_of(&forms, true)?;
    let (whole, whole_tokens, whole_bytes) = archive_of(&forms, false)?;
    let of = |records: &std::collections::HashMap<String, u64>, kind: &str| {
        records.get(kind).copied().unwrap_or(0)
    };
    println!("{} forms, {canonical} bytes of composites", forms.len());
    println!("| | tokens | their bytes | token records | frame records | all records |");
    for (name, records, tokens, bytes) in [
        ("atoms shared", &shared, shared_tokens, shared_bytes),
        ("kept whole", &whole, whole_tokens, whole_bytes),
    ] {
        println!(
            "| {name} | {tokens} | {bytes} | {} | {} | {} |",
            of(records, "Tokens"),
            of(records, "Frame"),
            records.values().sum::<u64>()
        );
    }
    println!("{shared:?}");
    println!("{whole:?}");
    Ok(())
}

/// A small deterministic generator of composites.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn scalar(&mut self) -> Value {
        match self.below(8) {
            0 => text(["hello world", "a", "", "\u{0}zero", "é"][self.below(5) as usize]),
            1 => Value::UnsignedInt(self.below(1000).into()),
            2 => Value::SignedInt(-(self.below(1000) as i128)),
            3 => Value::Float(self.below(100) as f64 / 8.0),
            4 => Value::Boolean(self.below(2) == 0),
            5 => Value::Bytes((0..self.below(6)).map(|n| n as u8).collect()),
            6 => Value::Symbol(attribute(["test/a", "test/b"][self.below(2) as usize])),
            _ => Value::Entity(entity(["a", "b"][self.below(2) as usize])),
        }
    }

    fn value(&mut self, depth: usize) -> Value {
        if depth > 0 && self.below(4) == 0 {
            Value::Composite(self.composite(depth - 1))
        } else {
            self.scalar()
        }
    }

    fn composite(&mut self, depth: usize) -> Composite {
        let len = [0, 1, 2, 3, 9, 20][self.below(6) as usize];
        if self.below(2) == 0 {
            let entries: Vec<_> = (0..len)
                .map(|_| (self.scalar(), self.value(depth)))
                .collect();
            Composite::map(entries).expect("a map")
        } else {
            let items: Vec<_> = (0..len).map(|_| self.value(depth)).collect();
            Composite::list(items).expect("a list")
        }
    }
}
