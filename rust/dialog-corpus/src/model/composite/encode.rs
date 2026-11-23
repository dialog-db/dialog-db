//! Writing the canonical layout.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use super::layout::{LIST, MAP, TABLE, head, varint};
use super::{CompositeError, MAX_DEPTH, SCAN_LIMIT};
use crate::{NUMBER_BAND, Value, ValueDataType, encode_key_value};

/// A scalar slot in a composite body refers to its atom in the table by its
/// place there.
pub(super) const TOKEN_REF: u8 = ValueDataType::ATOM_REFERENCE;

/// A composite's content, its keys and scalar values already in the bytes a
/// slot holds them in.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Node {
    /// Entries, each a key's slot and a value, in any order and repeated.
    Map(Vec<(Vec<u8>, Node)>),
    /// Items, in order.
    List(Vec<Node>),
    /// A scalar value: its type byte and its order-preserving encoding.
    Slot(Vec<u8>),
}

/// The slot a scalar value is held in: its type byte (every number in the
/// band) and its order-preserving encoding.
pub(super) fn scalar(value: &Value) -> Result<Vec<u8>, CompositeError> {
    match value {
        Value::Float(float) if float.is_nan() => Err(CompositeError::NotANumber),
        Value::Composite(_) => Err(CompositeError::CompositeKey),
        value => {
            let mut slot = vec![u8::from(value.data_type().band(NUMBER_BAND))];
            encode_key_value(value, NUMBER_BAND, &mut slot);
            Ok(slot)
        }
    }
}

/// The node a value is in a composite: a nested composite as its content.
pub(super) fn node(value: &Value) -> Result<Node, CompositeError> {
    match value {
        Value::Composite(composite) => Ok(composite.node()),
        value => scalar(value).map(Node::Slot),
    }
}

impl Node {
    fn depth(&self) -> usize {
        match self {
            Node::Slot(_) => 0,
            Node::Map(entries) => 1 + entries.iter().map(|(_, v)| v.depth()).max().unwrap_or(0),
            Node::List(items) => 1 + items.iter().map(Node::depth).max().unwrap_or(0),
        }
    }

    /// The atoms used as a map's key anywhere in it.
    #[cfg(feature = "research-table-order")]
    fn keys<'a>(&'a self, into: &mut BTreeSet<&'a [u8]>) {
        match self {
            Node::Map(entries) => {
                for (key, value) in entries {
                    into.insert(key);
                    value.keys(into);
                }
            }
            Node::List(items) => items.iter().for_each(|item| item.keys(into)),
            Node::Slot(_) => {}
        }
    }

    fn tokens<'a>(&'a self, into: &mut BTreeSet<&'a [u8]>) {
        match self {
            Node::Map(entries) => {
                for (key, value) in entries {
                    into.insert(key);
                    value.tokens(into);
                }
            }
            Node::List(items) => items.iter().for_each(|item| item.tokens(into)),
            Node::Slot(bytes) => {
                into.insert(bytes);
            }
        }
    }
}

/// The canonical bytes of `node`, a map or a list, refusing one nested
/// deeper than [`MAX_DEPTH`].
pub(super) fn canonical(node: &Node) -> Result<Vec<u8>, CompositeError> {
    if node.depth() > MAX_DEPTH {
        return Err(CompositeError::TooDeep);
    }
    let bytes = encode(node, SCAN_LIMIT);
    // Every count, size and offset is at most the length, so a length that
    // fits in a `u32` is one every reader reads back.
    if u32::try_from(bytes.len()).is_err() {
        return Err(CompositeError::TooLarge);
    }
    Ok(bytes)
}

/// `node` written with offsets in every sequence of more than `limit`
/// items. [`SCAN_LIMIT`] gives the canonical bytes; another limit gives a
/// layout every reader reads but [`Composite::from_bytes`](super::Composite::from_bytes)
/// refuses, which only measurement uses.
pub(super) fn encode(node: &Node, limit: usize) -> Vec<u8> {
    let mut keys = BTreeSet::new();
    node.tokens(&mut keys);
    let ordered: Vec<&[u8]> = table_order(node, keys);
    let index: BTreeMap<&[u8], usize> = ordered
        .iter()
        .enumerate()
        .map(|(at, key)| (*key, at))
        .collect();
    let table: Vec<Vec<u8>> = ordered.iter().map(|key| key.to_vec()).collect();
    let mut out = Vec::new();
    write_seq(TABLE, &table, limit, &mut out);
    body(node, &index, limit, &mut out);
    out
}

/// The atoms in the order the table holds them: sorted by bytes, or, with
/// the research switch on, the map keys first and then the rest, each run
/// sorted by bytes (`research/composite-table-order.md`).
fn table_order<'a>(node: &'a Node, atoms: BTreeSet<&'a [u8]>) -> Vec<&'a [u8]> {
    #[cfg(feature = "research-table-order")]
    if research::keys_first() {
        let mut keys = BTreeSet::new();
        node.keys(&mut keys);
        let mut ordered: Vec<&[u8]> = keys.iter().copied().collect();
        ordered.extend(atoms.into_iter().filter(|atom| !keys.contains(atom)));
        return ordered;
    }
    let _ = node;
    atoms.into_iter().collect()
}

/// The run-time switch a measurement of the keys-first table sets. Never
/// set outside research code.
#[cfg(feature = "research-table-order")]
pub mod research {
    use std::sync::atomic::{AtomicBool, Ordering};

    static KEYS_FIRST: AtomicBool = AtomicBool::new(false);

    /// Whether tables are written, and keys looked up, keys first.
    pub fn keys_first() -> bool {
        KEYS_FIRST.load(Ordering::Relaxed)
    }

    /// Writes tables keys first from now on, or sorted by bytes again.
    pub fn set_keys_first(on: bool) {
        KEYS_FIRST.store(on, Ordering::Relaxed);
    }
}

/// A map's or a list's sequence, naming keys by their place in `index`.
fn body(node: &Node, index: &BTreeMap<&[u8], usize>, limit: usize, out: &mut Vec<u8>) {
    match node {
        Node::Slot(_) => unreachable!("a composite is a map or a list"),
        Node::List(items) => {
            let items: Vec<Vec<u8>> = items.iter().map(|item| slot(item, index, limit)).collect();
            write_seq(LIST, &items, limit, out);
        }
        Node::Map(entries) => {
            let mut held: Vec<(usize, Vec<u8>, &Node)> = entries
                .iter()
                .map(|(key, value)| (index[key.as_slice()], slot(value, index, limit), value))
                .collect();
            held.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| order(a.2, b.2)));
            // Equal values under one table are equal slots.
            held.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
            let items: Vec<Vec<u8>> = held
                .into_iter()
                .map(|(key, slot, _)| {
                    let mut item = Vec::with_capacity(slot.len() + 2);
                    varint(key, &mut item);
                    item.extend_from_slice(&slot);
                    item
                })
                .collect();
            write_seq(MAP, &items, limit, out);
        }
    }
}

/// The order of two values under one key: a scalar's slot bytes, and a
/// nested composite by its canonical bytes on its own, so the order does not
/// depend on the table of the composite that holds them.
fn order(left_node: &Node, right_node: &Node) -> Ordering {
    match (left_node, right_node) {
        (Node::Slot(a), Node::Slot(b)) => a.cmp(b),
        (Node::Slot(a), _) => [a[0]].cmp(&[ValueDataType::Composite as u8]),
        (_, Node::Slot(b)) => [ValueDataType::Composite as u8].cmp(&[b[0]]),
        (left, right) => {
            let mut a = vec![ValueDataType::Composite as u8];
            a.extend(encode(left, SCAN_LIMIT));
            let mut b = vec![ValueDataType::Composite as u8];
            b.extend(encode(right, SCAN_LIMIT));
            a.cmp(&b)
        }
    }
}

fn slot(node: &Node, index: &BTreeMap<&[u8], usize>, limit: usize) -> Vec<u8> {
    match node {
        Node::Slot(bytes) => {
            let mut out = vec![TOKEN_REF];
            varint(index[bytes.as_slice()], &mut out);
            out
        }
        nested => {
            let mut out = vec![ValueDataType::Composite as u8];
            body(nested, index, limit, &mut out);
            out
        }
    }
}

/// `head · count · size · offsets · items`: offsets, each of the narrowest
/// width that holds the last, when there are more than `limit` items.
pub(super) fn write_seq<T: AsRef<[u8]>>(kind: u8, items: &[T], limit: usize, out: &mut Vec<u8>) {
    let count = items.len();
    let mut starts = Vec::with_capacity(count);
    let mut total = 0usize;
    for item in items {
        starts.push(total);
        total += item.as_ref().len();
    }
    let width = if count > limit && count > 1 {
        let last = starts[count - 1];
        if last < 1 << 8 {
            1
        } else if last < 1 << 16 {
            2
        } else {
            4
        }
    } else {
        0
    };
    out.push(head(kind, width));
    varint(count, out);
    let offsets = if width == 0 { 0 } else { width * (count - 1) };
    varint(offsets + total, out);
    if width > 0 {
        for start in &starts[1..] {
            out.extend_from_slice(&start.to_be_bytes()[size_of::<usize>() - width..]);
        }
    }
    for item in items {
        out.extend_from_slice(item.as_ref());
    }
}
