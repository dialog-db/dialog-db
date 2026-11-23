//! A composite: one value made of values, a map or a list.
//!
//! A composite is atomic, as any value is: it is written and replaced whole,
//! never edited in place. It is one of two kinds:
//!
//! - a **map**: entries, each a key and a value. A key is any value that is
//!   not a composite, text the usual one. A key may hold several values, so
//!   a map is a set of entries, sorted, without repeats.
//! - a **list**: items in order, repeats kept.
//!
//! A **record** is the sub-band of the map in which every key is an
//! attribute (a [`Value::Symbol`]), as the naturals are a band of the
//! numbers: one kind, told by its keys, never by a flag
//! ([`Composite::is_record`]).
//!
//! # Canonical bytes
//!
//! ```text
//! composite := table · body              a composite on its own: a buffer of one
//! table     := seq(TABLE) of atoms       every key and scalar value the
//!                                        composite and its nested values use,
//!                                        once, sorted by bytes
//! body      := seq(MAP) of (index · slot)   entries sorted by key, then by value
//!            | seq(LIST) of slot             items in order
//! slot      := 26 · index                 a scalar's place in the table
//!            | 22 · body                 a nested composite, its keys in the table
//! seq(kind) := head · count · size · offsets · items
//! head      := kind | width << 2         width: no offsets, or 1, 2 or 4 bytes
//! offsets   := where each item after the first starts, from the first, big
//!              endian, in the narrowest width that holds the last; written
//!              when, and only when, there are more than SCAN_LIMIT items
//! ```
//!
//! `count`, `size` (the bytes of the offsets and the items) and a key's
//! `index` are LEB128 varints. Every rule fixes one choice, so each
//! composite has exactly one encoding, and [`Composite::from_bytes`] refuses
//! any other. Equal composites on their own therefore have equal bytes, so a
//! composite is keyed in the tree by its bytes.
//!
//! # Views
//!
//! A [`Composite`] is a view: the buffer that holds it and where its body
//! starts. A nested composite read out of another shares its buffer and its
//! table, so it is never written again to be read, and reading a field, an
//! item, the length or an iteration reads only what it returns: a nested
//! body says its size, so it is stepped over without being read, and a long
//! sequence's offsets reach an item without reading those before it.
//!
//! # Identity
//!
//! A map's [merkle reference](Composite::reference) is a map under
//! `composite:` from each key's reference to its value's reference, or to
//! the `set:` of its values' references when it holds several; a list's is
//! the `list:` of its items' references. A nested composite is its own
//! reference, so no part of the layout, the table or the offsets, enters a
//! composite's identity.

mod encode;
mod layout;

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::hash::{Hash, Hasher};
use std::iter::from_fn;
use std::ops::Range;
use std::sync::Arc;

use merkle_reference::{Reference, Type};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser::SerializeSeq};
use thiserror::Error;

use self::encode::{Node, TOKEN_REF, canonical, node, scalar};
use self::layout::{LIST, MAP, Seq, TABLE, read_varint, seq, slot_end};
use crate::identity::COMPOSITE;
use crate::{
    DialogCorpusError, Relation, Value, ValueDataType, ValueView, decode_value, decode_value_view,
};

/// How many items a sequence holds before it is written with offsets.
/// Below it, finding an item by stepping over those before it is as fast as
/// reading an offset (`design/composite-value.md` has the measurements), and
/// the offsets would only add bytes.
pub const SCAN_LIMIT: usize = 8;

/// How deeply composites may nest in one another.
pub const MAX_DEPTH: usize = 128;

/// Why bytes are not a composite, or values do not make one.
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompositeError {
    /// The bytes do not read as a composite at all.
    #[error("the bytes do not read as a composite")]
    Malformed,
    /// The bytes read as a composite, but are not its canonical encoding:
    /// keys or entries out of order or repeated, a key no entry uses, a
    /// varint longer than it needs, offsets where there should be none or
    /// none where there should be some, or bytes past the end.
    #[error("the bytes are not the canonical encoding of the composite they read as")]
    NotCanonical,
    /// A key was a composite: a key is a scalar value.
    #[error("a map's key cannot be a composite")]
    CompositeKey,
    /// A value or a key was NaN, which is not a value.
    #[error("a composite cannot hold NaN, which is not a value")]
    NotANumber,
    /// Composites nested deeper than [`MAX_DEPTH`].
    #[error("composites nest at most {MAX_DEPTH} deep")]
    TooDeep,
    /// A composite whose bytes would run past `u32::MAX`, the most a size
    /// or an offset in them can say.
    #[error("a composite's bytes are at most 4 GiB")]
    TooLarge,
}

impl From<CompositeError> for DialogCorpusError {
    fn from(error: CompositeError) -> Self {
        DialogCorpusError::InvalidValue(error.to_string())
    }
}

/// Which kind a composite is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CompositeKind {
    /// Entries, each a key and a value. A record is a map.
    Map,
    /// Items in order.
    List,
}

/// A map or a list of values: a view over the buffer that holds it.
#[derive(Clone)]
pub struct Composite {
    buffer: Arc<[u8]>,
    /// Where its body's head is in `buffer`.
    body: usize,
}

#[cfg(any(test, feature = "helpers"))]
use std::cell::Cell;

#[cfg(any(test, feature = "helpers"))]
thread_local! {
    /// How many scalars this thread has decoded out of composites, so a test
    /// can say that a read decoded only what it returned.
    static DECODED: Cell<usize> = const { Cell::new(0) };
}

/// How many scalars this thread has decoded out of composites so far.
#[cfg(any(test, feature = "helpers"))]
pub(crate) fn decoded() -> usize {
    DECODED.with(Cell::get)
}

fn counted() {
    #[cfg(any(test, feature = "helpers"))]
    DECODED.with(|count| count.set(count.get() + 1));
}

impl Composite {
    /// The map holding `entries`: each a key and a value, in any order, a
    /// key repeated for each value it holds. An entry given twice is held
    /// once, and so are two numbers that are one number (the natural `3` and
    /// the float `3.0`).
    pub fn map<I>(entries: I) -> Result<Self, CompositeError>
    where
        I: IntoIterator<Item = (Value, Value)>,
    {
        let mut held = Vec::new();
        for (key, value) in entries {
            held.push((scalar(&key)?, node(&value)?));
        }
        Self::of(&Node::Map(held))
    }

    /// The record holding `fields`: a map whose keys are attributes.
    pub fn record<I>(fields: I) -> Result<Self, CompositeError>
    where
        I: IntoIterator<Item = (Relation, Value)>,
    {
        Self::map(
            fields
                .into_iter()
                .map(|(attribute, value)| (Value::Symbol(attribute), value)),
        )
    }

    /// [`Composite::record`], by the name it had first.
    pub fn new<I>(fields: I) -> Result<Self, CompositeError>
    where
        I: IntoIterator<Item = (Relation, Value)>,
    {
        Self::record(fields)
    }

    /// The list of `items`, in the order given.
    pub fn list<I>(items: I) -> Result<Self, CompositeError>
    where
        I: IntoIterator<Item = Value>,
    {
        let items = items
            .into_iter()
            .map(|item| node(&item))
            .collect::<Result<Vec<_>, _>>()?;
        Self::of(&Node::List(items))
    }

    fn of(node: &Node) -> Result<Self, CompositeError> {
        Ok(Self::root(canonical(node)?.into()))
    }

    /// The composite at the root of `buffer`, which holds one.
    fn root(buffer: Arc<[u8]>) -> Self {
        let body = seq(&buffer, 0).map_or(0, |table| table.end);
        Self { buffer, body }
    }

    /// The composite whose canonical bytes are `bytes`, refusing bytes that
    /// do not read as a composite or are not the canonical encoding of the
    /// one they read as.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CompositeError> {
        let read = Self::root(bytes.into());
        let node = read.checked_node()?;
        if canonical(&node)? != bytes {
            return Err(CompositeError::NotCanonical);
        }
        Ok(read)
    }

    /// Splits a composite's bytes into the atoms of its table, each a
    /// scalar's type byte and key encoding (the bytes a tree key holds the
    /// scalar in), and its body, which refers to them by their place in the
    /// table. A store that keeps scalars once keeps a composite as its
    /// atoms' names and its body, and [`Composite::join_atoms`] writes the
    /// same bytes back.
    ///
    /// Only the structure is read: the table, each atom's extent, and the
    /// body's head reaching the end. Nothing is decoded and the bytes are
    /// not checked to be canonical, so a caller that needs the very bytes
    /// back joins the parts and compares. `None` for bytes without that
    /// structure.
    pub fn split_atoms(bytes: &[u8]) -> Option<(Vec<&[u8]>, &[u8])> {
        let table = seq(bytes, 0).filter(|table| table.kind == TABLE)?;
        // Sized by what was read, never by a count the bytes claim.
        let mut atoms = Vec::new();
        let mut at = table.items;
        for index in 0..table.count {
            if table.width > 0 && table.offset(bytes, index) != Some(at) {
                return None;
            }
            if bytes.get(at) == Some(&(ValueDataType::Composite as u8)) {
                return None;
            }
            let end = slot_end(bytes, at, table.end)?;
            atoms.push(bytes.get(at..end)?);
            at = end;
        }
        if at != table.end {
            return None;
        }
        let body = seq(bytes, table.end).filter(|body| body.kind != TABLE)?;
        (body.end == bytes.len()).then(|| (atoms, &bytes[table.end..]))
    }

    /// Writes the bytes whose table holds `atoms`, in that order, and whose
    /// body is `body`: the inverse of [`Composite::split_atoms`].
    pub fn join_atoms(atoms: &[&[u8]], body: &[u8], out: &mut Vec<u8>) {
        encode::write_seq(TABLE, atoms, SCAN_LIMIT, out);
        out.extend_from_slice(body);
    }

    /// The composite in `bytes`, read with every check
    /// [`Composite::from_bytes`] makes but the last: whether they are the
    /// canonical layout. Tests read layouts with other limits than
    /// [`SCAN_LIMIT`] through it.
    #[cfg(test)]
    pub(crate) fn from_layout(bytes: &[u8]) -> Result<Self, CompositeError> {
        let read = Self::root(bytes.into());
        read.checked_node()?;
        Ok(read)
    }

    /// This composite written with offsets in every sequence of more than
    /// `limit` items: a layout readers read but [`Composite::from_bytes`]
    /// refuses unless `limit` is [`SCAN_LIMIT`].
    #[cfg(test)]
    pub(crate) fn layout(&self, limit: usize) -> Vec<u8> {
        encode::encode(&self.node(), limit)
    }

    /// Whether this is the composite at the root of its buffer, rather than
    /// one nested in another.
    pub fn is_root(&self) -> bool {
        seq(&self.buffer, 0).is_some_and(|table| table.end == self.body)
    }

    /// Its canonical bytes on its own: the buffer it is the root of, or, for
    /// a composite nested in another, its content written again.
    pub fn to_bytes(&self) -> Cow<'_, [u8]> {
        if self.is_root() {
            Cow::Borrowed(&self.buffer)
        } else {
            Cow::Owned(encode::encode(&self.node(), SCAN_LIMIT))
        }
    }

    /// The bytes of the buffer this composite is a view over, and the span
    /// of its body in them.
    pub fn buffer(&self) -> (&[u8], Range<usize>) {
        let end = self.seq().map_or(self.body, |body| body.end);
        (&self.buffer, self.body..end)
    }

    fn seq(&self) -> Option<Seq> {
        seq(&self.buffer, self.body)
    }

    fn table(&self) -> Option<Seq> {
        seq(&self.buffer, 0).filter(|table| table.kind == TABLE)
    }

    /// Which kind it is.
    pub fn kind(&self) -> CompositeKind {
        match self.seq().map(|body| body.kind) {
            Some(LIST) => CompositeKind::List,
            _ => CompositeKind::Map,
        }
    }

    /// Whether it is a map.
    pub fn is_map(&self) -> bool {
        self.kind() == CompositeKind::Map
    }

    /// Whether it is a list.
    pub fn is_list(&self) -> bool {
        self.kind() == CompositeKind::List
    }

    /// Whether it is a record: a map every key of which is an attribute. The
    /// table is sorted by type byte first, so the attributes are one run of
    /// it, and the entries are sorted by key, so only the first entry's key
    /// and the last's are read.
    pub fn is_record(&self) -> bool {
        let Some(body) = self.seq().filter(|body| body.kind == MAP) else {
            return false;
        };
        let first = self.entry_at(&body, 0).map(|(key, _, _)| key);
        let last = body
            .count
            .checked_sub(1)
            .and_then(|last| self.entry_at(&body, last))
            .map(|(key, _, _)| key);
        [first, last].into_iter().flatten().all(|key| {
            self.key_bytes(key)
                .is_some_and(|key| key.first() == Some(&(ValueDataType::Symbol as u8)))
        })
    }

    /// How many entries or items it holds at its top level: read from its
    /// head.
    pub fn len(&self) -> usize {
        self.seq().map_or(0, |body| body.count)
    }

    /// Whether it holds nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The slot bytes of the key at `index` in the table.
    fn key_bytes(&self, index: usize) -> Option<&[u8]> {
        let table = self.table()?;
        let start = match table.offset(&self.buffer, index) {
            Some(start) => start,
            None if table.width == 0 && index < table.count => {
                let mut at = table.items;
                for _ in 0..index {
                    at = slot_end(&self.buffer, at, table.end)?;
                }
                at
            }
            None => return None,
        };
        let end = slot_end(&self.buffer, start, table.end)?;
        self.buffer.get(start..end)
    }

    /// The atom a scalar slot at `slot..end` refers to: its bytes in the
    /// table. `None` for a slot that is not a reference.
    fn atom_at(&self, slot: usize, end: usize) -> Option<&[u8]> {
        if self.buffer.get(slot) != Some(&TOKEN_REF) {
            return None;
        }
        let (index, after) = read_varint(&self.buffer, slot + 1)?;
        (after == end).then(|| self.key_bytes(index)).flatten()
    }

    /// The place of `key` (a slot's bytes) in the table: by halving where
    /// the table has offsets, by stepping through it otherwise. No key is
    /// decoded either way; their bytes are compared.
    fn key_index(&self, key: &[u8]) -> Option<usize> {
        let table = self.table()?;
        if table.width > 0 {
            let (mut low, mut high) = (0, table.count);
            while low < high {
                let middle = (low + high) / 2;
                match self.key_bytes(middle)?.cmp(key) {
                    Ordering::Less => low = middle + 1,
                    Ordering::Greater => high = middle,
                    Ordering::Equal => return Some(middle),
                }
            }
            return None;
        }
        let mut at = table.items;
        for index in 0..table.count {
            let end = slot_end(&self.buffer, at, table.end)?;
            match self.buffer[at..end].cmp(key) {
                Ordering::Less => at = end,
                Ordering::Equal => return Some(index),
                Ordering::Greater => return None,
            }
        }
        None
    }

    /// The entry at `at` in a map body: its key's index, where its slot
    /// starts and where it ends.
    fn entry(&self, body: &Seq, at: usize) -> Option<(usize, usize, usize)> {
        let (key, slot) = read_varint(&self.buffer, at)?;
        let end = slot_end(&self.buffer, slot, body.end)?;
        Some((key, slot, end))
    }

    /// The `index`th entry of a map body, by its offset or by stepping over
    /// those before it.
    fn entry_at(&self, body: &Seq, index: usize) -> Option<(usize, usize, usize)> {
        if index >= body.count {
            return None;
        }
        if let Some(at) = body.offset(&self.buffer, index) {
            return self.entry(body, at);
        }
        let mut at = body.items;
        for _ in 0..index {
            at = self.entry(body, at)?.2;
        }
        self.entry(body, at)
    }

    /// The slot spans of every value `key` holds.
    fn spans(&self, key: &Value) -> Vec<(usize, usize)> {
        let mut spans = Vec::new();
        self.each_span(key, &mut |slot, end| {
            spans.push((slot, end));
            true
        });
        spans
    }

    /// The span of the first value `key` holds.
    fn first_span(&self, key: &Value) -> Option<(usize, usize)> {
        let mut first = None;
        self.each_span(key, &mut |slot, end| {
            first = Some((slot, end));
            false
        });
        first
    }

    /// Visits the slot span of each value `key` holds, in order, while
    /// `visit` answers true.
    fn each_span(&self, key: &Value, visit: &mut dyn FnMut(usize, usize) -> bool) {
        let (Some(body), Ok(wanted)) = (self.seq().filter(|b| b.kind == MAP), scalar(key)) else {
            return;
        };
        let Some(index) = self.key_index(&wanted) else {
            return;
        };
        // The first entry of the key: by halving over the offsets, or by
        // stepping from the front.
        let mut at = if body.width > 0 {
            let (mut low, mut high) = (0, body.count);
            while low < high {
                let middle = (low + high) / 2;
                match self.entry_at(&body, middle) {
                    Some((found, _, _)) if found < index => low = middle + 1,
                    Some(_) => high = middle,
                    None => return,
                }
            }
            match body.offset(&self.buffer, low) {
                Some(at) => at,
                None => return,
            }
        } else {
            body.items
        };
        while at < body.end {
            let Some((found, slot, end)) = self.entry(&body, at) else {
                break;
            };
            if found > index {
                break;
            }
            if found == index && !visit(slot, end) {
                break;
            }
            at = end;
        }
    }

    /// The value in the slot at `slot..end`: a scalar decoded, a nested
    /// composite as a view of this buffer.
    fn value_at(&self, slot: usize, end: usize) -> Option<Value> {
        if *self.buffer.get(slot)? == ValueDataType::Composite as u8 {
            return Some(Value::Composite(self.nested(slot)));
        }
        let bytes = self.atom_at(slot, end)?;
        counted();
        let (value, rest) = decode_value(ValueDataType::from(bytes[0]), &bytes[1..])?;
        rest.is_empty().then_some(value)
    }

    /// The value in the slot at `slot..end`, its text and bytes borrowed
    /// from the buffer.
    fn view_at(&self, slot: usize, end: usize) -> Option<ValueView<'_>> {
        if *self.buffer.get(slot)? == ValueDataType::Composite as u8 {
            return Some(ValueView::Composite(self.nested(slot)));
        }
        let bytes = self.atom_at(slot, end)?;
        counted();
        decode_value_view(ValueDataType::from(bytes[0]), &bytes[1..]).ok()
    }

    /// The values `key` holds in a map, in canonical order: none when it
    /// holds none, or this is a list. Only those values are decoded; a
    /// nested composite is a view of this one's buffer.
    pub fn get(&self, key: &Value) -> Vec<Value> {
        self.spans(key)
            .into_iter()
            .filter_map(|(slot, end)| self.value_at(slot, end))
            .collect()
    }

    /// The first value `key` holds in a map.
    pub fn first(&self, key: &Value) -> Option<Value> {
        let (slot, end) = self.first_span(key)?;
        self.value_at(slot, end)
    }

    /// The first value `key` holds in a map, its text and bytes borrowed.
    pub fn first_view(&self, key: &Value) -> Option<ValueView<'_>> {
        let (slot, end) = self.first_span(key)?;
        self.view_at(slot, end)
    }

    /// The values the attribute `attribute` holds in a record.
    pub fn field(&self, attribute: &Relation) -> Vec<Value> {
        self.get(&Value::Symbol(attribute.clone()))
    }

    /// The item at `index` of a list: by its offset, or by stepping over
    /// the items before it, a nested one by its size.
    pub fn item(&self, index: usize) -> Option<Value> {
        let (slot, end) = self.item_span(index)?;
        self.value_at(slot, end)
    }

    /// The item at `index` of a list, its text and bytes borrowed.
    pub fn item_view(&self, index: usize) -> Option<ValueView<'_>> {
        let (slot, end) = self.item_span(index)?;
        self.view_at(slot, end)
    }

    fn item_span(&self, index: usize) -> Option<(usize, usize)> {
        let body = self.seq().filter(|body| body.kind == LIST)?;
        if index >= body.count {
            return None;
        }
        let start = match body.offset(&self.buffer, index) {
            Some(start) => start,
            None => {
                let mut at = body.items;
                for _ in 0..index {
                    at = slot_end(&self.buffer, at, body.end)?;
                }
                at
            }
        };
        Some((start, slot_end(&self.buffer, start, body.end)?))
    }

    /// A map's entries in canonical order: by key, then by value. Each
    /// entry is decoded as it is reached.
    pub fn entries(&self) -> impl Iterator<Item = (Value, Value)> + '_ {
        let body = self.seq().filter(|body| body.kind == MAP);
        let mut at = body.map_or(0, |body| body.items);
        from_fn(move || {
            let body = body?;
            if at >= body.end {
                return None;
            }
            let (key, slot, end) = self.entry(&body, at)?;
            at = end;
            let key_bytes = self.key_bytes(key)?;
            counted();
            let (key, rest) = decode_value(ValueDataType::from(key_bytes[0]), &key_bytes[1..])?;
            if !rest.is_empty() {
                return None;
            }
            Some((key, self.value_at(slot, end)?))
        })
    }

    /// A list's items in order, each decoded as it is reached.
    pub fn items(&self) -> impl Iterator<Item = Value> + '_ {
        let body = self.seq().filter(|body| body.kind == LIST);
        let mut at = body.map_or(0, |body| body.items);
        from_fn(move || {
            let body = body?;
            if at >= body.end {
                return None;
            }
            let end = slot_end(&self.buffer, at, body.end)?;
            let value = self.value_at(at, end);
            at = end;
            value
        })
    }

    /// A record's fields, in canonical order: every entry whose key is an
    /// attribute.
    pub fn fields(&self) -> Vec<(Relation, Value)> {
        self.entries()
            .filter_map(|(key, value)| match key {
                Value::Symbol(attribute) => Some((attribute, value)),
                _ => None,
            })
            .collect()
    }

    /// Its content, keys and scalars as their slot bytes, read without
    /// checks: what writing it again reads.
    fn node(&self) -> Node {
        // Its buffer was checked by `from_bytes` or written by `canonical`,
        // which refuses one past `u32::MAX` bytes, the most a varint here
        // reads: every part of it reads back.
        self.read_node(self.body)
            .expect("a composite reads back as the content it was written from")
    }

    /// Its content, every part checked to read: what [`Self::from_bytes`]
    /// compares against its canonical encoding.
    fn checked_node(&self) -> Result<Node, CompositeError> {
        let table = self.table().ok_or(CompositeError::Malformed)?;
        let body = self.seq().ok_or(CompositeError::Malformed)?;
        if body.kind == TABLE || body.end != self.buffer.len() {
            return Err(CompositeError::Malformed);
        }
        // Every key in the table is a scalar that decodes.
        let mut at = table.items;
        for _ in 0..table.count {
            let end = slot_end(&self.buffer, at, table.end).ok_or(CompositeError::Malformed)?;
            if self.buffer[at] == ValueDataType::Composite as u8 {
                return Err(CompositeError::Malformed);
            }
            self.check_scalar(at, end)?;
            at = end;
        }
        if at != table.end {
            return Err(CompositeError::Malformed);
        }
        self.read_node_checked(self.body, 0)
    }

    fn check_scalar(&self, slot: usize, end: usize) -> Result<(), CompositeError> {
        let kind = self.buffer[slot];
        if kind > ValueDataType::Number as u8 {
            return Err(CompositeError::Malformed);
        }
        match decode_value(ValueDataType::from(kind), &self.buffer[slot + 1..end]) {
            Some((Value::Float(float), [])) if float.is_nan() => Err(CompositeError::NotANumber),
            Some((value, [])) => {
                // A scalar holds the encoding its value is written in, and
                // the type byte it is keyed under.
                if scalar(&value)? == self.buffer[slot..end] {
                    Ok(())
                } else {
                    Err(CompositeError::NotCanonical)
                }
            }
            _ => Err(CompositeError::Malformed),
        }
    }

    fn read_node_checked(&self, at: usize, depth: usize) -> Result<Node, CompositeError> {
        if depth >= MAX_DEPTH {
            return Err(CompositeError::TooDeep);
        }
        let body = seq(&self.buffer, at).ok_or(CompositeError::Malformed)?;
        let table = self.table().ok_or(CompositeError::Malformed)?;
        let mut cursor = body.items;
        // Sized by what was read, never by a count the bytes claim: a count
        // a level claims is not the items it holds, and 128 levels of such
        // claims would reserve more memory than there is.
        let mut parts = Vec::new();
        for index in 0..body.count {
            // Where offsets are written, each is where the item is.
            if body.width > 0 && body.offset(&self.buffer, index) != Some(cursor) {
                return Err(CompositeError::Malformed);
            }
            let (key, slot) = if body.kind == MAP {
                let (key, slot) =
                    read_varint(&self.buffer, cursor).ok_or(CompositeError::Malformed)?;
                if key >= table.count {
                    return Err(CompositeError::Malformed);
                }
                (Some(key), slot)
            } else {
                (None, cursor)
            };
            let end = slot_end(&self.buffer, slot, body.end).ok_or(CompositeError::Malformed)?;
            // A slot is a nested composite or a reference to an atom.
            let value = if self.buffer[slot] == ValueDataType::Composite as u8 {
                self.read_node_checked(slot + 1, depth + 1)?
            } else {
                Node::Slot(
                    self.atom_at(slot, end)
                        .ok_or(CompositeError::Malformed)?
                        .to_vec(),
                )
            };
            parts.push((key, value));
            cursor = end;
        }
        if cursor != body.end {
            return Err(CompositeError::Malformed);
        }
        Ok(match body.kind {
            MAP => Node::Map(
                parts
                    .into_iter()
                    .map(|(key, value)| {
                        let key = key.and_then(|key| self.key_bytes(key)).map(<[u8]>::to_vec);
                        key.map(|key| (key, value)).ok_or(CompositeError::Malformed)
                    })
                    .collect::<Result<_, _>>()?,
            ),
            LIST => Node::List(parts.into_iter().map(|(_, value)| value).collect()),
            _ => return Err(CompositeError::Malformed),
        })
    }

    fn read_node(&self, at: usize) -> Option<Node> {
        let body = seq(&self.buffer, at)?;
        let mut cursor = body.items;
        let mut entries = Vec::new();
        let mut items = Vec::new();
        while cursor < body.end {
            let (key, slot) = if body.kind == MAP {
                let (key, slot) = read_varint(&self.buffer, cursor)?;
                (Some(key), slot)
            } else {
                (None, cursor)
            };
            let end = slot_end(&self.buffer, slot, body.end)?;
            let value = if self.buffer[slot] == ValueDataType::Composite as u8 {
                self.read_node(slot + 1)?
            } else {
                Node::Slot(self.atom_at(slot, end)?.to_vec())
            };
            match key {
                Some(key) => entries.push((self.key_bytes(key)?.to_vec(), value)),
                None => items.push(value),
            }
            cursor = end;
        }
        Some(if body.kind == LIST {
            Node::List(items)
        } else {
            Node::Map(entries)
        })
    }

    /// Its identity: for a map, a map under `composite:` from each key's
    /// reference to its value's, or to the `set:` of its values' when it
    /// holds several; for a list, the `list:` of its items' references.
    pub fn reference(&self) -> Reference {
        match self.kind() {
            CompositeKind::List => Type::LIST.list(self.items().map(|item| item.reference())),
            CompositeKind::Map => {
                let mut grouped: Vec<(Reference, Vec<Reference>)> = Vec::new();
                let mut last: Option<Value> = None;
                for (key, value) in self.entries() {
                    let value = value.reference();
                    match (&last, grouped.last_mut()) {
                        (Some(previous), Some((_, values))) if *previous == key => {
                            values.push(value)
                        }
                        _ => {
                            grouped.push((key.reference(), vec![value]));
                            last = Some(key);
                        }
                    }
                }
                COMPOSITE.map(grouped.into_iter().map(|(key, values)| {
                    let value = match values.as_slice() {
                        [one] => *one,
                        _ => Type::SET.set(values),
                    };
                    (key, value)
                }))
            }
        }
    }

    /// Walks the content, keys and scalars as their bytes, so two views over
    /// different tables are compared and hashed alike.
    fn walk(&self, visit: &mut dyn FnMut(&[u8])) {
        let Some(body) = self.seq() else { return };
        visit(&[body.kind, 0xff]);
        visit(&body.count.to_be_bytes());
        let mut at = body.items;
        while at < body.end {
            let (key, slot) = if body.kind == MAP {
                let Some((key, slot)) = read_varint(&self.buffer, at) else {
                    return;
                };
                (self.key_bytes(key), slot)
            } else {
                (None, at)
            };
            let Some(end) = slot_end(&self.buffer, slot, body.end) else {
                return;
            };
            if let Some(key) = key {
                visit(key);
            }
            if self.buffer[slot] == ValueDataType::Composite as u8 {
                self.nested(slot).walk(visit);
            } else if let Some(atom) = self.atom_at(slot, end) {
                visit(atom);
            } else {
                return;
            }
            at = end;
        }
    }

    /// The composite nested in the slot at `slot`, a view of this buffer.
    fn nested(&self, slot: usize) -> Composite {
        Composite {
            buffer: self.buffer.clone(),
            body: slot + 1,
        }
    }

    /// The part of a sequence at `at`: a map entry's key bytes, and where
    /// its slot starts and ends.
    fn part(&self, seq: &Seq, at: usize) -> Option<(Option<&[u8]>, usize, usize)> {
        let (key, slot) = if seq.kind == MAP {
            let (key, slot) = read_varint(&self.buffer, at)?;
            (Some(self.key_bytes(key)?), slot)
        } else {
            (None, at)
        };
        Some((key, slot, slot_end(&self.buffer, slot, seq.end)?))
    }

    /// Whether two composites hold the same content, whatever buffers hold
    /// them: compared part by part, never written again.
    fn same(&self, other: &Self) -> bool {
        if self.is_root() && other.is_root() {
            return self.buffer == other.buffer;
        }
        let (Some(left), Some(right)) = (self.seq(), other.seq()) else {
            return false;
        };
        if left.kind != right.kind || left.count != right.count {
            return false;
        }
        let (mut a, mut b) = (left.items, right.items);
        while a < left.end && b < right.end {
            let (Some((key_a, slot_a, end_a)), Some((key_b, slot_b, end_b))) =
                (self.part(&left, a), other.part(&right, b))
            else {
                return false;
            };
            if key_a != key_b {
                return false;
            }
            let nested = ValueDataType::Composite as u8;
            match (
                self.buffer[slot_a] == nested,
                other.buffer[slot_b] == nested,
            ) {
                (true, true) => {
                    if !self.nested(slot_a).same(&other.nested(slot_b)) {
                        return false;
                    }
                }
                (false, false) => {
                    if self.atom_at(slot_a, end_a) != other.atom_at(slot_b, end_b) {
                        return false;
                    }
                }
                _ => return false,
            }
            a = end_a;
            b = end_b;
        }
        a == left.end && b == right.end
    }
}

impl PartialEq for Composite {
    fn eq(&self, other: &Self) -> bool {
        self.same(other)
    }
}

impl Eq for Composite {}

impl Hash for Composite {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.walk(&mut |bytes| {
            state.write_usize(bytes.len());
            state.write(bytes);
        })
    }
}

impl PartialOrd for Composite {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Composites order by their canonical bytes on their own, as the tree keys
/// them.
impl Ord for Composite {
    fn cmp(&self, other: &Self) -> Ordering {
        self.to_bytes().cmp(&other.to_bytes())
    }
}

impl Debug for Composite {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self.kind() {
            CompositeKind::List => f.debug_list().entries(self.items()).finish(),
            CompositeKind::Map => f.debug_map().entries(self.entries()).finish(),
        }
    }
}

/// A human-readable format writes a map as `{"map": [[key, value], ...]}`,
/// its entries in canonical order (a list of pairs, since a key may hold
/// several values), and a list as `{"list": [item, ...]}`; a binary format
/// writes its canonical bytes.
impl Serialize for Composite {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        if !serializer.is_human_readable() {
            return serde_bytes::Bytes::new(&self.to_bytes()).serialize(serializer);
        }
        struct Entries<'a>(&'a Composite);
        impl Serialize for Entries<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                for entry in self.0.entries() {
                    seq.serialize_element(&entry)?;
                }
                seq.end()
            }
        }
        struct Items<'a>(&'a Composite);
        impl Serialize for Items<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                for item in self.0.items() {
                    seq.serialize_element(&item)?;
                }
                seq.end()
            }
        }
        let mut map = serializer.serialize_map(Some(1))?;
        match self.kind() {
            CompositeKind::Map => map.serialize_entry("map", &Entries(self))?,
            CompositeKind::List => map.serialize_entry("list", &Items(self))?,
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Composite {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if !deserializer.is_human_readable() {
            let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?;
            return Composite::from_bytes(&bytes).map_err(de::Error::custom);
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "lowercase")]
        enum Form {
            Map(Vec<(Value, Value)>),
            List(Vec<Value>),
        }
        match Form::deserialize(deserializer)? {
            Form::Map(entries) => Composite::map(entries),
            Form::List(items) => Composite::list(items),
        }
        .map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests;
