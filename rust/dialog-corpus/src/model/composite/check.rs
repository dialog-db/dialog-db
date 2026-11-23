//! A quick way to show bytes are a composite's canonical encoding: one pass
//! over them, no tree built, nothing written again.
//!
//! [`canonical`] answers `true` only for bytes that are exactly what the
//! encoder writes for the content they hold, and `false` for any it cannot
//! show to be, whether or not they are. [`Composite::from_bytes`] takes
//! `true` as its answer and runs the full check (read every part into a
//! tree, write it again, compare) on `false`, so its answers and its errors
//! are the full check's; this only saves that check's cost on the bytes
//! the encoder wrote, which are nearly all a reader meets.
//!
//! Why `true` is right: the encoder writes the table as every scalar the
//! content holds, once, sorted by bytes (here: sorted strictly, each used,
//! each the encoding of its value), then each body with its entries sorted
//! by key, scalars before nested composites and scalars by their atom's
//! place (here: the order is checked as it is met, and a key with two
//! nested composites, which the encoder orders by their bytes on their own,
//! is left to the full check), every varint minimal (`read_varint` refuses
//! any other), every sequence's offsets present exactly when it holds more
//! than [`SCAN_LIMIT`] items, in the narrowest width, each where its item
//! is, and its size the bytes it spans. Bytes that pass all of that are the
//! bytes the encoder writes for them.
//!
//! [`Composite::from_bytes`]: super::Composite::from_bytes

use super::SCAN_LIMIT;
use super::encode::{TOKEN_REF, scalar};
use super::layout::{LAYOUT, LIST, MAP, Seq, TABLE, layout_of, read_varint, seq, slot_end};
use crate::{ValueDataType, decode_value};

/// How deep this pass follows nested composites before leaving the bytes
/// to the full check, well inside [`MAX_DEPTH`](super::MAX_DEPTH).
const DEEPEST: usize = 64;

/// Whether `bytes` are shown, in one pass, to be a composite's canonical
/// encoding. `false` says only that they were not shown to be.
pub(super) fn canonical(bytes: &[u8]) -> bool {
    if layout_of(bytes) != LAYOUT {
        return false;
    }
    let Some(table) = seq(bytes, 0).filter(|table| table.kind == TABLE) else {
        return false;
    };
    let Some(count) = items_canonical(bytes, &table) else {
        return false;
    };
    // The atoms, sorted strictly, each the encoding of its value.
    let mut at = table.items;
    let mut previous: Option<&[u8]> = None;
    for _ in 0..count {
        let Some(end) = slot_end(bytes, at, table.end) else {
            return false;
        };
        let Some(atom) = bytes.get(at..end) else {
            return false;
        };
        if previous.is_some_and(|previous| previous >= atom) || !atom_canonical(atom) {
            return false;
        }
        previous = Some(atom);
        at = end;
    }
    if at != table.end {
        return false;
    }
    // Every atom used, as the encoder's table holds only what is used.
    let mut used = Used::new(count);
    let Some(end) = body(bytes, table.end, count, &mut used, 0) else {
        return false;
    };
    end == bytes.len() && used.all()
}

/// Which atoms a body refers to: a word of bits for up to 64 of them, more
/// in a vector, so a small composite allocates nothing.
enum Used {
    Few(u64, usize),
    Many(Vec<bool>),
}

impl Used {
    fn new(count: usize) -> Self {
        if count <= 64 {
            Used::Few(0, count)
        } else {
            Used::Many(vec![false; count])
        }
    }

    fn mark(&mut self, index: usize) {
        match self {
            Used::Few(bits, _) => *bits |= 1 << index,
            Used::Many(used) => {
                if let Some(slot) = used.get_mut(index) {
                    *slot = true;
                }
            }
        }
    }

    fn all(&self) -> bool {
        match self {
            Used::Few(bits, count) => *count == 0 || *bits == u64::MAX >> (64 - *count),
            Used::Many(used) => used.iter().all(|used| *used),
        }
    }
}

/// The body at `at`, a map or a list, checked: where it ends, or `None`
/// when it is not shown canonical.
fn body(bytes: &[u8], at: usize, atoms: usize, used: &mut Used, depth: usize) -> Option<usize> {
    if depth >= DEEPEST {
        return None;
    }
    let body = seq(bytes, at).filter(|body| body.kind == MAP || body.kind == LIST)?;
    items_canonical(bytes, &body)?;
    let mut cursor = body.items;
    // The last entry's key and what its value was: an atom's place, or a
    // nested composite.
    let mut last: Option<(usize, Option<usize>)> = None;
    for _ in 0..body.count {
        let (key, slot) = if body.kind == MAP {
            let (key, slot) = read_varint(bytes, cursor)?;
            if key >= atoms {
                return None;
            }
            used.mark(key);
            (Some(key), slot)
        } else {
            (None, cursor)
        };
        let (value, end) = match *bytes.get(slot)? {
            TOKEN_REF => {
                let (index, end) = read_varint(bytes, slot + 1)?;
                if index >= atoms || end > body.end {
                    return None;
                }
                used.mark(index);
                (Some(index), end)
            }
            kind if kind == ValueDataType::Composite as u8 => {
                (None, self::body(bytes, slot + 1, atoms, used, depth + 1)?)
            }
            _ => return None,
        };
        if let Some(key) = key {
            if let Some((last_key, last_value)) = last
                && !(key > last_key || key == last_key && after(last_value, value))
            {
                return None;
            }
            last = Some((key, value));
        }
        cursor = end;
    }
    (cursor == body.end).then_some(body.end)
}

/// Whether a value under one key, `value`, comes strictly after the one
/// before it, `last`, as the encoder orders them: scalars first, by their
/// atom's place; a nested composite after them. Two nested composites
/// under one key are not shown ordered here.
fn after(last: Option<usize>, value: Option<usize>) -> bool {
    match (last, value) {
        (Some(last), Some(value)) => value > last,
        (Some(_), None) => true,
        (None, _) => false,
    }
}

/// Whether a sequence's offsets are as the encoder writes them: present
/// exactly when it holds more than [`SCAN_LIMIT`] items, in the narrowest
/// width that holds the last, each where its item is. The count, if so.
fn items_canonical(bytes: &[u8], seq: &Seq) -> Option<usize> {
    if seq.count <= SCAN_LIMIT || seq.count < 2 {
        return (seq.width == 0).then_some(seq.count);
    }
    // The items' starts, read by stepping: a map's by its entries.
    let mut at = seq.items;
    let mut last = 0;
    for index in 0..seq.count {
        if seq.offset(bytes, index) != Some(at) {
            return None;
        }
        last = at - seq.items;
        at = item_end(bytes, seq, at)?;
    }
    let width = if last < 1 << 8 {
        1
    } else if last < 1 << 16 {
        2
    } else {
        4
    };
    (seq.width == width).then_some(seq.count)
}

/// Where the item at `at` of `seq` ends.
fn item_end(bytes: &[u8], seq: &Seq, at: usize) -> Option<usize> {
    let slot = if seq.kind == MAP {
        read_varint(bytes, at)?.1
    } else {
        at
    };
    slot_end(bytes, slot, seq.end)
}

/// Whether `atom`, a type byte and a scalar's encoding, is the encoding its
/// value is written in, keyed under the type it is keyed under. Text with
/// no zero byte is checked where it is; any other atom is decoded and
/// written again.
fn atom_canonical(atom: &[u8]) -> bool {
    let Some((&kind, payload)) = atom.split_first() else {
        return false;
    };
    if kind > ValueDataType::Number as u8 {
        return false;
    }
    if kind == ValueDataType::String as u8
        && let Some((&0, text)) = payload.split_last()
        && !text.contains(&0)
    {
        return std::str::from_utf8(text).is_ok();
    }
    match decode_value(ValueDataType::from(kind), payload) {
        Some((value, [])) => scalar(&value).is_ok_and(|written| written == atom),
        _ => false,
    }
}
