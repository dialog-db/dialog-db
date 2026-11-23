//! Reading the layout: varints, the head of a sequence, where a slot ends.
//!
//! Every function here reads bytes that may be anything (a key from
//! storage, a block from a peer) and answers `None` for bytes it cannot
//! read, never panicking and never reading past the end.

use super::encode::TOKEN_REF;
use crate::ValueDataType;

/// The sequence that lists a composite's keys, at the front of its buffer.
pub(super) const TABLE: u8 = 0;
/// A map: entries, each a key's place in the table and a value's slot.
pub(super) const MAP: u8 = 1;
/// A list: items, each a value's slot.
pub(super) const LIST: u8 = 2;

/// The widths an offset may be written in, by the code a head carries:
/// none, then one, two or four bytes.
const WIDTHS: [usize; 4] = [0, 1, 2, 4];

/// A sequence's head, read: where its parts sit in the buffer.
#[derive(Clone, Copy, Debug)]
pub(super) struct Seq {
    /// [`TABLE`], [`MAP`] or [`LIST`].
    pub kind: u8,
    /// How many items it holds.
    pub count: usize,
    /// The width of each offset, or 0 when it has no offsets.
    pub width: usize,
    /// Where its offsets start.
    pub offsets: usize,
    /// Where its first item starts.
    pub items: usize,
    /// Where it ends.
    pub end: usize,
}

/// The head byte a sequence of `kind` with offsets `width` bytes wide
/// starts with.
pub(super) fn head(kind: u8, width: usize) -> u8 {
    let code = WIDTHS
        .iter()
        .position(|w| *w == width)
        .expect("a width offsets are written in") as u8;
    kind | code << 2
}

pub(super) fn varint(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// A varint at `at`, and where it ends. A varint is minimal (no trailing
/// zero group), so each number has one encoding.
///
/// The number is read in a `u64` and refused past `u32::MAX`, the most
/// offsets four bytes wide can reach, so the same bytes read alike on a
/// 32-bit target (wasm32), where a `usize` would drop the high bits of a
/// five-byte varint, and a 64-bit one.
pub(super) fn read_varint(bytes: &[u8], at: usize) -> Option<(usize, usize)> {
    let mut n = 0u64;
    for (step, &byte) in bytes.get(at..)?.iter().enumerate().take(5) {
        n |= u64::from(byte & 0x7f) << (7 * step);
        if byte & 0x80 == 0 {
            if step > 0 && byte == 0 {
                return None;
            }
            let n = usize::try_from(u32::try_from(n).ok()?).ok()?;
            return Some((n, at + step + 1));
        }
    }
    None
}

/// The sequence whose head is at `at`.
pub(super) fn seq(bytes: &[u8], at: usize) -> Option<Seq> {
    let head = *bytes.get(at)?;
    let kind = head & 0b11;
    let width = *WIDTHS.get(usize::from(head >> 2))?;
    if kind > LIST {
        return None;
    }
    let (count, after_count) = read_varint(bytes, at + 1)?;
    let (size, offsets) = read_varint(bytes, after_count)?;
    let end = offsets.checked_add(size)?;
    if end > bytes.len() {
        return None;
    }
    let table = if width == 0 {
        0
    } else {
        width.checked_mul(count.checked_sub(1)?)?
    };
    let items = offsets.checked_add(table)?;
    if items > end {
        return None;
    }
    Some(Seq {
        kind,
        count,
        width,
        offsets,
        items,
        end,
    })
}

impl Seq {
    /// Where item `index` starts, read from the offsets. `None` when the
    /// sequence has none, or the index or the offset is out of range.
    pub fn offset(&self, bytes: &[u8], index: usize) -> Option<usize> {
        if self.width == 0 || index >= self.count {
            return None;
        }
        if index == 0 {
            return Some(self.items);
        }
        let at = self.offsets + (index - 1) * self.width;
        let mut offset = 0usize;
        for byte in bytes.get(at..at + self.width)? {
            offset = offset << 8 | usize::from(*byte);
        }
        let start = self.items.checked_add(offset)?;
        (start < self.end).then_some(start)
    }
}

/// The length of one `0x00`-escaped, `0x00`-terminated string at the front
/// of `bytes`, its terminator included.
fn terminated_len(bytes: &[u8]) -> Option<usize> {
    let mut at = 0;
    loop {
        let zero = at + memchr::memchr(0, bytes.get(at..)?)?;
        match bytes.get(zero + 1) {
            Some(0xFF) => at = zero + 2,
            _ => return Some(zero + 1),
        }
    }
}

/// Where the slot (a type byte and a value) at `at` ends, no further than
/// `end`. A nested composite's slot ends where its sequence says, so it is
/// skipped without being read.
pub(super) fn slot_end(bytes: &[u8], at: usize, end: usize) -> Option<usize> {
    let bytes = bytes.get(..end)?;
    let kind = *bytes.get(at)?;
    let rest = bytes.get(at + 1..)?;
    if kind == TOKEN_REF {
        return read_varint(bytes, at + 1).map(|(_, end)| end);
    }
    let len = if kind == ValueDataType::Number as u8 {
        dialog_number::encoded_len(rest)?
    } else if kind == ValueDataType::Composite as u8 {
        let nested = seq(bytes, at + 1)?;
        if nested.kind == TABLE {
            return None;
        }
        return Some(nested.end);
    } else if kind == ValueDataType::Boolean as u8 {
        1
    } else if scalar_terminated(kind) {
        terminated_len(rest)?
    } else {
        return None;
    };
    // A slot ends within the bytes: a boolean's type byte with nothing
    // after it does not.
    let slot_end = at.checked_add(1)?.checked_add(len)?;
    (slot_end <= bytes.len()).then_some(slot_end)
}

/// Whether a slot of type `kind` holds a terminated string.
fn scalar_terminated(kind: u8) -> bool {
    [
        ValueDataType::Bytes,
        ValueDataType::Entity,
        ValueDataType::String,
        ValueDataType::Record,
        ValueDataType::Symbol,
    ]
    .iter()
    .any(|t| *t as u8 == kind)
}
