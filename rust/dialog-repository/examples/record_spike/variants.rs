//! Where a record's attribute names sit, three ways, over the same
//! canonical content: the pairs layout's sorted fields, names resolved.
//!
//! - **inline** ([`super::codec`]): every field writes its name.
//! - **header**: every distinct name in the whole record once, sorted, at
//!   the front; a field names its attribute by index (from 1; 0 ends a list
//!   of fields).
//! - **encounter**: a name is written at its first use, in the canonical
//!   order of the fields, and every later use is `0x01` and its index in
//!   order of first use. (A name never starts with `0x00` or `0x01`; the
//!   spike's names do not, a format would have to say so.)
//!
//! In both, a nested record's fields name attributes in its outer record's
//! table, so a nested record is not a record on its own: binding one needs
//! it written again with its own names ([`header_standalone`],
//! [`encounter_standalone`]).

use super::codec;
use std::collections::BTreeMap;

/// A record's canonical content: its fields in canonical order, each name
/// as a key encodes it (`name 00`), each value as its slot, or a record.
pub struct Canon<'a>(pub Vec<(&'a [u8], Item<'a>)>);

pub enum Item<'a> {
    Slot(&'a [u8]),
    Record(Canon<'a>),
}

const RECORD: u8 = 7;

/// The canonical content of a pairs record.
pub fn canon(bytes: &[u8]) -> Canon<'_> {
    Canon(
        codec::parts(bytes)
            .expect("parts")
            .into_iter()
            .map(|part| {
                let item = if part.slot[0] == RECORD {
                    Item::Record(canon(&part.slot[1..]))
                } else {
                    Item::Slot(part.slot)
                };
                (part.attribute, item)
            })
            .collect(),
    )
}

fn varint(mut n: usize, out: &mut Vec<u8>) {
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

fn read_varint(bytes: &[u8]) -> (usize, usize) {
    let (mut n, mut shift, mut at) = (0usize, 0, 0);
    loop {
        let byte = bytes[at];
        n |= ((byte & 0x7f) as usize) << shift;
        at += 1;
        if byte & 0x80 == 0 {
            return (n, at);
        }
        shift += 7;
    }
}

fn names<'a>(canon: &Canon<'a>, into: &mut BTreeMap<&'a [u8], usize>) {
    for (name, item) in &canon.0 {
        into.insert(name, 0);
        if let Item::Record(nested) = item {
            names(nested, into);
        }
    }
}

/// The header layout: `count · name 00 … · fields · 0`.
pub fn header(canon: &Canon<'_>) -> Vec<u8> {
    let mut table = BTreeMap::new();
    names(canon, &mut table);
    for (index, slot) in table.values_mut().enumerate() {
        *slot = index + 1;
    }
    let mut out = Vec::new();
    varint(table.len(), &mut out);
    for name in table.keys() {
        out.extend_from_slice(name);
    }
    header_fields(canon, &table, &mut out);
    out
}

fn header_fields(canon: &Canon<'_>, table: &BTreeMap<&[u8], usize>, out: &mut Vec<u8>) {
    for (name, item) in &canon.0 {
        varint(table[name], out);
        match item {
            Item::Slot(slot) => out.extend_from_slice(slot),
            Item::Record(nested) => {
                out.push(RECORD);
                header_fields(nested, table, out);
            }
        }
    }
    out.push(0);
}

/// The length of a value slot at the front of `bytes` in the header
/// layout: a scalar as the pairs layout writes it, a record its fields.
fn header_slot_len(bytes: &[u8]) -> usize {
    if bytes[0] != RECORD {
        return scalar_len(bytes);
    }
    1 + header_fields_len(&bytes[1..])
}

fn header_fields_len(bytes: &[u8]) -> usize {
    let mut at = 0;
    loop {
        let (index, used) = read_varint(&bytes[at..]);
        at += used;
        if index == 0 {
            return at;
        }
        at += header_slot_len(&bytes[at..]);
    }
}

fn scalar_len(slot: &[u8]) -> usize {
    1 + codec::value_len(slot[0], &slot[1..]).expect("a value")
}

/// The slots `name` holds at the top of a header-layout record.
pub fn header_slots<'a>(bytes: &'a [u8], name: &[u8]) -> Vec<&'a [u8]> {
    let (count, mut at) = read_varint(bytes);
    let mut wanted = None;
    for index in 1..=count {
        let len = memchr::memchr(0, &bytes[at..]).expect("a name") + 1;
        if &bytes[at..at + len] == name {
            wanted = Some(index);
        }
        at += len;
    }
    let mut out = Vec::new();
    let Some(wanted) = wanted else {
        return out;
    };
    loop {
        let (index, used) = read_varint(&bytes[at..]);
        at += used;
        if index == 0 {
            return out;
        }
        let len = header_slot_len(&bytes[at..]);
        if index == wanted {
            out.push(&bytes[at..at + len]);
        }
        at += len;
    }
}

/// The encounter layout: a name at its first use, then `0x01 · index`.
pub fn encounter(canon: &Canon<'_>) -> Vec<u8> {
    let mut seen = BTreeMap::new();
    let mut out = Vec::new();
    encounter_fields(canon, &mut seen, &mut out);
    out
}

fn encounter_fields<'a>(
    canon: &Canon<'a>,
    seen: &mut BTreeMap<&'a [u8], usize>,
    out: &mut Vec<u8>,
) {
    for (name, item) in &canon.0 {
        match seen.get(name) {
            Some(index) => {
                out.push(1);
                varint(*index, out);
            }
            None => {
                let next = seen.len();
                seen.insert(name, next);
                out.extend_from_slice(name);
            }
        }
        match item {
            Item::Slot(slot) => out.extend_from_slice(slot),
            Item::Record(nested) => {
                out.push(RECORD);
                encounter_fields(nested, seen, out);
            }
        }
    }
    out.push(0);
}

/// Walks an encounter-layout list of fields from `at`, learning names as
/// it goes (a nested record's too, which a later back-link may name), and
/// returns where it ends; `visit` sees each top-level field's name and slot.
fn encounter_walk<'a>(
    bytes: &'a [u8],
    mut at: usize,
    table: &mut Vec<&'a [u8]>,
    visit: &mut dyn FnMut(&'a [u8], &'a [u8]),
    top: bool,
) -> usize {
    loop {
        let name = match bytes[at] {
            0 => return at + 1,
            1 => {
                let (index, used) = read_varint(&bytes[at + 1..]);
                at += 1 + used;
                table[index]
            }
            _ => {
                let len = memchr::memchr(0, &bytes[at..]).expect("a name") + 1;
                let name = &bytes[at..at + len];
                table.push(name);
                at += len;
                name
            }
        };
        let start = at;
        if bytes[at] == RECORD {
            at = encounter_walk(bytes, at + 1, table, &mut |_, _| {}, false);
        } else {
            at += scalar_len(&bytes[at..]);
        }
        if top {
            visit(name, &bytes[start..at]);
        }
    }
}

/// The slots `name` holds at the top of an encounter-layout record.
pub fn encounter_slots<'a>(bytes: &'a [u8], name: &[u8]) -> Vec<&'a [u8]> {
    let mut out = Vec::new();
    let mut table = Vec::new();
    encounter_walk(
        bytes,
        0,
        &mut table,
        &mut |n, slot| {
            if n == name {
                out.push(slot);
            }
        },
        true,
    );
    out
}

/// A nested record of a header-layout record, written as a record of its
/// own: its fields read back through the outer table, then a header of
/// their own names.
pub fn header_standalone(outer: &[u8], nested_slot: &[u8]) -> Vec<u8> {
    let (count, mut at) = read_varint(outer);
    let mut table = vec![&outer[0..0]];
    for _ in 0..count {
        let len = memchr::memchr(0, &outer[at..]).expect("a name") + 1;
        table.push(&outer[at..at + len]);
        at += len;
    }
    let canon = header_canon(&nested_slot[1..], &table);
    header(&canon)
}

fn header_canon<'a>(bytes: &'a [u8], table: &[&'a [u8]]) -> Canon<'a> {
    let mut fields = Vec::new();
    let mut at = 0;
    loop {
        let (index, used) = read_varint(&bytes[at..]);
        at += used;
        if index == 0 {
            return Canon(fields);
        }
        let len = header_slot_len(&bytes[at..]);
        let slot = &bytes[at..at + len];
        let item = if slot[0] == RECORD {
            Item::Record(header_canon(&slot[1..], table))
        } else {
            Item::Slot(slot)
        };
        fields.push((table[index], item));
        at += len;
    }
}

/// Every top-level nested record of an encounter-layout record, each
/// written as a record of its own.
pub fn encounter_standalone_all(bytes: &[u8]) -> Vec<Vec<u8>> {
    // The names a nested record uses may have been first written anywhere
    // before it, so the whole record is walked to know them.
    let mut table = Vec::new();
    let mut nested = Vec::new();
    encounter_walk(
        bytes,
        0,
        &mut table,
        &mut |_, slot| {
            if slot[0] == RECORD {
                nested.push(slot);
            }
        },
        true,
    );
    // Indices are given in one order of first use over the whole record,
    // so the full table resolves every back-link a nested record holds.
    nested
        .into_iter()
        .map(|slot| encounter(&encounter_canon_nested(&slot[1..], &table)))
        .collect()
}

/// Reads a record back to its canonical content's pairs bytes, from any
/// layout, to check every layout holds the same content.
pub fn pairs_of(canon: &Canon<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, item) in &canon.0 {
        out.extend_from_slice(name);
        match item {
            Item::Slot(slot) => out.extend_from_slice(slot),
            Item::Record(nested) => {
                out.push(RECORD);
                out.extend_from_slice(&pairs_of(nested));
            }
        }
    }
    out.push(0);
    out
}

/// The canonical content of a header-layout record.
pub fn header_read(bytes: &[u8]) -> Canon<'_> {
    let (count, mut at) = read_varint(bytes);
    let mut table = vec![&bytes[0..0]];
    for _ in 0..count {
        let len = memchr::memchr(0, &bytes[at..]).expect("a name") + 1;
        table.push(&bytes[at..at + len]);
        at += len;
    }
    header_canon(&bytes[at..], &table)
}

/// The canonical content of an encounter-layout record.
pub fn encounter_read(bytes: &[u8]) -> Canon<'_> {
    let mut fields = Vec::new();
    let mut table: Vec<&[u8]> = Vec::new();
    let mut at = 0;
    loop {
        let name = match bytes[at] {
            0 => return Canon(fields),
            1 => {
                let (index, used) = read_varint(&bytes[at + 1..]);
                at += 1 + used;
                table[index]
            }
            _ => {
                let len = memchr::memchr(0, &bytes[at..]).expect("a name") + 1;
                let name = &bytes[at..at + len];
                table.push(name);
                at += len;
                name
            }
        };
        let start = at;
        let item = if bytes[at] == RECORD {
            let before = table.clone();
            let end = encounter_walk(bytes, at + 1, &mut table, &mut |_, _| {}, false);
            at = end;
            Item::Record(encounter_canon_nested(&bytes[start + 1..end], &before))
        } else {
            at += scalar_len(&bytes[at..]);
            Item::Slot(&bytes[start..at])
        };
        fields.push((name, item));
    }
}

fn encounter_canon_nested<'a>(bytes: &'a [u8], before: &[&'a [u8]]) -> Canon<'a> {
    let mut fields = Vec::new();
    let mut table = before.to_vec();
    let mut at = 0;
    loop {
        let name = match bytes[at] {
            0 => return Canon(fields),
            1 => {
                let (index, used) = read_varint(&bytes[at + 1..]);
                at += 1 + used;
                table[index]
            }
            _ => {
                let len = memchr::memchr(0, &bytes[at..]).expect("a name") + 1;
                let name = &bytes[at..at + len];
                table.push(name);
                at += len;
                name
            }
        };
        let start = at;
        let item = if bytes[at] == RECORD {
            let inner_before = table.clone();
            let end = encounter_walk(bytes, at + 1, &mut table, &mut |_, _| {}, false);
            at = end;
            Item::Record(encounter_canon_nested(
                &bytes[start + 1..end],
                &inner_before,
            ))
        } else {
            at += scalar_len(&bytes[at..]);
            Item::Slot(&bytes[start..at])
        };
        fields.push((name, item));
    }
}
