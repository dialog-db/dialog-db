//! A record's merkle reference: a map under `record:` from each attribute
//! (its relation as a `symbol:`) to its value's reference, or to the `set:`
//! of its values' references when it holds several. A nested record is its
//! own reference. Nothing of the bytes' layout enters it: names inline, in a
//! header or in a buffer's table give one reference.

use super::codec::{self, Field};
use dialog_corpus::identity::{RECORD, SYMBOL};
use merkle_reference::{Reference, Type};
use std::collections::HashMap;

/// The reference of a pairs-encoded record, every part hashed.
pub fn reference(bytes: &[u8]) -> Reference {
    combine(
        bytes,
        &mut |attribute: &[u8]| attribute_reference(attribute),
        &mut |slot| slot_reference(slot),
    )
}

fn attribute_reference(attribute: &[u8]) -> Reference {
    let (name, _) = dialog_corpus::decode_bytes_cow(attribute).expect("an attribute");
    SYMBOL.reference_of(&name)
}

fn slot_reference(slot: &[u8]) -> Reference {
    match codec::field_of(slot).expect("a field") {
        Field::Value(value) => value.reference(),
        Field::Record(_) => reference(&slot[1..]),
    }
}

/// The map of a record from its parts' references, which `attribute` and
/// `slot` give: computed, or looked up where a buffer keeps them.
fn combine(
    bytes: &[u8],
    attribute: &mut dyn FnMut(&[u8]) -> Reference,
    slot: &mut dyn FnMut(&[u8]) -> Reference,
) -> Reference {
    let parts = codec::parts(bytes).expect("parts");
    let mut entries = Vec::new();
    let mut at = 0;
    while at < parts.len() {
        let name = parts[at].attribute;
        let mut values = Vec::new();
        while at < parts.len() && parts[at].attribute == name {
            values.push(slot(parts[at].slot));
            at += 1;
        }
        let value = if values.len() == 1 {
            values[0]
        } else {
            Type::SET.set(values)
        };
        entries.push((attribute(name), value));
    }
    RECORD.map(entries)
}

/// The references a buffer would keep beside its tokens: every attribute's
/// and every scalar value slot's, once.
pub struct Kept<'a> {
    pub parts: HashMap<&'a [u8], Reference>,
}

impl<'a> Kept<'a> {
    pub fn of(records: &'a [Vec<u8>]) -> Self {
        let mut parts = HashMap::new();
        fn walk<'a>(bytes: &'a [u8], parts: &mut HashMap<&'a [u8], Reference>) {
            for part in codec::parts(bytes).expect("parts") {
                parts
                    .entry(part.attribute)
                    .or_insert_with(|| attribute_reference(part.attribute));
                if part.slot[0] == dialog_corpus::ValueDataType::Record as u8 {
                    walk(&part.slot[1..], parts);
                } else {
                    parts
                        .entry(part.slot)
                        .or_insert_with(|| slot_reference(part.slot));
                }
            }
        }
        for record in records {
            walk(record, &mut parts);
        }
        Self { parts }
    }

    /// A record's reference from the kept references of its parts: only the
    /// maps and sets are hashed.
    pub fn reference(&self, bytes: &[u8]) -> Reference {
        combine(bytes, &mut |attribute| self.parts[attribute], &mut |slot| {
            if slot[0] == dialog_corpus::ValueDataType::Record as u8 {
                self.reference(&slot[1..])
            } else {
                self.parts[slot]
            }
        })
    }
}
