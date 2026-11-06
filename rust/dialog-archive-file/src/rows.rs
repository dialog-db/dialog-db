//! A node as columnar rows, and how a frame writes them.
//!
//! A node's rows are the ids its entries, links and buffered operations are
//! made of: token ids for keys and values, node ids for children. A frame
//! writes them column by column, each column as zigzag deltas against the
//! row before, and, against the rows of the node it supersedes, as runs
//! copied from those rows around the rows that are new.

use std::collections::HashMap;
use std::io;

/// One row of a section: the ids an entry, a link or a buffer is made of.
/// Which field means what depends on the [`Section`].
pub(crate) type Row = [u32; 6];

/// The field of an entry's or an operation's row that holds its datum.
pub(crate) const DATUM: usize = 5;

/// The most token slots a key splits into.
pub(crate) const SLOTS: usize = 4;

/// A field with no value: a retraction's datum, a link to a child the file
/// holds no frame of.
pub(crate) const NONE: u32 = u32::MAX;

/// The kind of a segment's rows.
pub(crate) const SEGMENT: u8 = 0;
/// The kind of an index's rows.
pub(crate) const INDEX: u8 = 1;

/// The sections rows are made of. A segment has one (`Entries`); an index
/// has three (`Links`, `Buffers`, `Ops`).
#[derive(Clone, Copy)]
pub(crate) enum Section {
    /// `[form, slot0, slot1, slot2, datum]`: a leaf entry.
    Entries,
    /// `[separator, child node or NONE, scale, child hash token or NONE, 0]`.
    Links,
    /// `[child index, ops in the buffer, 0, 0, 0, 0]`.
    Buffers,
    /// `[form, slot0, slot1, slot2, datum or NONE for a retraction]`.
    Ops,
}

const SEGMENT_SECTIONS: [Section; 1] = [Section::Entries];
const INDEX_SECTIONS: [Section; 3] = [Section::Links, Section::Buffers, Section::Ops];

/// The sections of rows of `kind`.
pub(crate) fn sections_of(kind: u8) -> &'static [Section] {
    if kind == INDEX {
        &INDEX_SECTIONS
    } else {
        &SEGMENT_SECTIONS
    }
}

/// How many token slots a key of `form` splits into.
pub(crate) fn slots_of(form: u8) -> usize {
    use dialog_artifacts::{
        ATTRIBUTE_KEY_TAG, COVERAGE_KEY_TAG, ENTITY_KEY_TAG, HISTORY_KEY_TAG, VALUE_KEY_TAG,
    };
    match form {
        ENTITY_KEY_TAG | ATTRIBUTE_KEY_TAG | VALUE_KEY_TAG => 3,
        HISTORY_KEY_TAG | COVERAGE_KEY_TAG => SLOTS,
        _ => 1,
    }
}

/// A node as columnar rows: its kind and its sections' rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Rows {
    pub kind: u8,
    pub sections: Vec<Vec<Row>>,
}

impl Rows {
    /// What a node is known by when the file looks for the node it
    /// supersedes: its first entry's key, or its first separator. `None`
    /// for a node with no rows.
    pub fn lineage(&self) -> Option<[u32; 4]> {
        let first = self.sections.first()?.first()?;
        Some(if self.kind == INDEX {
            [first[0], 0, 0, 0]
        } else {
            [first[0], first[1], first[2], first[3]]
        })
    }

    /// The bytes the rows hold in memory.
    pub fn footprint(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .sections
                .iter()
                .map(|section| {
                    std::mem::size_of::<Vec<Row>>() + section.capacity() * size_of::<Row>()
                })
                .sum::<usize>()
    }
}

pub(crate) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// A cursor over bytes the file wrote, failing rather than panicking on
/// bytes that end early.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    /// How many bytes have been read.
    pub fn position(&self) -> usize {
        self.at
    }

    pub fn is_empty(&self) -> bool {
        self.at >= self.bytes.len()
    }

    pub fn byte(&mut self) -> io::Result<u8> {
        let byte = *self
            .bytes
            .get(self.at)
            .ok_or_else(|| invalid("a record ends early"))?;
        self.at += 1;
        Ok(byte)
    }

    pub fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| invalid("a record ends early"))?;
        let taken = &self.bytes[self.at..end];
        self.at = end;
        Ok(taken)
    }

    pub fn varint(&mut self) -> io::Result<u64> {
        let (mut value, mut shift) = (0u64, 0);
        loop {
            let byte = self.byte()?;
            if shift > 63 {
                return Err(invalid("a varint runs too long"));
            }
            value |= ((byte & 0x7F) as u64) << shift;
            if byte < 0x80 {
                return Ok(value);
            }
            shift += 7;
        }
    }

    pub fn u32(&mut self) -> io::Result<u32> {
        u32::try_from(self.varint()?).map_err(|_| invalid("an id over 32 bits"))
    }

    pub fn zigzag(&mut self) -> io::Result<i64> {
        let raw = self.varint()?;
        Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
    }
}

fn put_zigzag(out: &mut Vec<u8>, value: i64) {
    put_varint(out, ((value << 1) ^ (value >> 63)) as u64);
}

/// Writes rows column by column. Ids are zigzag deltas against the same
/// column's previous row, so a sorted or repeating column (the entity of
/// an EAV segment, the attribute of an AEV one) costs a byte a row.
fn put_columns(out: &mut Vec<u8>, section: Section, rows: &[&Row]) {
    let delta = |out: &mut Vec<u8>, field: usize, when: &dyn Fn(&Row) -> bool| {
        let mut previous = 0i64;
        for row in rows.iter().filter(|row| when(row)) {
            put_zigzag(out, row[field] as i64 - previous);
            previous = row[field] as i64;
        }
    };
    match section {
        Section::Entries | Section::Ops => {
            for row in rows {
                out.push(row[0] as u8);
            }
            for slot in 0..SLOTS {
                delta(out, 1 + slot, &|row| slots_of(row[0] as u8) > slot);
            }
            if matches!(section, Section::Ops) {
                for row in rows {
                    out.push((row[DATUM] != NONE) as u8);
                }
            }
            delta(out, DATUM, &|row| row[DATUM] != NONE);
        }
        Section::Links => {
            delta(out, 0, &|_| true);
            // The scale, with the top bit marking a child the file holds no
            // frame of, ahead of the columns that depend on it.
            for row in rows {
                out.push(row[2] as u8 | (((row[1] == NONE) as u8) << 7));
            }
            delta(out, 1, &|row| row[1] != NONE);
            delta(out, 3, &|row| row[1] == NONE);
        }
        Section::Buffers => {
            for row in rows {
                put_varint(out, row[0] as u64);
                put_varint(out, row[1] as u64);
            }
        }
    }
}

/// Reads `count` rows written by [`put_columns`].
fn get_columns(reader: &mut Reader, section: Section, count: usize) -> io::Result<Vec<Row>> {
    let mut rows = vec![[0u32; 6]; count];
    let delta = |rows: &mut [Row],
                 reader: &mut Reader,
                 field: usize,
                 when: &dyn Fn(&Row) -> bool|
     -> io::Result<()> {
        let mut previous = 0i64;
        for row in rows.iter_mut() {
            if when(row) {
                previous += reader.zigzag()?;
                row[field] = previous as u32;
            }
        }
        Ok(())
    };
    match section {
        Section::Entries | Section::Ops => {
            for row in rows.iter_mut() {
                row[0] = reader.byte()? as u32;
            }
            for slot in 0..SLOTS {
                delta(&mut rows, reader, 1 + slot, &|row| {
                    slots_of(row[0] as u8) > slot
                })?;
            }
            if matches!(section, Section::Ops) {
                for row in rows.iter_mut() {
                    // Mark retractions; asserted datums are read next.
                    row[DATUM] = if reader.byte()? == 1 { 0 } else { NONE };
                }
            }
            delta(&mut rows, reader, DATUM, &|row| row[DATUM] != NONE)?;
        }
        Section::Links => {
            delta(&mut rows, reader, 0, &|_| true)?;
            for row in rows.iter_mut() {
                let flag = reader.byte()?;
                row[2] = (flag & 0x7F) as u32;
                // A child is named by node id or by hash token, never both.
                if flag >> 7 == 1 {
                    row[1] = NONE;
                } else {
                    row[3] = NONE;
                }
            }
            delta(&mut rows, reader, 1, &|row| row[1] != NONE)?;
            delta(&mut rows, reader, 3, &|row| row[1] == NONE)?;
        }
        Section::Buffers => {
            for row in rows.iter_mut() {
                row[0] = reader.u32()?;
                row[1] = reader.u32()?;
            }
        }
    }
    Ok(rows)
}

/// One step of a section written against the rows of the node it
/// supersedes: copy a run of that node's rows, or take the next rows from
/// the frame's own columns.
enum Op {
    Copy { from: usize, len: usize },
    Literal(usize),
}

/// The ops that rebuild `rows` from `base`: runs of rows the predecessor
/// already has are copied, the rest are literals. Rows are sorted by key
/// in both, so a rewrite that changes one entry is two copies around one
/// literal.
fn diff(rows: &[Row], base: &[Row]) -> Vec<Op> {
    let at: HashMap<&Row, usize> = base.iter().enumerate().map(|(i, row)| (row, i)).collect();
    let mut ops = Vec::new();
    let mut i = 0;
    let mut expected = 0;
    while i < rows.len() {
        let start = if base.get(expected) == Some(&rows[i]) {
            Some(expected)
        } else {
            at.get(&rows[i]).copied()
        };
        match start {
            Some(from) => {
                let mut len = 1;
                while i + len < rows.len()
                    && from + len < base.len()
                    && rows[i + len] == base[from + len]
                {
                    len += 1;
                }
                ops.push(Op::Copy { from, len });
                expected = from + len;
                i += len;
            }
            None => {
                match ops.last_mut() {
                    Some(Op::Literal(n)) => *n += 1,
                    _ => ops.push(Op::Literal(1)),
                }
                i += 1;
            }
        }
    }
    ops
}

/// Writes one section: its row count, then, against a predecessor, the
/// ops, then the literal rows as columns.
fn put_section(out: &mut Vec<u8>, section: Section, rows: &[Row], base: Option<&[Row]>) {
    put_varint(out, rows.len() as u64);
    let Some(base) = base else {
        put_columns(out, section, &rows.iter().collect::<Vec<_>>());
        return;
    };
    let ops = diff(rows, base);
    put_varint(out, ops.len() as u64);
    let mut literals = Vec::new();
    let (mut i, mut expected) = (0usize, 0i64);
    for op in &ops {
        match op {
            Op::Copy { from, len } => {
                put_varint(out, (len << 1 | 1) as u64);
                put_zigzag(out, *from as i64 - expected);
                expected = (*from + *len) as i64;
                i += len;
            }
            Op::Literal(n) => {
                put_varint(out, (n << 1) as u64);
                literals.extend(&rows[i..i + n]);
                i += n;
            }
        }
    }
    put_columns(out, section, &literals);
}

fn get_section(
    reader: &mut Reader,
    section: Section,
    base: Option<&[Row]>,
) -> io::Result<Vec<Row>> {
    let count = reader.varint()? as usize;
    let Some(base) = base else {
        return get_columns(reader, section, count);
    };
    let steps = reader.varint()? as usize;
    let mut ops = Vec::with_capacity(steps.min(count));
    let (mut literal, mut expected) = (0usize, 0i64);
    for _ in 0..steps {
        let head = reader.varint()? as usize;
        if head & 1 == 1 {
            let from = expected + reader.zigzag()?;
            let len = head >> 1;
            let from = usize::try_from(from).map_err(|_| invalid("a copy before its base"))?;
            if from + len > base.len() {
                return Err(invalid("a copy past its base"));
            }
            expected = (from + len) as i64;
            ops.push(Op::Copy { from, len });
        } else {
            literal += head >> 1;
            ops.push(Op::Literal(head >> 1));
        }
    }
    let literals = get_columns(reader, section, literal)?;
    let mut rows = Vec::with_capacity(count);
    let mut next = 0;
    for op in ops {
        match op {
            Op::Copy { from, len } => rows.extend_from_slice(&base[from..from + len]),
            Op::Literal(n) => {
                rows.extend_from_slice(&literals[next..next + n]);
                next += n;
            }
        }
    }
    if rows.len() != count {
        return Err(invalid("a section's rows do not add up"));
    }
    Ok(rows)
}

/// Writes `rows`' sections, against `base` when given (rows of the same
/// kind).
pub(crate) fn put_rows(out: &mut Vec<u8>, rows: &Rows, base: Option<&Rows>) {
    for (i, section) in sections_of(rows.kind).iter().enumerate() {
        let against = base.map(|base| base.sections[i].as_slice());
        put_section(out, *section, &rows.sections[i], against);
    }
}

/// Reads the sections [`put_rows`] wrote for rows of `kind`.
pub(crate) fn get_rows(reader: &mut Reader, kind: u8, base: Option<&Rows>) -> io::Result<Rows> {
    if base.is_some_and(|base| base.kind != kind) {
        return Err(invalid("a frame written against a node of another kind"));
    }
    let sections = sections_of(kind)
        .iter()
        .enumerate()
        .map(|(i, section)| {
            let against = base.map(|base| base.sections[i].as_slice());
            get_section(reader, *section, against)
        })
        .collect::<io::Result<Vec<_>>>()?;
    Ok(Rows { kind, sections })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(rows: Vec<Row>) -> Rows {
        Rows {
            kind: SEGMENT,
            sections: vec![rows],
        }
    }

    #[test]
    fn it_reads_back_rows_written_whole_and_against_a_base() -> io::Result<()> {
        let form = dialog_artifacts::ENTITY_KEY_TAG as u32;
        let base = segment(
            (0..50)
                .map(|n| [form, 1, n + 2, n * 3, 0, n + 100])
                .collect(),
        );
        let mut changed = base.sections[0].clone();
        changed[10][DATUM] = 999;
        changed.remove(30);
        changed.insert(40, [form, 1, 77, 5, 0, 200]);
        let rows = segment(changed);

        let mut whole = Vec::new();
        put_rows(&mut whole, &rows, None);
        assert_eq!(get_rows(&mut Reader::new(&whole), SEGMENT, None)?, rows);

        let mut against = Vec::new();
        put_rows(&mut against, &rows, Some(&base));
        assert!(against.len() < whole.len() / 4);
        assert_eq!(
            get_rows(&mut Reader::new(&against), SEGMENT, Some(&base))?,
            rows
        );
        Ok(())
    }

    #[test]
    fn it_reads_back_links_to_children_by_id_and_by_hash() -> io::Result<()> {
        let rows = Rows {
            kind: INDEX,
            sections: vec![
                vec![
                    [3, 0, 1, NONE, 0, 0],
                    [5, NONE, 2, 40, 0, 0],
                    [9, 7, 0, NONE, 0, 0],
                ],
                vec![[0, 2, 0, 0, 0, 0]],
                vec![[0xFE, 11, 0, 0, 0, 12], [0xFE, 13, 0, 0, 0, NONE]],
            ],
        };
        let mut bytes = Vec::new();
        put_rows(&mut bytes, &rows, None);
        assert_eq!(get_rows(&mut Reader::new(&bytes), INDEX, None)?, rows);
        Ok(())
    }

    #[test]
    fn it_fails_on_bytes_that_end_early() {
        let rows = segment(vec![[0xFE, 7, 0, 0, 0, 9]; 4]);
        let mut bytes = Vec::new();
        put_rows(&mut bytes, &rows, None);
        bytes.truncate(bytes.len() - 2);
        assert!(get_rows(&mut Reader::new(&bytes), SEGMENT, None).is_err());
    }
}
