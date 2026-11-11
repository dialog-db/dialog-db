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
    use dialog_corpus::{
        ATTRIBUTE_KEY_TAG, COVERAGE_KEY_TAG, ENTITY_KEY_TAG, HISTORY_KEY_TAG, VALUE_KEY_TAG,
    };
    match form {
        ENTITY_KEY_TAG | ATTRIBUTE_KEY_TAG | VALUE_KEY_TAG => 3,
        HISTORY_KEY_TAG | COVERAGE_KEY_TAG => SLOTS,
        _ => 1,
    }
}

/// `rows` with each token id renamed by `token` and each child's node id
/// by `node`: for rows drafted before an append's turn, whose new tokens
/// and nodes took other ids by the time it came.
pub(crate) fn rename(rows: &Rows, token: impl Fn(u32) -> u32, node: impl Fn(u32) -> u32) -> Rows {
    let mut renamed = rows.clone();
    for (section, rows) in sections_of(rows.kind).iter().zip(&mut renamed.sections) {
        for row in rows {
            match section {
                Section::Entries | Section::Ops => {
                    for slot in 1..=slots_of(row[0] as u8) {
                        row[slot] = token(row[slot]);
                    }
                    if row[DATUM] != NONE {
                        row[DATUM] = token(row[DATUM]);
                    }
                }
                Section::Links => {
                    row[0] = token(row[0]);
                    if row[1] != NONE {
                        row[1] = node(row[1]);
                    }
                    if row[3] != NONE {
                        row[3] = token(row[3]);
                    }
                }
                Section::Buffers => {}
            }
        }
    }
    renamed
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

/// Writes `value` as an LEB128 varint: seven bits a byte. The file's
/// integers are mostly ids and deltas between them, from a few hundred to
/// a few tens of thousands, which LEB128 writes in two bytes where bijou64
/// takes three. Nothing in the file is hashed as written, so the one
/// encoding per number bijou64 gives is not needed here: a frame is
/// checked by reading it back.
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

    #[cfg(test)]
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

    /// An LEB128 varint.
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
    put_varint(out, zigzag(value));
}

/// Writes `values` as runs, as Automerge's columns do: a signed count, then
/// for a positive count one value repeated that many times, for a negative
/// one that many values one by one. A column that repeats (a leaf's form,
/// the entity of an EAV leaf, a sorted column's deltas) costs a run, not a
/// byte a row.
fn put_runs(out: &mut Vec<u8>, values: &[u64]) {
    let mut at = 0;
    let mut literals = 0..0;
    let flush = |out: &mut Vec<u8>, literals: &std::ops::Range<usize>| {
        if !literals.is_empty() {
            put_zigzag(out, -(literals.len() as i64));
            for value in &values[literals.clone()] {
                put_varint(out, *value);
            }
        }
    };
    while at < values.len() {
        let mut end = at + 1;
        while end < values.len() && values[end] == values[at] {
            end += 1;
        }
        if end - at >= 2 {
            flush(out, &literals);
            literals = end..end;
            put_zigzag(out, (end - at) as i64);
            put_varint(out, values[at]);
        } else {
            if literals.is_empty() {
                literals = at..at;
            }
            literals.end = end;
        }
        at = end;
    }
    flush(out, &literals);
}

/// Reads `count` values written by [`put_runs`].
fn get_runs(reader: &mut Reader, count: usize) -> io::Result<Vec<u64>> {
    let mut values = Vec::with_capacity(count);
    while values.len() < count {
        let run = reader.zigzag()?;
        let len = run.unsigned_abs() as usize;
        if len == 0 || values.len() + len > count {
            return Err(invalid("a run past its column"));
        }
        if run > 0 {
            let value = reader.varint()?;
            values.resize(values.len() + len, value);
        } else {
            for _ in 0..len {
                values.push(reader.varint()?);
            }
        }
    }
    Ok(values)
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(raw: u64) -> i64 {
    ((raw >> 1) as i64) ^ -((raw & 1) as i64)
}

/// Writes rows column by column, each column in runs ([`put_runs`]). Ids
/// are zigzag deltas against the same column's previous row, so a sorted
/// or repeating column (the entity of an EAV segment, the attribute of an
/// AEV one) costs little more than a run.
fn put_columns(out: &mut Vec<u8>, section: Section, rows: &[&Row]) {
    let delta = |out: &mut Vec<u8>, field: usize, when: &dyn Fn(&Row) -> bool| {
        let mut previous = 0i64;
        let values: Vec<u64> = rows
            .iter()
            .filter(|row| when(row))
            .map(|row| {
                let value = zigzag(row[field] as i64 - previous);
                previous = row[field] as i64;
                value
            })
            .collect();
        put_runs(out, &values);
    };
    let plain = |out: &mut Vec<u8>, value: &dyn Fn(&Row) -> u64| {
        let values: Vec<u64> = rows.iter().map(|row| value(row)).collect();
        put_runs(out, &values);
    };
    match section {
        Section::Entries | Section::Ops => {
            plain(out, &|row| row[0] as u64);
            for slot in 0..SLOTS {
                delta(out, 1 + slot, &|row| slots_of(row[0] as u8) > slot);
            }
            if matches!(section, Section::Ops) {
                plain(out, &|row| (row[DATUM] != NONE) as u64);
            }
            delta(out, DATUM, &|row| row[DATUM] != NONE);
        }
        Section::Links => {
            delta(out, 0, &|_| true);
            // The scale and whether the file holds a frame of the child, as
            // one value (`scale << 1 | no frame`), ahead of the columns
            // that depend on it. A scale takes all eight bits: a node over a
            // large tree has one of 128 or more.
            plain(out, &|row| (row[2] as u64) << 1 | (row[1] == NONE) as u64);
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
        let count = rows.iter().filter(|row| when(row)).count();
        let values = get_runs(reader, count)?;
        let mut previous = 0i64;
        let mut values = values.into_iter();
        for row in rows.iter_mut() {
            if when(row) {
                previous += unzigzag(values.next().expect("a value a row"));
                row[field] = u32::try_from(previous).map_err(|_| invalid("an id over 32 bits"))?;
            }
        }
        Ok(())
    };
    match section {
        Section::Entries | Section::Ops => {
            for (row, form) in rows.iter_mut().zip(get_runs(reader, count)?) {
                row[0] = u8::try_from(form).map_err(|_| invalid("a form over 8 bits"))? as u32;
            }
            for slot in 0..SLOTS {
                delta(&mut rows, reader, 1 + slot, &|row| {
                    slots_of(row[0] as u8) > slot
                })?;
            }
            if matches!(section, Section::Ops) {
                for (row, asserted) in rows.iter_mut().zip(get_runs(reader, count)?) {
                    // Mark retractions; asserted datums are read next.
                    row[DATUM] = if asserted == 1 { 0 } else { NONE };
                }
            }
            delta(&mut rows, reader, DATUM, &|row| row[DATUM] != NONE)?;
        }
        Section::Links => {
            delta(&mut rows, reader, 0, &|_| true)?;
            for (row, flag) in rows.iter_mut().zip(get_runs(reader, count)?) {
                row[2] =
                    u8::try_from(flag >> 1).map_err(|_| invalid("a scale over 8 bits"))? as u32;
                // A child is named by node id or by hash token, never both.
                if flag & 1 == 1 {
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
        let form = dialog_corpus::ENTITY_KEY_TAG as u32;
        // Ids as a real leaf has them: an entity column that rises, the
        // rest scattered, so the whole rows do not collapse into runs.
        let base = segment(
            (0..50)
                .map(|n| {
                    [
                        form,
                        n + 2,
                        (n * 37) % 101,
                        (n * n * 7) % 211,
                        0,
                        (n * 13) % 997 + 100,
                    ]
                })
                .collect(),
        );
        let mut changed = base.sections[0].clone();
        changed[10][DATUM] = 999;
        changed.remove(30);
        changed.insert(40, [form, 41, 77, 5, 0, 200]);
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
                    // Scales of 128 and more, a node over a large tree's.
                    [11, 8, 135, NONE, 0, 0],
                    [13, NONE, 255, 41, 0, 0],
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
    fn it_reads_back_columns_written_in_runs() -> io::Result<()> {
        let columns: [&[u64]; 6] = [
            &[],
            &[7],
            &[3; 100],
            &[1, 2, 1, 2, 1, 2],
            &[5, 5, 9, 4, 4, 4, 0, u64::MAX, u64::MAX, 6],
            &[0, 1, 2, 3, 3, 3, 3, 2, 1, 0],
        ];
        for column in columns {
            let mut bytes = Vec::new();
            put_runs(&mut bytes, column);
            let mut reader = Reader::new(&bytes);
            assert_eq!(get_runs(&mut reader, column.len())?, column);
            assert!(reader.is_empty(), "a column reads to its end");
        }
        let mut long = Vec::new();
        put_runs(&mut long, &[3; 100]);
        assert!(long.len() <= 3, "a repeated value is one run");
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
