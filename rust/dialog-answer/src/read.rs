use std::sync::Arc;

use crate::scalar::{Tag, width};
use crate::{ABSENT, Error, Kind, MAGIC, Scalar, UNBOUND};

/// The reading side of a session: the bytes of every frame it accepted,
/// and where each token sits in them. A cell is read by slicing the frame
/// that wrote its token; nothing is copied out of a frame.
#[derive(Debug, Default)]
pub struct Dictionary {
    frames: Vec<Arc<[u8]>>,
    tokens: Vec<Token>,
}

#[derive(Debug, Clone, Copy)]
struct Token {
    frame: u32,
    at: u32,
    len: u32,
    tag: Tag,
}

/// The fewest bytes a token takes in a frame: its tag and its length.
const TOKEN_HEAD: usize = 1 + 4;

impl Token {
    /// The token's bytes in `frame`, the frame that wrote it.
    fn slice<'a>(&self, frame: &'a [u8]) -> &'a [u8] {
        &frame[self.at as usize..(self.at + self.len) as usize]
    }
}

/// One accepted frame: its columns and where its sections' lanes sit.
#[derive(Debug, Clone)]
pub struct Answer {
    bytes: Arc<[u8]>,
    kind: Kind,
    columns: Vec<Box<str>>,
    sections: Vec<(u32, usize)>,
}

/// What a row holds for one column.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Cell<'a> {
    /// The row left the field unbound.
    Unbound,
    /// The row found the field absent.
    Absent,
    /// The field's value.
    Value(Scalar<'a>),
}

/// The rows of one section of an [`Answer`].
#[derive(Debug, Clone, Copy)]
pub struct Rows<'a> {
    answer: &'a Answer,
    rows: u32,
    lanes: usize,
}

/// One row of a section.
#[derive(Debug, Clone, Copy)]
pub struct Row<'a> {
    rows: Rows<'a>,
    index: u32,
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self.at.checked_add(n).ok_or(Error::Truncated)?;
        let slice = self.bytes.get(self.at..end).ok_or(Error::Truncated)?;
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("two bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }
}

impl Dictionary {
    /// How many tokens the session has received.
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// Whether the session has received no token.
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Accepts the session's next frame: checks it, learns its tokens and
    /// keeps its bytes, which its tokens are read from from then on.
    ///
    /// A refused frame changes nothing: every check, the column names'
    /// included, runs before the dictionary learns a token, so the
    /// session's real next frame is still accepted after it.
    pub fn accept(&mut self, bytes: Arc<[u8]>) -> Result<Answer, Error> {
        let mut cursor = Cursor {
            bytes: &bytes,
            at: 0,
        };
        if cursor.take(4)? != MAGIC {
            return Err(Error::Magic);
        }
        let kind = match cursor.u8()? {
            0 => Kind::Snapshot,
            1 => Kind::Delta,
            tag => return Err(Error::Tag(tag)),
        };
        let base = cursor.u32()?;
        if base as usize != self.tokens.len() {
            return Err(Error::Sequence {
                expected: self.tokens.len() as u32,
                found: base,
            });
        }
        let count = cursor.u32()?;
        let frame = self.frames.len() as u32;
        // The count is the sender's word: reserve no more tokens than the
        // frame's remaining bytes could hold, each at least a tag and a
        // length, so a short frame declaring billions is refused as
        // truncated instead of reserving room for them.
        let room = (bytes.len() - cursor.at) / TOKEN_HEAD;
        let mut learned = Vec::with_capacity((count as usize).min(room));
        for index in 0..count {
            let id = base + index;
            let tag = cursor.u8()?;
            let tag = Tag::from_byte(tag).ok_or(Error::Tag(tag))?;
            let len = cursor.u32()?;
            let at = cursor.at;
            let token = cursor.take(len as usize)?;
            if tag.is_text() && std::str::from_utf8(token).is_err() {
                return Err(Error::Malformed(id));
            }
            if width(tag).is_some_and(|width| width != len as usize) {
                return Err(Error::Malformed(id));
            }
            learned.push(Token {
                frame,
                at: at as u32,
                len,
                tag,
            });
        }
        let known = base + count;
        let column_count = cursor.u16()? as usize;
        let mut column_ids = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            column_ids.push(cursor.u32()?);
        }
        let mut sections = Vec::new();
        let expected = if kind == Kind::Delta { 2 } else { 1 };
        for _ in 0..expected {
            let rows = cursor.u32()?;
            let at = cursor.at;
            let lane_bytes = (rows as usize)
                .checked_mul(column_count * 4)
                .ok_or(Error::Truncated)?;
            let lanes = cursor.take(lane_bytes)?;
            for id in lanes.as_chunks::<4>().0 {
                let id = u32::from_le_bytes(*id);
                if id != ABSENT && id != UNBOUND && id >= known {
                    return Err(Error::Unknown(id));
                }
            }
            sections.push((rows, at));
        }
        // A column is named by a token the session sent before or this
        // frame sends: resolved before anything is learned.
        let columns = column_ids
            .into_iter()
            .map(|id| {
                let name = if id < base {
                    self.read(id)
                } else if id < known {
                    let token = &learned[(id - base) as usize];
                    Some(Scalar::decode(token.tag, token.slice(&bytes)))
                } else {
                    return Err(Error::Unknown(id));
                };
                match name {
                    Some(Scalar::Text(name)) => Ok(Box::<str>::from(name)),
                    _ => Err(Error::Malformed(id)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.tokens.extend(learned);
        self.frames.push(bytes.clone());
        Ok(Answer {
            bytes,
            kind,
            columns,
            sections,
        })
    }

    /// The scalar token `id` holds.
    pub fn read(&self, id: u32) -> Option<Scalar<'_>> {
        let token = self.tokens.get(id as usize)?;
        let frame = &self.frames[token.frame as usize];
        Some(Scalar::decode(token.tag, token.slice(frame)))
    }
}

impl Answer {
    /// Whether this is a snapshot or a delta.
    pub fn kind(&self) -> Kind {
        self.kind
    }

    /// The answer's columns, in order.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(AsRef::as_ref)
    }

    /// The index of the column named `name`.
    pub fn column(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| &**column == name)
    }

    /// The rows of section `index`: a snapshot's rows, or a delta's
    /// asserted (0) or retracted (1) rows.
    ///
    /// # Panics
    ///
    /// When the answer has no such section: past 0 for a snapshot, past 1
    /// for a delta. The count follows from [`Self::kind`], not from the
    /// frame's bytes.
    pub fn section(&self, index: usize) -> Rows<'_> {
        let (rows, lanes) = self.sections[index];
        Rows {
            answer: self,
            rows,
            lanes,
        }
    }

    /// The frame's size, in bytes.
    pub fn size(&self) -> usize {
        self.bytes.len()
    }
}

impl<'a> Rows<'a> {
    /// How many rows.
    pub fn len(&self) -> usize {
        self.rows as usize
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Row `index`.
    pub fn row(&self, index: usize) -> Row<'a> {
        assert!(index < self.rows as usize, "row {index} out of range");
        Row {
            rows: *self,
            index: index as u32,
        }
    }

    /// Every row.
    pub fn iter(&self) -> impl Iterator<Item = Row<'a>> + 'a {
        let rows = *self;
        (0..self.rows).map(move |index| Row { rows, index })
    }
}

impl<'a> Row<'a> {
    /// The token id the row holds for `column`, or [`ABSENT`] / [`UNBOUND`];
    /// `None` when the answer has no such column. A lane is read only
    /// within its own section: an index past the columns would land on the
    /// next section's bytes.
    pub fn id(&self, column: usize) -> Option<u32> {
        if column >= self.rows.answer.columns.len() {
            return None;
        }
        let at = self.rows.lanes + (column * self.rows.rows as usize + self.index as usize) * 4;
        let bytes = &self.rows.answer.bytes[at..at + 4];
        Some(u32::from_le_bytes(bytes.try_into().expect("four bytes")))
    }

    /// What the row holds for `column`, read from `dictionary`: unbound
    /// for a column the answer does not have, as nothing is bound there.
    pub fn cell<'d>(&self, dictionary: &'d Dictionary, column: usize) -> Cell<'d> {
        match self.id(column) {
            None | Some(UNBOUND) => Cell::Unbound,
            Some(ABSENT) => Cell::Absent,
            Some(id) => dictionary
                .read(id)
                .map(Cell::Value)
                .unwrap_or(Cell::Unbound),
        }
    }

    /// The value the row holds for `column`, if it holds one.
    pub fn get<'d>(&self, dictionary: &'d Dictionary, column: usize) -> Option<Scalar<'d>> {
        match self.cell(dictionary, column) {
            Cell::Value(scalar) => Some(scalar),
            _ => None,
        }
    }
}
