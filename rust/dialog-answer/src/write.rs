use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use crate::{ABSENT, Kind, MAGIC, Scalar, UNBOUND};

/// The writing side of a session: every token the session has sent, by
/// its bytes, so a frame refers to a token it sent before by id instead of
/// writing it again.
///
/// One session per receiver: the [`Dictionary`](crate::Dictionary) on the
/// other side must accept every frame the session writes, in order.
#[derive(Debug, Default)]
pub struct Session {
    tokens: HashMap<Box<[u8]>, u32, BuildHasherDefault<Fnv>>,
    next: u32,
    /// The tag and bytes of the token being looked up, reused.
    probe: Vec<u8>,
}

impl Session {
    /// How many tokens the session has sent.
    pub fn len(&self) -> usize {
        self.next as usize
    }

    /// Whether the session has sent no token.
    pub fn is_empty(&self) -> bool {
        self.next == 0
    }

    /// Starts a frame of `kind` with `columns`, in the order its rows give
    /// their cells.
    pub fn frame<'s>(&'s mut self, kind: Kind, columns: &[&str]) -> Frame<'s> {
        let base = self.next;
        let mut frame = Frame {
            session: self,
            kind,
            base,
            count: 0,
            tokens: Vec::new(),
            columns: Vec::with_capacity(columns.len()),
            sections: vec![Section::new(columns.len())],
        };
        for name in columns {
            let id = frame.intern(Scalar::Text(name));
            frame.columns.push(id);
        }
        frame
    }
}

#[derive(Debug)]
struct Section {
    rows: u32,
    lanes: Vec<Vec<u32>>,
}

impl Section {
    fn new(columns: usize) -> Self {
        Section {
            rows: 0,
            lanes: vec![Vec::new(); columns],
        }
    }
}

/// A frame being written: rows go in, and [`Frame::finish`] lays out the
/// bytes. Tokens new to the session are written into the frame as they
/// are met; the session remembers them as soon as they are, so a frame
/// that is started must be finished and sent.
#[derive(Debug)]
pub struct Frame<'s> {
    session: &'s mut Session,
    kind: Kind,
    base: u32,
    count: u32,
    tokens: Vec<u8>,
    columns: Vec<u32>,
    sections: Vec<Section>,
}

impl Frame<'_> {
    /// The id of `scalar`'s token, written into this frame if the session
    /// has not sent it.
    fn intern(&mut self, scalar: Scalar<'_>) -> u32 {
        let mut scratch = [0u8; 16];
        let (tag, bytes) = scalar.encode(&mut scratch);
        let probe = &mut self.session.probe;
        probe.clear();
        probe.push(tag as u8);
        probe.extend_from_slice(bytes);
        if let Some(id) = self.session.tokens.get(probe.as_slice()) {
            return *id;
        }
        let id = self.session.next;
        self.session.next += 1;
        self.session
            .tokens
            .insert(probe.as_slice().into(), id);
        self.tokens.push(tag as u8);
        self.tokens
            .extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        self.tokens.extend_from_slice(bytes);
        self.count += 1;
        id
    }

    /// Adds a row to the current section: a cell per column, `None` for a
    /// field the row left unbound and `Some(None)`-style absence through
    /// [`Frame::row_with`].
    pub fn row<'v>(&mut self, cells: impl IntoIterator<Item = Option<Scalar<'v>>>) {
        self.row_with(cells.into_iter().map(|cell| cell.map(Some)))
    }

    /// Adds a row whose cells distinguish an unbound field (`None`) from
    /// an absent one (`Some(None)`).
    pub fn row_with<'v>(&mut self, cells: impl IntoIterator<Item = Option<Option<Scalar<'v>>>>) {
        let at = self.sections.len() - 1;
        let mut column = 0;
        for cell in cells {
            let id = match cell {
                None => UNBOUND,
                Some(None) => ABSENT,
                Some(Some(scalar)) => self.intern(scalar),
            };
            self.sections[at].lanes[column].push(id);
            column += 1;
        }
        let section = &mut self.sections[at];
        for lane in &mut section.lanes[column..] {
            lane.push(UNBOUND);
        }
        section.rows += 1;
    }

    /// Ends the asserted rows of a delta; the rows after are retracted.
    pub fn retracted(&mut self) {
        debug_assert_eq!(self.kind, Kind::Delta, "only a delta retracts");
        if self.sections.len() < 2 {
            self.sections.push(Section::new(self.columns.len()));
        }
    }

    /// The frame's bytes.
    pub fn finish(mut self) -> Vec<u8> {
        if self.kind == Kind::Delta && self.sections.len() < 2 {
            self.sections.push(Section::new(self.columns.len()));
        }
        let lanes: usize = self
            .sections
            .iter()
            .map(|section| 4 + section.lanes.len() * section.rows as usize * 4)
            .sum();
        let mut out =
            Vec::with_capacity(13 + self.tokens.len() + 2 + self.columns.len() * 4 + lanes);
        out.extend_from_slice(MAGIC);
        out.push(self.kind as u8);
        out.extend_from_slice(&self.base.to_le_bytes());
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&self.tokens);
        out.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());
        for id in &self.columns {
            out.extend_from_slice(&id.to_le_bytes());
        }
        for section in &self.sections {
            out.extend_from_slice(&section.rows.to_le_bytes());
            for lane in &section.lanes {
                for id in lane {
                    out.extend_from_slice(&id.to_le_bytes());
                }
            }
        }
        out
    }
}

/// FNV-1a: the session's token table is probed once per cell, and SipHash
/// over a short token costs more than the rest of the cell.
#[derive(Debug, Clone, Copy)]
pub struct Fnv(u64);

impl Default for Fnv {
    fn default() -> Self {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
}

impl Hasher for Fnv {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
        }
    }
}
