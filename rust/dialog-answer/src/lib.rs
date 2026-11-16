//! A query's answer as a binary frame that the other side of a wire reads
//! in place.
//!
//! An answer travels from the engine (a service worker, a shared worker, a
//! native worker) to whatever renders it (a page, a guest, a CLI). Encoding
//! it as JSON copies every value into a string the receiver parses back
//! into an object graph, whether or not it reads the value. A frame is laid
//! out like a leaf of the dialog search tree instead:
//!
//! - every distinct value (a token: an entity, a text, a number) is written
//!   once, and
//! - each column of the answer is a lane of token ids, one per row.
//!
//! The tokens belong to the [`Session`], as a chronicle's tokens belong to
//! its file: a token one frame wrote is referenced by id from every later
//! frame of the session, so a subscription's deltas carry only the values
//! the session has not seen. The receiving [`Dictionary`] holds each frame's
//! bytes and reads a cell by slicing them; nothing is decoded until a cell
//! is read, and a cell nobody reads costs nothing.
//!
//! # Layout
//!
//! Little-endian throughout.
//!
//! ```text
//! frame    := "DAF1" kind:u8 base:u32 tokens columns section+
//! tokens   := count:u32 (tag:u8 length:u32 bytes)*      ids base, base+1, ...
//! columns  := count:u16 name:u32*                        names are tokens
//! section  := rows:u32 lane*                             one lane per column
//! lane     := id:u32 * rows                              ABSENT, UNBOUND or a token id
//! ```
//!
//! A snapshot has one section; a delta two, the asserted rows then the
//! retracted ones.
//!
//! ```
//! use dialog_answer::{Dictionary, Kind, Scalar, Session};
//!
//! let mut session = Session::default();
//! let mut frame = session.frame(Kind::Snapshot, &["this", "name"]);
//! frame.row([Some(Scalar::Entity("did:key:z6Mk")), Some(Scalar::Text("Alice"))]);
//! let bytes = frame.finish();
//!
//! let mut dictionary = Dictionary::default();
//! let answer = dictionary.accept(bytes.into()).unwrap();
//! let name = answer.column("name").unwrap();
//! let row = answer.section(0).row(0);
//! assert_eq!(row.get(&dictionary, name), Some(Scalar::Text("Alice")));
//! ```

#![warn(missing_docs)]

mod read;
mod scalar;
mod write;

pub use read::*;
pub use scalar::*;
pub use write::*;

/// The magic a frame starts with.
pub const MAGIC: &[u8; 4] = b"DAF1";

/// A lane's id for a field the row found absent.
pub const ABSENT: u32 = u32::MAX;

/// A lane's id for a field the row left unbound.
pub const UNBOUND: u32 = u32::MAX - 1;

/// What a frame carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The whole answer: one section.
    Snapshot = 0,
    /// A change to it: the asserted rows, then the retracted ones.
    Delta = 1,
}

/// Why a frame could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The bytes are not a frame of this version.
    Magic,
    /// The frame ended early.
    Truncated,
    /// The frame's tokens do not follow the session's: a frame was lost,
    /// or this frame belongs to another session.
    Sequence {
        /// The id the dictionary expected the frame's first token to have.
        expected: u32,
        /// The id the frame says it has.
        found: u32,
    },
    /// A lane names a token the session never sent.
    Unknown(u32),
    /// A text token is not UTF-8, or a number token has the wrong width.
    Malformed(u32),
    /// An unknown frame kind or token tag.
    Tag(u8),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Magic => write!(f, "not an answer frame"),
            Error::Truncated => write!(f, "answer frame ends early"),
            Error::Sequence { expected, found } => {
                write!(
                    f,
                    "answer frame's tokens start at {found}, expected {expected}"
                )
            }
            Error::Unknown(id) => write!(f, "answer frame names unknown token {id}"),
            Error::Malformed(id) => write!(f, "token {id} is malformed"),
            Error::Tag(tag) => write!(f, "unknown tag {tag}"),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests;
