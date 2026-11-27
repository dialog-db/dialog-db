//! A session codec for tree edits over Dialog values.
//!
//! Frames start with a version, sequence and operation count. An ID or
//! string is an inline UTF-8 token, a new dictionary entry, or a varint
//! reference. Other values use corpus value bytes. A new encoder and
//! decoder start at sequence zero with an empty dictionary.

use std::collections::HashMap;
use std::sync::Arc;

use dialog_corpus::{Composite, Value, ValueDataType, decode_value, encode_value};

use super::{TreeEdit, TreeId, TreePosition};

const VERSION: u8 = 1;
const TOKENS: usize = 16_384;
const TOKEN_BYTES: usize = 256;
const VALUE_DEPTH: usize = 128;

/// A frame was malformed or did not follow the previous frame.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    /// The framing version is unknown.
    #[error("unsupported tree-edit frame version")]
    Version,
    /// A frame was replayed or a frame is missing.
    #[error("unexpected tree-edit frame sequence, expected {expected}, got {actual}")]
    Sequence {
        /// The next sequence the decoder expected.
        expected: u64,
        /// The sequence encoded in the frame.
        actual: u64,
    },
    /// A length, token, value, or operation could not be decoded.
    #[error("malformed tree-edit frame")]
    Malformed,
}

fn integer(out: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Writes consecutive frames and interns short strings for the session.
#[derive(Default)]
pub struct EditEncoder {
    next: u64,
    tokens: HashMap<TreeId, usize>,
}

impl EditEncoder {
    /// Encode the next ordered batch. Keep this encoder for later
    /// frames sent to the same receiver.
    pub fn encode(&mut self, edits: &[TreeEdit<Value>]) -> Vec<u8> {
        let mut out = vec![VERSION];
        integer(&mut out, self.next);
        integer(&mut out, edits.len() as u64);
        for edit in edits {
            match edit {
                TreeEdit::Insert { id, at, value } => {
                    out.push(0);
                    self.token(&mut out, Some(id));
                    self.position(&mut out, at);
                    self.value(&mut out, value);
                }
                TreeEdit::Set { id, value } => {
                    out.push(1);
                    self.token(&mut out, Some(id));
                    self.value(&mut out, value);
                }
                TreeEdit::Move { id, to } => {
                    out.push(2);
                    self.token(&mut out, Some(id));
                    self.position(&mut out, to);
                }
                TreeEdit::Remove { id } => {
                    out.push(3);
                    self.token(&mut out, Some(id));
                }
            }
        }
        self.next += 1;
        out
    }

    fn position(&mut self, out: &mut Vec<u8>, position: &TreePosition) {
        self.token(out, position.parent.as_deref());
        self.token(out, position.before.as_deref());
    }

    fn token(&mut self, out: &mut Vec<u8>, token: Option<&str>) {
        let Some(token) = token else {
            out.push(0);
            return;
        };
        if let Some(id) = self.tokens.get(token) {
            integer(out, *id as u64 + 3);
            return;
        }
        if token.len() <= TOKEN_BYTES && self.tokens.len() < TOKENS {
            let id = self.tokens.len();
            self.tokens.insert(Arc::from(token), id);
            out.push(2);
        } else {
            out.push(1);
        }
        integer(out, token.len() as u64);
        out.extend_from_slice(token.as_bytes());
    }

    fn value(&mut self, out: &mut Vec<u8>, value: &Value) {
        out.push(u8::from(value.data_type()));
        match value {
            Value::String(text) => self.token(out, Some(text)),
            Value::Composite(value) if value.is_list() => {
                integer(out, value.len() as u64);
                for item in value.items() {
                    self.value(out, &item);
                }
            }
            Value::Composite(value) => {
                // A map can associate multiple values with one key.
                integer(out, value.len() as u64);
                for (key, item) in value.entries() {
                    self.value(out, &key);
                    self.value(out, &item);
                }
            }
            _ => encode_value(value, out),
        }
    }
}

/// Reads the ordered frames of one encoder. A rejected frame leaves the
/// sequence and dictionary unchanged.
#[derive(Default)]
pub struct EditDecoder {
    next: u64,
    tokens: Vec<TreeId>,
}

impl EditDecoder {
    /// Decode and accept one complete frame.
    pub fn decode(&mut self, bytes: &[u8]) -> Result<Vec<TreeEdit<Value>>, FrameError> {
        let mut frame = Frame {
            bytes,
            held: &self.tokens,
            added: Vec::new(),
        };
        if frame.byte()? != VERSION {
            return Err(FrameError::Version);
        }
        let actual = frame.integer()?;
        if actual != self.next {
            return Err(FrameError::Sequence {
                expected: self.next,
                actual,
            });
        }
        let count = frame.length()?;
        if count > frame.bytes.len() / 2 {
            return Err(FrameError::Malformed);
        }
        let mut edits = Vec::new();
        for _ in 0..count {
            let opcode = frame.byte()?;
            let id = frame.id()?;
            edits.push(match opcode {
                0 => TreeEdit::Insert {
                    id,
                    at: frame.position()?,
                    value: frame.value()?,
                },
                1 => TreeEdit::Set {
                    id,
                    value: frame.value()?,
                },
                2 => TreeEdit::Move {
                    id,
                    to: frame.position()?,
                },
                3 => TreeEdit::Remove { id },
                _ => return Err(FrameError::Malformed),
            });
        }
        if !frame.bytes.is_empty() {
            return Err(FrameError::Malformed);
        }
        self.tokens.extend(frame.added);
        self.next += 1;
        Ok(edits)
    }
}

struct Frame<'a> {
    bytes: &'a [u8],
    held: &'a [TreeId],
    added: Vec<TreeId>,
}

impl Frame<'_> {
    fn byte(&mut self) -> Result<u8, FrameError> {
        let (&byte, rest) = self.bytes.split_first().ok_or(FrameError::Malformed)?;
        self.bytes = rest;
        Ok(byte)
    }

    fn integer(&mut self) -> Result<u64, FrameError> {
        let mut result = 0;
        for shift in (0..=63).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                return Err(FrameError::Malformed);
            }
            result |= u64::from(byte & 127) << shift;
            if byte < 128 {
                return Ok(result);
            }
        }
        Err(FrameError::Malformed)
    }

    fn length(&mut self) -> Result<usize, FrameError> {
        usize::try_from(self.integer()?).map_err(|_| FrameError::Malformed)
    }

    fn token(&mut self) -> Result<Option<TreeId>, FrameError> {
        match self.length()? {
            0 => Ok(None),
            form @ (1 | 2) => {
                let len = self.length()?;
                let (text, rest) = self
                    .bytes
                    .split_at_checked(len)
                    .ok_or(FrameError::Malformed)?;
                self.bytes = rest;
                let text: TreeId = std::str::from_utf8(text)
                    .map_err(|_| FrameError::Malformed)?
                    .into();
                if form == 2 {
                    if len > TOKEN_BYTES || self.held.len() + self.added.len() >= TOKENS {
                        return Err(FrameError::Malformed);
                    }
                    self.added.push(text.clone());
                }
                Ok(Some(text))
            }
            id => {
                let id = id - 3;
                self.held
                    .get(id)
                    .or_else(|| self.added.get(id.checked_sub(self.held.len())?))
                    .cloned()
                    .map(Some)
                    .ok_or(FrameError::Malformed)
            }
        }
    }

    fn id(&mut self) -> Result<TreeId, FrameError> {
        self.token()?.ok_or(FrameError::Malformed)
    }

    fn position(&mut self) -> Result<TreePosition, FrameError> {
        Ok(TreePosition {
            parent: self.token()?,
            before: self.token()?,
        })
    }

    fn value(&mut self) -> Result<Value, FrameError> {
        self.value_at(0)
    }

    fn value_at(&mut self, depth: usize) -> Result<Value, FrameError> {
        let tag = self.byte()?;
        let kind = ValueDataType::from(tag);
        if u8::from(kind) != tag {
            return Err(FrameError::Malformed);
        }
        if kind == ValueDataType::String {
            return Ok(Value::String(self.id()?.to_string()));
        }
        if matches!(kind, ValueDataType::Map | ValueDataType::List) {
            if depth >= VALUE_DEPTH {
                return Err(FrameError::Malformed);
            }
            let count = self.length()?;
            if count > self.bytes.len() {
                return Err(FrameError::Malformed);
            }
            let value = if kind == ValueDataType::List {
                let mut items = Vec::new();
                for _ in 0..count {
                    items.push(self.value_at(depth + 1)?);
                }
                Composite::list(items)
            } else {
                let mut entries = Vec::new();
                for _ in 0..count {
                    entries.push((self.value_at(depth + 1)?, self.value_at(depth + 1)?));
                }
                Composite::map(entries)
            };
            return value.map(Value::from).map_err(|_| FrameError::Malformed);
        }
        let (value, rest) = decode_value(kind, self.bytes).ok_or(FrameError::Malformed)?;
        self.bytes = rest;
        Ok(value)
    }
}
