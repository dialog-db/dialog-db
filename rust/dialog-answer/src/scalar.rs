/// One cell's value, as a frame stores it and as a reader reads it: text
/// and bytes borrow from the frame they were read from.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Scalar<'a> {
    /// A UTF-8 string.
    Text(&'a str),
    /// An entity, as its URI.
    Entity(&'a str),
    /// A symbol (an attribute as a value).
    Symbol(&'a str),
    /// A byte buffer.
    Bytes(&'a [u8]),
    /// A record's bytes.
    Record(&'a [u8]),
    /// A natural number.
    Natural(u128),
    /// An integer.
    Integer(i128),
    /// A float.
    Float(f64),
    /// A boolean.
    Boolean(bool),
}

/// A token's tag: which [`Scalar`] its bytes are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Tag {
    Text = 1,
    Entity = 2,
    Symbol = 3,
    Bytes = 4,
    Record = 5,
    Natural = 6,
    Integer = 7,
    Float = 8,
    Boolean = 9,
}

impl Tag {
    pub(crate) fn from_byte(byte: u8) -> Option<Tag> {
        Some(match byte {
            1 => Tag::Text,
            2 => Tag::Entity,
            3 => Tag::Symbol,
            4 => Tag::Bytes,
            5 => Tag::Record,
            6 => Tag::Natural,
            7 => Tag::Integer,
            8 => Tag::Float,
            9 => Tag::Boolean,
            _ => return None,
        })
    }

    pub(crate) fn is_text(self) -> bool {
        matches!(self, Tag::Text | Tag::Entity | Tag::Symbol)
    }
}

impl<'a> Scalar<'a> {
    /// The tag and the bytes a token stores this scalar as, numbers
    /// written into `scratch`.
    pub(crate) fn encode<'s>(&self, scratch: &'s mut [u8; 16]) -> (Tag, &'s [u8])
    where
        'a: 's,
    {
        match *self {
            Scalar::Text(text) => (Tag::Text, text.as_bytes()),
            Scalar::Entity(text) => (Tag::Entity, text.as_bytes()),
            Scalar::Symbol(text) => (Tag::Symbol, text.as_bytes()),
            Scalar::Bytes(bytes) => (Tag::Bytes, bytes),
            Scalar::Record(bytes) => (Tag::Record, bytes),
            Scalar::Natural(n) => {
                *scratch = n.to_le_bytes();
                (Tag::Natural, &scratch[..])
            }
            Scalar::Integer(n) => {
                *scratch = n.to_le_bytes();
                (Tag::Integer, &scratch[..])
            }
            Scalar::Float(n) => {
                scratch[..8].copy_from_slice(&n.to_le_bytes());
                (Tag::Float, &scratch[..8])
            }
            Scalar::Boolean(b) => {
                scratch[0] = b as u8;
                (Tag::Boolean, &scratch[..1])
            }
        }
    }

    /// Reads a token's bytes as the scalar its tag says. The bytes were
    /// checked when their frame was accepted.
    pub(crate) fn decode(tag: Tag, bytes: &'a [u8]) -> Scalar<'a> {
        let text = || std::str::from_utf8(bytes).unwrap_or_default();
        match tag {
            Tag::Text => Scalar::Text(text()),
            Tag::Entity => Scalar::Entity(text()),
            Tag::Symbol => Scalar::Symbol(text()),
            Tag::Bytes => Scalar::Bytes(bytes),
            Tag::Record => Scalar::Record(bytes),
            Tag::Natural => Scalar::Natural(u128::from_le_bytes(sized(bytes))),
            Tag::Integer => Scalar::Integer(i128::from_le_bytes(sized(bytes))),
            Tag::Float => Scalar::Float(f64::from_le_bytes(sized(bytes))),
            Tag::Boolean => Scalar::Boolean(bytes.first().is_some_and(|b| *b != 0)),
        }
    }
}

fn sized<const N: usize>(bytes: &[u8]) -> [u8; N] {
    let mut out = [0; N];
    out.copy_from_slice(&bytes[..N]);
    out
}

/// The width a number token must have.
pub(crate) fn width(tag: Tag) -> Option<usize> {
    match tag {
        Tag::Natural | Tag::Integer => Some(16),
        Tag::Float => Some(8),
        Tag::Boolean => Some(1),
        _ => None,
    }
}
