//! A composite value in a key, kept as the names of its atoms.
//!
//! A tree key holds a composite as its type byte and its canonical bytes,
//! escaped and terminated as a byte string is. Those bytes start with the
//! composite's table of atoms, each a scalar it holds as the bytes a key
//! holds that scalar in, and its body refers to the atoms by their place in
//! the table. The file keeps each atom as a token of its own (the token a
//! key holding that scalar alone names too) and the composite as a packed
//! token that names its atoms:
//!
//! ```text
//! packed := 22 · FF 'C' 'T' 01 · count · id… · body
//! ```
//!
//! `count` is a varint, each `id` four bytes little-endian, in the table's
//! order, and `body` is the body as the canonical bytes hold it. An id is
//! fixed-width so that naming a drafted atom by its place in the file, when
//! an append lays out, changes no token's length.
//!
//! A packed token is told from a composite kept whole by its second byte.
//! A composite kept whole is `22` and its escaped canonical bytes, or an
//! escaped prefix of them when the value spilled, and those start with the
//! table's head, a byte below `0x10`, escaped as `00 FF` when it is zero;
//! never with `FF`.
//!
//! A composite is packed only when the packed token realizes to the very
//! bytes the key holds. A spilled composite, whose key holds only a prefix
//! of its bytes, is kept whole on purpose, and so is any value whose bytes
//! do not split or would not come back the same: a store never returns a
//! key other than the one it was given.

use std::fmt;
use std::io;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use dialog_corpus::{
    ATTRIBUTE_KEY_TAG, COVERAGE_KEY_TAG, Composite, ENTITY_KEY_TAG, HISTORY_KEY_TAG, VALUE_KEY_TAG,
    ValueDataType, decode_bytes_cow, encode_bytes,
};

use crate::rows::{Reader, put_varint};
use crate::tokens::{Draft, TokenRead};

/// What follows the type byte of a packed token, before its count.
pub(crate) const MAGIC: &[u8] = &[0xFF, b'C', b'T', 1];

const COMPOSITE: u8 = ValueDataType::Composite as u8;

/// How a packed token fails to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PackedError {
    /// It ends before the names of the atoms its count says it has.
    Truncated,
    /// It names an atom the file does not hold.
    MissingAtom(u32),
    /// It names an atom by a drafted id no token of the append took.
    Unresolved(u32),
}

impl fmt::Display for PackedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PackedError::Truncated => {
                write!(f, "a packed composite token ends before its atoms' names")
            }
            PackedError::MissingAtom(id) => {
                write!(
                    f,
                    "a packed composite token names atom {id}, which the file lacks"
                )
            }
            PackedError::Unresolved(id) => write!(
                f,
                "a packed composite token names drafted atom {id}, which no token took"
            ),
        }
    }
}

impl std::error::Error for PackedError {}

impl From<PackedError> for io::Error {
    fn from(error: PackedError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, error)
    }
}

/// How many composites whose key holds them whole were interned whole
/// anyway, because they did not split or would not realize to the same
/// bytes: none, for composites the corpus wrote. Kept for tests, which
/// assert it does not move.
#[cfg(test)]
static KEPT_WHOLE: AtomicU64 = AtomicU64::new(0);

/// How many times a composite has been split and packed: what a test
/// counts the work of interning by.
#[cfg(test)]
pub(crate) static PACKS: AtomicU64 = AtomicU64::new(0);

/// Whether a draft remembers the composites it packed: a measurement turns
/// it off to weigh what remembering saves.
#[cfg(test)]
pub(crate) static MEMO: AtomicBool = AtomicBool::new(true);

/// Whether composites are packed: a measurement turns it off to weigh a
/// file that keeps every composite whole.
#[cfg(test)]
pub(crate) static PACKING: AtomicBool = AtomicBool::new(true);

/// How many inline composites have been interned whole so far.
#[cfg(test)]
pub(crate) fn kept_whole() -> u64 {
    KEPT_WHOLE.load(Ordering::Relaxed)
}

/// Where the value is among a split key's tokens, as [`key_tokens`]
/// returns them: always the last, whatever the ordering. `None` for a key
/// that does not split into a value.
///
/// [`key_tokens`]: super::key_tokens
pub(crate) fn value_slot(form: u8, count: usize) -> Option<usize> {
    match form {
        ENTITY_KEY_TAG | ATTRIBUTE_KEY_TAG | VALUE_KEY_TAG | HISTORY_KEY_TAG | COVERAGE_KEY_TAG => {
            count.checked_sub(1)
        }
        _ => None,
    }
}

/// Whether `value` is a packed token.
pub(crate) fn is_packed(value: &[u8]) -> bool {
    value.first() == Some(&COMPOSITE) && value[1..].starts_with(MAGIC)
}

/// A packed token's count of atoms and where their ids start, checked to
/// fit in it.
fn header(value: &[u8]) -> Result<(usize, usize), PackedError> {
    let after = 1 + MAGIC.len();
    let mut reader = Reader::new(value.get(after..).ok_or(PackedError::Truncated)?);
    let count = reader.varint().map_err(|_| PackedError::Truncated)?;
    let start = after + reader.position();
    let fits = usize::try_from(count)
        .ok()
        .and_then(|count| count.checked_mul(4))
        .is_some_and(|ids| ids <= value.len() - start);
    if !fits {
        return Err(PackedError::Truncated);
    }
    Ok((count as usize, start))
}

/// The id at `at` of a packed token.
fn id_at(value: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(value[at..at + 4].try_into().expect("four bytes"))
}

/// Writes the value token `value` as the key holds it: a packed token
/// realized to its type byte and its escaped canonical bytes, any other
/// token as it is.
pub(crate) fn realize<T: TokenRead + ?Sized>(
    value: &[u8],
    dictionary: &T,
    out: &mut Vec<u8>,
) -> io::Result<()> {
    if !is_packed(value) {
        out.extend_from_slice(value);
        return Ok(());
    }
    let (count, start) = header(value)?;
    let mut atoms = Vec::with_capacity(count);
    for at in (start..start + 4 * count).step_by(4) {
        let id = id_at(value, at);
        atoms.push(dictionary.get(id).ok_or(PackedError::MissingAtom(id))?);
    }
    let mut bytes = Vec::new();
    Composite::join_atoms(&atoms, &value[start + 4 * count..], &mut bytes);
    out.push(COMPOSITE);
    encode_bytes(&bytes, out);
    Ok(())
}

/// Interns the value token `value` of a key: a composite the key holds
/// whole as a packed token naming its atoms, interned too; anything else,
/// a `spilled` value among them, byte for byte.
pub(crate) fn intern_value(tokens: &mut Draft<'_>, value: &[u8], spilled: bool) -> u32 {
    if spilled || value.first() != Some(&COMPOSITE) {
        return tokens.intern(value);
    }
    #[cfg(test)]
    if !PACKING.load(Ordering::Relaxed) {
        return tokens.intern(value);
    }
    #[cfg(test)]
    let remembered = MEMO.load(Ordering::Relaxed);
    #[cfg(not(test))]
    let remembered = true;
    if remembered && let Some(id) = tokens.packed(value) {
        return id;
    }
    match pack(tokens, value) {
        Some(packed) => {
            let id = tokens.intern(&packed);
            tokens.remember_packed(value, id);
            id
        }
        None => {
            #[cfg(test)]
            KEPT_WHOLE.fetch_add(1, Ordering::Relaxed);
            tokens.intern(value)
        }
    }
}

/// The packed token of the composite value `value`, its atoms interned;
/// `None`, with nothing interned, when its bytes do not split or the
/// packed token would not realize to `value` again.
fn pack(tokens: &mut Draft<'_>, value: &[u8]) -> Option<Vec<u8>> {
    #[cfg(test)]
    PACKS.fetch_add(1, Ordering::Relaxed);
    let (bytes, rest) = decode_bytes_cow(&value[1..])?;
    if !rest.is_empty() {
        return None;
    }
    let (atoms, body) = Composite::split_atoms(&bytes)?;
    let mark = tokens.pending();
    let mut packed = Vec::with_capacity(1 + MAGIC.len() + 5 + 4 * atoms.len() + body.len());
    packed.push(COMPOSITE);
    packed.extend_from_slice(MAGIC);
    put_varint(&mut packed, atoms.len() as u64);
    for atom in atoms {
        packed.extend_from_slice(&tokens.intern(atom).to_le_bytes());
    }
    packed.extend_from_slice(body);
    let mut back = Vec::with_capacity(value.len());
    if realize(&packed, &*tokens, &mut back).is_ok() && back == value {
        Some(packed)
    } else {
        tokens.roll_back(mark);
        None
    }
}

/// Renames the atoms a packed token names through `rename`, in place: a
/// drafted id to the id the token took in the file. Any other token is
/// left as it is.
pub(crate) fn remap(
    value: &mut [u8],
    mut rename: impl FnMut(u32) -> Result<u32, PackedError>,
) -> io::Result<()> {
    if !is_packed(value) {
        return Ok(());
    }
    let (count, start) = header(value)?;
    for at in (start..start + 4 * count).step_by(4) {
        let id = rename(id_at(value, at))?;
        value[at..at + 4].copy_from_slice(&id.to_le_bytes());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
