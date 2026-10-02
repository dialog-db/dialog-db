//! Bytes that keep the blake3 hash the boundary rules read from them.
//!
//! The coins, the ladders and the anchor elections all decide from
//! `blake3(bytes)` of a key or a separator, and they ask again every time a
//! window is regrouped. The hash is a pure function of the bytes, so whatever
//! owns the bytes keeps it: an entry keeps the hash of its key, a separator
//! its own, and a stored leaf the hashes of the keys in it. It is computed
//! the first time it is asked for, and it lives exactly as long as its owner.

use std::ops::Deref;
use std::sync::{Arc, OnceLock};

use dialog_common::Blake3Hash;

use crate::distribution::audit;

/// The hashes a stored node keeps with its bytes (see
/// `PersistentNode::hashes`): one cell per entry of a leaf, for its key, or
/// per link of an index, for its separator. Everything opened from the node
/// shares the cells, so a hash computed through any opening serves every
/// later one.
pub(crate) type HashColumn = Arc<[OnceLock<Blake3Hash>]>;

/// The blake3 hash of some bytes, computed when first asked for and kept
/// beside them.
///
/// It holds no bytes of its own: its owner passes them in, and must pass the
/// same ones every time. Cloning carries a hash already computed along.
#[derive(Clone, Debug)]
pub struct LazyHash(Cell);

#[derive(Clone, Debug)]
enum Cell {
    /// Bytes no stored node holds yet: the hash is kept here.
    Own(OnceLock<Blake3Hash>),
    /// The bytes at `at` of a stored node: the hash is kept with the node.
    Stored { column: HashColumn, at: u32 },
}

impl Default for LazyHash {
    fn default() -> Self {
        Self(Cell::Own(OnceLock::new()))
    }
}

impl LazyHash {
    /// The cell at `at` of a stored node whose hashes are `column`.
    pub(crate) fn stored(column: &HashColumn, at: usize) -> Self {
        match u32::try_from(at) {
            Ok(at) if (at as usize) < column.len() => Self(Cell::Stored {
                column: column.clone(),
                at,
            }),
            _ => Self::default(),
        }
    }

    /// The hash of `bytes`, which must be the bytes this cell was made for.
    pub fn get(&self, bytes: &[u8]) -> Blake3Hash {
        self.cell().get_or_init(|| compute(bytes)).clone()
    }

    /// The hash, if it has been asked for already.
    pub(crate) fn known(&self) -> Option<Blake3Hash> {
        self.cell().get().cloned()
    }

    fn cell(&self) -> &OnceLock<Blake3Hash> {
        match &self.0 {
            Cell::Own(cell) => cell,
            Cell::Stored { column, at } => &column[*at as usize],
        }
    }
}

/// Hashes `bytes`, counting the work.
fn compute(bytes: &[u8]) -> Blake3Hash {
    audit::hashed(bytes.len());
    Blake3Hash::hash(bytes)
}

/// Bytes a boundary rule decides from, with wherever their hash is kept.
///
/// What [`Distribution`](crate::Distribution) is handed: a rule that reads
/// the hash asks for [`hash`](Self::hash), and one that reads the bytes
/// themselves takes [`bytes`](Self::bytes).
#[derive(Clone, Copy, Debug)]
pub struct Hashed<'a> {
    bytes: &'a [u8],
    hash: Source<'a>,
}

#[derive(Clone, Copy, Debug)]
enum Source<'a> {
    /// Nothing keeps the hash: every ask computes it.
    None,
    /// The owner's cell.
    Lazy(&'a LazyHash),
    /// A hash already in hand.
    Known(&'a Blake3Hash),
}

impl<'a> Hashed<'a> {
    /// `bytes` whose hash is kept in `hash`.
    pub fn new(bytes: &'a [u8], hash: &'a LazyHash) -> Self {
        Self {
            bytes,
            hash: Source::Lazy(hash),
        }
    }

    /// `bytes` whose hash is already known to be `hash`.
    pub fn known(bytes: &'a [u8], hash: &'a Blake3Hash) -> Self {
        Self {
            bytes,
            hash: Source::Known(hash),
        }
    }

    /// The bytes.
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Their blake3 hash.
    pub fn hash(&self) -> Blake3Hash {
        match self.hash {
            Source::None => compute(self.bytes),
            Source::Lazy(hash) => hash.get(self.bytes),
            Source::Known(hash) => hash.clone(),
        }
    }
}

impl Deref for Hashed<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.bytes
    }
}

impl AsRef<[u8]> for Hashed<'_> {
    fn as_ref(&self) -> &[u8] {
        self.bytes
    }
}

/// Bytes nothing keeps the hash of: every ask computes it.
impl<'a> From<&'a [u8]> for Hashed<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            hash: Source::None,
        }
    }
}

impl<'a> From<&'a Vec<u8>> for Hashed<'a> {
    fn from(bytes: &'a Vec<u8>) -> Self {
        Self::from(bytes.as_slice())
    }
}

impl<'a, const N: usize> From<&'a [u8; N]> for Hashed<'a> {
    fn from(bytes: &'a [u8; N]) -> Self {
        Self::from(bytes.as_slice())
    }
}

/// The separator at a node's left edge, with its hash.
///
/// The seam coin ranks a link by `blake3(separator)` every time its parent is
/// regrouped, so the separator keeps that hash. It is replaced whole when the
/// seam moves, never edited in place, which is what keeps the hash true.
#[derive(Clone, Debug, Default)]
pub struct Separator {
    bytes: Vec<u8>,
    hash: LazyHash,
}

impl Separator {
    /// The separator's bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// The separator's bytes, taken out of it.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// The bytes with the cell their hash is kept in.
    pub fn hashed(&self) -> Hashed<'_> {
        Hashed::new(&self.bytes, &self.hash)
    }

    /// The hash, if it has been asked for already.
    pub(crate) fn known_hash(&self) -> Option<Blake3Hash> {
        self.hash.known()
    }

    /// This separator as the one at `at` of a stored index whose hashes are
    /// `column`: its hash is kept with the index from here on.
    pub(crate) fn stored(self, column: &HashColumn, at: usize) -> Self {
        Self {
            bytes: self.bytes,
            hash: LazyHash::stored(column, at),
        }
    }
}

impl From<Vec<u8>> for Separator {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            hash: LazyHash::default(),
        }
    }
}

impl From<&[u8]> for Separator {
    fn from(bytes: &[u8]) -> Self {
        Self::from(bytes.to_vec())
    }
}

impl Deref for Separator {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

impl AsRef<[u8]> for Separator {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl PartialEq for Separator {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for Separator {}

impl PartialEq<[u8]> for Separator {
    fn eq(&self, other: &[u8]) -> bool {
        self.bytes == other
    }
}

impl PartialEq<Vec<u8>> for Separator {
    fn eq(&self, other: &Vec<u8>) -> bool {
        &self.bytes == other
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, OnceLock};

    use dialog_common::Blake3Hash;

    use super::{HashColumn, Hashed, LazyHash, Separator};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[dialog_common::test]
    fn it_answers_the_hash_of_its_bytes_from_every_source() {
        let bytes = b"a key".as_slice();
        let expected = Blake3Hash::hash(bytes);
        let cell = LazyHash::default();

        assert_eq!(Hashed::from(bytes).hash(), expected);
        assert_eq!(Hashed::new(bytes, &cell).hash(), expected);
        assert_eq!(Hashed::known(bytes, &expected).hash(), expected);
        assert_eq!(Separator::from(bytes).hashed().hash(), expected);
    }

    /// A clone carries the hash along: asking the clone does not need the
    /// bytes again, which a clone handed different bytes shows.
    #[dialog_common::test]
    fn it_carries_a_computed_hash_into_its_clones() {
        let cell = LazyHash::default();
        let expected = cell.get(b"first");

        assert_eq!(cell.clone().get(b"never hashed"), expected);
    }

    /// Entries opened from one stored leaf share its cells: a hash computed
    /// through one opening is there for the next, and each key keeps its own.
    #[dialog_common::test]
    fn it_shares_a_stored_leafs_hashes_between_openings() {
        let column: HashColumn = Arc::from(vec![OnceLock::new(), OnceLock::new()]);
        let first = LazyHash::stored(&column, 0).get(b"zero");

        assert_eq!(LazyHash::stored(&column, 0).get(b"never hashed"), first);
        assert_eq!(column[0].get(), Some(&first));
        assert_eq!(column[1].get(), None);
        assert_eq!(
            LazyHash::stored(&column, 1).get(b"one"),
            Blake3Hash::hash(b"one")
        );
    }

    /// A position the column does not cover keeps its hash to itself rather
    /// than reading another key's.
    #[dialog_common::test]
    fn it_keeps_its_own_hash_past_the_end_of_a_column() {
        let column: HashColumn = Arc::from(vec![OnceLock::new()]);

        assert_eq!(
            LazyHash::stored(&column, 1).get(b"beyond"),
            Blake3Hash::hash(b"beyond")
        );
        assert_eq!(column[0].get(), None);
    }
}
