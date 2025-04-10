use rand::Rng;

use crate::Blake3Hash;

/// Produces a [Reference], which is a type-alias for a 32-byte array; in practice, these
/// bytes are the BLAKE3 hash of the inputs to this function
pub fn make_reference<B>(bytes: B) -> Blake3Hash
where
    B: AsRef<[u8]>,
{
    blake3::hash(bytes.as_ref()).as_bytes().to_owned().into()
}

pub(crate) fn make_seed() -> [u8; 32] {
    rand::rng().random()
}
