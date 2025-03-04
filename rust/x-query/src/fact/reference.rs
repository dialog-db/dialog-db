/// A type-alias for a 32-byte array
pub type Reference = [u8; 32];

/// The low bound of valid [Reference]s (all zeroes)
pub const REFERENCE_MIN: [u8; 32] = [u8::MIN; 32];
/// The high bound of valid [Reference]s (every bit set to 1)
pub const REFERENCE_MAX: [u8; 32] = [u8::MAX; 32];

/// Produces a [Reference], which is a type-alias for a 32-byte array; in practice, these
/// bytes are the BLAKE3 hash of the inputs to this function
pub fn make_reference<B>(bytes: B) -> Reference
where
    B: AsRef<[u8]>,
{
    blake3::hash(bytes.as_ref()).as_bytes().to_owned()
}
