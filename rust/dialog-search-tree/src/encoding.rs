use rkyv::{Archive, Deserialize, de::Pool, rancor::Strategy};

use crate::DialogSearchTreeError;

/// Deserializes an archived value into an owned value.
///
/// The [`Archive::Archived`] type is a zero-copy representation that references
/// serialized bytes directly in memory. This function deserializes it into a
/// fully owned value that can be used independently of the original buffer.
pub fn into_owned<T>(archived: &T::Archived) -> Result<T, DialogSearchTreeError>
where
    T: Archive,
    T::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    rkyv::deserialize(archived).map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))
}
