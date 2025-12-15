use rkyv::{Archive, Deserialize, de::Pool, rancor::Strategy};

use crate::DialogSearchTreeError;

pub fn into_owned<T>(archived: &T::Archived) -> Result<T, DialogSearchTreeError>
where
    T: Archive,
    T::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    rkyv::deserialize(archived).map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))
}
