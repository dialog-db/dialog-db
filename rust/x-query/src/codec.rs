use serde::{Serialize, de::DeserializeOwned};

use crate::XQueryError;

pub trait Codec {
    type Error: Into<XQueryError>;

    fn serialize<T>(value: T) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize;
    fn deserialize<T, U>(bytes: U) -> Result<T, Self::Error>
    where
        T: DeserializeOwned,
        U: AsRef<[u8]>;
}
