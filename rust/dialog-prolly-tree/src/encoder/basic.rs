mod io;
use std::{fmt::Display, marker::PhantomData};

pub use io::*;

use async_trait::async_trait;
use dialog_storage::{DialogStorageError, Encoder};
use nonempty::NonEmpty;

use crate::{Block, BlockType, Entry, KeyType, Reference, ValueType};

/// A [`BasicEncoder`] encodes blocks as a compact byte representation. It
/// includes support for data structures that contain unsigned integers and byte
/// arrays
#[derive(Clone)]
pub struct BasicEncoder<Key, Value>(PhantomData<Key>, PhantomData<Value>)
where
    Key: KeyType + 'static,
    <Key as TryFrom<Vec<u8>>>::Error: Display,
    Value: ValueType,
    <Value as TryFrom<Vec<u8>>>::Error: Display;

impl<Key, Value> Default for BasicEncoder<Key, Value>
where
    Key: KeyType + 'static,
    <Key as TryFrom<Vec<u8>>>::Error: Display,
    Value: ValueType,
    <Value as TryFrom<Vec<u8>>>::Error: Display,
{
    fn default() -> Self {
        Self(PhantomData, PhantomData)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> Encoder<32> for BasicEncoder<Key, Value>
where
    Key: KeyType + 'static,
    <Key as TryFrom<Vec<u8>>>::Error: Display,
    Value: ValueType,
    <Value as TryFrom<Vec<u8>>>::Error: Display,
{
    type Bytes = Vec<u8>;
    type Hash = [u8; 32];
    type Error = DialogStorageError;

    async fn encode<T>(&self, block: &T) -> Result<(Self::Hash, Self::Bytes), Self::Error> {
        todo!();
        // let mut writer = Writer::new();
        // let block_type = BlockType::from(block);
        // writer
        //     .write(&block_type)
        //     .map_err(|error| DialogStorageError::EncodeFailed(format!("{error}")))?;

        // match block_type {
        //     BlockType::Branch => {
        //         let refs = block.references()?;
        //         writer.write_u32(
        //             refs.len()
        //                 .try_into()
        //                 .map_err(|error| DialogStorageError::EncodeFailed(format!("{error}")))?,
        //         )?;
        //         for node_ref in refs {
        //             writer.write(&node_ref.upper_bound().as_ref())?;
        //             writer.write::<&[u8]>(&node_ref.hash().as_ref())?;
        //         }
        //     }
        //     BlockType::Segment => {
        //         let entries = block.entries()?;
        //         writer.write_u32(
        //             entries
        //                 .len()
        //                 .try_into()
        //                 .map_err(|error| DialogStorageError::EncodeFailed(format!("{error}")))?,
        //         )?;
        //         for entry in entries {
        //             writer.write(&entry.key.as_ref())?;
        //             writer.write(&entry.value.serialize().as_ref())?;
        //         }
        //     }
        // }

        // let bytes = writer.into_inner();
        // let hash = blake3::hash(&bytes).as_bytes().to_owned();

        // Ok((hash, bytes))
    }

    /// Decode bytes into a `Block`.
    async fn decode<T>(&self, bytes: &[u8]) -> Result<T, Self::Error> {
        todo!();
        // let reader = Reader::new(bytes);
        // let block_type = reader.read::<BlockType>()?;
        // let child_count = reader.read_u32()?;

        // match block_type {
        //     BlockType::Branch => {
        //         let mut children = vec![];
        //         for _ in 0..child_count {
        //             let boundary: Key = reader
        //                 .read::<Vec<u8>>()?
        //                 .try_into()
        //                 .map_err(|error| DialogStorageError::DecodeFailed(format!("{error}")))?;
        //             let hash: Self::Hash = reader.read::<Vec<u8>>()?.try_into().map_err(|_| {
        //                 DialogStorageError::DecodeFailed(
        //                     "Could not convert bytes to hash".to_string(),
        //                 )
        //             })?;
        //             children.push(Reference::new(boundary, hash))
        //         }
        //         let children = NonEmpty::from_vec(children).ok_or_else(|| {
        //             DialogStorageError::DecodeFailed("Branch seems to have zero children".into())
        //         })?;

        //         Ok(Block::branch(children))
        //     }
        //     BlockType::Segment => {
        //         let mut children = vec![];
        //         for _ in 0..child_count {
        //             let key: Key = reader
        //                 .read::<Vec<u8>>()?
        //                 .try_into()
        //                 .map_err(|error| DialogStorageError::DecodeFailed(format!("{error}")))?;
        //             let value: Value = reader
        //                 .read::<Vec<u8>>()?
        //                 .try_into()
        //                 .map_err(|error| DialogStorageError::DecodeFailed(format!("{error}")))?;
        //             children.push(Entry::new(key, value))
        //         }
        //         let children = NonEmpty::from_vec(children).ok_or_else(|| {
        //             DialogStorageError::DecodeFailed("Segment seems to have zero entries".into())
        //         })?;
        //         Ok(Block::segment(children))
        //     }
        // }
    }
}
