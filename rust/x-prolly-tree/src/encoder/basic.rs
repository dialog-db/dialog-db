mod io;
pub use io::*;

use async_trait::async_trait;
use nonempty::NonEmpty;
use x_storage::{Encoder, XStorageError};

use crate::{Block, BlockType, Entry, Reference};

/// A [`BasicEncoder`] encodes blocks as a compact byte representation. It
/// includes support for data structures that contain unsigned integers and byte
/// arrays
pub struct BasicEncoder;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Encoder<32> for BasicEncoder {
    type Block = Block<32, Vec<u8>, Vec<u8>, Self::Hash>;
    type Bytes = Vec<u8>;
    type Hash = [u8; 32];
    type Error = XStorageError;

    async fn encode(&self, block: &Self::Block) -> Result<(Self::Hash, Self::Bytes), Self::Error> {
        let mut writer = Writer::new();
        let block_type = BlockType::from(block);
        writer
            .write(&block_type)
            .map_err(|error| XStorageError::EncodeFailed(format!("{error}")))?;

        match block_type {
            BlockType::Branch => {
                let refs = block.references()?;
                writer.write_u32(
                    refs.len()
                        .try_into()
                        .map_err(|error| XStorageError::EncodeFailed(format!("{error}")))?,
                )?;
                for node_ref in refs {
                    writer.write(&node_ref.upper_bound().as_ref())?;
                    writer.write::<&[u8]>(&node_ref.hash().as_ref())?;
                }
            }
            BlockType::Segment => {
                let entries = block.entries()?;
                writer.write_u32(
                    entries
                        .len()
                        .try_into()
                        .map_err(|error| XStorageError::EncodeFailed(format!("{error}")))?,
                )?;
                for entry in entries {
                    writer.write(&entry.key.as_ref())?;
                    writer.write(&entry.value.as_ref())?;
                }
            }
        }

        let bytes = writer.into_inner();
        let hash = blake3::hash(&bytes).as_bytes().to_owned();

        Ok((hash, bytes))
    }

    /// Decode bytes into a `Block`.
    async fn decode(&self, bytes: &[u8]) -> Result<Self::Block, Self::Error> {
        let reader = Reader::new(bytes);
        let block_type = reader.read::<BlockType>()?;
        let child_count = reader.read_u32()?;

        match block_type {
            BlockType::Branch => {
                let mut children = vec![];
                for _ in 0..child_count {
                    let boundary: Vec<u8> = reader
                        .read::<Vec<u8>>()?
                        .try_into()
                        .map_err(|error| XStorageError::DecodeFailed(format!("{error}")))?;
                    let hash: Self::Hash = reader.read::<Vec<u8>>()?.try_into().map_err(|_| {
                        XStorageError::DecodeFailed(format!("Could not convert bytes to hash",))
                    })?;
                    children.push(Reference::new(boundary, hash))
                }
                let children = NonEmpty::from_vec(children).ok_or_else(|| {
                    XStorageError::DecodeFailed("Branch seems to have zero children".into())
                })?;

                Ok(Block::branch(children))
            }
            BlockType::Segment => {
                let mut children = vec![];
                for _ in 0..child_count {
                    let key: Vec<u8> = reader
                        .read::<Vec<u8>>()?
                        .try_into()
                        .map_err(|error| XStorageError::DecodeFailed(format!("{error}")))?;
                    let value: Vec<u8> = reader
                        .read::<Vec<u8>>()?
                        .try_into()
                        .map_err(|error| XStorageError::DecodeFailed(format!("{error}")))?;
                    children.push(Entry::new(key, value))
                }
                let children = NonEmpty::from_vec(children).ok_or_else(|| {
                    XStorageError::DecodeFailed("Segment seems to have zero entries".into())
                })?;
                Ok(Block::segment(children))
            }
        }
    }
}
