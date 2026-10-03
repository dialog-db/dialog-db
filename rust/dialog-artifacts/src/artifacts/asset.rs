use base58::ToBase58;
use dialog_storage::Blake3Hash;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::{ByteBuf, Bytes};
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;

use crate::{
    Artifact, Attribute, DialogArtifactsError, Entity, Statement, Update, Value, make_reference,
};

/// The attribute recording that a branch holds an asset: the fact
/// `asset:<hash> dialog.asset/size <size>`.
///
/// The fact is written only by the commit that stores the asset's bytes
/// (the `dialog.` namespace is closed to application writes), so its size
/// is derived from the bytes rather than claimed. Push ships the bytes of
/// every asset whose fact a revision adds, a blob read hydrates only content
/// such a fact vouches for, and retracting the fact drops the branch's
/// reference to the bytes.
pub const ASSET_SIZE: &str = "dialog.asset/size";

/// Content stored in the blob store and recorded by a fact on its entity,
/// `asset:<hash>`.
///
/// Asserting an asset on a transaction stores it: the commit writes its
/// bytes through the blob store and records `asset:<hash> dialog.asset/size
/// <size>` in the same revision as the transaction's other facts, so those
/// facts may point at [`entity`](Asset::entity). Retracting an asset retracts
/// that fact, which is what makes its bytes collectible.
///
/// An asset built with [`new`](Asset::new) carries its bytes. One built with
/// [`stored`](Asset::stored) names bytes already in the blob store, such as
/// a streamed upload's, and the commit checks the store holds them at that
/// size before recording the fact.
///
/// The hash is the BLAKE3 hash of the bytes, the digest the blob store
/// reports for them and the reference a spilled value of them carries.
#[derive(Clone, PartialEq, Eq)]
pub struct Asset {
    hash: Blake3Hash,
    size: u64,
    content: Option<Arc<[u8]>>,
}

impl Asset {
    /// An asset carrying `content`, named by its hash.
    pub fn new(content: impl Into<Vec<u8>>) -> Self {
        let content: Arc<[u8]> = content.into().into();
        Self {
            hash: make_reference(&content),
            size: content.len() as u64,
            content: Some(content),
        }
    }

    /// An asset naming `size` bytes the blob store already holds under
    /// `hash`, such as a streamed upload's.
    ///
    /// Asserting it checks the store holds `size` bytes under `hash`.
    /// Retracting it ignores `size`: the commit retracts whatever size the
    /// line records for `hash` (see [`Update::discard`]).
    pub fn stored(hash: Blake3Hash, size: u64) -> Self {
        Self {
            hash,
            size,
            content: None,
        }
    }

    /// The BLAKE3 hash of the content.
    pub fn hash(&self) -> &Blake3Hash {
        &self.hash
    }

    /// The content's size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The content, when this asset carries it rather than naming stored
    /// bytes. Cloning an asset shares it.
    pub fn content(&self) -> Option<&[u8]> {
        self.content.as_deref()
    }

    /// The entity naming the content, `asset:<hash>`.
    pub fn entity(&self) -> Result<Entity, DialogArtifactsError> {
        Ok(Entity::from_blob(&self.hash)?)
    }

    /// The fact recording that a branch holds this asset.
    pub fn fact(&self) -> Result<Artifact, DialogArtifactsError> {
        Ok(Artifact {
            the: ASSET_SIZE.parse::<Attribute>()?,
            of: self.entity()?,
            is: Value::UnsignedInt(self.size as u128),
            cause: None,
        })
    }
}

impl From<Vec<u8>> for Asset {
    fn from(content: Vec<u8>) -> Self {
        Self::new(content)
    }
}

impl From<&[u8]> for Asset {
    fn from(content: &[u8]) -> Self {
        Self::new(content.to_vec())
    }
}

/// Asserting an asset stores it; retracting it drops the branch's
/// reference to its bytes. See [`Update::import`] and [`Update::discard`].
impl Statement for Asset {
    fn assert(self, update: &mut impl Update) {
        update.import(self);
    }

    fn retract(self, update: &mut impl Update) {
        update.discard(self);
    }
}

impl Debug for Asset {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Asset")
            .field("hash", &self.hash.to_base58())
            .field("size", &self.size)
            .field("carried", &self.content.is_some())
            .finish()
    }
}

/// The serialized shape of an [`Asset`]: its bytes when it carries them,
/// from which the hash and size are derived rather than trusted, otherwise
/// the hash and size it names.
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum AssetShape {
    Content(ByteBuf),
    Stored {
        #[serde(with = "serde_bytes")]
        hash: Blake3Hash,
        size: u64,
    },
}

/// [`AssetShape`] borrowed from an asset, for encoding its bytes without
/// copying them.
#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum AssetShapeRef<'a> {
    Content(&'a Bytes),
    Stored {
        #[serde(with = "serde_bytes")]
        hash: &'a Blake3Hash,
        size: u64,
    },
}

impl Serialize for Asset {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.content {
            Some(content) => AssetShapeRef::Content(Bytes::new(content)),
            None => AssetShapeRef::Stored {
                hash: &self.hash,
                size: self.size,
            },
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Asset {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match AssetShape::deserialize(deserializer)? {
            AssetShape::Content(content) => Self::new(content.into_vec()),
            AssetShape::Stored { hash, size } => Self::stored(hash, size),
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::Changes;

    #[dialog_common::test]
    fn it_names_content_by_its_blake3_hash() {
        let asset = Asset::new(b"hello".to_vec());
        assert_eq!(asset.hash(), blake3::hash(b"hello").as_bytes());
        assert_eq!(asset.size(), 5);
        assert_eq!(asset.content(), Some(&b"hello"[..]));
    }

    #[dialog_common::test]
    fn it_names_content_with_the_asset_entity() -> Result<(), DialogArtifactsError> {
        let asset = Asset::new(b"hello".to_vec());
        let entity = asset.entity()?;
        assert!(entity.as_str().starts_with("asset:"));
        assert_eq!(entity.blob_hash(), Some(*asset.hash()));
        Ok(())
    }

    #[dialog_common::test]
    fn it_records_its_size_as_a_fact_on_its_entity() -> Result<(), DialogArtifactsError> {
        let asset = Asset::new(b"hello".to_vec());
        let fact = asset.fact()?;
        assert_eq!(fact.the.as_str(), ASSET_SIZE);
        assert_eq!(fact.of, asset.entity()?);
        assert_eq!(fact.is, Value::UnsignedInt(5));
        Ok(())
    }

    /// A stored asset names the same content, entity, and fact as the
    /// asset that carries the bytes.
    #[dialog_common::test]
    fn it_names_stored_content_like_carried_content() -> Result<(), DialogArtifactsError> {
        let carried = Asset::new(b"hello".to_vec());
        let stored = Asset::stored(*carried.hash(), carried.size());
        assert_eq!(stored.content(), None);
        assert_eq!(stored.entity()?, carried.entity()?);
        assert_eq!(stored.fact()?, carried.fact()?);
        Ok(())
    }

    #[dialog_common::test]
    fn it_imports_on_assert_and_discards_on_retract() {
        let asset = Asset::new(b"hello".to_vec());

        let mut asserted = Changes::new();
        asset.clone().assert(&mut asserted);
        assert_eq!(asserted.imports().count(), 1);
        assert_eq!(asserted.discards().count(), 0);

        let mut retracted = Changes::new();
        asset.retract(&mut retracted);
        assert_eq!(retracted.imports().count(), 0);
        assert_eq!(retracted.discards().count(), 1);
    }

    #[dialog_common::test]
    fn it_round_trips_through_dag_cbor() {
        for asset in [
            Asset::new(vec![0u8, 1, 2, 255]),
            Asset::stored([7u8; 32], 1_000_000),
        ] {
            let bytes = serde_ipld_dagcbor::to_vec(&asset).expect("encode asset");
            let decoded: Asset = serde_ipld_dagcbor::from_slice(&bytes).expect("decode asset");
            assert_eq!(decoded, asset);
        }
    }
}
