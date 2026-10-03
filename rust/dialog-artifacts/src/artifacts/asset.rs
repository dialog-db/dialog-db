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

/// The attribute recording a sealed line's asset: the fact
/// `asset:<hash> dialog.asset/sealed <copy>`, its value a [`SealedCopy`]
/// together with the asset's size.
///
/// A sealed asset records this fact in place of [`ASSET_SIZE`]: its
/// plaintext is stored nowhere, so nothing should vouch for it. The fact
/// lives in the line's tree, which is itself sealed, so only a party that
/// can read the node holding it learns where the sealed copy lives, and
/// push and export ship that copy straight from the fact, as they ship a
/// plaintext asset's bytes from its size.
pub const ASSET_SEALED: &str = "dialog.asset/sealed";

/// Where an asset's sealed copy is stored, and its length: an ordinary
/// blob, under the hash of its own bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealedCopy {
    /// The hash of the sealed bytes, which the blob store keeps them under.
    pub address: Blake3Hash,
    /// The sealed bytes' length.
    pub length: u64,
}

/// The encoded length of a [`ASSET_SEALED`] value.
const SEALED_VALUE: usize = 32 + 8 + 8;

impl SealedCopy {
    /// The [`ASSET_SEALED`] value recording this copy of an asset of
    /// `size` bytes: `address ‖ size ‖ length`, both lengths big-endian.
    pub fn value(&self, size: u64) -> Value {
        let mut bytes = Vec::with_capacity(SEALED_VALUE);
        bytes.extend_from_slice(&self.address);
        bytes.extend_from_slice(&size.to_be_bytes());
        bytes.extend_from_slice(&self.length.to_be_bytes());
        Value::Bytes(bytes)
    }

    /// The copy and asset size an [`ASSET_SEALED`] value records, or
    /// `None` when it is not one.
    pub fn from_value(value: &Value) -> Option<(Self, u64)> {
        let Value::Bytes(bytes) = value else {
            return None;
        };
        Self::from_bytes(bytes)
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<(Self, u64)> {
        let bytes: &[u8; SEALED_VALUE] = bytes.try_into().ok()?;
        let (address, rest) = bytes.split_first_chunk::<32>()?;
        let (size, length) = rest.split_first_chunk::<8>()?;
        let length: [u8; 8] = length.try_into().ok()?;
        Some((
            Self {
                address: *address,
                length: u64::from_be_bytes(length),
            },
            u64::from_be_bytes(*size),
        ))
    }
}

/// How an asset's bytes are kept.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AssetSealing {
    /// As the line recording it keeps content: sealed on a sealed line,
    /// in the clear on any other.
    #[default]
    Line,
    /// In the clear, even on a sealed line. Opt in with
    /// [`Asset::plaintext`].
    Plaintext,
    /// Sealed already, as a sealed line's streamed import leaves it.
    Sealed(SealedCopy),
}

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
///
/// On a sealed line an asset is sealed unless it says otherwise: the commit
/// seals the bytes an asset carries, a streamed import seals as it writes
/// ([`AssetSealing::Sealed`]), and only an asset marked
/// [`plaintext`](Asset::plaintext) is kept in the clear. A stored asset
/// naming plaintext bytes is refused there unless so marked.
#[derive(Clone, PartialEq, Eq)]
pub struct Asset {
    hash: Blake3Hash,
    size: u64,
    content: Option<Arc<[u8]>>,
    sealing: AssetSealing,
}

impl Asset {
    /// An asset carrying `content`, named by its hash.
    pub fn new(content: impl Into<Vec<u8>>) -> Self {
        let content: Arc<[u8]> = content.into().into();
        Self {
            hash: make_reference(&content),
            size: content.len() as u64,
            content: Some(content),
            sealing: AssetSealing::Line,
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
            sealing: AssetSealing::Line,
        }
    }

    /// An asset of `size` bytes hashing to `hash`, whose sealed copy the
    /// blob store already holds, such as a sealed line's streamed import.
    ///
    /// Asserting it on a sealed line checks the store holds the copy at its
    /// length. A plain line refuses it: nothing there can open it.
    pub fn sealed(hash: Blake3Hash, size: u64, copy: SealedCopy) -> Self {
        Self {
            hash,
            size,
            content: None,
            sealing: AssetSealing::Sealed(copy),
        }
    }

    /// Keep this asset's bytes in the clear, even on a sealed line.
    ///
    /// For content meant to be public, or to be read by parties outside
    /// the line: anyone holding the blob store, or a remote it was pushed
    /// to, can read it. The fact recording it still lives in the line's
    /// tree, sealed with it.
    #[must_use]
    pub fn plaintext(mut self) -> Self {
        self.sealing = AssetSealing::Plaintext;
        self
    }

    /// How this asset's bytes are kept.
    pub fn sealing(&self) -> &AssetSealing {
        &self.sealing
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

    /// The fact recording that a sealed line keeps this asset as `copy`.
    pub fn sealed_fact(&self, copy: &SealedCopy) -> Result<Artifact, DialogArtifactsError> {
        Ok(Artifact {
            the: ASSET_SEALED.parse::<Attribute>()?,
            of: self.entity()?,
            is: copy.value(self.size),
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
            .field("sealing", &self.sealing)
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
    Plaintext(ByteBuf),
    Stored {
        #[serde(with = "serde_bytes")]
        hash: Blake3Hash,
        size: u64,
        #[serde(default)]
        plaintext: bool,
    },
    Sealed {
        #[serde(with = "serde_bytes")]
        hash: Blake3Hash,
        size: u64,
        #[serde(with = "serde_bytes")]
        address: Blake3Hash,
        length: u64,
    },
}

/// [`AssetShape`] borrowed from an asset, for encoding its bytes without
/// copying them.
#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum AssetShapeRef<'a> {
    Content(&'a Bytes),
    Plaintext(&'a Bytes),
    Stored {
        #[serde(with = "serde_bytes")]
        hash: &'a Blake3Hash,
        size: u64,
        #[serde(skip_serializing_if = "is_false")]
        plaintext: bool,
    },
    Sealed {
        #[serde(with = "serde_bytes")]
        hash: &'a Blake3Hash,
        size: u64,
        #[serde(with = "serde_bytes")]
        address: &'a Blake3Hash,
        length: u64,
    },
}

fn is_false(flag: &bool) -> bool {
    !flag
}

impl Serialize for Asset {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match (&self.content, &self.sealing) {
            (_, AssetSealing::Sealed(copy)) => AssetShapeRef::Sealed {
                hash: &self.hash,
                size: self.size,
                address: &copy.address,
                length: copy.length,
            },
            (Some(content), AssetSealing::Plaintext) => {
                AssetShapeRef::Plaintext(Bytes::new(content))
            }
            (Some(content), AssetSealing::Line) => AssetShapeRef::Content(Bytes::new(content)),
            (None, sealing) => AssetShapeRef::Stored {
                hash: &self.hash,
                size: self.size,
                plaintext: *sealing == AssetSealing::Plaintext,
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
            AssetShape::Plaintext(content) => Self::new(content.into_vec()).plaintext(),
            AssetShape::Stored {
                hash,
                size,
                plaintext: false,
            } => Self::stored(hash, size),
            AssetShape::Stored {
                hash,
                size,
                plaintext: true,
            } => Self::stored(hash, size).plaintext(),
            AssetShape::Sealed {
                hash,
                size,
                address,
                length,
            } => Self::sealed(hash, size, SealedCopy { address, length }),
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
            Asset::new(vec![0u8, 1, 2, 255]).plaintext(),
            Asset::stored([7u8; 32], 1_000_000),
            Asset::stored([7u8; 32], 1_000_000).plaintext(),
            Asset::sealed(
                [7u8; 32],
                1_000_000,
                SealedCopy {
                    address: [9u8; 32],
                    length: 1_000_065,
                },
            ),
        ] {
            let bytes = serde_ipld_dagcbor::to_vec(&asset).expect("encode asset");
            let decoded: Asset = serde_ipld_dagcbor::from_slice(&bytes).expect("decode asset");
            assert_eq!(decoded, asset);
        }
    }

    /// An asset encoded before it could choose how it is kept decodes as
    /// kept the way its line keeps content.
    #[dialog_common::test]
    fn it_decodes_an_earlier_stored_asset_as_kept_by_its_line() {
        #[derive(Serialize)]
        #[serde(rename_all = "lowercase")]
        enum Earlier {
            Stored {
                #[serde(with = "serde_bytes")]
                hash: Blake3Hash,
                size: u64,
            },
        }
        let bytes = serde_ipld_dagcbor::to_vec(&Earlier::Stored {
            hash: [7u8; 32],
            size: 10,
        })
        .expect("encode");
        let decoded: Asset = serde_ipld_dagcbor::from_slice(&bytes).expect("decode");
        assert_eq!(decoded, Asset::stored([7u8; 32], 10));
        assert_eq!(decoded.sealing(), &AssetSealing::Line);
    }

    /// The sealed fact names the asset's entity and records the copy and
    /// the size, which read back from it.
    #[dialog_common::test]
    fn it_records_its_sealed_copy_as_a_fact_on_its_entity() -> Result<(), DialogArtifactsError> {
        let copy = SealedCopy {
            address: [9u8; 32],
            length: 75,
        };
        let asset = Asset::sealed([7u8; 32], 10, copy);
        let fact = asset.sealed_fact(&copy)?;
        assert_eq!(fact.the.as_str(), ASSET_SEALED);
        assert_eq!(fact.of, asset.entity()?);
        assert_eq!(SealedCopy::from_value(&fact.is), Some((copy, 10)));
        assert_eq!(SealedCopy::from_value(&Value::Bytes(vec![9u8; 32])), None);
        Ok(())
    }
}
