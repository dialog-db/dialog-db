use dialog_storage::Blake3Hash;
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{DialogArtifactsError, Entity, Value, ValueDataType};

/// Produces a [Reference], which is a type-alias for a 32-byte array; in practice, these
/// bytes are the BLAKE3 hash of the inputs to this function
pub fn make_reference<B>(bytes: B) -> Blake3Hash
where
    B: AsRef<[u8]>,
{
    blake3::hash(bytes.as_ref()).as_bytes().to_owned()
}

/// A value as the tree holds it: carried whole, or named by the hash of
/// content held elsewhere.
///
/// [`Value`] is the logical view of a value. `Reference` is the physical
/// view of the same value, which adds the one form a [`Value`] cannot
/// express: content addressed by its hash rather than carried. Either form
/// names its content by [`entity`](Reference::entity), so any text, bytes,
/// or record value can be pointed at by other facts.
///
/// The kind rides the reference, never the content hash: text and bytes
/// with the same content hash identically, and the kind is what tells them
/// apart when the reference is read back.
#[derive(Clone, Debug, PartialEq)]
pub enum Reference {
    /// A value that carries its content.
    Inline(Value),
    /// A value named by the hash of content held elsewhere.
    Addressed {
        /// The type of the value the content decodes to.
        kind: ValueDataType,
        /// The BLAKE3 hash of the content.
        hash: Blake3Hash,
        /// The leading bytes of the content, as an index key carries them.
        prefix: Vec<u8>,
    },
}

impl Reference {
    /// The type of the value this reference names.
    pub fn kind(&self) -> ValueDataType {
        match self {
            Reference::Inline(value) => value.data_type(),
            Reference::Addressed { kind, .. } => *kind,
        }
    }

    /// The BLAKE3 hash of the content this reference names.
    pub fn hash(&self) -> Blake3Hash {
        match self {
            Reference::Inline(value) => value.to_reference(),
            Reference::Addressed { hash, .. } => *hash,
        }
    }

    /// The entity naming this reference's content.
    ///
    /// Text, bytes, and records are named `asset:<hash>` by the hash of their
    /// content, whichever form holds them, so the same content is the same
    /// entity whether a tree keeps it inline or by address. An entity value
    /// is its own entity. Other kinds are not content and have no entity.
    pub fn entity(&self) -> Result<Entity, DialogArtifactsError> {
        match (self, self.kind()) {
            (Reference::Inline(Value::Entity(entity)), _) => Ok(entity.clone()),
            (_, ValueDataType::Bytes | ValueDataType::String | ValueDataType::Record) => {
                Ok(Entity::from_blob(&self.hash())?)
            }
            (_, kind) => Err(DialogArtifactsError::InvalidValue(format!(
                "a {kind:?} reference names no content entity"
            ))),
        }
    }
}

impl From<Value> for Reference {
    fn from(value: Value) -> Self {
        Reference::Inline(value)
    }
}

/// The serialized shape of a [`Reference`]. An inline value travels as its
/// kind and raw bytes, since [`Value`]'s own serialization cannot tell bytes
/// from a record or text from a symbol.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ReferenceShape {
    Inline {
        kind: ValueDataType,
        #[serde(with = "serde_bytes")]
        content: Vec<u8>,
    },
    Addressed {
        kind: ValueDataType,
        #[serde(with = "serde_bytes")]
        hash: Blake3Hash,
        #[serde(with = "serde_bytes")]
        prefix: Vec<u8>,
    },
}

impl Serialize for Reference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Reference::Inline(value) => ReferenceShape::Inline {
                kind: value.data_type(),
                content: value.to_bytes(),
            },
            Reference::Addressed { kind, hash, prefix } => ReferenceShape::Addressed {
                kind: *kind,
                hash: *hash,
                prefix: prefix.clone(),
            },
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Reference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match ReferenceShape::deserialize(deserializer)? {
            ReferenceShape::Inline { kind, content } => Reference::Inline(
                Value::try_from((kind, content)).map_err(DeserializeError::custom)?,
            ),
            ReferenceShape::Addressed { kind, hash, prefix } => {
                Reference::Addressed { kind, hash, prefix }
            }
        })
    }
}

// TODO: We only have one "reference type" now, maybe deconstruct this macro
macro_rules! reference_type {
    ( $struct:ident ) => {
        impl From<Blake3Hash> for $struct {
            fn from(value: Blake3Hash) -> Self {
                Self(value)
            }
        }

        impl From<$struct> for Blake3Hash {
            fn from(value: $struct) -> Self {
                value.0
            }
        }

        impl std::ops::Deref for $struct {
            type Target = Blake3Hash;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl TryFrom<Vec<u8>> for $struct {
            type Error = crate::DialogArtifactsError;

            fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
                Ok(Self(value.try_into().map_err(|value: Vec<u8>| {
                    crate::DialogArtifactsError::InvalidReference(format!(
                        "Incorrect length (expected {}, got {})",
                        crate::HASH_SIZE,
                        value.len()
                    ))
                })?))
            }
        }
    };
}

pub(crate) use reference_type;

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::Asset;

    #[dialog_common::test]
    fn it_names_text_and_bytes_of_the_same_content_as_one_entity()
    -> Result<(), DialogArtifactsError> {
        let text = Reference::from(Value::String("hello".into()));
        let bytes = Reference::from(Value::Bytes(b"hello".to_vec()));
        assert_eq!(text.entity()?, bytes.entity()?);
        assert_eq!(text.kind(), ValueDataType::String);
        assert_eq!(bytes.kind(), ValueDataType::Bytes);
        Ok(())
    }

    #[dialog_common::test]
    fn it_names_content_with_the_entity_an_asset_of_it_names() -> Result<(), DialogArtifactsError> {
        let reference = Reference::from(Value::Bytes(b"payload".to_vec()));
        assert_eq!(
            reference.entity()?,
            Asset::new(b"payload".to_vec()).entity()?
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_names_addressed_content_like_inline_content() -> Result<(), DialogArtifactsError> {
        let value = Value::String("z".repeat(10));
        let inline = Reference::from(value.clone());
        let addressed = Reference::Addressed {
            kind: ValueDataType::String,
            hash: value.to_reference(),
            prefix: b"zz".to_vec(),
        };
        assert_eq!(addressed.entity()?, inline.entity()?);
        assert_eq!(addressed.hash(), inline.hash());
        Ok(())
    }

    #[dialog_common::test]
    fn it_takes_an_entity_value_as_its_own_entity() -> Result<(), DialogArtifactsError> {
        let entity: Entity = "did:key:z6MkQmQKzPsjyUz49pvaxYdiiZEuQXyNqeBkS88GTrvqnov".parse()?;
        let reference = Reference::from(Value::Entity(entity.clone()));
        assert_eq!(reference.entity()?, entity);
        Ok(())
    }

    #[dialog_common::test]
    fn it_names_no_entity_for_a_number() {
        let reference = Reference::from(Value::UnsignedInt(7));
        assert!(reference.entity().is_err());
    }

    #[dialog_common::test]
    fn it_round_trips_the_kind_through_dag_cbor() {
        for value in [
            Value::String("text".into()),
            Value::Bytes(b"text".to_vec()),
            Value::Record(b"text".to_vec()),
        ] {
            let reference = Reference::from(value);
            let bytes = serde_ipld_dagcbor::to_vec(&reference).expect("encode reference");
            let decoded: Reference =
                serde_ipld_dagcbor::from_slice(&bytes).expect("decode reference");
            assert_eq!(decoded, reference);
        }

        let addressed = Reference::Addressed {
            kind: ValueDataType::String,
            hash: [7u8; 32],
            prefix: b"prefix".to_vec(),
        };
        let bytes = serde_ipld_dagcbor::to_vec(&addressed).expect("encode reference");
        let decoded: Reference = serde_ipld_dagcbor::from_slice(&bytes).expect("decode reference");
        assert_eq!(decoded, addressed);
    }
}
