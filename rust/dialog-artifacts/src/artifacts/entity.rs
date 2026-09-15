//! Entity types for semantic triple subjects.
//!
//! This module defines the [`Entity`] type which represents the subject part of
//! semantic triples. Entities are based on URIs and provide unique identification
//! for objects in the triple store.

use std::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    ops::Deref,
    str::FromStr,
};

use base58::{FromBase58, ToBase58};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::{DialogArtifactsError, ENTITY_LENGTH, Uri};

/// An [`Entity`] is the subject part of a semantic triple. An [`Entity`] can
/// be embodied by any valid [`Uri`].
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(into = "String", try_from = "String")]
pub struct Entity(Uri, [u8; ENTITY_LENGTH]);

/// Serializes an entity to UTF-8 format for CSV export.
pub(crate) fn to_utf8<S>(entity: &Entity, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    entity.0.serialize(serializer)
}

/// Deserializes an entity from UTF-8 format for CSV import.
pub(crate) fn from_utf8<'de, D>(deserializer: D) -> Result<Entity, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .parse::<Entity>()
        .map_err(|error| de::Error::custom(format!("{error:?}")))
}

impl AsRef<Entity> for Entity {
    fn as_ref(&self) -> &Entity {
        self
    }
}

impl Deref for Entity {
    type Target = Uri;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Uri> for Entity {
    type Error = DialogArtifactsError;

    fn try_from(value: Uri) -> Result<Self, Self::Error> {
        let bytes = value.key_bytes()?;
        Ok(Self(value, bytes))
    }
}

impl FromStr for Entity {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(Uri::from_str(s)?)
    }
}

impl TryFrom<String> for Entity {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<Vec<u8>> for Entity {
    type Error = DialogArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Entity::try_from(
            String::from_utf8(value)
                .map_err(|error| DialogArtifactsError::InvalidEntity(format!("{error}")))?,
        )
    }
}

impl From<Entity> for String {
    fn from(value: Entity) -> Self {
        value.to_string()
    }
}

impl Display for Entity {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", **self)
    }
}

/// Scheme prefix for blob-reference entities.
const BLOB_SCHEME: &str = "blob:";

/// Scheme prefix for tree-node entities.
const TREE_SCHEME: &str = "tree:";

impl Entity {
    /// Initialize a new [`Entity`] with a randomly generated, globally unique
    /// URI. The URI is formatted as an ed25519 DID Key.
    pub fn new() -> Result<Entity, DialogArtifactsError> {
        Self::try_from(Uri::unique()?)
    }

    /// Get the [`Entity`] as a string reference
    pub fn as_str(&self) -> &str {
        (**self).as_str()
    }

    /// Reconstructs an [`Entity`] from a string read back out of the index,
    /// verifying it is a canonical URI rendering (see [`Uri::from_stored`]).
    /// A string that fails the check is a corrupt or foreign-written entry
    /// and errors as [`CorruptEntry`](DialogArtifactsError::CorruptEntry),
    /// which scan paths treat as an ignorable row. The key bytes are still
    /// derived (they are not stored alongside the string).
    pub(crate) fn from_stored(s: &str) -> Result<Self, DialogArtifactsError> {
        Self::try_from(Uri::from_stored(s)?)
    }

    /// Get the raw byte representation of the [`Entity`] as it should be
    /// formatted for use in an index key.
    pub fn key_bytes(&self) -> &[u8; ENTITY_LENGTH] {
        &self.1
    }

    /// The canonical entity reference for a stored blob:
    /// `blob:<base58(hash)>`.
    pub fn from_blob(hash: &dialog_storage::Blake3Hash) -> Result<Entity, DialogArtifactsError> {
        format!("{}{}", BLOB_SCHEME, hash.to_base58()).parse()
    }

    /// The blob hash carried by a `blob:` entity, if this entity
    /// is one and its payload decodes to 32 base58 bytes.
    pub fn blob_hash(&self) -> Option<dialog_storage::Blake3Hash> {
        let payload = self.as_str().strip_prefix(BLOB_SCHEME)?;
        let bytes = payload.from_base58().ok()?;
        <[u8; 32]>::try_from(bytes).ok()
    }

    /// The canonical entity reference for a tree node: `tree:<base58(hash)>`.
    ///
    /// A node is content-addressed, so its hash IS its name — this puts a
    /// scheme on it, exactly as [`from_blob`](Self::from_blob) does for a
    /// blob. Nothing is derived or hashed: the entity and the reference are
    /// the same 32 bytes in two renderings, and [`node_hash`](Self::node_hash)
    /// reads them back.
    ///
    /// This is what lets a tree row be the subject of a fact. The `tree/*`
    /// resolvers answer with rows of slots, which have no subject; a rule
    /// concluding over them needs one, because every fact has an entity.
    pub fn from_node(hash: &dialog_storage::Blake3Hash) -> Result<Entity, DialogArtifactsError> {
        format!("{}{}", TREE_SCHEME, hash.to_base58()).parse()
    }

    /// The canonical entity for a POSITION within a tree node:
    /// `tree:<base58(hash)>/<at>` — one span of an index, or one entry of a
    /// segment.
    ///
    /// A position needs its own name rather than borrowing the node's,
    /// for two reasons that have nothing to do with each other.
    ///
    /// An ENTRY has no other name: it is a key in a segment, not a block,
    /// so it has no hash of its own and its position is its whole identity.
    ///
    /// A SPAN could borrow its child's name, and should not: a span is the
    /// PARENT's statement about a range — the separator, the seam rank that
    /// made the boundary, the ops still buffered for that subtree — while
    /// the child's own row says what the child is. They are two subjects,
    /// and collapsing them would hang the parent's bookkeeping on the
    /// child. (Within one tree a node appears exactly once, since its hash
    /// covers its keys and sibling ranges are disjoint — so this is about
    /// keeping the two subjects apart, not about disambiguating a node
    /// that repeats. Across REVISIONS a node is shared by many trees, and
    /// there naming an edge by its child would genuinely collide.)
    pub fn from_node_position(
        hash: &dialog_storage::Blake3Hash,
        at: u64,
    ) -> Result<Entity, DialogArtifactsError> {
        format!("{}{}/{}", TREE_SCHEME, hash.to_base58(), at).parse()
    }

    /// The node hash carried by a `tree:` entity, if this entity is one and
    /// its payload decodes to 32 base58 bytes.
    ///
    /// A POSITION entity (`tree:<hash>/<at>`) is deliberately not a node: it
    /// names a place inside one, so it does not answer here and cannot be
    /// handed back to a resolver as a node reference.
    pub fn node_hash(&self) -> Option<dialog_storage::Blake3Hash> {
        let payload = self.as_str().strip_prefix(TREE_SCHEME)?;
        let bytes = payload.from_base58().ok()?;
        <[u8; 32]>::try_from(bytes).ok()
    }
}

impl Debug for Entity {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(&self.0.to_string())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    #[dialog_common::test]
    fn it_round_trips_a_blob_entity() {
        let hash: dialog_storage::Blake3Hash = [7u8; 32];
        let entity = Entity::from_blob(&hash).expect("constructs");
        assert!(entity.as_str().starts_with("blob:"));
        assert_eq!(entity.blob_hash(), Some(hash));
        // String round-trip: parse the display form back.
        let reparsed: Entity = entity.as_str().parse().expect("parses");
        assert_eq!(reparsed.blob_hash(), Some(hash));
    }

    /// A node entity is its reference with a scheme on it, and it survives
    /// the URL normalization every entity goes through — so the string that
    /// comes back out of a row parses to the same node.
    #[dialog_common::test]
    fn it_round_trips_a_node_entity() {
        let hash: dialog_storage::Blake3Hash = [9u8; 32];
        let entity = Entity::from_node(&hash).expect("constructs");
        assert!(entity.as_str().starts_with("tree:"));
        assert_eq!(entity.node_hash(), Some(hash));
        let reparsed: Entity = entity.as_str().parse().expect("parses");
        assert_eq!(reparsed.node_hash(), Some(hash));
        assert_eq!(
            reparsed.as_str(),
            entity.as_str(),
            "canonical form is stable"
        );
    }

    /// A position inside a node is a different entity from the node, and is
    /// not itself a node reference — two spans of one index are two rows,
    /// and neither of them is the index.
    #[dialog_common::test]
    fn it_names_a_position_inside_a_node() {
        let hash: dialog_storage::Blake3Hash = [9u8; 32];
        let node = Entity::from_node(&hash).expect("constructs");
        let first = Entity::from_node_position(&hash, 0).expect("constructs");
        let second = Entity::from_node_position(&hash, 1).expect("constructs");

        assert_ne!(first, second, "positions are distinct");
        assert_ne!(first, node, "a position is not its node");
        assert_eq!(
            first.node_hash(),
            None,
            "a position is not a node reference"
        );
        let reparsed: Entity = first.as_str().parse().expect("parses");
        assert_eq!(
            reparsed.as_str(),
            first.as_str(),
            "canonical form is stable"
        );
    }

    #[dialog_common::test]
    fn it_returns_none_for_non_blob_entities() {
        let entity: Entity = "user:alice".parse().expect("parses");
        assert_eq!(entity.blob_hash(), None);
        // Garbage after the scheme is not a hash.
        let bogus: Entity = "blob:notbase58!!!".parse().expect("still a valid uri");
        assert_eq!(bogus.blob_hash(), None);
    }
}
