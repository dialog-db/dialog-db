//! The key schedule: one key per node, one secret per level.
//!
//! A link carries a single 32-byte key — the child's [`StructureKey`]. Every
//! other key a node needs is derived from it and a level secret that is held
//! once, not per node:
//!
//! ```text
//! structure  S = keyed_hash(content_secret, plaintext)   carried in the parent's header
//! range      R = keyed_hash(range_secret,   S)           L2 = L1 + one secret
//! content    C = keyed_hash(content_secret, R)           L3 = L2 + one secret
//! ```
//!
//! Holding structure keys alone lets a party walk the tree and nothing more.
//! Adding the range secret opens separators; adding the content secret opens
//! bodies. The nesting is cryptographic rather than a convention: the content
//! key is derived from the range key, so content access is impossible without
//! range access.
//!
//! # Why the structure key comes from the plaintext
//!
//! The parent's header holds the child's structure key, so the key is part of
//! the parent's bytes and therefore of the parent's address. Two replicas that
//! build the same node must derive the same key, or every address above it
//! would differ and diffs would stop pruning. Deriving it from the plaintext
//! makes it convergent; keying the derivation with the content secret stops
//! anyone who merely holds structure keys from confirming a guess at a node's
//! contents by hashing it.
//!
//! # Which regions can use a fixed nonce
//!
//! The range and content keys each encrypt exactly one message, derived from
//! that message's own node: two different messages under one key would need
//! two different plaintexts deriving the same structure key, a BLAKE3
//! collision. So those regions need no nonce of their own.
//!
//! The structure region is the exception. It records the children's
//! addresses, which depend on the generations the children were sealed
//! under, so the same node can seal two different structure regions under one
//! structure key. It takes a synthetic nonce instead; see
//! [`Envelope`](super::Envelope).
//!
//! # Values
//!
//! A value a node refers to without holding it (a spilled value) is sealed
//! on its own, under `keyed_hash(content_secret, reference)`, where the
//! reference is the value's plaintext hash, which the node records. Only the
//! content secret opens it: a party that can read the node that names the
//! value can read the value, and no one else can.

use std::collections::BTreeMap;

use dialog_common::Blake3Hash;

use crate::EpochId;

/// Domain separator for structure keys.
const STRUCTURE_DOMAIN: &[u8] = b"dialog/keyring/layered/structure/v1";
/// Domain separator for range keys.
const RANGE_DOMAIN: &[u8] = b"dialog/keyring/layered/range/v1";
/// Domain separator for content keys.
const CONTENT_DOMAIN: &[u8] = b"dialog/keyring/layered/content/v1";
/// Domain separator for a sealed value's key.
const VALUE_DOMAIN: &[u8] = b"dialog/keyring/layered/value/v1";

/// The one per-node key: opens a node's header, and — with a level secret —
/// derives the node's other keys.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StructureKey([u8; 32]);

impl StructureKey {
    /// Wrap raw key bytes.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for StructureKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StructureKey(..)")
    }
}

/// One generation of a level's secret.
///
/// A level's secret rotates when someone loses access to that level. Nodes
/// written before the rotation stay sealed under the old generation, so a
/// holder keeps every generation it was given and the envelope names which
/// one applies.
#[derive(Clone)]
pub struct LevelSecret {
    /// The name of this generation, recorded in every envelope sealed under it.
    generation: EpochId,
    /// The secret itself.
    secret: [u8; 32],
}

impl LevelSecret {
    /// A generation named `generation`, holding `secret`.
    #[must_use]
    pub fn new(generation: EpochId, secret: [u8; 32]) -> Self {
        Self { generation, secret }
    }

    /// This generation's name.
    #[must_use]
    pub fn generation(&self) -> &EpochId {
        &self.generation
    }
}

impl std::fmt::Debug for LevelSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LevelSecret")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// Every generation of one level's secret that a party holds.
#[derive(Clone, Debug, Default)]
pub struct Level {
    /// Secrets by generation.
    secrets: BTreeMap<EpochId, LevelSecret>,
}

impl Level {
    /// A level holding no generations.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// This level with one more generation added.
    #[must_use]
    pub fn with(mut self, secret: LevelSecret) -> Self {
        self.insert(secret);
        self
    }

    /// Add a generation.
    pub fn insert(&mut self, secret: LevelSecret) {
        self.secrets.insert(secret.generation.clone(), secret);
    }

    /// The secret for `generation`, if held.
    fn get(&self, generation: &EpochId) -> Option<&[u8; 32]> {
        self.secrets.get(generation).map(|level| &level.secret)
    }
}

/// What a party can open beyond the structure keys it is handed.
///
/// There are three shapes, and the constructors are the only way to build
/// one, so content access without range access cannot be expressed.
#[derive(Clone, Debug, Default)]
pub struct Access {
    /// Range secrets, by generation.
    range: Level,
    /// Content secrets, by generation.
    content: Level,
}

impl Access {
    /// Structure only: walk the tree, verify and replicate blocks, read
    /// nothing. What a replicator holds.
    #[must_use]
    pub fn structure() -> Self {
        Self::default()
    }

    /// Structure and ranges: route a key to the leaf that would hold it,
    /// without reading any entry.
    #[must_use]
    pub fn ranges(range: Level) -> Self {
        Self {
            range,
            content: Level::new(),
        }
    }

    /// Everything. Content access requires range access, because the content
    /// key is derived from the range key.
    #[must_use]
    pub fn content(range: Level, content: Level) -> Self {
        Self { range, content }
    }

    /// The range key for a node, if this party holds the generation.
    pub(crate) fn range_key(
        &self,
        structure: &StructureKey,
        generation: &EpochId,
    ) -> Result<[u8; 32], crate::KeyringError> {
        let secret = self
            .range
            .get(generation)
            .ok_or_else(|| crate::KeyringError::MissingGeneration(generation.clone()))?;
        Ok(derive_range(secret, structure))
    }

    /// The content key for a node, if this party holds both generations.
    pub(crate) fn content_key(
        &self,
        structure: &StructureKey,
        range_generation: &EpochId,
        content_generation: &EpochId,
    ) -> Result<[u8; 32], crate::KeyringError> {
        let range = self.range_key(structure, range_generation)?;
        let secret = self
            .content
            .get(content_generation)
            .ok_or_else(|| crate::KeyringError::MissingGeneration(content_generation.clone()))?;
        Ok(derive_content(secret, &range))
    }

    /// The key for the sealed value whose plaintext hashes to `reference`,
    /// if this party holds the content generation it was sealed under.
    pub(crate) fn value_key(
        &self,
        generation: &EpochId,
        reference: &Blake3Hash,
    ) -> Result<[u8; 32], crate::KeyringError> {
        let secret = self
            .content
            .get(generation)
            .ok_or_else(|| crate::KeyringError::MissingGeneration(generation.clone()))?;
        Ok(derive_value(secret, reference))
    }
}

/// The generations a writer seals new nodes under.
#[derive(Clone, Debug)]
pub struct Writer {
    /// The current range generation.
    range: LevelSecret,
    /// The current content generation.
    content: LevelSecret,
}

impl Writer {
    /// A writer sealing under these generations.
    #[must_use]
    pub fn new(range: LevelSecret, content: LevelSecret) -> Self {
        Self { range, content }
    }

    /// The range generation new nodes are sealed under.
    #[must_use]
    pub fn range_generation(&self) -> &EpochId {
        &self.range.generation
    }

    /// The content generation new nodes are sealed under.
    #[must_use]
    pub fn content_generation(&self) -> &EpochId {
        &self.content.generation
    }

    /// The structure key for a node with this plaintext.
    pub(crate) fn structure_key(&self, plaintext: &[u8]) -> StructureKey {
        let mut hasher = blake3::Hasher::new_keyed(&self.content.secret);
        hasher.update(STRUCTURE_DOMAIN);
        hasher.update(plaintext);
        StructureKey(*hasher.finalize().as_bytes())
    }

    /// The range key for a node sealed under this writer's generation.
    pub(crate) fn range_key(&self, structure: &StructureKey) -> [u8; 32] {
        derive_range(&self.range.secret, structure)
    }

    /// The content key for a node sealed under this writer's generations.
    pub(crate) fn content_key(&self, structure: &StructureKey) -> [u8; 32] {
        derive_content(&self.content.secret, &self.range_key(structure))
    }

    /// The key for a value whose plaintext hashes to `reference`, sealed
    /// under this writer's content generation.
    pub(crate) fn value_key(&self, reference: &Blake3Hash) -> [u8; 32] {
        derive_value(&self.content.secret, reference)
    }
}

/// `R = keyed_hash(range_secret, S)`.
fn derive_range(secret: &[u8; 32], structure: &StructureKey) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_keyed(secret);
    hasher.update(RANGE_DOMAIN);
    hasher.update(structure.as_bytes());
    *hasher.finalize().as_bytes()
}

/// `C = keyed_hash(content_secret, R)`.
fn derive_content(secret: &[u8; 32], range: &[u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_keyed(secret);
    hasher.update(CONTENT_DOMAIN);
    hasher.update(range);
    *hasher.finalize().as_bytes()
}

/// `V = keyed_hash(content_secret, reference)`, where `reference` is the
/// value's plaintext hash. One key per distinct value, so, as for the range
/// and content regions, each key encrypts exactly one message.
fn derive_value(secret: &[u8; 32], reference: &Blake3Hash) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_keyed(secret);
    hasher.update(VALUE_DOMAIN);
    hasher.update(reference.as_bytes());
    *hasher.finalize().as_bytes()
}
