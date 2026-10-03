//! The wire format of a layered node.
//!
//! ```text
//! version(1) ‖ range generation(32) ‖ content generation(32)
//!   ‖ structure nonce(12) ‖ structure length(4) ‖ structure region
//!   ‖ range length(4)     ‖ range region
//!   ‖ content region
//! ```
//!
//! The header (version and generations) is plaintext: a reader has to know
//! which generation's secret to use before it can open anything. Every region
//! authenticates it, so relabelling a node with another generation fails to
//! open rather than silently trying the wrong secret.
//!
//! Each region is AES-256-GCM under its own key. The range and content
//! regions use a fixed nonce: their keys derive from the node's own bytes, so
//! each encrypts exactly one message (see [`keys`](super::keys)). The
//! structure region cannot: it records the children's addresses, and those
//! depend on the generations the children were sealed under, not only on
//! this node. The same node, under the same structure key, can link children
//! at different addresses, so its structure region takes a synthetic nonce:
//! a keyed hash of what it encrypts, stored beside it. Deterministic, so
//! replicas still converge, and two different structure regions never share
//! a nonce.

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use dialog_common::Blake3Hash;

use super::keys::{Access, StructureKey, Writer};
use crate::{EpochId, KeyringError};

/// The envelope format this build writes and reads.
const VERSION: u8 = 1;

/// The authenticated header: version and two generations.
const HEADER: usize = 1 + 32 + 32;

/// The nonce of the range and content regions. Fixed, because neither key
/// encrypts twice.
const NONCE: [u8; 12] = [0; 12];

/// Domain separator for the structure region's synthetic nonce.
const NONCE_DOMAIN: &[u8] = b"dialog/keyring/layered/structure-nonce/v1";

/// Region labels, bound into each region's additional data so a region
/// cannot be presented as another.
const STRUCTURE: u8 = 1;
const RANGE: u8 = 2;
const CONTENT: u8 = 3;

/// One layered node: a plaintext header and three sealed regions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    /// The range generation the range region is sealed under.
    range_generation: EpochId,
    /// The content generation the content region is sealed under.
    content_generation: EpochId,
    /// The structure region's synthetic nonce.
    structure_nonce: [u8; 12],
    /// Each child's address and structure key, under the node's structure key.
    structure: Vec<u8>,
    /// Each child's separator, under the node's range key.
    range: Vec<u8>,
    /// The node's plaintext, under its content key.
    content: Vec<u8>,
}

impl Envelope {
    /// Seal a node.
    ///
    /// `children` are each child's address and structure key, and
    /// `separators` each child's separator, both in child order; a leaf has
    /// neither. `plain` is the node's own bytes.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Crypto`] if the cipher fails.
    pub(crate) fn seal(
        writer: &Writer,
        structure_key: &StructureKey,
        children: &[(Blake3Hash, StructureKey)],
        separators: &[Vec<u8>],
        plain: &[u8],
    ) -> Result<Self, KeyringError> {
        let range_generation = writer.range_generation().clone();
        let content_generation = writer.content_generation().clone();
        let header = header(&range_generation, &content_generation);

        let mut structure = Vec::with_capacity(4 + children.len() * 64);
        structure.extend_from_slice(&count(children.len())?);
        for (address, key) in children {
            structure.extend_from_slice(address.as_bytes());
            structure.extend_from_slice(key.as_bytes());
        }

        let mut range = Vec::new();
        range.extend_from_slice(&count(separators.len())?);
        for separator in separators {
            range.extend_from_slice(&count(separator.len())?);
            range.extend_from_slice(separator);
        }

        let structure_nonce = synthetic_nonce(structure_key, &header, &structure);
        Ok(Self {
            structure: seal(
                structure_key.as_bytes(),
                &structure_nonce,
                STRUCTURE,
                &header,
                &structure,
            )?,
            structure_nonce,
            range: seal(
                &writer.range_key(structure_key),
                &NONCE,
                RANGE,
                &header,
                &range,
            )?,
            content: seal(
                &writer.content_key(structure_key),
                &NONCE,
                CONTENT,
                &header,
                plain,
            )?,
            range_generation,
            content_generation,
        })
    }

    /// The envelope's bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            HEADER + 12 + 8 + self.structure.len() + self.range.len() + self.content.len(),
        );
        bytes.extend_from_slice(&header(&self.range_generation, &self.content_generation));
        bytes.extend_from_slice(&self.structure_nonce);
        bytes.extend_from_slice(&(self.structure.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.structure);
        bytes.extend_from_slice(&(self.range.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.range);
        bytes.extend_from_slice(&self.content);
        bytes
    }

    /// Decode an envelope's bytes, without opening anything.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Malformed`] if the bytes are too short for
    /// what they declare, or [`KeyringError::UnsupportedVersion`] for a
    /// version this build does not read.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, KeyringError> {
        let (&version, rest) = bytes.split_first().ok_or(KeyringError::Malformed)?;
        if version != VERSION {
            return Err(KeyringError::UnsupportedVersion(version));
        }
        let (range_generation, rest) = take::<32>(rest)?;
        let (content_generation, rest) = take::<32>(rest)?;
        let (structure_nonce, rest) = take::<12>(rest)?;
        let (structure, rest) = region(rest)?;
        let (range, content) = region(rest)?;
        Ok(Self {
            range_generation: EpochId::from(range_generation),
            content_generation: EpochId::from(content_generation),
            structure_nonce,
            structure: structure.to_vec(),
            range: range.to_vec(),
            content: content.to_vec(),
        })
    }

    /// The address this envelope is stored under: `blake3` of its bytes.
    #[must_use]
    pub fn address(&self) -> Blake3Hash {
        Blake3Hash::hash(&self.to_bytes())
    }

    /// The range generation this node is sealed under.
    #[must_use]
    pub fn range_generation(&self) -> &EpochId {
        &self.range_generation
    }

    /// The content generation this node is sealed under.
    #[must_use]
    pub fn content_generation(&self) -> &EpochId {
        &self.content_generation
    }

    /// Each child's address and structure key, in child order. Needs only
    /// the node's own structure key.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Failed`] if `key` is not this node's
    /// structure key or the region was tampered with.
    pub fn children(
        &self,
        key: &StructureKey,
    ) -> Result<Vec<(Blake3Hash, StructureKey)>, KeyringError> {
        let plain = open(
            key.as_bytes(),
            &self.structure_nonce,
            STRUCTURE,
            &self.header(),
            &self.structure,
        )?;
        let (children, mut rest) = read_count(&plain)?;
        let mut parsed = Vec::with_capacity(children);
        for _ in 0..children {
            let (address, after) = take::<32>(rest)?;
            let (child, after) = take::<32>(after)?;
            parsed.push((Blake3Hash::from(address), StructureKey::from_bytes(child)));
            rest = after;
        }
        Ok(parsed)
    }

    /// Each child's separator, in child order. Needs the range generation.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::MissingGeneration`] without the range
    /// generation, and [`KeyringError::Failed`] if the region does not open.
    pub fn separators(
        &self,
        access: &Access,
        key: &StructureKey,
    ) -> Result<Vec<Vec<u8>>, KeyringError> {
        let range_key = access.range_key(key, &self.range_generation)?;
        let plain = open(&range_key, &NONCE, RANGE, &self.header(), &self.range)?;
        let (separators, mut rest) = read_count(&plain)?;
        let mut parsed = Vec::with_capacity(separators);
        for _ in 0..separators {
            let (length, after) = read_count(rest)?;
            let separator = after.get(..length).ok_or(KeyringError::Malformed)?;
            parsed.push(separator.to_vec());
            rest = &after[length..];
        }
        Ok(parsed)
    }

    /// The node's own bytes. Needs both generations.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::MissingGeneration`] without either
    /// generation, and [`KeyringError::Failed`] if the region does not open.
    pub fn content(&self, access: &Access, key: &StructureKey) -> Result<Vec<u8>, KeyringError> {
        let content_key =
            access.content_key(key, &self.range_generation, &self.content_generation)?;
        open(&content_key, &NONCE, CONTENT, &self.header(), &self.content)
    }

    fn header(&self) -> [u8; HEADER] {
        header(&self.range_generation, &self.content_generation)
    }
}

fn header(range: &EpochId, content: &EpochId) -> [u8; HEADER] {
    let mut header = [0u8; HEADER];
    header[0] = VERSION;
    header[1..33].copy_from_slice(range.as_bytes());
    header[33..].copy_from_slice(content.as_bytes());
    header
}

/// The additional data a region is sealed with: the header and its label.
fn aad(label: u8, header: &[u8; HEADER]) -> [u8; HEADER + 1] {
    let mut aad = [0u8; HEADER + 1];
    aad[..HEADER].copy_from_slice(header);
    aad[HEADER] = label;
    aad
}

/// The structure region's nonce: a keyed hash of the header and the region's
/// plaintext, so it changes whenever what the key encrypts does.
fn synthetic_nonce(key: &StructureKey, header: &[u8; HEADER], plain: &[u8]) -> [u8; 12] {
    let mut hasher = blake3::Hasher::new_keyed(key.as_bytes());
    hasher.update(NONCE_DOMAIN);
    hasher.update(header);
    hasher.update(plain);
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&hasher.finalize().as_bytes()[..12]);
    nonce
}

fn seal(
    key: &[u8; 32],
    nonce: &[u8; 12],
    label: u8,
    header: &[u8; HEADER],
    plain: &[u8],
) -> Result<Vec<u8>, KeyringError> {
    Aes256Gcm::new(key.into())
        .encrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: plain,
                aad: &aad(label, header),
            },
        )
        .map_err(|error| KeyringError::Crypto(error.to_string()))
}

fn open(
    key: &[u8; 32],
    nonce: &[u8; 12],
    label: u8,
    header: &[u8; HEADER],
    sealed: &[u8],
) -> Result<Vec<u8>, KeyringError> {
    Aes256Gcm::new(key.into())
        .decrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: sealed,
                aad: &aad(label, header),
            },
        )
        .map_err(|_| KeyringError::Failed)
}

/// A length or count as four little-endian bytes.
fn count(value: usize) -> Result<[u8; 4], KeyringError> {
    u32::try_from(value)
        .map(u32::to_le_bytes)
        .map_err(|_| KeyringError::Crypto("a node region exceeds 4 GiB".into()))
}

fn read_count(bytes: &[u8]) -> Result<(usize, &[u8]), KeyringError> {
    let (value, rest) = take::<4>(bytes)?;
    Ok((u32::from_le_bytes(value) as usize, rest))
}

fn take<const N: usize>(bytes: &[u8]) -> Result<([u8; N], &[u8]), KeyringError> {
    let (head, rest) = bytes
        .split_first_chunk::<N>()
        .ok_or(KeyringError::Malformed)?;
    Ok((*head, rest))
}

/// A length-prefixed region and what follows it.
fn region(bytes: &[u8]) -> Result<(&[u8], &[u8]), KeyringError> {
    let (length, rest) = read_count(bytes)?;
    if rest.len() < length {
        return Err(KeyringError::Malformed);
    }
    Ok(rest.split_at(length))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layered::keys::LevelSecret;

    fn writer() -> Writer {
        Writer::new(
            LevelSecret::new(EpochId::from([1; 32]), [11; 32]),
            LevelSecret::new(EpochId::from([2; 32]), [22; 32]),
        )
    }

    /// The case a fixed nonce gets wrong: one node, so one structure key,
    /// linking its children at different addresses (as after a rotation,
    /// when some children were resealed and others were not). The structure
    /// regions differ, so their nonces must too, and both must still open.
    #[test]
    fn it_never_reuses_a_structure_nonce_for_different_children() {
        let writer = writer();
        let plain = b"one index node";
        let key = writer.structure_key(plain);
        let separators = vec![b"a".to_vec(), b"m".to_vec()];
        let before = [
            (Blake3Hash::hash(b"left"), StructureKey::from_bytes([3; 32])),
            (
                Blake3Hash::hash(b"right"),
                StructureKey::from_bytes([4; 32]),
            ),
        ];
        let after = [
            before[0].clone(),
            (
                Blake3Hash::hash(b"right, resealed"),
                StructureKey::from_bytes([5; 32]),
            ),
        ];

        let first = Envelope::seal(&writer, &key, &before, &separators, plain).expect("seal");
        let second = Envelope::seal(&writer, &key, &after, &separators, plain).expect("seal");

        assert_ne!(first.structure_nonce, second.structure_nonce);
        assert_eq!(first.children(&key).expect("open"), before);
        assert_eq!(second.children(&key).expect("open"), after);
    }

    /// The same inputs seal to the same bytes, which is what lets replicas
    /// converge.
    #[test]
    fn it_seals_deterministically() {
        let writer = writer();
        let plain = b"a leaf";
        let key = writer.structure_key(plain);
        let one = Envelope::seal(&writer, &key, &[], &[], plain).expect("seal");
        let two = Envelope::seal(&writer, &key, &[], &[], plain).expect("seal");
        assert_eq!(one.to_bytes(), two.to_bytes());
    }
}
