//! UCAN Container format utilities.
//!
//! This module provides [`Container`], a type that represents a UCAN container
//! following the [UCAN Container spec](https://github.com/ucan-wg/container).
//!
//! The container is the DAG-CBOR map `{ "ctn-v1": [token_bytes_0,
//! token_bytes_1, ..., token_bytes_n] }`, where tokens are DAG-CBOR
//! serialized UCANs. On the wire it travels either as that map
//! ([`Container::to_bytes`]) or in the spec's tagged serialization
//! ([`Container::encode`]): one byte naming the base encoding and
//! whether the bytes are gzip-compressed ([`Tag`]), then the bytes so
//! encoded, which is how a container fits an HTTP header.
//!
//! # Usage
//!
//! `Container` can be converted to/from:
//! - [`DelegationChain`] - A chain of delegations
//! - [`InvocationChain`] - An invocation with its delegation chain

pub mod bundle;
pub mod delegation;
pub mod invocation;
pub mod revocation;

mod check_failed;
pub use check_failed::check_failed_to_container_error;

use dialog_varsig::Did;
use ipld_core::cid::Cid;
use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;
use thiserror::Error;

/// Errors that can occur when working with UCAN containers, delegation chains,
/// and invocation chains.
#[derive(Debug, Error)]
pub enum ContainerError {
    /// Failed to parse or validate a UCAN token/invocation.
    #[error("Invocation error: {0}")]
    Invocation(String),

    /// A delegation link in the chain does not carry a valid signature
    /// from the principal its `iss` field claims. Kept distinct from
    /// [`Invocation`](Self::Invocation) so the authorize boundary can
    /// name the forged issuer.
    #[error("Delegation from '{issuer}' does not carry a valid signature: {detail}")]
    InvalidDelegationSignature {
        /// The principal the forged proof claims as its issuer.
        issuer: Did,
        /// Human-readable description of the verification failure.
        detail: String,
    },

    /// A delegation in the chain has been revoked by a principal entitled
    /// to revoke it. Kept distinct from a signature failure so the authorize
    /// boundary can say the authority was withdrawn rather than forged.
    #[error("Delegation '{cid}' was revoked by '{revoker}'")]
    Revoked {
        /// The revoked delegation.
        cid: Cid,
        /// The principal that revoked it.
        revoker: Did,
    },

    /// The chain does not authorize the invocation: a link's audience,
    /// subject, command, policy, or validity window did not hold up.
    ///
    /// Carries the [`CheckFailed`] itself rather than its rendered text,
    /// so a caller answering this to its own clients can distinguish an
    /// expired proof from a forged chain and say which. Rendering it
    /// early would leave every one of these looking like malformed
    /// input.
    #[error(transparent)]
    Unauthorized(#[from] crate::invocation::CheckFailed),

    /// Invalid configuration.
    #[error("Configuration error: {0}")]
    Configuration(String),
}

/// UCAN Container version key
pub const CONTAINER_VERSION: &str = "ctn-v1";

/// The most a compressed container is allowed to inflate to: a chain
/// is a few kilobytes and a header block is bounded well under this,
/// so anything larger is not a container but an attempt to exhaust
/// the reader.
pub const MAX_INFLATED_BYTES: usize = 16 * 1024 * 1024;

/// How a serialized container is encoded and whether it is compressed,
/// named by the byte the spec prepends to the serialization.
///
/// The raw forms suit a binary body; the base64 forms suit a header or
/// a URL. Compression trades CPU for bytes: a chain's signatures, keys
/// and nonces do not compress, and its repeated principals, commands
/// and policies do, so whether it pays is a per-chain question.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tag {
    /// `@`: DAG-CBOR bytes as they are.
    Raw,
    /// `B`: base64, standard alphabet, padded.
    Base64,
    /// `C`: base64, URL alphabet, unpadded.
    Base64Url,
    /// `M`: gzip over the DAG-CBOR bytes.
    RawGzip,
    /// `O`: gzip, then base64, standard alphabet, padded.
    Base64Gzip,
    /// `P`: gzip, then base64, URL alphabet, unpadded.
    Base64UrlGzip,
}

impl Tag {
    /// The byte the spec prepends for this tag.
    pub fn byte(self) -> u8 {
        match self {
            Tag::Raw => b'@',
            Tag::Base64 => b'B',
            Tag::Base64Url => b'C',
            Tag::RawGzip => b'M',
            Tag::Base64Gzip => b'O',
            Tag::Base64UrlGzip => b'P',
        }
    }

    /// The tag a serialization's first byte names, if it names one.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            b'@' => Some(Tag::Raw),
            b'B' => Some(Tag::Base64),
            b'C' => Some(Tag::Base64Url),
            b'M' => Some(Tag::RawGzip),
            b'O' => Some(Tag::Base64Gzip),
            b'P' => Some(Tag::Base64UrlGzip),
            _ => None,
        }
    }

    /// Whether the serialization is gzip-compressed.
    pub fn compressed(self) -> bool {
        matches!(self, Tag::RawGzip | Tag::Base64Gzip | Tag::Base64UrlGzip)
    }

    /// Whether the serialization is text: base64 in either alphabet.
    pub fn is_text(self) -> bool {
        !matches!(self, Tag::Raw | Tag::RawGzip)
    }

    fn encode_base64(self, bytes: &[u8]) -> Vec<u8> {
        use base64::Engine as _;
        match self {
            Tag::Raw | Tag::RawGzip => bytes.to_vec(),
            Tag::Base64 | Tag::Base64Gzip => base64::engine::general_purpose::STANDARD
                .encode(bytes)
                .into_bytes(),
            Tag::Base64Url | Tag::Base64UrlGzip => base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(bytes)
                .into_bytes(),
        }
    }

    fn decode_base64(self, bytes: &[u8]) -> Result<Vec<u8>, ContainerError> {
        use base64::Engine as _;
        let decoded = match self {
            Tag::Raw | Tag::RawGzip => return Ok(bytes.to_vec()),
            Tag::Base64 | Tag::Base64Gzip => {
                base64::engine::general_purpose::STANDARD.decode(bytes)
            }
            Tag::Base64Url | Tag::Base64UrlGzip => {
                base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(bytes)
            }
        };
        decoded.map_err(|e| ContainerError::Invocation(format!("container is not base64: {e}")))
    }
}

/// A UCAN container holding a sequence of DAG-CBOR encoded tokens.
///
/// This is the wire format for UCAN delegation chains and invocation
/// chains. The container is the DAG-CBOR map `{ "ctn-v1": [token
/// bytes...] }`; [`encode`](Self::encode) and [`decode`](Self::decode)
/// carry it as the spec's tagged serialization, and
/// [`to_bytes`](Self::to_bytes) and [`from_bytes`](Self::from_bytes) as
/// the bare map, which is what a request body has always carried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Container {
    /// The DAG-CBOR encoded tokens in order.
    tokens: Vec<Vec<u8>>,
}

impl Container {
    /// Create a new container with the given token bytes.
    ///
    /// # Arguments
    ///
    /// * `tokens` - Vector of DAG-CBOR encoded token bytes
    pub fn new(tokens: Vec<Vec<u8>>) -> Self {
        Self { tokens }
    }

    /// Get the tokens in this container.
    pub fn tokens(&self) -> &[Vec<u8>] {
        &self.tokens
    }

    /// Consume the container and return the tokens.
    pub fn into_tokens(self) -> Vec<Vec<u8>> {
        self.tokens
    }

    /// Parse a container from DAG-CBOR bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The bytes are not valid DAG-CBOR
    /// - The container is missing the "ctn-v1" key
    /// - The tokens array is invalid
    /// - The container is empty
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ContainerError> {
        // Deserialize as a map with "ctn-v1" key
        let container: BTreeMap<String, Ipld> =
            serde_ipld_dagcbor::from_slice(bytes).map_err(|e| {
                ContainerError::Invocation(format!("failed to decode container: {}", e))
            })?;

        // Extract the token array under "ctn-v1"
        let tokens_ipld = container.get(CONTAINER_VERSION).ok_or_else(|| {
            ContainerError::Invocation(format!("missing '{}' key", CONTAINER_VERSION))
        })?;

        let Ipld::List(tokens) = tokens_ipld else {
            return Err(ContainerError::Invocation(
                "tokens must be an array".to_string(),
            ));
        };

        if tokens.is_empty() {
            return Err(ContainerError::Invocation(
                "container must contain at least one token".to_string(),
            ));
        }

        // Extract token bytes
        let mut token_bytes: Vec<Vec<u8>> = Vec::with_capacity(tokens.len());
        for (i, token) in tokens.iter().enumerate() {
            let Ipld::Bytes(bytes) = token else {
                return Err(ContainerError::Invocation(format!(
                    "token {} must be bytes",
                    i
                )));
            };
            token_bytes.push(bytes.clone());
        }

        Ok(Self {
            tokens: token_bytes,
        })
    }

    /// Serialize the container to DAG-CBOR bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ContainerError> {
        self.clone().into_bytes()
    }

    /// Serialize the container to DAG-CBOR bytes, consuming it.
    ///
    /// Callers that build a container purely to encode it, every
    /// chain's `to_bytes`, avoid copying every token this way.
    pub fn into_bytes(self) -> Result<Vec<u8>, ContainerError> {
        // Build container: { "ctn-v1": [token_bytes...] }
        let tokens: Vec<Ipld> = self.tokens.into_iter().map(Ipld::Bytes).collect();
        let mut container: BTreeMap<String, Ipld> = BTreeMap::new();
        container.insert(CONTAINER_VERSION.to_string(), Ipld::List(tokens));

        serde_ipld_dagcbor::to_vec(&container)
            .map_err(|e| ContainerError::Invocation(format!("failed to encode container: {}", e)))
    }

    /// Serialize the container as the spec's tagged form: the tag's
    /// byte, then the DAG-CBOR bytes compressed and encoded as the tag
    /// says. A text tag yields ASCII, fit for a header.
    pub fn encode(self, tag: Tag) -> Result<Vec<u8>, ContainerError> {
        let mut bytes = self.into_bytes()?;
        if tag.compressed() {
            use std::io::Write as _;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder
                .write_all(&bytes)
                .and_then(|()| encoder.finish())
                .map(|compressed| bytes = compressed)
                .map_err(|e| ContainerError::Invocation(format!("failed to compress: {e}")))?;
        }
        let mut encoded = Vec::with_capacity(1 + bytes.len() * 4 / 3);
        encoded.push(tag.byte());
        encoded.extend(tag.encode_base64(&bytes));
        Ok(encoded)
    }

    /// Read a container from the spec's tagged form, whichever tag it
    /// carries. A compressed container may inflate to at most
    /// [`MAX_INFLATED_BYTES`].
    pub fn decode(encoded: &[u8]) -> Result<Self, ContainerError> {
        let Some((&first, rest)) = encoded.split_first() else {
            return Err(ContainerError::Invocation("container is empty".to_string()));
        };
        let tag = Tag::from_byte(first).ok_or_else(|| {
            ContainerError::Invocation(format!("container has an unknown tag byte {first:#04x}"))
        })?;
        let mut bytes = tag.decode_base64(rest)?;
        if tag.compressed() {
            use std::io::Read as _;
            let mut inflated = Vec::new();
            flate2::read::GzDecoder::new(bytes.as_slice())
                .take(MAX_INFLATED_BYTES as u64 + 1)
                .read_to_end(&mut inflated)
                .map_err(|e| ContainerError::Invocation(format!("failed to inflate: {e}")))?;
            if inflated.len() > MAX_INFLATED_BYTES {
                return Err(ContainerError::Invocation(format!(
                    "container inflates past {MAX_INFLATED_BYTES} bytes"
                )));
            }
            bytes = inflated;
        }
        Self::from_bytes(&bytes)
    }

    /// Check if the container is empty.
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Get the number of tokens in the container.
    pub fn len(&self) -> usize {
        self.tokens.len()
    }
}

impl TryFrom<&[u8]> for Container {
    type Error = ContainerError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_roundtrips_through_bytes() {
        let original_bytes = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]];

        let container = Container::new(original_bytes.clone());
        let serialized = container.to_bytes().unwrap();
        let parsed = Container::from_bytes(&serialized).unwrap();

        assert_eq!(parsed.tokens(), &original_bytes[..]);
    }

    #[test]
    fn it_roundtrips_through_every_tag() {
        let tokens = vec![vec![1u8; 300], vec![2u8; 300], (0..255).collect()];
        for tag in [
            Tag::Raw,
            Tag::Base64,
            Tag::Base64Url,
            Tag::RawGzip,
            Tag::Base64Gzip,
            Tag::Base64UrlGzip,
        ] {
            let encoded = Container::new(tokens.clone()).encode(tag).unwrap();
            assert_eq!(encoded[0], tag.byte(), "{tag:?} leads with its byte");
            assert_eq!(
                Tag::from_byte(encoded[0]),
                Some(tag),
                "{tag:?} reads back from its byte"
            );
            if tag.is_text() {
                assert!(
                    encoded.iter().all(u8::is_ascii),
                    "{tag:?} is text, fit for a header"
                );
            }
            let parsed = Container::decode(&encoded).unwrap();
            assert_eq!(parsed.tokens(), &tokens[..], "{tag:?} roundtrips");
        }
    }

    #[test]
    fn it_compresses_what_repeats() {
        let tokens = vec![vec![7u8; 2048]; 3];
        let plain = Container::new(tokens.clone())
            .encode(Tag::Base64Url)
            .unwrap();
        let packed = Container::new(tokens).encode(Tag::Base64UrlGzip).unwrap();
        assert!(
            packed.len() < plain.len() / 10,
            "{} vs {}",
            packed.len(),
            plain.len()
        );
    }

    #[test]
    fn it_rejects_an_unknown_tag() {
        let mut encoded = Container::new(vec![vec![1]])
            .encode(Tag::Base64Url)
            .unwrap();
        encoded[0] = b'Z';
        let error = Container::decode(&encoded).unwrap_err().to_string();
        assert!(error.contains("unknown tag"), "{error}");
        assert!(Container::decode(b"").is_err());
    }

    #[test]
    fn it_rejects_text_that_is_not_base64() {
        assert!(Container::decode(b"C!!!not base64!!!").is_err());
    }

    #[test]
    fn it_rejects_a_container_that_inflates_past_the_limit() {
        use std::io::Write as _;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        let zeros = vec![0u8; 1024 * 1024];
        for _ in 0..(MAX_INFLATED_BYTES / zeros.len() + 1) {
            encoder.write_all(&zeros).unwrap();
        }
        let mut encoded = vec![Tag::RawGzip.byte()];
        encoded.extend(encoder.finish().unwrap());
        let error = Container::decode(&encoded).unwrap_err().to_string();
        assert!(error.contains("inflates past"), "{error}");
    }

    #[test]
    fn it_fails_on_empty_container() {
        let container = Container::new(vec![]);
        let serialized = container.to_bytes().unwrap();
        let result = Container::from_bytes(&serialized);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least one token")
        );
    }

    #[test]
    fn it_fails_on_missing_version_key() {
        let mut container: BTreeMap<String, Ipld> = BTreeMap::new();
        container.insert("wrong-key".to_string(), Ipld::List(vec![]));
        let bytes = serde_ipld_dagcbor::to_vec(&container).unwrap();

        let result = Container::from_bytes(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ctn-v1"));
    }
}
