//! UCAN Container format utilities.
//!
//! This module provides [`Container`], a type that represents a UCAN container
//! following the [UCAN Container spec](https://github.com/ucan-wg/container).
//!
//! The container format is:
//! ```text
//! { "ctn-v1": [token_bytes_0, token_bytes_1, ..., token_bytes_n] }
//! ```
//!
//! Where tokens are DAG-CBOR serialized UCANs.
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

/// The container key carrying the bytes an invocation acts on, beside
/// the tokens. See [`Container::payload`].
pub const PAYLOAD_KEY: &str = "payload";

/// A UCAN container holding a sequence of DAG-CBOR encoded tokens.
///
/// This is the wire format for UCAN delegation chains and invocation chains.
/// The container is serialized as `{ "ctn-v1": [token_bytes...] }`, with
/// an optional `"payload"` key beside it (see [`Container::payload`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Container {
    /// The DAG-CBOR encoded tokens in order.
    tokens: Vec<Vec<u8>>,
    /// Bytes the invocation acts on, carried beside the tokens so one
    /// request can both prove an operation and supply its content: the
    /// block an `archive` put stores, the value a `memory` publish sets.
    /// Absent from a container that only proves.
    payload: Option<Vec<u8>>,
}

impl Container {
    /// Create a new container with the given token bytes.
    ///
    /// # Arguments
    ///
    /// * `tokens` - Vector of DAG-CBOR encoded token bytes
    pub fn new(tokens: Vec<Vec<u8>>) -> Self {
        Self {
            tokens,
            payload: None,
        }
    }

    /// Attach the bytes the invocation acts on. See [`Container::payload`].
    pub fn with_payload(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.payload = Some(payload.into());
        self
    }

    /// The bytes the invocation acts on, when the container carries them.
    ///
    /// A responder that understands the payload performs the operation
    /// with it in the same request that proved it. One that does not
    /// reads the tokens alone, the payload being a separate key it never
    /// looks at, and answers as it always has.
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    /// Take the payload out of the container, leaving the tokens.
    pub fn take_payload(&mut self) -> Option<Vec<u8>> {
        self.payload.take()
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

        let payload = match container.get(PAYLOAD_KEY) {
            None => None,
            Some(Ipld::Bytes(bytes)) => Some(bytes.clone()),
            Some(_) => {
                return Err(ContainerError::Invocation(format!(
                    "'{PAYLOAD_KEY}' must be bytes"
                )));
            }
        };

        Ok(Self {
            tokens: token_bytes,
            payload,
        })
    }

    /// Serialize the container to DAG-CBOR bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ContainerError> {
        self.clone().into_bytes()
    }

    /// Serialize the container to DAG-CBOR bytes, consuming it.
    ///
    /// Callers that build a container purely to encode it — every
    /// chain's `to_bytes` — avoid copying every token this way.
    pub fn into_bytes(self) -> Result<Vec<u8>, ContainerError> {
        // Build container: { "ctn-v1": [token_bytes...] }
        let tokens: Vec<Ipld> = self.tokens.into_iter().map(Ipld::Bytes).collect();
        let mut container: BTreeMap<String, Ipld> = BTreeMap::new();
        container.insert(CONTAINER_VERSION.to_string(), Ipld::List(tokens));
        if let Some(payload) = self.payload {
            container.insert(PAYLOAD_KEY.to_string(), Ipld::Bytes(payload));
        }

        serde_ipld_dagcbor::to_vec(&container)
            .map_err(|e| ContainerError::Invocation(format!("failed to encode container: {}", e)))
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
    fn it_carries_a_payload_beside_the_tokens() {
        let container = Container::new(vec![vec![1, 2, 3]]).with_payload(vec![9, 9, 9]);
        let parsed = Container::from_bytes(&container.to_bytes().unwrap()).unwrap();

        assert_eq!(parsed.tokens(), &[vec![1, 2, 3]][..]);
        assert_eq!(parsed.payload(), Some(&[9, 9, 9][..]));
        assert_eq!(parsed, container);
    }

    #[test]
    fn it_reads_a_container_without_a_payload_as_having_none() {
        let container = Container::new(vec![vec![1, 2, 3]]);
        let parsed = Container::from_bytes(&container.to_bytes().unwrap()).unwrap();
        assert_eq!(parsed.payload(), None);
    }

    /// The payload is a separate key, so a reader that only knows the
    /// tokens still decodes the tokens from a container that carries one.
    #[test]
    fn it_keeps_the_tokens_readable_by_a_tokens_only_reader() {
        let container = Container::new(vec![vec![1, 2, 3]]).with_payload(vec![9]);
        let bytes = container.to_bytes().unwrap();
        let map: BTreeMap<String, Ipld> = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
        assert!(matches!(map.get(CONTAINER_VERSION), Some(Ipld::List(_))));
        assert!(matches!(map.get(PAYLOAD_KEY), Some(Ipld::Bytes(_))));
    }

    #[test]
    fn it_rejects_a_payload_that_is_not_bytes() {
        let mut map: BTreeMap<String, Ipld> = BTreeMap::new();
        map.insert(
            CONTAINER_VERSION.to_string(),
            Ipld::List(vec![Ipld::Bytes(vec![1])]),
        );
        map.insert(PAYLOAD_KEY.to_string(), Ipld::String("no".into()));
        let bytes = serde_ipld_dagcbor::to_vec(&map).unwrap();
        assert!(Container::from_bytes(&bytes).is_err());
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
