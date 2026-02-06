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
//! - [`DelegationChain`](super::DelegationChain) - A chain of delegations
//! - [`InvocationChain`](super::InvocationChain) - An invocation with its delegation chain

use crate::capability::AccessError;
use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;

/// UCAN Container version key
pub const CONTAINER_VERSION: &str = "ctn-v1";

/// A UCAN container holding a sequence of DAG-CBOR encoded tokens.
///
/// This is the wire format for UCAN delegation chains and invocation chains.
/// The container is serialized as `{ "ctn-v1": [token_bytes...] }`.
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
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AccessError> {
        // Deserialize as a map with "ctn-v1" key
        let container: BTreeMap<String, Ipld> = serde_ipld_dagcbor::from_slice(bytes)
            .map_err(|e| AccessError::Invocation(format!("failed to decode container: {}", e)))?;

        // Extract the token array under "ctn-v1"
        let tokens_ipld = container.get(CONTAINER_VERSION).ok_or_else(|| {
            AccessError::Invocation(format!("missing '{}' key", CONTAINER_VERSION))
        })?;

        let Ipld::List(tokens) = tokens_ipld else {
            return Err(AccessError::Invocation(
                "tokens must be an array".to_string(),
            ));
        };

        if tokens.is_empty() {
            return Err(AccessError::Invocation(
                "container must contain at least one token".to_string(),
            ));
        }

        // Extract token bytes
        let mut token_bytes: Vec<Vec<u8>> = Vec::with_capacity(tokens.len());
        for (i, token) in tokens.iter().enumerate() {
            let Ipld::Bytes(bytes) = token else {
                return Err(AccessError::Invocation(format!(
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
    pub fn to_bytes(&self) -> Result<Vec<u8>, AccessError> {
        // Build container: { "ctn-v1": [token_bytes...] }
        let tokens: Vec<Ipld> = self.tokens.iter().cloned().map(Ipld::Bytes).collect();
        let mut container: BTreeMap<String, Ipld> = BTreeMap::new();
        container.insert(CONTAINER_VERSION.to_string(), Ipld::List(tokens));

        serde_ipld_dagcbor::to_vec(&container)
            .map_err(|e| AccessError::Invocation(format!("failed to encode container: {}", e)))
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
    type Error = AccessError;

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
