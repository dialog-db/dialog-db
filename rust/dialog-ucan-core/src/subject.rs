//! UCAN Subject type.

use dialog_varsig::did::Did;
use serde::{Serialize, de::Deserialize, ser::Serializer};
use std::fmt::Display;

/// The Subject of a delegation.
///
/// This represents what is being delegated to be later invoked.
/// To allow for powerline delegation (a node in the auth graph
/// that is a mere proxy for ANY capability), the wildcard `Any`
/// may be used.
///
/// Since it is so powerful, only use `Any` directly if you know
/// what you're doing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Subject {
    /// A specific subject (recommended)
    Specific(Did),

    /// A wildcard subject (specialized use case)
    Any,
}

impl Subject {
    /// Check that the [`Subject`] either matches the given DID, or is `Any`.
    #[must_use]
    pub fn allows(&self, subject: &Did) -> bool {
        match self {
            Subject::Specific(did) => did == subject,
            Subject::Any => true,
        }
    }

    /// Both sides match, or one is `Any`.
    #[must_use]
    pub fn coherent(&self, other: &Self) -> bool {
        match (self, other) {
            (Subject::Any, _) | (_, Subject::Any) => true,
            (Subject::Specific(did), Subject::Specific(other_did)) => did == other_did,
        }
    }
}

impl From<Did> for Subject {
    fn from(subject: Did) -> Self {
        Subject::Specific(subject)
    }
}

impl Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Subject::Specific(did) => Display::fmt(did, f),
            Subject::Any => "Null".fmt(f),
        }
    }
}

impl Serialize for Subject {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Subject::Specific(did) => did.serialize(serializer),
            Subject::Any => serializer.serialize_none(),
        }
    }
}

impl<'de> Deserialize<'de> for Subject {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_value::Value::deserialize(deserializer)?;

        if value == serde_value::Value::Option(None) {
            return Ok(Subject::Any);
        }

        if let Ok(did) = Did::deserialize(value.clone()) {
            return Ok(Subject::Specific(did));
        }

        Err(serde::de::Error::custom("invalid subject format"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_credentials::ed25519::Ed25519Signer;
    use dialog_varsig::{did::Did, principal::Principal};
    use serde_ipld_dagcbor::{from_slice, to_vec};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test;

    #[test]
    fn any_serializes_to_null() {
        let subject = Subject::Any;
        let bytes = to_vec(&subject).unwrap();
        // CBOR null is encoded as 0xf6
        assert_eq!(bytes, vec![0xf6]);
    }

    #[test]
    fn any_deserializes_from_null() {
        // CBOR null is encoded as 0xf6
        let bytes = vec![0xf6];
        let subject: Subject = from_slice(&bytes).unwrap();
        assert_eq!(subject, Subject::Any);
    }

    #[test]
    fn any_roundtrip() {
        let subject = Subject::Any;
        let bytes = to_vec(&subject).unwrap();
        let decoded: Subject = from_slice(&bytes).unwrap();
        assert_eq!(decoded, Subject::Any);
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn specific_roundtrip() {
        let signer = Ed25519Signer::import(&[55u8; 32]).await.unwrap();
        let did_key: Did = signer.did();
        let subject = Subject::Specific(did_key.clone());

        let bytes = to_vec(&subject).unwrap();
        let decoded: Subject = from_slice(&bytes).unwrap();

        assert_eq!(decoded, Subject::Specific(did_key));
    }
}
