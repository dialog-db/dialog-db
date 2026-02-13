//! [Nonce]s & utilities.
//!
//! [Nonce]: https://en.wikipedia.org/wiki/Cryptographic_nonce

use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};
use std::{fmt, hash::Hash};
use thiserror::Error;

#[cfg(any(test, feature = "test_utils"))]
use arbitrary::Arbitrary;

/// Known [`Nonce`] types
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(into = "SerialNonce", from = "SerialNonce")]
#[cfg_attr(any(test, feature = "test_utils"), derive(Arbitrary))]
pub enum Nonce {
    /// 128-bit, 16-byte nonce
    Nonce16([u8; 16]),

    /// Dynamic sized nonce
    Custom(Vec<u8>),
}

impl PartialEq for Nonce {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Nonce::Nonce16(a), Nonce::Nonce16(b)) => a == b,
            (Nonce::Custom(a), Nonce::Custom(b)) => a == b,
            (Nonce::Custom(a), Nonce::Nonce16(b)) => a.as_slice() == b,
            (Nonce::Nonce16(a), Nonce::Custom(b)) => a == b.as_slice(),
        }
    }
}

impl From<[u8; 16]> for Nonce {
    fn from(s: [u8; 16]) -> Self {
        Nonce::Nonce16(s)
    }
}

impl From<Nonce> for Vec<u8> {
    fn from(nonce: Nonce) -> Self {
        match nonce {
            Nonce::Nonce16(arr) => arr.to_vec(),
            Nonce::Custom(bytes) => bytes,
        }
    }
}

impl From<Vec<u8>> for Nonce {
    fn from(nonce: Vec<u8>) -> Self {
        if let Ok(sixteen) = <[u8; 16]>::try_from(nonce.clone()) {
            return sixteen.into();
        }

        Nonce::Custom(nonce)
    }
}

impl Nonce {
    /// Generate a 128-bit, 16-byte nonce
    ///
    /// # Arguments
    ///
    /// * `salt` - A salt. This may be left empty, but is recommended to avoid collision.
    ///
    /// # Errors
    ///
    /// If the random number generator fails, an error is returned.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use dialog_ucan::crypto::nonce::Nonce;
    /// #
    /// let mut salt = "did:example:123".as_bytes().to_vec();
    /// let nonce = Nonce::generate_16().unwrap();
    ///
    /// assert_eq!(Vec::<u8>::from(nonce).len(), 16);
    /// ```
    pub fn generate_16() -> Result<Nonce, getrandom::Error> {
        let mut buf = [0; 16];
        getrandom::getrandom(&mut buf)?;
        Ok(Nonce::Nonce16(buf))
    }
}

impl fmt::Display for Nonce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "0x")?;
        }

        let nonce_bytes = match self {
            Nonce::Nonce16(nonce) => nonce.as_slice(),
            Nonce::Custom(nonce) => nonce.as_slice(),
        };

        nonce_bytes
            .iter()
            .try_fold((), |(), byte| write!(f, "{byte:02x}"))
    }
}

impl From<Nonce> for Ipld {
    fn from(nonce: Nonce) -> Self {
        match nonce {
            Nonce::Nonce16(nonce) => Ipld::Bytes(nonce.to_vec()),
            Nonce::Custom(nonce) => Ipld::Bytes(nonce),
        }
    }
}

impl TryFrom<Ipld> for Nonce {
    type Error = NoncesMustBeBytes;

    #[allow(clippy::expect_used)]
    fn try_from(ipld: Ipld) -> Result<Self, Self::Error> {
        if let Ipld::Bytes(v) = ipld {
            match v.len() {
                16 => Ok(Nonce::Nonce16(
                    v.try_into()
                        .expect("16 bytes because we checked in the match"),
                )),
                _ => Ok(Nonce::Custom(v)),
            }
        } else {
            Err(NoncesMustBeBytes)
        }
    }
}

/// Error indicating that nonces must be byte arrays
#[derive(Debug, Clone, Copy, Error)]
#[error("nonces must be byte arrays")]
pub struct NoncesMustBeBytes;

impl Hash for Nonce {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Nonce::Nonce16(nonce) => nonce.to_vec().hash(state),
            Nonce::Custom(nonce) => nonce.hash(state),
        }
    }
}

/// Helper for serializing nonce as bytes
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
struct SerialNonce(serde_bytes::ByteBuf);

impl From<Nonce> for SerialNonce {
    fn from(nonce: Nonce) -> Self {
        SerialNonce(serde_bytes::ByteBuf::from(Vec::from(nonce)))
    }
}

impl From<SerialNonce> for Nonce {
    fn from(bytes: SerialNonce) -> Self {
        Nonce::from(bytes.0.into_vec())
    }
}

#[cfg(test)]
mod test {
    use testresult::TestResult;

    use super::*;

    #[test]
    fn ipld_roundtrip_16() -> TestResult {
        let nonce = Nonce::generate_16()?;
        let ipld = Ipld::from(nonce.clone());

        let inner = if let Nonce::Nonce16(nonce) = nonce {
            Ipld::Bytes(nonce.to_vec())
        } else {
            panic!("No conversion!")
        };

        assert_eq!(ipld, inner);
        assert_eq!(nonce, ipld.try_into().unwrap());

        Ok(())
    }

    #[cfg(all(feature = "property_test", not(target_arch = "wasm32")))]
    mod proptest_tests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn proptest_roundtrip_serde(bytes in any::<Vec<u8>>()) {
                let nonce = Nonce::from(bytes);
                let ipld = Ipld::from(nonce.clone());
                let de: Nonce = ipld.try_into().unwrap();

                prop_assert_eq!(nonce, de);
            }
        }
    }
}
