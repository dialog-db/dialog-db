//! Principal trait for types that have a DID identity.

use crate::did::Did;

/// An entity identified by a [DID].
///
/// Implemented by anything that has a DID — key types, signers,
/// DID documents, etc. Does not imply any cryptographic capability.
///
/// [DID]: https://www.w3.org/TR/did-core/
pub trait Principal {
    /// Returns this entity's DID.
    fn did(&self) -> Did;
}

impl Principal for Did {
    fn did(&self) -> Did {
        self.clone()
    }
}
