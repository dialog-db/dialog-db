//! Credential capability hierarchy — store operations.
//!
//! Provides credential store operations scoped to a
//! repository subject via the [`Credential`] attenuation.
//!
//! Authorization types are in [`crate::access`] but re-exported here
//! for backward compatibility.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (operator DID)
//! └── Credential (ability: /credential)
//!     ├── Retrieve<C> { address } -> Effect -> Result<C, CredentialError>
//!     ├── Save<C> { address, credentials } -> Effect -> Result<(), CredentialError>
//!     ├── List<C> { prefix } -> Effect -> Result<Vec<Address<C>>, CredentialError>
//!     └── Import<M> { material: M } -> Effect -> Result<(), CredentialError>
//! ```

pub use crate::{Attenuation, Capability, Did, Effect, Policy, Subject};
use dialog_common::ConditionalSend;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use thiserror::Error;

pub use crate::site::Local;

// Re-export authorization types from access for backward compatibility.
pub use crate::access::{Access, Allow, Authorization, Authorize, AuthorizeError, Protocol};

/// Root attenuation for credential operations.
///
/// Attaches to Subject and provides the `/credential` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Credential;

impl Attenuation for Credential {
    type Of = Subject;
}

/// A typed address for looking up credentials in the credential store.
///
/// The phantom type `C` ties the address to a specific credential type,
/// ensuring type-safe lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address<C> {
    /// The identifier for this credential (e.g., endpoint URL, bucket name).
    pub id: String,
    #[serde(skip)]
    _credentials: PhantomData<C>,
}

impl<C> Address<C> {
    /// Create a new address with the given identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            _credentials: PhantomData,
        }
    }

    /// Get the address identifier.
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl<C> PartialEq for Address<C> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

/// Retrieve credentials from the credential store by address.
#[derive(Debug, Clone, Serialize, Deserialize, crate::Claim)]
#[serde(bound(deserialize = ""))]
pub struct Retrieve<C> {
    /// The address to look up.
    pub address: Address<C>,
}

impl<C> Effect for Retrieve<C>
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    type Of = Credential;
    type Output = Result<C, CredentialError>;
}

/// Save credentials to the credential store at an address.
#[derive(Debug, Clone, Serialize, Deserialize, crate::Claim)]
#[serde(bound(deserialize = "C: DeserializeOwned"))]
pub struct Save<C: Serialize> {
    /// The address to store at.
    pub address: Address<C>,
    /// The credentials to save.
    pub credentials: C,
}

impl<C> Effect for Save<C>
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    type Of = Credential;
    type Output = Result<(), CredentialError>;
}

/// Error type for credential operations.
#[derive(Debug, Error)]
pub enum CredentialError {
    /// The requested credential was not found.
    #[error("Credential not found: {0}")]
    NotFound(String),

    /// Signing operation failed.
    #[error("Signing failed: {0}")]
    SigningFailed(String),

    /// Import of credential material failed.
    #[error("Import failed: {0}")]
    ImportFailed(String),
}

/// List credential addresses by prefix.
///
/// Returns all addresses whose ID starts with the given prefix.
/// Use [`Retrieve`] to fetch the credential at each returned address.
#[derive(Debug, Clone, Serialize, Deserialize, crate::Claim)]
#[serde(bound(deserialize = ""))]
pub struct List<C> {
    /// The prefix to match against address IDs.
    pub prefix: Address<C>,
}

impl<C> List<C> {
    /// Create a new list query with the given prefix.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: Address::new(prefix),
        }
    }
}

impl<C> Effect for List<C>
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    type Of = Credential;
    type Output = Result<Vec<Address<C>>, CredentialError>;
}

/// Import credential material into the credential store.
#[derive(Debug, Clone, Serialize, Deserialize, crate::Claim)]
pub struct Import<Material: Serialize> {
    /// The credential material to import.
    pub material: Material,
}

impl<Material: Serialize + DeserializeOwned + ConditionalSend + 'static> Effect
    for Import<Material>
{
    type Of = Credential;
    type Output = Result<(), CredentialError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::did;

    #[test]
    fn it_builds_credential_claim_path() {
        let claim = Subject::from(did!("key:zSpace")).attenuate(Credential);

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/credential");
    }

    #[test]
    fn it_builds_retrieve_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .invoke(Retrieve::<String> {
                address: Address::new("s3://my-bucket"),
            });

        assert_eq!(claim.ability(), "/credential/retrieve");
    }

    #[test]
    fn it_builds_save_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .invoke(Save {
                address: Address::new("s3://my-bucket"),
                credentials: "secret-key".to_string(),
            });

        assert_eq!(claim.ability(), "/credential/save");
    }
}
