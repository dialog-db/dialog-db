//! Profile — a named identity backed by a signing credential.

pub mod access;
mod error;
mod open;
mod save;
mod space;
#[cfg(test)]
mod test;

pub use error::ProfileError;
pub use open::OpenProfile;
pub use save::SaveDelegation;
pub use space::SpaceHandle;

use crate::operator::OperatorBuilder;
use dialog_capability::{Capability, Subject};
use dialog_credentials::SignerCredential;
use dialog_ucan::UcanDelegation;
use dialog_varsig::{Did, Principal};

/// An opened profile — holds a signing credential.
#[derive(Debug, Clone)]
pub struct Profile {
    credential: SignerCredential,
}

impl Profile {
    /// Open a profile — loads existing or creates new.
    pub fn open(name: impl Into<String>) -> OpenProfile {
        OpenProfile::open(name.into())
    }

    /// Load an existing profile — fails if not found.
    pub fn load(name: impl Into<String>) -> OpenProfile {
        OpenProfile::load(name.into())
    }

    /// Create a new profile — fails if one already exists.
    pub fn create(name: impl Into<String>) -> OpenProfile {
        OpenProfile::create(name.into())
    }

    /// The profile's DID.
    pub fn did(&self) -> Did {
        self.credential.did()
    }

    /// The signing credential.
    pub fn credential(&self) -> &SignerCredential {
        &self.credential
    }

    /// Store a delegation chain under this profile's DID.
    pub fn save(&self, chain: UcanDelegation) -> SaveDelegation {
        SaveDelegation {
            did: self.did(),
            chain,
        }
    }

    /// Get an access handle for claiming and delegating capabilities.
    pub fn access(&self) -> access::Access<'_> {
        access::Access::new(&self.credential)
    }

    /// Derive an operator from this profile with the given context seed.
    pub fn derive(&self, context: impl Into<Vec<u8>>) -> OperatorBuilder {
        OperatorBuilder::new(self, context.into())
    }

    /// Get a handle to a named repository space under this profile.
    ///
    /// The returned handle can open, load, or create a repository
    /// through an operator that verifies the profile DID.
    pub fn repository(&self, name: impl Into<String>) -> SpaceHandle {
        SpaceHandle {
            profile_did: self.did(),
            name: name.into(),
        }
    }
}

impl From<&Profile> for Capability<Subject> {
    fn from(p: &Profile) -> Self {
        Subject::from(p.credential.did()).into()
    }
}

impl Principal for Profile {
    fn did(&self) -> Did {
        self.credential.did()
    }
}

impl TryFrom<dialog_credentials::Credential> for Profile {
    type Error = ProfileError;

    fn try_from(credential: dialog_credentials::Credential) -> Result<Self, ProfileError> {
        match credential {
            dialog_credentials::Credential::Signer(s) => Ok(Profile { credential: s }),
            dialog_credentials::Credential::Verifier(_) => Err(ProfileError::Key(
                "profile credential is verifier-only".into(),
            )),
        }
    }
}
