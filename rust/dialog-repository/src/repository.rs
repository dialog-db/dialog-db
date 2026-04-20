//! Capability-based repository system.
//!
//! This module provides a repository abstraction built on top of the
//! capability-based effect system (`dialog-capability` / `dialog-effects`).
//!
//! - [`archive`] — CAS adapter bridging capabilities with prolly tree storage
//! - [`memory`] — Transactional memory cells with edition tracking
//! - [`revision`] — Revision tracking and logical timestamps

mod archive;
mod branch;
mod create;
mod error;
mod load;
mod memory;
mod open;
mod remote;
mod revision;
mod tree;

pub use archive::*;
pub use branch::*;
pub use create::*;
pub use error::*;
pub use load::*;
pub use memory::*;
pub use open::*;
pub use remote::*;
pub use revision::*;
pub use tree::*;

use dialog_capability::{Capability, Did, Subject};
use dialog_credentials::Ed25519Signer;
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_effects::space::SpaceSubjectExt;
use dialog_operator::SpaceHandle;
use dialog_operator::access::Access as ProfileAccess;
use dialog_varsig::Principal;

/// A repository scoped to a specific subject.
///
/// The credential type parameter determines access level:
/// - `Repository<SignerCredential>` -- owns the keypair, can delegate
/// - `Repository<Credential>` -- either signer or verifier, determined at runtime
pub struct Repository<C: Principal = Credential> {
    credential: C,
}

impl<C: Principal> Repository<C> {
    fn new(credential: C) -> Self {
        Self { credential }
    }

    /// Get the credential.
    pub fn credential(&self) -> &C {
        &self.credential
    }

    /// The subject DID.
    pub fn did(&self) -> Did {
        self.credential.did()
    }

    /// The subject.
    pub fn subject(&self) -> Subject {
        self.did().into()
    }

    /// Get a branch reference for the given name.
    ///
    /// Call `.open()` or `.load()` on the returned reference.
    pub fn branch(&self, name: impl Into<String>) -> BranchReference {
        self.subject().branch(name)
    }

    /// Get a remote reference for the given name.
    ///
    /// Call `.create(address)` or `.load()` on the returned reference.
    pub fn remote(&self, name: impl Into<String>) -> RemoteReference {
        self.subject().remote(name)
    }
}

impl<C: Principal> Principal for Repository<C> {
    fn did(&self) -> Did {
        self.credential.did()
    }
}

impl<C: Principal> From<&Repository<C>> for Capability<Subject> {
    fn from(r: &Repository<C>) -> Self {
        Subject::from(r.did()).into()
    }
}

impl Repository<SignerCredential> {
    /// Access handle for claiming and delegating capabilities.
    pub fn access(&self) -> ProfileAccess<'_> {
        ProfileAccess::new(&self.credential)
    }
}

impl Repository {
    /// Access handle for claiming and delegating capabilities.
    ///
    /// Returns `None` if the credential is verifier-only.
    pub fn try_access(&self) -> Option<ProfileAccess<'_>> {
        match &self.credential {
            Credential::Signer(s) => Some(ProfileAccess::new(s)),
            Credential::Verifier(_) => None,
        }
    }
}

impl From<Credential> for Repository {
    fn from(credential: Credential) -> Self {
        Self::new(credential)
    }
}

impl From<SignerCredential> for Repository<SignerCredential> {
    fn from(credential: SignerCredential) -> Self {
        Self::new(credential)
    }
}

impl TryFrom<Credential> for Repository<SignerCredential> {
    type Error = RepositoryError;

    fn try_from(credential: Credential) -> Result<Self, RepositoryError> {
        match credential {
            Credential::Signer(s) => Ok(Self::new(s)),
            Credential::Verifier(_) => Err(RepositoryError::StorageError(
                "repository credential is verifier-only".into(),
            )),
        }
    }
}

impl From<Ed25519Signer> for Repository<SignerCredential> {
    fn from(signer: Ed25519Signer) -> Self {
        SignerCredential::from(signer).into()
    }
}

/// Extension trait for opening repositories from a [`SpaceHandle`].
///
/// Enables `profile.repository("name").open().perform(&operator)`.
pub trait RepositoryExt {
    /// Open or create a repository, loading existing or creating new.
    fn open(self) -> OpenRepository;

    /// Load an existing repository, failing if not found.
    fn load(self) -> LoadRepository;

    /// Create a new repository, failing if one already exists.
    fn create(self) -> CreateRepository;
}

impl RepositoryExt for SpaceHandle {
    fn open(self) -> OpenRepository {
        OpenRepository(self.profile_did.space(self.name))
    }

    fn load(self) -> LoadRepository {
        LoadRepository(self.profile_did.space(self.name))
    }

    fn create(self) -> CreateRepository {
        CreateRepository(self.profile_did.space(self.name))
    }
}
