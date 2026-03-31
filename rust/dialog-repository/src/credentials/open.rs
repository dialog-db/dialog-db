//! Profile open command — loads or creates a profile keypair.
//!
//! [`Open`] is a self-rooted command (no subject needed). Storage backends
//! implement `Provider<Open>` to load an existing profile key or generate
//! and persist a new one.

use dialog_capability::command::Command;
use dialog_credentials::Ed25519Signer;
use dialog_effects::credential::CredentialError;
use dialog_varsig::{Did, Principal};

/// An opened profile keypair.
#[derive(Debug, Clone)]
pub struct ProfileSigner {
    signer: Ed25519Signer,
}

impl ProfileSigner {
    /// Create from an `Ed25519Signer`.
    pub fn new(signer: Ed25519Signer) -> Self {
        Self { signer }
    }

    /// The profile's DID.
    pub fn did(&self) -> Did {
        self.signer.did()
    }

    /// The underlying signer.
    pub fn signer(&self) -> &Ed25519Signer {
        &self.signer
    }

    /// Consume and return the inner signer.
    pub fn into_signer(self) -> Ed25519Signer {
        self.signer
    }
}

/// Open (load or create) a profile keypair by name.
///
/// Returns a [`ProfileSigner`] wrapping the profile's Ed25519 keypair.
/// If the profile doesn't exist, the provider generates a new keypair
/// and persists it before returning.
#[derive(Debug, Clone)]
pub struct Open {
    /// The profile name to open.
    pub name: String,
}

impl Open {
    /// Create a new Open command for the given profile name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Command for Open {
    type Input = Self;
    type Output = Result<ProfileSigner, CredentialError>;
}

impl Open {
    /// Execute this command against a provider.
    pub async fn perform<P: dialog_capability::Provider<Self>>(
        self,
        provider: &P,
    ) -> Result<ProfileSigner, CredentialError> {
        provider.execute(self).await
    }
}
