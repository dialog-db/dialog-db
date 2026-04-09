//! Extension traits for fluent credential capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::credential::prelude::*;
//! ```

use dialog_capability::{Capability, Did, Subject};

use super::{Credential, Key, Load, Save};

/// Extension trait to start a credential capability chain.
pub trait CredentialSubjectExt {
    /// The resulting credential chain type.
    type Credential;
    /// Begin a credential capability chain.
    fn credential(self) -> Self::Credential;
}

impl CredentialSubjectExt for Subject {
    type Credential = Capability<Credential>;
    fn credential(self) -> Capability<Credential> {
        self.attenuate(Credential)
    }
}

impl CredentialSubjectExt for Did {
    type Credential = Capability<Credential>;
    fn credential(self) -> Capability<Credential> {
        Subject::from(self).attenuate(Credential)
    }
}

/// Extension trait for attenuating a credential capability with an address.
pub trait CredentialCapabilityExt {
    /// The resulting key chain type.
    type Key;
    /// Attenuate this credential capability with a key address.
    fn key(self, address: impl Into<String>) -> Self::Key;
}

impl CredentialCapabilityExt for Capability<Credential> {
    type Key = Capability<Key>;
    fn key(self, address: impl Into<String>) -> Capability<Key> {
        self.attenuate(Key::new(address))
    }
}

/// Extension methods for invoking effects on a credential key address.
pub trait CredentialAddressExt {
    /// The resulting load chain type.
    type Load;
    /// The resulting save chain type.
    type Save;
    /// Load a credential from this address.
    fn load(self) -> Self::Load;
    /// Save a credential to this address.
    fn save(self, credential: dialog_credentials::Credential) -> Self::Save;
}

impl CredentialAddressExt for Capability<Key> {
    type Load = Capability<Load<dialog_credentials::Credential>>;
    type Save = Capability<Save<dialog_credentials::Credential>>;

    fn load(self) -> Capability<Load<dialog_credentials::Credential>> {
        self.invoke(Load::new())
    }

    fn save(
        self,
        credential: dialog_credentials::Credential,
    ) -> Capability<Save<dialog_credentials::Credential>> {
        self.invoke(Save::new(credential))
    }
}
