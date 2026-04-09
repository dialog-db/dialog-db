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
    /// The resulting address chain type.
    type Address;
    /// Begin a credential capability chain scoped to the given address.
    fn credential(self, address: impl Into<String>) -> Self::Address;
}

impl CredentialSubjectExt for Subject {
    type Address = Capability<Key>;
    fn credential(self, address: impl Into<String>) -> Capability<Key> {
        self.attenuate(Credential).attenuate(Key::new(address))
    }
}

impl CredentialSubjectExt for Did {
    type Address = Capability<Key>;
    fn credential(self, address: impl Into<String>) -> Capability<Key> {
        Subject::from(self)
            .attenuate(Credential)
            .attenuate(Key::new(address))
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
