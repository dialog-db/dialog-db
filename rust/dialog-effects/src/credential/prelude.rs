//! Extension traits for fluent credential capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::credential::prelude::*;
//! ```

use dialog_capability::{Capability, Did, Subject};

use super::{Credential, Load, Name, Save};

/// Extension trait to start a credential capability chain.
pub trait CredentialSubjectExt {
    /// The resulting address chain type.
    type Address;
    /// Begin a credential capability chain scoped to the given address.
    fn credential(self, address: impl Into<String>) -> Self::Address;
}

impl CredentialSubjectExt for Subject {
    type Address = Capability<Name>;
    fn credential(self, address: impl Into<String>) -> Capability<Name> {
        self.attenuate(Credential).attenuate(Name::new(address))
    }
}

impl CredentialSubjectExt for Did {
    type Address = Capability<Name>;
    fn credential(self, address: impl Into<String>) -> Capability<Name> {
        Subject::from(self)
            .attenuate(Credential)
            .attenuate(Name::new(address))
    }
}

/// Extension methods for invoking effects on a credential address.
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

impl CredentialAddressExt for Capability<Name> {
    type Load = Capability<Load>;
    type Save = Capability<Save>;

    fn load(self) -> Capability<Load> {
        self.invoke(Load)
    }

    fn save(self, credential: dialog_credentials::Credential) -> Capability<Save> {
        self.invoke(Save { credential })
    }
}
