//! Extension traits for fluent credential capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::credential::prelude::*;
//! ```

use dialog_capability::{Capability, Did, SiteId, Subject};

use super::{Credential, Key, Load, Save, Secret, Site};

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

/// Extension methods on the credential capability to select key or site.
pub trait CredentialCapabilityExt {
    /// The resulting key chain type.
    type Key;
    /// The resulting site chain type.
    type Site;
    /// Select a key credential by name.
    fn key(self, address: impl Into<String>) -> Self::Key;
    /// Select a site credential by address.
    fn site(self, address: impl Into<SiteId>) -> Self::Site;
}

impl CredentialCapabilityExt for Capability<Credential> {
    type Key = Capability<Key>;
    type Site = Capability<Site>;

    fn key(self, address: impl Into<String>) -> Capability<Key> {
        self.attenuate(Key::new(address))
    }

    fn site(self, address: impl Into<SiteId>) -> Capability<Site> {
        self.attenuate(Site::new(address))
    }
}

/// Extension methods for invoking effects on a key credential.
pub trait CredentialKeyExt {
    /// The resulting load chain type.
    type Load;
    /// The resulting save chain type.
    type Save;
    /// Load a key credential from this address.
    fn load(self) -> Self::Load;
    /// Save a key credential to this address.
    fn save(self, credential: dialog_credentials::Credential) -> Self::Save;
}

impl CredentialKeyExt for Capability<Key> {
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

/// Extension methods for invoking effects on a site credential.
pub trait CredentialSiteExt {
    /// The resulting load chain type.
    type Load;
    /// The resulting save chain type.
    type Save;
    /// Load a site secret from this address.
    fn load(self) -> Self::Load;
    /// Save a site secret to this address.
    fn save(self, secret: Secret) -> Self::Save;
}

impl CredentialSiteExt for Capability<Site> {
    type Load = Capability<Load<Secret>>;
    type Save = Capability<Save<Secret>>;

    fn load(self) -> Capability<Load<Secret>> {
        self.invoke(Load::new())
    }

    fn save(self, secret: Secret) -> Capability<Save<Secret>> {
        self.invoke(Save::new(secret))
    }
}
