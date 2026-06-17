//! Extension traits for fluent credential capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::credential::prelude::*;
//! ```

use super::{Credential, Key, Load, Save, Secret, Site};
use dialog_capability::{Capability, Did, Policy, SiteId, Subject};
use dialog_credentials::Credential as KeyCredential;

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
    fn save(self, credential: KeyCredential) -> Self::Save;
}

impl CredentialKeyExt for Capability<Key> {
    type Load = Capability<Load<KeyCredential>>;
    type Save = Capability<Save<KeyCredential>>;

    fn load(self) -> Capability<Load<KeyCredential>> {
        self.invoke(Load::new())
    }

    fn save(self, credential: KeyCredential) -> Capability<Save<KeyCredential>> {
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

/// Field accessors on `Capability<Load<Credential>>`.
pub trait LoadCredentialExt {
    /// Get the key address from the capability chain.
    fn address(&self) -> &str;
}

impl LoadCredentialExt for Capability<Load<KeyCredential>> {
    fn address(&self) -> &str {
        &Key::of(self).address
    }
}

/// Field accessors on `Capability<Save<Credential>>`.
pub trait SaveCredentialExt {
    /// Get the key address from the capability chain.
    fn address(&self) -> &str;
    /// Get the credential to save.
    fn credential(&self) -> &KeyCredential;
}

impl SaveCredentialExt for Capability<Save<KeyCredential>> {
    fn address(&self) -> &str {
        &Key::of(self).address
    }

    fn credential(&self) -> &KeyCredential {
        &Save::<KeyCredential>::of(self).credential
    }
}

/// Field accessors on `Capability<Load<Secret>>`.
pub trait LoadSecretExt {
    /// Get the site address from the capability chain.
    fn address(&self) -> &SiteId;
}

impl LoadSecretExt for Capability<Load<Secret>> {
    fn address(&self) -> &SiteId {
        &Site::of(self).address
    }
}

/// Field accessors on `Capability<Save<Secret>>`.
pub trait SaveSecretExt {
    /// Get the site address from the capability chain.
    fn address(&self) -> &SiteId;
    /// Get the secret to save.
    fn secret(&self) -> &Secret;
}

impl SaveSecretExt for Capability<Save<Secret>> {
    fn address(&self) -> &SiteId {
        &Site::of(self).address
    }

    fn secret(&self) -> &Secret {
        &Save::<Secret>::of(self).credential
    }
}
