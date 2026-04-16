//! Site trait for declaring remote execution targets.
//!
//! A [`Site`] is a marker trait that declares what authorization format
//! and address type are needed for a target location.
//!
//! No methods — all execution logic lives in [`Fork`](crate::fork::Fork)
//! and [`Provider`](crate::Provider) impls.

use dialog_common::ConditionalSend;
use dialog_varsig::Did;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// The identity performing a fork operation.
///
/// Contains the operator DID (ephemeral session key) and the profile
/// DID (long-lived identity) it acts on behalf of.
#[derive(Debug, Clone)]
pub struct SiteIssuer {
    /// The operator's DID (ephemeral session key).
    pub operator: Did,
    /// The profile's DID (long-lived identity).
    pub profile: Did,
}

/// A stable identifier for a site address.
///
/// Used as a key in credential stores. The filesystem backend hashes
/// this internally for safe filenames; other backends may use it as-is.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, serde::Deserialize)]
pub struct SiteId(String);

impl SiteId {
    /// The identifier string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for SiteId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SiteId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<&SiteId> for SiteId {
    fn from(id: &SiteId) -> Self {
        id.clone()
    }
}

impl AsRef<str> for SiteId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<SiteId> for String {
    fn from(id: SiteId) -> Self {
        id.0
    }
}

impl std::fmt::Display for SiteId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub trait SiteAddress:
    Serialize + DeserializeOwned + Clone + Into<SiteId> + ConditionalSend + 'static
{
    /// The site type this address belongs to.
    type Site: Site<Address = Self>;
}

impl<T> From<&T> for SiteId
where
    T: SiteAddress,
{
    fn from(address: &T) -> Self {
        address.clone().into()
    }
}

/// Marker trait for remote execution targets.
///
/// Associates an authorization type, address type, and claim type.
/// The claim type bundles a capability + issuer + address and knows
/// how to authorize against an environment.
pub trait Site: Clone + ConditionalSend + 'static {
    /// The authorization material for this site.
    type Authorization: ConditionalSend + 'static;

    /// The address type for this site.
    type Address: Serialize + DeserializeOwned + Clone + ConditionalSend + 'static;

    /// A claim bundles a capability + issuer + address, ready for authorization.
    type Claim<Fx: crate::Effect>: From<(crate::Capability<Fx>, SiteIssuer, Self::Address)>;
}
