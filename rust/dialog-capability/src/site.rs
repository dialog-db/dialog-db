//! Site trait for declaring remote execution targets.
//!
//! A [`Site`] is a marker trait that declares what authorization format
//! and address type are needed for a target location.
//!
//! No methods — all execution logic lives in [`Fork`](crate::fork::Fork)
//! and [`Provider`](crate::Provider) impls.

use std::fmt::{self, Display, Formatter};

use crate::Effect;
use crate::fork::Fork;
use dialog_common::ConditionalSend;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// A stable identifier for a site address. Used as a key in credential stores.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

impl Display for SiteId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A typed address for a specific [`Site`].
///
/// Each site has its own address shape (e.g. S3 has an endpoint +
/// region + bucket; UCAN has an access-service endpoint). This trait
/// connects the address type back to its site via the `Site` associated
/// type, so code that holds an address can resolve the site it belongs
/// to without having to pass the site generically.
///
/// The `Into<SiteId>` bound gives every address a stable, hashable
/// identifier suitable for use as a credential-store key. Implementors
/// choose how to serialize themselves into a `SiteId`; the identifier
/// must be stable across runs for the credential store to find the
/// saved credential again.
pub trait SiteAddress:
    Into<SiteId> + Serialize + DeserializeOwned + Clone + ConditionalSend + 'static
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
/// Associates an authorization type, an address type, and a site-owned
/// fork wrapper. The wrapper is constructed from the generic
/// [`Fork<Self, Fx>`] and is where site-specific behavior (authorization
/// via [`SiteFork`](crate::fork::SiteFork)) is implemented.
pub trait Site: Clone + ConditionalSend + 'static {
    /// The authorization material for this site.
    type Authorization: ConditionalSend + 'static;

    /// The address type for this site.
    type Address: Serialize + DeserializeOwned + Clone + ConditionalSend + 'static;

    /// The site-owned fork wrapper to side step orphan-rule limitations with
    /// a generic Fork.
    type Fork<Fx: Effect>: From<Fork<Self, Fx>>;
}
