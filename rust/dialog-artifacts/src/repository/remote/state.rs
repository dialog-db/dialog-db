use std::fmt::{Display, Formatter, Result as FmtResult};

use dialog_capability::Did;
use serde::{Deserialize, Serialize};

use crate::environment::Address;

/// Persisted configuration for a remote site.
///
/// Stored in a memory cell keyed by the remote name, this captures
/// the issuer DID and the credentials used to authenticate operations
/// against this remote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteState {
    /// The DID of the issuer who has access to this remote.
    pub issuer: Did,
    /// The credentials used to authenticate remote operations.
    pub address: Address,
}

/// Named identifier for a remote site (e.g., "origin", "backup").
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SiteName(String);

impl SiteName {
    /// Returns the site name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for SiteName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

impl PartialEq<str> for SiteName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for SiteName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl From<&SiteName> for SiteName {
    fn from(value: &SiteName) -> Self {
        value.clone()
    }
}

impl From<&str> for SiteName {
    fn from(value: &str) -> Self {
        SiteName(value.to_string())
    }
}

impl From<String> for SiteName {
    fn from(value: String) -> Self {
        SiteName(value)
    }
}
