use std::fmt::{Display, Formatter, Result as FmtResult};

use serde::{Deserialize, Serialize};

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
