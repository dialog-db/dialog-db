use std::fmt::{Display, Formatter, Result as FmtResult};

use serde::{Deserialize, Serialize};

/// Named identifier for a remote site (e.g., "origin", "backup").
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RemoteName(String);

impl RemoteName {
    /// Returns the site name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for RemoteName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

impl PartialEq<str> for RemoteName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for RemoteName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl From<&RemoteName> for RemoteName {
    fn from(value: &RemoteName) -> Self {
        value.clone()
    }
}

impl From<&str> for RemoteName {
    fn from(value: &str) -> Self {
        RemoteName(value.to_string())
    }
}

impl From<String> for RemoteName {
    fn from(value: String) -> Self {
        RemoteName(value)
    }
}

impl From<&RemoteName> for String {
    fn from(value: &RemoteName) -> Self {
        value.0.clone()
    }
}
