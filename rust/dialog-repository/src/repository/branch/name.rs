use dialog_prolly_tree::KeyType;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    string::FromUtf8Error,
};

/// Unique name for the branch
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BranchName(String);

impl BranchName {
    /// Creates a new branch name from a string.
    pub fn new(name: String) -> Self {
        BranchName(name)
    }

    /// Returns a reference to the branch name string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl KeyType for BranchName {
    fn bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl TryFrom<Vec<u8>> for BranchName {
    type Error = FromUtf8Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(BranchName(String::from_utf8(bytes)?))
    }
}

impl Display for BranchName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

impl From<&BranchName> for BranchName {
    fn from(value: &BranchName) -> Self {
        value.clone()
    }
}

impl From<&str> for BranchName {
    fn from(value: &str) -> Self {
        BranchName(value.to_string())
    }
}

impl From<String> for BranchName {
    fn from(value: String) -> Self {
        BranchName(value)
    }
}
