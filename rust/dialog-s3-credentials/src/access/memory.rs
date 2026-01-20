//! Memory access commands.
//!
//! Dos for authorizing transactional memory (CAS) operations.
//! Each effect returns a `Result<RequestDescriptor, AuthorizationError>` that can
//! be used to make the actual HTTP request.

use super::{AuthorizationError, Claim, Precondition, RequestDescriptor};
use crate::Checksum;
use dialog_common::Effect;
use serde::Deserialize;

#[cfg(feature = "ucan")]
use super::Args;
#[cfg(feature = "ucan")]
use dialog_common::Provider;

/// Edition identifier for CAS operations.
pub type Edition = String;

/// Memory command enum for UCAN parsing.
#[cfg(feature = "ucan")]
#[derive(Debug)]
pub enum Do {
    Resolve(Resolve),
    Update(Update),
    Delete(Delete),
}

#[cfg(feature = "ucan")]
impl Effect for Do {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

#[cfg(feature = "ucan")]
impl<'a> TryFrom<(&'a [&'a str], Args<'a>)> for Do {
    type Error = AuthorizationError;

    fn try_from((segments, args): (&'a [&'a str], Args<'a>)) -> Result<Self, Self::Error> {
        match segments {
            ["resolve"] => Ok(Do::Resolve(args.deserialize()?)),
            ["update"] => Ok(Do::Update(args.deserialize()?)),
            ["delete"] => Ok(Do::Delete(args.deserialize()?)),
            _ => Err(AuthorizationError::Invocation(format!(
                "Unknown memory command: {:?}",
                segments
            ))),
        }
    }
}

/// Trait for providers that can execute all memory commands.
#[cfg(feature = "ucan")]
pub trait MemoryProvider: Provider<Resolve> + Provider<Update> + Provider<Delete> {}

#[cfg(feature = "ucan")]
impl<T> MemoryProvider for T where T: Provider<Resolve> + Provider<Update> + Provider<Delete> {}

#[cfg(feature = "ucan")]
impl Do {
    /// Perform this command using the given provider.
    pub async fn perform<P: MemoryProvider>(
        self,
        provider: &P,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Do::Resolve(cmd) => cmd.perform(provider).await,
            Do::Update(cmd) => cmd.perform(provider).await,
            Do::Delete(cmd) => cmd.perform(provider).await,
        }
    }
}

/// Resolve current cell content and edition.
#[derive(Debug, Deserialize)]
pub struct Resolve {
    /// Memory space.
    pub space: String,
    /// Cell name.
    pub cell: String,
}

impl Resolve {
    /// Create a new Resolve command.
    pub fn new(space: impl Into<String>, cell: impl Into<String>) -> Self {
        Self {
            space: space.into(),
            cell: cell.into(),
        }
    }
}

impl Claim for Resolve {
    fn method(&self) -> &'static str {
        "GET"
    }
    fn path(&self) -> String {
        format!("{}/{}", self.space, self.cell)
    }
}

impl Effect for Resolve {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

/// Update cell content with CAS semantics.
///
/// - `when: Some(edition)` → only update if current edition matches
/// - `when: None` → only update if cell doesn't exist (create)
#[derive(Debug, Deserialize)]
pub struct Update {
    /// Memory space.
    pub space: String,
    /// Cell name.
    pub cell: String,
    /// Expected current edition for CAS. None means cell must not exist.
    pub when: Option<Edition>,
    /// Checksum for integrity verification (32 bytes SHA-256).
    pub checksum: Checksum,
}

impl Update {
    /// Create a new Update command.
    pub fn new(
        space: impl Into<String>,
        cell: impl Into<String>,
        when: Option<Edition>,
        checksum: Checksum,
    ) -> Self {
        Self {
            space: space.into(),
            cell: cell.into(),
            when,
            checksum,
        }
    }
}

impl Effect for Update {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}

impl Claim for Update {
    fn method(&self) -> &'static str {
        "PUT"
    }
    fn path(&self) -> String {
        format!("{}/{}", self.space, self.cell)
    }
    fn checksum(&self) -> Option<&Checksum> {
        Some(&self.checksum)
    }
    fn precondition(&self) -> Precondition {
        match &self.when {
            Some(edition) => Precondition::IfMatch(edition.clone()),
            None => Precondition::IfNoneMatch,
        }
    }
}

/// Delete cell with CAS semantics.
///
/// Delete only succeeds if current edition matches `when`.
/// If `when` doesn't match, the delete is a no-op.
#[derive(Debug, Deserialize)]
pub struct Delete {
    /// Memory space.
    pub space: String,
    /// Cell name.
    pub cell: String,
    /// Required current edition. Delete is no-op if edition doesn't match.
    pub when: Edition,
}

impl Delete {
    /// Create a new Delete command.
    pub fn new(space: impl Into<String>, cell: impl Into<String>, when: Edition) -> Self {
        Self {
            space: space.into(),
            cell: cell.into(),
            when,
        }
    }
}

impl Claim for Delete {
    fn method(&self) -> &'static str {
        "DELETE"
    }
    fn path(&self) -> String {
        format!("{}/{}", self.space, self.cell)
    }
    fn precondition(&self) -> Precondition {
        Precondition::IfMatch(self.when.clone())
    }
}

impl Effect for Delete {
    type Output = Result<RequestDescriptor, AuthorizationError>;
}
