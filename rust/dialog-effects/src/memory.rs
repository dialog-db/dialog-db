//! Memory capability hierarchy.
//!
//! Memory provides transactional cell storage with CAS (Compare-And-Swap) semantics.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   └── Memory (ability: /memory)
//!         └── Space { space: String }
//!               └── Cell { cell: String }
//!                     ├── Resolve → Effect → Result<Option<Edition<Vec<u8>>>, MemoryError>
//!                     ├── Publish { content, when } → Effect → Result<Bytes, MemoryError>
//!                     └── Retract { when } → Effect → Result<(), MemoryError>
//! ```

use crate::Verb;
use crate::verb;
use std::fmt;
use std::marker::PhantomData;
use std::str;

use crate::Rejection;
use base58::ToBase58;
use dialog_capability::access::AuthorizeError;
pub use dialog_capability::{
    Attenuate, Attenuation, Capability, Constraint, Effect, Policy, StorageError, Subject,
};
use dialog_common::Checksum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The memory namespace, under a verb: `/use/get/memory/...`.
///
/// Generic over the verb it hangs from, because the same namespace is
/// reached by reading, writing and deleting. The verb sits above it in
/// the chain, so the path reads verb-then-namespace without any link
/// having to spell the combination out.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Memory<V = verb::Get>(PhantomData<V>);

impl<V> Memory<V> {
    /// The memory namespace under `V`.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<V> Default for Memory<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: Verb> Attenuation for Memory<V>
where
    V::Of: Constraint,
{
    type Of = V;

    fn attenuation() -> &'static str {
        "memory"
    }
}

/// Space policy that scopes operations to a memory space.
///
/// Silent in the ability path: the space *name* scopes the capability
/// and travels in the invocation's parameters, but `space` is not a
/// path segment -- `/use/get/memory/cell` names the kind of thing
/// reached, not which one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Space<V = verb::Get> {
    /// The space name (typically a DID).
    pub space: String,
    /// The verb this policy hangs from. A type-level marker: it holds
    /// no data and never reaches the wire.
    #[serde(skip)]
    pub verb: PhantomData<V>,
}

impl<V> Space<V> {
    /// Create a new Space policy.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            space: name.into(),
            verb: PhantomData,
        }
    }
}

impl<V: Verb> Policy for Space<V>
where
    V::Of: Constraint,
{
    type Of = Memory<V>;
}

/// Cell policy that scopes operations to a specific cell within a space.
///
/// Contributes `cell` to the ability path: it is the resource the verb
/// applies to, and so completes the command. The cell *name* scopes the
/// capability and travels in the parameters, as the space name does.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Cell<V = verb::Get> {
    /// The cell name.
    pub cell: String,
    /// The verb this policy hangs from. A type-level marker: it holds
    /// no data and never reaches the wire.
    #[serde(skip)]
    pub verb: PhantomData<V>,
}

impl<V> Cell<V> {
    /// Create a new Cell policy.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            cell: name.into(),
            verb: PhantomData,
        }
    }
}

impl<V: Verb> Attenuation for Cell<V>
where
    V::Of: Constraint,
{
    type Of = Space<V>;

    fn attenuation() -> &'static str {
        "cell"
    }
}

/// Opaque version identifier for CAS operations.
///
/// Backends produce version tokens in whatever form suits them -- S3 hands
/// back ASCII ETags, content-addressed stores hand back raw hashes. This
/// newtype keeps the underlying bytes intact (no lossy UTF-8 conversion)
/// while providing readable [`Debug`] / [`Display`] output.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Version(#[serde(with = "serde_bytes")] Vec<u8>);

impl Version {
    /// View the raw version bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume the wrapper and return the raw bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Whether this version is empty (zero-length). Empty versions are
    /// sometimes used as sentinels for "no prior version".
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for Version {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<&[u8]> for Version {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for Version {
    fn from(bytes: &[u8; N]) -> Self {
        Self(bytes.to_vec())
    }
}

impl From<String> for Version {
    fn from(s: String) -> Self {
        Self(s.into_bytes())
    }
}

impl From<&str> for Version {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

impl From<dialog_common::Blake3Hash> for Version {
    fn from(hash: dialog_common::Blake3Hash) -> Self {
        Self(hash.as_bytes().to_vec())
    }
}

impl From<&dialog_common::Blake3Hash> for Version {
    fn from(hash: &dialog_common::Blake3Hash) -> Self {
        Self(hash.as_bytes().to_vec())
    }
}

impl AsRef<[u8]> for Version {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Render bytes as a UTF-8 string when all bytes are printable ASCII,
/// otherwise fall back to base58.
impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.iter().all(|b| b.is_ascii_graphic() || *b == b' ') {
            // Safety: all bytes are ASCII graphic/space, hence valid UTF-8.
            f.write_str(str::from_utf8(&self.0).expect("ascii is valid utf8"))
        } else {
            f.write_str(&self.0.to_base58())
        }
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Version({})", self)
    }
}

/// A cell's current state: content and its version.
///
/// Returned by [`Resolve`] when the cell has content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edition<T> {
    /// The cell's current content.
    pub content: T,
    /// The version identifier for this content.
    pub version: Version,
}

/// Resolve operation - reads current cell content and version.
///
/// Returns `None` if the cell has no content (empty/uninitialized).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Attenuate)]
pub struct Resolve;

impl Policy for Resolve {
    type Of = Cell<verb::Get>;
}

impl Effect for Resolve {
    type Output = Result<Option<Edition<Vec<u8>>>, MemoryError>;
}

/// Publish operation - sets cell content with CAS semantics.
///
/// - If `when` is `None`, expects cell to be empty (first publish)
/// - If `when` is `Some(edition)`, expects current edition to match
/// - Returns new edition on success
/// - Returns `MemoryError::VersionMismatch` if expectation doesn't match
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Publish {
    /// The content to publish.
    #[serde(with = "serde_bytes")]
    #[attenuate(into = Checksum, with = Checksum::sha256, rename = checksum)]
    pub content: Vec<u8>,
    /// The expected current version, or None if expecting empty cell.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<Version>,
}

impl Publish {
    /// Create a new Publish effect.
    pub fn new(content: impl Into<Vec<u8>>, when: Option<Version>) -> Self {
        Self {
            content: content.into(),
            when,
        }
    }
}

impl Policy for Publish {
    type Of = Cell<verb::Put>;
}

impl Effect for Publish {
    type Output = Result<Version, MemoryError>;
}

/// Retract operation - removes cell content with CAS semantics.
///
/// - Requires `when` to match current edition
/// - Returns `MemoryError::VersionMismatch` if edition doesn't match
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Retract {
    /// The expected current version.
    pub when: Version,
}

impl Retract {
    /// Create a new Retract effect.
    pub fn new(when: impl Into<Version>) -> Self {
        Self { when: when.into() }
    }
}

impl Policy for Retract {
    type Of = Cell<verb::Delete>;
}

impl Effect for Retract {
    type Output = Result<(), MemoryError>;
}

pub mod prelude;

/// Errors that can occur during memory operations.
#[derive(Debug, Error)]
pub enum MemoryError {
    /// CAS edition mismatch.
    #[error("Version mismatch: expected {expected:?}, got {actual:?}")]
    VersionMismatch {
        /// The expected version.
        expected: Option<Version>,
        /// The actual version found.
        actual: Option<Version>,
    },

    /// Storage backend error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// The request was not carried out, for a reason that is not an
    /// access decision.
    #[error(transparent)]
    Rejected(#[from] Rejection),

    /// The request was not authorized.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),
}

impl From<StorageError> for MemoryError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::prelude::CellScope;
    use crate::prelude::*;
    use dialog_capability::{Subject, did};

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn cell() -> CellScope {
        Subject::from(did!("key:zSpace"))
            .memory()
            .space("local")
            .cell("main")
    }

    /// The verb sits above the namespace, so the same cell reads,
    /// writes and deletes through three different roots while the
    /// caller names it the same way each time.
    #[dialog_common::test]
    fn it_builds_the_three_cell_commands() {
        assert_eq!(cell().resolve().ability(), "/use/get/memory/cell");
        assert_eq!(
            cell().publish(b"test".to_vec(), None).ability(),
            "/use/put/memory/cell"
        );
        assert_eq!(cell().retract(b"v1").ability(), "/use/delete/memory/cell");
    }

    /// The space and cell names scope the capability without appearing
    /// in the path: two cells differ in what they authorize, not in how
    /// the command reads.
    #[dialog_common::test]
    fn it_scopes_by_name_without_changing_the_path() {
        let subject = Subject::from(did!("key:zSpace"));
        let main = subject.clone().memory().space("local").cell("main");
        let other = subject.memory().space("local").cell("other");

        assert_eq!(main.resolve().ability(), other.resolve().ability());
    }

    /// The chain keeps the subject it started from.
    #[dialog_common::test]
    fn it_keeps_its_subject() {
        let claim = Subject::from(did!("key:zSpace"))
            .memory()
            .space("local")
            .cell("main")
            .resolve();

        assert_eq!(claim.subject(), &did!("key:zSpace"));
    }
}
