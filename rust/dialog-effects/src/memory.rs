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
//!               ├── List → Effect → Result<Vec<String>, MemoryError>
//!               └── Cell { cell: String }
//!                     ├── Resolve → Effect → Result<Option<Edition<Vec<u8>>>, MemoryError>
//!                     ├── Watch → Effect → Result<Editions, MemoryError>
//!                     ├── Publish { content, when } → Effect → Result<Bytes, MemoryError>
//!                     └── Retract { when } → Effect → Result<(), MemoryError>
//! ```

use crate::Method;
use crate::method;
use std::fmt;
use std::marker::PhantomData;
use std::str;

use crate::Rejection;
use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::access::AuthorizeError;
pub use dialog_capability::{
    Attenuate, Attenuation, Capability, Constraint, Effect, Policy, StorageError, Subject,
};
use dialog_common::{Checksum, ConditionalSend};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The memory namespace, under a verb: `/use/get/memory/...`.
///
/// Generic over the verb it hangs from, because the same namespace is
/// reached by reading, writing and deleting. The verb sits above it in
/// the chain, so the path reads verb-then-namespace without any link
/// having to spell the combination out.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Memory<V = method::Get>(PhantomData<V>);

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

impl<M: Method> Attenuation for Memory<M>
where
    M::Of: Constraint,
{
    type Of = M;

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
pub struct Space<V = method::Get> {
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

impl<M: Method> Policy for Space<M>
where
    M::Of: Constraint,
{
    type Of = Memory<M>;
}

/// Cell policy that scopes operations to a specific cell within a space.
///
/// Contributes `cell` to the ability path: it is the resource the verb
/// applies to, and so completes the command. The cell *name* scopes the
/// capability and travels in the parameters, as the space name does.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Cell<V = method::Get> {
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

impl<M: Method> Attenuation for Cell<M>
where
    M::Of: Constraint,
{
    type Of = Space<M>;

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
    type Of = Cell<method::Get>;
}

impl Effect for Resolve {
    type Output = Result<Option<Edition<Vec<u8>>>, MemoryError>;
}

/// Watch operation - follows a cell: answers what it holds now, then
/// what it holds each time that changes, for as long as the answer is
/// read.
///
/// Reading a cell once and reading it as it changes disclose the same
/// thing, so a watch is a read: its command is
/// `/use/get/memory/cell/watch`, which any grant to read the cell covers.
/// It is a command of its own so that whoever serves it can tell it from
/// a [`Resolve`].
///
/// A site that cannot follow a cell answers
/// [`Rejection::Unsupported`], and its cells are read by resolving them
/// again instead.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Attenuate)]
pub struct Watch;

impl Attenuation for Watch {
    type Of = Cell<method::Get>;

    fn attenuation() -> &'static str {
        "watch"
    }
}

impl Effect for Watch {
    type Output = Result<Editions, MemoryError>;
}

/// What a watched cell holds: its edition, or nothing when it is empty.
pub type CellState = Option<Edition<Vec<u8>>>;

/// The states a watched cell takes, in order, as a [`Watch`] answers
/// them. The first is what the cell holds when the watch begins.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait EditionSource: ConditionalSend {
    /// The cell's next state, or `None` once the watch has ended and
    /// nothing more will be answered.
    ///
    /// A state may be skipped when the cell changes faster than it is
    /// read, but the last state the cell takes before the watch ends is
    /// never skipped: a reader that keeps reading learns where the cell
    /// stands.
    async fn next(&mut self) -> Result<Option<CellState>, MemoryError>;
}

/// The answer to a [`Watch`]. `Box<dyn EditionSource>` so that every site
/// answers one type.
pub type Editions = Box<dyn EditionSource>;

/// List operation - names every cell stored under a space.
///
/// Answers each cell's path relative to the space, including cells in
/// the spaces nested below it: listing `remote` finds
/// `origin/address` as well as `origin/branch/main/revision`. Space and
/// cell names may both contain `/`, and a store keeps only the joined
/// path, so the split between them is not recoverable: the paths are
/// what a caller resolves against the listed space.
///
/// Names only, never content: a cell still has to be resolved to be
/// read. Its command names the space, `/use/get/memory/space`, since
/// the space is what is read.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Attenuate)]
pub struct List;

impl Attenuation for List {
    type Of = Space<method::Get>;

    fn attenuation() -> &'static str {
        "space"
    }
}

impl Effect for List {
    type Output = Result<Vec<String>, MemoryError>;
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
    type Of = Cell<method::Put>;
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
    type Of = Cell<method::Delete>;
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
    use crate::prelude::*;
    use dialog_capability::{Subject, did};

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn subject() -> Subject {
        Subject::from(did!("key:zSpace"))
    }

    /// A watch is a read of the cell, under a command of its own: any
    /// grant to read the cell covers it, and whoever serves it tells it
    /// from a resolve.
    #[dialog_common::test]
    fn it_names_a_watch_below_the_read_of_a_cell() {
        let watch = subject()
            .reader()
            .memory()
            .space("local")
            .cell("main")
            .watch();
        assert_eq!(watch.ability(), "/use/get/memory/cell/watch");
        assert!(watch.ability().starts_with("/use/get/memory/cell/"));
    }

    /// The method sits above the namespace, so the same cell reads,
    /// writes and deletes through three different chains -- each named
    /// in the order its path reads.
    #[dialog_common::test]
    fn it_builds_the_three_cell_commands() {
        assert_eq!(
            subject()
                .reader()
                .memory()
                .space("local")
                .cell("main")
                .resolve()
                .ability(),
            "/use/get/memory/cell"
        );
        assert_eq!(
            subject()
                .writer()
                .memory()
                .space("local")
                .cell("main")
                .publish(b"test".to_vec(), None)
                .ability(),
            "/use/put/memory/cell"
        );
        assert_eq!(
            subject()
                .user()
                .delete()
                .memory()
                .space("local")
                .cell("main")
                .retract(b"v1")
                .ability(),
            "/use/delete/memory/cell"
        );
    }

    /// Listing reads a space rather than a cell, so its command names
    /// the space, and a grant of cell reads does not cover it.
    #[dialog_common::test]
    fn it_builds_the_list_command() {
        let list = subject().reader().memory().space("remote").list();
        assert_eq!(list.ability(), "/use/get/memory/space");
        assert_eq!(list.space(), "remote");
    }

    /// The space and cell names scope the capability without appearing
    /// in the path: two cells differ in what they authorize, not in how
    /// the command reads.
    #[dialog_common::test]
    fn it_scopes_by_name_without_changing_the_path() {
        let main = subject().reader().memory().space("local").cell("main");
        let other = subject().reader().memory().space("local").cell("other");

        assert_eq!(main.resolve().ability(), other.resolve().ability());
    }

    /// The chain keeps the subject it started from.
    #[dialog_common::test]
    fn it_keeps_its_subject() {
        let claim = subject()
            .reader()
            .memory()
            .space("local")
            .cell("main")
            .resolve();

        assert_eq!(claim.subject(), &did!("key:zSpace"));
    }
}
