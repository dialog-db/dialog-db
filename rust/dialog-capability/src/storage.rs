//! Storage capability hierarchy.
//!
//! Storage provides key-value store operations.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//! └── Storage (ability: /storage)
//!     └── Store { store: String }
//!         ├── Get { key } → Effect → Result<Option<Bytes>, StorageError>
//!         ├── Set { key, value } → Effect → Result<(), StorageError>
//!         ├── Delete { key } → Effect → Result<(), StorageError>
//!         └── List { continuation_token } → Effect → Result<ListResult, StorageError>
//! ```

pub use crate::{Attenuation, Capability, Caveat, Claim, Did, Effect, Policy, Subject, did};
use dialog_common::Checksum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Root attenuation for storage operations.
///
/// Attaches to Subject and provides the `/storage` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Storage;

impl Storage {
    /// Build a location capability from an address.
    pub fn locate<A: Caveat>(address: A) -> Capability<Location<A>> {
        Subject::from(did!("local:storage"))
            .attenuate(Storage)
            .attenuate(Location(address))
    }

    /// Build a mount capability for the given DID at the given address.
    pub fn mount<A>(did: Did, address: A) -> Capability<Mount<A>>
    where
        A: Caveat + dialog_common::ConditionalSend + 'static,
    {
        Subject::from(did)
            .attenuate(Storage)
            .attenuate(Location(address))
            .mount()
    }

    /// Build a load capability for the given address.
    pub fn load<Content, A>(address: A) -> Capability<Load<Content, A>>
    where
        Content: dialog_common::ConditionalSend + 'static,
        A: Caveat + dialog_common::ConditionalSend + 'static,
    {
        Self::locate(address).load()
    }

    /// Build a save capability for the given address and content.
    pub fn save<Content, A>(address: A, content: Content) -> Capability<Save<Content, A>>
    where
        Content: Serialize + serde::de::DeserializeOwned + dialog_common::ConditionalSend + 'static,
        A: Caveat + dialog_common::ConditionalSend + 'static,
    {
        Self::locate(address).save(content)
    }
}

impl Attenuation for Storage {
    type Of = Subject;
}

/// Store policy that scopes operations to a named store.
///
/// This is a policy (not attenuation) so it doesn't contribute to the ability path.
/// It restricts operations to a specific store (e.g., "index", "blob").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Store {
    /// The store name (e.g., "index", "blob").
    pub store: String,
}

impl Store {
    /// Create a new Store policy.
    pub fn new(name: impl Into<String>) -> Self {
        Self { store: name.into() }
    }
}

impl Policy for Store {
    type Of = Storage;
}

/// Get operation - retrieves a value by key.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Get {
    /// The key to look up.
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
}

impl Get {
    /// Create a new Get effect.
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self { key: key.into() }
    }
}

impl Effect for Get {
    type Of = Store;
    type Output = Result<Option<Vec<u8>>, StorageError>;
}

impl Capability<Get> {
    /// Get the store name from the capability chain.
    pub fn store(&self) -> &str {
        &Store::of(self).store
    }

    /// Get the key from the capability chain.
    pub fn key(&self) -> &[u8] {
        &Get::of(self).key
    }
}

/// Set operation - sets a value for a key.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Set {
    /// The key to update.
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    /// The value to set.
    #[serde(with = "serde_bytes")]
    #[claim(into = Checksum, with = Checksum::sha256, rename = checksum)]
    pub value: Vec<u8>,
}

impl Set {
    /// Create a new Set effect.
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl Effect for Set {
    type Of = Store;
    type Output = Result<(), StorageError>;
}

impl Attenuation for SetClaim {
    type Of = Store;
    fn attenuation() -> &'static str {
        "set"
    }
}

impl Capability<Set> {
    /// Get the store name from the capability chain.
    pub fn store(&self) -> &str {
        &Store::of(self).store
    }

    /// Get the key from the capability chain.
    pub fn key(&self) -> &[u8] {
        &Set::of(self).key
    }

    /// Get the value from the capability chain.
    pub fn value(&self) -> &[u8] {
        &Set::of(self).value
    }
}

/// Delete operation - removes a key.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Delete {
    /// The key to delete.
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
}

impl Delete {
    /// Create a new Delete effect.
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        Self { key: key.into() }
    }
}

impl Effect for Delete {
    type Of = Store;
    type Output = Result<(), StorageError>;
}

impl Capability<Delete> {
    /// Get the store name from the capability chain.
    pub fn store(&self) -> &str {
        &Store::of(self).store
    }

    /// Get the key from the capability chain.
    pub fn key(&self) -> &[u8] {
        &Delete::of(self).key
    }
}

/// List operation - lists keys in a store by prefix.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct List {
    /// Key prefix to filter by. Empty string means list all.
    #[serde(with = "serde_bytes")]
    pub prefix: Vec<u8>,
    /// Continuation token for pagination.
    pub continuation_token: Option<String>,
}

impl List {
    /// Create a new List effect with optional continuation token.
    pub fn new(continuation_token: Option<String>) -> Self {
        Self {
            prefix: Vec::new(),
            continuation_token,
        }
    }

    /// Create a List effect filtered by key prefix.
    pub fn with_prefix(prefix: impl Into<Vec<u8>>) -> Self {
        Self {
            prefix: prefix.into(),
            continuation_token: None,
        }
    }
}

impl Effect for List {
    type Of = Store;
    type Output = Result<ListResult, StorageError>;
}

/// Result of a list operation.
#[derive(Debug, Clone)]
pub struct ListResult {
    /// Object keys returned in this response.
    pub keys: Vec<String>,
    /// If true, there are more results to fetch.
    pub is_truncated: bool,
    /// Token to use for fetching the next page of results.
    pub next_continuation_token: Option<String>,
}

impl Capability<List> {
    /// Get the store name from the capability chain.
    pub fn store(&self) -> &str {
        &Store::of(self).store
    }

    /// Get the key prefix to filter by.
    pub fn prefix(&self) -> &[u8] {
        &List::of(self).prefix
    }

    /// Get the continuation token from the capability chain.
    pub fn continuation_token(&self) -> Option<&str> {
        List::of(self).continuation_token.as_deref()
    }
}

/// Errors that can occur during storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Storage backend error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Location policy — wraps a provider-specific address.
///
/// Generic over the address type `A`. Each storage provider defines
/// its own address type (e.g. `fs::Address`, `volatile::Address`).
/// The [`Address`](crate) enum in `dialog-storage` tags them for dispatch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct Location<A>(pub A);

impl<A> Location<A> {
    /// The inner address.
    pub fn address(&self) -> &A {
        &self.0
    }

    /// Transform the address type.
    pub fn map<B>(self, f: impl FnOnce(A) -> B) -> Location<B> {
        Location(f(self.0))
    }
}

impl<A: Caveat> Policy for Location<A> {
    type Of = Storage;
}

impl<A: Caveat> Capability<Location<A>> {
    /// Create a load effect capability for this location.
    pub fn load<Content>(self) -> Capability<Load<Content, A>>
    where
        Content: dialog_common::ConditionalSend + 'static,
        A: dialog_common::ConditionalSend + 'static,
    {
        self.invoke(Load::default())
    }

    /// Create a save effect capability for this location.
    pub fn save<Content>(self, content: Content) -> Capability<Save<Content, A>>
    where
        Content: Serialize + serde::de::DeserializeOwned + dialog_common::ConditionalSend + 'static,
        A: dialog_common::ConditionalSend + 'static,
    {
        self.invoke(Save::new(content))
    }

    /// Create a mount effect capability for this location.
    ///
    /// The subject DID from the capability chain will be registered
    /// to route to this location's address.
    pub fn mount(self) -> Capability<Mount<A>>
    where
        A: dialog_common::ConditionalSend + 'static,
    {
        self.invoke(Mount::default())
    }
}

/// Mount effect — registers a subject DID to be routed to this location.
///
/// The subject DID in the capability chain is the identity to mount.
/// The `Location<A>` carries the address where its data lives.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Mount<A>(std::marker::PhantomData<A>);

impl<A> Mount<A> {
    /// Create a new Mount effect.
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<A> Default for Mount<A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A> Effect for Mount<A>
where
    A: Caveat + dialog_common::ConditionalSend + 'static,
{
    type Of = Location<A>;
    type Output = Result<(), StorageError>;
}

/// Load effect — reads typed content from a location.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Load<Content, A>(std::marker::PhantomData<(Content, A)>);

impl<Content, A> Load<Content, A> {
    /// Create a new Load effect.
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<Content, A> Default for Load<Content, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Content, A> Effect for Load<Content, A>
where
    Content: dialog_common::ConditionalSend + 'static,
    A: Caveat + dialog_common::ConditionalSend + 'static,
{
    type Of = Location<A>;
    type Output = Result<Content, StorageError>;
}

/// Save effect — writes typed content to a location.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
#[serde(bound(
    serialize = "Content: Serialize",
    deserialize = "Content: for<'a> Deserialize<'a>"
))]
pub struct Save<Content: Serialize, A> {
    /// The content to save.
    pub content: Content,
    #[serde(skip)]
    _address: std::marker::PhantomData<A>,
}

impl<Content: Serialize, A> Save<Content, A> {
    /// Create a new Save effect.
    pub fn new(content: Content) -> Self {
        Self {
            content,
            _address: std::marker::PhantomData,
        }
    }
}

impl<Content, A> Effect for Save<Content, A>
where
    Content: Serialize + serde::de::DeserializeOwned + dialog_common::ConditionalSend + 'static,
    A: Caveat + dialog_common::ConditionalSend + 'static,
{
    type Of = Location<A>;
    type Output = Result<(), StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::did;

    #[test]
    fn it_builds_storage_claim_path() {
        let claim = Subject::from(did!("key:zSpace")).attenuate(Storage);

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/storage");
    }

    #[test]
    fn it_builds_store_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Storage)
            .attenuate(Store::new("index"));

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        // Store is Policy, not Ability, so it doesn't add to path
        assert_eq!(claim.ability(), "/storage");
    }

    #[test]
    fn it_builds_get_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Get::new(vec![1, 2, 3]));

        assert_eq!(claim.ability(), "/storage/get");
    }

    #[test]
    fn it_builds_set_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Storage)
            .attenuate(Store::new("index"))
            .invoke(Set::new(vec![1, 2, 3], vec![4, 5, 6]));

        assert_eq!(claim.ability(), "/storage/set");

        // Use policy() method to extract nested constraints
        assert_eq!(claim.policy::<Store, _>().store, "index");
        assert_eq!(&claim.policy::<Set, _>().key[..], &[1, 2, 3]);
    }
}
