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

pub use crate::{Attenuation, Capability, Claim, Effect, Policy, Subject, did};
use dialog_common::Checksum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Root attenuation for storage operations.
///
/// Attaches to Subject and provides the `/storage` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Storage;

impl Storage {
    /// Storage location capability for the platform profile directory.
    pub fn profile() -> Capability<Location> {
        Subject::from(did!("local:storage"))
            .attenuate(Storage)
            .attenuate(Location::profile())
    }

    /// Storage location capability for a unique temporary directory.
    ///
    /// Each call creates a new unique sub-path under `temp://`.
    pub fn temp() -> Capability<Location> {
        use dialog_common::time;
        let id = format!(
            "dialog-{}",
            time::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let location = Location::temp()
            .resolve(&id)
            .expect("timestamp is a valid path segment");
        Subject::from(did!("local:storage"))
            .attenuate(Storage)
            .attenuate(location)
    }

    /// Storage location capability for the current/project directory.
    pub fn storage() -> Capability<Location> {
        Subject::from(did!("local:storage"))
            .attenuate(Storage)
            .attenuate(Location::storage())
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

/// Extension trait for `Capability<Get>` to access its fields.
pub trait GetCapability {
    /// Get the store name from the capability chain.
    fn store(&self) -> &str;
    /// Get the key from the capability chain.
    fn key(&self) -> &[u8];
}

impl GetCapability for Capability<Get> {
    fn store(&self) -> &str {
        &Store::of(self).store
    }

    fn key(&self) -> &[u8] {
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

/// Extension trait for `Capability<Set>` to access its fields.
pub trait SetCapability {
    /// Get the store name from the capability chain.
    fn store(&self) -> &str;
    /// Get the key from the capability chain.
    fn key(&self) -> &[u8];
    /// Get the value from the capability chain.
    fn value(&self) -> &[u8];
}

impl SetCapability for Capability<Set> {
    fn store(&self) -> &str {
        &Store::of(self).store
    }

    fn key(&self) -> &[u8] {
        &Set::of(self).key
    }

    fn value(&self) -> &[u8] {
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

/// Extension trait for `Capability<Delete>` to access its fields.
pub trait DeleteCapability {
    /// Get the store name from the capability chain.
    fn store(&self) -> &str;
    /// Get the key from the capability chain.
    fn key(&self) -> &[u8];
}

impl DeleteCapability for Capability<Delete> {
    fn store(&self) -> &str {
        &Store::of(self).store
    }

    fn key(&self) -> &[u8] {
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

/// Extension trait for `Capability<List>` to access its fields.
pub trait ListCapability {
    /// Get the store name from the capability chain.
    fn store(&self) -> &str;
    /// Get the key prefix to filter by.
    fn prefix(&self) -> &[u8];
    /// Get the continuation token from the capability chain.
    fn continuation_token(&self) -> Option<&str>;
}

impl ListCapability for Capability<List> {
    fn store(&self) -> &str {
        &Store::of(self).store
    }

    fn prefix(&self) -> &[u8] {
        &List::of(self).prefix
    }

    fn continuation_token(&self) -> Option<&str> {
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

/// Location policy — a URI scoping storage operations.
///
/// The URI scheme determines the storage root:
/// - `profile://` — platform profile directory
/// - `temp://` — temporary directory
/// - `storage://` — project/working directory
///
/// Locations can only be narrowed via `.resolve()`, never broadened.
/// Obtain one from `Storage::profile()`, `Storage::temp()`, etc.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct Location(url::Url);

impl Location {
    /// Profile storage root.
    pub fn profile() -> Self {
        Self(url::Url::parse("profile:///").expect("valid URL"))
    }

    /// Temporary storage root.
    pub fn temp() -> Self {
        Self(url::Url::parse("temp:///").expect("valid URL"))
    }

    /// Project/working storage root.
    pub fn storage() -> Self {
        Self(url::Url::parse("storage:///").expect("valid URL"))
    }

    /// The URI scheme (e.g. `"profile"`, `"temp"`, `"storage"`).
    pub fn scheme(&self) -> &str {
        self.0.scheme()
    }

    /// The path portion of the URI.
    pub fn path(&self) -> &str {
        self.0.path()
    }

    /// The underlying URL.
    pub fn url(&self) -> &url::Url {
        &self.0
    }

    /// Resolve a sub-path under this location.
    ///
    /// Uses URI resolution to ensure the result is always nested
    /// under this location. Returns an error if the segment would
    /// escape the base.
    pub fn resolve(&self, segment: &str) -> Result<Self, StorageError> {
        let mut base = self.0.clone();
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }

        let resolved = base
            .join(&format!("./{segment}"))
            .map_err(|e| StorageError::Storage(format!("URL join failed: {e}")))?;

        if !resolved.path().starts_with(base.path()) {
            return Err(StorageError::Storage(format!(
                "path '{segment}' escapes base '{}'",
                base.path()
            )));
        }

        Ok(Self(resolved))
    }
}

impl Policy for Location {
    type Of = Storage;
}

impl Capability<Location> {
    /// Resolve a sub-path under this location, returning a new capability.
    pub fn resolve(&self, segment: &str) -> Result<Self, StorageError> {
        let location = Location::of(self);
        let resolved = location.resolve(segment)?;
        let subject = self.subject().clone();
        Ok(Subject::from(subject)
            .attenuate(Storage)
            .attenuate(resolved))
    }

    /// Create a load effect capability for this location.
    pub fn load<Content: dialog_common::ConditionalSend + 'static>(
        self,
    ) -> Capability<Load<Content>> {
        self.invoke(Load::default())
    }

    /// Create a save effect capability for this location.
    pub fn save<Content>(self, content: Content) -> Capability<Save<Content>>
    where
        Content: Serialize + serde::de::DeserializeOwned + dialog_common::ConditionalSend + 'static,
    {
        self.invoke(Save::new(content))
    }

    /// Create a mount effect capability for this location.
    pub fn mount<Resource: dialog_common::ConditionalSend + 'static>(
        self,
    ) -> Capability<Mount<Resource>> {
        self.invoke(Mount::default())
    }
}

/// A storage provider that knows what type it mounts.
///
/// Implemented by platform storage providers (e.g. `FileSystem` → `FileStore`).
pub trait Mountable {
    /// The local store type produced by mounting a location.
    type Store: dialog_common::ConditionalSend + 'static;
}

/// Mount effect — opens a storage resource scoped to this location.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Mount<Resource>(std::marker::PhantomData<Resource>);

impl<Resource> Mount<Resource> {
    /// Create a new Mount effect.
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<Resource> Default for Mount<Resource> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Resource: dialog_common::ConditionalSend + 'static> Effect for Mount<Resource> {
    type Of = Location;
    type Output = Result<Resource, StorageError>;
}

/// Load effect — reads typed content from this location.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
pub struct Load<Content>(std::marker::PhantomData<Content>);

impl<Content> Load<Content> {
    /// Create a new Load effect.
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<Content> Default for Load<Content> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Content: dialog_common::ConditionalSend + 'static> Effect for Load<Content> {
    type Of = Location;
    type Output = Result<Content, StorageError>;
}

/// Save effect — writes typed content to this location.
#[derive(Debug, Clone, Serialize, Deserialize, Claim)]
#[serde(bound(
    serialize = "Content: Serialize",
    deserialize = "Content: for<'a> Deserialize<'a>"
))]
pub struct Save<Content: Serialize> {
    /// The content to save.
    pub content: Content,
}

impl<Content: Serialize> Save<Content> {
    /// Create a new Save effect.
    pub fn new(content: Content) -> Self {
        Self { content }
    }
}

impl<Content: Serialize + serde::de::DeserializeOwned + dialog_common::ConditionalSend + 'static>
    Effect for Save<Content>
{
    type Of = Location;
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

    mod location_tests {
        use super::*;

        #[test]
        fn resolve_appends_segment() {
            let loc = Location::profile();
            let resolved = loc.resolve("personal").unwrap();
            assert!(resolved.path().contains("personal"));
        }

        #[test]
        fn resolve_chains() {
            let loc = Location::profile()
                .resolve("personal")
                .unwrap()
                .resolve("credentials")
                .unwrap()
                .resolve("self")
                .unwrap();
            assert!(loc.path().contains("personal/credentials/self"));
        }

        #[test]
        fn resolve_rejects_parent_traversal() {
            let loc = Location::profile().resolve("personal").unwrap();
            let result = loc.resolve("../other");
            assert!(result.is_err(), ".. should be rejected");
        }

        #[test]
        fn resolve_rejects_prefix_attack() {
            let loc = Location::profile().resolve("foo").unwrap();
            // "fooled/you" resolved from foo/ should be foo/fooled/you, not /fooled/you
            let resolved = loc.resolve("fooled/you").unwrap();
            assert!(
                resolved.path().contains("foo/fooled"),
                "should be nested under foo: {}",
                resolved.path()
            );
        }

        #[test]
        fn different_schemes() {
            assert_eq!(Location::profile().scheme(), "profile");
            assert_eq!(Location::temp().scheme(), "temp");
            assert_eq!(Location::storage().scheme(), "storage");
        }
    }

    #[cfg(feature = "ucan")]
    mod parameters_tests {
        use super::*;
        use crate::ucan::parameters;
        use ipld_core::ipld::Ipld;

        #[test]
        fn it_collects_storage_parameters() {
            let cap = Subject::from(did!("key:zSpace")).attenuate(Storage);
            let params = parameters(&cap);

            // Storage is a unit struct, should produce empty map
            assert!(params.is_empty());
        }

        #[test]
        fn it_collects_store_parameters() {
            let cap = Subject::from(did!("key:zSpace"))
                .attenuate(Storage)
                .attenuate(Store::new("index"));
            let params = parameters(&cap);

            assert_eq!(params.get("store"), Some(&Ipld::String("index".into())));
        }

        #[test]
        fn it_collects_get_parameters() {
            let cap = Subject::from(did!("key:zSpace"))
                .attenuate(Storage)
                .attenuate(Store::new("index"))
                .invoke(Get::new(vec![1, 2, 3]));
            let params = parameters(&cap);

            assert_eq!(params.get("store"), Some(&Ipld::String("index".into())));
            assert_eq!(params.get("key"), Some(&Ipld::Bytes(vec![1, 2, 3])));
        }

        #[test]
        fn it_collects_set_parameters() {
            let cap = Subject::from(did!("key:zSpace"))
                .attenuate(Storage)
                .attenuate(Store::new("mystore"))
                .invoke(Set::new(vec![10, 20], vec![30, 40, 50]));
            let params = parameters(&cap);

            assert_eq!(params.get("store"), Some(&Ipld::String("mystore".into())));
            assert_eq!(params.get("key"), Some(&Ipld::Bytes(vec![10, 20])));
            assert_eq!(params.get("value"), Some(&Ipld::Bytes(vec![30, 40, 50])));
        }

        #[test]
        fn it_collects_delete_parameters() {
            let cap = Subject::from(did!("key:zSpace"))
                .attenuate(Storage)
                .attenuate(Store::new("trash"))
                .invoke(Delete::new(vec![99]));
            let params = parameters(&cap);

            assert_eq!(params.get("store"), Some(&Ipld::String("trash".into())));
            assert_eq!(params.get("key"), Some(&Ipld::Bytes(vec![99])));
        }
    }
}
