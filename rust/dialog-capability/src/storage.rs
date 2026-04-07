//! Storage capability hierarchy.
//!
//! Storage provides location-based storage operations using typed addresses.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//! └── Storage (ability: /storage)
//!     └── Location<A> { address: A }
//!         ├── Mount → Effect → Result<(), StorageError>
//!         ├── Load<Content> → Effect → Result<Content, StorageError>
//!         └── Save<Content> → Effect → Result<(), StorageError>
//! ```

pub use crate::{Attenuation, Capability, Caveat, Claim, Did, Effect, Policy, Subject, did};
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

    /// A test address type for location capabilities.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestAddress(String);

    #[test]
    fn it_builds_storage_claim_path() {
        let claim = Subject::from(did!("key:zSpace")).attenuate(Storage);

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/storage");
    }

    #[test]
    fn it_builds_location_claim_path() {
        let claim = Storage::locate(TestAddress("test".into()));

        // Location is a Policy, not an Ability, so path stays /storage
        assert_eq!(claim.ability(), "/storage");
        assert_eq!(
            Location::<TestAddress>::of(&claim).address(),
            &TestAddress("test".into())
        );
    }

    #[test]
    fn it_builds_load_claim_path() {
        let claim = Storage::load::<Vec<u8>, _>(TestAddress("data".into()));

        assert_eq!(claim.ability(), "/storage/load");
    }

    #[test]
    fn it_builds_save_claim_path() {
        let claim = Storage::save(TestAddress("data".into()), vec![1u8, 2, 3]);

        assert_eq!(claim.ability(), "/storage/save");
    }
}
