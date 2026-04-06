//! Factory implementation for FileSystem storage.

use super::FileStore;
use crate::provider::location::Location;
use crate::provider::space::Factory;

/// Factory that creates FileStore providers from a Location.
#[derive(Debug, Clone, Default)]
pub struct FileSystemFactory;

impl Factory for FileSystemFactory {
    type Provider = FileStore;

    fn create(&self, location: &Location) -> FileStore {
        let address = super::Address::from(location);
        super::FileSystem::mount(&address).expect("valid filesystem location")
    }
}
