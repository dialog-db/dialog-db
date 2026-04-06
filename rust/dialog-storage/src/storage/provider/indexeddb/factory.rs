//! Factory implementation for IndexedDB storage.

use super::IndexedDb;
use crate::provider::location::Location;
use crate::provider::space::Factory;

/// Factory that creates IndexedDb providers from a Location.
#[derive(Debug, Clone, Default)]
pub struct IndexedDbFactory;

impl Factory for IndexedDbFactory {
    type Provider = IndexedDb;

    fn create(&self, location: &Location) -> IndexedDb {
        let address = super::Address::from(location);
        IndexedDb::mount(&address)
    }
}
