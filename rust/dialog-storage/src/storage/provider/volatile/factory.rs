//! Factory implementation for Volatile storage.

use super::Volatile;
use crate::provider::location::Location;
use crate::provider::space::Factory;

/// Factory that creates Volatile providers from a Location.
#[derive(Debug, Clone, Default)]
pub struct VolatileFactory;

impl Factory for VolatileFactory {
    type Provider = Volatile;

    fn create(&self, location: &Location) -> Volatile {
        let address = super::Address::from(location);
        Volatile::mount(&address)
    }
}
