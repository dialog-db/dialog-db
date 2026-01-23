//! Settings trait for capability parameter serialization.

#[cfg(feature = "ucan")]
use ipld_core::{ipld::Ipld, serde::to_ipld};
use serde::Serialize;
#[cfg(feature = "ucan")]
use std::collections::BTreeMap;

/// Parameters for UCAN capability invocations, mapping string keys to IPLD values.
#[cfg(feature = "ucan")]
pub type Parameters = BTreeMap<String, Ipld>;

/// Trait for types that can contribute parameters to capability invocations.
pub trait Settings {
    /// Serialize this type's fields into the given parameters map.
    #[cfg(feature = "ucan")]
    fn parametrize(&self, settings: &mut Parameters);

    /// Collect all parameters from this type into a new map.
    #[cfg(feature = "ucan")]
    fn parameters(&self) -> Parameters {
        let mut parameters = Parameters::new();
        self.parametrize(&mut parameters);
        parameters
    }
}

impl<P: Serialize> Settings for P {
    #[cfg(feature = "ucan")]
    fn parametrize(&self, settings: &mut Parameters) {
        if let Ok(Ipld::Map(constraint_map)) = to_ipld(self) {
            settings.extend(constraint_map)
        }
    }
}
