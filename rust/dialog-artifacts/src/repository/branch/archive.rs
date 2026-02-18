use dialog_capability::{Capability, Subject};
use dialog_effects::archive as fx;

/// Pre-attenuated archive capability (`Subject → Archive`).
///
/// Wraps `Capability<fx::Archive>` so callers can further attenuate with a
/// `Catalog` and then invoke `Get`/`Put`.
#[derive(Debug, Clone)]
pub struct Archive(Capability<fx::Archive>);

impl Archive {
    /// Create a new archive capability for the given subject.
    pub fn new(subject: Subject) -> Self {
        Self(subject.attenuate(fx::Archive))
    }

    /// Create a catalog-scoped capability for content-addressed storage.
    pub fn catalog(&self, name: &str) -> Capability<fx::Catalog> {
        self.0.clone().attenuate(fx::Catalog::new(name))
    }

    /// The index catalog used for search tree node storage.
    pub fn index(&self) -> Capability<fx::Catalog> {
        self.catalog("index")
    }
}
