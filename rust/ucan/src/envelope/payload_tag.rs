//! Trait for types that can provide their envelope type tag.

/// The type tag is used as the key in the envelope payload map,
/// e.g., `"ucan/dlg@1.0.0-rc.1"` for delegations.
pub trait PayloadTag {
    /// The specification name for the envelope type.
    fn spec_id() -> &'static str;

    /// Returns the version string for the envelope type.
    fn version() -> &'static str;

    /// Constructs the full tag string.
    #[must_use]
    fn tag() -> String {
        format!("ucan/{}@{}", Self::spec_id(), Self::version())
    }
}
