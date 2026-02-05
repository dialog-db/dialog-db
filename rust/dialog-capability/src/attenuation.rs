use crate::settings::Caveat;
use crate::{Constraint, Effect, Policy};

/// Trait for constraints that narrow the ability path.
///
/// Attenuation implies [`Policy`] via blanket impl. The `attenuation()` method
/// provides the path segment added to the ability path.
///
/// # Ability Path Segment
///
/// By default, the ability path segment is derived from the struct name
/// (lowercased). For example, `struct Storage;` adds `/storage` to the path.
///
/// You can override this by implementing the `attenuation()` method:
///
/// ```rust
/// use dialog_capability::{Attenuation, Subject};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct BlobStore;
///
/// impl Attenuation for BlobStore {
///     type Of = Subject;
///
///     // Custom path segment instead of default "blobstore"
///     fn attenuation() -> &'static str {
///         "blob"
///     }
/// }
/// ```
///
/// Note: [`Effect`] types automatically implement `Attenuation` via blanket impl.
pub trait Attenuation: Sized + Caveat {
    /// The capability this type constrains.
    /// Must implement [`Constraint`] so the blanket [`Policy`] impl works.
    type Of: Constraint;

    /// Returns the path segment this attenuation adds to the ability path.
    ///
    /// By default, derives the segment from the struct name (lowercased).
    /// Override this method to use a custom segment.
    fn attenuation() -> &'static str {
        let full = std::any::type_name::<Self>();
        full.rsplit("::").next().unwrap_or(full)
    }
}

// Attenuation implies Policy (with attenuation override)
impl<T: Attenuation> Policy for T {
    type Of = <T as Attenuation>::Of;

    fn attenuation() -> Option<&'static str> {
        Some(<T as Attenuation>::attenuation())
    }
}

// Effect implies Attenuation
impl<T: Effect> Attenuation for T {
    type Of = <T as Effect>::Of;
}
