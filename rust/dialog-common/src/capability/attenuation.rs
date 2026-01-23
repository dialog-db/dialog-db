//! Attenuation trait for command path segments.

use super::constraint::Constraint;
use super::effect::Effect;
use super::policy::Policy;
use super::settings::Settings;

/// Marker trait for policies that also constrain a command path.
///
/// Attenuation implies `Policy` via blanket impl. The `attenuation()` method
/// provides the path segment for the command path.
///
/// Note: `Effect` types automatically implement `Attenuation` via blanket impl.
pub trait Attenuation: Sized + Settings {
    /// The capability this type constrains.
    /// Must implement `Constraint` so the blanket `Policy` impl works.
    type Of: Constraint;

    /// Get the attenuation segment for this type.
    /// Attenuation types contribute to the command path.
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
