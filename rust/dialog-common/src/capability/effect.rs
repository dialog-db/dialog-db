//! Effect trait for types that can be performed.

use super::{Constraint, Settings};
use crate::ConditionalSend;

/// Trait for effect types that can be performed.
///
/// Effects are capabilities that can be invoked and therefor require their
/// output type. Implementing `Effect` automatically makes the type an
/// [`Attenuation`] (and thus a [`Policy`]) via blanket impls.
pub trait Effect: Sized + Settings {
    /// The capability this effect requires (the parent in the chain).
    type Of: Constraint;
    /// The output type produced by the invoaction of this effect when performed.
    type Output: ConditionalSend;
}
