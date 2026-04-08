use crate::{Attenuate, Caveat, Constraint};
use dialog_common::ConditionalSend;

/// Trait for effect types that can be performed.
///
/// Effects are capabilities that can be invoked and therefor require their
/// output type. Implementing `Effect` automatically makes the type an
/// [`Attenuation`] (and thus a [`Policy`]) via blanket impls.
///
/// Effects must also implement [`Attenuate`] to support authorization. The
/// `Attenuate` trait defines how the effect is represented during authorization —
/// payload fields (like content bytes) become checksums.
pub trait Effect: Sized + Caveat + Attenuate {
    /// The capability this effect requires (the parent in the chain).
    type Of: Constraint;
    /// The output type produced by the invocation of this effect when performed.
    type Output: ConditionalSend;
}
