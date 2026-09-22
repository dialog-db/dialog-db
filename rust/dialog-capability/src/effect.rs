use crate::{Attenuate, Caveat, Policy};
use dialog_common::ConditionalSend;

/// Trait for effect types that can be performed.
///
/// Effects are capabilities that can be invoked and therefor require
/// their output type.
///
/// # Naming
///
/// An effect is a link in the chain like any other, so it says which
/// kind of link it is: implement [`Attenuation`](crate::Attenuation) to
/// contribute a segment to the ability path, or [`Policy`](crate::Policy)
/// to stay out of it.
///
/// Most effects are `Policy`. Where the chain is
/// `Use -> Get -> Memory -> Cell`, the path `/use/get/memory/cell` is
/// complete before the effect is reached, and a `resolve` segment on the
/// end would say twice what `get` already said.
///
/// Effects must also implement [`Attenuate`] to support authorization. The
/// `Attenuate` trait defines how the effect is represented during authorization —
/// payload fields (like content bytes) become checksums.
pub trait Effect: Sized + Caveat + Attenuate + Policy {
    /// The output type produced by the invocation of this effect when performed.
    type Output: ConditionalSend;
}
