use crate::attenuation::type_segment;
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

    /// Whether this effect names itself in the ability path.
    ///
    /// An effect is a link in the capability chain like any other, and
    /// like any other it chooses whether to contribute a segment. The
    /// default is the effect's type name, so `Lookup` under a `Get`
    /// reads `/use/get/lookup`.
    ///
    /// An effect whose parents already name the whole command sets this
    /// to `false` and stays silent: where the chain is
    /// `Use -> Get -> Memory -> Cell`, the path `/use/get/memory/cell`
    /// is complete before the effect is reached, and a `resolve` segment
    /// on the end would say twice what `get` already said.
    ///
    /// This is a choice rather than a default because the alternative --
    /// every effect forced to emit, and effects that wanted a different
    /// path hand-writing it as a string -- let the path drift from the
    /// chain that produced it.
    const NAMED: bool = true;

    /// The segment this effect contributes when [`NAMED`](Self::NAMED).
    fn segment() -> &'static str {
        type_segment::<Self>()
    }
}
