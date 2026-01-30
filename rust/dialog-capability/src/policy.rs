use crate::settings::Caveat;
use crate::{Ability, Constrained, Constraint, Selector};

/// Trait for policy types that restrict capabilities.
///
/// `Policy` is for types that represent restrictions on what can be done
/// with a capability. Implement this for types that don't contribute to
/// the command path.
///
/// For types that contribute to the command path, implement [`Attenuation`]
/// instead (which provides `Policy` via blanket impl).
pub trait Policy: Sized + Caveat {
    /// The capability this policy restricts.
    /// Must implement `Constraint` so we can compute the full chain type.
    type Of: Constraint;

    /// Get the attenuation segment for this type, if it contributes to the
    /// command path. Default returns None (policies don't attenuate the
    /// command path by default). Attenuation types override this to return
    /// Some(name).
    fn attenuation() -> Option<&'static str> {
        None
    }

    /// Extract this type from a capability chain. Type parameters allow
    /// compiler to infer where in the constrain chain desired policy type
    /// is.
    fn of<Head, Tail, Index>(capability: &Constrained<Head, Tail>) -> &Self
    where
        Head: Policy,
        Tail: Ability,
        Constrained<Head, Tail>: Selector<Self, Index>,
    {
        capability.select()
    }
}
