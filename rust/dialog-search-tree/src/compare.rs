use std::cmp::Ordering;

use rkyv::Archive;

/// A trait for comparing archived and owned values symmetrically.
pub trait SymmetryWith<T: ?Sized>: PartialOrd<T>
where
    T: Archive<Archived = Self> + Ord,
{
    /// Compares this archived value with an owned value.
    fn cmp(&self, other: &T) -> Ordering {
        // SAFETY: partial_cmp returns None only for incomparable values (e.g.,
        // `0f32.cmp(f32::NAN)`). `<T as Archive>::Archived` is a code-generated
        // derivative of `T` and should be byte-for-byte symmetrical to `T`
        // (already known to be `Ord`).
        self.partial_cmp(other).unwrap()
    }
}
