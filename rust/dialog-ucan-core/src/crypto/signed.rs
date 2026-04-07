//! Signed payload wrapper.

/// Signed payload wrapper.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Signed<V, T, S> {
    varsig_header: V,
    payload: T,
    signature: S,
}
