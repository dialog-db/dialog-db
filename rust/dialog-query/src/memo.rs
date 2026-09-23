//! A value derived once from its owner and kept alongside it.

use std::fmt;
use std::sync::{Arc, OnceLock};

/// A value computed from its owner on first use and kept with it: a
/// content-addressed identity, say, which is costly to hash and never
/// changes once the owner is built.
///
/// Clones share the cell, so a value computed through any copy serves
/// them all -- which is what makes it pay off for owners that are
/// cloned per use, like a rule copied out of a static for every query.
/// That is sound only because the value is a function of the owner's
/// other fields and those do not change: an owner that does change them
/// must take a fresh [`Memo`].
///
/// Carries no information of its own, so every copy compares equal and
/// a derived `PartialEq` on the owner is unaffected by whether it has
/// been computed yet.
pub(crate) struct Memo<T>(Arc<OnceLock<T>>);

impl<T> Memo<T> {
    /// The value, computing it with `init` if this is the first ask.
    pub(crate) fn get_or_init(&self, init: impl FnOnce() -> T) -> &T {
        self.0.get_or_init(init)
    }
}

impl<T> Default for Memo<T> {
    fn default() -> Self {
        Self(Arc::new(OnceLock::new()))
    }
}

impl<T> Clone for Memo<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> PartialEq for Memo<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl<T> fmt::Debug for Memo<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Memo")
    }
}
