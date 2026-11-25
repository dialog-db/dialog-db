//! Stored claims this replica cannot read yet.
//!
//! A replica takes claims from writers it does not control, such as
//! another implementation or a newer release it syncs with, and a pull
//! checks no claim's value. So it can hold a claim it cannot read yet:
//! its value does not decode as its type, or its bytes are ones this
//! release would not write (text that is not UTF-8, an entity that is
//! not a URI or not in its canonical rendering, a malformed attribute).
//! Such a claim is not complete yet as far as this replica can tell, and
//! its author, or whoever sees it reported, can complete or correct it.
//! Until then it takes no part: every read treats it as if it did not
//! exist, no write fails on it, and a cardinality-one write to its cell
//! succeeds it with the claim the write replaces. [`pass_over`] is where
//! each of them goes, one at a time as it is met, so what is passed over
//! can be reported as it happens.

/// A stored claim this replica cannot read yet, passed over as if it did not
/// exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unread {
    key: Option<Vec<u8>>,
    reason: String,
}

impl Unread {
    /// The claim's stored key, when the claim was read from the tree.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Why this replica cannot read the claim yet: what does not decode,
    /// or what this release would not write.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// Passes over the stored claim at `key`, which cannot be read for
/// `reason`: the one place every read and write that passes over such a
/// claim goes through. It logs the claim and hands it back, for a caller
/// that reports what it passed over.
pub fn pass_over(key: Option<&[u8]>, reason: String) -> Unread {
    tracing::warn!(%reason, "passing over a stored claim this replica cannot read yet");
    Unread {
        key: key.map(<[u8]>::to_vec),
        reason,
    }
}
