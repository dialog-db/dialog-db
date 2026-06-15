//! Batched tree edits in the style of Clojure transients.
//!
//! A [`Transient`] is a short-lived, mutable view over an otherwise immutable
//! tree. It behaves like a real tree whose nodes are one of two kinds (named
//! after the persistent/transient data structure distinction, not Rust's
//! borrow/own one; see [`NodeEdit`]):
//!
//! - **persistent**: a sealed node of the durable tree, named by hash and never
//!   copied. Whole untouched subtrees stay in this form, shared with the
//!   original, and cost nothing when the batch is sealed.
//! - **transient**: a node the batch has edited. The first edit to a node loads
//!   and copies it into transient form; every later edit in the same batch
//!   mutates it in place. Its hash is not computed until
//!   [`Transient::persist`].
//!
//! Editing therefore copies one node per touched node per batch (not per
//! operation), and serialization plus hashing happen once, at the end, only for
//! the nodes that changed. Reads of untouched children are lazy: a transient
//! node keeps its children as persistent references until the batch descends
//! into them, mirroring the sparse navigation in [`crate::TreeDifference`].
//!
//! Canonicalization (grouping children into nodes at rank boundaries) is reused
//! from [`crate::TreeShaper`] at persist time, so a batch produces the exact
//! same canonical tree as applying the same operations one at a time.

mod node;
pub use node::*;

mod edit;
pub use edit::*;
