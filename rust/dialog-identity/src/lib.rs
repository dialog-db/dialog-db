//! Profile identity: the long-lived signing credential and its access API.
//!
//! A [`Profile`] is the durable identity a person holds: it opens from a
//! named credential, claims and delegates capabilities, and derives the
//! session material operators are built from. This crate holds only the
//! identity primitives; storage routing lives in `dialog-storage` and the
//! operating environment composed from a profile lives in
//! `dialog-operator`, above the repository layer.

mod authority;
mod profile;

pub use authority::*;
pub use profile::*;
