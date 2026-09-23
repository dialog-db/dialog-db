//! Identity primitives: the long-lived signing credential a peer holds,
//! how it is opened, and the access API for claiming and delegating with
//! it.
//!
//! This crate holds only the primitives. Storage routing lives in
//! `dialog-storage`, and the operating environment composed over such a
//! credential, the peer and its sessions, lives in `dialog-peer`, above
//! the repository layer.

pub mod access;
mod authority;
mod error;
mod open;
mod secret;
mod space;

pub use access::{Claim, ClaimExt, SaveDelegation};
pub use authority::*;
pub use error::IdentityError;
pub use open::OpenCredential;
pub use secret::*;
pub use space::SpaceHandle;
