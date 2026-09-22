//! The identity vocabulary: how a thing is named, independent of how it
//! is stored.
//!
//! A [`Uri`] is a normalized name; an [`Entity`] is a name paired with
//! the padded byte form key encodings carry. Both sit here rather than
//! with the artifact machinery because the wire vocabulary needs to
//! name things without pulling in a search tree or a storage backend --
//! the same reason [`history`](crate::history) holds `Origin`,
//! `Version` and `Edition`.

mod entity;
mod error;
mod uri;

pub use entity::*;
pub use error::*;
pub use uri::*;

/// Length of the padded entity byte representation carried by
/// [`Entity`].
///
/// Keys no longer pad entities (they are lossless and
/// variable-length); this width only sizes the legacy
/// `[u8; ENTITY_LENGTH]` companion buffer.
pub const ENTITY_LENGTH: usize = 64;
