//! The name of the primary, the layer a row with no layer named lies in.
//!
//! Where a row lies is a dimension of the row, `in`. A write naming no
//! layer lands in the primary, and a row read with no layer stamped on
//! it ([`FactView::layer`](crate::FactView::layer)) is the primary's: a
//! read over several layers stamps the rows of the others and leaves the
//! primary's bare, so the common case moves nothing per row.

use std::sync::LazyLock;

use crate::Entity;

/// The name of the primary: the base durable branch of a session.
pub const MEMORY: &str = "memory:";

// Parsed once: a read binding each row's layer asks for it per row.
static MEMORY_LAYER: LazyLock<Entity> =
    LazyLock::new(|| MEMORY.parse().expect("a bare scheme is an entity"));

/// The entity of the primary, [`MEMORY`].
pub fn memory_layer() -> Entity {
    MEMORY_LAYER.clone()
}
