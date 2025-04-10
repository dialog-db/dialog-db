#[cfg(doc)]
use crate::Fact;
#[cfg(doc)]
use x_prolly_tree::Tree;

/// The number of bytes in a hash used by the [`Tree`]s that constitute [`Fact`]
/// indexes
pub const HASH_SIZE: usize = 32;

/// The branch factor of the [`Tree`]s that constitute [`Fact`] indexes
pub const BRANCH_FACTOR: u32 = 64;
