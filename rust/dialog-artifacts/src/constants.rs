#[cfg(doc)]
use crate::Artifact;
#[cfg(doc)]
use dialog_prolly_tree::Tree;

/// The number of bytes in a hash used by the [`Tree`]s that constitute [`Artifact`]
/// indexes
pub const HASH_SIZE: usize = 32;

/// The branch factor of the [`Tree`]s that constitute [`Artifact`] indexes
pub const BRANCH_FACTOR: u32 = 64;
