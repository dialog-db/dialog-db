use std::fmt::Display;

use base58::ToBase58;
use dialog_storage::HashType;
use serde::{Deserialize, Serialize};

use crate::KeyType;

/// A serializable reference to a [`Node`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reference<Key, Hash> {
    upper_bound: Key,
    hash: Hash,
}

impl<Key, Hash> Reference<Key, Hash>
where
    Key: KeyType,
    Hash: HashType,
{
    /// Create a new [`Reference`].
    pub fn new(upper_bound: Key, hash: Hash) -> Self {
        Reference { upper_bound, hash }
    }

    /// The hash for this [`Reference`].
    pub fn hash(&self) -> &Hash {
        &self.hash
    }

    /// The upper bounds as a key for this [`Reference`].
    pub fn upper_bound(&self) -> &Key {
        &self.upper_bound
    }
}

impl<Key, Hash> Display for Reference<Key, Hash>
where
    Key: KeyType,
    Hash: HashType,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.hash().as_ref().to_base58())
    }
}
