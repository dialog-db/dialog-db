use std::fmt::Display;

use base58::ToBase58;
use x_storage::HashType;

use crate::KeyType;

/// A serializable reference to a [`Node`].
#[derive(Debug, Clone, PartialEq)]
pub struct Reference<const HASH_SIZE: usize, Key, Hash>
where
    Key: KeyType,
    Hash: HashType<HASH_SIZE>,
{
    upper_bound: Key,
    hash: Hash,
}

impl<const HASH_SIZE: usize, Key, Hash> Reference<HASH_SIZE, Key, Hash>
where
    Key: KeyType,
    Hash: HashType<HASH_SIZE>,
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

impl<const HASH_SIZE: usize, Key, Hash> Display for Reference<HASH_SIZE, Key, Hash>
where
    Key: KeyType,
    Hash: HashType<HASH_SIZE>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.hash().bytes().to_base58())
    }
}
