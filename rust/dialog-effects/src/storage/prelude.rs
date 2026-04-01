//! Extension traits for fluent storage capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::storage::prelude::*;
//! ```

use dialog_capability::{Capability, Did, Subject};

use super::{Delete, Get, List, Set, Storage, Store};

/// Extension trait to start a storage capability chain.
pub trait SubjectExt {
    /// The resulting storage chain type.
    type Storage;
    /// Begin a storage capability chain.
    fn storage(self) -> Self::Storage;
}

impl SubjectExt for Subject {
    type Storage = Capability<Storage>;
    fn storage(self) -> Capability<Storage> {
        self.attenuate(Storage)
    }
}

impl SubjectExt for Did {
    type Storage = Capability<Storage>;
    fn storage(self) -> Capability<Storage> {
        Subject::from(self).attenuate(Storage)
    }
}

/// Extension methods for scoping storage to a named store.
pub trait StorageExt {
    /// The resulting store chain type.
    type Store;
    /// Scope to a named store.
    fn store(self, name: impl Into<String>) -> Self::Store;
}

impl StorageExt for Capability<Storage> {
    type Store = Capability<Store>;
    fn store(self, name: impl Into<String>) -> Capability<Store> {
        self.attenuate(Store::new(name))
    }
}

/// Extension methods for invoking effects on a store.
pub trait StoreExt {
    /// The resulting get chain type.
    type Get;
    /// The resulting set chain type.
    type Set;
    /// The resulting delete chain type.
    type Delete;
    /// The resulting list chain type.
    type List;
    /// Get a value by key.
    fn get(self, key: impl Into<Vec<u8>>) -> Self::Get;
    /// Set a value for a key.
    fn set(self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self::Set;
    /// Delete a key.
    fn delete(self, key: impl Into<Vec<u8>>) -> Self::Delete;
    /// List keys in the store.
    fn list(self, continuation_token: Option<String>) -> Self::List;
}

impl StoreExt for Capability<Store> {
    type Get = Capability<Get>;
    type Set = Capability<Set>;
    type Delete = Capability<Delete>;
    type List = Capability<List>;

    fn get(self, key: impl Into<Vec<u8>>) -> Capability<Get> {
        self.invoke(Get::new(key))
    }

    fn set(self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Capability<Set> {
        self.invoke(Set::new(key, value))
    }

    fn delete(self, key: impl Into<Vec<u8>>) -> Capability<Delete> {
        self.invoke(Delete::new(key))
    }

    fn list(self, continuation_token: Option<String>) -> Capability<List> {
        self.invoke(List::new(continuation_token))
    }
}
