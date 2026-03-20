//! Extension traits for fluent storage capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::storage::prelude::*;
//! ```

use dialog_capability::{Capability, Did, Subject, site::Site};

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

impl<S: Site> SubjectExt for Capability<Subject, S> {
    type Storage = Capability<Storage, S>;
    fn storage(self) -> Capability<Storage, S> {
        self.attenuate(Storage)
    }
}

/// Extension methods for scoping storage to a named store.
pub trait StorageExt {
    /// The resulting store chain type.
    type Store;
    /// Scope to a named store.
    fn store(self, name: impl Into<String>) -> Self::Store;
}

impl<S: Site> StorageExt for Capability<Storage, S> {
    type Store = Capability<Store, S>;
    fn store(self, name: impl Into<String>) -> Capability<Store, S> {
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

impl<S: Site> StoreExt for Capability<Store, S> {
    type Get = Capability<Get, S>;
    type Set = Capability<Set, S>;
    type Delete = Capability<Delete, S>;
    type List = Capability<List, S>;

    fn get(self, key: impl Into<Vec<u8>>) -> Capability<Get, S> {
        self.invoke(Get::new(key))
    }

    fn set(self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Capability<Set, S> {
        self.invoke(Set::new(key, value))
    }

    fn delete(self, key: impl Into<Vec<u8>>) -> Capability<Delete, S> {
        self.invoke(Delete::new(key))
    }

    fn list(self, continuation_token: Option<String>) -> Capability<List, S> {
        self.invoke(List::new(continuation_token))
    }
}
