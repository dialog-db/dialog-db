//! The block stores answering node requests through the translation layer.
//!
//! A store keeps blocks and knows no other place to look, so it answers a
//! [`Read`] as the decode of the block it holds, a [`Write`] as the import
//! of the nodes' blocks, and a [`Preload`] by doing nothing: it holds no
//! cache for the node to land in. A storage routes each request to the
//! space whose subject the catalog names, and a space hands it to its
//! archive. A store that keeps nodes in a form of its own implements these
//! itself, in place of the translation.

use dialog_capability::{Provider, StorageError};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::ArchiveError;
use dialog_storage::provider::{Space, Storage, Volatile};
use rkyv::{
    bytecheck::CheckBytes,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use super::{Preload, Read, Ready, Write};
use crate::{DialogSearchTreeError, PersistentNode};

/// A store answers the node requests through the translation layer, over
/// the blocks it holds.
macro_rules! translates {
    ($store:ty) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<Key, Value> Provider<Read<Key, Value>> for $store
        where
            Key: crate::Key + ConditionalSend + ConditionalSync + 'static,
            Value: crate::Value + ConditionalSync + 'static,
            Value::Archived: for<'a> CheckBytes<
                Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
            >,
        {
            async fn execute(
                &self,
                read: Read<Key, Value>,
            ) -> Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError> {
                read.through_blocks(self, None).await
            }
        }

        /// A store holds no nodes in memory: nothing is answered at once.
        impl<Key, Value> Ready<Key, Value> for $store {
            fn ready(&self, _: &Read<Key, Value>) -> Option<PersistentNode<Key, Value>> {
                None
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<Key, Value> Provider<Preload<Key, Value>> for $store
        where
            Key: crate::Key + ConditionalSend + ConditionalSync + 'static,
            Value: crate::Value + ConditionalSync + 'static,
        {
            async fn execute(&self, _: Preload<Key, Value>) {}
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<Key, Value> Provider<Write<Key, Value>> for $store
        where
            Key: crate::Key + ConditionalSend + ConditionalSync + 'static,
            Value: crate::Value + ConditionalSync + 'static,
        {
            async fn execute(&self, write: Write<Key, Value>) -> Result<(), DialogSearchTreeError> {
                write.through_blocks(self, None).await
            }
        }
    };
}

translates!(Volatile);
translates!(dialog_storage::provider::FileSystem);
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
translates!(dialog_storage::provider::IndexedDb);

/// A space hands a node request to its archive.
macro_rules! space_forwards {
    ($request:ident) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<Key, Value, A, M, C, D, B> Provider<$request<Key, Value>> for Space<A, M, C, D, B>
        where
            Key: crate::Key + ConditionalSend + ConditionalSync + 'static,
            Value: crate::Value + ConditionalSync + 'static,
            A: Provider<$request<Key, Value>> + ConditionalSync,
            M: ConditionalSend + ConditionalSync,
            C: ConditionalSend + ConditionalSync,
            D: ConditionalSend + ConditionalSync,
            B: ConditionalSend + ConditionalSync,
        {
            async fn execute(
                &self,
                request: $request<Key, Value>,
            ) -> <$request<Key, Value> as dialog_capability::Command>::Output {
                self.archive.execute(request).await
            }
        }
    };
}

space_forwards!(Read);
space_forwards!(Preload);
space_forwards!(Write);

/// A space answers at once what its archive does.
impl<Key, Value, A, M, C, D, B> Ready<Key, Value> for Space<A, M, C, D, B>
where
    A: Ready<Key, Value>,
{
    fn ready(&self, read: &Read<Key, Value>) -> Option<PersistentNode<Key, Value>> {
        self.archive.ready(read)
    }
}

/// A storage answers at once what the subject's space does.
impl<Key, Value, S> Ready<Key, Value> for Storage<S>
where
    S: Ready<Key, Value> + Clone,
{
    fn ready(&self, read: &Read<Key, Value>) -> Option<PersistentNode<Key, Value>> {
        self.space(read.catalog.subject())?.ready(read)
    }
}

/// A storage routes a node request to the space the catalog's subject is
/// mounted as. A subject this storage does not serve is not a backend
/// failure, and reports as its own condition.
macro_rules! storage_routes {
    ($request:ident, $missing:ident) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<Key, Value, S> Provider<$request<Key, Value>> for Storage<S>
        where
            Key: crate::Key + ConditionalSend + ConditionalSync + 'static,
            Value: crate::Value + ConditionalSync + 'static,
            S: Provider<$request<Key, Value>> + Clone + ConditionalSend + ConditionalSync,
        {
            async fn execute(
                &self,
                request: $request<Key, Value>,
            ) -> <$request<Key, Value> as dialog_capability::Command>::Output {
                let subject = request.catalog.subject();
                match self.space(subject) {
                    Some(space) => space.execute(request).await,
                    None => $missing(StorageError::SubjectNotFound {
                        subject: subject.clone(),
                    }),
                }
            }
        }
    };
}

/// A subject this storage does not serve, as the read or write's error.
fn unserved<T>(error: StorageError) -> Result<T, DialogSearchTreeError> {
    Err(DialogSearchTreeError::Storage(
        ArchiveError::from(error).into(),
    ))
}

/// A subject this storage does not serve has nothing to preload.
fn nothing(_: StorageError) {}

storage_routes!(Read, unserved);
storage_routes!(Preload, nothing);
storage_routes!(Write, unserved);

/// The temporary file store of the test helpers provides every command its
/// file store does; it holds nothing in memory either.
#[cfg(feature = "helpers")]
impl<Key, Value> Ready<Key, Value> for dialog_storage::TempFileSystem {
    fn ready(&self, _: &Read<Key, Value>) -> Option<PersistentNode<Key, Value>> {
        None
    }
}
