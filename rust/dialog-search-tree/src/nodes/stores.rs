//! The block stores answering node requests through the translation layer.
//!
//! A store keeps blocks and knows no other place to look, so it answers a
//! [`Read`] as the decode of the block it holds, a [`Write`] as the import
//! of the nodes' blocks, and a [`Preload`] by doing nothing: it holds no
//! cache for the node to land in. A storage routes each request to the
//! space whose subject the catalog names, and a space hands it to its
//! archive. A store that keeps nodes in a form of its own implements these
//! itself, in place of the translation.

use dialog_capability::{Provider, Resume, StorageError};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::ArchiveError;
use dialog_storage::provider::{Space, Storage, Volatile};

use super::{Node, Preload, Read, Write};
use crate::DialogSearchTreeError;

/// A store answers the node requests through the translation layer, over
/// the blocks it holds.
macro_rules! translates {
    ($store:ty) => {
        impl<N: Node> Provider<Read<N>> for $store {
            fn execute(
                &self,
                read: Read<N>,
            ) -> Resume<'_, Result<Option<N>, DialogSearchTreeError>> {
                read.through_blocks(self, None)
            }
        }

        /// A store holds no cache for a node to land in: nothing to do.
        impl<N> Provider<Preload<N>> for $store {
            fn execute(&self, _: Preload<N>) -> Resume<'_, ()> {
                Resume::now(())
            }
        }

        impl<N: Node> Provider<Write<N>> for $store {
            fn execute(&self, write: Write<N>) -> Resume<'_, Result<(), DialogSearchTreeError>> {
                write.through_blocks(self, None)
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
        impl<N, A, M, C, D, B> Provider<$request<N>> for Space<A, M, C, D, B>
        where
            A: Provider<$request<N>>,
        {
            fn execute(
                &self,
                request: $request<N>,
            ) -> Resume<'_, <$request<N> as dialog_capability::Command>::Output> {
                self.archive.execute(request)
            }
        }
    };
}

space_forwards!(Read);
space_forwards!(Preload);
space_forwards!(Write);

/// A storage routes a node request to the space the catalog's subject is
/// mounted as. A subject this storage does not serve is not a backend
/// failure, and reports as its own condition.
///
/// The space is handed out owned, so the answer waits in a future that
/// owns it, even when the space would have answered at once.
macro_rules! storage_routes {
    ($request:ident, $missing:ident) => {
        impl<N, S> Provider<$request<N>> for Storage<S>
        where
            N: Node,
            S: Provider<$request<N>> + Clone + ConditionalSend + ConditionalSync + 'static,
        {
            fn execute(
                &self,
                request: $request<N>,
            ) -> Resume<'_, <$request<N> as dialog_capability::Command>::Output> {
                let subject = request.catalog.subject();
                match self.space(subject) {
                    Some(space) => Resume::later(async move { space.execute(request).await }),
                    None => Resume::now($missing(StorageError::SubjectNotFound {
                        subject: subject.clone(),
                    })),
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
