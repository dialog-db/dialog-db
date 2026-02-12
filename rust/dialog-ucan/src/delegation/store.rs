//! Delegation stores.

use std::{
    borrow::Borrow,
    cell::RefCell,
    collections::HashMap,
    convert::Infallible,
    error::Error,
    hash::BuildHasher,
    rc::Rc,
    sync::{Arc, Mutex},
};

use dialog_varsig::signature::Signature;
use futures::{
    FutureExt,
    future::{BoxFuture, LocalBoxFuture},
};
use ipld_core::cid::Cid;
use thiserror::Error;

use crate::future::{FutureKind, Local, Sendable};

use super::Delegation;

/// Delegation store.
pub trait DelegationStore<K: FutureKind, S: Signature, T: Borrow<Delegation<S>>> {
    /// Error type for insertion operations.
    type InsertError: Error;

    /// Error type for retrieval operations.
    type GetError: Error;

    /// Retrieves a delegation by its CID.
    fn get_all<'a>(&'a self, cid: &'a [Cid]) -> K::Future<'a, Result<Vec<T>, Self::GetError>>;

    /// Inserts a delegation by its CID.
    fn insert_by_cid(
        &self,
        cid: Cid,
        delegation: T,
    ) -> K::Future<'_, Result<(), Self::InsertError>>;
}

/// Inserts a delegation and returns its CID.
///
/// # Errors
///
/// If insertion fails, an error defined by the `impl DelegationStore` is returned
/// (the `S::InsertError` associated type).
pub async fn insert<
    K: FutureKind,
    S: Signature,
    T: Borrow<Delegation<S>>,
    St: DelegationStore<K, S, T>,
>(
    store: &St,
    delegation: T,
) -> Result<Cid, St::InsertError> {
    let cid = delegation.borrow().to_cid();
    store.insert_by_cid(cid, delegation).await?;
    Ok(cid)
}

impl<S: Signature, H: BuildHasher> DelegationStore<Local, S, Rc<Delegation<S>>>
    for Rc<RefCell<HashMap<Cid, Rc<Delegation<S>>, H>>>
{
    type InsertError = Infallible;
    type GetError = Missing;

    fn insert_by_cid(
        &self,
        cid: Cid,
        delegation: Rc<Delegation<S>>,
    ) -> LocalBoxFuture<'_, Result<(), Self::InsertError>> {
        async move {
            self.borrow_mut().insert(cid, delegation);
            Ok(())
        }
        .boxed_local()
    }

    fn get_all<'a>(
        &'a self,
        cid: &'a [Cid],
    ) -> LocalBoxFuture<'a, Result<Vec<Rc<Delegation<S>>>, Self::GetError>> {
        async move {
            let store = RefCell::borrow(self);
            let mut dlgs = Vec::new();
            for c in cid {
                if let Some(dlg) = store.get(c) {
                    dlgs.push(dlg.clone());
                } else {
                    Err(Missing(*c))?;
                }
            }
            Ok(dlgs)
        }
        .boxed_local()
    }
}

impl<S: Signature, H: BuildHasher> DelegationStore<Local, S, Arc<Delegation<S>>>
    for Arc<Mutex<HashMap<Cid, Arc<Delegation<S>>, H>>>
{
    type InsertError = StorePoisoned;
    type GetError = LockedStoreGetError;

    fn insert_by_cid(
        &self,
        cid: Cid,
        delegation: Arc<Delegation<S>>,
    ) -> LocalBoxFuture<'_, Result<(), Self::InsertError>> {
        async move {
            let mut locked = self.lock().map_err(|_| StorePoisoned)?;
            locked.insert(cid, delegation);
            Ok(())
        }
        .boxed_local()
    }

    fn get_all<'a>(
        &'a self,
        cid: &'a [Cid],
    ) -> LocalBoxFuture<'a, Result<Vec<Arc<Delegation<S>>>, Self::GetError>> {
        async move {
            let locked = self.lock().map_err(|_| StorePoisoned)?;
            let mut dlgs = Vec::new();
            for c in cid {
                if let Some(dlg) = locked.get(c) {
                    dlgs.push(dlg.clone());
                } else {
                    return Err(Missing(*c))?;
                }
            }
            Ok(dlgs)
        }
        .boxed_local()
    }
}

impl<S: Signature + Send + Sync, H: BuildHasher + Send>
    DelegationStore<Sendable, S, Arc<Delegation<S>>>
    for Arc<Mutex<HashMap<Cid, Arc<Delegation<S>>, H>>>
where
    S::Algorithm: Send + Sync,
{
    type InsertError = StorePoisoned;
    type GetError = LockedStoreGetError;

    fn insert_by_cid(
        &self,
        cid: Cid,
        delegation: Arc<Delegation<S>>,
    ) -> BoxFuture<'_, Result<(), Self::InsertError>> {
        async move {
            let mut locked = self.lock().map_err(|_| StorePoisoned)?;
            locked.insert(cid, delegation);
            Ok(())
        }
        .boxed()
    }

    fn get_all<'a>(
        &'a self,
        cid: &'a [Cid],
    ) -> BoxFuture<'a, Result<Vec<Arc<Delegation<S>>>, Self::GetError>> {
        async move {
            let locked = self.lock().map_err(|_| StorePoisoned)?;
            let mut dlgs = Vec::new();
            for c in cid {
                if let Some(dlg) = locked.get(c) {
                    dlgs.push(dlg.clone());
                } else {
                    return Err(Missing(*c))?;
                }
            }
            Ok(dlgs)
        }
        .boxed()
    }
}

/// Error for when the delegation store's [`Mutex`] is poisoned.
#[derive(Debug, Clone, Copy, Error)]
#[error("delegation store poisoned")]
pub struct StorePoisoned;

/// Error for when a delegation is missing from the store.
#[derive(Debug, Clone, Copy, Error)]
#[error("delegation with cid {0} is missing")]
pub struct Missing(pub Cid);

/// Error for when the delegation store's [`Mutex`] is poisoned.
#[derive(Debug, Clone, Copy, Error)]
pub enum LockedStoreGetError {
    /// Delegation is missing
    #[error(transparent)]
    Missing(#[from] Missing),

    /// Mutex was poisoned
    #[error(transparent)]
    StorePoisoned(#[from] StorePoisoned),
}
