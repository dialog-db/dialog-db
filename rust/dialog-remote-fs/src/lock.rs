//! Cross-process write lock for memory-cell CAS.
//!
//! - **Native**: `pidlock` against `{cell}.lock` — same primitive as
//!   `dialog-storage::fs::memory`, so any native consumer of that
//!   provider and a native FS-remote consumer of this crate serialize
//!   their writes correctly against each other.
//! - **WASM**: the Web Locks API
//!   (`navigator.locks.request(name, callback)`) — coordinates across
//!   browser tabs, dedicated workers, and service workers. The lock name
//!   is derived from `{vault_id}:{cell_path}` so two vaults don't
//!   contend.
//!
//! The two schemes do *not* coordinate with each other: a native process
//! holding the `pidlock` does not block a browser tab whose Web Lock
//! lives in a different lock domain (and vice versa). Consumers that
//! enable both a native and a browser writer against the same directory
//! must enforce mutual exclusion at a layer above this crate.
//!
//! The guard is RAII: drop releases the lock. The acquire path is `async`
//! on both platforms so the providers have a single call site for both.

use crate::FsError;

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use super::*;
    use crate::handle::Handle;

    pub(crate) struct LockGuard {
        inner: pidlock::Pidlock,
    }

    impl LockGuard {
        pub(crate) async fn acquire(target: &Handle) -> Result<Self, FsError> {
            let path = target.path().with_extension("lock");

            // pidlock 0.2's acquire() handles stale-lock cleanup internally
            // (atomic create_new + dead-PID check) and never panics on I/O
            // errors. new_validated also creates the parent directory and
            // rejects malformed paths up front.
            let mut lock = pidlock::Pidlock::new_validated(&path)
                .map_err(|e| FsError::Lock(format!("Invalid lock path: {e:?}")))?;
            match lock.acquire() {
                Ok(()) => Ok(Self { inner: lock }),
                Err(pidlock::PidlockError::LockExists) => {
                    // Holder is alive. Look up its PID for diagnostics; an
                    // error reading the file just means we can't include it.
                    let holder = lock
                        .get_owner()
                        .ok()
                        .flatten()
                        .map(|pid| pid.to_string())
                        .unwrap_or_else(|| "<unknown>".into());
                    Err(FsError::Lock(format!(
                        "Concurrent write in progress (lock held by pid {holder})",
                    )))
                }
                Err(e) => Err(FsError::Lock(format!("Failed to acquire lock: {e:?}"))),
            }
        }
    }

    impl Drop for LockGuard {
        fn drop(&mut self) {
            // 0.2's release returns Result instead of panicking; pidlock's
            // own Drop also cleans up on a best-effort basis.
            let _ = self.inner.release();
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod web {
    use super::*;
    use crate::handle::Handle;
    use wasm_bindgen::JsCast;
    use wasm_bindgen::JsValue;
    use wasm_bindgen::closure::Closure;
    use wasm_bindgen_futures::JsFuture;

    /// Held while a CAS critical section is in progress. The Web Lock is
    /// released when the inner callback's Promise resolves — which happens
    /// when `release_tx` is sent (in [`Drop`]).
    pub(crate) struct LockGuard {
        release_tx: Option<tokio::sync::oneshot::Sender<()>>,
        /// The outer Promise returned by `navigator.locks.request(...)`.
        /// Drained on drop via [`wasm_bindgen_futures::spawn_local`] so any
        /// lock-manager rejection doesn't surface as an unhandled rejection.
        outer_promise: Option<js_sys::Promise>,
    }

    impl LockGuard {
        pub(crate) async fn acquire(target: &Handle) -> Result<Self, FsError> {
            let lock_name = format!(
                "dialog-remote-fs:{}:{}",
                target.handle_id,
                target.segments.join("/"),
            );

            let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel::<()>();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

            // The browser invokes this callback once the lock is granted.
            // We signal acquisition and return a Promise that stays
            // pending until `release_rx` fires — keeping the lock held.
            let callback = Closure::once_into_js(move |_lock: JsValue| -> JsValue {
                let _ = acquired_tx.send(());
                wasm_bindgen_futures::future_to_promise(async move {
                    let _ = release_rx.await;
                    Ok(JsValue::UNDEFINED)
                })
                .into()
            });

            let mgr = lock_manager()?;
            let outer_promise = mgr.request_with_callback(&lock_name, callback.unchecked_ref());

            acquired_rx
                .await
                .map_err(|_| FsError::Lock("Web Lock acquisition cancelled".into()))?;

            Ok(Self {
                release_tx: Some(release_tx),
                outer_promise: Some(outer_promise),
            })
        }
    }

    impl Drop for LockGuard {
        fn drop(&mut self) {
            if let Some(tx) = self.release_tx.take() {
                let _ = tx.send(());
            }
            // Drain the outer promise so any lock-manager rejection
            // doesn't propagate as an unhandled rejection. We don't care
            // about the result — by the time we're here the critical
            // section has completed.
            if let Some(promise) = self.outer_promise.take() {
                wasm_bindgen_futures::spawn_local(async move {
                    let _ = JsFuture::from(promise).await;
                });
            }
        }
    }

    fn lock_manager() -> Result<web_sys::LockManager, FsError> {
        let global = js_sys::global();
        let navigator = js_sys::Reflect::get(&global, &"navigator".into())
            .map_err(|e| FsError::Lock(format!("no navigator: {e:?}")))?;
        js_sys::Reflect::get(&navigator, &"locks".into())
            .map_err(|e| FsError::Lock(format!("no lock manager: {e:?}")))?
            .dyn_into()
            .map_err(|_| FsError::Lock("expected LockManager".into()))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native::LockGuard;
#[cfg(target_arch = "wasm32")]
pub(crate) use web::LockGuard;
