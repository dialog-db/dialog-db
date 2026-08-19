//! Race-free settling of IndexedDB transactions.
//!
//! `rexie`'s `Transaction::done()` (via `idb`'s `TransactionFuture`)
//! installs its `complete`/`error`/`abort` handlers only when the done
//! future is first polled. The adapters here issue request awaits
//! before that, and every request await yields to the event loop — so
//! a transaction can auto-commit and dispatch `complete` in one of
//! those turns, before any handler exists. `done()` then arms handlers
//! on an already-settled transaction, no event ever fires, and the
//! await strands forever. Under a busy event loop (concurrent workers,
//! a service-worker handoff) that race hits reliably, and a stranded
//! settle inside a commit wedges every lock the commit holds.
//!
//! [`arm`] closes the race: it takes the transaction *before any
//! request is issued* and polls the done future once, which installs
//! the terminal handlers synchronously. Requests run afterwards;
//! [`Armed::settle`] then awaits the pre-armed future — under a
//! watchdog, so if settling is ever stranded again it surfaces as a
//! loud, distinctive error instead of an eternal hang (the caller's
//! CAS/retry machinery treats it like any failed write).
//!
//! One more sharp edge lives on the error path: when a request fails,
//! the transaction fires `error` and then `abort`. The done future
//! resolves on the first, and dropping it drops the Rust closures while
//! the transaction's JS side still points at their shims — the trailing
//! `abort` then throws `closure invoked … after being dropped` into the
//! console. [`Armed::settle`] parks the resolved future briefly instead
//! of dropping it, so the trailing event lands in a live closure.
//!
//! A residual remains out of reach from here: rexie's *request*-level
//! wiring drops its own once-closure shims when a request future
//! resolves, and an abort path can still land a late event on one of
//! those dead shims (the same console throw, request-flavored).
//! Silencing that class fully means replacing rexie's request wiring —
//! a follow-up; the transaction-level share, which is what production
//! bursts were made of, is covered here.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use rexie::TransactionResult;
use thiserror::Error as ThisError;
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::Closure;

/// How long a transaction may take to settle before the watchdog calls
/// it stranded. Generous: a slow disk under a congested event loop is
/// normal; a terminal event that never arrives is not.
const SETTLE_WATCHDOG_MS: u32 = 30_000;

/// How long a resolved-with-error done future is kept alive so the
/// trailing `abort` event finds live closures.
const ERROR_LINGER_MS: u32 = 2_000;

type DoneFuture = Pin<Box<dyn Future<Output = rexie::Result<TransactionResult>>>>;

/// Why a transaction failed to settle.
#[derive(Debug, ThisError)]
pub(crate) enum SettleError {
    /// The transaction settled with an error.
    #[error("{0}")]
    Transaction(rexie::Error),
    /// The transaction was aborted.
    #[error("transaction aborted")]
    Aborted,
    /// The terminal event never arrived within the watchdog window —
    /// the stranded-transaction signature, named so it can never again
    /// masquerade as an ordinary hang.
    #[error(
        "IndexedDB transaction settle exceeded {SETTLE_WATCHDOG_MS}ms: \
         terminal event never delivered (stranded transaction)"
    )]
    Stranded,
}

/// A transaction whose terminal handlers are already installed.
///
/// Dropping an `Armed` without settling it (a request errored and the
/// caller early-returned) parks the done future instead of dropping the
/// closures the transaction's trailing events still target.
pub(crate) struct Armed {
    done: Option<DoneFuture>,
}

/// Consume `transaction` into its done future and poll it once, which
/// runs `done()` up to its first await and installs the terminal event
/// handlers **now**, before the caller issues any request. Obtain the
/// object store(s) from the transaction before calling this.
pub(crate) fn arm(transaction: rexie::Transaction) -> Armed {
    let mut done: DoneFuture = Box::pin(transaction.done());
    poll_once(done.as_mut());
    Armed { done: Some(done) }
}

impl Armed {
    /// Await the transaction's terminal event.
    ///
    /// `Ok(())` on commit; `Err` on an aborted or failed transaction —
    /// and, per the watchdog, on a settle that never delivers its
    /// terminal event, reported distinctively so a stranded transaction
    /// reads as what it is rather than as an unbounded hang.
    pub(crate) async fn settle(mut self) -> Result<(), SettleError> {
        let mut done = self
            .done
            .take()
            .expect("settle consumes the armed future once");
        let mut watchdog = Box::pin(sleep_ms(SETTLE_WATCHDOG_MS));
        let outcome = futures_util::future::select(done.as_mut(), watchdog.as_mut()).await;
        match outcome {
            futures_util::future::Either::Left((result, _)) => match result {
                Ok(TransactionResult::Committed) => Ok(()),
                Ok(TransactionResult::Aborted) => {
                    linger(done);
                    Err(SettleError::Aborted)
                }
                Err(error) => {
                    linger(done);
                    Err(SettleError::Transaction(error))
                }
            },
            futures_util::future::Either::Right(((), _)) => {
                // Keep the armed future alive: if the event does arrive
                // late it lands in live closures instead of throwing.
                linger(done);
                Err(SettleError::Stranded)
            }
        }
    }
}

impl Drop for Armed {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            linger(done);
        }
    }
}

/// Poll a future exactly once with a no-op waker, driving an async fn
/// body to its first await point.
fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) {
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let _ = future.poll(&mut context);
}

/// Park a resolved-or-stranded done future for a beat before dropping
/// it, so trailing transaction events find live closures.
fn linger(done: DoneFuture) {
    wasm_bindgen_futures::spawn_local(async move {
        sleep_ms(ERROR_LINGER_MS).await;
        drop(done);
    });
}

/// A `setTimeout` sleep via `js-sys` alone, so this works in windows,
/// dedicated workers, and service workers without web-sys scope
/// features.
async fn sleep_ms(ms: u32) {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let global = js_sys::global();
        let set_timeout = js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout"))
            .expect("global setTimeout exists");
        let set_timeout: js_sys::Function = set_timeout.into();
        let closure = Closure::once_into_js(move || {
            let _ = resolve.call0(&JsValue::NULL);
        });
        let _ = set_timeout.call2(&global, &closure, &JsValue::from_f64(ms as f64));
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

#[cfg(test)]
mod tests {
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use rexie::{ObjectStore, Rexie, TransactionMode};

    const STORE: &str = "settle-test";

    async fn database(name: &str) -> Rexie {
        Rexie::builder(name)
            .version(1)
            .add_object_store(ObjectStore::new(STORE).auto_increment(false))
            .build()
            .await
            .expect("test database opens")
    }

    /// Let the browser run macrotasks so any pending IndexedDB terminal
    /// event has definitely been dispatched.
    async fn drain_event_loop() {
        for _ in 0..5 {
            sleep_ms(0).await;
        }
    }

    /// The fix, pinned: a transaction whose `complete` event fired long
    /// before the settle is awaited still settles, because [`arm`]
    /// installed the terminal handlers before the first request.
    #[dialog_common::test]
    async fn it_settles_a_transaction_that_completed_before_the_await() {
        let db = database("settle-armed").await;
        let tx = db
            .transaction(&[STORE], TransactionMode::ReadWrite)
            .expect("transaction opens");
        let store = tx.store(STORE).expect("store opens");
        let armed = arm(tx);

        store
            .put(
                &wasm_bindgen::JsValue::from_str("value"),
                Some(&wasm_bindgen::JsValue::from_str("key")),
            )
            .await
            .expect("put succeeds");

        // By the time we await, the transaction has auto-committed and
        // dispatched `complete` — the exact window where a late-armed
        // await strands.
        drain_event_loop().await;
        armed.settle().await.expect("the armed settle resolves");
    }

    /// The upstream defect, characterized: `done()` awaited after the
    /// terminal event was dispatched installs handlers on an
    /// already-settled transaction and never resolves. This is WHY
    /// [`arm`] exists.
    ///
    /// Deliberately not a hard assertion: whether `complete` has been
    /// dispatched by the time `done()` arms is a race against the
    /// browser (under suite load the commit can outlast the drain), so
    /// the test documents both outcomes and only guarantees that a
    /// late await never becomes an unbounded hang here. When a
    /// rexie/idb upgrade makes the stranded arm impossible, the logged
    /// outcome flips permanently and the arm-early workaround can be
    /// retired.
    #[dialog_common::test]
    async fn it_characterizes_the_late_await_stranding() {
        let db = database("settle-stranded").await;
        let tx = db
            .transaction(&[STORE], TransactionMode::ReadWrite)
            .expect("transaction opens");
        let store = tx.store(STORE).expect("store opens");

        store
            .put(
                &wasm_bindgen::JsValue::from_str("value"),
                Some(&wasm_bindgen::JsValue::from_str("key")),
            )
            .await
            .expect("put succeeds");
        drain_event_loop().await;

        // The transaction has settled; done() arms its handlers only
        // now, and no event will ever fire them.
        let late = Box::pin(tx.done());
        let timeout = Box::pin(sleep_ms(1_500));
        match futures_util::future::select(late, timeout).await {
            futures_util::future::Either::Left((outcome, _)) => {
                // The event had not been dispatched yet, so the late arm
                // caught it — the race went the survivable way this run.
                web_sys::console::log_1(
                    &format!("late-armed done() resolved this run: {outcome:?}").into(),
                );
            }
            futures_util::future::Either::Right(((), late)) => {
                // Stranded, as characterized: handlers armed after the
                // terminal event never fire. Park the future so its
                // closures outlive any late event.
                web_sys::console::log_1(
                    &"late-armed done() stranded, as characterized".into(),
                );
                wasm_bindgen_futures::spawn_local(async move {
                    let _ = late.await;
                });
            }
        }
    }
}
