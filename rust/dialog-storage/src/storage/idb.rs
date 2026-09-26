//! A minimal IndexedDB layer over `web_sys`, owned outright.
//!
//! IndexedDB is an eager event source: a transaction starts settling
//! the moment the event loop turns, and a request delivers its
//! `success` or `error` whether or not anyone is still waiting. The
//! `rexie`/`idb` wrappers this replaces armed a transaction's terminal
//! listeners lazily (when its `done()` future was first polled) and
//! kept a request's listeners inside the request future, so a dropped
//! future freed closures the browser still pointed at. The first could
//! strand an await forever behind a lock; the second threw `closure
//! invoked … after being dropped` from every late event. Both are ruled
//! out here by construction:
//!
//! - **Terminal listeners are registered when the [`Transaction`] is
//!   created**, before any request can be issued, so no yield between
//!   requests and [`Transaction::settle`] can lose the event. Settling
//!   still runs under a watchdog: if a terminal event is ever lost
//!   again it surfaces as [`IdbError::Stranded`] instead of an
//!   unbounded hang.
//! - **Every wrapper clears its JS handler slots on drop** (database,
//!   transaction and request alike), before its closures are freed, so
//!   an event that fires after a future was dropped dispatches into
//!   nothing. A request cannot be aborted; it completes and its result
//!   is discarded.
//! - Handlers are plain `FnMut` closures that record the first outcome
//!   and absorb the rest, so a duplicate or trailing event never
//!   double-invokes anything. A transaction settles on `complete` or
//!   `abort` only: a failed request's `error` bubbles up to it first,
//!   and the `abort` it causes carries the reason.
//! - **A connection closes itself on `versionchange`**, as `idb` did,
//!   so another connection's schema upgrade is never blocked on it.
//!
//! The surface is what dialog uses: open-with-stores, two-mode
//! transactions, `put`/`get`/`delete`/`get_all`/`get_all_keys`, and a
//! bounded key range. The store methods keep `rexie`'s signatures.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use wasm_bindgen::prelude::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{
    Event, IdbDatabase, IdbKeyRange, IdbObjectStore, IdbOpenDbRequest, IdbRequest, IdbTransaction,
    IdbTransactionMode,
};

/// How long a transaction may take to settle before the watchdog calls
/// it stranded. Generous: a slow disk under a congested event loop is
/// normal; a terminal event that never arrives is not.
const SETTLE_WATCHDOG_MS: u32 = 30_000;

/// Errors from the IndexedDB layer.
#[derive(Debug, thiserror::Error)]
pub enum IdbError {
    /// A platform call failed; the message carries the DOM detail.
    #[error("{0}")]
    Platform(String),
    /// The transaction was aborted.
    #[error("transaction aborted: {0}")]
    Aborted(String),
    /// The terminal event never arrived within the watchdog window: the
    /// stranded-transaction signature, named so it can never pass for
    /// an ordinary hang.
    #[error(
        "IndexedDB transaction settle exceeded {SETTLE_WATCHDOG_MS}ms: \
         terminal event never delivered (stranded transaction)"
    )]
    Stranded,
}

fn platform(context: &str, value: JsValue) -> IdbError {
    let detail = value
        .dyn_ref::<web_sys::DomException>()
        .map(|e| format!("{}: {}", e.name(), e.message()))
        .unwrap_or_else(|| format!("{value:?}"));
    IdbError::Platform(format!("{context}: {detail}"))
}

/// Transaction mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionMode {
    /// Reads only.
    ReadOnly,
    /// Reads and writes.
    ReadWrite,
}

impl From<TransactionMode> for IdbTransactionMode {
    fn from(mode: TransactionMode) -> Self {
        match mode {
            TransactionMode::ReadOnly => IdbTransactionMode::Readonly,
            TransactionMode::ReadWrite => IdbTransactionMode::Readwrite,
        }
    }
}

/// Shared single-consumer completion cell: an outcome written once by
/// an event handler, a waker parked by the awaiting future.
struct Cell<T> {
    outcome: Option<T>,
    waker: Option<Waker>,
}

impl<T> Cell<T> {
    fn new() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            outcome: None,
            waker: None,
        }))
    }
}

fn fill<T>(cell: &Rc<RefCell<Cell<T>>>, value: T) {
    let mut cell = cell.borrow_mut();
    // First outcome wins; a duplicate or trailing event is absorbed.
    if cell.outcome.is_none() {
        cell.outcome = Some(value);
    }
    if let Some(waker) = cell.waker.take() {
        waker.wake();
    }
}

/// Resolves when its cell is filled.
struct Filled<'a, T> {
    cell: &'a Rc<RefCell<Cell<T>>>,
}

impl<T> Future for Filled<'_, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut cell = self.cell.borrow_mut();
        match cell.outcome.take() {
            Some(outcome) => Poll::Ready(outcome),
            None => {
                cell.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// An open database connection. Closed when dropped.
pub struct Database {
    inner: IdbDatabase,
    _on_version_change: Closure<dyn FnMut(Event)>,
}

impl Database {
    /// Open `name` at `version` (or at its current version when
    /// `None`), creating any of `stores` it lacks during the upgrade.
    /// Stores it already holds are kept.
    pub async fn open(
        name: &str,
        version: Option<u32>,
        stores: &[&str],
    ) -> Result<Database, IdbError> {
        let factory = js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("indexedDB"))
            .map_err(|e| platform("resolving indexedDB", e))?
            .dyn_into::<web_sys::IdbFactory>()
            .map_err(|e| platform("indexedDB is not an IDBFactory", e))?;

        let request: IdbOpenDbRequest = match version {
            Some(version) => factory.open_with_u32(name, version),
            None => factory.open(name),
        }
        .map_err(|e| platform("opening database", e))?;

        // An open cannot be cancelled: once issued, the browser will
        // deliver `upgradeneeded` (maybe) and then `success` or `error`
        // whether or not anyone still waits. So its handlers are owned by
        // JS (freed once they run) rather than by this future, and a
        // connection that opens after its waiter went away is closed on
        // arrival instead of lingering and blocking every later upgrade.
        // The handler that never runs (the other of success/error, and
        // upgradeneeded when no upgrade happens) is leaked with the few
        // bytes it captures; opens are rare.
        let state = Rc::new(RefCell::new(Opening::default()));

        // Stores are created inside `upgradeneeded`, which fires before
        // the open request succeeds.
        let wanted: Vec<String> = stores.iter().map(|store| store.to_string()).collect();
        let on_upgrade = Closure::once_into_js(move |event: Event| {
            let Some(db) = event
                .target()
                .and_then(|target| target.dyn_into::<IdbRequest>().ok())
                .and_then(|request| request.result().ok())
                .and_then(|result| result.dyn_into::<IdbDatabase>().ok())
            else {
                return;
            };
            let existing = db.object_store_names();
            for store in &wanted {
                if !existing.contains(store) {
                    let _ = db.create_object_store(store);
                }
            }
        });
        let opened = state.clone();
        let succeeded = request.clone();
        let on_success = Closure::once_into_js(move |_: Event| {
            let outcome = succeeded
                .result()
                .map_err(|e| platform("reading open result", e))
                .and_then(|result| {
                    result
                        .dyn_into::<IdbDatabase>()
                        .map_err(|e| platform("open result is not a database", e))
                });
            Opening::arrive(&opened, outcome);
        });
        let failed = state.clone();
        let errored = request.clone();
        let on_error = Closure::once_into_js(move |_: Event| {
            let detail = errored
                .error()
                .ok()
                .flatten()
                .map(|e| format!("{}: {}", e.name(), e.message()))
                .unwrap_or_else(|| "unknown open error".to_string());
            Opening::arrive(&failed, Err(IdbError::Platform(detail)));
        });
        request.set_onupgradeneeded(Some(on_upgrade.unchecked_ref()));
        request.set_onsuccess(Some(on_success.unchecked_ref()));
        request.set_onerror(Some(on_error.unchecked_ref()));

        let inner = OpenWaiter { state }.await?;

        // Another connection asking to upgrade waits until this one
        // closes; close at once so it never blocks on us. A transaction
        // started on a closed connection fails, and callers reopen.
        let closing = inner.clone();
        let on_version_change = Closure::<dyn FnMut(Event)>::new(move |_: Event| {
            closing.close();
        });
        inner.set_onversionchange(Some(on_version_change.as_ref().unchecked_ref()));

        Ok(Database {
            inner,
            _on_version_change: on_version_change,
        })
    }

    /// The database's current version.
    pub fn version(&self) -> u32 {
        self.inner.version() as u32
    }

    /// The names of every object store in the database.
    pub fn store_names(&self) -> Vec<String> {
        let names = self.inner.object_store_names();
        (0..names.length()).filter_map(|i| names.get(i)).collect()
    }

    /// Begin a transaction over `stores`. Its terminal listeners are
    /// registered here, before any request can be issued.
    pub fn transaction(
        &self,
        stores: &[&str],
        mode: TransactionMode,
    ) -> Result<Transaction, IdbError> {
        let names = js_sys::Array::new();
        for store in stores {
            names.push(&JsValue::from_str(store));
        }
        let inner = self
            .inner
            .transaction_with_str_sequence_and_mode(&names, mode.into())
            .map_err(|e| platform("opening transaction", e))?;
        Ok(Transaction::armed(inner, mode))
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.inner.set_onversionchange(None);
        // Pending transactions still settle after close.
        self.inner.close();
    }
}

/// The shared state of an open request: its outcome once it arrives,
/// the waiter's waker, and whether the waiter is gone.
#[derive(Default)]
struct Opening {
    outcome: Option<Result<IdbDatabase, IdbError>>,
    waker: Option<Waker>,
    abandoned: bool,
}

impl Opening {
    /// Record the open's outcome, or close the connection outright when
    /// nobody is waiting for it any more.
    fn arrive(state: &Rc<RefCell<Opening>>, outcome: Result<IdbDatabase, IdbError>) {
        let mut state = state.borrow_mut();
        if state.abandoned {
            if let Ok(db) = outcome {
                db.close();
            }
            return;
        }
        state.outcome = Some(outcome);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
}

/// Awaits an open request. Dropped early, it marks the open abandoned
/// and closes a connection that arrived but was never taken.
struct OpenWaiter {
    state: Rc<RefCell<Opening>>,
}

impl Future for OpenWaiter {
    type Output = Result<IdbDatabase, IdbError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();
        match state.outcome.take() {
            Some(outcome) => Poll::Ready(outcome),
            None => {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl Drop for OpenWaiter {
    fn drop(&mut self) {
        let mut state = self.state.borrow_mut();
        state.abandoned = true;
        state.waker = None;
        if let Some(Ok(db)) = state.outcome.take() {
            db.close();
        }
    }
}

/// A transaction, armed at construction: its `complete` and `abort`
/// listeners are live before the first request is issued, so no yield
/// can lose the terminal event. Those are its only terminal events: an
/// `error` reaching a transaction is a request's error bubbling up, and
/// the `abort` that follows it is what settles the transaction.
///
/// A read-write transaction commits only through
/// [`settle`](Self::settle): dropped before it settled (an early return
/// on an error, a cancelled future, a stranded settle), it is aborted,
/// so no caller ever leaves behind a write it did not see succeed. A
/// read-only transaction may simply be dropped once its reads resolve.
pub struct Transaction {
    inner: IdbTransaction,
    mode: TransactionMode,
    settled: bool,
    terminal: Rc<RefCell<Cell<Result<(), IdbError>>>>,
    _on_complete: Closure<dyn FnMut(Event)>,
    _on_abort: Closure<dyn FnMut(Event)>,
}

impl Transaction {
    fn armed(inner: IdbTransaction, mode: TransactionMode) -> Transaction {
        let terminal = Cell::new();
        let detail = |transaction: &IdbTransaction, fallback: &str| {
            transaction
                .error()
                .map(|e| format!("{}: {}", e.name(), e.message()))
                .unwrap_or_else(|| fallback.to_string())
        };

        let cell = terminal.clone();
        let on_complete = Closure::<dyn FnMut(Event)>::new(move |_: Event| {
            fill(&cell, Ok(()));
        });
        let cell = terminal.clone();
        let aborted = inner.clone();
        let on_abort = Closure::<dyn FnMut(Event)>::new(move |_: Event| {
            fill(
                &cell,
                Err(IdbError::Aborted(detail(
                    &aborted,
                    "aborted by the platform or a failed request",
                ))),
            );
        });

        inner.set_oncomplete(Some(on_complete.as_ref().unchecked_ref()));
        inner.set_onabort(Some(on_abort.as_ref().unchecked_ref()));

        Transaction {
            inner,
            mode,
            settled: false,
            terminal,
            _on_complete: on_complete,
            _on_abort: on_abort,
        }
    }

    /// An object store within this transaction.
    pub fn store(&self, name: &str) -> Result<ObjectStore, IdbError> {
        let inner = self
            .inner
            .object_store(name)
            .map_err(|e| platform("opening object store", e))?;
        Ok(ObjectStore { inner })
    }

    /// Await the transaction's terminal event, under the watchdog.
    pub async fn settle(mut self) -> Result<(), IdbError> {
        let terminal = self.terminal.clone();
        let settled = Filled { cell: &terminal };
        let watchdog = sleep_ms(SETTLE_WATCHDOG_MS);
        match futures_util::future::select(std::pin::pin!(settled), std::pin::pin!(watchdog)).await
        {
            futures_util::future::Either::Left((outcome, _)) => {
                self.settled = true;
                outcome
            }
            // Left unsettled, so dropping `self` aborts it.
            futures_util::future::Either::Right(((), _)) => Err(IdbError::Stranded),
        }
        // `self` drops after this, clearing the handler slots: whatever
        // fires later dispatches into nothing.
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.mode == TransactionMode::ReadWrite && !self.settled {
            // Fails harmlessly when the transaction already finished.
            let _ = self.inner.abort();
        }
        // Clear the JS-side slots before the closures are freed, so a
        // trailing event can never dispatch into a freed closure.
        self.inner.set_oncomplete(None);
        self.inner.set_onabort(None);
    }
}

/// A key range for [`ObjectStore::get_all`] and
/// [`ObjectStore::get_all_keys`].
pub struct KeyRange {
    inner: IdbKeyRange,
}

impl KeyRange {
    /// Keys between `lower` and `upper`; each bound is inclusive unless
    /// its `open` flag says otherwise.
    pub fn bound(
        lower: &JsValue,
        upper: &JsValue,
        lower_open: Option<bool>,
        upper_open: Option<bool>,
    ) -> Result<KeyRange, IdbError> {
        let inner = IdbKeyRange::bound_with_lower_open_and_upper_open(
            lower,
            upper,
            lower_open.unwrap_or(false),
            upper_open.unwrap_or(false),
        )
        .map_err(|e| platform("building key range", e))?;
        Ok(KeyRange { inner })
    }
}

/// An object store handle within a live transaction.
pub struct ObjectStore {
    inner: IdbObjectStore,
}

impl ObjectStore {
    /// The value under `key`, if any.
    pub async fn get(&self, key: JsValue) -> Result<Option<JsValue>, IdbError> {
        let request = self
            .inner
            .get(&key)
            .map_err(|e| platform("issuing get", e))?;
        let value = request_outcome(request).await?;
        Ok((!value.is_undefined()).then_some(value))
    }

    /// Store `value`, under `key` when given, overwriting. Returns the
    /// key it was stored under.
    pub async fn put(&self, value: &JsValue, key: Option<&JsValue>) -> Result<JsValue, IdbError> {
        let request = match key {
            Some(key) => self.inner.put_with_key(value, key),
            None => self.inner.put(value),
        }
        .map_err(|e| platform("issuing put", e))?;
        request_outcome(request).await
    }

    /// Store every `(value, key)` pair, overwriting. The puts are issued
    /// together and only the last is awaited: requests in a transaction
    /// complete in order, and a failure among the earlier ones aborts
    /// the transaction, which fails the last request and its settle.
    pub async fn put_all(
        &self,
        entries: impl Iterator<Item = (JsValue, Option<JsValue>)>,
    ) -> Result<(), IdbError> {
        let mut last = None;
        for (value, key) in entries {
            let request = match &key {
                Some(key) => self.inner.put_with_key(&value, key),
                None => self.inner.put(&value),
            }
            .map_err(|e| platform("issuing put", e))?;
            last = Some(request);
        }
        match last {
            Some(request) => request_outcome(request).await.map(|_| ()),
            None => Ok(()),
        }
    }

    /// Delete the entry under `key`. An absent key is a no-op.
    pub async fn delete(&self, key: JsValue) -> Result<(), IdbError> {
        let request = self
            .inner
            .delete(&key)
            .map_err(|e| platform("issuing delete", e))?;
        request_outcome(request).await.map(|_| ())
    }

    /// Every value in `range` (or the whole store), at most `limit`.
    pub async fn get_all(
        &self,
        range: Option<KeyRange>,
        limit: Option<u32>,
    ) -> Result<Vec<JsValue>, IdbError> {
        let request = match (range, limit) {
            (Some(range), Some(limit)) => {
                self.inner.get_all_with_key_and_limit(&range.inner, limit)
            }
            (Some(range), None) => self.inner.get_all_with_key(&range.inner),
            (None, Some(limit)) => self
                .inner
                .get_all_with_key_and_limit(&JsValue::UNDEFINED, limit),
            (None, None) => self.inner.get_all(),
        }
        .map_err(|e| platform("issuing get_all", e))?;
        array(request_outcome(request).await?, "get_all")
    }

    /// Every key in `range` (or the whole store), at most `limit`.
    pub async fn get_all_keys(
        &self,
        range: Option<KeyRange>,
        limit: Option<u32>,
    ) -> Result<Vec<JsValue>, IdbError> {
        let request = match (range, limit) {
            (Some(range), Some(limit)) => self
                .inner
                .get_all_keys_with_key_and_limit(&range.inner, limit),
            (Some(range), None) => self.inner.get_all_keys_with_key(&range.inner),
            (None, Some(limit)) => self
                .inner
                .get_all_keys_with_key_and_limit(&JsValue::UNDEFINED, limit),
            (None, None) => self.inner.get_all_keys(),
        }
        .map_err(|e| platform("issuing get_all_keys", e))?;
        array(request_outcome(request).await?, "get_all_keys")
    }
}

fn array(value: JsValue, context: &str) -> Result<Vec<JsValue>, IdbError> {
    let values: js_sys::Array = value
        .dyn_into()
        .map_err(|e| platform(&format!("{context} result is not an array"), e))?;
    Ok(values.iter().collect())
}

/// Await one request's `success` or `error`. The handler slots are
/// cleared when this future goes away, resolved or not, so dropping it
/// mid-flight leaves nothing for the late event to call.
async fn request_outcome(request: IdbRequest) -> Result<JsValue, IdbError> {
    let cell = Cell::new();

    let filled = cell.clone();
    let succeeded = request.clone();
    let on_success = Closure::<dyn FnMut(Event)>::new(move |_: Event| {
        let value = succeeded
            .result()
            .map_err(|e| platform("reading request result", e));
        fill(&filled, value);
    });
    let filled = cell.clone();
    let errored = request.clone();
    let on_error = Closure::<dyn FnMut(Event)>::new(move |_: Event| {
        let detail = errored
            .error()
            .ok()
            .flatten()
            .map(|e| format!("{}: {}", e.name(), e.message()))
            .unwrap_or_else(|| "unknown request error".to_string());
        fill(&filled, Err(IdbError::Platform(detail)));
    });

    // The guard owns both closures; its `Drop` clears the request's slots
    // before its fields, the closures, are freed.
    let _guard = RequestGuard {
        request: request.clone(),
        on_success,
        on_error,
    };
    _guard
        .request
        .set_onsuccess(Some(_guard.on_success.as_ref().unchecked_ref()));
    _guard
        .request
        .set_onerror(Some(_guard.on_error.as_ref().unchecked_ref()));

    Filled { cell: &cell }.await
}

/// Owns a request's handlers and clears the request's slots before
/// freeing them, including when the awaiting future is dropped.
struct RequestGuard {
    request: IdbRequest,
    on_success: Closure<dyn FnMut(Event)>,
    on_error: Closure<dyn FnMut(Event)>,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.request.set_onsuccess(None);
        self.request.set_onerror(None);
    }
}

/// A `setTimeout` sleep via `js-sys` alone, so it works in windows,
/// dedicated workers and service workers alike. Dropping it clears the
/// timer before freeing its callback, so an abandoned sleep (a settle
/// that finished long before its watchdog) never fires into a freed
/// closure.
fn sleep_ms(ms: u32) -> Sleep {
    let cell = Cell::new();
    let filled = cell.clone();
    let callback = Closure::<dyn FnMut()>::new(move || fill(&filled, ()));
    let global = js_sys::global();
    // Every worker and window scope has setTimeout; without one the
    // sleep simply never ends, which leaves a watchdog inert rather
    // than tripping it.
    let timer = js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout"))
        .ok()
        .and_then(|set_timeout| set_timeout.dyn_into::<js_sys::Function>().ok())
        .and_then(|set_timeout| {
            set_timeout
                .call2(
                    &global,
                    callback.as_ref(),
                    &JsValue::from_f64(f64::from(ms)),
                )
                .ok()
        });
    Sleep {
        cell,
        timer,
        _callback: callback,
    }
}

/// A pending [`sleep_ms`].
struct Sleep {
    cell: Rc<RefCell<Cell<()>>>,
    timer: Option<JsValue>,
    _callback: Closure<dyn FnMut()>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut cell = self.cell.borrow_mut();
        match cell.outcome.take() {
            Some(()) => Poll::Ready(()),
            None => {
                cell.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        let Some(timer) = &self.timer else { return };
        let global = js_sys::global();
        if let Some(clear_timeout) =
            js_sys::Reflect::get(&global, &JsValue::from_str("clearTimeout"))
                .ok()
                .and_then(|clear_timeout| clear_timeout.dyn_into::<js_sys::Function>().ok())
        {
            let _ = clear_timeout.call1(&global, timer);
        }
    }
}

#[cfg(test)]
mod tests {
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    const STORE: &str = "idb-test";

    async fn database(name: &str) -> Database {
        Database::open(name, Some(1), &[STORE])
            .await
            .expect("test database opens")
    }

    /// Let the browser run macrotasks so any pending IndexedDB event has
    /// been dispatched.
    async fn drain_event_loop() {
        for _ in 0..5 {
            sleep_ms(0).await;
        }
    }

    /// Records every uncaught error the worker reports while it lives.
    struct Uncaught {
        seen: Rc<RefCell<Vec<String>>>,
        listener: Closure<dyn FnMut(web_sys::ErrorEvent)>,
    }

    impl Uncaught {
        fn listen() -> Self {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let recorded = seen.clone();
            let listener = Closure::<dyn FnMut(web_sys::ErrorEvent)>::new(
                move |event: web_sys::ErrorEvent| {
                    recorded.borrow_mut().push(event.message());
                },
            );
            let global: web_sys::EventTarget = js_sys::global().unchecked_into();
            global
                .add_event_listener_with_callback("error", listener.as_ref().unchecked_ref())
                .expect("the worker accepts an error listener");
            Self { seen, listener }
        }

        fn messages(&self) -> Vec<String> {
            self.seen.borrow().clone()
        }
    }

    impl Drop for Uncaught {
        fn drop(&mut self) {
            let global: web_sys::EventTarget = js_sys::global().unchecked_into();
            let _ = global.remove_event_listener_with_callback(
                "error",
                self.listener.as_ref().unchecked_ref(),
            );
        }
    }

    /// A read whose future is dropped before its request settles must
    /// not leave the late `success` event a freed closure to call: the
    /// request completes, its result is discarded, and nothing throws.
    #[dialog_common::test]
    async fn it_drops_a_pending_read_without_a_late_throw() {
        let db = database("idb-drop-read").await;
        let uncaught = Uncaught::listen();

        for _ in 0..16 {
            let tx = db
                .transaction(&[STORE], TransactionMode::ReadOnly)
                .expect("transaction opens");
            let store = tx.store(STORE).expect("store opens");
            let mut read = Box::pin(store.get(JsValue::from_str("key")));
            // One poll issues the request and registers its handlers;
            // the future is then dropped with the request in flight.
            let waker = futures_util::task::noop_waker();
            let mut context = Context::from_waker(&waker);
            assert!(read.as_mut().poll(&mut context).is_pending());
            drop(read);
            drop(tx);
        }
        drain_event_loop().await;

        assert_eq!(
            uncaught.messages(),
            Vec::<String>::new(),
            "no late event may call into a dropped read"
        );
    }

    /// A read still returns its value, and a missing key reads as none.
    #[dialog_common::test]
    async fn it_reads_back_what_it_wrote() {
        let db = database("idb-round-trip").await;
        let tx = db
            .transaction(&[STORE], TransactionMode::ReadWrite)
            .expect("transaction opens");
        let store = tx.store(STORE).expect("store opens");
        store
            .put(&JsValue::from_str("value"), Some(&JsValue::from_str("key")))
            .await
            .expect("put succeeds");
        tx.settle().await.expect("the write settles");

        let tx = db
            .transaction(&[STORE], TransactionMode::ReadOnly)
            .expect("transaction opens");
        let store = tx.store(STORE).expect("store opens");
        assert_eq!(
            store.get(JsValue::from_str("key")).await.expect("get"),
            Some(JsValue::from_str("value"))
        );
        assert_eq!(
            store.get(JsValue::from_str("missing")).await.expect("get"),
            None
        );
        assert_eq!(
            store
                .get_all(
                    Some(
                        KeyRange::bound(
                            &JsValue::from_str("k"),
                            &JsValue::from_str("l"),
                            None,
                            None
                        )
                        .expect("range")
                    ),
                    None
                )
                .await
                .expect("get_all")
                .len(),
            1
        );
    }

    /// A transaction whose `complete` event fired long before the settle
    /// is awaited still settles: its listeners were live from the start.
    #[dialog_common::test]
    async fn it_settles_a_transaction_that_completed_before_the_await() {
        let db = database("idb-settle-late").await;
        let tx = db
            .transaction(&[STORE], TransactionMode::ReadWrite)
            .expect("transaction opens");
        let store = tx.store(STORE).expect("store opens");
        store
            .put(&JsValue::from_str("value"), Some(&JsValue::from_str("key")))
            .await
            .expect("put succeeds");

        // By now the transaction has auto-committed and dispatched
        // `complete`: the window where a late-armed await strands.
        drain_event_loop().await;
        tx.settle().await.expect("the settle resolves");
    }

    /// An aborted transaction fails its in-flight request and settles as
    /// `Aborted`, even though the request's `error` bubbles up to the
    /// transaction before its `abort` arrives; none of those events
    /// lands in a freed closure.
    #[dialog_common::test]
    async fn it_reports_an_aborted_transaction_without_a_late_throw() {
        let db = database("idb-abort").await;
        let uncaught = Uncaught::listen();
        let tx = db
            .transaction(&[STORE], TransactionMode::ReadWrite)
            .expect("transaction opens");
        let store = tx.store(STORE).expect("store opens");
        let (value, key) = (JsValue::from_str("value"), JsValue::from_str("key"));
        let mut write = Box::pin(store.put(&value, Some(&key)));
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(write.as_mut().poll(&mut context).is_pending());
        tx.inner.abort().expect("the transaction aborts");

        assert!(write.await.is_err(), "the in-flight request fails");
        assert!(
            matches!(tx.settle().await, Err(IdbError::Aborted(_))),
            "the transaction reports its abort"
        );
        drain_event_loop().await;
        assert_eq!(uncaught.messages(), Vec::<String>::new());
    }

    /// An open whose future is dropped before the connection arrives
    /// closes the connection when it does arrive: it neither throws nor
    /// lingers to block a later upgrade (which, with no `versionchange`
    /// handler of its own, it would do forever).
    #[dialog_common::test]
    async fn it_closes_a_connection_whose_open_was_dropped() {
        let uncaught = Uncaught::listen();
        let mut opening = Box::pin(Database::open("idb-dropped-open", Some(1), &[STORE]));
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(opening.as_mut().poll(&mut context).is_pending());
        drop(opening);
        drain_event_loop().await;

        let upgrade = Database::open("idb-dropped-open", Some(2), &[STORE, "second"]);
        let deadline = sleep_ms(5_000);
        match futures_util::future::select(std::pin::pin!(upgrade), std::pin::pin!(deadline)).await
        {
            futures_util::future::Either::Left((upgraded, _)) => {
                upgraded.expect("the upgrade opens");
            }
            futures_util::future::Either::Right(((), _)) => {
                panic!("the abandoned connection blocked the upgrade")
            }
        }
        assert_eq!(uncaught.messages(), Vec::<String>::new());
    }

    /// A write transaction dropped without settling is aborted: what it
    /// wrote is not committed.
    #[dialog_common::test]
    async fn it_rolls_back_a_write_that_never_settled() {
        let db = database("idb-rollback").await;
        {
            let tx = db
                .transaction(&[STORE], TransactionMode::ReadWrite)
                .expect("transaction opens");
            let store = tx.store(STORE).expect("store opens");
            store
                .put(&JsValue::from_str("value"), Some(&JsValue::from_str("key")))
                .await
                .expect("put succeeds");
            // Dropped here, as an early return or a cancelled future
            // would drop it.
        }
        drain_event_loop().await;

        let tx = db
            .transaction(&[STORE], TransactionMode::ReadOnly)
            .expect("transaction opens");
        let store = tx.store(STORE).expect("store opens");
        assert_eq!(
            store.get(JsValue::from_str("key")).await.expect("get"),
            None,
            "the unsettled write was rolled back"
        );
    }

    /// A settle that finished well before its watchdog leaves no timer
    /// behind to fire into a freed callback.
    #[dialog_common::test]
    async fn it_clears_an_abandoned_timer() {
        let uncaught = Uncaught::listen();
        let mut sleeping = Box::pin(sleep_ms(10));
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(sleeping.as_mut().poll(&mut context).is_pending());
        drop(sleeping);
        sleep_ms(50).await;
        drain_event_loop().await;
        assert_eq!(uncaught.messages(), Vec::<String>::new());
    }

    /// An upgrade from another connection is not blocked by an open one:
    /// the open connection closes on `versionchange`.
    #[dialog_common::test]
    async fn it_yields_to_another_connections_upgrade() {
        let first = database("idb-upgrade").await;
        let upgraded = Database::open("idb-upgrade", Some(2), &[STORE, "second"])
            .await
            .expect("the upgrade is not blocked by the first connection");
        assert!(upgraded.store_names().contains(&"second".to_string()));
        assert!(
            first
                .transaction(&[STORE], TransactionMode::ReadOnly)
                .is_err(),
            "the first connection closed itself"
        );
    }
}
