//! A client's side of the socket: one connection per address, shared by
//! everything the site does there, with each reply handed to whoever is
//! waiting on the frame or the watch it names.
//!
//! A connection holds no authority and no environment: every frame it
//! sends is an invocation minted and authorized before it is handed over,
//! and what reads the connection only routes the replies it receives.
//!
//! A connection is kept honest by a keeper that runs beside it (see
//! [`Liveness`]): it pings the service, takes the connection for dead when
//! nothing has been heard from it for a while, and closes it once nothing
//! waits on it. Silence on a socket cannot tell a cell that does not
//! change from a connection that is gone, so a connection the keeper has
//! not heard from is closed, and everything waiting on it is told.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use dialog_common::time::{self, Instant, SystemTime};

use dialog_effects::Rejection;
use dialog_effects::memory::{CellState, EditionSource, MemoryError};
use futures_util::future::{Either, select};

use super::{Reply, Request};

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
use native::Link;

#[cfg(target_arch = "wasm32")]
mod web;
#[cfg(target_arch = "wasm32")]
use web::Link;

/// How long a socket that could not be opened is not tried again: what
/// would have gone over it goes as requests meanwhile, without paying for
/// a connection attempt each time.
const RETRY: Duration = Duration::from_secs(30);

/// How a site keeps its socket connections honest, and when it gives up
/// on one.
///
/// A connection that has gone quiet (a laptop that slept, a network that
/// changed, a NAT that forgot the mapping) is not closed by anyone: the
/// platform reports nothing until something it sends fails, and a watch
/// sends nothing. So the client pings every [`heartbeat`](Self::heartbeat),
/// and a connection it has heard nothing from, not even the answer to a
/// ping, for [`deadline`](Self::deadline) is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Liveness {
    /// How often the client pings the service, and checks on the
    /// connection.
    pub heartbeat: Duration,
    /// How long the client goes without hearing anything from the service
    /// before it takes the connection for dead and closes it.
    pub deadline: Duration,
    /// How long an invocation waits for its answer, and a watch for the
    /// first state of its cell.
    pub answer: Duration,
    /// How long a connection nothing waits on is kept open.
    pub idle: Duration,
}

impl Default for Liveness {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_secs(15),
            deadline: Duration::from_secs(45),
            answer: Duration::from_secs(30),
            idle: Duration::from_secs(60),
        }
    }
}

/// The connections a site keeps, one per socket address: every watch of
/// a cell in one space at one service shares one.
#[derive(Debug, Clone, Default)]
pub struct Sockets {
    connections: Arc<Mutex<HashMap<String, Connection>>>,
    /// Socket addresses being opened: one open per address at a time, so
    /// invocations that race to a socket share the connection.
    opening: Arc<Mutex<HashMap<String, Arc<futures::lock::Mutex<()>>>>>,
    /// Socket addresses that could not be opened, and when.
    failed: Arc<Mutex<HashMap<String, SystemTime>>>,
    liveness: Liveness,
}

impl Sockets {
    /// Connections kept by `liveness`.
    pub fn with_liveness(liveness: Liveness) -> Self {
        Self {
            liveness,
            ..Self::default()
        }
    }

    /// How these connections are kept.
    pub fn liveness(&self) -> Liveness {
        self.liveness
    }

    /// The connection to `url`, opened when there is none or the one
    /// there closed. A socket that could not be opened is answered as
    /// unavailable, without trying it again, until [`RETRY`] has passed.
    pub(crate) async fn connect(&self, url: &str) -> Result<Connection, MemoryError> {
        if let Some(connection) = self.open(url) {
            return Ok(connection);
        }
        let gate = self
            .opening
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(url.to_string())
            .or_default()
            .clone();
        let _opening = gate.lock().await;
        // Whoever held the gate before may have opened it.
        if let Some(connection) = self.open(url) {
            return Ok(connection);
        }
        let recently = self
            .failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(url)
            .is_some_and(|failed| {
                time::now()
                    .duration_since(*failed)
                    .is_ok_and(|since| since < RETRY)
            });
        if recently {
            return Err(unavailable("it could not be opened a moment ago"));
        }
        let routes = Routes::default();
        let link = match Link::open(url, routes.clone()).await {
            Ok(link) => Arc::new(link),
            Err(error) => {
                self.failed
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(url.to_string(), time::now());
                return Err(error);
            }
        };
        self.failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(url);
        keep(Arc::downgrade(&link), routes.clone(), self.liveness);
        let connection = Connection {
            link,
            routes,
            liveness: self.liveness,
        };
        self.connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(url.to_string(), connection.clone());
        Ok(connection)
    }

    /// The open connection to `url`, if there is one. A closed one is
    /// forgotten, so its socket is released.
    fn open(&self, url: &str) -> Option<Connection> {
        let mut connections = self
            .connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match connections.get(url) {
            Some(connection) if connection.is_open() => Some(connection.clone()),
            Some(_) => {
                connections.remove(url);
                None
            }
            None => None,
        }
    }
}

/// Run the keeper of a connection beside it, for as long as the link is
/// held: ping the service, close the connection once nothing has been
/// heard from it for the deadline, and once nothing has waited on it for
/// the idle time.
fn keep(link: Weak<Link>, routes: Routes, liveness: Liveness) {
    let keeper = async move {
        let mut idle = Duration::ZERO;
        loop {
            time::sleep(liveness.heartbeat).await;
            let Some(link) = link.upgrade() else {
                return;
            };
            if routes.is_closed() {
                return;
            }
            if routes.silence() >= liveness.deadline {
                link.close();
                return;
            }
            if routes.is_busy() {
                idle = Duration::ZERO;
            } else {
                idle += liveness.heartbeat;
                if idle >= liveness.idle {
                    link.close();
                    return;
                }
            }
            if link.ping().is_err() {
                link.close();
                return;
            }
        }
    };
    #[cfg(not(target_arch = "wasm32"))]
    tokio::spawn(keeper);
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_futures::spawn_local(keeper);
}

/// What a wait receives: each state of a watched cell, coalesced to the
/// newest, then the reply that ends it, if any.
#[derive(Debug, Default)]
struct Slot {
    /// The newest state not yet read. A state that arrives before the
    /// last was read replaces it: a watch may skip states, but never the
    /// last one before it ends.
    state: Option<CellState>,
    /// The reply that ends the wait: an invocation's answer, or the end of
    /// a watch.
    last: Option<Reply>,
    /// The connection closed without such a reply.
    closed: bool,
    waker: Option<Waker>,
}

/// What a slot hands its reader.
#[derive(Debug)]
enum Received {
    State(CellState),
    Reply(Reply),
}

/// A slot shared between what routes replies into it and what reads them.
#[derive(Debug, Clone, Default)]
struct Wait(Arc<Mutex<Slot>>);

impl Wait {
    fn update(&self, change: impl FnOnce(&mut Slot)) {
        let mut slot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        change(&mut slot);
        if let Some(waker) = slot.waker.take() {
            waker.wake();
        }
    }

    /// The next thing received: the newest state, then the reply that
    /// ended the wait, then nothing once the wait is over.
    fn next(&self) -> impl Future<Output = Option<Received>> + '_ {
        std::future::poll_fn(move |context: &mut Context<'_>| {
            let mut slot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(state) = slot.state.take() {
                return Poll::Ready(Some(Received::State(state)));
            }
            if let Some(reply) = slot.last.take() {
                slot.closed = true;
                return Poll::Ready(Some(Received::Reply(reply)));
            }
            if slot.closed {
                return Poll::Ready(None);
            }
            slot.waker = Some(context.waker().clone());
            Poll::Pending
        })
    }
}

/// Who is waiting on which reply, shared between a connection and what
/// reads it.
///
/// An invocation awaiting its answer is found by the frame that carried
/// it, which the answer names even when the service could not read the
/// invocation, or else by the invocation. A watch, once the service
/// answers it with the cell's state, is found by its invocation, which
/// every later state names.
#[derive(Debug, Clone)]
pub(crate) struct Routes {
    waits: Arc<Mutex<Waits>>,
    frames: Arc<std::sync::atomic::AtomicU64>,
    closed: Arc<std::sync::atomic::AtomicBool>,
    /// When anything was last heard from the service.
    heard: Arc<Mutex<Instant>>,
}

impl Default for Routes {
    fn default() -> Self {
        Self {
            waits: Arc::default(),
            frames: Arc::default(),
            closed: Arc::default(),
            heard: Arc::new(Mutex::new(Instant::now())),
        }
    }
}

#[derive(Debug, Default)]
struct Waits {
    /// Invocations awaiting their answer, by the frame that carried them,
    /// with the invocation's name.
    answers: HashMap<u64, (String, Wait)>,
    /// Watches the service began, by invocation.
    watches: HashMap<String, Wait>,
}

impl Routes {
    /// Wait for what the service answers `invocation` with: the frame to
    /// send it in, and where the replies arrive.
    fn wait(&self, invocation: String) -> (u64, Wait) {
        let frame = self
            .frames
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let wait = Wait::default();
        self.waits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .answers
            .insert(frame, (invocation, wait.clone()));
        (frame, wait)
    }

    fn forget(&self, invocation: &str) {
        let mut waits = self.waits.lock().unwrap_or_else(PoisonError::into_inner);
        waits
            .answers
            .retain(|_, (awaited, _)| awaited != invocation);
        waits.watches.remove(invocation);
    }

    /// Something was heard from the service: the connection is alive.
    pub(crate) fn heard(&self) {
        *self.heard.lock().unwrap_or_else(PoisonError::into_inner) = Instant::now();
    }

    /// How long since anything was heard from the service.
    fn silence(&self) -> Duration {
        self.heard
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .elapsed()
    }

    /// Whether anything waits on the connection.
    fn is_busy(&self) -> bool {
        let waits = self.waits.lock().unwrap_or_else(PoisonError::into_inner);
        !waits.answers.is_empty() || !waits.watches.is_empty()
    }

    /// Hand `reply` to whoever waits on it. An answer, or the end of a
    /// watch, is the last reply its wait gets; a watch's first state moves
    /// it from awaiting its answer to following its cell.
    ///
    /// An answer naming neither a frame nor an invocation answers a frame
    /// the service could not read at all, which could have been any of
    /// those awaiting an answer, so every one of them is handed it.
    pub(crate) fn route(&self, reply: Reply) {
        self.heard();
        let mut waits = self.waits.lock().unwrap_or_else(PoisonError::into_inner);
        match reply {
            Reply::Answer {
                id: None,
                invocation: None,
                ..
            } => {
                for (_, (_, wait)) in waits.answers.drain() {
                    let reply = reply.clone();
                    wait.update(|slot| slot.last = Some(reply));
                }
            }
            Reply::Answer {
                id, ref invocation, ..
            } => {
                let frame = id.filter(|id| waits.answers.contains_key(id)).or_else(|| {
                    waits
                        .answers
                        .iter()
                        .find(|(_, (awaited, _))| Some(awaited) == invocation.as_ref())
                        .map(|(frame, _)| *frame)
                });
                if let Some((_, wait)) = frame.and_then(|frame| waits.answers.remove(&frame)) {
                    wait.update(|slot| slot.last = Some(reply));
                }
            }
            Reply::State { invocation, state } => {
                let began = waits
                    .answers
                    .iter()
                    .find(|(_, (awaited, _))| *awaited == invocation)
                    .map(|(frame, _)| *frame);
                if let Some((_, wait)) = began.and_then(|frame| waits.answers.remove(&frame)) {
                    waits.watches.insert(invocation.clone(), wait);
                }
                if let Some(wait) = waits.watches.get(&invocation) {
                    wait.update(|slot| slot.state = Some(state));
                }
            }
            Reply::Ended { ref invocation, .. } => {
                if let Some(wait) = waits.watches.remove(invocation) {
                    wait.update(|slot| slot.last = Some(reply));
                }
            }
        }
    }

    /// The connection closed: every wait ends.
    pub(crate) fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut waits = self.waits.lock().unwrap_or_else(PoisonError::into_inner);
        for (_, (_, wait)) in waits.answers.drain() {
            wait.update(|slot| slot.closed = true);
        }
        for (_, wait) in waits.watches.drain() {
            wait.update(|slot| slot.closed = true);
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Why an invocation over the socket has no answer.
#[derive(Debug)]
pub(crate) enum Failed {
    /// The frame never left: the connection could not take it. Whatever
    /// the invocation does, it was not done, and it may go another way.
    NotSent,
    /// The frame was sent and no answer came: the connection closed, or
    /// went quiet past the deadline, or the service could not read a frame
    /// that may have been this one. Whether the invocation was carried out
    /// is not known.
    Lost(MemoryError),
}

/// A connection to a service's socket.
#[derive(Debug, Clone)]
pub(crate) struct Connection {
    link: Arc<Link>,
    routes: Routes,
    liveness: Liveness,
}

impl Connection {
    fn is_open(&self) -> bool {
        !self.routes.is_closed()
    }

    /// Begin the watch `container` carries, named `invocation`: what the
    /// service answers it with arrives through the returned source, its
    /// first item the state of the cell or the refusal of the watch.
    pub(crate) fn watch(
        &self,
        invocation: String,
        container: Vec<u8>,
    ) -> Result<Watching, MemoryError> {
        let (id, wait) = self.routes.wait(invocation.clone());
        let frame = Request::Invoke {
            id: Some(id),
            container,
            payload: None,
        }
        .encode();
        if let Err(error) = self.link.send(frame) {
            self.routes.forget(&invocation);
            return Err(error);
        }
        Ok(Watching {
            invocation,
            wait,
            first: None,
            connection: self.clone(),
            ended: false,
        })
    }

    /// Send the invocation `container` carries, named `invocation`, with
    /// the bytes a write stores, and wait for the service's answer, for no
    /// longer than [`Liveness::answer`].
    pub(crate) async fn invoke(
        &self,
        invocation: String,
        container: Vec<u8>,
        payload: Option<Vec<u8>>,
    ) -> Result<Reply, Failed> {
        let (id, wait) = self.routes.wait(invocation.clone());
        let frame = Request::Invoke {
            id: Some(id),
            container,
            payload,
        }
        .encode();
        if self.link.send(frame).is_err() {
            self.routes.forget(&invocation);
            return Err(Failed::NotSent);
        }
        let answered = within(self.liveness.answer, wait.next()).await;
        self.routes.forget(&invocation);
        match answered {
            None => Err(Failed::Lost(unavailable(
                "the service did not answer in time",
            ))),
            Some(Some(Received::Reply(Reply::Answer {
                id: None,
                invocation: None,
                ..
            }))) => Err(Failed::Lost(unavailable(
                "the service could not read a frame, which may have been this one",
            ))),
            Some(Some(Received::Reply(reply))) => Ok(reply),
            Some(Some(Received::State(_))) => Err(Failed::Lost(unavailable(
                "the service answered an invocation with the state of a cell",
            ))),
            Some(None) => Err(Failed::Lost(unavailable(
                "the socket closed before the service answered",
            ))),
        }
    }

    fn cancel(&self, invocation: &str) {
        self.routes.forget(invocation);
        let frame = Request::Cancel {
            invocation: invocation.to_string(),
        }
        .encode();
        // Best effort: a connection that is gone has nothing to stop.
        let _ = self.link.send(frame);
    }
}

/// `future`'s output, or `None` once `limit` has passed.
async fn within<F: Future>(limit: Duration, future: F) -> Option<F::Output> {
    let future = std::pin::pin!(future);
    let timer = Box::pin(time::sleep(limit));
    match select(future, timer).await {
        Either::Left((output, _)) => Some(output),
        Either::Right(((), _)) => None,
    }
}

/// A watch over a socket: the states the service delivers, until the
/// watch ends. Dropping it cancels the watch.
pub(crate) struct Watching {
    invocation: String,
    wait: Wait,
    /// The first state, when [`begun`](Self::begun) read it already.
    first: Option<CellState>,
    connection: Connection,
    ended: bool,
}

impl Watching {
    /// The watch, once the service began it: what it answered first, the
    /// state of the cell, is the first state the watch delivers. A watch
    /// the service refused is that refusal, and one it did not answer in
    /// time is unavailable.
    pub(crate) async fn begun(mut self) -> Result<Self, MemoryError> {
        let limit = self.connection.liveness.answer;
        match within(limit, self.wait.next()).await {
            Some(Some(Received::State(state))) => {
                self.first = Some(state);
                Ok(self)
            }
            Some(Some(Received::Reply(reply))) => {
                self.ended = true;
                Err(refusal(reply))
            }
            Some(None) => {
                self.ended = true;
                Err(unavailable("the socket closed before the watch began"))
            }
            None => Err(unavailable("the service did not begin the watch in time")),
        }
    }
}

/// The error a reply ending a watch carries.
fn refusal(reply: Reply) -> MemoryError {
    match reply {
        Reply::Answer { status, body, .. } | Reply::Ended { status, body, .. } => {
            crate::direct::read_refusal(status, &body).into()
        }
        Reply::State { .. } => unavailable("a watch ended with a state"),
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl EditionSource for Watching {
    async fn next(&mut self) -> Result<Option<CellState>, MemoryError> {
        if let Some(state) = self.first.take() {
            return Ok(Some(state));
        }
        if self.ended {
            return Ok(None);
        }
        match self.wait.next().await {
            Some(Received::State(state)) => Ok(Some(state)),
            Some(Received::Reply(reply)) => {
                self.ended = true;
                Err(refusal(reply))
            }
            None => {
                self.ended = true;
                Ok(None)
            }
        }
    }
}

impl Drop for Watching {
    fn drop(&mut self) {
        if !self.ended {
            self.connection.cancel(&self.invocation);
        }
    }
}

/// The connection could not be used.
pub(crate) fn unavailable(reason: impl std::fmt::Display) -> MemoryError {
    Rejection::Unavailable {
        reason: format!("the socket could not be used: {reason}"),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::Sockets;
    use dialog_effects::Rejection;
    use dialog_effects::memory::MemoryError;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Accept `stream` as a WebSocket that speaks the subprotocol a client
    /// asks for, as the service does.
    #[cfg(not(target_arch = "wasm32"))]
    // The handshake callback's error type is tungstenite's, not ours.
    #[allow(clippy::result_large_err)]
    async fn accept(
        stream: tokio::net::TcpStream,
    ) -> Option<tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>> {
        use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
        use tokio_tungstenite::tungstenite::http::HeaderValue;
        let speaks = |_: &Request, mut response: Response| -> Result<Response, ErrorResponse> {
            response.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                HeaderValue::from_static(crate::socket::SUBPROTOCOL),
            );
            Ok(response)
        };
        tokio_tungstenite::accept_hdr_async(stream, speaks)
            .await
            .ok()
    }

    /// A socket that could not be opened is not tried again at once: the
    /// second connect is answered as unavailable without reaching it.
    ///
    /// Native only, for the listener that counts attempts; the browser
    /// connects through the same `Sockets`, whose memory of failures this
    /// pins.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_does_not_try_a_failed_socket_again_at_once() -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}/", listener.local_addr()?);
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = attempts.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });

        let sockets = Sockets::default();
        assert!(sockets.connect(&url).await.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let again = sockets.connect(&url).await;
        assert!(
            matches!(
                again,
                Err(MemoryError::Rejected(Rejection::Unavailable { .. }))
            ),
            "{:?}",
            again.err()
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "it was not tried again");
        Ok(())
    }

    /// A service that answers a frame it cannot attribute to an invocation
    /// (one whose container it could not read, say) answers with no
    /// invocation named, and the client has no way to match that answer to
    /// what it sent: whoever sent the frame waits for an answer that never
    /// reaches them. An invocation over the socket must be answered, or
    /// fail, within a bounded time, as a request would.
    ///
    /// Native only, for the service it stands up; the browser routes
    /// replies through the same `Routes`.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_does_not_wait_forever_on_an_answer_naming_no_invocation() -> anyhow::Result<()> {
        use crate::socket::Reply;
        use futures_util::{SinkExt as _, StreamExt as _};
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}/", listener.local_addr()?);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let Some(mut socket) = accept(stream).await else {
                        return;
                    };
                    while let Some(Ok(message)) = socket.next().await {
                        if let Message::Binary(_) = message {
                            let reply = Reply::Answer {
                                id: None,
                                invocation: None,
                                status: 400,
                                version: None,
                                body: br#"{"kind":"Malformed","detail":"unreadable"}"#.to_vec(),
                            };
                            if socket.send(Message::binary(reply.encode())).await.is_err() {
                                break;
                            }
                        }
                    }
                });
            }
        });

        let sockets = Sockets::default();
        let connection = sockets.connect(&url).await?;
        let answered = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            connection.invoke("bafy-unreadable".into(), b"not a container".to_vec(), None),
        )
        .await;
        assert!(
            matches!(answered, Ok(Err(_))),
            "the invocation must fail rather than wait forever; got {answered:?}"
        );
        Ok(())
    }

    /// A client that is done with a service (its site dropped, with every
    /// watch) closes the connection, so the service stops delivering to,
    /// and checking the authority of, watches nobody reads.
    ///
    /// Native only, for the service it stands up; the browser's link closes
    /// its socket when it is dropped.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_closes_the_connection_when_the_client_is_dropped() -> anyhow::Result<()> {
        use futures_util::StreamExt as _;
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}/", listener.local_addr()?);
        let (closed, saw_close) = tokio::sync::oneshot::channel::<bool>();
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Some(mut socket) = accept(stream).await else {
                return;
            };
            // The next thing the service reads is the client going away.
            let close = match socket.next().await {
                None | Some(Ok(Message::Close(_))) | Some(Err(_)) => true,
                Some(Ok(_)) => false,
            };
            let _ = closed.send(close);
        });

        let sockets = Sockets::default();
        let connection = sockets.connect(&url).await?;
        drop(connection);
        drop(sockets);

        let seen = tokio::time::timeout(std::time::Duration::from_secs(2), saw_close).await;
        assert!(
            matches!(seen, Ok(Ok(true))),
            "the service must see the connection close once the client is gone; got {seen:?}"
        );
        Ok(())
    }

    /// A liveness short enough for a test to see a connection given up on.
    fn quick() -> super::Liveness {
        super::Liveness {
            heartbeat: std::time::Duration::from_millis(100),
            deadline: std::time::Duration::from_millis(500),
            answer: std::time::Duration::from_secs(5),
            idle: std::time::Duration::from_secs(5),
        }
    }

    /// Stand up a service that completes the handshake, answers each
    /// frame with what `answer` gives for it (nothing, for `None`), and
    /// then neither answers nor closes: what a client sees of a connection
    /// that went half-open, as one does when a laptop sleeps or a NAT
    /// forgets the mapping. Answers the socket's URL.
    #[cfg(not(target_arch = "wasm32"))]
    async fn silent_after(
        answer: impl Fn() -> Option<crate::socket::Reply>
        + dialog_common::ConditionalSend
        + dialog_common::ConditionalSync
        + 'static,
    ) -> anyhow::Result<String> {
        use futures_util::{SinkExt as _, StreamExt as _};
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}/", listener.local_addr()?);
        let answer = Arc::new(answer);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let answer = answer.clone();
                tokio::spawn(async move {
                    let Some(mut socket) = accept(stream).await else {
                        return;
                    };
                    let mut answered = false;
                    while let Some(Ok(message)) = socket.next().await {
                        if let Message::Binary(_) = message
                            && !answered
                        {
                            answered = true;
                            if let Some(reply) = answer() {
                                let _ = socket.send(Message::binary(reply.encode())).await;
                            }
                        }
                        // Everything after is read and ignored, and the
                        // connection is held open.
                    }
                });
            }
        });
        Ok(url)
    }

    /// An invocation sent over a connection that has gone silent fails
    /// within a bounded time rather than waiting for ever. Every resolve
    /// and publish of a space's cells rides its socket, and
    /// `Branch::replicate` awaits each pull and push inline, so one silent
    /// socket stalls the replication of every branch at that space.
    ///
    /// The connection here is kept with a short [`Liveness`], so the
    /// deadline it takes the connection for dead by falls well inside the
    /// 10 seconds the test waits.
    ///
    /// Native only, for the service it stands up; a browser's WebSocket
    /// does not surface a half-open connection either, and routes replies
    /// through the same `Routes`.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_does_not_wait_forever_on_a_connection_that_went_silent() -> anyhow::Result<()> {
        let url = silent_after(|| None).await?;
        let sockets = Sockets::with_liveness(quick());
        let connection = sockets.connect(&url).await?;
        let answered = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            connection.invoke("bafy-resolve".into(), b"a resolve".to_vec(), None),
        )
        .await;
        assert!(
            matches!(answered, Ok(Err(_))),
            "the invocation must fail rather than wait for ever; got {answered:?}"
        );
        Ok(())
    }

    /// A watch over a connection that went silent after the watch began
    /// ends within a bounded time. Until it ends, `Branch::replicate`
    /// counts the upstream as watched, so its ticks merge the head last
    /// recorded instead of asking the remote, and the branch silently
    /// stops following its upstream.
    ///
    /// Native only, for the service it stands up; see
    /// `it_does_not_wait_forever_on_a_connection_that_went_silent`.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_ends_a_watch_whose_connection_went_silent() -> anyhow::Result<()> {
        use crate::socket::Reply;
        use dialog_effects::memory::EditionSource as _;

        let url = silent_after(|| {
            Some(Reply::State {
                invocation: "bafy-watch".into(),
                state: None,
            })
        })
        .await?;
        let sockets = Sockets::with_liveness(quick());
        let connection = sockets.connect(&url).await?;
        let mut watching = connection.watch("bafy-watch".into(), b"a watch".to_vec())?;
        assert_eq!(watching.next().await?, Some(None), "the watch began");

        let next = tokio::time::timeout(std::time::Duration::from_secs(10), watching.next()).await;
        assert!(
            matches!(next, Ok(Ok(None)) | Ok(Err(_))),
            "the watch must end once its connection is found dead; got {next:?}"
        );
        Ok(())
    }

    /// Accept connections at a fresh address, counting how many were
    /// opened and saying when each one closes. Answers the socket's URL,
    /// how many connections were opened, and a receiver of closes.
    #[cfg(not(target_arch = "wasm32"))]
    async fn counted() -> anyhow::Result<(
        String,
        Arc<AtomicUsize>,
        tokio::sync::mpsc::UnboundedReceiver<()>,
    )> {
        use futures_util::{SinkExt as _, StreamExt as _};
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}/", listener.local_addr()?);
        let opened = Arc::new(AtomicUsize::new(0));
        let (closed, closes) = tokio::sync::mpsc::unbounded_channel();
        let counting = opened.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                counting.fetch_add(1, Ordering::SeqCst);
                let closed = closed.clone();
                tokio::spawn(async move {
                    let Some(mut socket) = accept(stream).await else {
                        return;
                    };
                    // A live service: it answers every ping.
                    while let Some(Ok(message)) = socket.next().await {
                        match message {
                            Message::Text(text) if text.as_str() == crate::socket::PING => {
                                let _ = socket.send(Message::text(crate::socket::PONG)).await;
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                    let _ = closed.send(());
                });
            }
        });
        Ok((url, opened, closes))
    }

    /// Invocations that race to a socket nobody has opened share one
    /// connection, rather than each opening its own and all but one being
    /// left open beside the one kept.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_opens_one_connection_for_invocations_that_race_to_it() -> anyhow::Result<()> {
        let (url, opened, _) = counted().await?;
        let sockets = Sockets::with_liveness(quick());
        let (first, second, third) = tokio::join!(
            sockets.connect(&url),
            sockets.connect(&url),
            sockets.connect(&url)
        );
        first?;
        second?;
        third?;
        assert_eq!(
            opened.load(Ordering::SeqCst),
            1,
            "one connection was opened"
        );
        Ok(())
    }

    /// A connection nothing waits on is closed once it has been idle for
    /// the idle time, though the site that opened it is still held: a site
    /// that has touched many spaces does not keep a socket open to each.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_closes_a_connection_nothing_waits_on() -> anyhow::Result<()> {
        let (url, opened, mut closes) = counted().await?;
        let liveness = super::Liveness {
            idle: std::time::Duration::from_millis(300),
            ..quick()
        };
        let sockets = Sockets::with_liveness(liveness);
        let _connection = sockets.connect(&url).await?;

        let closed = tokio::time::timeout(std::time::Duration::from_secs(5), closes.recv()).await;
        assert!(
            matches!(closed, Ok(Some(()))),
            "the idle connection must be closed; got {closed:?}"
        );

        // What comes next opens a connection again.
        sockets.connect(&url).await?;
        assert_eq!(opened.load(Ordering::SeqCst), 2);
        Ok(())
    }

    /// A connection to a service that answers its pings is kept while
    /// something waits on it, however long nothing else is said.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_keeps_a_connection_whose_service_answers_pings() -> anyhow::Result<()> {
        let (url, opened, mut closes) = counted().await?;
        let sockets = Sockets::with_liveness(quick());
        let connection = sockets.connect(&url).await?;
        // A watch the service never answers keeps the connection busy.
        let _watching = connection.watch("bafy-watch".into(), b"a watch".to_vec())?;

        let closed =
            tokio::time::timeout(std::time::Duration::from_millis(1500), closes.recv()).await;
        assert!(closed.is_err(), "the connection was kept; got {closed:?}");
        assert!(sockets.connect(&url).await?.is_open());
        assert_eq!(opened.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
