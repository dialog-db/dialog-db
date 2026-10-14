//! A client's side of the socket: one connection per address, shared by
//! everything the site does there, with each reply handed to whoever is
//! waiting on the invocation it names.
//!
//! A connection holds no authority and no environment: every frame it
//! sends is an invocation minted and authorized before it is handed over,
//! and what reads the connection only routes the replies it receives.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use dialog_common::time::{self, SystemTime};

use dialog_effects::Rejection;
use dialog_effects::memory::{CellState, EditionSource, MemoryError};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_util::StreamExt as _;

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

/// The connections a site keeps, one per socket address: every watch of
/// a cell in one space at one service shares one.
#[derive(Debug, Clone, Default)]
pub struct Sockets {
    connections: Arc<Mutex<HashMap<String, Connection>>>,
    /// Socket addresses that could not be opened, and when.
    failed: Arc<Mutex<HashMap<String, SystemTime>>>,
}

impl Sockets {
    /// The connection to `url`, opened when there is none or the one
    /// there closed. A socket that could not be opened is answered as
    /// unavailable, without trying it again, until [`RETRY`] has passed.
    pub(crate) async fn connect(&self, url: &str) -> Result<Connection, MemoryError> {
        if let Some(connection) = self
            .connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(url)
            .filter(|connection| connection.is_open())
        {
            return Ok(connection.clone());
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
            Ok(link) => link,
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
        let connection = Connection {
            link: Arc::new(link),
            routes,
        };
        self.connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(url.to_string(), connection.clone());
        Ok(connection)
    }
}

/// Who is waiting on which invocation, shared between a connection and
/// what reads it.
#[derive(Debug, Clone, Default)]
pub(crate) struct Routes {
    waiting: Arc<Mutex<HashMap<String, UnboundedSender<Reply>>>>,
    closed: Arc<std::sync::atomic::AtomicBool>,
}

impl Routes {
    fn wait(&self, invocation: String) -> UnboundedReceiver<Reply> {
        let (sender, receiver) = unbounded();
        self.waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(invocation, sender);
        receiver
    }

    fn forget(&self, invocation: &str) {
        self.waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(invocation);
    }

    /// Hand `reply` to whoever waits on the invocation it names. A reply
    /// that ends what was waited on (a refused watch, an ended one) ends
    /// the wait.
    pub(crate) fn route(&self, reply: Reply) {
        let Some(invocation) = reply.invocation().map(str::to_string) else {
            return;
        };
        let last = !matches!(reply, Reply::State { .. });
        let mut waiting = self.waiting.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(sender) = waiting.get(&invocation)
            && sender.unbounded_send(reply).is_err()
        {
            waiting.remove(&invocation);
        }
        if last {
            waiting.remove(&invocation);
        }
    }

    /// The connection closed: every wait ends.
    pub(crate) fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        self.waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// A connection to a service's socket.
#[derive(Debug, Clone)]
pub(crate) struct Connection {
    link: Arc<Link>,
    routes: Routes,
}

impl Connection {
    fn is_open(&self) -> bool {
        !self.routes.is_closed()
    }

    /// Begin the watch `container` carries, named `invocation`: what the
    /// service answers it with arrives through the returned source.
    pub(crate) fn watch(
        &self,
        invocation: String,
        container: Vec<u8>,
    ) -> Result<Watching, MemoryError> {
        let replies = self.routes.wait(invocation.clone());
        let frame = Request::Invoke {
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
            replies,
            connection: self.clone(),
            ended: false,
        })
    }

    /// Send the invocation `container` carries, named `invocation`, with
    /// the bytes a write stores, and wait for the service's answer.
    pub(crate) async fn invoke(
        &self,
        invocation: String,
        container: Vec<u8>,
        payload: Option<Vec<u8>>,
    ) -> Result<Reply, MemoryError> {
        let mut replies = self.routes.wait(invocation.clone());
        let frame = Request::Invoke { container, payload }.encode();
        if let Err(error) = self.link.send(frame) {
            self.routes.forget(&invocation);
            return Err(error);
        }
        replies
            .next()
            .await
            .ok_or_else(|| unavailable("the socket closed before the service answered"))
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

/// A watch over a socket: the states the service delivers, until the
/// watch ends. Dropping it cancels the watch.
pub(crate) struct Watching {
    invocation: String,
    replies: UnboundedReceiver<Reply>,
    connection: Connection,
    ended: bool,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl EditionSource for Watching {
    async fn next(&mut self) -> Result<Option<CellState>, MemoryError> {
        if self.ended {
            return Ok(None);
        }
        match self.replies.next().await {
            Some(Reply::State { state, .. }) => Ok(Some(state)),
            Some(Reply::Answer { status, body, .. }) | Some(Reply::Ended { status, body, .. }) => {
                self.ended = true;
                Err(crate::direct::read_refusal(status, &body).into())
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
}
