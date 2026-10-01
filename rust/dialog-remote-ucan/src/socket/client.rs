//! A client's side of the socket: one connection per address, shared by
//! everything the site does there, with each reply handed to whoever is
//! waiting on the invocation it names.
//!
//! A connection holds no authority and no environment: every frame it
//! sends is an invocation minted and authorized before it is handed over,
//! and what reads the connection only routes the replies it receives.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

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

/// The connections a site keeps, one per socket address: every watch of
/// a cell in one space at one service shares one.
#[derive(Debug, Clone, Default)]
pub struct Sockets {
    connections: Arc<Mutex<HashMap<String, Connection>>>,
}

impl Sockets {
    /// The connection to `url`, opened when there is none or the one
    /// there closed.
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
        let routes = Routes::default();
        let link = Link::open(url, routes.clone()).await?;
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
