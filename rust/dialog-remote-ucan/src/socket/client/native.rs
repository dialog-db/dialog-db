//! The connection on native: a WebSocket whose reading and writing run as
//! tasks of their own, holding nothing but the socket and the routes.

use dialog_effects::memory::MemoryError;
use futures::channel::mpsc::{UnboundedSender, unbounded};
use futures_util::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use super::{Routes, unavailable};
use crate::socket::{PING, Reply, SUBPROTOCOL};

/// The open socket: the queue its writing task sends from, and its
/// reading task. Closing it, or dropping it, closes the socket, so the
/// service ends the session and stops delivering to watches nobody reads,
/// and ends every wait on it.
#[derive(Debug)]
pub(crate) struct Link {
    outgoing: UnboundedSender<Message>,
    reader: tokio::task::AbortHandle,
    routes: Routes,
}

impl Link {
    pub(crate) async fn open(url: &str, routes: Routes) -> Result<Self, MemoryError> {
        let mut request = url.into_client_request().map_err(unavailable)?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static(SUBPROTOCOL),
        );
        let (socket, _) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(unavailable)?;
        let (mut sink, mut source) = socket.split();
        let (outgoing, mut queue) = unbounded::<Message>();
        tokio::spawn(async move {
            while let Some(message) = queue.next().await {
                if sink.send(message).await.is_err() {
                    return;
                }
            }
            // The queue ends when the link closes: close the socket
            // cleanly, so the service sees the client go.
            let _ = sink.send(Message::Close(None)).await;
            let _ = sink.close().await;
        });
        let reading = routes.clone();
        let reader = tokio::spawn(async move {
            let reason = loop {
                match source.next().await {
                    Some(Ok(Message::Close(frame))) => {
                        break match frame {
                            Some(frame) => format!(
                                "it was closed with code {}{}{}",
                                u16::from(frame.code),
                                if frame.reason.is_empty() { "" } else { ": " },
                                frame.reason
                            ),
                            None => "it was closed".to_string(),
                        };
                    }
                    Some(Ok(message)) => {
                        // Anything the service sends, a pong among them,
                        // says the connection is alive.
                        reading.heard();
                        if let Message::Binary(bytes) = message
                            && let Ok(reply) = Reply::decode(&bytes)
                        {
                            reading.route(reply);
                        }
                    }
                    Some(Err(error)) => break format!("it failed: {error}"),
                    None => break "it ended".to_string(),
                }
            };
            reading.close(&reason);
        });
        routes.heard();
        Ok(Self {
            outgoing,
            reader: reader.abort_handle(),
            routes,
        })
    }

    pub(crate) fn send(&self, frame: Vec<u8>) -> Result<(), MemoryError> {
        self.outgoing
            .unbounded_send(Message::binary(frame))
            .map_err(unavailable)
    }

    /// Ask the service whether the connection is alive.
    pub(crate) fn ping(&self) -> Result<(), MemoryError> {
        self.outgoing
            .unbounded_send(Message::text(PING))
            .map_err(unavailable)
    }

    /// Close the socket, for `reason`, and end every wait on it.
    pub(crate) fn close(&self, reason: &str) {
        // Ending the queue has the writing task close the socket; the
        // reading task holds the other half of it open until it stops.
        self.outgoing.close_channel();
        self.reader.abort();
        self.routes.close(reason);
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        self.close("the connection was released");
    }
}
