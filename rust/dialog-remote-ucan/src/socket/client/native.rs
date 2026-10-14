//! The connection on native: a WebSocket whose reading and writing run as
//! tasks of their own, holding nothing but the socket and the routes.

use dialog_effects::memory::MemoryError;
use futures::channel::mpsc::{UnboundedSender, unbounded};
use futures_util::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use super::{Routes, unavailable};
use crate::socket::{Reply, SUBPROTOCOL};

/// The open socket: the queue its writing task sends from, and its
/// reading task. Dropping it closes the socket, so the service ends the
/// session and stops delivering to watches nobody reads.
#[derive(Debug)]
pub(crate) struct Link {
    outgoing: UnboundedSender<Message>,
    reader: tokio::task::AbortHandle,
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
            // The queue ends when the link is dropped: close the socket
            // cleanly, so the service sees the client go.
            let _ = sink.send(Message::Close(None)).await;
            let _ = sink.close().await;
        });
        let reader = tokio::spawn(async move {
            while let Some(Ok(message)) = source.next().await {
                if let Message::Binary(bytes) = message
                    && let Ok(reply) = Reply::decode(&bytes)
                {
                    routes.route(reply);
                }
            }
            routes.close();
        });
        Ok(Self {
            outgoing,
            reader: reader.abort_handle(),
        })
    }

    pub(crate) fn send(&self, frame: Vec<u8>) -> Result<(), MemoryError> {
        self.outgoing
            .unbounded_send(Message::binary(frame))
            .map_err(unavailable)
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        // Ending the queue has the writing task close the socket; the
        // reading task holds the other half of it open until it stops.
        self.outgoing.close_channel();
        self.reader.abort();
    }
}
