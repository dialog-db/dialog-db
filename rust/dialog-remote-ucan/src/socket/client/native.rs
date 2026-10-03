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

#[derive(Debug)]
pub(crate) struct Link {
    outgoing: UnboundedSender<Message>,
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
                    break;
                }
            }
        });
        tokio::spawn(async move {
            while let Some(Ok(message)) = source.next().await {
                if let Message::Binary(bytes) = message
                    && let Ok(reply) = Reply::decode(&bytes)
                {
                    routes.route(reply);
                }
            }
            routes.close();
        });
        Ok(Self { outgoing })
    }

    pub(crate) fn send(&self, frame: Vec<u8>) -> Result<(), MemoryError> {
        self.outgoing
            .unbounded_send(Message::binary(frame))
            .map_err(unavailable)
    }
}
