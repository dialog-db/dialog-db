//! A loopback access service: [`Access`] over a [`MemoryStore`], behind
//! a small HTTP server, provisioned for cross-target tests. Request
//! bodies reach the layer as they arrive and a blob read's answer
//! leaves as its source yields, so the streaming paths are the ones
//! the tests drive.
//!
//! Beside it, the service's socket: a [`Session`] per connection, told
//! of every cell the store writes, which is how a watch over the socket
//! learns of a change made over HTTP or over another connection.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use dialog_common::helpers::{Provider, Service};
use dialog_effects::blob::{BlobError, BlobReader, BlobSource};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt as _, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::server::conn::http1;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use async_trait::async_trait;
use dialog_capability::{Capability, Did, Provider as Perform};
use dialog_common::time::{self, UNIX_EPOCH};
use dialog_effects::archive::{self, ArchiveError};
use dialog_effects::blob::{self, BlobWriter};
use dialog_effects::memory::prelude::{PublishExt as _, RetractExt as _};
use dialog_effects::memory::{self, CellState, Edition, MemoryError, Version};
use futures_util::{SinkExt as _, StreamExt as _};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{
    ErrorResponse, Request as Handshake, Response as Accepted,
};
use tokio_tungstenite::tungstenite::http::HeaderValue;

use super::{MemoryStore, UcanServiceAddress};
use crate::server::{Access, Answer, Content, Payload, Request as AccessRequest};
use crate::socket::{Change, SUBPROTOCOL, Session};

/// How long a watch's authority stands, once found to hold, before it is
/// checked again on delivering to it.
const RECHECK: std::time::Duration = std::time::Duration::from_secs(30);

/// A running access service over an in-memory store.
pub struct UcanServer {
    /// The endpoint URL the service listens at.
    pub endpoint: String,
    /// The URL of the service's socket.
    pub socket: String,
    /// The store the service performs operations against.
    pub store: MemoryStore,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    socket_shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

struct ServerState {
    access: Access<Announcing>,
    max_body_bytes: u64,
}

/// A cell the store wrote, and what it now holds.
#[derive(Debug, Clone)]
struct Changed {
    subject: Did,
    space: String,
    cell: String,
    state: CellState,
}

/// The store, telling every open connection of each cell it writes.
#[derive(Debug, Clone)]
struct Announcing {
    store: MemoryStore,
    changes: broadcast::Sender<Changed>,
}

impl UcanServer {
    /// Start the service on a free loopback port, refusing bodies over
    /// `max_body_bytes`.
    pub async fn start(max_body_bytes: u64) -> anyhow::Result<Self> {
        let store = MemoryStore::default();
        let (changes, _) = broadcast::channel(64);
        let state = Arc::new(ServerState {
            access: Access::new(Announcing {
                store: store.clone(),
                changes: changes.clone(),
            }),
            max_body_bytes,
        });

        let socket_state = state.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { continue };
                        let state = state.clone();
                        tokio::spawn(async move {
                            let service = hyper::service::service_fn(move |req| {
                                let state = state.clone();
                                async move { handle(req, state).await }
                            });
                            let _ = http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service)
                                .await;
                        });
                    }
                }
            }
        });

        let sockets = TcpListener::bind("127.0.0.1:0").await?;
        let socket = format!("ws://{}/", sockets.local_addr()?);
        let (socket_shutdown_tx, mut socket_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut socket_shutdown_rx => break,
                    accepted = sockets.accept() => {
                        let Ok((stream, _)) = accepted else { continue };
                        let state = socket_state.clone();
                        let changes = changes.subscribe();
                        tokio::spawn(serve_socket(stream, state, changes));
                    }
                }
            }
        });

        Ok(Self {
            endpoint,
            socket,
            store,
            shutdown_tx,
            socket_shutdown_tx,
        })
    }
}

/// One connection to the socket: a session answering its frames, and
/// delivering each change the store announces to the watches it began.
async fn serve_socket(
    stream: tokio::net::TcpStream,
    state: Arc<ServerState>,
    mut changes: broadcast::Receiver<Changed>,
) {
    let speaks = |request: &Handshake, mut response: Accepted| -> Result<Accepted, ErrorResponse> {
        let asked = request
            .headers()
            .get_all("Sec-WebSocket-Protocol")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|protocol| protocol.trim() == SUBPROTOCOL);
        if asked {
            response.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                HeaderValue::from_static(SUBPROTOCOL),
            );
        }
        Ok(response)
    };
    let Ok(socket) = tokio_tungstenite::accept_hdr_async(stream, speaks).await else {
        return;
    };
    let (mut sink, mut source) = socket.split();
    let mut session = Session::new();
    loop {
        tokio::select! {
            message = source.next() => match message {
                Some(Ok(Message::Binary(bytes))) => {
                    if let Some(reply) = session.receive(&state.access, &bytes).await
                        && sink.send(Message::binary(reply.encode())).await.is_err()
                    {
                        break;
                    }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            change = changes.recv() => match change {
                Ok(change) => {
                    let delivered = session
                        .deliver(
                            &state.access,
                            Change {
                                subject: &change.subject,
                                space: &change.space,
                                cell: &change.cell,
                                state: &change.state,
                            },
                            RECHECK,
                            now_s(),
                        )
                        .await;
                    for reply in delivered {
                        if sink.send(Message::binary(reply.encode())).await.is_err() {
                            return;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
}

fn now_s() -> u64 {
    time::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default()
}

impl Announcing {
    fn announce(&self, subject: &Did, space: &str, cell: &str, state: CellState) {
        // Nobody listening is not a failure: there is no watch to tell.
        let _ = self.changes.send(Changed {
            subject: subject.clone(),
            space: space.to_string(),
            cell: cell.to_string(),
            state,
        });
    }
}

#[async_trait]
impl Perform<archive::Get> for Announcing {
    async fn execute(
        &self,
        capability: Capability<archive::Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        Perform::<archive::Get>::execute(&self.store, capability).await
    }
}

#[async_trait]
impl Perform<archive::Put> for Announcing {
    async fn execute(&self, capability: Capability<archive::Put>) -> Result<(), ArchiveError> {
        Perform::<archive::Put>::execute(&self.store, capability).await
    }
}

#[async_trait]
impl Perform<blob::Read> for Announcing {
    async fn execute(
        &self,
        capability: Capability<blob::Read>,
    ) -> Result<BlobReader, blob::BlobError> {
        Perform::<blob::Read>::execute(&self.store, capability).await
    }
}

#[async_trait]
impl Perform<blob::Import> for Announcing {
    async fn execute(
        &self,
        capability: Capability<blob::Import>,
    ) -> Result<BlobWriter, blob::BlobError> {
        Perform::<blob::Import>::execute(&self.store, capability).await
    }
}

#[async_trait]
impl Perform<memory::Resolve> for Announcing {
    async fn execute(
        &self,
        capability: Capability<memory::Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        Perform::<memory::Resolve>::execute(&self.store, capability).await
    }
}

#[async_trait]
impl Perform<memory::Publish> for Announcing {
    async fn execute(
        &self,
        capability: Capability<memory::Publish>,
    ) -> Result<Version, MemoryError> {
        let subject = capability.subject().clone();
        let space = capability.space().to_string();
        let cell = capability.cell().to_string();
        let content = capability.content().to_vec();
        let version = Perform::<memory::Publish>::execute(&self.store, capability).await?;
        self.announce(
            &subject,
            &space,
            &cell,
            Some(Edition {
                content,
                version: version.clone(),
            }),
        );
        Ok(version)
    }
}

#[async_trait]
impl Perform<memory::Retract> for Announcing {
    async fn execute(&self, capability: Capability<memory::Retract>) -> Result<(), MemoryError> {
        let subject = capability.subject().clone();
        let space = capability.space().to_string();
        let cell = capability.cell().to_string();
        Perform::<memory::Retract>::execute(&self.store, capability).await?;
        self.announce(&subject, &space, &cell, None);
        Ok(())
    }
}

type Body = UnsyncBoxBody<Bytes, std::io::Error>;

fn respond(status: StatusCode) -> hyper::http::response::Builder {
    Response::builder()
        .status(status)
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "POST, OPTIONS")
        .header(
            "Access-Control-Allow-Headers",
            "Authorization, Content-Type, Accept",
        )
        .header("Access-Control-Expose-Headers", "Content-Type, ETag")
        .header("Cache-Control", "no-store")
}

fn body(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

/// A response body streamed from a blob source, chunk by chunk.
fn streamed(source: BlobReader) -> Body {
    let chunks = futures_util::stream::unfold(source, |mut source| async move {
        match source.next().await {
            Ok(Some(chunk)) => Some((Ok(Frame::data(Bytes::from(chunk))), source)),
            Ok(None) => None,
            Err(error) => Some((Err(std::io::Error::other(error.to_string())), source)),
        }
    });
    StreamBody::new(chunks).boxed_unsync()
}

/// A request body as the layer reads it: chunk by chunk, as it arrives.
struct IncomingSource {
    body: Incoming,
}

#[async_trait::async_trait]
impl BlobSource for IncomingSource {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        loop {
            match self.body.frame().await {
                None => return Ok(None),
                Some(Err(error)) => return Err(BlobError::Storage(error.to_string())),
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        return Ok(Some(data.to_vec()));
                    }
                }
            }
        }
    }
}

async fn handle(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> Result<Response<Body>, Infallible> {
    if req.method() == Method::OPTIONS {
        return Ok(respond(StatusCode::NO_CONTENT).body(body("")).unwrap());
    }
    if req.method() != Method::POST {
        return Ok(respond(StatusCode::METHOD_NOT_ALLOWED)
            .body(body("Method not allowed"))
            .unwrap());
    }

    let authorization = req
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let declared = req
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if declared.is_some_and(|declared| declared > state.max_body_bytes) {
        return Ok(too_large(state.max_body_bytes));
    }
    let payload: BlobReader = Box::new(IncomingSource {
        body: req.into_body(),
    });
    let request = AccessRequest::new(authorization.as_deref()).payload(Payload::Stream(payload));
    let response = match state.access.handle(request).await {
        Answer::Performed(response) => response,
        Answer::Refused(refusal) => refusal.into_response(),
        Answer::Unsupported => {
            return Ok(respond(StatusCode::NOT_ACCEPTABLE)
                .header("Content-Type", "application/json")
                .body(body(
                    r#"{"kind":"Unsupported","detail":"this service performs operations only; send an invocation under the UCAN scheme"}"#,
                ))
                .unwrap());
        }
    };
    let mut builder = respond(StatusCode::from_u16(response.status).expect("a valid status"))
        .header("Content-Type", response.content_type);
    if let Some(version) = &response.version {
        builder = builder.header("ETag", format!("\"{version}\""));
    }
    let content = match response.body {
        Content::Bytes(bytes) => body(bytes),
        Content::Stream(source) => streamed(source),
    };
    Ok(builder.body(content).unwrap())
}

fn too_large(limit: u64) -> Response<Body> {
    respond(StatusCode::PAYLOAD_TOO_LARGE)
        .header("Content-Type", "application/json")
        .body(body(format!(
            r#"{{"error":{{"code":"PAYLOAD_TOO_LARGE","message":"request body exceeds the {limit}-byte limit"}}}}"#
        )))
        .unwrap()
}

#[async_trait::async_trait]
impl Provider for UcanServer {
    async fn stop(self) -> anyhow::Result<()> {
        let _ = self.shutdown_tx.send(());
        let _ = self.socket_shutdown_tx.send(());
        Ok(())
    }
}

/// How to provision the service.
#[derive(Debug, Clone)]
pub struct UcanSettings {
    /// The largest request body the service reads. Defaults to 32 MiB,
    /// room for any block a write carries.
    pub max_body_bytes: u64,
}

impl Default for UcanSettings {
    fn default() -> Self {
        Self {
            max_body_bytes: 32 * 1024 * 1024,
        }
    }
}

/// Start an access service over an in-memory store.
#[dialog_common::provider]
pub async fn ucan(
    settings: UcanSettings,
) -> anyhow::Result<Service<UcanServiceAddress, UcanServer>> {
    let server = UcanServer::start(settings.max_body_bytes).await?;
    let address = UcanServiceAddress {
        endpoint: server.endpoint.clone(),
        socket: server.socket.clone(),
    };
    Ok(Service::new(address, server))
}
