//! A loopback access service: [`Access`] over a [`MemoryStore`], behind
//! a small HTTP server, provisioned for cross-target tests. Request
//! bodies reach the layer as they arrive and a blob read's answer
//! leaves as its source yields, so the streaming paths are the ones
//! the tests drive.

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

use super::{MemoryStore, UcanServiceAddress};
use crate::server::{Access, Answer, Content, Payload, Request as AccessRequest};

/// A running access service over an in-memory store.
pub struct UcanServer {
    /// The endpoint URL the service listens at.
    pub endpoint: String,
    /// The store the service performs operations against.
    pub store: MemoryStore,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

struct ServerState {
    access: Access<MemoryStore>,
    max_body_bytes: u64,
}

impl UcanServer {
    /// Start the service on a free loopback port, refusing bodies over
    /// `max_body_bytes`.
    pub async fn start(max_body_bytes: u64) -> anyhow::Result<Self> {
        let store = MemoryStore::default();
        let state = Arc::new(ServerState {
            access: Access::new(store.clone()),
            max_body_bytes,
        });

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

        Ok(Self {
            endpoint,
            store,
            shutdown_tx,
        })
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
    };
    Ok(Service::new(address, server))
}
