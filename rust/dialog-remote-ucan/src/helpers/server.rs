//! A loopback access service: [`Access`] over a [`MemoryStore`], behind
//! a small HTTP server, provisioned for cross-target tests.

use std::convert::Infallible;
use std::sync::Arc;

use dialog_common::helpers::{Provider, Service};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use super::{MemoryStore, UcanServiceAddress};
use crate::server::{Access, Answer, Request as AccessRequest};

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

type Body = http_body_util::Full<bytes::Bytes>;

fn respond(status: StatusCode) -> hyper::http::response::Builder {
    Response::builder()
        .status(status)
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "POST, OPTIONS")
        .header("Access-Control-Allow-Headers", "Content-Type, Accept")
        .header("Access-Control-Expose-Headers", "Content-Type, ETag")
        .header("Cache-Control", "no-store")
}

fn body(bytes: impl Into<bytes::Bytes>) -> Body {
    http_body_util::Full::new(bytes.into())
}

async fn handle(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> Result<Response<Body>, Infallible> {
    use http_body_util::BodyExt as _;

    if req.method() == Method::OPTIONS {
        return Ok(respond(StatusCode::NO_CONTENT).body(body("")).unwrap());
    }
    if req.method() != Method::POST {
        return Ok(respond(StatusCode::METHOD_NOT_ALLOWED)
            .body(body("Method not allowed"))
            .unwrap());
    }

    let accept = req
        .headers()
        .get("accept")
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
    let bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => {
            return Ok(respond(StatusCode::BAD_REQUEST)
                .body(body(format!("Failed to read body: {error}")))
                .unwrap());
        }
    };
    if bytes.len() as u64 > state.max_body_bytes {
        return Ok(too_large(state.max_body_bytes));
    }

    let mut request = AccessRequest::new(&bytes);
    if let Some(accept) = accept.as_deref() {
        request = request.accept(accept);
    }
    let response = match state.access.handle(request).await {
        Answer::Performed(response) => response,
        Answer::Refused(refusal) => refusal.into_response(),
        Answer::Unsupported => {
            return Ok(respond(StatusCode::NOT_ACCEPTABLE)
                .header("Content-Type", "application/json")
                .body(body(
                    r#"{"kind":"Unsupported","detail":"this service performs operations only; ask for the outcome"}"#,
                ))
                .unwrap());
        }
    };
    let mut builder = respond(StatusCode::from_u16(response.status).expect("a valid status"))
        .header("Content-Type", response.content_type);
    if let Some(version) = &response.version {
        builder = builder.header("ETag", format!("\"{version}\""));
    }
    Ok(builder.body(body(response.body)).unwrap())
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
