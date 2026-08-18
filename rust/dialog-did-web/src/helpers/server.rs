//! The native local HTTP server behind [`did_web_server`].
//!
//! Deliberately hand-rolled over a raw [`TcpListener`] rather than pulling an
//! HTTP-server framework: the three behaviors are static HTTP/1.1 responses
//! with no request parsing or routing, so a full server stack would be pure
//! dependency weight. It reads and discards the request, then writes a fixed
//! response.

use std::net::SocketAddr;

use dialog_common::helpers::{Provider, Service};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use super::{Behavior, DidWebServerAddress, DidWebServerSettings};

/// A running local did:web test server.
pub struct DidWebServer {
    shutdown: Option<oneshot::Sender<()>>,
}

#[async_trait::async_trait]
impl Provider for DidWebServer {
    async fn stop(mut self) -> anyhow::Result<()> {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

/// Provision a local HTTP server that applies `settings.behavior` to every
/// request, returning its `did.json` URL.
#[dialog_common::provider]
pub async fn did_web_server(
    settings: DidWebServerSettings,
) -> anyhow::Result<Service<DidWebServerAddress, DidWebServer>> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    let url = format!("http://{addr}/did.json");

    let (shutdown, mut shutdown_rx) = oneshot::channel::<()>();
    let response = render(&settings.behavior);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accepted = listener.accept() => {
                    let Ok((mut stream, _)) = accepted else { continue };
                    let response = response.clone();
                    tokio::spawn(async move {
                        // Read and discard the request head; we serve a fixed
                        // response regardless of what was asked for.
                        let mut buf = [0u8; 1024];
                        let _ = stream.read(&mut buf).await;
                        let _ = stream.write_all(&response).await;
                        let _ = stream.flush().await;
                    });
                }
            }
        }
    });

    Ok(Service::new(
        DidWebServerAddress { url },
        DidWebServer {
            shutdown: Some(shutdown),
        },
    ))
}

/// Render a [`Behavior`] into the raw HTTP/1.1 response bytes.
fn render(behavior: &Behavior) -> Vec<u8> {
    match behavior {
        Behavior::Serve(body) => http_response(200, "OK", &[], body),
        Behavior::Redirect(location) => {
            http_response(302, "Found", &[("Location", location.as_str())], &[])
        }
        Behavior::Sized { declared } => {
            // An explicit, honest Content-Length: exercises the pre-read check,
            // which refuses before the body is pulled.
            let len = declared.to_string();
            let body = vec![b'a'; *declared as usize];
            http_response(200, "OK", &[("Content-Length", len.as_str())], &body)
        }
        Behavior::Unsized { actual } => {
            // No Content-Length: the body ends when the connection closes. The
            // pre-read check sees no declared length, so the post-read check on
            // the actual bytes is the backstop.
            let body = vec![b'a'; *actual];
            http_response_no_length(200, "OK", &body)
        }
    }
}

/// A raw HTTP/1.1 response with NO `Content-Length`, body delimited by
/// `Connection: close` (HTTP/1.0-style framing). The client reads until EOF.
fn http_response_no_length(status: u16, reason: &str, body: &[u8]) -> Vec<u8> {
    let head = format!("HTTP/1.1 {status} {reason}\r\nConnection: close\r\n\r\n");
    let mut out = head.into_bytes();
    out.extend_from_slice(body);
    out
}

/// Assemble a raw HTTP/1.1 response. If no `Content-Length` header is supplied,
/// one is added for the body so the client reads exactly `body.len()` bytes.
fn http_response(status: u16, reason: &str, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    let mut head = format!("HTTP/1.1 {status} {reason}\r\n");
    let has_len = headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("content-length"));
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    if !has_len {
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    head.push_str("Connection: close\r\n\r\n");

    let mut out = head.into_bytes();
    out.extend_from_slice(body);
    out
}
