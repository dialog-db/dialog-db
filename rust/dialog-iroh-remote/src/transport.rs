//! The [`Channel`] iroh actually backs, and the loop that answers one.
//!
//! Everything else in this crate is deliberately transport-free: the
//! protocol is a container in and a [`Response`] out, and the tests
//! prove it over a function call. This module is where that meets QUIC,
//! and it is the only place that knows it.
//!
//! # The endpoint is supplied, not built
//!
//! An [`Endpoint`] carries the choices that decide whether two peers can
//! reach each other at all — which relays to use, how an endpoint id is
//! resolved to a route, which crypto provider signs the TLS — and iroh
//! bundles them as presets because they are deployment decisions. They
//! are the embedder's for the same reason the channel itself is: made
//! once, for a process, not per remote. So both halves here take an
//! endpoint that is already bound and neither reaches for a default.
//!
//! # One exchange is one stream
//!
//! The dialer opens a bidirectional stream, writes the container,
//! finishes its side, and reads until the peer finishes its own. QUIC
//! supplies framing, ordering and independent cancellation per stream,
//! which is why [`crate::wire`] adds none of them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointId};

use dialog_capability::Provider;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_did_web::Resolve;
use dialog_ucan_core::revocation::RevocationChecker;

use crate::channel::{Channel, ChannelError};
use crate::serve::{Responder, Store};
use crate::site::IrohAddress;
use crate::wire::encode;

/// What this protocol is called on the wire.
///
/// Versioned, and checked by iroh during the handshake rather than by
/// anything here: a peer speaking a different version is refused at
/// connection time, which is the earliest a mismatch can be caught and
/// well before either side has read a byte the other wrote.
pub const ALPN: &[u8] = b"dialog/remote/1";

/// How much of an answer to read before giving up on it.
///
/// A peer is a stranger, and `read_to_end` with no bound is that
/// stranger choosing how much memory this process allocates. The cap is
/// generous enough for any single block or cell an effect answers with
/// and finite, which is the part that matters.
pub const MAX_RESPONSE: usize = 64 * 1024 * 1024;

/// Reaches peers over an iroh [`Endpoint`].
///
/// Connections are kept and reused per peer. Dialing is not free — a
/// QUIC handshake, possibly a relay round trip and a hole punch — and a
/// repository sync is thousands of small exchanges with the same peer,
/// so a connection per exchange would put that cost in front of every
/// block.
#[derive(Debug, Clone)]
pub struct IrohChannel {
    endpoint: Endpoint,
    connections: Arc<Mutex<HashMap<EndpointId, Connection>>>,
}

impl IrohChannel {
    /// Dial peers from `endpoint`.
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The endpoint this dials from, whose address is what a peer needs
    /// to dial back.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// A live connection to `peer`, reused if there is one.
    async fn connect(&self, peer: &IrohAddress) -> Result<Connection, ChannelError> {
        // A cached connection may have died since it was stored, and a
        // closed one fails every stream opened on it. Asking is cheap;
        // discovering it through a failed exchange is not.
        let cached = self
            .connections
            .lock()
            .expect("not poisoned")
            .get(peer.endpoint())
            .filter(|connection| connection.close_reason().is_none())
            .cloned();
        if let Some(connection) = cached {
            return Ok(connection);
        }

        let connection = self
            .endpoint
            .connect(peer.addr().clone(), ALPN)
            .await
            .map_err(|error| ChannelError::Unreachable {
                peer: peer.to_string(),
                detail: error.to_string(),
            })?;

        // Two exchanges starting at once can both dial; the loser's
        // connection is dropped rather than leaked, and neither request
        // is affected.
        self.connections
            .lock()
            .expect("not poisoned")
            .insert(*peer.endpoint(), connection.clone());

        Ok(connection)
    }
}

#[async_trait::async_trait]
impl Channel for IrohChannel {
    async fn exchange(
        &self,
        peer: &IrohAddress,
        request: Vec<u8>,
    ) -> Result<Vec<u8>, ChannelError> {
        let connection = self.connect(peer).await?;

        let interrupted = |error: &dyn std::fmt::Display| ChannelError::Interrupted {
            peer: peer.to_string(),
            detail: error.to_string(),
        };

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .map_err(|error| interrupted(&error))?;
        send.write_all(&request)
            .await
            .map_err(|error| interrupted(&error))?;
        // Finishing is what makes the peer's `read_to_end` return, so
        // the exchange deadlocks without it rather than merely leaking.
        send.finish().map_err(|error| interrupted(&error))?;

        recv.read_to_end(MAX_RESPONSE)
            .await
            .map_err(|error| interrupted(&error))
    }
}

/// Answer invocations arriving on `endpoint` until it closes.
///
/// Returns when the endpoint stops accepting, which is what
/// [`Endpoint::close`] causes — so a caller shuts this down by closing
/// the endpoint it handed over, not by aborting the task, and gets the
/// graceful close of every live connection as part of it.
///
/// The endpoint must have been bound with [`ALPN`] among its `alpns`, or
/// iroh rejects every connection before this sees it.
pub async fn accept<S, Resolver, Revocations>(
    endpoint: Endpoint,
    responder: Arc<Responder<S, Resolver, Revocations>>,
) where
    S: Store + ConditionalSend + 'static,
    Resolver: Provider<Resolve> + ConditionalSync + 'static,
    Revocations: RevocationChecker + ConditionalSync + 'static,
{
    while let Some(incoming) = endpoint.accept().await {
        let responder = responder.clone();
        // Per connection, so one peer that stalls mid-request does not
        // hold up every other peer behind it.
        tokio::spawn(async move {
            let connection = match incoming.await {
                Ok(connection) => connection,
                // A handshake that failed produced no request, so there
                // is nobody to answer and nothing to record but this.
                Err(error) => {
                    tracing::debug!(%error, "a connection was not established");
                    return;
                }
            };
            serve_connection(connection, responder).await;
        });
    }
}

/// Answer every exchange on one connection.
async fn serve_connection<S, Resolver, Revocations>(
    connection: Connection,
    responder: Arc<Responder<S, Resolver, Revocations>>,
) where
    S: Store + ConditionalSend + 'static,
    Resolver: Provider<Resolve> + ConditionalSync + 'static,
    Revocations: RevocationChecker + ConditionalSync + 'static,
{
    let peer = connection.remote_id();

    // A connection is reused for many exchanges, so this accepts streams
    // until the dialer closes it rather than answering once and hanging
    // up. `accept_bi` ends with an error when it does, which is the
    // ordinary way out and not worth reporting as a failure.
    loop {
        let (mut send, mut recv) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(error) => {
                tracing::trace!(%peer, %error, "a peer stopped sending");
                return;
            }
        };

        let request = match recv.read_to_end(MAX_RESPONSE).await {
            Ok(request) => request,
            // Nothing was fully received, so nothing can be answered:
            // the stream is abandoned and the connection stays up.
            Err(error) => {
                tracing::debug!(%peer, %error, "a request did not arrive whole");
                continue;
            }
        };

        let response = responder.answer(&request).await;
        let encoded = match encode("response", &response) {
            Ok(encoded) => encoded,
            // The responder's own answer would not encode. That is this
            // peer's bug, not the caller's, and there is no honest way
            // to report it in a protocol the caller can read.
            Err(error) => {
                tracing::error!(%peer, %error, "an answer could not be encoded");
                continue;
            }
        };

        if let Err(error) = send.write_all(&encoded).await {
            tracing::debug!(%peer, %error, "an answer could not be sent");
            continue;
        }
        if let Err(error) = send.finish() {
            tracing::debug!(%peer, %error, "an answer was not finished");
        }
    }
}

#[cfg(test)]
mod tests;
