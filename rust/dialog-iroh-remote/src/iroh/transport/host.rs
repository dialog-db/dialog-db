//! Serving spaces over the iroh remote protocol.
//!
//! [`HostRegistry`] is the [`ProtocolHandler`] accepted on
//! [`ALPN`](crate::protocol::ALPN): per request stream it parses and
//! verifies the UCAN container — the same checks the UCAN-S3 access
//! service runs (signatures, delegation chain walk, command-prefix and
//! policy checks) — then routes to the [`SpaceHost`] serving the
//! invocation's subject.
//!
//! [`SpaceHost`] reconstructs the effect from the *verified* invocation
//! arguments plus the request body, cross-checks the body against the
//! signed content bindings (digests/checksums), performs the effect
//! against its local storage environment, and encodes the response. The
//! peer is its own access service and its own store.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use dialog_capability::{Capability, Did, Provider, Subject};
use dialog_common::{Blake3Hash, Buffer, Checksum};
use dialog_credentials::Ed25519KeyResolver;
use dialog_effects::memory::MemoryError;
use dialog_effects::{archive, blob, memory};
use dialog_ucan_core::InvocationChain;
use dialog_ucan_core::promise::Promised;
use ipld_core::ipld::Ipld;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler};
use serde::de::DeserializeOwned;

use super::framing::{read_cbor, read_frame, write_cbor, write_end, write_frame};
use super::swarm::SwarmHandle;
use crate::IrohRemoteError;
use crate::protocol::{
    BlobImportResponse, BlobImported, BlobReadResponse, GetResponse, PublishResponse,
    RequestEnvelope, ResolveResponse, UnitResponse, WireEdition, WireError,
};

type Args = BTreeMap<String, Promised>;

/// The storage environment a [`SpaceHost`] serves from: a local provider
/// of the full effect vocabulary the remote protocol exposes — the same
/// environment a `Repository` over the replica uses.
pub trait SpaceStorage:
    Provider<archive::Get>
    + Provider<archive::Put>
    + Provider<archive::Import>
    + Provider<memory::Resolve>
    + Provider<memory::Publish>
    + Provider<memory::Retract>
    + Provider<blob::Read>
    + Provider<blob::Import>
    + Send
    + Sync
    + 'static
{
}

impl<Env> SpaceStorage for Env where
    Env: Provider<archive::Get>
        + Provider<archive::Put>
        + Provider<archive::Import>
        + Provider<memory::Resolve>
        + Provider<memory::Publish>
        + Provider<memory::Retract>
        + Provider<blob::Read>
        + Provider<blob::Import>
        + Send
        + Sync
        + 'static
{
}

/// A request whose UCAN container has been parsed and verified, ready for
/// dispatch to the host serving its subject.
pub struct VerifiedRequest {
    /// The invocation's subject DID.
    pub subject: Did,
    /// The invocation's command path segments (e.g. `["archive", "get"]`).
    pub segments: Vec<String>,
    /// The invocation's (flat) argument map.
    pub args: Args,
    /// The request body, if the effect carries payload bytes.
    pub body: Option<Vec<u8>>,
}

/// Object-safe host interface the registry and the swarm responder use.
#[async_trait]
pub trait SubjectHost: Send + Sync + 'static {
    /// Perform a verified request, writing the response frames to `send`
    /// (and consuming chunk frames from `recv` for streaming imports).
    async fn handle(
        &self,
        request: VerifiedRequest,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> Result<(), IrohRemoteError>;

    /// Whether this host's replica holds the given block (swarm responder).
    async fn has_block(&self, catalog: &str, digest: &Blake3Hash) -> bool;

    /// Wire up the swarm handle so the host can announce published
    /// revisions. Called by the node when the swarm is joined.
    fn attach_swarm(&self, swarm: Arc<SwarmHandle>);
}

/// Serves a single space (subject) from a local storage environment.
pub struct SpaceHost<Env> {
    subject: Did,
    env: Env,
    swarm: OnceLock<Arc<SwarmHandle>>,
}

impl<Env: SpaceStorage> SpaceHost<Env> {
    /// Create a host serving `subject` from `env`.
    pub fn new(subject: Did, env: Env) -> Self {
        Self {
            subject,
            env,
            swarm: OnceLock::new(),
        }
    }

    /// The subject this host serves.
    pub fn subject(&self) -> &Did {
        &self.subject
    }
}

/// Deserialize a typed value from the flat UCAN args map via IPLD
/// round-trip; unknown fields are ignored, so this works on the map
/// containing fields from every capability chain layer.
fn from_args<T: DeserializeOwned>(args: &Args) -> Result<T, WireError> {
    let map: BTreeMap<String, Ipld> = args
        .iter()
        .map(|(key, value)| {
            Ipld::try_from(value)
                .map(|ipld| (key.clone(), ipld))
                .map_err(|e| WireError::Rejected(format!("unresolved promise for '{key}': {e}")))
        })
        .collect::<Result<_, _>>()?;
    ipld_core::serde::from_ipld(Ipld::Map(map))
        .map_err(|e| WireError::Rejected(format!("malformed arguments: {e}")))
}

fn require_body(body: &Option<Vec<u8>>) -> Result<&[u8], WireError> {
    body.as_deref()
        .ok_or_else(|| WireError::Rejected("missing request body".into()))
}

fn execution(e: impl std::fmt::Display) -> WireError {
    WireError::Execution(e.to_string())
}

fn memory_error(e: MemoryError) -> WireError {
    match e {
        MemoryError::VersionMismatch { expected, actual } => {
            WireError::VersionMismatch { expected, actual }
        }
        other => WireError::Execution(other.to_string()),
    }
}

impl<Env: SpaceStorage> SpaceHost<Env> {
    fn archive_capability<Fx>(&self, args: &Args, effect: Fx) -> Result<Capability<Fx>, WireError>
    where
        Fx: dialog_capability::Effect<Of = archive::Catalog>,
    {
        let catalog: archive::Catalog = from_args(args)?;
        Ok(Subject::from(self.subject.clone())
            .attenuate(archive::Archive)
            .attenuate(catalog)
            .invoke(effect))
    }

    fn memory_capability<Fx>(&self, args: &Args, effect: Fx) -> Result<Capability<Fx>, WireError>
    where
        Fx: dialog_capability::Effect<Of = memory::Cell>,
    {
        let space: memory::Space = from_args(args)?;
        let cell: memory::Cell = from_args(args)?;
        Ok(Subject::from(self.subject.clone())
            .attenuate(memory::Memory)
            .attenuate(space)
            .attenuate(cell)
            .invoke(effect))
    }

    fn blob_capability<Fx>(&self, _args: &Args, effect: Fx) -> Result<Capability<Fx>, WireError>
    where
        Fx: dialog_capability::Effect<Of = blob::Blob>,
    {
        Ok(Subject::from(self.subject.clone())
            .attenuate(archive::Archive)
            .attenuate(blob::Blob)
            .invoke(effect))
    }

    async fn archive_get(&self, request: &VerifiedRequest) -> GetResponse {
        let get: archive::Get = from_args(&request.args)?;
        let capability = self.archive_capability(&request.args, get)?;
        Provider::<archive::Get>::execute(&self.env, capability)
            .await
            .map(|block| block.map(serde_bytes::ByteBuf::from))
            .map_err(execution)
    }

    async fn archive_put(&self, request: &VerifiedRequest) -> UnitResponse {
        let attenuation: <archive::Put as dialog_capability::Attenuate>::Attenuation =
            from_args(&request.args)?;
        let body = require_body(&request.body)?;
        let put = archive::Put::new(Buffer::from(body));
        // The body must be the exact content the invocation signed over.
        if put.block.blake3_hash() != &attenuation.digest {
            return Err(WireError::Rejected(format!(
                "body digest {} does not match signed digest {}",
                put.block.blake3_hash(),
                attenuation.digest
            )));
        }
        let capability = self.archive_capability(&request.args, put)?;
        Provider::<archive::Put>::execute(&self.env, capability)
            .await
            .map_err(execution)
    }

    async fn archive_import(&self, request: &VerifiedRequest) -> UnitResponse {
        let attenuation: <archive::Import as dialog_capability::Attenuate>::Attenuation =
            from_args(&request.args)?;
        let body = require_body(&request.body)?;
        let blocks: Vec<serde_bytes::ByteBuf> = serde_ipld_dagcbor::from_slice(body)
            .map_err(|e| WireError::Rejected(format!("malformed import body: {e}")))?;
        if blocks.len() != attenuation.checksums.len() {
            return Err(WireError::Rejected(format!(
                "import carries {} blocks but {} were signed",
                blocks.len(),
                attenuation.checksums.len()
            )));
        }
        for (block, checksum) in blocks.iter().zip(&attenuation.checksums) {
            if &Checksum::sha256(block.as_ref()) != checksum {
                return Err(WireError::Rejected(
                    "import block does not match its signed checksum".into(),
                ));
            }
        }
        let import = archive::Import::new(blocks.into_iter().map(|b| b.into_vec()));
        let capability = self.archive_capability(&request.args, import)?;
        Provider::<archive::Import>::execute(&self.env, capability)
            .await
            .map_err(execution)
    }

    async fn memory_resolve(&self, request: &VerifiedRequest) -> ResolveResponse {
        let capability = self.memory_capability(&request.args, memory::Resolve)?;
        Provider::<memory::Resolve>::execute(&self.env, capability)
            .await
            .map(|edition| {
                edition.map(|edition| WireEdition {
                    content: edition.content,
                    version: edition.version,
                })
            })
            .map_err(memory_error)
    }

    async fn memory_publish(&self, request: &VerifiedRequest) -> PublishResponse {
        let attenuation: <memory::Publish as dialog_capability::Attenuate>::Attenuation =
            from_args(&request.args)?;
        let body = require_body(&request.body)?;
        if Checksum::sha256(body) != attenuation.checksum {
            return Err(WireError::Rejected(
                "body does not match the signed publish checksum".into(),
            ));
        }
        let publish = memory::Publish::new(body.to_vec(), attenuation.when);
        let capability = self.memory_capability(&request.args, publish)?;
        let version = Provider::<memory::Publish>::execute(&self.env, capability)
            .await
            .map_err(memory_error)?;

        // A peer just moved a cell on this device: wake local subscribers
        // and announce it to the swarm so other peers can pull reactively.
        if let Some(swarm) = self.swarm.get()
            && let Ok(space) = from_args::<memory::Space>(&request.args)
            && let Ok(cell) = from_args::<memory::Cell>(&request.args)
        {
            swarm
                .published(space.space, cell.cell, version.clone())
                .await;
        }

        Ok(version)
    }

    async fn memory_retract(&self, request: &VerifiedRequest) -> UnitResponse {
        let retract: memory::Retract = from_args(&request.args)?;
        let capability = self.memory_capability(&request.args, retract)?;
        Provider::<memory::Retract>::execute(&self.env, capability)
            .await
            .map_err(memory_error)
    }

    async fn blob_read(
        &self,
        request: &VerifiedRequest,
        send: &mut SendStream,
    ) -> Result<(), IrohRemoteError> {
        let reader = async {
            let read: blob::Read = from_args(&request.args)?;
            let capability = self.blob_capability(&request.args, read)?;
            Provider::<blob::Read>::execute(&self.env, capability)
                .await
                .map_err(execution)
        }
        .await;

        let mut reader = match reader {
            Ok(reader) => {
                write_cbor(send, &BlobReadResponse::Ok(())).await?;
                reader
            }
            Err(error) => {
                return write_cbor(send, &BlobReadResponse::Err(error)).await;
            }
        };

        loop {
            match reader.next().await {
                // Zero-length frames terminate the stream, so empty chunks
                // must be skipped rather than written.
                Ok(Some(chunk)) if chunk.is_empty() => continue,
                Ok(Some(chunk)) => write_frame(send, &chunk).await?,
                Ok(None) => return write_end(send).await,
                Err(e) => {
                    // The header already said Ok; the only honest signal
                    // left is to abort the stream.
                    return Err(IrohRemoteError::Protocol(format!(
                        "blob read failed mid-stream: {e}"
                    )));
                }
            }
        }
    }

    async fn blob_import(
        &self,
        request: &VerifiedRequest,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> Result<(), IrohRemoteError> {
        let sink = async {
            let import: blob::Import = from_args(&request.args)?;
            let capability = self.blob_capability(&request.args, import)?;
            Provider::<blob::Import>::execute(&self.env, capability)
                .await
                .map_err(execution)
        }
        .await;

        let mut sink = match sink {
            Ok(sink) => sink,
            Err(error) => {
                return write_cbor(send, &BlobImportResponse::Err(error)).await;
            }
        };

        let response: BlobImportResponse = async {
            while let Some(chunk) = read_frame(recv)
                .await
                .map_err(|e| WireError::Execution(e.to_string()))?
            {
                sink.write_all(&chunk).await.map_err(execution)?;
            }
            let digest = sink.finish().await.map_err(execution)?;
            Ok(BlobImported {
                digest: digest.as_bytes().to_vec(),
            })
        }
        .await;

        write_cbor(send, &response).await
    }
}

#[async_trait]
impl<Env: SpaceStorage> SubjectHost for SpaceHost<Env> {
    async fn handle(
        &self,
        request: VerifiedRequest,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> Result<(), IrohRemoteError> {
        let segments: Vec<&str> = request.segments.iter().map(String::as_str).collect();
        match segments.as_slice() {
            ["archive", "get"] => write_cbor(send, &self.archive_get(&request).await).await,
            ["archive", "put"] => write_cbor(send, &self.archive_put(&request).await).await,
            ["archive", "import"] => write_cbor(send, &self.archive_import(&request).await).await,
            ["memory", "resolve"] => write_cbor(send, &self.memory_resolve(&request).await).await,
            ["memory", "publish"] => write_cbor(send, &self.memory_publish(&request).await).await,
            ["memory", "retract"] => write_cbor(send, &self.memory_retract(&request).await).await,
            ["archive", "blob", "read"] => self.blob_read(&request, send).await,
            ["archive", "blob", "import"] => self.blob_import(&request, send, recv).await,
            other => {
                write_cbor(
                    send,
                    &UnitResponse::Err(WireError::Rejected(format!(
                        "unsupported command: /{}",
                        other.join("/")
                    ))),
                )
                .await
            }
        }
    }

    async fn has_block(&self, catalog: &str, digest: &Blake3Hash) -> bool {
        let capability = Subject::from(self.subject.clone())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new(catalog))
            .invoke(archive::Get::new(digest.clone()));
        matches!(
            Provider::<archive::Get>::execute(&self.env, capability).await,
            Ok(Some(_))
        )
    }

    fn attach_swarm(&self, swarm: Arc<SwarmHandle>) {
        let _ = self.swarm.set(swarm);
    }
}

/// [`ProtocolHandler`] for the dialog remote ALPN: verifies each request's
/// UCAN container and routes it to the host serving its subject.
#[derive(Clone)]
pub struct HostRegistry {
    hosts: Arc<HashMap<Did, Arc<dyn SubjectHost>>>,
}

impl HostRegistry {
    pub(crate) fn new(hosts: Arc<HashMap<Did, Arc<dyn SubjectHost>>>) -> Self {
        Self { hosts }
    }

    /// Parse and verify the envelope into a [`VerifiedRequest`] plus the
    /// host serving its subject.
    async fn verify(
        &self,
        envelope: RequestEnvelope,
    ) -> Result<(Arc<dyn SubjectHost>, VerifiedRequest), WireError> {
        let chain = InvocationChain::try_from(envelope.invocation.as_slice())
            .map_err(|e| WireError::Denied(format!("malformed invocation: {e}")))?;
        chain
            .verify(&Ed25519KeyResolver)
            .await
            .map_err(|e| WireError::Denied(format!("invocation failed verification: {e}")))?;

        let subject = chain.subject().clone();
        let host =
            self.hosts.get(&subject).cloned().ok_or_else(|| {
                WireError::Rejected(format!("this peer does not serve {subject}"))
            })?;

        let segments = chain.command().0.clone();
        let args = chain.arguments().clone();

        Ok((
            host,
            VerifiedRequest {
                subject,
                segments,
                args,
                body: envelope.body,
            },
        ))
    }

    async fn handle_stream(
        &self,
        mut send: SendStream,
        mut recv: RecvStream,
    ) -> Result<(), IrohRemoteError> {
        let envelope: RequestEnvelope = read_cbor(&mut recv).await?;
        match self.verify(envelope).await {
            Ok((host, request)) => host.handle(request, &mut send, &mut recv).await?,
            Err(error) => {
                // Error frames encode identically for every response type.
                write_cbor(&mut send, &UnitResponse::Err(error)).await?;
            }
        }
        send.finish()
            .map_err(|e| IrohRemoteError::Connection(format!("finishing response: {e}")))?;
        Ok(())
    }
}

impl std::fmt::Debug for HostRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostRegistry")
            .field("subjects", &self.hosts.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ProtocolHandler for HostRegistry {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        loop {
            match connection.accept_bi().await {
                Ok((send, recv)) => {
                    let registry = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = registry.handle_stream(send, recv).await {
                            tracing::debug!("iroh remote request failed: {e}");
                        }
                    });
                }
                // The peer closing the connection ends the accept loop.
                Err(_) => return Ok(()),
            }
        }
    }
}
