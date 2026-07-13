//! Client-side request execution over an established connection.
//!
//! One request per bidirectional stream. The caller supplies the CBOR UCAN
//! container (built at authorize time) and any payload body; these helpers
//! frame the exchange and decode the typed response.

use dialog_effects::blob::{BlobError, BlobSink, BlobSource};
use dialog_effects::memory::{Edition, Version};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use serde::de::DeserializeOwned;

use super::framing::{read_cbor, read_frame, write_cbor, write_end, write_frame};
use crate::IrohRemoteError;
use crate::protocol::{
    BlobImportResponse, BlobReadResponse, GetResponse, PublishResponse, RequestEnvelope,
    ResolveResponse, UnitResponse, WireEdition, WireError,
};

/// Open a stream and send the request envelope.
async fn open(
    connection: &Connection,
    invocation: Vec<u8>,
    body: Option<Vec<u8>>,
) -> Result<(SendStream, RecvStream), IrohRemoteError> {
    let (mut send, recv) = connection
        .open_bi()
        .await
        .map_err(|e| IrohRemoteError::Connection(format!("opening stream: {e}")))?;
    write_cbor(&mut send, &RequestEnvelope { invocation, body }).await?;
    Ok((send, recv))
}

/// Fire a single-frame request and decode the single-frame response.
async fn roundtrip<T: DeserializeOwned>(
    connection: &Connection,
    invocation: Vec<u8>,
    body: Option<Vec<u8>>,
) -> Result<T, IrohRemoteError> {
    let (mut send, mut recv) = open(connection, invocation, body).await?;
    send.finish()
        .map_err(|e| IrohRemoteError::Connection(format!("finishing request: {e}")))?;
    read_cbor(&mut recv).await
}

/// `/archive/get`
pub(crate) async fn archive_get(
    connection: &Connection,
    invocation: Vec<u8>,
) -> Result<Result<Option<Vec<u8>>, WireError>, IrohRemoteError> {
    let response: GetResponse = roundtrip(connection, invocation, None).await?;
    Ok(response.map(|block| block.map(serde_bytes::ByteBuf::into_vec)))
}

/// `/archive/put` — `body` is the block content.
pub(crate) async fn archive_put(
    connection: &Connection,
    invocation: Vec<u8>,
    block: Vec<u8>,
) -> Result<UnitResponse, IrohRemoteError> {
    roundtrip(connection, invocation, Some(block)).await
}

/// `/archive/import` — `blocks` are CBOR-encoded into the body.
pub(crate) async fn archive_import(
    connection: &Connection,
    invocation: Vec<u8>,
    blocks: Vec<serde_bytes::ByteBuf>,
) -> Result<UnitResponse, IrohRemoteError> {
    let body = serde_ipld_dagcbor::to_vec(&blocks)
        .map_err(|e| IrohRemoteError::Protocol(format!("encoding import body: {e}")))?;
    roundtrip(connection, invocation, Some(body)).await
}

/// `/memory/resolve`
pub(crate) async fn memory_resolve(
    connection: &Connection,
    invocation: Vec<u8>,
) -> Result<Result<Option<Edition<Vec<u8>>>, WireError>, IrohRemoteError> {
    let response: ResolveResponse = roundtrip(connection, invocation, None).await?;
    Ok(response.map(|edition| {
        edition.map(|WireEdition { content, version }| Edition { content, version })
    }))
}

/// `/memory/publish` — `content` is the cell content.
pub(crate) async fn memory_publish(
    connection: &Connection,
    invocation: Vec<u8>,
    content: Vec<u8>,
) -> Result<Result<Version, WireError>, IrohRemoteError> {
    let response: PublishResponse = roundtrip(connection, invocation, Some(content)).await?;
    Ok(response)
}

/// `/memory/retract`
pub(crate) async fn memory_retract(
    connection: &Connection,
    invocation: Vec<u8>,
) -> Result<UnitResponse, IrohRemoteError> {
    roundtrip(connection, invocation, None).await
}

/// `/archive/blob/read` — returns a streaming source over the response
/// chunk frames.
pub(crate) async fn blob_read(
    connection: &Connection,
    invocation: Vec<u8>,
) -> Result<Result<RemoteBlobSource, WireError>, IrohRemoteError> {
    let (mut send, mut recv) = open(connection, invocation, None).await?;
    send.finish()
        .map_err(|e| IrohRemoteError::Connection(format!("finishing request: {e}")))?;
    let header: BlobReadResponse = read_cbor(&mut recv).await?;
    match header {
        Ok(()) => Ok(Ok(RemoteBlobSource { recv, done: false })),
        Err(error) => Ok(Err(error)),
    }
}

/// A [`BlobSource`] streaming chunk frames from the peer.
pub struct RemoteBlobSource {
    recv: RecvStream,
    done: bool,
}

#[async_trait::async_trait]
impl BlobSource for RemoteBlobSource {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        if self.done {
            return Ok(None);
        }
        match read_frame(&mut self.recv).await {
            Ok(Some(chunk)) => Ok(Some(chunk)),
            Ok(None) => {
                self.done = true;
                Ok(None)
            }
            Err(e) => Err(BlobError::Io(e.to_string())),
        }
    }
}

/// `/archive/blob/import` — returns a streaming sink writing chunk frames
/// to the peer; `finish` reads the committed digest.
pub(crate) async fn blob_import(
    connection: &Connection,
    invocation: Vec<u8>,
) -> Result<RemoteBlobSink, IrohRemoteError> {
    let (send, recv) = open(connection, invocation, None).await?;
    Ok(RemoteBlobSink { send, recv })
}

/// A [`BlobSink`] writing chunk frames to the peer.
pub struct RemoteBlobSink {
    send: SendStream,
    recv: RecvStream,
}

#[async_trait::async_trait]
impl BlobSink for RemoteBlobSink {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        for chunk in bytes.chunks(crate::protocol::BLOB_CHUNK_SIZE) {
            write_frame(&mut self.send, chunk)
                .await
                .map_err(|e| BlobError::Io(e.to_string()))?;
        }
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<dialog_common::Blake3Hash, BlobError> {
        write_end(&mut self.send)
            .await
            .map_err(|e| BlobError::Io(e.to_string()))?;
        self.send
            .finish()
            .map_err(|e| BlobError::Io(format!("finishing import stream: {e}")))?;
        let response: BlobImportResponse = read_cbor(&mut self.recv)
            .await
            .map_err(|e| BlobError::Io(e.to_string()))?;
        let imported = response.map_err(|e| match e {
            WireError::Denied(e) => BlobError::AuthorizationError(e),
            other => BlobError::ExecutionError(other.to_string()),
        })?;
        let digest: [u8; 32] = imported.digest.as_slice().try_into().map_err(|_| {
            BlobError::ExecutionError("peer returned a malformed digest".to_string())
        })?;
        Ok(dialog_common::Blake3Hash::from(digest))
    }
}
