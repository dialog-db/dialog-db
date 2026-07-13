//! Length-prefixed frame I/O over QUIC streams.
//!
//! Every frame is a u32-BE length followed by that many bytes. CBOR frames
//! wrap DAG-CBOR values; chunk frames (blob streaming) are raw bytes with a
//! zero-length frame as end-of-stream marker.

use crate::IrohRemoteError;
use crate::protocol::MAX_FRAME_SIZE;
use iroh::endpoint::{RecvStream, SendStream};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Write one raw frame.
pub(crate) async fn write_frame(
    stream: &mut SendStream,
    bytes: &[u8],
) -> Result<(), IrohRemoteError> {
    let length = u32::try_from(bytes.len())
        .ok()
        .filter(|length| *length <= MAX_FRAME_SIZE)
        .ok_or_else(|| {
            IrohRemoteError::Protocol(format!("frame of {} bytes exceeds limit", bytes.len()))
        })?;
    stream
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|e| IrohRemoteError::Connection(format!("writing frame length: {e}")))?;
    stream
        .write_all(bytes)
        .await
        .map_err(|e| IrohRemoteError::Connection(format!("writing frame: {e}")))?;
    Ok(())
}

/// Read one raw frame. Returns `None` on a zero-length (end-of-stream)
/// frame.
pub(crate) async fn read_frame(
    stream: &mut RecvStream,
) -> Result<Option<Vec<u8>>, IrohRemoteError> {
    let mut length_bytes = [0u8; 4];
    stream
        .read_exact(&mut length_bytes)
        .await
        .map_err(|e| IrohRemoteError::Connection(format!("reading frame length: {e}")))?;
    let length = u32::from_be_bytes(length_bytes);
    if length == 0 {
        return Ok(None);
    }
    if length > MAX_FRAME_SIZE {
        return Err(IrohRemoteError::Protocol(format!(
            "declared frame of {length} bytes exceeds limit"
        )));
    }
    let mut bytes = vec![0u8; length as usize];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|e| IrohRemoteError::Connection(format!("reading frame: {e}")))?;
    Ok(Some(bytes))
}

/// Write the zero-length end-of-stream marker.
pub(crate) async fn write_end(stream: &mut SendStream) -> Result<(), IrohRemoteError> {
    stream
        .write_all(&0u32.to_be_bytes())
        .await
        .map_err(|e| IrohRemoteError::Connection(format!("writing end frame: {e}")))
}

/// Write one DAG-CBOR frame.
pub(crate) async fn write_cbor<T: Serialize>(
    stream: &mut SendStream,
    value: &T,
) -> Result<(), IrohRemoteError> {
    let bytes = serde_ipld_dagcbor::to_vec(value)
        .map_err(|e| IrohRemoteError::Protocol(format!("encoding frame: {e}")))?;
    write_frame(stream, &bytes).await
}

/// Read one DAG-CBOR frame; end-of-stream is a protocol error here.
pub(crate) async fn read_cbor<T: DeserializeOwned>(
    stream: &mut RecvStream,
) -> Result<T, IrohRemoteError> {
    let bytes = read_frame(stream)
        .await?
        .ok_or_else(|| IrohRemoteError::Protocol("unexpected end-of-stream frame".into()))?;
    serde_ipld_dagcbor::from_slice(&bytes)
        .map_err(|e| IrohRemoteError::Protocol(format!("decoding frame: {e}")))
}
