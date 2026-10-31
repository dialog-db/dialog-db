//! Following a peer's cell, which answers many times on one stream.
//!
//! A watch sends its container and nothing more, like a blob read, and
//! the peer answers with a frame per state: the cell as it stands, then
//! each new state, coalesced to the newest. The stream finishes when
//! the watch ends. A refusal in place of a state is the peer ending it,
//! which is how a watch whose grant was revoked hears so.
//!
//! Dropping the [`Editions`](memory::Editions) drops the stream, and the
//! peer stops following when it sees that.

use dialog_capability::{ForkInvocation, Provider};
use dialog_effects::Rejection;
use dialog_effects::memory::{self, CellState, EditionSource, MemoryError};

use super::provider::refused;
use crate::channel::{ChannelError, Transfer};
use crate::site::Iroh;
use crate::wire::{MAX_FRAME, Refusal, Response, decode};

fn broken(error: ChannelError) -> MemoryError {
    match error {
        // A channel that does not stream cannot carry a watch, however
        // often it is asked: say so, and the caller fetches instead.
        ChannelError::Unsupported { .. } => MemoryError::from(Rejection::Unsupported {
            reason: error.to_string(),
        }),
        error => MemoryError::from(Rejection::unavailable(error.to_string())),
    }
}

/// The peer's states as they arrive.
struct PeerEditions {
    transfer: Box<dyn Transfer>,
    /// Bytes received past the last frame read.
    pending: Vec<u8>,
    /// The state the peer opened with, not yet handed out.
    first: Option<CellState>,
    ended: bool,
}

impl PeerEditions {
    /// Bytes until `pending` holds `len`, or `false` when the peer
    /// finished with nothing pending: the clean end between frames.
    async fn fill(&mut self, len: usize) -> Result<bool, MemoryError> {
        while self.pending.len() < len {
            match self.transfer.recv().await.map_err(broken)? {
                Some(chunk) => self.pending.extend_from_slice(&chunk),
                None if self.pending.is_empty() => return Ok(false),
                None => {
                    return Err(broken(ChannelError::Interrupted {
                        peer: String::new(),
                        detail: "the peer stopped part-way through a state".into(),
                    }));
                }
            }
        }
        Ok(true)
    }

    /// The next state the peer sent, `None` once it finished.
    async fn frame(&mut self) -> Result<Option<CellState>, MemoryError> {
        if !self.fill(4).await? {
            return Ok(None);
        }
        let len = u32::from_be_bytes([
            self.pending[0],
            self.pending[1],
            self.pending[2],
            self.pending[3],
        ]) as usize;
        if len > MAX_FRAME {
            return Err(MemoryError::from(Rejection::Unclassified {
                detail: format!("the peer sent a {len}-byte state, past {MAX_FRAME}"),
            }));
        }
        if !self.fill(4 + len).await? {
            return Ok(None);
        }
        let body: Vec<u8> = self.pending.drain(..4 + len).skip(4).collect();
        state(&body).map(Some)
    }
}

/// One frame of a watch's answer, as the cell's state.
fn state(frame: &[u8]) -> Result<CellState, MemoryError> {
    match decode::<Response>("response", frame) {
        Ok(Response::Performed(output)) => {
            decode::<Result<CellState, MemoryError>>("cell state", &output).unwrap_or_else(
                |error| {
                    Err(MemoryError::from(Rejection::Unclassified {
                        detail: format!("the peer's answer did not fit the command: {error}"),
                    }))
                },
            )
        }
        // A peer that does not know the watch command (one from before
        // watches, or one serving without streams) will never follow the
        // cell: the caller fetches instead.
        Ok(Response::Refused(Refusal::UnknownCommand(command))) => {
            Err(MemoryError::from(Rejection::Unsupported {
                reason: format!("the peer does not serve {command}"),
            }))
        }
        Ok(Response::Refused(refusal)) => Err(refused(refusal)),
        Err(error) => Err(MemoryError::from(Rejection::Unclassified {
            detail: format!("the peer did not answer in this protocol: {error}"),
        })),
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl EditionSource for PeerEditions {
    async fn next(&mut self) -> Result<Option<CellState>, MemoryError> {
        if let Some(first) = self.first.take() {
            return Ok(Some(first));
        }
        if self.ended {
            return Ok(None);
        }
        let next = self.frame().await;
        // Whatever ended the watch — the peer finishing, a refusal, a
        // broken stream — is said once; after it there is nothing more.
        if !matches!(next, Ok(Some(_))) {
            self.ended = true;
        }
        next
    }
}

#[dialog_capability::provider]
impl Provider<ForkInvocation<Iroh, memory::Watch>> for Iroh {
    async fn execute(
        &self,
        invocation: ForkInvocation<Iroh, memory::Watch>,
    ) -> Result<memory::Editions, MemoryError> {
        let ForkInvocation {
            address,
            authorization,
            ..
        } = invocation;

        let connection = self.connection().await.map_err(broken)?;
        let mut transfer = match connection.open(&address, authorization.into_bytes()).await {
            Ok(transfer) => transfer,
            Err(error) => {
                self.broke(&connection, &error).await;
                return Err(broken(error));
            }
        };
        // Nothing follows a watch's container.
        transfer.finish().await.map_err(broken)?;

        let mut editions = PeerEditions {
            transfer,
            pending: Vec::new(),
            first: None,
            ended: false,
        };
        // The opening state is read here, so a watch the peer refuses
        // fails as the effect rather than as its first edition.
        match editions.frame().await? {
            Some(first) => editions.first = Some(first),
            None => {
                return Err(broken(ChannelError::Interrupted {
                    peer: address.to_string(),
                    detail: "the peer finished without answering the watch".into(),
                }));
            }
        }
        Ok(Box::new(editions))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::encode;

    /// A peer from before watches refuses the command by name; replication
    /// must hear that as unsupported, so it fetches instead.
    #[test]
    fn a_peer_that_does_not_know_the_watch_is_unsupported() {
        let refusal = Response::Refused(Refusal::UnknownCommand("/memory/watch".into()));
        let frame = encode("response", &refusal).expect("encodes");
        assert!(matches!(
            state(&frame),
            Err(MemoryError::Rejected(Rejection::Unsupported { .. }))
        ));
    }

    /// A channel that does not stream cannot carry a watch.
    #[test]
    fn a_channel_that_does_not_stream_is_unsupported() {
        let error = ChannelError::Unsupported {
            attempted: "stream",
            detail: "one request and one answer".into(),
        };
        assert!(matches!(
            broken(error),
            MemoryError::Rejected(Rejection::Unsupported { .. })
        ));
    }
}
