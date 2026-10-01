//! The frames a socket carries, encoded as dag-cbor, one to a message.

use dialog_effects::memory::CellState;
use serde::{Deserialize, Serialize};

/// The subprotocol a socket speaks, which a client asks for when it
/// connects and a service that speaks it answers with.
pub const SUBPROTOCOL: &str = "dialog.ucan.v1";

/// A frame a client sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Request {
    /// An invocation, and the bytes it stores when it is a write: what a
    /// request carries in its `Authorization` header and its body.
    Invoke {
        /// The invocation's container, as dag-cbor.
        #[serde(with = "serde_bytes")]
        container: Vec<u8>,
        /// The bytes a write stores.
        #[serde(default, with = "serde_bytes", skip_serializing_if = "Option::is_none")]
        payload: Option<Vec<u8>>,
    },
    /// Stop the watch `invocation` began. It needs no authority: it only
    /// stops what this connection started.
    Cancel {
        /// The watch invocation's content identifier.
        invocation: String,
    },
}

/// A frame a service sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Reply {
    /// How an invocation was answered: what a response to a request
    /// carries. A watch that is not accepted is answered this way too.
    Answer {
        /// The invocation answered, or none for a frame whose invocation
        /// could not be read.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        invocation: Option<String>,
        /// The status a response would have.
        status: u16,
        /// The object's version, which a response carries as its `ETag`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<String>,
        /// The body a response would have.
        #[serde(with = "serde_bytes")]
        body: Vec<u8>,
    },
    /// What the cell a watch follows holds: when the watch begins, then
    /// each time it changes.
    State {
        /// The watch invocation.
        invocation: String,
        /// What the cell holds.
        state: CellState,
    },
    /// The watch ended, for the reason `body` gives, as a refused
    /// request's body would: its authority no longer holds.
    Ended {
        /// The watch invocation.
        invocation: String,
        /// The status a refused request would have.
        status: u16,
        /// The reason, as a refused request's body carries it.
        #[serde(with = "serde_bytes")]
        body: Vec<u8>,
    },
}

/// A frame that could not be read.
#[derive(Debug, thiserror::Error)]
#[error("The frame could not be read: {0}")]
pub struct Unreadable(String);

impl Request {
    /// The frame's bytes.
    pub fn encode(&self) -> Vec<u8> {
        serde_ipld_dagcbor::to_vec(self).expect("a request frame always encodes")
    }

    /// The frame `bytes` carry.
    pub fn decode(bytes: &[u8]) -> Result<Self, Unreadable> {
        serde_ipld_dagcbor::from_slice(bytes).map_err(|error| Unreadable(error.to_string()))
    }
}

impl Reply {
    /// The frame's bytes.
    pub fn encode(&self) -> Vec<u8> {
        serde_ipld_dagcbor::to_vec(self).expect("a reply frame always encodes")
    }

    /// The frame `bytes` carry.
    pub fn decode(bytes: &[u8]) -> Result<Self, Unreadable> {
        serde_ipld_dagcbor::from_slice(bytes).map_err(|error| Unreadable(error.to_string()))
    }

    /// The invocation the frame answers, when it names one.
    pub fn invocation(&self) -> Option<&str> {
        match self {
            Reply::Answer { invocation, .. } => invocation.as_deref(),
            Reply::State { invocation, .. } | Reply::Ended { invocation, .. } => Some(invocation),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_effects::memory::{Edition, Version};

    /// Every frame reads back as it was written.
    #[dialog_common::test]
    fn it_reads_back_every_frame() {
        let requests = [
            Request::Invoke {
                container: vec![1, 2, 3],
                payload: Some(vec![4, 5]),
            },
            Request::Invoke {
                container: vec![1],
                payload: None,
            },
            Request::Cancel {
                invocation: "bafy".into(),
            },
        ];
        for request in requests {
            assert_eq!(Request::decode(&request.encode()).unwrap(), request);
        }
        let replies = [
            Reply::Answer {
                invocation: Some("bafy".into()),
                status: 200,
                version: Some("v1".into()),
                body: vec![9],
            },
            Reply::Answer {
                invocation: None,
                status: 400,
                version: None,
                body: Vec::new(),
            },
            Reply::State {
                invocation: "bafy".into(),
                state: Some(Edition {
                    content: vec![7],
                    version: Version::from(b"v2".as_slice()),
                }),
            },
            Reply::State {
                invocation: "bafy".into(),
                state: None,
            },
            Reply::Ended {
                invocation: "bafy".into(),
                status: 403,
                body: b"{}".to_vec(),
            },
        ];
        for reply in replies {
            assert_eq!(Reply::decode(&reply.encode()).unwrap(), reply);
        }
    }

    /// Bytes that are not a frame are refused rather than misread.
    #[dialog_common::test]
    fn it_refuses_bytes_that_are_not_a_frame() {
        assert!(Request::decode(b"not cbor").is_err());
        assert!(
            Reply::decode(
                &Request::Cancel {
                    invocation: "x".into()
                }
                .encode()
            )
            .is_err()
        );
    }
}
