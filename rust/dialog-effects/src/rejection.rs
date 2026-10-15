//! Why a request was not carried out.

use thiserror::Error;

/// A request that could not be carried out for a reason that is not an
/// access decision.
///
/// Authorization failures are *not* here: they are
/// [`AuthorizeError`](dialog_capability::access::AuthorizeError), which
/// already names them (revoked, expired, audience mismatch, unproven
/// subject, and the rest), and effect errors carry that type directly
/// rather than restating it.
///
/// Version conflicts are not here either: they are
/// [`MemoryError::VersionMismatch`](crate::memory::MemoryError), which
/// carries the versions themselves and is what every caller already
/// matches on.
///
/// What is left over is the handful of ways a request fails when
/// authority was never in question and no state moved. Deliberately not
/// a status code or a response: a caller holding one of these has no way
/// to learn what carried the request, because that is never what it
/// needs to decide.
#[derive(Clone, Debug, Error, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind")]
pub enum Rejection {
    /// Temporarily unable to serve. Retryable as-is.
    #[error("Temporarily unable to serve the request: {reason}")]
    Unavailable {
        /// Why, as far as the responder would say.
        reason: String,
        /// How many seconds to wait before trying again, when the
        /// responder said (a rate limit, say). Left out of the encoding
        /// when it did not, so a peer that predates it reads the same.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after: Option<u64>,
    },

    /// The responder does not carry out requests of this kind at all.
    ///
    /// Not retryable as-is, and not an access decision: the same request
    /// fails the same way for anyone. A caller with another way to get
    /// what it asked for (resolving a cell again instead of watching it,
    /// say) takes that way.
    #[error("The request is not supported: {reason}")]
    Unsupported {
        /// What is not supported, as far as the responder would say.
        reason: String,
    },

    /// Not carried out, for a reason this version does not recognize.
    ///
    /// The honest variant. Folding an unrecognized failure into a named
    /// one would claim knowledge we do not have, and would silently
    /// change meaning the day the responder starts saying something new.
    /// `detail` is kept for logs; matching on it is a mistake, because
    /// what lands here is exactly what has no agreed meaning.
    #[error("The request was not carried out: {detail}")]
    Unclassified {
        /// Whatever came back, verbatim and bounded.
        detail: String,
    },
}

impl Rejection {
    /// Temporarily unable to serve, for `reason`, with no word on when
    /// to try again.
    pub fn unavailable(reason: impl Into<String>) -> Self {
        Rejection::Unavailable {
            reason: reason.into(),
            retry_after: None,
        }
    }

    /// How many seconds to wait before trying again, when the responder
    /// said.
    pub fn retry_after(&self) -> Option<u64> {
        match self {
            Rejection::Unavailable { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// Whether retrying the identical request could succeed.
    ///
    /// True only for [`Rejection::Unavailable`]. An unrecognized
    /// failure is not a known-transient one, so it does not count.
    pub fn is_transient(&self) -> bool {
        matches!(self, Rejection::Unavailable { .. })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::Rejection;

    /// A refusal that says when to try again carries it across the wire,
    /// and one that does not says nothing of it, so a peer that predates
    /// the field reads it and writes it as before.
    #[dialog_common::test]
    fn it_carries_when_to_try_again_and_leaves_it_out_when_unsaid() {
        let limited = Rejection::Unavailable {
            reason: "rate limited".into(),
            retry_after: Some(30),
        };
        let encoded = serde_json::to_value(&limited).expect("encodes");
        assert_eq!(encoded["retry_after"], 30);
        let decoded: Rejection = serde_json::from_value(encoded).expect("decodes");
        assert_eq!(decoded.retry_after(), Some(30));

        let unsaid = serde_json::to_value(Rejection::unavailable("down")).expect("encodes");
        assert!(unsaid.get("retry_after").is_none(), "{unsaid}");

        let older: Rejection =
            serde_json::from_str(r#"{"kind":"Unavailable","reason":"down"}"#).expect("decodes");
        assert_eq!(older, Rejection::unavailable("down"));
        assert_eq!(older.retry_after(), None);
    }
}
