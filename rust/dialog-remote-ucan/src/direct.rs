//! The exchange with the access service: one request that carries the
//! signed invocation, and the payload when the operation stores one, and
//! answers with either the operation's outcome or a permit.

use dialog_capability::access::AuthorizeError;
use dialog_effects::Rejection;
use dialog_remote_s3::{Permit, S3Error, http_client};
use dialog_remote_ucan_s3::UcanAuthorization;
use dialog_ucan_core::Container;

use crate::address::UcanAddress;

/// The media type of an object's bytes, and of the outcome of an
/// operation performed in the request that proved it.
pub const OBJECT_MEDIA_TYPE: &str = "application/octet-stream";

/// The media type of a permit, which a service answers with when it
/// does not perform operations itself.
pub const PERMIT_MEDIA_TYPE: &str = "application/cbor";

/// What the site asks for, in order of preference: the outcome, else a
/// permit. A service that performs operations reads the first; one that
/// does not never looks and answers with the second.
pub const ACCEPT: &str = "application/octet-stream, application/cbor";

/// How much of a refusal body is read for its reason.
const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;

/// How the service answered a request.
#[derive(Debug)]
pub(crate) enum Outcome {
    /// The service does not perform operations: here is the permit to
    /// perform this one with.
    Permit(Permit),
    /// The service performed the operation; this is its answer.
    Answer(Answer),
}

/// An operation's answer: the status the object route would have given,
/// the object's version when the operation has one, and the body.
#[derive(Debug)]
pub(crate) struct Answer {
    pub status: u16,
    pub etag: Option<String>,
    pub body: Vec<u8>,
}

impl Answer {
    /// Whether the status reports success.
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// Whether the service refused the request on access grounds.
    pub fn is_refusal(&self) -> bool {
        matches!(self.status, 401 | 403)
    }

    /// Whether the service turned the request away for its size, which
    /// a container carrying a payload can run into at a service that
    /// only ever expected tokens.
    pub fn is_too_large(&self) -> bool {
        self.status == 413
    }

    /// The reason the request was refused, as the service sent it.
    pub fn refusal(&self) -> S3Error {
        read_refusal(self.status, &self.body)
    }

    /// The object's version, which every successful answer to a cell
    /// operation carries.
    pub fn version(&self) -> Result<String, S3Error> {
        self.etag.clone().ok_or_else(|| {
            S3Error::Serialization("the answer carries no ETag for the version".to_string())
        })
    }
}

/// Send the invocation to the service and read how it answered.
///
/// The body is the container the permit flow sends, plus the payload
/// under its own key when there is one. The `Accept` header names the
/// outcome first and a permit second, so a service that performs
/// operations answers with the outcome and every other service answers
/// as before.
pub(crate) async fn invoke(
    address: &UcanAddress,
    authorization: &UcanAuthorization,
    payload: Option<&[u8]>,
) -> Result<Outcome, S3Error> {
    let mut container = Container::from(authorization.invocation().chain());
    if let Some(payload) = payload {
        container = container.with_payload(payload);
    }
    let body = container
        .into_bytes()
        .map_err(|e| S3Error::Serialization(e.to_string()))?;

    let response = http_client()
        .post(address.endpoint())
        .header("Content-Type", PERMIT_MEDIA_TYPE)
        .header("Accept", ACCEPT)
        .body(body)
        .send()
        .await?;

    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .unwrap_or_default();
    let etag = response
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim_matches('"').to_string());
    let body = response.bytes().await?.to_vec();

    if (200..300).contains(&status) && content_type.starts_with(PERMIT_MEDIA_TYPE) {
        let permit = serde_ipld_dagcbor::from_slice(&body)
            .map_err(|e| S3Error::Serialization(format!("failed to decode the permit: {e}")))?;
        return Ok(Outcome::Permit(permit));
    }
    Ok(Outcome::Answer(Answer { status, etag, body }))
}

/// Read the reason a request was refused, as the permit flow does: the
/// reason travels as itself, so nothing here knows a vocabulary of wire
/// names, and an older responder degrades to "something went wrong".
fn read_refusal(status: u16, body: &[u8]) -> S3Error {
    let bounded = &body[..body.len().min(MAX_ERROR_BODY_BYTES)];

    if let Ok(reason) = serde_json::from_slice::<AuthorizeError>(bounded) {
        return S3Error::Authorization(reason);
    }
    if let Ok(reason) = serde_json::from_slice::<Rejection>(bounded) {
        return S3Error::Rejected(reason);
    }

    S3Error::Rejected(Rejection::Unclassified {
        detail: format!("responder answered {status} with no reason we could read"),
    })
}
