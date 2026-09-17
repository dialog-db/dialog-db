//! The exchange with the access service: one request whose
//! `Authorization` header carries the signed invocation and whose body
//! carries the bytes the operation stores. `Accept` names what the site
//! will take back, in order: the operation's outcome (the status the
//! object route would have given, the object's version, the bytes a
//! read asked for), or a permit to perform the operation itself, which
//! a service that does not perform operations answers with.

use dialog_capability::access::AuthorizeError;
use dialog_effects::Rejection;
use dialog_effects::blob::{BlobError, BlobReader, BlobSource};
use dialog_remote_s3::{Permit, S3Error, http_client};
use dialog_remote_ucan_s3::UcanAuthorization;
use dialog_ucan_core::{Container, ContainerError, Tag};

use crate::address::UcanAddress;

/// The `Authorization` scheme an invocation travels under.
pub const SCHEME: &str = "UCAN";

/// The media type of an object's bytes: what a write's body carries and
/// what a read's answer carries.
pub const OBJECT_MEDIA_TYPE: &str = "application/octet-stream";

/// The media type of a permit, which a service answers with when it
/// does not perform operations itself.
pub const PERMIT_MEDIA_TYPE: &str = "application/cbor";

/// What the site asks for, in order of preference: the outcome, else a
/// permit. A service that performs operations answers with the first;
/// one that does not answers with the second, and the site completes
/// the operation with it.
pub const ACCEPT: &str = "application/octet-stream, application/cbor";

/// How much of a refusal body is read for its reason.
const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;

/// The `Authorization` header value that carries `container`: the
/// scheme, a space, and the container in the spec's text serialization,
/// gzipped when that comes out shorter.
pub fn credential(container: Container) -> Result<String, ContainerError> {
    let plain = container.clone().encode(Tag::Base64Url)?;
    let packed = container.encode(Tag::Base64UrlGzip)?;
    let encoded = if packed.len() < plain.len() {
        packed
    } else {
        plain
    };
    let text = String::from_utf8(encoded).map_err(|e| ContainerError::Invocation(e.to_string()))?;
    Ok(format!("{SCHEME} {text}"))
}

/// Whether an `Authorization` header value uses the UCAN scheme.
pub fn is_credential(value: &str) -> bool {
    value
        .trim_start()
        .split_once(char::is_whitespace)
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case(SCHEME))
}

/// The container an `Authorization` header value carries.
pub fn credential_container(value: &str) -> Result<Container, ContainerError> {
    let (scheme, rest) = value
        .trim()
        .split_once(char::is_whitespace)
        .ok_or_else(|| {
            ContainerError::Invocation("the credential names a scheme and nothing after it".into())
        })?;
    if !scheme.eq_ignore_ascii_case(SCHEME) {
        return Err(ContainerError::Invocation(format!(
            "the credential's scheme is {scheme}, not {SCHEME}"
        )));
    }
    Container::decode(rest.trim_start().as_bytes())
}

/// How the service answered a request.
#[derive(Debug)]
pub(crate) enum Outcome {
    /// The service performed the operation; this is its answer.
    Answered(Answer),
    /// The service verified the invocation but does not perform
    /// operations: here is the permit to perform this one with.
    Permit(Permit),
    /// The service does not read invocations this way: it answered a
    /// request it could not read as such, turned it away for its size,
    /// or the request never completed. The operation goes through the
    /// permit flow from the start.
    Unsupported,
}

/// An answer's body: still on the wire, or already read.
enum Body {
    Pending(reqwest::Response),
    Read(Vec<u8>),
}

/// An operation's answer: the status the object route would have given,
/// the object's version when the operation has one, and the body.
pub(crate) struct Answer {
    pub status: u16,
    pub etag: Option<String>,
    body: Body,
}

impl std::fmt::Debug for Answer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Answer")
            .field("status", &self.status)
            .field("etag", &self.etag)
            .finish_non_exhaustive()
    }
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

    /// The whole body.
    pub async fn bytes(self) -> Result<Vec<u8>, S3Error> {
        match self.body {
            Body::Pending(response) => Ok(response.bytes().await?.to_vec()),
            Body::Read(bytes) => Ok(bytes),
        }
    }

    /// The body as a stream of chunks.
    pub fn source(self) -> BlobReader {
        match self.body {
            Body::Pending(response) => Box::new(Source::from_response(response)),
            Body::Read(bytes) => Box::new(Source::from_bytes(bytes)),
        }
    }

    /// The reason the request was refused, as the service sent it.
    pub async fn refusal(self) -> S3Error {
        let status = self.status;
        let body = self.bytes().await.unwrap_or_default();
        read_refusal(status, &body)
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
/// The invocation rides in `Authorization`, the payload is the body,
/// and `Accept` names the outcome first and a permit second. A service
/// that performs answers with the outcome, one that redeems answers
/// with a permit, and the site tells the two apart by content type.
pub(crate) async fn invoke(
    address: &UcanAddress,
    authorization: &UcanAuthorization,
    payload: Option<Vec<u8>>,
) -> Result<Outcome, S3Error> {
    let container = Container::from(authorization.invocation().chain());
    let credential = credential(container).map_err(|e| S3Error::Serialization(e.to_string()))?;

    let mut request = http_client()
        .post(address.endpoint())
        .header("Authorization", credential)
        .header("Accept", ACCEPT);
    if let Some(payload) = payload {
        request = request
            .header("Content-Type", OBJECT_MEDIA_TYPE)
            .body(payload);
    }
    let response = match request.send().await {
        Ok(response) => response,
        // A service that refuses a body on its declared length answers
        // before reading it, which a browser reports as a failed fetch
        // rather than as the refusal.
        Err(error) => {
            let error = S3Error::from(error);
            return if error.is_transport() {
                Ok(Outcome::Unsupported)
            } else {
                Err(error)
            };
        }
    };

    let status = response.status().as_u16();
    let etag = response
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim_matches('"').to_string());
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .unwrap_or_default();

    if (200..300).contains(&status) && content_type.starts_with(PERMIT_MEDIA_TYPE) {
        let permit = serde_ipld_dagcbor::from_slice(&response.bytes().await?)
            .map_err(|e| S3Error::Serialization(format!("failed to decode the permit: {e}")))?;
        return Ok(Outcome::Permit(permit));
    }
    if status == 413 {
        return Ok(Outcome::Unsupported);
    }
    if status == 400 {
        // An older service looks for the container in the body, finds
        // none, and says the request is malformed. That is the one
        // 400 that means "not this way" rather than "not this".
        let body = bounded(response).await?;
        if matches!(
            serde_json::from_slice::<AuthorizeError>(&body),
            Ok(AuthorizeError::Malformed { .. })
        ) {
            return Ok(Outcome::Unsupported);
        }
        return Ok(Outcome::Answered(Answer {
            status,
            etag,
            body: Body::Read(body),
        }));
    }
    Ok(Outcome::Answered(Answer {
        status,
        etag,
        body: Body::Pending(response),
    }))
}

/// At most [`MAX_ERROR_BODY_BYTES`] of a body.
async fn bounded(response: reqwest::Response) -> Result<Vec<u8>, S3Error> {
    let mut bytes = response.bytes().await?.to_vec();
    bytes.truncate(MAX_ERROR_BODY_BYTES);
    Ok(bytes)
}

/// Whether an error is the request failing to complete in transport,
/// as opposed to the service answering it.
pub(crate) trait Transport {
    /// See the trait.
    fn is_transport(&self) -> bool;
}

impl Transport for S3Error {
    fn is_transport(&self) -> bool {
        matches!(self, S3Error::Transport(_))
    }
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

/// A stream of decoded byte chunks.
trait ByteChunks:
    futures_util::Stream<Item = Result<Vec<u8>, BlobError>> + dialog_common::ConditionalSend
{
}
impl<T> ByteChunks for T where
    T: futures_util::Stream<Item = Result<Vec<u8>, BlobError>> + dialog_common::ConditionalSend
{
}

/// An answer's body as chunks: the response's own chunks on native,
/// the whole body as one chunk on the web, where the fetch backend
/// reads bodies whole.
struct Source {
    stream: std::pin::Pin<Box<dyn ByteChunks>>,
}

impl Source {
    fn from_response(response: reqwest::Response) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let stream: std::pin::Pin<Box<dyn ByteChunks>> = {
            use futures_util::StreamExt as _;
            Box::pin(response.bytes_stream().map(|chunk| {
                chunk
                    .map(|bytes| bytes.to_vec())
                    .map_err(|e| BlobError::Storage(e.to_string()))
            }))
        };
        #[cfg(target_arch = "wasm32")]
        let stream: std::pin::Pin<Box<dyn ByteChunks>> =
            Box::pin(futures_util::stream::once(async move {
                response
                    .bytes()
                    .await
                    .map(|bytes| bytes.to_vec())
                    .map_err(|e| BlobError::Storage(e.to_string()))
            }));
        Self { stream }
    }

    fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            stream: Box::pin(futures_util::stream::once(async move { Ok(bytes) })),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobSource for Source {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        use futures_util::StreamExt as _;
        self.stream.next().await.transpose()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    #[dialog_common::test]
    fn it_carries_a_container_under_the_ucan_scheme() {
        let container = Container::new(vec![vec![1, 2, 3], vec![4, 5, 6]]);
        let value = credential(container.clone()).unwrap();
        assert!(value.starts_with("UCAN "), "{value}");
        assert!(is_credential(&value));
        assert!(is_credential("ucan Cabc"), "the scheme is case-insensitive");
        assert!(!is_credential("Bearer abc"));
        assert!(!is_credential("UCAN"), "a scheme alone carries nothing");
        assert_eq!(credential_container(&value).unwrap(), container);
    }

    #[dialog_common::test]
    fn it_reads_the_container_in_either_text_form() {
        let container = Container::new(vec![vec![9u8; 64]]);
        for tag in [
            Tag::Base64Url,
            Tag::Base64UrlGzip,
            Tag::Base64,
            Tag::Base64Gzip,
        ] {
            let text = String::from_utf8(container.clone().encode(tag).unwrap()).unwrap();
            let parsed = credential_container(&format!("UCAN {text}")).unwrap();
            assert_eq!(parsed, container, "{tag:?}");
        }
    }

    #[dialog_common::test]
    fn it_rejects_a_credential_under_another_scheme() {
        assert!(credential_container("Bearer abc").is_err());
        assert!(credential_container("UCAN").is_err());
        assert!(credential_container("UCAN Z").is_err());
    }
}
