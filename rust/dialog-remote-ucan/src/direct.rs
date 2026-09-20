//! The exchange with the access service: one request whose
//! `Authorization` header carries the signed invocation and whose body
//! carries the bytes the operation stores, answered with the
//! operation's outcome: the status the object route would have given,
//! the object's version, and the bytes a read asked for.

use dialog_capability::access::AuthorizeError;
use dialog_effects::Rejection;
use dialog_effects::blob::{BlobError, BlobReader, BlobSource};
use dialog_remote_s3::{S3Error, http_client};
use dialog_remote_ucan_s3::UcanAuthorization;
use dialog_ucan_core::{Container, ContainerError, InvocationChain, Tag};
use dialog_varsig::AnySignature;

use crate::address::UcanAddress;

/// The `Authorization` scheme an invocation travels under.
pub const SCHEME: &str = "UCAN";

/// The media type of an object's bytes: what a write's body carries and
/// what a read's answer carries.
pub const OBJECT_MEDIA_TYPE: &str = "application/octet-stream";

/// The media type of a permit, which is what a service that does not
/// perform operations answers an invocation with, and which this
/// exchange has no use for.
const PERMIT_MEDIA_TYPE: &str = "application/cbor";

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

/// An operation's answer: the status the object route would have given,
/// the object's version when the operation has one, and the body.
pub(crate) struct Answer {
    pub status: u16,
    pub etag: Option<String>,
    response: reqwest::Response,
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
        Ok(self.response.bytes().await?.to_vec())
    }

    /// The body as a stream of chunks.
    pub fn source(self) -> BlobReader {
        Box::new(Source::from_response(self.response))
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
/// and `Accept` names the object's media type, which is what the
/// outcome of an operation is answered as.
///
/// A service that does not speak this exchange gives itself away in
/// one of two ways, and each is answered with an error that says so: a
/// permit in place of the outcome, from a service that verified the
/// invocation but performs nothing; or a refusal to read the request,
/// from a service that looks for the invocation in the body.
pub(crate) async fn invoke(
    address: &UcanAddress,
    authorization: &UcanAuthorization,
    payload: Option<Vec<u8>>,
) -> Result<Answer, S3Error> {
    let chain = authorization.invocation().chain();
    let url = labeled(address.endpoint(), chain);
    let credential =
        credential(Container::from(chain)).map_err(|e| S3Error::Serialization(e.to_string()))?;

    let mut request = http_client()
        .post(url)
        .header("Authorization", credential)
        .header("Accept", OBJECT_MEDIA_TYPE);
    if let Some(payload) = payload {
        request = request
            .header("Content-Type", OBJECT_MEDIA_TYPE)
            .body(payload);
    }
    let response = request.send().await?;

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
        return Err(does_not_perform(
            address,
            "it answered a permit in place of the outcome",
        ));
    }
    if status == 400 {
        let body = bounded(response).await?;
        if let Ok(AuthorizeError::Malformed { detail }) = serde_json::from_slice(&body) {
            return Err(does_not_perform(
                address,
                &format!("it did not read the invocation from the request: {detail}"),
            ));
        }
        return Err(read_refusal(status, &body));
    }
    Ok(Answer {
        status,
        etag,
        response,
    })
}

/// The endpoint with the invocation's command and subject in its query,
/// as `cmd` and `sub`.
///
/// The service ignores them; they are for whoever reads a network log,
/// where every request to the endpoint otherwise looks the same. Only
/// the command and the subject go there, not the arguments: a browser
/// keys its preflight cache by URL, so a session's worth of requests
/// must share a handful of URLs to share a handful of preflights. Both
/// values are made of characters a query carries as they are, so they
/// read in a log as they read here.
pub(crate) fn labeled(endpoint: &str, chain: &InvocationChain<AnySignature>) -> String {
    let separator = if endpoint.contains('?') { '&' } else { '?' };
    format!(
        "{endpoint}{separator}cmd={}&sub={}",
        chain.command(),
        chain.subject().as_str()
    )
}

/// The error for a service that does not speak this exchange.
fn does_not_perform(address: &UcanAddress, evidence: &str) -> S3Error {
    S3Error::Rejected(Rejection::Unclassified {
        detail: format!(
            "the service at {} does not perform invocations: {evidence}",
            address.endpoint()
        ),
    })
}

/// At most [`MAX_ERROR_BODY_BYTES`] of a body.
async fn bounded(response: reqwest::Response) -> Result<Vec<u8>, S3Error> {
    let mut bytes = response.bytes().await?.to_vec();
    bytes.truncate(MAX_ERROR_BODY_BYTES);
    Ok(bytes)
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
