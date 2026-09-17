//! The server side: decode the invocation, verify it, and perform it.
//!
//! An embedder brings a [`Provider`] of the effects a remote performs,
//! the same traits a local store implements, and wraps it in
//! [`Access`]. Every request then goes through one path: the container
//! is decoded, the invocation's chain is verified against the resolver
//! and the revocation checker the embedder configured, the operation it
//! authorizes is read back as a capability, and only then is the
//! provider called with that capability. A write's bytes come out of
//! the container's payload and are checked against the digest and
//! checksum the invocation bound before the provider sees them.
//!
//! The layer speaks no HTTP framework. It takes the request's `Accept`
//! and body and answers with a status, a content type, a version and a
//! body, which the embedder relays however it serves HTTP.

use std::sync::Arc;

use dialog_capability::access::AuthorizeError;
use dialog_capability::{Did, Policy, Provider, Subject};
use dialog_common::{Blake3Hash, Buffer, Checksum, ConditionalSync};
use dialog_did_web::{CachingResolver, Resolve, WebResolver};
use dialog_effects::archive::{self, Catalog, PutAttenuation};
use dialog_effects::memory::{self, Cell, PublishAttenuation, Space};
use dialog_effects::{Rejection, Use};
use dialog_remote_ucan_s3::{Args, FromUcanArgs, verify_invocation};
use dialog_ucan_core::revocation::RevocationChecker;
use dialog_ucan_core::{Container, InvocationChain, UnverifiedRevocations};
use dialog_varsig::AnySignature;
use serde::Serialize;

use crate::direct::OBJECT_MEDIA_TYPE;

/// The media type of a refusal's body: the reason, as JSON, in the
/// shape the client reads back.
pub const REFUSAL_MEDIA_TYPE: &str = "application/json";

/// An access service over a provider of the effects it performs.
pub struct Access<P, Resolver = CachingResolver<WebResolver>, Revocations = UnverifiedRevocations> {
    provider: P,
    resolver: Arc<Resolver>,
    revocations: Arc<Revocations>,
}

impl<P: std::fmt::Debug, Resolver, Revocations> std::fmt::Debug
    for Access<P, Resolver, Revocations>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Access")
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

impl<P> Access<P> {
    /// An access service over `provider`, resolving `did:web` issuers
    /// over the web and checking no revocations.
    pub fn new(provider: P) -> Self {
        Self::with_resolver(provider, CachingResolver::new(WebResolver::new()))
    }
}

impl<P, Resolver> Access<P, Resolver> {
    /// An access service over `provider`, resolving issuers through
    /// `resolver`.
    pub fn with_resolver(provider: P, resolver: Resolver) -> Self {
        Self::with_shared_resolver(provider, Arc::new(resolver))
    }

    /// An access service over `provider`, resolving issuers through a
    /// `resolver` shared with others: an embedder that builds a service
    /// per request keeps one resolver, and its cache, across them.
    pub fn with_shared_resolver(provider: P, resolver: Arc<Resolver>) -> Self {
        Self {
            provider,
            resolver,
            revocations: Arc::new(UnverifiedRevocations),
        }
    }
}

impl<P, Resolver, Revocations> Access<P, Resolver, Revocations> {
    /// The same service, checking every link of a chain against
    /// `revocations`.
    pub fn with_revocations<Checked>(self, revocations: Checked) -> Access<P, Resolver, Checked> {
        Access {
            provider: self.provider,
            resolver: self.resolver,
            revocations: Arc::new(revocations),
        }
    }

    /// The provider the service performs operations with.
    pub fn provider(&self) -> &P {
        &self.provider
    }
}

/// What the layer needs from an HTTP request: what the client asked
/// for, and the container it sent.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    accept: Option<&'a str>,
    body: &'a [u8],
}

impl<'a> Request<'a> {
    /// A request carrying `body`, with no `Accept` header.
    pub fn new(body: &'a [u8]) -> Self {
        Self { accept: None, body }
    }

    /// The same request with its `Accept` header.
    pub fn accept(mut self, accept: &'a str) -> Self {
        self.accept = Some(accept);
        self
    }

    /// Whether the client asked for the operation's outcome rather than
    /// a permit: it named the outcome's media type in `Accept`.
    pub fn wants_outcome(&self) -> bool {
        self.accept
            .is_some_and(|accept| accept.contains(OBJECT_MEDIA_TYPE))
    }

    /// The container the request carries.
    pub fn body(&self) -> &'a [u8] {
        self.body
    }
}

/// What the layer answers, for the embedder to relay: the status the
/// object route would have given, the content type, the object's
/// version when the operation has one (an `ETag`), and the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// The HTTP status.
    pub status: u16,
    /// The body's media type.
    pub content_type: &'static str,
    /// The object's version, for the `ETag` header.
    pub version: Option<String>,
    /// The body.
    pub body: Vec<u8>,
}

impl Response {
    fn outcome(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: OBJECT_MEDIA_TYPE,
            version: None,
            body,
        }
    }

    fn status(status: u16) -> Self {
        Self::outcome(status, Vec::new())
    }

    fn versioned(status: u16, version: String, body: Vec<u8>) -> Self {
        Self {
            version: Some(version),
            ..Self::outcome(status, body)
        }
    }

    fn json(status: u16, value: &impl Serialize) -> Self {
        Self {
            status,
            content_type: REFUSAL_MEDIA_TYPE,
            version: None,
            body: serde_json::to_vec(value).unwrap_or_default(),
        }
    }

    fn rejected(status: u16, rejection: Rejection) -> Self {
        Self::json(status, &rejection)
    }

    /// Whether the status reports success.
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// Why an invocation was not performed: it did not verify.
///
/// Carries the reason itself, so an embedder can log or meter it, and
/// renders as the response the client reads the reason back from.
#[derive(Debug, Clone, PartialEq)]
pub struct Refusal(AuthorizeError);

impl Refusal {
    /// The reason.
    pub fn reason(&self) -> &AuthorizeError {
        &self.0
    }

    /// The status the reason answers with: 401 for authority that does
    /// not hold, 403 for authority that holds but is declined, 400 for
    /// a request that could not be read, 503 for a check that could not
    /// be made.
    pub fn status(&self) -> u16 {
        match &self.0 {
            AuthorizeError::InvalidSignature { .. }
            | AuthorizeError::InvalidAudience { .. }
            | AuthorizeError::Expired { .. }
            | AuthorizeError::NotValidBefore { .. } => 401,
            AuthorizeError::UnprovenSubject { .. }
            | AuthorizeError::CommandEscalation { .. }
            | AuthorizeError::PolicyViolation { .. }
            | AuthorizeError::Declined { .. }
            | AuthorizeError::Revoked { .. } => 403,
            AuthorizeError::Malformed { .. } | AuthorizeError::UnavailableProof { .. } => 400,
            AuthorizeError::Unavailable { .. } => 503,
        }
    }

    /// The response the refusal answers with.
    pub fn into_response(self) -> Response {
        Response::json(self.status(), &self.0)
    }
}

impl From<AuthorizeError> for Refusal {
    fn from(reason: AuthorizeError) -> Self {
        Self(reason)
    }
}

/// An invocation that verified: its chain, and the payload the
/// container carried beside it.
#[derive(Debug)]
pub struct Verified {
    chain: InvocationChain<AnySignature>,
    payload: Option<Vec<u8>>,
}

impl Verified {
    /// The verified chain.
    pub fn chain(&self) -> &InvocationChain<AnySignature> {
        &self.chain
    }

    /// The subject the invocation acts on.
    pub fn subject(&self) -> &Did {
        self.chain.subject()
    }

    /// The command's segments.
    pub fn command(&self) -> Vec<&str> {
        self.chain.command().0.iter().map(String::as_str).collect()
    }

    /// The bytes the invocation acts on, when the container carried
    /// them.
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }
}

/// How the layer answered a request.
#[derive(Debug)]
pub enum Answer {
    /// The invocation verified and the operation ran; here is its
    /// outcome, which includes the operation's own refusals (a missing
    /// object, a version that did not match).
    Performed(Response),
    /// The invocation did not verify.
    Refused(Refusal),
    /// The layer does not answer this request: the client did not ask
    /// for the outcome, or the invocation names an operation the layer
    /// does not perform (blob streams, which read and write ranges over
    /// a URL). The embedder answers it its own way, with a permit.
    Unsupported,
}

/// The effects a provider performs for the layer.
pub trait Store:
    Provider<archive::Get>
    + Provider<archive::Put>
    + Provider<memory::Resolve>
    + Provider<memory::Publish>
    + Provider<memory::Retract>
    + ConditionalSync
{
}

impl<T> Store for T where
    T: Provider<archive::Get>
        + Provider<archive::Put>
        + Provider<memory::Resolve>
        + Provider<memory::Publish>
        + Provider<memory::Retract>
        + ConditionalSync
{
}

impl<P, Resolver, Revocations> Access<P, Resolver, Revocations>
where
    Resolver: Provider<Resolve> + ConditionalSync,
    Revocations: RevocationChecker + ConditionalSync,
{
    /// Decode and verify the invocation `body` carries.
    pub async fn verify(&self, body: &[u8]) -> Result<Verified, Refusal> {
        let mut container = Container::from_bytes(body).map_err(|e| AuthorizeError::Malformed {
            detail: e.to_string(),
        })?;
        let payload = container.take_payload();
        let chain =
            verify_invocation(container, self.resolver.as_ref(), &*self.revocations).await?;
        Ok(Verified { chain, payload })
    }

    /// Answer a request: verify the invocation and perform it, when the
    /// client asked for the outcome and the layer performs the operation.
    pub async fn handle(&self, request: Request<'_>) -> Answer
    where
        P: Store,
    {
        if !request.wants_outcome() {
            return Answer::Unsupported;
        }
        match self.verify(request.body()).await {
            Ok(verified) => self.perform(verified).await,
            Err(refusal) => Answer::Refused(refusal),
        }
    }

    /// Perform a verified invocation with the provider.
    pub async fn perform(&self, verified: Verified) -> Answer
    where
        P: Store,
    {
        let subject = verified.subject().clone();
        let args = verified.chain().arguments();
        let command = verified.command();
        let outcome = match command.as_slice() {
            ["use", "get", "archive", "block"] | ["archive", "get"] => {
                self.get(&subject, args).await
            }
            ["use", "put", "archive", "block"] | ["archive", "put"] => {
                self.put(&subject, args, verified.payload()).await
            }
            ["use", "get", "memory", "cell"] | ["memory", "resolve"] => {
                self.resolve(&subject, args).await
            }
            ["use", "put", "memory", "cell"] | ["memory", "publish"] => {
                self.publish(&subject, args, verified.payload()).await
            }
            ["use", "delete", "memory", "cell"] | ["memory", "retract"] => {
                self.retract(&subject, args).await
            }
            _ => return Answer::Unsupported,
        };
        match outcome {
            Ok(response) => Answer::Performed(response),
            Err(Failure::Refused(refusal)) => Answer::Refused(refusal),
            Err(Failure::Answered(response)) => Answer::Performed(response),
        }
    }

    async fn get(&self, subject: &Did, args: &Args) -> Result<Response, Failure>
    where
        P: Store,
    {
        let capability = archive::Get::capability_from_args(subject, args)?;
        match Provider::<archive::Get>::execute(&self.provider, capability).await {
            Ok(Some(bytes)) => Ok(Response::outcome(200, bytes)),
            Ok(None) => Ok(Response::status(404)),
            Err(error) => Err(Failure::from(error)),
        }
    }

    async fn put(
        &self,
        subject: &Did,
        args: &Args,
        payload: Option<&[u8]>,
    ) -> Result<Response, Failure>
    where
        P: Store,
    {
        let attenuated = archive::Put::capability_from_args(subject, args)?;
        let payload = payload.ok_or_else(Failure::length_required)?;
        let bound = PutAttenuation::of(&attenuated);
        if Blake3Hash::hash(payload) != bound.digest || Checksum::sha256(payload) != bound.checksum
        {
            return Err(Failure::checksum_mismatch());
        }
        let capability = Subject::from(subject.clone())
            .attenuate(Use)
            .attenuate(archive::Archive)
            .attenuate(Catalog::of(&attenuated).clone())
            .invoke(archive::Put::new(Buffer::from(payload.to_vec())));
        Provider::<archive::Put>::execute(&self.provider, capability).await?;
        Ok(Response::status(200))
    }

    async fn resolve(&self, subject: &Did, args: &Args) -> Result<Response, Failure>
    where
        P: Store,
    {
        let capability = memory::Resolve::capability_from_args(subject, args)?;
        match Provider::<memory::Resolve>::execute(&self.provider, capability).await {
            Ok(Some(edition)) => Ok(Response::versioned(
                200,
                render(&edition.version)?,
                edition.content,
            )),
            Ok(None) => Ok(Response::status(404)),
            Err(error) => Err(Failure::from(error)),
        }
    }

    async fn publish(
        &self,
        subject: &Did,
        args: &Args,
        payload: Option<&[u8]>,
    ) -> Result<Response, Failure>
    where
        P: Store,
    {
        let attenuated = memory::Publish::capability_from_args(subject, args)?;
        let payload = payload.ok_or_else(Failure::length_required)?;
        let bound = PublishAttenuation::of(&attenuated);
        if Checksum::sha256(payload) != bound.checksum {
            return Err(Failure::checksum_mismatch());
        }
        let capability = Subject::from(subject.clone())
            .attenuate(Use)
            .attenuate(memory::Memory)
            .attenuate(Space::of(&attenuated).clone())
            .attenuate(Cell::of(&attenuated).clone())
            .invoke(memory::Publish::new(payload.to_vec(), bound.when.clone()));
        let version = Provider::<memory::Publish>::execute(&self.provider, capability).await?;
        Ok(Response::versioned(200, render(&version)?, Vec::new()))
    }

    async fn retract(&self, subject: &Did, args: &Args) -> Result<Response, Failure>
    where
        P: Store,
    {
        let capability = memory::Retract::capability_from_args(subject, args)?;
        Provider::<memory::Retract>::execute(&self.provider, capability).await?;
        Ok(Response::status(204))
    }
}

/// A version as the `ETag` it travels as. A provider's versions are
/// the strings its `ETag`s carry, as bytes, so the version a client
/// echoes back in a precondition is the one the provider compares.
fn render(version: &memory::Version) -> Result<String, Failure> {
    String::from_utf8(version.as_bytes().to_vec()).map_err(|_| {
        Failure::Answered(Response::rejected(
            500,
            Rejection::Unclassified {
                detail: "the provider's version is not a text ETag".to_string(),
            },
        ))
    })
}

/// Why an operation did not run to its outcome: the invocation was
/// refused after all (the provider's own authority check), or the
/// request was answered on other grounds.
enum Failure {
    Refused(Refusal),
    Answered(Response),
}

impl Failure {
    fn length_required() -> Self {
        Self::Answered(Response::json(
            411,
            &serde_json::json!({
                "kind": "LengthRequired",
                "detail": "a write must carry the bytes it stores in the container's payload",
            }),
        ))
    }

    fn checksum_mismatch() -> Self {
        Self::Answered(Response::json(
            400,
            &serde_json::json!({
                "kind": "ChecksumMismatch",
                "detail": "the payload does not hash to the digest the invocation binds",
            }),
        ))
    }

    fn storage(detail: String) -> Self {
        Self::Answered(Response::rejected(
            503,
            Rejection::Unavailable { reason: detail },
        ))
    }
}

impl From<dialog_remote_s3::S3Error> for Failure {
    fn from(error: dialog_remote_s3::S3Error) -> Self {
        match error {
            dialog_remote_s3::S3Error::Authorization(reason) => Self::Refused(Refusal(reason)),
            dialog_remote_s3::S3Error::Rejected(rejection) => {
                Self::Answered(Response::rejected(400, rejection))
            }
            other => Self::Answered(Response::json(
                400,
                &Rejection::Unclassified {
                    detail: other.to_string(),
                },
            )),
        }
    }
}

impl From<archive::ArchiveError> for Failure {
    fn from(error: archive::ArchiveError) -> Self {
        match error {
            archive::ArchiveError::Authorization(reason) => Self::Refused(Refusal(reason)),
            archive::ArchiveError::Rejected(rejection) => {
                Self::Answered(Response::rejected(503, rejection))
            }
            archive::ArchiveError::Storage(detail) => Self::storage(detail),
        }
    }
}

impl From<memory::MemoryError> for Failure {
    fn from(error: memory::MemoryError) -> Self {
        match error {
            memory::MemoryError::VersionMismatch { .. } => Self::Answered(Response::status(412)),
            memory::MemoryError::Authorization(reason) => Self::Refused(Refusal(reason)),
            memory::MemoryError::Rejected(rejection) => {
                Self::Answered(Response::rejected(503, rejection))
            }
            memory::MemoryError::Storage(detail) => Self::storage(detail),
        }
    }
}
