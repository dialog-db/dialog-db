//! The server side: decode the invocation, verify it, and perform it.
//!
//! An embedder brings a [`Provider`] of the effects a remote performs,
//! the same traits a local store implements, and wraps it in
//! [`Access`]. Every request then goes through one path: the container
//! is read out of the `Authorization` header, the invocation's chain is
//! verified against the resolver and the revocation checker the
//! embedder configured, the operation it authorizes is read back as a
//! capability, and only then is the provider called with that
//! capability. A write's bytes are the request body; they are checked
//! against the digest and checksum the invocation bound before the
//! provider sees them, and a blob's bytes are streamed into the
//! provider's sink as they arrive.
//!
//! The layer speaks no HTTP framework. It takes the request's
//! `Authorization` value and its body and answers with a status, a
//! content type, a version and a body, which the embedder relays
//! however it serves HTTP.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dialog_capability::access::AuthorizeError;
use dialog_capability::{Did, Policy, Provider, Subject};
use dialog_common::time::{self, UNIX_EPOCH};
use dialog_common::{Blake3Hash, Buffer, Checksum, ConditionalSync};
use dialog_did_web::{CachingResolver, Resolve, WebResolver};
use dialog_effects::MethodExt as _;
use dialog_effects::Rejection;
use dialog_effects::archive::prelude::ArchiveExt as _;
use dialog_effects::archive::{self, Catalog, PutAttenuation};
use dialog_effects::blob::prelude::{BlobImportExt as _, BlobReadExt as _};
use dialog_effects::blob::{self, BlobError, BlobReader};
use dialog_effects::memory::prelude::MemoryExt as _;
use dialog_effects::memory::{self, Cell, PublishAttenuation, Space};
use dialog_remote_ucan_s3::{Args, FromUcanArgs, verify_invocation, verify_invocation_at};
use dialog_ucan_core::revocation::RevocationChecker;
use dialog_ucan_core::{Container, InvocationChain, UnverifiedRevocations};
use dialog_varsig::AnySignature;
use serde::{Deserialize, Serialize};

use crate::direct::{OBJECT_MEDIA_TYPE, credential_container, is_credential};

/// The media type of a refusal's body: the reason, as JSON, in the
/// shape the client reads back.
pub const REFUSAL_MEDIA_TYPE: &str = "application/json";

/// How long after it was issued an invocation is accepted, and how far
/// ahead of the service's clock its issue time may be: an invocation is
/// good when it is made, and the window allows for its trip and for
/// clocks that disagree.
pub const FRESHNESS: Duration = Duration::from_secs(30);

/// Whether an invocation must say when it was issued.
///
/// One that says is refused once it is older than the freshness window,
/// whichever way it arrives. One that does not say cannot be aged, so
/// where a captured invocation would grant lasting access (a watch,
/// which follows a cell for as long as its chain holds) saying is
/// required, and elsewhere it is not, so clients that predate it are
/// still served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Issuance {
    /// An invocation that does not say when it was issued is refused.
    Required,
    /// An invocation that does not say when it was issued is accepted.
    Optional,
}

/// The invocations a service accepted lately, remembered so that one
/// presented again is refused.
///
/// An invocation carries a nonce, so one made again is never the same as
/// one made before: an invocation presented twice was copied. It need
/// only be remembered while it is fresh, since after that it is refused
/// as stale anyway.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Presented: ConditionalSync {
    /// Remember `invocation`, its content identifier, until `until` (Unix
    /// seconds), answering `false` when it is remembered already.
    async fn record(&self, invocation: &str, until: u64) -> bool;
}

/// Presented invocations remembered in the memory of one process: what
/// a service that runs in one process, or keeps one instance per space,
/// needs. A service spread over many processes shares a record they all
/// reach instead.
#[derive(Debug, Default)]
pub struct RecentInvocations(Mutex<HashMap<String, u64>>);

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Presented for RecentInvocations {
    async fn record(&self, invocation: &str, until: u64) -> bool {
        let now = now_s();
        let mut presented = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        presented.retain(|_, remembered| *remembered >= now);
        if presented.contains_key(invocation) {
            return false;
        }
        presented.insert(invocation.to_string(), until);
        true
    }
}

/// An access service over a provider of the effects it performs.
pub struct Access<P, Resolver = CachingResolver<WebResolver>, Revocations = UnverifiedRevocations> {
    provider: P,
    resolver: Arc<Resolver>,
    revocations: Arc<Revocations>,
    freshness: Duration,
    presented: Arc<dyn Presented>,
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
            freshness: FRESHNESS,
            presented: Arc::new(RecentInvocations::default()),
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
            freshness: self.freshness,
            presented: self.presented,
        }
    }

    /// The same service, accepting an invocation for `freshness` after it
    /// was issued (see [`FRESHNESS`]).
    pub fn with_freshness(mut self, freshness: Duration) -> Self {
        self.freshness = freshness;
        self
    }

    /// The same service, remembering the invocations it accepts in
    /// `presented`: an embedder that builds a service per request keeps
    /// one record across them.
    pub fn with_presented(mut self, presented: Arc<dyn Presented>) -> Self {
        self.presented = presented;
        self
    }

    /// The provider the service performs operations with.
    pub fn provider(&self) -> &P {
        &self.provider
    }
}

/// A request body: the bytes a write stores, whole or as they arrive.
pub enum Payload {
    /// The whole body.
    Bytes(Vec<u8>),
    /// The body as chunks, read as the operation consumes them.
    Stream(BlobReader),
}

impl Payload {
    /// The whole body, read to its end.
    pub async fn collect(self) -> Result<Vec<u8>, BlobError> {
        match self {
            Payload::Bytes(bytes) => Ok(bytes),
            Payload::Stream(mut source) => {
                let mut bytes = Vec::new();
                while let Some(chunk) = source.next().await? {
                    bytes.extend_from_slice(&chunk);
                }
                Ok(bytes)
            }
        }
    }
}

impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Payload::Bytes(bytes) => f.debug_tuple("Bytes").field(&bytes.len()).finish(),
            Payload::Stream(_) => f.write_str("Stream"),
        }
    }
}

impl From<Vec<u8>> for Payload {
    fn from(bytes: Vec<u8>) -> Self {
        Payload::Bytes(bytes)
    }
}

impl From<BlobReader> for Payload {
    fn from(source: BlobReader) -> Self {
        Payload::Stream(source)
    }
}

/// What the layer needs from an HTTP request: its `Authorization`
/// value, and its body.
#[derive(Debug)]
pub struct Request<'a> {
    authorization: Option<&'a str>,
    payload: Payload,
}

impl<'a> Request<'a> {
    /// A request with `authorization` as its `Authorization` header
    /// value, and no body.
    pub fn new(authorization: Option<&'a str>) -> Self {
        Self {
            authorization,
            payload: Payload::Bytes(Vec::new()),
        }
    }

    /// The same request with its body.
    pub fn payload(mut self, payload: impl Into<Payload>) -> Self {
        self.payload = payload.into();
        self
    }

    /// Whether the request carries an invocation for this layer: an
    /// `Authorization` value under the UCAN scheme.
    pub fn is_credentialed(&self) -> bool {
        self.authorization.is_some_and(is_credential)
    }
}

/// A response body: whole, or as chunks a blob read yields.
pub enum Content {
    /// The whole body.
    Bytes(Vec<u8>),
    /// The body as chunks.
    Stream(BlobReader),
}

impl Content {
    /// The whole body, read to its end.
    pub async fn collect(self) -> Result<Vec<u8>, BlobError> {
        match self {
            Content::Bytes(bytes) => Ok(bytes),
            Content::Stream(mut source) => {
                let mut bytes = Vec::new();
                while let Some(chunk) = source.next().await? {
                    bytes.extend_from_slice(&chunk);
                }
                Ok(bytes)
            }
        }
    }
}

impl std::fmt::Debug for Content {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Content::Bytes(bytes) => f.debug_tuple("Bytes").field(&bytes.len()).finish(),
            Content::Stream(_) => f.write_str("Stream"),
        }
    }
}

/// What the layer answers, for the embedder to relay: the status the
/// object route would have given, the content type, the object's
/// version when the operation has one (an `ETag`), the body's length
/// when it is known ahead of the body, and the body.
#[derive(Debug)]
pub struct Response {
    /// The HTTP status.
    pub status: u16,
    /// The body's media type.
    pub content_type: &'static str,
    /// The object's version, for the `ETag` header.
    pub version: Option<String>,
    /// The body's length, when known before the body is read.
    pub length: Option<u64>,
    /// The body.
    pub body: Content,
}

impl Response {
    fn outcome(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: OBJECT_MEDIA_TYPE,
            version: None,
            length: Some(body.len() as u64),
            body: Content::Bytes(body),
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

    fn stream(status: u16, length: Option<u64>, body: BlobReader) -> Self {
        Self {
            status,
            content_type: OBJECT_MEDIA_TYPE,
            version: None,
            length,
            body: Content::Stream(body),
        }
    }

    fn json(status: u16, value: &impl Serialize) -> Self {
        let body = serde_json::to_vec(value).unwrap_or_default();
        Self {
            status,
            content_type: REFUSAL_MEDIA_TYPE,
            version: None,
            length: Some(body.len() as u64),
            body: Content::Bytes(body),
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
            | AuthorizeError::NotValidBefore { .. }
            | AuthorizeError::Stale { .. }
            | AuthorizeError::Replayed { .. } => 401,
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

/// An invocation that verified.
#[derive(Debug)]
pub struct Verified {
    chain: InvocationChain<AnySignature>,
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
}

/// A watch the service accepted: the cell it follows, and the invocation
/// that authorized it, kept so that its authority can be checked again
/// for as long as the watch runs.
///
/// Plain data, so a service that keeps its watches between messages (a
/// Durable Object that hibernates, say) stores it and reads it back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscription {
    invocation: String,
    subject: Did,
    space: String,
    cell: String,
    container: Vec<u8>,
    checked: u64,
}

impl Subscription {
    /// The watch invocation's content identifier, which names the watch.
    pub fn invocation(&self) -> &str {
        &self.invocation
    }

    /// Whether the watch follows the cell `cell` in `space` of `subject`.
    pub fn follows(&self, subject: &Did, space: &str, cell: &str) -> bool {
        &self.subject == subject && self.space == space && self.cell == cell
    }

    /// When the watch's authority was last found to hold, in Unix seconds.
    pub fn checked(&self) -> u64 {
        self.checked
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
    /// The layer does not answer this request: it carries no invocation
    /// under the UCAN scheme, or names an operation the layer does not
    /// perform. The embedder answers it its own way.
    Unsupported,
}

/// The effects a provider performs for the layer.
pub trait Store:
    Provider<archive::Get>
    + Provider<archive::Put>
    + Provider<blob::Read>
    + Provider<blob::Import>
    + Provider<memory::Resolve>
    + Provider<memory::Publish>
    + Provider<memory::Retract>
    + ConditionalSync
{
}

impl<T> Store for T where
    T: Provider<archive::Get>
        + Provider<archive::Put>
        + Provider<blob::Read>
        + Provider<blob::Import>
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
    /// Verify the invocation `container` carries, accepting one that does
    /// not say when it was issued.
    pub async fn verify(&self, container: Container) -> Result<Verified, Refusal> {
        self.admit(container, Issuance::Optional).await
    }

    /// Verify the invocation `container` carries, and accept it only when
    /// it is fresh and was not presented before.
    ///
    /// The issue time and the record are consulted after the chain
    /// verifies, so an invocation is only ever refused as stale or as
    /// replayed when it is authentic.
    pub async fn admit(
        &self,
        container: Container,
        issuance: Issuance,
    ) -> Result<Verified, Refusal> {
        let chain =
            verify_invocation(container, self.resolver.as_ref(), &*self.revocations).await?;
        let at = now_s();
        let window = self.freshness.as_secs();
        let issued_at = chain.invocation.issued_at().map(|issued| issued.to_unix());
        let fresh = match issued_at {
            Some(issued) => {
                issued.saturating_add(window) >= at && issued <= at.saturating_add(window)
            }
            None => issuance == Issuance::Optional,
        };
        if !fresh {
            return Err(AuthorizeError::Stale { issued_at, at }.into());
        }
        let invocation = chain.invocation.to_cid().to_string();
        let until = issued_at.unwrap_or(at).saturating_add(window);
        if !self.presented.record(&invocation, until).await {
            return Err(AuthorizeError::Replayed { invocation }.into());
        }
        Ok(Verified { chain })
    }

    /// Accept a watch: admit the invocation, which must say when it was
    /// issued (see [`Issuance`]), and answer the subscription with what
    /// the cell holds now.
    ///
    /// A watch is authority that lasts, so an invocation that could not
    /// be aged is not accepted for one. Anything that is not a watch, or
    /// does not verify, is answered as a request would be.
    pub async fn subscribe(
        &self,
        container: Container,
    ) -> Result<(Subscription, memory::CellState), Answer>
    where
        P: Store,
    {
        let bytes = container.to_bytes().map_err(|error| {
            Answer::Refused(Refusal(AuthorizeError::Malformed {
                detail: error.to_string(),
            }))
        })?;
        let verified = self
            .admit(container, Issuance::Required)
            .await
            .map_err(Answer::Refused)?;
        if verified.command() != ["use", "get", "memory", "cell", "watch"] {
            return Err(Answer::Unsupported);
        }
        let subject = verified.subject().clone();
        let watch = memory::Watch::capability_from_args(&subject, verified.chain().arguments())
            .map_err(|error| {
                Answer::Refused(Refusal(AuthorizeError::Malformed {
                    detail: error.to_string(),
                }))
            })?;
        let space = Space::<dialog_effects::method::Get>::of(&watch)
            .space
            .clone();
        let cell = Cell::<dialog_effects::method::Get>::of(&watch).cell.clone();
        // What the cell holds now is what a resolve of it would read: the
        // same cell, named by the same arguments.
        let resolve = memory::Resolve::capability_from_args(&subject, verified.chain().arguments())
            .map_err(|error| {
                Answer::Refused(Refusal(AuthorizeError::Malformed {
                    detail: error.to_string(),
                }))
            })?;
        let state = match Provider::<memory::Resolve>::execute(&self.provider, resolve).await {
            Ok(state) => state,
            Err(error) => {
                return Err(match Failure::from(error) {
                    Failure::Refused(refusal) => Answer::Refused(refusal),
                    Failure::Answered(response) => Answer::Performed(response),
                });
            }
        };
        let subscription = Subscription {
            invocation: verified.chain().invocation.to_cid().to_string(),
            subject,
            space,
            cell,
            container: bytes,
            checked: now_s(),
        };
        Ok((subscription, state))
    }

    /// Check, before delivering to `subscription` at `at` (Unix seconds),
    /// that the authority behind the watch still holds, when it was last
    /// found to hold more than `interval` before.
    ///
    /// The invocation's chain is verified again as of `at`: a delegation
    /// that lapsed or was revoked since the watch began is refused, as
    /// [`AuthorizeError::Expired`] or [`AuthorizeError::Revoked`], and so
    /// is the watch's own invocation once its `exp` passes. Its issue time
    /// is not judged again: it was fresh when the watch began. Checking
    /// only when there is something to deliver means a watch nothing
    /// happens to costs nothing, and that what reaches a watcher after its
    /// authority ends is bounded by `interval`.
    pub async fn recheck(
        &self,
        subscription: &mut Subscription,
        interval: Duration,
        at: u64,
    ) -> Result<(), Refusal> {
        if subscription.checked.saturating_add(interval.as_secs()) > at {
            return Ok(());
        }
        let container = Container::from_bytes(&subscription.container).map_err(|error| {
            Refusal(AuthorizeError::Malformed {
                detail: error.to_string(),
            })
        })?;
        let time = dialog_ucan_core::time::Timestamp::new(
            dialog_ucan_core::time::timestamp::UNIX_EPOCH
                + dialog_ucan_core::time::timestamp::Duration::from_secs(at),
        )
        .map_err(|error| {
            Refusal(AuthorizeError::Unavailable {
                detail: error.to_string(),
            })
        })?;
        verify_invocation_at(container, self.resolver.as_ref(), &*self.revocations, time).await?;
        subscription.checked = at;
        Ok(())
    }

    /// Answer a request: read the invocation out of its `Authorization`
    /// value, verify it, and perform it with the body.
    pub async fn handle(&self, request: Request<'_>) -> Answer
    where
        P: Store,
    {
        let Request {
            authorization,
            payload,
        } = request;
        let Some(credential) = authorization.filter(|value| is_credential(value)) else {
            return Answer::Unsupported;
        };
        let container = match credential_container(credential) {
            Ok(container) => container,
            Err(error) => {
                return Answer::Refused(Refusal(AuthorizeError::Malformed {
                    detail: error.to_string(),
                }));
            }
        };
        match self.verify(container).await {
            Ok(verified) => self.perform(verified, payload).await,
            Err(refusal) => Answer::Refused(refusal),
        }
    }

    /// Perform a verified invocation with the provider, `payload` being
    /// the request body.
    pub async fn perform(&self, verified: Verified, payload: Payload) -> Answer
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
                self.put(&subject, args, payload).await
            }
            ["use", "get", "archive", "blob"] => self.read_blob(&subject, args).await,
            ["use", "put", "archive", "blob"] => self.import_blob(&subject, args, payload).await,
            ["use", "get", "memory", "cell"] | ["memory", "resolve"] => {
                self.resolve(&subject, args).await
            }
            ["use", "put", "memory", "cell"] | ["memory", "publish"] => {
                self.publish(&subject, args, payload).await
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

    async fn put(&self, subject: &Did, args: &Args, payload: Payload) -> Result<Response, Failure>
    where
        P: Store,
    {
        let attenuated = archive::Put::capability_from_args(subject, args)?;
        let payload = payload.collect().await.map_err(Failure::unreadable)?;
        if payload.is_empty() {
            return Err(Failure::length_required());
        }
        let bound = PutAttenuation::of(&attenuated);
        if Blake3Hash::hash(&payload) != bound.digest
            || Checksum::sha256(&payload) != bound.checksum
        {
            return Err(Failure::checksum_mismatch());
        }
        let capability = Subject::from(subject.clone())
            .writer()
            .archive()
            .attenuate(Catalog::of(&attenuated).clone())
            .attenuate(archive::Block::new())
            .invoke(archive::Put::new(Buffer::from(payload)));
        Provider::<archive::Put>::execute(&self.provider, capability).await?;
        Ok(Response::status(200))
    }

    async fn read_blob(&self, subject: &Did, args: &Args) -> Result<Response, Failure>
    where
        P: Store,
    {
        let capability = blob::Read::capability_from_args(subject, args)?;
        let range = capability.range();
        let source = Provider::<blob::Read>::execute(&self.provider, capability).await?;
        match range {
            Some(range) => Ok(Response::stream(206, range.length, source)),
            None => Ok(Response::stream(200, None, source)),
        }
    }

    async fn import_blob(
        &self,
        subject: &Did,
        args: &Args,
        payload: Payload,
    ) -> Result<Response, Failure>
    where
        P: Store,
    {
        let capability = blob::Import::capability_from_args(subject, args)?;
        let declared = capability.size();
        let mut sink = Provider::<blob::Import>::execute(&self.provider, capability).await?;
        let mut written = 0u64;
        match payload {
            Payload::Bytes(bytes) => {
                written = bytes.len() as u64;
                sink.write_all(&bytes).await?;
            }
            Payload::Stream(mut source) => {
                while let Some(chunk) = source.next().await.map_err(Failure::unreadable)? {
                    written += chunk.len() as u64;
                    sink.write_all(&chunk).await?;
                }
            }
        }
        if written != declared {
            return Err(Failure::size_mismatch(declared, written));
        }
        sink.finish().await?;
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
        payload: Payload,
    ) -> Result<Response, Failure>
    where
        P: Store,
    {
        let attenuated = memory::Publish::capability_from_args(subject, args)?;
        let payload = payload.collect().await.map_err(Failure::unreadable)?;
        if payload.is_empty() {
            return Err(Failure::length_required());
        }
        let bound = PublishAttenuation::of(&attenuated);
        if Checksum::sha256(&payload) != bound.checksum {
            return Err(Failure::checksum_mismatch());
        }
        let capability = Subject::from(subject.clone())
            .writer()
            .memory()
            .attenuate(Space::of(&attenuated).clone())
            .attenuate(Cell::of(&attenuated).clone())
            .invoke(memory::Publish::new(payload, bound.when.clone()));
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
                "detail": "a write must carry the bytes it stores as the request body",
            }),
        ))
    }

    fn unreadable(error: BlobError) -> Self {
        Self::Answered(Response::json(
            400,
            &serde_json::json!({
                "kind": "UnreadableBody",
                "detail": format!("the request body could not be read: {error}"),
            }),
        ))
    }

    fn checksum_mismatch() -> Self {
        Self::Answered(Response::json(
            400,
            &serde_json::json!({
                "kind": "ChecksumMismatch",
                "detail": "the body does not hash to the digest the invocation binds",
            }),
        ))
    }

    fn size_mismatch(declared: u64, written: u64) -> Self {
        Self::Answered(Response::json(
            400,
            &serde_json::json!({
                "kind": "SizeMismatch",
                "detail": format!("the invocation declares {declared} bytes, the body carried {written}"),
            }),
        ))
    }

    fn digest_mismatch(expected: String, actual: String) -> Self {
        Self::Answered(Response::json(
            400,
            &serde_json::json!({
                "kind": "DigestMismatch",
                "detail": format!("the body hashes to {actual}, the invocation declares {expected}"),
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

impl From<BlobError> for Failure {
    fn from(error: BlobError) -> Self {
        match error {
            BlobError::NotFound(_) => Self::Answered(Response::status(404)),
            BlobError::DigestMismatch { expected, actual } => {
                Self::digest_mismatch(expected, actual)
            }
            BlobError::SizeMismatch {
                digest,
                expected,
                held,
            } => Self::Answered(Response::json(
                400,
                &serde_json::json!({
                    "kind": "SizeMismatch",
                    "detail": format!("blob {digest} was named at {expected} bytes, {held} are held"),
                }),
            )),
            BlobError::Authorization(reason) => Self::Refused(Refusal(reason)),
            BlobError::Rejected(rejection) => Self::Answered(Response::rejected(503, rejection)),
            BlobError::Storage(detail) => Self::storage(detail),
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

/// The service's clock, in Unix seconds.
fn now_s() -> u64 {
    time::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default()
}
