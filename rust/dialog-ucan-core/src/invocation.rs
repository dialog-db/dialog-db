//! UCAN Invocation
//!
//! The spec for UCAN Invocations can be found at
//! [the GitHub repo](https://github.com/ucan-wg/invocation/).

pub mod builder;

use crate::{
    Delegation,
    cid::to_dagcbor_cid,
    command::Command,
    crypto::nonce::Nonce,
    delegation::{
        policy::predicate::{Predicate, RunError},
        store::DelegationStore,
    },
    envelope::{Envelope, EnvelopePayload, payload_tag::PayloadTag},
    future::FutureKind,
    promise::{Promised, WaitingOn},
    time::{TimeBoundError, range::TimeRange, timestamp::Timestamp},
};
use crate::{
    delegation::SignatureVerificationError as DelegationSignatureError,
    delegation::chain::check_chain,
    revocation::{RevocationChecker, RevocationMatch, RevocationSelector},
    verification::{Verifiable, VerificationContext},
};
use builder::InvocationBuilder;
use dialog_varsig::{Did, Resolver, Signature, Verifier};
use futures::{StreamExt, stream::FuturesUnordered};
use ipld_core::{cid::Cid, ipld::Ipld};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, Visitor},
};
use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Debug,
};
use thiserror::Error;

/// Request to perform a UCAN-authorized action.
///
/// This type implements the [UCAN Invocation spec](https://github.com/ucan-wg/invocation/blob/main/README.md).
/// An invocation references one or more [`Delegation`] proofs that authorize it.
#[derive(Clone)]
pub struct Invocation<S: Signature>(Envelope<S, InvocationPayload>);

impl<S: Signature> Invocation<S> {
    /// Creates a blank [`InvocationBuilder`] instance.
    #[must_use]
    pub const fn builder() -> InvocationBuilder<S> {
        InvocationBuilder::new()
    }

    /// Getter for the `issuer` field.
    #[must_use]
    pub const fn issuer(&self) -> &Did {
        &self.payload().issuer
    }

    /// Getter for the `audience` field.
    /// Returns the subject if no explicit audience was set.
    #[must_use]
    pub fn audience(&self) -> &Did {
        self.payload().audience()
    }

    /// Getter for the `subject` field.
    #[must_use]
    pub const fn subject(&self) -> &Did {
        &self.payload().subject
    }

    /// Getter for the `command` field.
    #[must_use]
    pub const fn command(&self) -> &Command {
        &self.payload().command
    }

    /// Getter for the `arguments` field.
    #[must_use]
    pub const fn arguments(&self) -> &BTreeMap<String, Promised> {
        &self.payload().arguments
    }

    /// Getter for the `proofs` field.
    #[must_use]
    pub const fn proofs(&self) -> &Vec<Cid> {
        &self.payload().proofs
    }

    /// Getter for the `cause` field.
    #[must_use]
    pub const fn cause(&self) -> Option<Cid> {
        self.payload().cause
    }

    /// Getter for the `expiration` field.
    #[must_use]
    pub const fn expiration(&self) -> Option<Timestamp> {
        self.payload().expiration
    }

    /// Getter for the `meta` field. Returns an empty map when meta is absent.
    #[must_use]
    pub fn meta(&self) -> &BTreeMap<String, Ipld> {
        static EMPTY: BTreeMap<String, Ipld> = BTreeMap::new();
        self.payload().meta.as_ref().unwrap_or(&EMPTY)
    }

    /// Getter for the `nonce` field.
    #[must_use]
    pub const fn nonce(&self) -> &Nonce {
        &self.payload().nonce
    }

    /// Compute the CID for this invocation.
    #[must_use]
    pub fn to_cid(&self) -> Cid {
        to_dagcbor_cid(&self)
    }

    /// Verify this invocation and its whole proof chain.
    ///
    /// Runs in three phases, ordered so that cheap refusals cost nothing and
    /// expensive work is shared and overlapped:
    ///
    /// 1. **Structural, no I/O.** Proof lookup and
    ///    [`syntactic_checks`](InvocationPayload::syntactic_checks): alignment,
    ///    attenuation, policy, and time bounds against the context's sampled
    ///    instant. A chain that fails here spends no network calls and no
    ///    crypto.
    /// 2. **Resolution, concurrent and deduplicated.** Every distinct issuer
    ///    DID is resolved exactly once, all in flight together. Deduplication
    ///    is load-bearing rather than an optimization: the caching resolver
    ///    has no in-flight dedup, so resolving per link concurrently would
    ///    turn one fetch into N for a DID that appears more than once — and
    ///    repeated issuers are the normal case.
    /// 3. **Signatures and revocations, concurrent.** Every link's signature
    ///    (against its already-resolved verifier) and every link's revocation
    ///    query are driven together. The first decisive refusal returns at
    ///    once, dropping the rest, which cancels the in-flight work.
    ///
    /// Verification is strict: a revocation query that cannot be performed
    /// fails the chain rather than proceeding as though the delegation stood.
    /// A caller willing to proceed without that evidence says so by wrapping
    /// the checker (see
    /// [`tolerate_unavailable`](crate::RevocationChecker::tolerate_unavailable)),
    /// which is a choice made where the environment is built.
    ///
    /// # Errors
    ///
    /// Returns a [`VerifyError`] if the chain does not check out,
    /// a signature does not verify, an issuer cannot be resolved, or a link
    /// has been revoked.
    pub async fn check<K, T, St, C>(
        &self,
        ctx: &VerificationContext<'_, C>,
    ) -> Result<
        TimeRange,
        VerifyError<
            K,
            S,
            T,
            St,
            <C::Resolver as Resolver<S>>::Error,
            <C::Revocations as RevocationChecker>::Error,
        >,
    >
    where
        K: FutureKind,
        T: Borrow<Delegation<S>>,
        St: DelegationStore<K, S, T>,
        C: Verifiable<K, S, Proof = T, Delegations = St>,
        // Cloned when a resolution failure is reported: several links may
        // name the same unresolvable issuer.
        <C::Resolver as Resolver<S>>::Error: Clone,
    {
        // Phase 1: structure. Nothing below runs if this fails.
        let (proofs, range) = self
            .payload()
            .check::<K, S, T, St, C, _>(ctx)
            .await
            .map_err(VerifyError::Invalid)?;

        let delegations: Vec<&Delegation<S>> = proofs.iter().map(Borrow::borrow).collect();

        // Phase 2 and the revocation half of phase 3 are independent, so they
        // run together rather than in sequence: revocation queries do not
        // need any resolved key.
        // Who may revoke each link.
        //
        // The revocation spec's pseudocode computes one `delegators` set for
        // the whole chain:
        //
        //     const delegators = invocation.prf.map(proof => proof.iss)
        //
        // Applied uniformly that is too permissive: for a chain a -> b -> c -> d
        // it lets d's issuer revoke c, so a principal who merely *received*
        // authority could revoke the grant it depends on. Authority to revoke
        // flows downward, so the candidate set is scoped per link: the issuers
        // at or above it, plus that link's own audience (its immediate
        // recipient, who may always disclaim what it was given).
        //
        //     check a  => [a.iss, a.aud]
        //     check c  => [a.iss, b.iss, c.iss, c.aud]
        //
        // d.iss never appears when checking c.
        let mut prefix: Vec<Did> = Vec::with_capacity(delegations.len() + 1);
        let revokers_per_link: Vec<Vec<Did>> = delegations
            .iter()
            .map(|delegation| {
                prefix.push(delegation.issuer().clone());
                let mut candidates = prefix.clone();
                candidates.push(delegation.audience().clone());
                candidates
            })
            .collect();

        let mut revocations = FuturesUnordered::new();
        for (delegation, revokers) in delegations.iter().zip(&revokers_per_link) {
            let cid = delegation.to_cid();
            let revokers = revokers.as_slice();
            revocations.push(async move {
                match ctx
                    .environment()
                    .revocations()
                    .query(RevocationSelector::new(cid, revokers))
                    .await
                {
                    Ok(None) => Ok(()),
                    Ok(Some(found)) => Err(VerifyError::<K, S, T, St, _, _>::Invalid(
                        Invalid::Revoked { cid, found },
                    )),
                    // The question went unanswered. A caller willing to
                    // proceed without the answer says so by wrapping the
                    // checker, not by us guessing here.
                    Err(source) => Err(VerifyError::Unavailable(Unavailable::RevocationLookup {
                        cid,
                        source,
                    })),
                }
            });
        }

        // Resolve each distinct issuer exactly once, while the revocation
        // lookups above are already in flight.
        let mut wanted: BTreeSet<&Did> = delegations.iter().map(|d| d.issuer()).collect();
        wanted.insert(self.issuer());
        let verifiers = resolve_each_once(ctx.environment().resolver(), wanted).await;

        let verifier_for = |did: &Did| {
            verifiers
                .get(did)
                .expect("every issuer was collected before resolving")
                .as_ref()
        };

        // Two homogeneous queues rather than one boxed queue: the two kinds
        // of check have different future types, and boxing them into a single
        // stream would make the whole verification `!Send`. They are still
        // driven together below.
        let mut signatures = FuturesUnordered::new();

        for delegation in delegations.iter().map(Some).chain(std::iter::once(None)) {
            // `None` stands for the invocation's own signature, checked
            // alongside the links rather than before them.
            let issuer = delegation.map_or_else(|| self.issuer(), |d| d.issuer());
            signatures.push(async move {
                let verifier = match verifier_for(issuer) {
                    Ok(verifier) => verifier,
                    // Without the key the signature cannot be checked, so the
                    // chain is unproven — not merely un-revocation-checked.
                    // No tolerance setting can accept that.
                    Err(detail) => {
                        return Err(VerifyError::Unavailable(Unavailable::DidResolution {
                            did: issuer.clone(),
                            detail: detail.clone(),
                        }));
                    }
                };

                match delegation {
                    Some(delegation) => delegation
                        .verify_with::<<C::Resolver as Resolver<S>>::Error>(verifier)
                        .await
                        .map_err(|source| {
                            VerifyError::Invalid(Invalid::DelegationSignature {
                                issuer: issuer.clone(),
                                source,
                            })
                        }),
                    None => self
                        .verify_with::<<C::Resolver as Resolver<S>>::Error>(verifier)
                        .await
                        .map_err(|err| VerifyError::Invalid(Invalid::InvocationSignature(err))),
                }
            });
        }

        // Drive both queues together, aborting on the first refusal.
        // Dropping them cancels whatever is still in flight.
        loop {
            futures::select_biased! {
                result = signatures.select_next_some() => result?,
                result = revocations.select_next_some() => result?,
                complete => break,
            }
        }

        Ok(range)
    }

    #[must_use]
    const fn signature(&self) -> &S {
        &self.0.0
    }

    #[must_use]
    const fn envelope(&self) -> &EnvelopePayload<S, InvocationPayload> {
        &self.0.1
    }

    #[must_use]
    const fn payload(&self) -> &InvocationPayload {
        &self.envelope().payload
    }

    /// Verify only the signature of this invocation using a resolver.
    ///
    /// The resolver resolves the issuer DID to a verifier, then verifies
    /// the signature.
    ///
    /// # Errors
    ///
    /// Returns a [`SignatureVerificationError`] if signature verification fails.
    pub async fn verify_signature<R>(
        &self,
        resolver: &R,
    ) -> Result<(), SignatureVerificationError<R::Error>>
    where
        R: Resolver<S>,
    {
        let verifier = resolver
            .resolve(self.issuer())
            .await
            .map_err(SignatureVerificationError::ResolutionError)?;
        self.verify_with::<R::Error>(&verifier).await
    }

    /// Verify this invocation's signature against an already-resolved verifier.
    ///
    /// Split out from [`verify_signature`](Self::verify_signature) so a chain
    /// can resolve each distinct issuer DID once and share the verifier with
    /// every delegation link that names the same issuer.
    ///
    /// # Errors
    ///
    /// Returns a [`SignatureVerificationError`] if the payload cannot be
    /// encoded or the signature does not verify.
    pub async fn verify_with<E: std::error::Error>(
        &self,
        verifier: &impl Verifier<S>,
    ) -> Result<(), SignatureVerificationError<E>> {
        let encoded = self
            .envelope()
            .encode()
            .map_err(SignatureVerificationError::EncodingError)?;
        Verifier::verify(verifier, &encoded, self.signature())
            .await
            .map_err(SignatureVerificationError::VerificationError)
    }
}

impl<S: Signature> Debug for Invocation<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Invocation").field(&self.0).finish()
    }
}

impl<S: Signature> Serialize for Invocation<S> {
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, S: Signature + for<'ze> Deserialize<'ze>> Deserialize<'de> for Invocation<S> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let envelope = Envelope::<S, InvocationPayload>::deserialize(deserializer)?;
        Ok(Invocation(envelope))
    }
}

/// The unsigned content of an [`Invocation`].
///
/// See the [UCAN Invocation payload spec](https://github.com/ucan-wg/invocation/blob/main/README.md#invocation-payload).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InvocationPayload {
    #[serde(rename = "iss")]
    pub(crate) issuer: Did,

    #[serde(rename = "aud", skip_serializing_if = "Option::is_none")]
    pub(crate) audience: Option<Did>,

    #[serde(rename = "sub")]
    pub(crate) subject: Did,

    #[serde(rename = "cmd")]
    pub(crate) command: Command,

    #[serde(rename = "args")]
    pub(crate) arguments: BTreeMap<String, Promised>,

    #[serde(rename = "prf")]
    pub(crate) proofs: Vec<Cid>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cause: Option<Cid>,

    #[serde(rename = "iat", skip_serializing_if = "Option::is_none")]
    pub(crate) issued_at: Option<Timestamp>,

    #[serde(rename = "exp")]
    pub(crate) expiration: Option<Timestamp>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) meta: Option<BTreeMap<String, Ipld>>,

    pub(crate) nonce: Nonce,
}

impl InvocationPayload {
    /// Getter for the `issuer` field.
    #[must_use]
    pub const fn issuer(&self) -> &Did {
        &self.issuer
    }

    /// Getter for the `audience` field.
    /// Returns the subject if no explicit audience was set.
    #[must_use]
    pub fn audience(&self) -> &Did {
        self.audience.as_ref().unwrap_or(&self.subject)
    }

    /// Getter for the `subject` field.
    #[must_use]
    pub const fn subject(&self) -> &Did {
        &self.subject
    }

    /// Getter for the `command` field.
    #[must_use]
    pub const fn command(&self) -> &Command {
        &self.command
    }

    /// Getter for the `arguments` field.
    #[must_use]
    pub const fn arguments(&self) -> &BTreeMap<String, Promised> {
        &self.arguments
    }

    /// Getter for the `proofs` field.
    #[must_use]
    pub const fn proofs(&self) -> &Vec<Cid> {
        &self.proofs
    }

    /// Getter for the `cause` field.
    #[must_use]
    pub const fn cause(&self) -> Option<Cid> {
        self.cause
    }

    /// Getter for the `expiration` field.
    #[must_use]
    pub const fn expiration(&self) -> Option<Timestamp> {
        self.expiration
    }

    /// Getter for the `meta` field. Returns an empty map when meta is absent.
    #[must_use]
    pub fn meta(&self) -> &BTreeMap<String, Ipld> {
        static EMPTY: BTreeMap<String, Ipld> = BTreeMap::new();
        self.meta.as_ref().unwrap_or(&EMPTY)
    }

    /// Getter for the `nonce` field.
    #[must_use]
    pub const fn nonce(&self) -> &Nonce {
        &self.nonce
    }

    /// Compute the CID for this invocation.
    #[must_use]
    pub fn to_cid(&self) -> Cid {
        to_dagcbor_cid(&self)
    }

    /// Check that this payload's proof chain authorizes it.
    ///
    /// This is the structural pass only: principal alignment, subject
    /// consistency, command attenuation, policy predicates, and time bounds
    /// judged against the context's sampled instant. It performs no key
    /// resolution, no signature verification, and no revocation lookup — the
    /// caller does those, after this has passed.
    ///
    /// Running structure first is deliberate: a chain that fails alignment
    /// costs nothing, rather than paying a DID resolution per link before
    /// finding out.
    ///
    /// # Errors
    ///
    /// Returns an [`Invalid`] if a proof is missing from the store or
    /// the chain does not check out.
    pub async fn check<K, S, T, St, C, RE>(
        &self,
        ctx: &VerificationContext<'_, C>,
    ) -> Result<(Vec<T>, TimeRange), Invalid<K, S, T, St, RE>>
    where
        K: FutureKind,
        S: Signature,
        T: Borrow<Delegation<S>>,
        St: DelegationStore<K, S, T>,
        C: Verifiable<K, S, Proof = T, Delegations = St>,
        RE: std::error::Error,
    {
        let proofs: Vec<T> = ctx
            .environment()
            .delegations()
            .get_all(&self.proofs)
            .await
            .map_err(Invalid::MissingProof)?;

        let range = {
            let borrowed: Vec<&Delegation<S>> = proofs.iter().map(Borrow::borrow).collect();
            self.syntactic_checks(borrowed, ctx.time())?
        };

        Ok((proofs, range))
    }

    /// Check if an [`InvocationPayload`] is valid.
    ///
    /// Returns the effective [`TimeRange`] — the intersection of all delegation
    /// and invocation time windows. If the intersection is empty (the chain can
    /// never be valid at any point in time), returns [`CheckFailed::InvalidTimeWindow`].
    ///
    /// # Errors
    ///
    /// Returns a [`CheckFailed`] if the check fails.
    pub fn syntactic_checks<'a, S: Signature + 'a, I: IntoIterator<Item = &'a Delegation<S>>>(
        &'a self,
        proofs: I,
        now: Option<Timestamp>,
    ) -> Result<TimeRange, CheckFailed> {
        let args: Ipld = self
            .arguments()
            .iter()
            .map(|(k, v)| v.try_into().map(|ipld| (k.clone(), ipld)))
            .collect::<Result<BTreeMap<String, Ipld>, _>>()?
            .into();

        // Proofs are expected in subject-to-invocation-issuer (root-to-leaf) order.
        let proofs: Vec<&'a Delegation<S>> = proofs.into_iter().collect();

        // Linkage, rooting, and time are properties of the delegation
        // sequence alone, so they come from one implementation shared with
        // `/ucan/revoke` witness paths rather than being re-derived here.
        let chain_range = check_chain(proofs.iter().copied(), self.subject(), now)?;

        // What is left needs the invocation: attenuation against its command,
        // and policy predicates over its arguments.
        for proof in &proofs {
            if !self.command.starts_with(proof.command()) {
                return Err(CheckFailed::CommandEscalation {
                    claimed: self.command.clone(),
                    authorized: proof.command().clone(),
                });
            }

            for predicate in proof.policy() {
                if !predicate.clone().run(&args)? {
                    return Err(CheckFailed::PolicyViolation(Box::new(predicate.clone())));
                }
            }
        }

        // The invocation's own expiration narrows the chain's window.
        let time_range = chain_range.intersect(TimeRange::from(self));
        let authorization = proofs.last().copied();

        // If proof chain was not empty we ensure that invocation
        // issuer aligns with outmost delegation audience.
        if let Some(proof) = authorization {
            if proof.audience() != self.issuer() {
                return Err(CheckFailed::DelegationAudienceMismatch {
                    claimed: self.issuer().clone(),
                    authorized: proof.audience().clone(),
                });
            }
        }
        // If proof chain was empty it's self issued invocation in
        // which case we ensure that claimed subject matches issuer
        else if self.issuer() != self.subject() {
            return Err(CheckFailed::UnauthorizedSubject {
                claimed: self.subject().clone(),
                authorized: self.issuer().clone(),
            });
        }

        // `check_chain` already judged the hops; this re-checks the window
        // once the invocation's own expiration is folded in.
        if !time_range.is_valid() {
            return Err(CheckFailed::InvalidTimeWindow { range: time_range });
        }
        if let Some(now) = now {
            time_range.check(&now)?;
        }

        Ok(time_range)
    }
}

impl From<&InvocationPayload> for TimeRange {
    fn from(payload: &InvocationPayload) -> Self {
        Self::new(None, payload.expiration)
    }
}

impl<'de> Deserialize<'de> for InvocationPayload {
    #[allow(clippy::too_many_lines)]
    fn deserialize<T>(deserializer: T) -> Result<Self, T::Error>
    where
        T: Deserializer<'de>,
    {
        struct PayloadVisitor;

        impl<'de> Visitor<'de> for PayloadVisitor {
            type Value = InvocationPayload;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a map with keys iss,sub,cmd,args,prf,nonce and optional aud,cause,iat,exp,meta")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut issuer: Option<Did> = None;
                let mut audience: Option<Did> = None;
                let mut subject: Option<Did> = None;
                let mut command: Option<Command> = None;
                let mut arguments: Option<BTreeMap<String, Promised>> = None;
                let mut proofs: Option<Vec<Cid>> = None;
                let mut cause: Option<Option<Cid>> = None;
                let mut issued_at: Option<Option<Timestamp>> = None;
                let mut expiration: Option<Option<Timestamp>> = None;
                let mut meta: Option<BTreeMap<String, Ipld>> = None;
                let mut nonce: Option<Nonce> = None;

                while let Some(key) = map.next_key::<Cow<'de, str>>()? {
                    match key.as_ref() {
                        "iss" => {
                            if issuer.is_some() {
                                return Err(de::Error::duplicate_field("iss"));
                            }
                            issuer = Some(map.next_value()?);
                        }
                        "aud" => {
                            if audience.is_some() {
                                return Err(de::Error::duplicate_field("aud"));
                            }
                            audience = Some(map.next_value()?);
                        }
                        "sub" => {
                            if subject.is_some() {
                                return Err(de::Error::duplicate_field("sub"));
                            }
                            subject = Some(map.next_value()?);
                        }
                        "cmd" => {
                            if command.is_some() {
                                return Err(de::Error::duplicate_field("cmd"));
                            }
                            command = Some(map.next_value()?);
                        }
                        "args" => {
                            if arguments.is_some() {
                                return Err(de::Error::duplicate_field("args"));
                            }
                            arguments = Some(map.next_value()?);
                        }
                        "prf" => {
                            if proofs.is_some() {
                                return Err(de::Error::duplicate_field("prf"));
                            }
                            proofs = Some(map.next_value()?);
                        }
                        "cause" => {
                            if cause.is_some() {
                                return Err(de::Error::duplicate_field("cause"));
                            }
                            cause = Some(map.next_value()?);
                        }
                        "iat" => {
                            if issued_at.is_some() {
                                return Err(de::Error::duplicate_field("iat"));
                            }
                            issued_at = Some(map.next_value()?);
                        }
                        "exp" => {
                            if expiration.is_some() {
                                return Err(de::Error::duplicate_field("exp"));
                            }
                            expiration = Some(map.next_value()?);
                        }
                        "meta" => {
                            if meta.is_some() {
                                return Err(de::Error::duplicate_field("meta"));
                            }
                            meta = Some(map.next_value()?);
                        }
                        "nonce" => {
                            if nonce.is_some() {
                                return Err(de::Error::duplicate_field("nonce"));
                            }
                            let ipld: Ipld = map.next_value()?;
                            let v = match ipld {
                                Ipld::Bytes(b) => b,
                                other @ (Ipld::Null
                                | Ipld::Bool(_)
                                | Ipld::Integer(_)
                                | Ipld::Float(_)
                                | Ipld::String(_)
                                | Ipld::List(_)
                                | Ipld::Map(_)
                                | Ipld::Link(_)) => {
                                    return Err(de::Error::custom(format!(
                                        "expected nonce to be bytes, got {other:?}"
                                    )));
                                }
                            };

                            if let Ok(arr) = <[u8; 16]>::try_from(v.clone()) {
                                nonce = Some(Nonce::Nonce16(arr));
                            } else {
                                nonce = Some(Nonce::Custom(v));
                            }
                        }
                        other => {
                            return Err(de::Error::unknown_field(
                                other,
                                &[
                                    "iss", "aud", "sub", "cmd", "args", "prf", "cause", "iat",
                                    "exp", "meta", "nonce",
                                ],
                            ));
                        }
                    }
                }

                let issuer = issuer.ok_or_else(|| de::Error::missing_field("iss"))?;
                let subject = subject.ok_or_else(|| de::Error::missing_field("sub"))?;
                let command = command.ok_or_else(|| de::Error::missing_field("cmd"))?;
                let arguments = arguments.ok_or_else(|| de::Error::missing_field("args"))?;
                let proofs = proofs.ok_or_else(|| de::Error::missing_field("prf"))?;
                let nonce = nonce.ok_or_else(|| de::Error::missing_field("nonce"))?;

                Ok(InvocationPayload {
                    issuer,
                    audience,
                    subject,
                    command,
                    arguments,
                    proofs,
                    nonce,
                    cause: cause.unwrap_or(None),
                    issued_at: issued_at.unwrap_or(None),
                    expiration: expiration.unwrap_or(None),
                    meta,
                })
            }
        }

        deserializer.deserialize_map(PayloadVisitor)
    }
}

impl PayloadTag for InvocationPayload {
    fn spec_id() -> &'static str {
        "inv"
    }

    fn version() -> &'static str {
        "1.0.0-rc.1"
    }
}

/// Resolve each distinct DID exactly once, all concurrently.
///
/// Deduplicating before resolving is the point: the caching resolver has no
/// in-flight dedup, so N concurrent lookups of the same DID would all miss the
/// cache together and all fetch. Collapsing to distinct DIDs first keeps that
/// at one request per DID no matter how many links name it.
///
/// The resolver's verifier type is opaque (RPITIT), so this returns the map by
/// way of a generic rather than naming it.
async fn resolve_each_once<'a, 'r, S, R>(
    resolver: &'r R,
    dids: BTreeSet<&'a Did>,
) -> HashMap<&'a Did, Result<impl Verifier<S> + use<'a, 'r, S, R>, R::Error>>
where
    S: Signature,
    R: Resolver<S>,
{
    let mut pending = FuturesUnordered::new();
    for did in dids {
        pending.push(async move { (did, resolver.resolve(did).await) });
    }

    let mut resolved = HashMap::new();
    while let Some((did, result)) = pending.next().await {
        resolved.insert(did, result);
    }
    resolved
}

/// Errors that can occur when checking an invocation
#[derive(Debug, Clone, Error)]
pub enum CheckFailed {
    /// Error indicating that the invocation is waiting on a promise to be resolved
    #[error(transparent)]
    WaitingOnPromise(#[from] WaitingOn),

    /// The invocation's command is not covered by the delegation's command scope.
    #[error("Claimed command '{claimed}' is not authorized by command '{authorized}'")]
    CommandEscalation {
        /// The command the invocation is trying to execute.
        claimed: Command,

        /// The command that is authorized.
        authorized: Command,
    },
    /// The invocation's arguments are incompatible with a delegation's
    /// policy — e.g. a selector references a field that doesn't exist,
    /// or a comparison involves incompatible types (NaN float vs integer).
    #[error(transparent)]
    PolicyIncompatibility(#[from] RunError),

    /// A delegation's policy predicate evaluated to `false` against the
    /// invocation's arguments. The invocation does not satisfy the
    /// constraints set by this delegation.
    #[error("Invocation arguments violate delegation policy: {0:?}")]
    PolicyViolation(Box<Predicate>),

    /// A proof's issuer does not match the previous delegation's audience.
    /// In a valid chain, each proof must be issued by whoever the previous
    /// link delegated to. For the first proof, that's the subject.
    #[error("Claimed issuer '{claimed}' does not match authorized audience '{authorized}'")]
    DelegationAudienceMismatch {
        /// The DID that was expected as the proof's issuer.
        claimed: Did,
        /// The DID that was actually authorized as the audience.
        authorized: Did,
    },

    /// The subject does not match the invocation subject.
    #[error("Claimed subject '{claimed}' is not authorized by subject '{authorized}'")]
    UnauthorizedSubject {
        /// The invocation's claimed subject.
        claimed: Did,
        /// The subject that is authorized.
        authorized: Did,
    },

    /// The delegation has no subject (`Any`) and no prior proof established
    /// one, so the issuer is taken as the implied subject — but it does not
    /// match the invocation subject.
    #[error("Delegation issuer '{issuer}' does not match claimed subject '{subject}'")]
    UnprovenSubject {
        /// The invocation's claimed subject.
        subject: Did,
        /// The delegation's issuer (used as implied subject).
        issuer: Did,
    },

    /// The intersection of all time bounds in the delegation chain is empty.
    /// There is no point in time at which this invocation could be valid.
    #[error("Delegation chain has no valid time window: {range}")]
    InvalidTimeWindow {
        /// The empty time range that was computed.
        range: TimeRange,
    },

    /// The invocation is outside the valid time window (expired or not yet valid).
    #[error(transparent)]
    TimeBound(#[from] TimeBoundError),
}

/// Error type for invocation signature verification.
#[derive(Debug, thiserror::Error)]
pub enum SignatureVerificationError<E: std::error::Error = signature::Error> {
    /// Payload encoding failed.
    #[error("encoding error: {0}")]
    EncodingError(serde_ipld_dagcbor::error::CodecError),

    /// DID resolution failed.
    #[error("resolution error: {0}")]
    ResolutionError(E),

    /// Cryptographic verification failed.
    #[error("verification error: {0}")]
    VerificationError(signature::Error),
}

/// Errors that can occur when checking an invocation (signature + proofs)
#[derive(Debug, Error)]
pub enum VerifyError<
    K: FutureKind,
    S: Signature,
    T: Borrow<Delegation<S>>,
    St: DelegationStore<K, S, T>,
    RE: std::error::Error,
    XE: std::error::Error,
> {
    /// The chain does not hold up: it is invalid, and no retry or better
    /// connectivity changes that.
    ///
    /// Every variant of [`Invalid`] is a statement about the caller's
    /// material.
    #[error(transparent)]
    Invalid(#[from] Invalid<K, S, T, St, RE>),

    /// We could not establish whether the chain holds up.
    ///
    /// This says nothing about the chain — only that something we depend on
    /// was unreachable. It must never be reported as a denial.
    #[error(transparent)]
    Unavailable(#[from] Unavailable<RE, XE>),
}

/// The error [`Invocation::check`] produces for a given environment.
///
/// Spells out the six parameters once so callers can name the type without
/// repeating them.
pub type CheckError<K, S, C> = VerifyError<
    K,
    S,
    <C as Verifiable<K, S>>::Proof,
    <C as Verifiable<K, S>>::Delegations,
    <<C as Verifiable<K, S>>::Resolver as Resolver<S>>::Error,
    <<C as Verifiable<K, S>>::Revocations as RevocationChecker>::Error,
>;

/// The chain is invalid. A statement about the caller's material.
#[derive(Debug, Error)]
pub enum Invalid<
    K: FutureKind,
    S: Signature,
    T: Borrow<Delegation<S>>,
    St: DelegationStore<K, S, T>,
    RE: std::error::Error,
> {
    /// The invocation's own signature did not verify.
    #[error("invocation does not carry a valid signature: {0}")]
    InvocationSignature(SignatureVerificationError<RE>),

    /// A delegation link's signature did not verify against its claimed
    /// issuer's key.
    #[error("delegation from '{issuer}' does not carry a valid signature: {source}")]
    DelegationSignature {
        /// The principal the proof claims as its issuer.
        issuer: Did,
        /// The underlying verification failure.
        source: DelegationSignatureError<RE>,
    },

    /// A proof the chain refers to was not supplied.
    #[error(transparent)]
    MissingProof(St::GetError),

    /// The chain's structure does not authorize the invocation: principal
    /// alignment, attenuation, policy, or time bounds.
    #[error(transparent)]
    Chain(#[from] CheckFailed),

    /// A delegation in the chain has been revoked.
    #[error("delegation '{cid}' was revoked by '{}'", found.principal)]
    Revoked {
        /// The revoked delegation.
        cid: Cid,
        /// The revocation that matched: its document and its issuer.
        found: RevocationMatch,
    },
}

/// We could not establish whether the chain holds up.
///
/// Distinct from [`Invalid`] because the two mean opposite things at a trust
/// boundary: one is grounds to refuse, the other is grounds to say "ask again".
#[derive(Debug, Error)]
pub enum Unavailable<RE: std::error::Error, XE: std::error::Error> {
    /// An issuer DID could not be resolved to a verifier.
    ///
    /// Without the key the signature cannot be checked, so the chain is
    /// unproven — not invalid. Distinct from a revocation gap: this leaves a
    /// signature unverified, so no tolerance setting can accept it.
    #[error("could not resolve issuer '{did}': {detail}")]
    DidResolution {
        /// The issuer that could not be resolved.
        did: Did,
        /// Why resolution failed.
        detail: RE,
    },

    /// A revocation lookup failed and the checker did not tolerate it.
    ///
    /// Says nothing about whether the delegation was revoked, only that the
    /// question went unanswered. A caller willing to proceed without that
    /// answer uses
    /// [`tolerate_unavailable`](crate::RevocationChecker::tolerate_unavailable),
    /// which turns this into an unknown recorded in the verdict instead.
    #[error("could not determine revocation status of '{cid}': {source}")]
    RevocationLookup {
        /// The delegation whose status is unknown.
        cid: Cid,
        /// Why the lookup failed.
        source: XE,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::Command,
        crypto::nonce::Nonce,
        delegation::{
            Delegation,
            builder::DelegationBuilder,
            policy::{predicate::Predicate, selector::select::Select},
            store,
        },
        future::Local,
        promise::Promised,
        revocation::{RevocationMatch, UnverifiedRevocations},
        subject::Subject,
        time::{TimeRange, Timestamp},
        verification::Environment,
    };
    use builder::InvocationBuilder;
    use dialog_credentials::DidKeyResolver;
    use dialog_credentials::ed25519::Ed25519Signer;
    use dialog_varsig::{did::Did, eddsa::Ed25519Signature, principal::Principal};
    use std::{
        cell::RefCell,
        collections::HashMap,
        ops::{Bound, RangeBounds},
        rc::Rc,
        str::FromStr,
    };
    use testresult::TestResult;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test;
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    /// Create a deterministic test signer from a seed.
    async fn test_signer(seed: u8) -> Ed25519Signer {
        Ed25519Signer::import(&[seed; 32]).await.unwrap()
    }

    /// Create a deterministic test DID from a seed.
    async fn test_did(seed: u8) -> Did {
        test_signer(seed).await.did()
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn issuer_round_trip() -> TestResult {
        let iss = test_signer(0).await;
        let aud = test_did(0).await;
        let sub = test_did(0).await;

        let builder = InvocationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(&sub)
            .command(vec!["read".to_string(), "write".to_string()])
            .proofs(vec![]);

        let invocation = builder.try_build().await?;

        assert_eq!(invocation.issuer().to_string(), iss.to_string());
        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn signature_type_inferred_from_issuer() -> TestResult {
        let invocation = InvocationBuilder::new()
            .issuer(test_signer(1).await)
            .audience(&test_did(2).await)
            .subject(&test_did(3).await)
            .command(vec!["test".into()])
            .proofs(vec![])
            .try_build()
            .await?;

        assert_eq!(invocation.issuer(), &test_did(1).await);
        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn invocation_has_correct_fields() -> TestResult {
        let iss = test_signer(10).await;
        let aud = test_did(20).await;
        let sub = test_did(30).await;
        let cmd = vec!["storage".to_string(), "write".to_string()];

        let invocation = InvocationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(&sub)
            .command(cmd.clone())
            .proofs(vec![])
            .try_build()
            .await?;

        let iss_did: Did = iss.did();
        assert_eq!(invocation.issuer(), &iss_did);
        assert_eq!(invocation.audience(), &aud);
        assert_eq!(invocation.subject(), &sub);
        assert_eq!(invocation.command(), &Command::new(cmd));

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn invocation_signature_verifies() -> TestResult {
        let iss = test_signer(42).await;
        let aud = test_did(43).await;
        let sub = test_did(44).await;

        let invocation = InvocationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(&sub)
            .command(vec!["test".to_string()])
            .proofs(vec![])
            .try_build()
            .await?;

        let resolver = DidKeyResolver;
        invocation.verify_signature(&resolver).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn invocation_serialization_roundtrip() -> TestResult {
        let iss = test_signer(50).await;
        let aud = test_did(51).await;
        let sub = test_did(52).await;

        let invocation = InvocationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(&sub)
            .command(vec!["roundtrip".to_string()])
            .proofs(vec![])
            .try_build()
            .await?;

        // Serialize to CBOR
        let bytes = serde_ipld_dagcbor::to_vec(&invocation)?;

        // Deserialize back
        let roundtripped: Invocation<Ed25519Signature> = serde_ipld_dagcbor::from_slice(&bytes)?;

        // Verify all fields match
        assert_eq!(roundtripped.issuer(), invocation.issuer());
        assert_eq!(roundtripped.audience(), invocation.audience());
        assert_eq!(roundtripped.subject(), invocation.subject());
        assert_eq!(roundtripped.command(), invocation.command());
        assert_eq!(roundtripped.nonce(), invocation.nonce());

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn invocation_with_explicit_nonce_is_deterministic() -> TestResult {
        let iss = test_signer(70).await;
        let aud = test_did(71).await;
        let sub = test_did(72).await;
        let nonce = Nonce::generate_16()?;

        // Build two invocations with the same nonce
        let invocation1 = InvocationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(&sub)
            .command(vec!["compare".to_string()])
            .proofs(vec![])
            .nonce(nonce.clone())
            .try_build()
            .await?;

        let invocation2 = InvocationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(&sub)
            .command(vec!["compare".to_string()])
            .proofs(vec![])
            .nonce(nonce)
            .try_build()
            .await?;

        // Both should have the same payload content
        assert_eq!(invocation1.issuer(), invocation2.issuer());
        assert_eq!(invocation1.audience(), invocation2.audience());
        assert_eq!(invocation1.subject(), invocation2.subject());
        assert_eq!(invocation1.command(), invocation2.command());
        assert_eq!(invocation1.nonce(), invocation2.nonce());

        // Both signatures should verify
        let resolver = DidKeyResolver;
        invocation1.verify_signature(&resolver).await?;
        invocation2.verify_signature(&resolver).await?;

        // With the same nonce and same signer, the serialized form should be identical
        // because Ed25519 is deterministic
        let bytes1 = serde_ipld_dagcbor::to_vec(&invocation1)?;
        let bytes2 = serde_ipld_dagcbor::to_vec(&invocation2)?;
        assert_eq!(
            bytes1, bytes2,
            "Serialized bytes should be identical with same nonce"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn invocation_different_signers_different_signatures() -> TestResult {
        let iss1 = test_signer(80).await;
        let iss2 = test_signer(81).await;
        let aud = test_did(82).await;
        let sub = test_did(83).await;
        let nonce = Nonce::generate_16()?;

        let invocation1 = InvocationBuilder::<Ed25519Signature>::new()
            .issuer(iss1.clone())
            .audience(&aud)
            .subject(&sub)
            .command(vec!["test".to_string()])
            .proofs(vec![])
            .nonce(nonce.clone())
            .try_build()
            .await?;

        let invocation2 = InvocationBuilder::<Ed25519Signature>::new()
            .issuer(iss2.clone())
            .audience(&aud)
            .subject(&sub)
            .command(vec!["test".to_string()])
            .proofs(vec![])
            .nonce(nonce)
            .try_build()
            .await?;

        // Different issuers should produce different serialized forms
        let bytes1 = serde_ipld_dagcbor::to_vec(&invocation1)?;
        let bytes2 = serde_ipld_dagcbor::to_vec(&invocation2)?;
        assert_ne!(
            bytes1, bytes2,
            "Different signers should produce different serialized invocations"
        );

        // But both should verify with their respective keys
        let resolver = DidKeyResolver;
        invocation1.verify_signature(&resolver).await?;
        invocation2.verify_signature(&resolver).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn invocation_with_arguments() -> TestResult {
        use std::collections::BTreeMap;

        let iss = test_signer(90).await;
        let aud = test_did(91).await;
        let sub = test_did(92).await;

        let mut args = BTreeMap::new();
        args.insert("path".to_string(), Promised::String("/foo/bar".to_string()));
        args.insert("count".to_string(), Promised::Integer(42));

        let invocation = InvocationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(&sub)
            .command(vec!["storage".to_string(), "read".to_string()])
            .arguments(args.clone())
            .proofs(vec![])
            .try_build()
            .await?;

        assert_eq!(invocation.arguments(), &args);

        // Signature should still verify
        let resolver = DidKeyResolver;
        invocation.verify_signature(&resolver).await?;

        Ok(())
    }

    type DelegationStore = Rc<RefCell<HashMap<Cid, Rc<Delegation<Ed25519Signature>>>>>;

    /// Helper to create an `Rc<RefCell<HashMap>>` delegation store.
    fn new_store() -> DelegationStore {
        Rc::new(RefCell::new(HashMap::new()))
    }

    /// Helper to insert a delegation into the store and return its CID.
    async fn store_delegation(
        store: &DelegationStore,
        delegation: Delegation<Ed25519Signature>,
    ) -> ipld_core::cid::Cid {
        store::insert(store, Rc::new(delegation)).await.unwrap()
    }

    /// A verification environment over `store`, judged against the system
    /// clock and performing no revocation lookups.
    type TestEnv = Environment<
        DelegationStore,
        DidKeyResolver,
        UnverifiedRevocations,
        Rc<Delegation<Ed25519Signature>>,
    >;

    fn env(store: &DelegationStore) -> TestEnv {
        Environment::new(store.clone(), DidKeyResolver, UnverifiedRevocations)
    }

    /// Verify against the system clock.
    async fn check(
        invocation: &Invocation<Ed25519Signature>,
        store: &DelegationStore,
    ) -> Result<TimeRange, String> {
        check_at(invocation, store, Some(Timestamp::now())).await
    }

    /// Verify against a specific instant (`None` skips time bounds).
    async fn check_at(
        invocation: &Invocation<Ed25519Signature>,
        store: &DelegationStore,
        time: Option<Timestamp>,
    ) -> Result<TimeRange, String> {
        let environment = env(store);
        let ctx = VerificationContext::at(&environment, time);
        invocation
            .check::<Local, _, _, _>(&ctx)
            .await
            .map_err(|err| err.to_string())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_valid_single_delegation() -> TestResult {
        // subject delegates to invoker via one proof
        let subject = test_signer(100).await;
        let invoker = test_signer(101).await;

        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store::insert(&delegation_store, Rc::new(delegation)).await?;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        check(&invocation, &delegation_store).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_fails_audience_issuer_mismatch() -> TestResult {
        // subject delegates to middleman, but invoker (different principal) tries to invoke
        let subject = test_signer(110).await;
        let middleman = test_signer(111).await;
        let invoker = test_signer(112).await;

        // Delegation: subject -> middleman
        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&middleman)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store::insert(&delegation_store, Rc::new(delegation)).await?;

        // Invocation by invoker (not middleman) — chain should fail because
        // delegation.audience (middleman) != invocation.issuer (invoker)
        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        let result = check(&invocation, &delegation_store).await;
        let err = result.expect_err("Chain check should fail when proof audience != invoker");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("does not match authorized audience"),
            "Error should mention delegation audience mismatch, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&middleman.did().to_string()),
            "Error should mention the middleman DID, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&invoker.did().to_string()),
            "Error should mention the invoker DID, got: {err_msg}"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_fails_proof_issuer_not_subject() -> TestResult {
        // A random principal (not the subject) issues a delegation
        let subject = test_signer(120).await;
        let random = test_signer(121).await;
        let invoker = test_signer(122).await;

        let delegation = DelegationBuilder::new()
            .issuer(random.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store::insert(&delegation_store, Rc::new(delegation)).await?;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        // Should fail: proof.issuer (random) != subject
        let result = check(&invocation, &delegation_store).await;
        let err = result.expect_err("Chain check should fail when proof issuer != subject");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("does not match claimed subject"),
            "Error should mention unproven subject, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&subject.did().to_string()),
            "Error should mention the subject DID, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&random.did().to_string()),
            "Error should mention the random issuer DID, got: {err_msg}"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn signature_verification_fails_with_tampered_payload() -> TestResult {
        let iss = test_signer(130).await;
        let aud = test_did(131).await;
        let sub = test_did(132).await;

        let invocation = InvocationBuilder::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(&sub)
            .command(vec!["test".to_string()])
            .proofs(vec![])
            .try_build()
            .await?;

        // Serialize, tamper with bytes, deserialize
        let mut bytes = serde_ipld_dagcbor::to_vec(&invocation)?;

        // Flip a byte in the middle of the payload
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;

        // Deserialization may fail (that's fine) or succeed with wrong data
        let tampered: Result<Invocation<Ed25519Signature>, _> =
            serde_ipld_dagcbor::from_slice(&bytes);
        if let Ok(tampered) = tampered {
            let resolver = DidKeyResolver;
            let result = tampered.verify_signature(&resolver).await;
            assert!(
                result.is_err(),
                "Tampered invocation should fail signature verification"
            );
        }

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_valid_with_any_subject() -> TestResult {
        // subject delegates with Subject::Any, invoker invokes on behalf of subject
        // did:key:a -> Any -> did:key:a
        let subject = test_signer(140).await;
        let invoker = test_signer(141).await;

        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Any)
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        let invocation = InvocationBuilder::<Ed25519Signature>::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        check(&invocation, &delegation_store).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_fails_specific_subject_mismatch() -> TestResult {
        // Delegation constrains subject to did:key:b, but invocation targets did:key:a
        let subject_a = test_signer(150).await;
        let subject_b = test_signer(151).await;
        let invoker = test_signer(152).await;

        // Delegation: subject_a delegates to invoker, but scoped to subject_b
        let delegation = DelegationBuilder::new()
            .issuer(subject_a.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject_b.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        // Invocation targets subject_a, but the proof only authorizes subject_b
        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject_a)
            .subject(&subject_a)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        let result = check(&invocation, &delegation_store).await;
        let err = result.expect_err("Should fail: proof subject (b) != invocation subject (a)");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("is not authorized by subject"),
            "Error should mention unauthorized subject, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&subject_a.did().to_string()),
            "Error should mention expected subject (a), got: {err_msg}"
        );
        assert!(
            err_msg.contains(&subject_b.did().to_string()),
            "Error should mention actual subject (b), got: {err_msg}"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_fails_root_issuer_not_subject() -> TestResult {
        // Root delegation issuer must match invocation subject.
        // Here subject is `a`, but the delegation is issued by `b`.
        let subject_a = test_signer(160).await;
        let imposter_b = test_signer(161).await;
        let invoker = test_signer(162).await;

        // Delegation issued by b (not the subject a), with subject set to a
        let delegation = DelegationBuilder::new()
            .issuer(imposter_b.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject_a.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject_a)
            .subject(&subject_a)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        let result = check(&invocation, &delegation_store).await;
        let err = result.expect_err("Should fail: root delegation issuer (b) != subject (a)");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("does not match claimed subject"),
            "Error should mention unproven subject, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&subject_a.did().to_string()),
            "Error should mention the subject DID, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&imposter_b.did().to_string()),
            "Error should mention the imposter DID, got: {err_msg}"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_valid_powerline_delegation() -> TestResult {
        // Powerline delegation (Subject::Any) implies subject == delegation.issuer.
        // subject delegates with Any, invoker invokes targeting subject — should succeed.
        let subject = test_signer(170).await;
        let invoker = test_signer(171).await;

        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Any)
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        check(&invocation, &delegation_store).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_fails_powerline_issuer_not_subject() -> TestResult {
        // Powerline delegation (Subject::Any) issued by `b`, but invocation targets `a`.
        // The root issuer must still match the invocation subject.
        let subject_a = test_signer(180).await;
        let imposter_b = test_signer(181).await;
        let invoker = test_signer(182).await;

        let delegation = DelegationBuilder::new()
            .issuer(imposter_b.clone())
            .audience(&invoker)
            .subject(Subject::Any)
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject_a)
            .subject(&subject_a)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        let result = check(&invocation, &delegation_store).await;
        let err = result.expect_err("Should fail: powerline issuer (b) != invocation subject (a)");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("does not match claimed subject"),
            "Error should mention unproven subject, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&subject_a.did().to_string()),
            "Error should mention the subject DID, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&imposter_b.did().to_string()),
            "Error should mention the imposter DID, got: {err_msg}"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_valid_two_hop_with_any_subject() -> TestResult {
        // Two-hop chain: subject -> middleman (Specific) -> invoker (Any)
        // The second proof uses Subject::Any but implied subject carries forward.
        let subject = test_signer(190).await;
        let middleman = test_signer(191).await;
        let invoker = test_signer(192).await;

        // First delegation: subject -> middleman, Specific subject
        let delegation1 = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&middleman)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        // Second delegation: middleman -> invoker, Any subject
        let delegation2 = DelegationBuilder::new()
            .issuer(middleman.clone())
            .audience(&invoker)
            .subject(Subject::Any)
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid1 = store_delegation(&delegation_store, delegation1).await;
        let cid2 = store_delegation(&delegation_store, delegation2).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid1, cid2])
            .try_build()
            .await?;

        check(&invocation, &delegation_store).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_fails_two_hop_subject_switch() -> TestResult {
        // Two-hop chain where second proof uses Subject::Specific(b) instead
        // of the established subject (a). Even though the chain linkage is valid,
        // the subject mismatch must be caught.
        let subject_a = test_signer(200).await;
        let subject_b = test_signer(201).await;
        let middleman = test_signer(202).await;
        let invoker = test_signer(203).await;

        // First delegation: subject_a -> middleman, Specific(a)
        let delegation1 = DelegationBuilder::new()
            .issuer(subject_a.clone())
            .audience(&middleman)
            .subject(Subject::Specific(subject_a.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        // Second delegation: middleman -> invoker, Specific(b) — wrong subject
        let delegation2 = DelegationBuilder::new()
            .issuer(middleman.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject_b.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid1 = store_delegation(&delegation_store, delegation1).await;
        let cid2 = store_delegation(&delegation_store, delegation2).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject_a)
            .subject(&subject_a)
            .command(vec!["test".to_string()])
            .proofs(vec![cid1, cid2])
            .try_build()
            .await?;

        let result = check(&invocation, &delegation_store).await;
        let err =
            result.expect_err("Should fail: second proof subject (b) != established subject (a)");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("is not authorized by subject"),
            "Error should mention unauthorized subject, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&subject_a.did().to_string()),
            "Error should mention subject a, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&subject_b.did().to_string()),
            "Error should mention subject b, got: {err_msg}"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_fails_self_issued_issuer_not_subject() -> TestResult {
        // Self-issued invocation (no proofs): issuer must equal subject.
        let subject_a = test_signer(210).await;
        let issuer_b = test_signer(211).await;

        let delegation_store = new_store();

        let invocation = InvocationBuilder::new()
            .issuer(issuer_b.clone())
            .audience(&subject_a)
            .subject(&subject_a)
            .command(vec!["test".to_string()])
            .proofs(vec![])
            .try_build()
            .await?;

        let result = check(&invocation, &delegation_store).await;
        let err = result.expect_err("Should fail: self-issued invocation with issuer != subject");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("is not authorized by subject"),
            "Error should mention unauthorized subject, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&subject_a.did().to_string()),
            "Error should mention the subject DID, got: {err_msg}"
        );
        assert!(
            err_msg.contains(&issuer_b.did().to_string()),
            "Error should mention the issuer DID, got: {err_msg}"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_valid_self_issued() -> TestResult {
        // Self-issued invocation (no proofs): issuer == subject should pass.
        let subject = test_signer(220).await;
        let delegation_store = new_store();

        let invocation = InvocationBuilder::new()
            .issuer(subject.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![])
            .try_build()
            .await?;

        check(&invocation, &delegation_store).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_fails_command_escalation() -> TestResult {
        // Delegation authorizes /storage/read but invocation claims /storage/write
        let subject = test_signer(230).await;
        let invoker = test_signer(231).await;

        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["storage".to_string(), "read".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["storage".to_string(), "write".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        let result = check(&invocation, &delegation_store).await;
        let err = result.expect_err("Should fail: invocation command not covered by delegation");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("is not authorized by command"),
            "Error should mention command escalation, got: {err_msg}"
        );
        assert!(
            err_msg.contains("storage/write"),
            "Error should mention the claimed command, got: {err_msg}"
        );
        assert!(
            err_msg.contains("storage/read"),
            "Error should mention the authorized command, got: {err_msg}"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_valid_command_subset() -> TestResult {
        // Delegation authorizes /storage, invocation claims /storage/read — should pass
        let subject = test_signer(232).await;
        let invoker = test_signer(233).await;

        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["storage".to_string(), "read".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        check(&invocation, &delegation_store).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_fails_policy_violation() -> TestResult {
        // Delegation has a policy that requires .path == "/allowed",
        // but invocation arguments have path = "/forbidden"
        let subject = test_signer(240).await;
        let invoker = test_signer(241).await;

        let policy = vec![Predicate::Equal(
            Select::from_str(".path").unwrap(),
            ipld_core::ipld::Ipld::String("/allowed".to_string()),
        )];

        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["storage".to_string(), "read".to_string()])
            .policy(policy)
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        let mut args = std::collections::BTreeMap::new();
        args.insert(
            "path".to_string(),
            Promised::String("/forbidden".to_string()),
        );

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["storage".to_string(), "read".to_string()])
            .arguments(args)
            .proofs(vec![cid])
            .try_build()
            .await?;

        let result = check(&invocation, &delegation_store).await;
        let err = result.expect_err("Should fail: arguments violate delegation policy");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("violate delegation policy"),
            "Error should mention policy violation, got: {err_msg}"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_valid_policy_satisfied() -> TestResult {
        // Delegation has a policy that requires .path == "/allowed",
        // and invocation arguments satisfy it
        let subject = test_signer(242).await;
        let invoker = test_signer(243).await;

        let policy = vec![Predicate::Equal(
            Select::from_str(".path").unwrap(),
            ipld_core::ipld::Ipld::String("/allowed".to_string()),
        )];

        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["storage".to_string(), "read".to_string()])
            .policy(policy)
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        let mut args = std::collections::BTreeMap::new();
        args.insert("path".to_string(), Promised::String("/allowed".to_string()));

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["storage".to_string(), "read".to_string()])
            .arguments(args)
            .proofs(vec![cid])
            .try_build()
            .await?;

        check(&invocation, &delegation_store).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_fails_three_hop_mid_chain_linkage() -> TestResult {
        // Three-hop chain: subject -> middleman1 -> middleman2 -> invoker
        // Break the chain: middleman2's delegation is issued by subject (not middleman1)
        let subject = test_signer(250).await;
        let middleman1 = test_signer(251).await;
        let middleman2 = test_signer(252).await;
        let invoker = test_signer(253).await;

        // First delegation: subject -> middleman1
        let delegation1 = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&middleman1)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        // Second delegation: subject -> middleman2 (WRONG! should be middleman1 -> middleman2)
        let delegation2 = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&middleman2)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        // Third delegation: middleman2 -> invoker
        let delegation3 = DelegationBuilder::new()
            .issuer(middleman2.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid1 = store_delegation(&delegation_store, delegation1).await;
        let cid2 = store_delegation(&delegation_store, delegation2).await;
        let cid3 = store_delegation(&delegation_store, delegation3).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid1, cid2, cid3])
            .try_build()
            .await?;

        let result = check(&invocation, &delegation_store).await;
        let err = result.expect_err("Should fail: chain linkage broken at second hop");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("does not match authorized audience"),
            "Error should mention audience mismatch, got: {err_msg}"
        );
        // delegation2.issuer (subject) != delegation1.audience (middleman1)
        assert!(
            err_msg.contains(&subject.did().to_string()),
            "Error should mention subject DID (the wrong issuer), got: {err_msg}"
        );
        assert!(
            err_msg.contains(&middleman1.did().to_string()),
            "Error should mention middleman1 DID (the expected audience), got: {err_msg}"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_valid_three_hop() -> TestResult {
        // Valid three-hop chain: subject -> middleman1 -> middleman2 -> invoker
        let subject = test_signer(254).await;
        let middleman1 = test_signer(255).await;
        let middleman2 = test_signer(1).await; // reuse different seed
        let invoker = test_signer(2).await;

        let delegation1 = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&middleman1)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation2 = DelegationBuilder::new()
            .issuer(middleman1.clone())
            .audience(&middleman2)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation3 = DelegationBuilder::new()
            .issuer(middleman2.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid1 = store_delegation(&delegation_store, delegation1).await;
        let cid2 = store_delegation(&delegation_store, delegation2).await;
        let cid3 = store_delegation(&delegation_store, delegation3).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid1, cid2, cid3])
            .try_build()
            .await?;

        check(&invocation, &delegation_store).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_valid_audience_differs_from_subject() -> TestResult {
        // Per spec, invocation audience is for routing and MAY differ from subject.
        // subject delegates to invoker, invocation targets a different audience (gateway).
        let subject = test_signer(3).await;
        let invoker = test_signer(4).await;
        let gateway = test_signer(5).await;

        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        // Invocation audience is the gateway, NOT the subject
        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&gateway)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        check(&invocation, &delegation_store).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_valid_self_issued_audience_differs_from_subject() -> TestResult {
        // Self-issued invocation where audience differs from subject.
        // Per spec, audience is for routing. issuer == subject should be sufficient.
        let subject = test_signer(6).await;
        let gateway = test_signer(7).await;

        let delegation_store = new_store();

        let invocation = InvocationBuilder::new()
            .issuer(subject.clone())
            .audience(&gateway)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![])
            .try_build()
            .await?;

        check(&invocation, &delegation_store).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_returns_unbounded_range_for_self_issued() -> TestResult {
        let subject = test_signer(8).await;
        let delegation_store = new_store();

        let invocation = InvocationBuilder::new()
            .issuer(subject.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![])
            .try_build()
            .await?;

        let range = check(&invocation, &delegation_store).await?;

        assert_eq!(range, TimeRange::unbounded());
        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_returns_delegation_time_bounds() -> TestResult {
        let subject = test_signer(9).await;
        let invoker = test_signer(10).await;

        let exp = Timestamp::five_minutes_from_now();

        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .expiration(exp)
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        let range = check(&invocation, &delegation_store).await?;

        assert_eq!(range.not_before, Bound::Unbounded);
        assert_eq!(range.expiration, Bound::Included(exp));
        assert!(range.contains(&Timestamp::now()));
        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_narrows_time_range_across_chain() -> TestResult {
        // Two-hop chain where each delegation has different time bounds.
        // The result should be the intersection: [later nbf, earlier exp].
        let subject = test_signer(11).await;
        let middleman = test_signer(12).await;
        let invoker = test_signer(13).await;

        let now = Timestamp::now();
        let exp_wide = Timestamp::five_years_from_now();
        let exp_narrow = Timestamp::five_minutes_from_now();

        // First delegation: wide expiration, has nbf = now
        let delegation1 = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&middleman)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .not_before(now)
            .expiration(exp_wide)
            .try_build()
            .await?;

        // Second delegation: narrow expiration, no nbf
        let delegation2 = DelegationBuilder::new()
            .issuer(middleman.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .expiration(exp_narrow)
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid1 = store_delegation(&delegation_store, delegation1).await;
        let cid2 = store_delegation(&delegation_store, delegation2).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid1, cid2])
            .try_build()
            .await?;

        let range = check(&invocation, &delegation_store).await?;

        // nbf = max(now, unbounded) = now
        assert_eq!(range.not_before, Bound::Included(now));
        // exp = min(exp_wide, exp_narrow) = exp_narrow
        assert_eq!(range.expiration, Bound::Included(exp_narrow));
        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_fails_empty_time_window() -> TestResult {
        // Two delegations with non-overlapping time windows.
        // First: expires at T1. Second: not valid before T2 where T2 > T1.
        // The intersection is empty.
        let subject = test_signer(14).await;
        let middleman = test_signer(15).await;
        let invoker = test_signer(16).await;

        // T1 = now (already in the past relative to T2)
        let t1 = Timestamp::now();
        // T2 = 5 years from now (well after T1)
        let t2 = Timestamp::five_years_from_now();

        // First delegation: expires at T1
        let delegation1 = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&middleman)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .expiration(t1)
            .try_build()
            .await?;

        // Second delegation: not valid before T2
        let delegation2 = DelegationBuilder::new()
            .issuer(middleman.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .not_before(t2)
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid1 = store_delegation(&delegation_store, delegation1).await;
        let cid2 = store_delegation(&delegation_store, delegation2).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid1, cid2])
            .try_build()
            .await?;

        let result = check(&invocation, &delegation_store).await;
        let err = result.expect_err("Should fail: time windows don't overlap");
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("no valid time window"),
            "Error should mention invalid time window, got: {err_msg}"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_invocation_expiration_narrows_range() -> TestResult {
        // Delegation has wide expiration, but invocation has a tighter one.
        let subject = test_signer(17).await;
        let invoker = test_signer(18).await;

        let exp_delegation = Timestamp::five_years_from_now();
        let exp_invocation = Timestamp::five_minutes_from_now();

        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .expiration(exp_delegation)
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid = store_delegation(&delegation_store, delegation).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .expiration(exp_invocation)
            .proofs(vec![cid])
            .try_build()
            .await?;

        let range = check(&invocation, &delegation_store).await?;

        // The invocation's tighter expiration should win
        assert_eq!(range.expiration, Bound::Included(exp_invocation));
        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn chain_check_narrow_then_wide_keeps_narrow_bounds() -> TestResult {
        // Two-hop chain: first delegation has a narrow window [now, now+5min],
        // second delegation has a wider window [no nbf, now+5years].
        // The result should be the narrow window from the first delegation.
        let subject = test_signer(19).await;
        let middleman = test_signer(20).await;
        let invoker = test_signer(21).await;

        let now = Timestamp::now();
        let exp_narrow = Timestamp::five_minutes_from_now();
        let exp_wide = Timestamp::five_years_from_now();

        // First delegation: narrow window [now, now+5min]
        let delegation1 = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&middleman)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .not_before(now)
            .expiration(exp_narrow)
            .try_build()
            .await?;

        // Second delegation: wide window [unbounded, now+5years]
        let delegation2 = DelegationBuilder::new()
            .issuer(middleman.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .expiration(exp_wide)
            .try_build()
            .await?;

        let delegation_store = new_store();
        let cid1 = store_delegation(&delegation_store, delegation1).await;
        let cid2 = store_delegation(&delegation_store, delegation2).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid1, cid2])
            .try_build()
            .await?;

        let range = check(&invocation, &delegation_store).await?;

        // nbf = max(now, unbounded) = now (narrow wins)
        assert_eq!(range.not_before, Bound::Included(now));
        // exp = min(exp_narrow, exp_wide) = exp_narrow (narrow wins)
        assert_eq!(range.expiration, Bound::Included(exp_narrow));
        Ok(())
    }

    use crate::time::timestamp::{Duration, UNIX_EPOCH};

    /// A checker that fails every lookup, standing in for an unreachable
    /// revocation service.
    #[derive(Debug, thiserror::Error)]
    #[error("revocation service unreachable")]
    struct Unreachable;

    #[derive(Debug, Clone, Copy)]
    struct OfflineRevocations;

    impl RevocationChecker for OfflineRevocations {
        type Error = Unreachable;

        async fn query(
            &self,
            _selector: RevocationSelector<'_>,
        ) -> Result<Option<RevocationMatch>, Self::Error> {
            Err(Unreachable)
        }
    }

    /// Records the candidate revoker sets it was queried with.
    #[derive(Debug, Clone, Default)]
    struct RecordingRevocations(Rc<RefCell<Vec<Vec<Did>>>>);

    impl RevocationChecker for RecordingRevocations {
        type Error = Unreachable;

        async fn query(
            &self,
            selector: RevocationSelector<'_>,
        ) -> Result<Option<RevocationMatch>, Self::Error> {
            RefCell::borrow_mut(&self.0).push(selector.by.to_vec());
            Ok(None)
        }
    }

    /// A checker that reports every delegation as revoked.
    #[derive(Debug, Clone)]
    struct AllRevoked(Did);

    impl RevocationChecker for AllRevoked {
        type Error = Unreachable;

        async fn query(
            &self,
            selector: RevocationSelector<'_>,
        ) -> Result<Option<RevocationMatch>, Self::Error> {
            Ok(Some(RevocationMatch {
                revocation: selector.delegation,
                principal: self.0.clone(),
            }))
        }
    }

    /// A resolver that counts how many times each DID was resolved.
    #[derive(Debug, Default, Clone)]
    struct CountingResolver(Rc<RefCell<usize>>);

    impl dialog_varsig::Resolver<Ed25519Signature> for CountingResolver {
        type Error = dialog_credentials::DidKeyResolveError;

        async fn resolve(
            &self,
            did: &Did,
        ) -> Result<impl dialog_varsig::Verifier<Ed25519Signature>, Self::Error> {
            *RefCell::borrow_mut(&self.0) += 1;
            dialog_varsig::Resolver::<Ed25519Signature>::resolve(&DidKeyResolver, did).await
        }
    }

    /// Build a one-link chain: `subject -> invoker`, invoked by `invoker`.
    async fn one_link_chain(
        expiration: Option<Timestamp>,
    ) -> TestResult<(Invocation<Ed25519Signature>, DelegationStore)> {
        let subject = test_signer(90).await;
        let invoker = test_signer(91).await;

        let mut builder = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()]);
        if let Some(exp) = expiration {
            builder = builder.expiration(exp);
        }
        let delegation = builder.try_build().await?;

        let store = new_store();
        let cid = store_delegation(&store, delegation).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        Ok((invocation, store))
    }

    /// Build an `n`-hop chain rooted at `subject`, invoked by the final
    /// audience. Returns the invocation, the store, and the signers in chain
    /// order (`[subject, hop1, hop2, ..., invoker]`).
    async fn multi_hop_chain(
        hops: usize,
    ) -> TestResult<(
        Invocation<Ed25519Signature>,
        DelegationStore,
        Vec<Ed25519Signer>,
    )> {
        assert!(hops >= 1, "a chain needs at least one delegation");

        let mut signers = Vec::with_capacity(hops + 1);
        for seed in 0..=hops {
            signers.push(test_signer(120 + u8::try_from(seed).expect("small seed")).await);
        }
        let subject = signers[0].clone();

        let store = new_store();
        let mut cids = Vec::with_capacity(hops);
        for hop in 0..hops {
            let delegation = DelegationBuilder::new()
                .issuer(signers[hop].clone())
                .audience(&signers[hop + 1])
                .subject(Subject::Specific(subject.did()))
                .command(vec!["test".to_string()])
                .try_build()
                .await?;
            cids.push(store_delegation(&store, delegation).await);
        }

        let invoker = signers[hops].clone();
        let invocation = InvocationBuilder::new()
            .issuer(invoker)
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(cids)
            .try_build()
            .await?;

        Ok((invocation, store, signers))
    }

    /// Run a verification whose revocation checker records what it was asked,
    /// returning the candidate revoker set per link, in chain order.
    async fn recorded_revokers(
        invocation: &Invocation<Ed25519Signature>,
        store: &DelegationStore,
    ) -> TestResult<Vec<Vec<Did>>> {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let environment: Environment<
            DelegationStore,
            DidKeyResolver,
            RecordingRevocations,
            Rc<Delegation<Ed25519Signature>>,
        > = Environment::new(
            store.clone(),
            DidKeyResolver,
            RecordingRevocations(seen.clone()),
        );
        let ctx = VerificationContext::new(&environment);
        invocation
            .check::<Local, _, _, _>(&ctx)
            .await
            .map_err(|e| e.to_string())?;

        let mut recorded = RefCell::borrow(&seen).clone();
        // Queries complete concurrently, so order by chain position: each
        // link's prefix is strictly longer than the one before it.
        recorded.sort_by_key(Vec::len);
        Ok(recorded)
    }

    #[dialog_common::test]
    async fn an_expired_delegation_is_refused() -> TestResult {
        // Expired in 2001. Before this check existed, it authorized.
        let expired = Timestamp::try_from(UNIX_EPOCH + Duration::from_secs(1_000_000_000))?;
        let (invocation, store) = one_link_chain(Some(expired)).await?;

        let result = check(&invocation, &store).await;
        assert!(result.is_err(), "an expired delegation must not authorize");
        Ok(())
    }

    #[dialog_common::test]
    async fn an_expired_delegation_passes_when_time_is_not_judged() -> TestResult {
        // `None` is the explicit opt-out: replaying history, or no trusted
        // clock. It must skip the bound rather than default to the epoch.
        let expired = Timestamp::try_from(UNIX_EPOCH + Duration::from_secs(1_000_000_000))?;
        let (invocation, store) = one_link_chain(Some(expired)).await?;

        check_at(&invocation, &store, None).await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn a_delegation_is_refused_before_it_is_valid() -> TestResult {
        let subject = test_signer(92).await;
        let invoker = test_signer(93).await;

        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&invoker)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .not_before(Timestamp::five_years_from_now())
            .try_build()
            .await?;

        let store = new_store();
        let cid = store_delegation(&store, delegation).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        assert!(
            check(&invocation, &store).await.is_err(),
            "a not-yet-valid delegation must not authorize"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn each_distinct_issuer_is_resolved_exactly_once() -> TestResult {
        // Regression guard for the concurrency-vs-cache hazard: the caching
        // resolver has no in-flight dedup, so resolving per link would turn
        // one fetch into N for a repeated DID.
        let (invocation, store) = one_link_chain(None).await?;

        let counter = Rc::new(RefCell::new(0usize));
        let environment = Environment::new(
            store.clone(),
            CountingResolver(counter.clone()),
            UnverifiedRevocations,
        );
        let ctx = VerificationContext::new(&environment);
        invocation
            .check::<Local, _, _, _>(&ctx)
            .await
            .map_err(|e| e.to_string())?;

        // Two distinct issuers here (subject and invoker), each resolved once.
        assert_eq!(
            *RefCell::borrow(&*counter),
            2,
            "each distinct issuer resolves once"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn a_structural_failure_costs_no_resolutions() -> TestResult {
        // Pins the phase ordering: structure is judged before any I/O.
        let subject = test_signer(94).await;
        let invoker = test_signer(95).await;
        let stranger = test_signer(96).await;

        // Delegation to someone other than the invoker: misaligned chain.
        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&stranger)
            .subject(Subject::Specific(subject.did()))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let store = new_store();
        let cid = store_delegation(&store, delegation).await;

        let invocation = InvocationBuilder::new()
            .issuer(invoker.clone())
            .audience(&subject)
            .subject(&subject)
            .command(vec!["test".to_string()])
            .proofs(vec![cid])
            .try_build()
            .await?;

        let counter = Rc::new(RefCell::new(0usize));
        let environment = Environment::new(
            store.clone(),
            CountingResolver(counter.clone()),
            UnverifiedRevocations,
        );
        let ctx = VerificationContext::new(&environment);
        let result = invocation.check::<Local, _, _, _>(&ctx).await;

        assert!(result.is_err(), "a misaligned chain must be refused");
        assert_eq!(
            *RefCell::borrow(&*counter),
            0,
            "a structural failure must not pay for any DID resolution"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn an_unreachable_revocation_service_is_not_a_denial() -> TestResult {
        let (invocation, store) = one_link_chain(None).await?;

        let environment = Environment::new(store.clone(), DidKeyResolver, OfflineRevocations);
        let ctx = VerificationContext::new(&environment);
        let err = invocation
            .check::<Local, _, _, _>(&ctx)
            .await
            .expect_err("a strict checker must not accept an unknown status");

        // It must read as "could not check", never as "invalid".
        assert!(
            matches!(
                err,
                VerifyError::Unavailable(Unavailable::RevocationLookup { .. })
            ),
            "expected an unavailability, got: {err:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn tolerating_an_unreachable_service_lets_a_valid_chain_pass() -> TestResult {
        // Wrapping the checker is the whole opt-in: verification stays strict
        // otherwise, and the caller made this choice where the environment
        // was built.
        let (invocation, store) = one_link_chain(None).await?;

        let environment = Environment::new(
            store.clone(),
            DidKeyResolver,
            OfflineRevocations.tolerate_unavailable(),
        );
        let ctx = VerificationContext::new(&environment);
        invocation
            .check::<Local, _, _, _>(&ctx)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    #[dialog_common::test]
    async fn tolerating_still_refuses_a_confirmed_revocation() -> TestResult {
        // Tolerance is about not knowing, never knowing-and-ignoring.
        let (invocation, store) = one_link_chain(None).await?;
        let revoker = test_did(90).await;

        let environment = Environment::new(
            store.clone(),
            DidKeyResolver,
            AllRevoked(revoker).tolerate_unavailable(),
        );
        let ctx = VerificationContext::new(&environment);
        let err = invocation
            .check::<Local, _, _, _>(&ctx)
            .await
            .expect_err("a confirmed revocation must be refused");

        assert!(
            matches!(err, VerifyError::Invalid(Invalid::Revoked { .. })),
            "expected a revocation refusal, got: {err:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn an_invalid_chain_beats_an_unreachable_revocation_service() -> TestResult {
        // The central invariant: if the chain is bad, the caller must learn
        // *that*, never "revocation service unreachable". Otherwise a caller
        // willing to fail open on unavailability would accept a bad chain.
        let expired = Timestamp::try_from(UNIX_EPOCH + Duration::from_secs(1_000_000_000))?;
        let (invocation, store) = one_link_chain(Some(expired)).await?;

        let environment = Environment::new(store.clone(), DidKeyResolver, OfflineRevocations);
        let ctx = VerificationContext::new(&environment);
        let err = invocation
            .check::<Local, _, _, _>(&ctx)
            .await
            .expect_err("an expired chain must be refused");

        assert!(
            matches!(err, VerifyError::Invalid(_)),
            "an invalid chain must report its invalidity, got: {err:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn a_single_link_is_revocable_by_its_issuer_and_its_audience() -> TestResult {
        // The root grant: its issuer made it, its audience received it, and
        // either may withdraw it.
        let (invocation, store, signers) = multi_hop_chain(1).await?;
        let recorded = recorded_revokers(&invocation, &store).await?;

        assert_eq!(recorded.len(), 1, "one query per link");
        assert_eq!(recorded[0], vec![signers[0].did(), signers[1].did()]);
        Ok(())
    }

    #[dialog_common::test]
    async fn each_link_is_revocable_by_its_prefix_and_its_own_audience() -> TestResult {
        // a -> b -> c -> d
        //   check a  => [a.iss, a.aud]
        //   check b  => [a.iss, b.iss, b.aud]
        //   check c  => [a.iss, b.iss, c.iss, c.aud]
        let (invocation, store, signers) = multi_hop_chain(3).await?;
        let recorded = recorded_revokers(&invocation, &store).await?;

        assert_eq!(recorded.len(), 3, "one query per link");
        assert_eq!(recorded[0], vec![signers[0].did(), signers[1].did()]);
        assert_eq!(
            recorded[1],
            vec![signers[0].did(), signers[1].did(), signers[2].did()]
        );
        assert_eq!(
            recorded[2],
            vec![
                signers[0].did(),
                signers[1].did(),
                signers[2].did(),
                signers[3].did()
            ]
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn a_downstream_principal_may_not_revoke_an_upstream_link() -> TestResult {
        // The central rule: in a -> b -> c -> d, d's issuer must not appear
        // when checking c. Authority to revoke flows downward, so a principal
        // that merely received authority cannot revoke the grant it rests on.
        let (invocation, store, signers) = multi_hop_chain(3).await?;
        let recorded = recorded_revokers(&invocation, &store).await?;

        // Link index i is delegation signers[i] -> signers[i+1]. Everyone
        // strictly below it must be absent.
        for (link, candidates) in recorded.iter().enumerate() {
            for (position, signer) in signers.iter().enumerate().skip(link + 2) {
                assert!(
                    !candidates.contains(&signer.did()),
                    "link {link}: principal at position {position} is downstream \
                     and must not be able to revoke it"
                );
            }
        }
        Ok(())
    }

    #[dialog_common::test]
    async fn the_invocation_issuer_may_revoke_only_the_link_it_received() -> TestResult {
        // The invoker is the final delegation's audience, so it may revoke
        // that link — but no earlier one.
        let (invocation, store, signers) = multi_hop_chain(3).await?;
        let recorded = recorded_revokers(&invocation, &store).await?;
        let invoker = signers.last().expect("chain has signers").did();

        assert!(
            recorded[2].contains(&invoker),
            "the invoker received the last link and may disclaim it"
        );
        assert!(
            !recorded[0].contains(&invoker) && !recorded[1].contains(&invoker),
            "the invoker must not be able to revoke links it did not receive"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn a_self_issued_invocation_queries_no_revocations() -> TestResult {
        // No proofs means no links, so there is nothing to look up.
        let signer = test_signer(140).await;
        let invocation = InvocationBuilder::new()
            .issuer(signer.clone())
            .audience(&signer)
            .subject(&signer)
            .command(vec!["test".to_string()])
            .proofs(vec![])
            .try_build()
            .await?;

        let recorded = recorded_revokers(&invocation, &new_store()).await?;
        assert!(recorded.is_empty(), "no proofs, no revocation queries");
        Ok(())
    }

    #[dialog_common::test]
    async fn a_revocation_by_an_authorized_principal_refuses_the_chain() -> TestResult {
        let (invocation, store, signers) = multi_hop_chain(2).await?;
        let root = signers[0].did();

        let environment: Environment<
            DelegationStore,
            DidKeyResolver,
            AllRevoked,
            Rc<Delegation<Ed25519Signature>>,
        > = Environment::new(store.clone(), DidKeyResolver, AllRevoked(root));
        let ctx = VerificationContext::new(&environment);
        let err = invocation
            .check::<Local, _, _, _>(&ctx)
            .await
            .expect_err("a revoked link must refuse the chain");

        assert!(
            matches!(err, VerifyError::Invalid(Invalid::Revoked { .. })),
            "expected a revocation refusal, got: {err:?}"
        );
        Ok(())
    }
}
