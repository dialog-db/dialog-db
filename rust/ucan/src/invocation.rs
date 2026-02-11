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
    subject::Subject,
    time::{range::TimeRange, timestamp::Timestamp},
};
use builder::InvocationBuilder;
use ipld_core::{cid::Cid, ipld::Ipld};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, Visitor},
};
use std::{
    borrow::{Borrow, Cow},
    collections::BTreeMap,
    fmt::Debug,
};
use thiserror::Error;
use varsig::{Did, Resolver, Signature, Verifier};

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

    /// Check if this invocation is valid.
    ///
    /// This method performs two checks:
    /// 1. Verifies that the invocation's signature is valid
    /// 2. Validates the proof chain using the provided delegation store
    ///
    /// # Errors
    ///
    /// Returns an [`InvocationCheckError`] if signature verification fails
    /// or if the proof chain validation fails.
    pub async fn check<
        K: FutureKind,
        T: Borrow<Delegation<S>>,
        St: DelegationStore<K, S, T>,
        R: Resolver<S>,
    >(
        &self,
        proof_store: &St,
        resolver: &R,
    ) -> Result<TimeRange, InvocationCheckError<K, S, T, St, R>> {
        // 1. Verify signature
        self.verify_signature(resolver)
            .await
            .map_err(InvocationCheckError::SignatureVerification)?;

        // 2. Check proof chain and compute valid time range
        let time_range = self
            .payload()
            .check(proof_store)
            .await
            .map_err(InvocationCheckError::StoredCheck)?;

        Ok(time_range)
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
        let encoded = self
            .envelope()
            .encode()
            .map_err(SignatureVerificationError::EncodingError)?;
        let verifier = resolver
            .resolve(self.issuer())
            .await
            .map_err(SignatureVerificationError::ResolutionError)?;
        Verifier::verify(&verifier, &encoded, self.signature())
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

    /// Check if an [`InvocationPayload`] with proofs stored in a delegation store is valid.
    ///
    /// # Errors
    ///
    /// Returns a [`StoredCheckError`] if the check fails.
    pub async fn check<
        K: FutureKind,
        S: Signature,
        T: Borrow<Delegation<S>>,
        St: DelegationStore<K, S, T>,
    >(
        &self,
        proof_store: &St,
    ) -> Result<TimeRange, StoredCheckError<K, S, T, St>> {
        let realized_proofs: Vec<T> = proof_store
            .get_all(&self.proofs)
            .await
            .map_err(StoredCheckError::GetError)?;
        let dlgs: Vec<&Delegation<S>> = realized_proofs.iter().map(Borrow::borrow).collect();
        Ok(self.syntactic_checks(dlgs)?)
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
    ) -> Result<TimeRange, CheckFailed> {
        let args: Ipld = self
            .arguments()
            .iter()
            .map(|(k, v)| v.try_into().map(|ipld| (k.clone(), ipld)))
            .collect::<Result<BTreeMap<String, Ipld>, _>>()?
            .into();

        // Collect proofs and normalize to root-to-leaf order.
        // The spec is ambiguous about proof ordering in `prf`:
        // https://github.com/ucan-wg/invocation/issues/41
        // TODO: settle on a single order once the spec clarifies this.
        let mut proofs: Vec<&'a Delegation<S>> = proofs.into_iter().collect();
        if proofs.len() > 1 && proofs.last().is_some_and(|p| p.issuer() == self.subject()) {
            proofs.reverse();
        }

        // Start with the invocation's own time bounds.
        let mut time_range = TimeRange::from(self);

        // Hold a last proof that was verified in the chain.
        let mut authorization: Option<&'a Delegation<S>> = None;

        for proof in proofs {
            // Resolve the delegation's subject: Specific(did) uses that did,
            // Any falls back to the previously implied subject.
            let subject = match proof.subject() {
                Subject::Specific(subject) => subject,
                Subject::Any => {
                    if authorization.is_none() {
                        proof.issuer()
                    } else {
                        self.subject()
                    }
                }
            };

            if subject != self.subject() {
                if authorization.is_none() && matches!(proof.subject(), Subject::Any) {
                    return Err(CheckFailed::UnprovenSubject {
                        subject: self.subject().clone(),
                        issuer: proof.issuer().clone(),
                    });
                }
                return Err(CheckFailed::UnauthorizedSubject {
                    claimed: self.subject().clone(),
                    authorized: subject.clone(),
                });
            }

            // Verify principal alignment: root proof's issuer must be the
            // subject, subsequent proofs' issuers must match previous audience.
            if let Some(evidence) = authorization {
                if proof.issuer() != evidence.audience() {
                    return Err(CheckFailed::DelegationAudienceMismatch {
                        claimed: proof.issuer().clone(),
                        authorized: evidence.audience().clone(),
                    });
                }
            } else if proof.issuer() != self.subject() {
                return Err(CheckFailed::UnprovenSubject {
                    subject: self.subject().clone(),
                    issuer: proof.issuer().clone(),
                });
            }

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

            // Intersect with this delegation's time bounds.
            time_range = time_range.intersect(proof.into());

            authorization = Some(proof);
        }

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

        // Verify the accumulated time window is non-empty.
        if !time_range.is_valid() {
            return Err(CheckFailed::InvalidTimeWindow { range: time_range });
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
}

/// Errors that can occur when checking an invocation with proofs stored in a delegation store
#[derive(Debug, Clone, Error)]
pub enum StoredCheckError<
    K: FutureKind,
    S: Signature,
    T: Borrow<Delegation<S>>,
    St: DelegationStore<K, S, T>,
> {
    /// Error getting proofs from the store
    #[error(transparent)]
    GetError(St::GetError),

    /// Proof check failed
    #[error(transparent)]
    CheckFailed(#[from] CheckFailed),
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
pub enum InvocationCheckError<
    K: FutureKind,
    S: Signature,
    T: Borrow<Delegation<S>>,
    St: DelegationStore<K, S, T>,
    R: Resolver<S>,
> {
    /// Signature verification failed
    #[error(transparent)]
    SignatureVerification(SignatureVerificationError<R::Error>),

    /// Proof chain check failed
    #[error(transparent)]
    StoredCheck(StoredCheckError<K, S, T, St>),
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
        promise::Promised,
        subject::Subject,
        time::{TimeRange, Timestamp},
    };
    use builder::InvocationBuilder;
    use dialog_credentials::ed25519::{Ed25519KeyResolver, Ed25519Signer};
    use std::{
        cell::RefCell,
        collections::HashMap,
        ops::{Bound, RangeBounds},
        rc::Rc,
        str::FromStr,
    };
    use testresult::TestResult;
    use varsig::{did::Did, eddsa::Ed25519Signature, principal::Principal};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test;

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

        let resolver = Ed25519KeyResolver;
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
        let resolver = Ed25519KeyResolver;
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
        let resolver = Ed25519KeyResolver;
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
        let resolver = Ed25519KeyResolver;
        invocation.verify_signature(&resolver).await?;

        Ok(())
    }

    /// Helper to create an `Rc<RefCell<HashMap>>` delegation store.
    fn new_store() -> Rc<RefCell<HashMap<ipld_core::cid::Cid, Rc<Delegation<Ed25519Signature>>>>> {
        Rc::new(RefCell::new(HashMap::new()))
    }

    /// Helper to insert a delegation into the store and return its CID.
    async fn store_delegation(
        store: &Rc<RefCell<HashMap<ipld_core::cid::Cid, Rc<Delegation<Ed25519Signature>>>>>,
        delegation: Delegation<Ed25519Signature>,
    ) -> ipld_core::cid::Cid {
        store::insert(store, Rc::new(delegation)).await.unwrap()
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

        invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        let result = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await;
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
        let result = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await;
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
            let resolver = Ed25519KeyResolver;
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

        invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        let result = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await;
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

        let result = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await;
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

        invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        let result = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await;
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

        invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        let result = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await;
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

        let result = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await;
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

        invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        let result = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await;
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

        invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        let result = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await;
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

        invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        let result = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await;
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

        invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

        Ok(())
    }

    // --- Time bounds tests ---

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

        let range = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        let range = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        let range = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        let result = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await;
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

        let range = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

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

        let range = invocation
            .check(&delegation_store, &Ed25519KeyResolver)
            .await?;

        // nbf = max(now, unbounded) = now (narrow wins)
        assert_eq!(range.not_before, Bound::Included(now));
        // exp = min(exp_narrow, exp_wide) = exp_narrow (narrow wins)
        assert_eq!(range.expiration, Bound::Included(exp_narrow));
        Ok(())
    }
}
