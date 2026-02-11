//! UCAN Delegation
//!
//! The spec for UCAN Delegations can be found at
//! [the GitHub repo](https://github.com/ucan-wg/invocation/).

pub mod builder;
pub mod policy;
pub mod store;

use crate::{
    cid::to_dagcbor_cid,
    command::Command,
    crypto::nonce::Nonce,
    envelope::{Envelope, EnvelopePayload, payload_tag::PayloadTag},
    subject::Subject,
    time::{TimeRange, Timestamp},
};
use dialog_varsig::{Signature, Verifier, did::Did};
use ipld_core::{cid::Cid, ipld::Ipld};
use policy::predicate::Predicate;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, Visitor},
};
use serde_ipld_dagcbor::error::CodecError;
use std::{borrow::Cow, collections::BTreeMap, fmt::Debug};

/// Grant or delegate a UCAN capability to another.
///
/// This type implements the [UCAN Delegation spec](https://github.com/ucan-wg/delegation/blob/main/README.md).
#[derive(Clone)]
pub struct Delegation<S: Signature>(Envelope<S, DelegationPayload>);

impl<S: Signature> Delegation<S> {
    /// Creates a blank [`DelegationBuilder`][builder::DelegationBuilder] instance.
    #[must_use]
    pub const fn builder() -> builder::DelegationBuilder<S> {
        builder::DelegationBuilder::new()
    }

    /// Getter for the `issuer` field.
    #[must_use]
    pub const fn issuer(&self) -> &Did {
        &self.payload().issuer
    }

    /// Getter for the `audience` field.
    #[must_use]
    pub const fn audience(&self) -> &Did {
        &self.payload().audience
    }

    /// Getter for the `subject` field.
    #[must_use]
    pub const fn subject(&self) -> &Subject {
        &self.payload().subject
    }

    /// Getter for the `command` field.
    #[must_use]
    pub const fn command(&self) -> &Command {
        &self.payload().command
    }

    /// Getter for the `policy` field.
    #[must_use]
    pub const fn policy(&self) -> &Vec<Predicate> {
        &self.payload().policy
    }

    /// Getter for the `expiration` field.
    #[must_use]
    pub const fn expiration(&self) -> Option<Timestamp> {
        self.payload().expiration
    }

    /// Getter for the `not_before` field.
    #[must_use]
    pub const fn not_before(&self) -> Option<Timestamp> {
        self.payload().not_before
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

    /// Compute the CID for this delegation.
    #[must_use]
    pub fn to_cid(&self) -> Cid {
        to_dagcbor_cid(&self)
    }

    const fn signature(&self) -> &S {
        &self.0.0
    }

    const fn envelope(&self) -> &EnvelopePayload<S, DelegationPayload> {
        &self.0.1
    }

    const fn payload(&self) -> &DelegationPayload {
        &self.envelope().payload
    }

    /// Verify only the signature of this delegation using a resolver.
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
        R: dialog_varsig::resolver::Resolver<S>,
    {
        let payload = self
            .envelope()
            .encode()
            .map_err(SignatureVerificationError::EncodingError)?;
        let verifier = resolver
            .resolve(self.issuer())
            .await
            .map_err(SignatureVerificationError::ResolutionError)?;
        Verifier::verify(&verifier, &payload, self.signature())
            .await
            .map_err(SignatureVerificationError::VerificationError)
    }
}

impl<S: Signature> Debug for Delegation<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Delegation").field(&self.0).finish()
    }
}

impl<S: Signature> Serialize for Delegation<S> {
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, S: Signature + for<'ze> Deserialize<'ze>> Deserialize<'de> for Delegation<S> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let envelope = Envelope::<S, DelegationPayload>::deserialize(deserializer)?;
        Ok(Delegation(envelope))
    }
}

/// The unsigned content of a [`Delegation`].
///
/// See the [UCAN Delegation payload spec](https://github.com/ucan-wg/delegation/blob/main/README.md#delegation-payload).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DelegationPayload {
    #[serde(rename = "iss")]
    pub(crate) issuer: Did,

    #[serde(rename = "aud")]
    pub(crate) audience: Did,

    #[serde(rename = "sub")]
    pub(crate) subject: Subject,

    #[serde(rename = "cmd")]
    pub(crate) command: Command,

    #[serde(rename = "pol")]
    pub(crate) policy: Vec<Predicate>,

    #[serde(rename = "exp")]
    pub(crate) expiration: Option<Timestamp>,

    #[serde(rename = "nbf", skip_serializing_if = "Option::is_none")]
    pub(crate) not_before: Option<Timestamp>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) meta: Option<BTreeMap<String, Ipld>>,

    pub(crate) nonce: Nonce,
}

impl DelegationPayload {
    /// Getter for the `issuer` field.
    #[must_use]
    pub const fn issuer(&self) -> &Did {
        &self.issuer
    }

    /// Getter for the `audience` field.
    #[must_use]
    pub const fn audience(&self) -> &Did {
        &self.audience
    }

    /// Getter for the `subject` field.
    #[must_use]
    pub const fn subject(&self) -> &Subject {
        &self.subject
    }

    /// Getter for the `command` field.
    #[must_use]
    pub const fn command(&self) -> &Command {
        &self.command
    }

    /// Getter for the `policy` field.
    #[must_use]
    pub const fn policy(&self) -> &Vec<Predicate> {
        &self.policy
    }

    /// Getter for the `expiration` field.
    #[must_use]
    pub const fn expiration(&self) -> Option<Timestamp> {
        self.expiration
    }

    /// Getter for the `not_before` field.
    #[must_use]
    pub const fn not_before(&self) -> Option<Timestamp> {
        self.not_before
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
}

impl<'de> Deserialize<'de> for DelegationPayload {
    #[allow(clippy::too_many_lines)]
    fn deserialize<T>(deserializer: T) -> Result<Self, T::Error>
    where
        T: Deserializer<'de>,
    {
        struct PayloadVisitor;

        impl<'de> Visitor<'de> for PayloadVisitor {
            type Value = DelegationPayload;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a map with keys iss,aud,sub,cmd,pol,exp,nbf,meta,nonce")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut issuer: Option<Did> = None;
                let mut audience: Option<Did> = None;
                let mut subject: Option<Subject> = None;
                let mut command: Option<Command> = None;
                let mut policy: Option<Vec<Predicate>> = None;
                let mut expiration: Option<Option<Timestamp>> = None;
                let mut not_before: Option<Option<Timestamp>> = None;
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
                            let cmd: Command = map.next_value()?;
                            command = Some(cmd);
                        }
                        "pol" => {
                            if policy.is_some() {
                                return Err(de::Error::duplicate_field("pol"));
                            }
                            policy = Some(map.next_value()?);
                        }
                        "exp" => {
                            if expiration.is_some() {
                                return Err(de::Error::duplicate_field("exp"));
                            }
                            expiration = Some(map.next_value()?);
                        }
                        "nbf" => {
                            if not_before.is_some() {
                                return Err(de::Error::duplicate_field("nbf"));
                            }
                            not_before = Some(map.next_value()?);
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
                                Ipld::String(s) => {
                                    return Err(de::Error::invalid_type(
                                        de::Unexpected::Str(&s),
                                        &"bytes",
                                    ));
                                }
                                Ipld::Integer(i) => {
                                    return Err(de::Error::invalid_type(
                                        de::Unexpected::Other(&i.to_string()),
                                        &"bytes",
                                    ));
                                }
                                Ipld::Float(f) => {
                                    return Err(de::Error::invalid_type(
                                        de::Unexpected::Float(f),
                                        &"bytes",
                                    ));
                                }
                                Ipld::Bool(b) => {
                                    return Err(de::Error::invalid_type(
                                        de::Unexpected::Bool(b),
                                        &"bytes",
                                    ));
                                }
                                Ipld::Null => {
                                    return Err(de::Error::invalid_type(
                                        de::Unexpected::Unit,
                                        &"bytes",
                                    ));
                                }
                                Ipld::List(_) => {
                                    return Err(de::Error::invalid_type(
                                        de::Unexpected::Other("list"),
                                        &"bytes",
                                    ));
                                }
                                Ipld::Map(_) => {
                                    return Err(de::Error::invalid_type(
                                        de::Unexpected::Map,
                                        &"bytes",
                                    ));
                                }
                                Ipld::Link(_) => {
                                    return Err(de::Error::invalid_type(
                                        de::Unexpected::Other("link"),
                                        &"bytes",
                                    ));
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
                                    "iss", "aud", "sub", "cmd", "pol", "exp", "nbf", "meta",
                                    "nonce",
                                ],
                            ));
                        }
                    }
                }

                let issuer = issuer.ok_or_else(|| de::Error::missing_field("iss"))?;
                let audience = audience.ok_or_else(|| de::Error::missing_field("aud"))?;
                let subject = subject.ok_or_else(|| de::Error::missing_field("sub"))?;
                let command = command.ok_or_else(|| de::Error::missing_field("cmd"))?;
                let policy = policy.ok_or_else(|| de::Error::missing_field("pol"))?;
                let nonce = nonce.ok_or_else(|| de::Error::missing_field("nonce"))?;

                Ok(DelegationPayload {
                    issuer,
                    audience,
                    subject,
                    command,
                    policy,
                    nonce,
                    expiration: expiration.unwrap_or(None),
                    not_before: not_before.unwrap_or(None),
                    meta,
                })
            }
        }

        deserializer.deserialize_map(PayloadVisitor)
    }
}

/// Error type for delegation signature verification.
#[derive(Debug, thiserror::Error)]
pub enum SignatureVerificationError<E: std::error::Error = signature::Error> {
    /// Payload encoding failed.
    #[error("encoding error: {0}")]
    EncodingError(CodecError),

    /// DID resolution failed.
    #[error("resolution error: {0}")]
    ResolutionError(E),

    /// Cryptographic verification failed.
    #[error("verification error: {0}")]
    VerificationError(signature::Error),
}

impl<S: Signature> From<&Delegation<S>> for TimeRange {
    fn from(delegation: &Delegation<S>) -> Self {
        Self::new(delegation.not_before(), delegation.expiration())
    }
}

impl PayloadTag for DelegationPayload {
    fn spec_id() -> &'static str {
        "dlg"
    }

    fn version() -> &'static str {
        "1.0.0-rc.1"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::Command, crypto::nonce::Nonce, delegation::builder::DelegationBuilder,
        subject::Subject,
    };
    use base64::prelude::*;
    use dialog_credentials::ed25519::{Ed25519KeyResolver, Ed25519Signer};
    use dialog_varsig::{did::Did, eddsa::Ed25519Signature, principal::Principal};
    use testresult::TestResult;

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
        let aud = test_signer(1).await;
        let sub = test_signer(2).await;

        let builder = DelegationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(Subject::Specific(sub.did()))
            .command(vec!["read".to_string(), "write".to_string()]);

        let delegation = builder.try_build().await?;

        assert_eq!(delegation.issuer().to_string(), iss.to_string());
        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn signature_type_inferred_from_issuer() -> TestResult {
        let delegation = DelegationBuilder::new()
            .issuer(test_signer(1).await)
            .audience(&test_did(2).await)
            .subject(Subject::Any)
            .command(vec!["test".into()])
            .try_build()
            .await?;

        assert_eq!(delegation.issuer(), &test_did(1).await);
        Ok(())
    }

    #[test]
    fn delegation_b64_fixture_roundtrip() -> TestResult {
        // Sample delegation with sub: null, cmd: "/", exp: null, meta: {}
        let b64 = "glhA0rict5hwniXnh54Y7b0v/ZEDNSlPdBx0rsoWDYC2Ylv+UzDr00s7ojPsfvNwrofqKItK911ZGJggZSkeQIB3DqJhaEg0Ae0B7QETcXN1Y2FuL2RsZ0AxLjAuMC1yYy4xqWNhdWR4OGRpZDprZXk6ejZNa2ZGSkJ4U0JGZ29BcVRRTFM3YlRmUDhNZ3lEeXB2YTVpNkNMNVBKTjhSSlpyY2NtZGEvY2V4cPZjaXNzeDhkaWQ6a2V5Ono2TWtyQXNxMU03dEVmUHZXNWRSMlVGQ3daU3pSTU5YWWVUVzh0R1pTS3ZVbTlFWmNuYmYaaSTxp2Nwb2yAY3N1YvZkbWV0YaBlbm9uY2VMVkDFeab+58p8SMpW";
        let bytes = BASE64_STANDARD.decode(b64)?;

        // Parse as Delegation
        let delegation: Delegation<Ed25519Signature> = serde_ipld_dagcbor::from_slice(&bytes)?;

        // Verify fields parsed correctly
        assert_eq!(delegation.subject(), &Subject::Any); // sub: null
        assert_eq!(delegation.command(), &vec![].into()); // cmd: "/"
        assert_eq!(delegation.expiration(), None); // exp: null
        assert!(delegation.not_before().is_some()); // nbf: 1764028839

        // Serialize back
        let reserialized = serde_ipld_dagcbor::to_vec(&delegation)?;

        // Verify byte-exact roundtrip
        assert_eq!(
            bytes, reserialized,
            "Reserialized bytes should match original"
        );

        // Deserialize again to verify roundtrip preserves all fields
        let roundtripped: Delegation<Ed25519Signature> =
            serde_ipld_dagcbor::from_slice(&reserialized)?;
        assert_eq!(roundtripped.subject(), delegation.subject());
        assert_eq!(roundtripped.command(), delegation.command());
        assert_eq!(roundtripped.expiration(), delegation.expiration());
        assert_eq!(roundtripped.not_before(), delegation.not_before());
        assert_eq!(roundtripped.issuer(), delegation.issuer());
        assert_eq!(roundtripped.audience(), delegation.audience());

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn delegation_any_subject_roundtrips() -> TestResult {
        let iss = test_signer(1).await;
        let aud = test_did(2).await;

        let delegation = DelegationBuilder::<Ed25519Signature>::new()
            .issuer(iss)
            .audience(&aud)
            .subject(Subject::Any)
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        assert_eq!(delegation.subject(), &Subject::Any);

        // Serialize to CBOR and deserialize back
        let bytes = serde_ipld_dagcbor::to_vec(&delegation)?;
        let roundtripped: Delegation<Ed25519Signature> = serde_ipld_dagcbor::from_slice(&bytes)?;

        // Subject should still be Any after roundtrip
        assert_eq!(roundtripped.subject(), &Subject::Any);

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn delegation_has_correct_fields() -> TestResult {
        let iss = test_signer(10).await;
        let aud = test_did(20).await;
        let sub = test_did(30).await;
        let cmd = vec!["storage".to_string(), "read".to_string()];

        let delegation = DelegationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(Subject::Specific(sub.clone()))
            .command(cmd.clone())
            .try_build()
            .await?;

        let iss_did: Did = iss.did();
        assert_eq!(delegation.issuer(), &iss_did);
        assert_eq!(delegation.audience(), &aud);
        assert_eq!(delegation.subject(), &Subject::Specific(sub));
        assert_eq!(delegation.command(), &Command::new(cmd));

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn delegation_signature_verifies() -> TestResult {
        let iss = test_signer(42).await;
        let aud = test_did(43).await;
        let sub = test_did(44).await;

        let delegation = DelegationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(Subject::Specific(sub))
            .command(vec!["test".to_string()])
            .try_build()
            .await?;

        let resolver = Ed25519KeyResolver;
        delegation.verify_signature(&resolver).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn delegation_serialization_roundtrip() -> TestResult {
        let iss = test_signer(50).await;
        let aud = test_did(51).await;
        let sub = test_did(52).await;

        let delegation = DelegationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(Subject::Specific(sub.clone()))
            .command(vec!["roundtrip".to_string()])
            .try_build()
            .await?;

        // Serialize to CBOR
        let bytes = serde_ipld_dagcbor::to_vec(&delegation)?;

        // Deserialize back
        let roundtripped: Delegation<Ed25519Signature> = serde_ipld_dagcbor::from_slice(&bytes)?;

        // Verify all fields match
        assert_eq!(roundtripped.issuer(), delegation.issuer());
        assert_eq!(roundtripped.audience(), delegation.audience());
        assert_eq!(roundtripped.subject(), delegation.subject());
        assert_eq!(roundtripped.command(), delegation.command());
        assert_eq!(roundtripped.nonce(), delegation.nonce());

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn delegation_with_any_subject() -> TestResult {
        let iss = test_signer(60).await;
        let aud = test_did(61).await;

        let delegation = DelegationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(Subject::Any)
            .command(vec!["any".to_string()])
            .try_build()
            .await?;

        assert_eq!(delegation.subject(), &Subject::Any);

        let resolver = Ed25519KeyResolver;
        delegation.verify_signature(&resolver).await?;

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn delegation_with_explicit_nonce_is_deterministic() -> TestResult {
        let iss = test_signer(70).await;
        let aud = test_did(71).await;
        let sub = test_did(72).await;
        let nonce = Nonce::generate_16()?;

        // Build two delegations with the same nonce
        let delegation1 = DelegationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(Subject::Specific(sub.clone()))
            .command(vec!["compare".to_string()])
            .nonce(nonce.clone())
            .try_build()
            .await?;

        let delegation2 = DelegationBuilder::<Ed25519Signature>::new()
            .issuer(iss.clone())
            .audience(&aud)
            .subject(Subject::Specific(sub.clone()))
            .command(vec!["compare".to_string()])
            .nonce(nonce)
            .try_build()
            .await?;

        // Both should have the same payload content
        assert_eq!(delegation1.issuer(), delegation2.issuer());
        assert_eq!(delegation1.audience(), delegation2.audience());
        assert_eq!(delegation1.subject(), delegation2.subject());
        assert_eq!(delegation1.command(), delegation2.command());
        assert_eq!(delegation1.nonce(), delegation2.nonce());

        // Both signatures should verify
        let resolver = Ed25519KeyResolver;
        delegation1.verify_signature(&resolver).await?;
        delegation2.verify_signature(&resolver).await?;

        // With the same nonce and same signer, the serialized form should be identical
        // because Ed25519 is deterministic
        let bytes1 = serde_ipld_dagcbor::to_vec(&delegation1)?;
        let bytes2 = serde_ipld_dagcbor::to_vec(&delegation2)?;
        assert_eq!(
            bytes1, bytes2,
            "Serialized bytes should be identical with same nonce"
        );

        Ok(())
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    async fn delegation_different_signers_different_signatures() -> TestResult {
        let iss1 = test_signer(80).await;
        let iss2 = test_signer(81).await;
        let aud = test_did(82).await;
        let nonce = Nonce::generate_16()?;

        let delegation1 = DelegationBuilder::<Ed25519Signature>::new()
            .issuer(iss1.clone())
            .audience(&aud)
            .subject(Subject::Any)
            .command(vec!["test".to_string()])
            .nonce(nonce.clone())
            .try_build()
            .await?;

        let delegation2 = DelegationBuilder::<Ed25519Signature>::new()
            .issuer(iss2.clone())
            .audience(&aud)
            .subject(Subject::Any)
            .command(vec!["test".to_string()])
            .nonce(nonce)
            .try_build()
            .await?;

        // Different issuers should produce different serialized forms
        let bytes1 = serde_ipld_dagcbor::to_vec(&delegation1)?;
        let bytes2 = serde_ipld_dagcbor::to_vec(&delegation2)?;
        assert_ne!(
            bytes1, bytes2,
            "Different signers should produce different serialized delegations"
        );

        // But both should verify with their respective keys
        let resolver = Ed25519KeyResolver;
        delegation1.verify_signature(&resolver).await?;
        delegation2.verify_signature(&resolver).await?;

        Ok(())
    }
}
