//! Credential capability hierarchy and remote resource authorization.
//!
//! Provides identity, signing, and credential store operations scoped to a
//! repository subject via the [`Profile`] policy.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (operator DID)
//! └── Credential (ability: /credential)
//!     └── Profile { profile: String }  (policy, scopes to named profile)
//!         ├── Identify -> Effect -> Result<Identity, CredentialError>
//!         ├── Sign { payload } -> Effect -> Result<Vec<u8>, CredentialError>
//!         ├── Authorize<Fx, S> { capability } -> Effect -> Result<S::Authorization<Fx>, AuthorizeError>
//!         ├── Retrieve<C> { address } -> Effect -> Result<C, CredentialError>
//!         ├── Save<C> { address, credentials } -> Effect -> Result<(), CredentialError>
//!         ├── List<C> { prefix } -> Effect -> Result<Vec<Address<C>>, CredentialError>
//!         └── Import<M> { material: M } -> Effect -> Result<(), CredentialError>
//! ```

pub use crate::{Attenuation, Capability, Did, Effect, Policy, Subject};
use crate::{Claim, Constraint};
use dialog_common::ConditionalSend;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use thiserror::Error;

pub use crate::site::Local;

/// Trait for types that can provide a credential lookup address.
///
/// Implemented by site address types to connect addresses to their
/// corresponding credential type for lookup in the credential store.
pub trait Addressable<C> {
    /// Get the credential address for looking up credentials.
    fn credential_address(&self) -> Address<C>;
}

impl Addressable<()> for () {
    fn credential_address(&self) -> Address<()> {
        Address::new("local")
    }
}

/// Root attenuation for credential operations.
///
/// Attaches to Subject and provides the `/credential` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Credential;

impl Attenuation for Credential {
    type Of = Subject;
}

/// Profile policy that scopes credential operations to a named profile.
///
/// A profile is a named user identity on a specific device (e.g. "default",
/// "work", "personal"), each with its own ed25519 keypair.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Profile {
    /// The profile name.
    pub profile: String,
}

impl Profile {
    /// Create a new Profile policy.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            profile: name.into(),
        }
    }
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            profile: "default".to_string(),
        }
    }
}

impl Policy for Profile {
    type Of = Credential;
}

/// The active credential session: profile, operator, and optional account.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    /// The profile's DID (device-local persistent identity).
    pub profile: Did,
    /// The operator's DID (ephemeral session key).
    pub operator: Did,
    /// The account's DID (optional cross-device recovery identity).
    pub account: Option<Did>,
}

/// Identify operation — returns the credential detail for the active session.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, crate::Claim)]
pub struct Identify;

impl Effect for Identify {
    type Of = Profile;
    type Output = Result<Identity, CredentialError>;
}

/// Sign operation — signs a payload using the operator's key.
#[derive(Debug, Clone, Serialize, Deserialize, crate::Claim)]
pub struct Sign {
    /// The payload to sign.
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl Sign {
    /// Create a new Sign effect.
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            payload: payload.into(),
        }
    }
}

impl Effect for Sign {
    type Of = Profile;
    type Output = Result<Vec<u8>, CredentialError>;
}

/// Extension trait for `Capability<Sign>` to access its fields.
pub trait SignCapability {
    /// Get the payload to sign.
    fn payload(&self) -> &[u8];
}

impl SignCapability for Capability<Sign> {
    fn payload(&self) -> &[u8] {
        &Sign::of(self).payload
    }
}

/// A typed address for looking up credentials in the credential store.
///
/// The phantom type `C` ties the address to a specific credential type,
/// ensuring type-safe lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address<C> {
    /// The identifier for this credential (e.g., endpoint URL, bucket name).
    pub id: String,
    #[serde(skip)]
    _credentials: PhantomData<C>,
}

impl<C> Address<C> {
    /// Create a new address with the given identifier.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            _credentials: PhantomData,
        }
    }

    /// Get the address identifier.
    pub fn id(&self) -> &str {
        &self.id
    }
}

impl<C> PartialEq for Address<C> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

/// Retrieve credentials from the credential store by address.
#[derive(Debug, Clone, Serialize, Deserialize, crate::Claim)]
#[serde(bound(deserialize = ""))]
pub struct Retrieve<C> {
    /// The address to look up.
    pub address: Address<C>,
}

impl<C> Effect for Retrieve<C>
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    type Of = Profile;
    type Output = Result<C, CredentialError>;
}

/// Save credentials to the credential store at an address.
#[derive(Debug, Clone, Serialize, Deserialize, crate::Claim)]
#[serde(bound(deserialize = "C: DeserializeOwned"))]
pub struct Save<C: Serialize> {
    /// The address to store at.
    pub address: Address<C>,
    /// The credentials to store.
    pub credentials: C,
}

impl<C> Effect for Save<C>
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    type Of = Profile;
    type Output = Result<(), CredentialError>;
}

/// Errors that can occur during credential operations.
#[derive(Debug, Error)]
pub enum CredentialError {
    /// The signing operation failed.
    #[error("Signing failed: {0}")]
    SigningFailed(String),

    /// No credentials available.
    #[error("No credentials available: {0}")]
    NotFound(String),
}

/// Trait describing the format of authorization material.
///
/// Different authorization schemes produce different proof types:
/// - [`Allow`]: no extra material (`Authorization<Fx> = ()`)
/// - UCAN format: a signed invocation chain
pub trait AuthorizationFormat: ConditionalSend + 'static {
    /// The authorization material produced for a given capability.
    type Authorization<Fx: Constraint>: ConditionalSend;
}

/// Simple authorization format — permission granted, no extra material.
///
/// Used by sites that don't need format-specific authorization (e.g., S3
/// direct access, local operations).
pub struct Allow;

impl AuthorizationFormat for Allow {
    type Authorization<Fx: Constraint> = ();
}

/// An authorized capability paired with format-specific authorization material.
///
/// Produced by `Provider<Authorize<Fx, F>>`. Carries both the authorized
/// capability and the format-specific proof.
pub struct Authorization<Fx: Constraint, F: AuthorizationFormat> {
    /// The authorized capability.
    pub capability: Capability<Fx>,
    /// The format-specific authorization material.
    pub authorization: F::Authorization<Fx>,
}

impl<Fx: Constraint, F: AuthorizationFormat> Authorization<Fx, F> {
    /// Create a new authorization from a capability and format-specific material.
    pub fn new(capability: Capability<Fx>, authorization: F::Authorization<Fx>) -> Self {
        Self {
            capability,
            authorization,
        }
    }

    /// Unwrap the authorized capability, discarding the proof.
    pub fn into_inner(self) -> Capability<Fx> {
        self.capability
    }
}

impl<Fx: Constraint, F: AuthorizationFormat> std::ops::Deref for Authorization<Fx, F> {
    type Target = Capability<Fx>;
    fn deref(&self) -> &Self::Target {
        &self.capability
    }
}

/// Request to authorize a capability for a specific authorization format.
///
/// The format's `Authorization<Fx>` GAT determines what the authorization
/// produces — e.g., `()` for `Allow`, or `UcanInvocation` for UCAN.
#[derive(Serialize, Deserialize)]
#[serde(bound(deserialize = ""))]
pub struct Authorize<Fx: Constraint, F: AuthorizationFormat = Allow> {
    /// The capability to authorize.
    pub capability: Capability<Fx>,
    /// The target format (used for routing to the correct provider).
    #[serde(skip)]
    _format: PhantomData<F>,
}

impl<Fx: Constraint, F: AuthorizationFormat> Authorize<Fx, F> {
    /// Create a new authorization request for the given capability and format.
    pub fn new(capability: Capability<Fx>) -> Self {
        Self {
            capability,
            _format: PhantomData,
        }
    }
}

impl<Fx, F> Claim for Authorize<Fx, F>
where
    Fx: Effect,
    Fx::Of: Constraint,
    F: AuthorizationFormat,
    Capability<Fx>: ConditionalSend,
    Self: ConditionalSend + 'static,
{
    type Claim = Self;
    fn claim(self) -> Self {
        self
    }
}

impl<Fx, F> Effect for Authorize<Fx, F>
where
    Fx: Effect,
    Fx::Of: Constraint,
    F: AuthorizationFormat,
    Capability<Fx>: ConditionalSend,
    Self: ConditionalSend + 'static,
{
    type Of = Profile;
    type Output = Result<Authorization<Fx, F>, AuthorizeError>;
}

/// List credential addresses by prefix.
///
/// Returns all addresses whose ID starts with the given prefix.
/// Use [`Retrieve`] to fetch the credential at each returned address.
#[derive(Debug, Clone, Serialize, Deserialize, crate::Claim)]
#[serde(bound(deserialize = ""))]
pub struct List<C> {
    /// The prefix to match against address IDs.
    pub prefix: Address<C>,
}

impl<C> List<C> {
    /// Create a new list query with the given prefix.
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: Address::new(prefix),
        }
    }
}

impl<C> Effect for List<C>
where
    C: Serialize + DeserializeOwned + ConditionalSend + 'static,
{
    type Of = Profile;
    type Output = Result<Vec<Address<C>>, CredentialError>;
}

/// Import credential material into the credential store.
#[derive(Debug, Clone, Serialize, Deserialize, crate::Claim)]
pub struct Import<Material: Serialize> {
    /// The credential material to import.
    pub material: Material,
}

impl<Material: Serialize + DeserializeOwned + ConditionalSend + 'static> Effect
    for Import<Material>
{
    type Of = Profile;
    type Output = Result<(), CredentialError>;
}

/// Error during the authorize step.
#[derive(Debug, Error)]
pub enum AuthorizeError {
    /// Authorization was denied.
    #[error("Authorization denied: {0}")]
    Denied(String),

    /// Configuration error (e.g., missing delegation chain).
    #[error("Authorization configuration error: {0}")]
    Configuration(String),
}

/// Blanket impl: any type can authorize with `Allow` format (no proof needed).
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env, Fx> crate::Provider<Authorize<Fx, Allow>> for Env
where
    Fx: crate::Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Authorize<Fx, Allow>: ConditionalSend + 'static,
    Env: ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<Authorize<Fx, Allow>>,
    ) -> Result<Authorization<Fx, Allow>, AuthorizeError> {
        let auth_request = input.into_inner().constraint;
        Ok(Authorization::new(auth_request.capability, ()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::did;

    #[test]
    fn it_builds_credential_claim_path() {
        let claim = Subject::from(did!("key:zSpace")).attenuate(Credential);

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/credential");
    }

    #[test]
    fn it_builds_profile_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Profile::new("default"));

        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/credential");
    }

    #[test]
    fn it_builds_identify_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Profile::new("default"))
            .invoke(Identify);

        assert_eq!(claim.ability(), "/credential/identify");
    }

    #[test]
    fn it_builds_sign_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Profile::new("default"))
            .invoke(Sign::new(b"hello"));

        assert_eq!(claim.ability(), "/credential/sign");
    }

    #[test]
    fn it_extracts_payload_from_sign() {
        let cap = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Profile::new("default"))
            .invoke(Sign::new(b"payload"));

        assert_eq!(cap.payload(), b"payload");
    }

    #[test]
    fn it_builds_retrieve_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Profile::new("default"))
            .invoke(Retrieve::<String> {
                address: Address::new("s3://my-bucket"),
            });

        assert_eq!(claim.ability(), "/credential/retrieve");
    }

    #[test]
    fn it_builds_save_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Profile::new("default"))
            .invoke(Save {
                address: Address::new("s3://my-bucket"),
                credentials: "secret-key".to_string(),
            });

        assert_eq!(claim.ability(), "/credential/save");
    }
}
