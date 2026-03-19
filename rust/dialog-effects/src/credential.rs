//! Credential capability hierarchy.
//!
//! Re-exports core credential types from [`dialog_capability::credential`]
//! and adds the [`Operator`] convenience trait for implementing credential
//! providers.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (repository DID)
//!   +-- Credential (ability: /credential)
//!         +-- Identify -> Effect -> Result<Did, CredentialError>
//!         +-- Sign { payload } -> Effect -> Result<Vec<u8>, CredentialError>
//! ```

pub use dialog_capability::credential::{
    Credential, CredentialError, Identify, Sign, SignCapability,
};
pub use dialog_capability::{Capability, Did, Subject};

/// Trait for types that can provide credential operations.
///
/// Implementors hold an operator keypair and can identify themselves
/// and sign payloads.
///
/// Implementors should also implement `Provider<Identify>` and
/// `Provider<Sign>` to be usable as an environment's credential provider.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Operator {
    /// Return the operator's DID.
    fn identify(&self) -> Result<Did, CredentialError>;

    /// Sign a payload using the operator's key.
    async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>, CredentialError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::did;

    #[test]
    fn it_re_exports_credential_types() {
        let claim = Subject::from(did!("key:zSpace")).attenuate(Credential);
        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/credential");
    }

    #[test]
    fn it_builds_identify_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .invoke(Identify);

        assert_eq!(claim.ability(), "/credential/identify");
    }

    #[test]
    fn it_builds_sign_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .invoke(Sign::new(b"hello"));

        assert_eq!(claim.ability(), "/credential/sign");
    }
}
