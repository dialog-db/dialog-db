//! Credential capability hierarchy.
//!
//! Re-exports core credential types from [`dialog_capability::credential`].
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject
//!   +-- Credential (ability: /credential)
//!         +-- Profile { profile: String }  (policy, scopes to named profile)
//!               +-- Identify -> Effect -> Result<Identity, CredentialError>
//!               +-- Sign { payload } -> Effect -> Result<Vec<u8>, CredentialError>
//! ```

pub use dialog_capability::credential::{
    Credential, CredentialError, Identify, Identity, Profile, Sign, SignCapability,
};
pub use dialog_capability::{Capability, Did, Subject};

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
            .attenuate(Profile::default())
            .invoke(Identify);

        assert_eq!(claim.ability(), "/credential/identify");
    }

    #[test]
    fn it_builds_sign_claim_path() {
        let claim = Subject::from(did!("key:zSpace"))
            .attenuate(Credential)
            .attenuate(Profile::default())
            .invoke(Sign::new(b"hello"));

        assert_eq!(claim.ability(), "/credential/sign");
    }
}
