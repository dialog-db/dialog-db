//! Credential capability hierarchy.
//!
//! Re-exports core credential types from [`dialog_capability::credential`]
//! and authority types from [`dialog_capability::authority`].
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject
//!   +-- Credential (ability: /credential)
//!         +-- Retrieve / Save / List / Import
//! ```

pub use dialog_capability::credential::{Credential, CredentialError};
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
}
