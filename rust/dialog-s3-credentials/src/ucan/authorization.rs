//! UCAN authorization proof management.
//!
//! This module provides [`UcanAuthorization`], which represents a proof of authority
//! for a specific capability claim using UCAN delegations.

use super::delegation::DelegationChain;
use dialog_common::capability::{Authorization, Did};

/// UCAN-based authorization proof for a capability.
///
/// This enum represents authorization in two forms:
/// - `Owned`: The subject is the same as the audience (self-authorization)
/// - `Delegated`: Authority is proven through a UCAN delegation chain
#[derive(Debug, Clone)]
pub enum UcanAuthorization {
    /// Self-authorization where subject == audience.
    Owned {
        /// The subject DID (also the audience).
        subject: Did,
        /// The command path this authorization permits.
        can: String,
    },
    /// Authorization through a delegation chain.
    Delegated {
        /// The delegation chain proving authority.
        chain: DelegationChain,
        /// Cached subject DID string.
        subject: Did,
        /// Cached audience DID string.
        audience: Did,
        /// Cached command path.
        can: String,
    },
}

impl UcanAuthorization {
    /// Create a self-issued authorization for an owner.
    pub fn owned(subject: impl Into<Did>, can: impl Into<String>) -> Self {
        Self::Owned {
            subject: subject.into(),
            can: can.into(),
        }
    }

    /// Create an authorization from a delegation chain.
    pub fn delegated(chain: DelegationChain) -> Self {
        // Pre-compute and cache the string representations
        let subject = chain
            .subject()
            .map(|did| did.to_string())
            .unwrap_or_default();
        let audience = chain.audience().to_string();
        let can = chain.can();

        Self::Delegated {
            chain,
            subject,
            audience,
            can,
        }
    }

    /// Get the delegation chain, if this is a delegated authorization.
    pub fn chain(&self) -> Option<&DelegationChain> {
        match self {
            Self::Owned { .. } => None,
            Self::Delegated { chain, .. } => Some(chain),
        }
    }
}

impl Authorization for UcanAuthorization {
    fn subject(&self) -> &Did {
        match self {
            Self::Owned { subject, .. } => subject,
            Self::Delegated { subject, .. } => subject,
        }
    }

    fn audience(&self) -> &Did {
        match self {
            Self::Owned { subject, .. } => subject, // For owned, audience == subject
            Self::Delegated { audience, .. } => audience,
        }
    }

    fn can(&self) -> &str {
        match self {
            Self::Owned { can, .. } => can,
            Self::Delegated { can, .. } => can,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ucan::delegation::tests::{create_delegation, generate_signer};

    #[test]
    fn it_creates_owned_authorization() {
        let auth = UcanAuthorization::owned("did:key:zTest", "/storage/get");

        assert_eq!(auth.subject(), "did:key:zTest");
        assert_eq!(auth.audience(), "did:key:zTest");
        assert_eq!(auth.can(), "/storage/get");
        assert!(auth.chain().is_none());
    }

    #[test]
    fn it_creates_delegated_authorization() {
        let subject_signer = generate_signer();
        let subject_did = subject_signer.did().clone();
        let operator_signer = generate_signer();

        let delegation = create_delegation(
            &subject_signer,
            operator_signer.did(),
            &subject_did,
            vec!["storage".to_string(), "get".to_string()],
        )
        .unwrap();

        let chain = DelegationChain::new(delegation);
        let auth = UcanAuthorization::delegated(chain);

        assert_eq!(auth.subject(), &subject_did.to_string());
        assert_eq!(auth.audience(), &operator_signer.did().to_string());
        assert_eq!(auth.can(), "/storage/get");
        assert!(auth.chain().is_some());
    }
}
