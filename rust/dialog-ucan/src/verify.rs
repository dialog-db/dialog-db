//! UCAN invocation verification.
//!
//! This module handles:
//! 1. Parsing DAG-CBOR invocations
//! 2. Signature verification
//! 3. Delegation chain validation
//! 4. Time bounds checking

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use ipld_core::cid::Cid;
use serde_ipld_dagcbor;
use ucan::{Delegation, did::Ed25519Did, future::Sendable, invocation::Invocation};

type DelegationStore = Arc<Mutex<HashMap<Cid, Arc<Delegation<Ed25519Did>>>>>;

/// Errors that can occur during UCAN verification.
#[derive(Debug, thiserror::Error)]
pub enum VerificationError {
    /// Failed to parse the invocation or delegation from DAG-CBOR.
    #[error("Failed to parse: {0}")]
    ParseError(String),

    /// Signature verification failed.
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),

    /// Audience does not match subject.
    ///
    /// UCAN invocations must be addressed to the space they operate on,
    /// meaning `aud` must equal `sub`.
    #[error("Audience mismatch: expected {expected}, got {got}")]
    AudienceMismatch {
        /// Expected audience (the subject DID)
        expected: String,
        /// Actual audience in the invocation
        got: String,
    },

    /// Invocation has expired.
    #[error("Invocation expired")]
    Expired,

    /// A proof delegation has expired.
    #[error("Proof[{index}] expired")]
    ProofExpired {
        /// Index of the expired proof in the proof array
        index: usize,
    },

    /// A proof delegation is not yet valid (nbf is in the future).
    #[error("Proof[{index}] not yet valid")]
    ProofNotYetValid {
        /// Index of the not-yet-valid proof in the proof array
        index: usize,
    },

    /// Subject is not authorized by the proof chain.
    #[error("Subject not allowed by proof")]
    SubjectNotAllowed,

    /// The proof issuer chain is invalid.
    #[error("Invalid proof issuer chain")]
    InvalidIssuerChain,

    /// The root proof issuer is not the subject.
    #[error("Root proof issuer is not the subject")]
    RootIssuerNotSubject,

    /// Command in invocation does not match the delegated command.
    #[error("Command mismatch: expected {expected:?}, found {found:?}")]
    CommandMismatch {
        /// The command expected by the delegation
        expected: Vec<String>,
        /// The command found in the invocation
        found: Vec<String>,
    },

    /// Required proof not found in the delegation store.
    #[error("Proof not found: {0}")]
    ProofNotFound(String),

    /// Policy predicate check failed.
    #[error("Predicate failed: {0}")]
    PredicateFailed(String),

    /// Policy predicate execution error.
    #[error("Predicate run error: {0}")]
    PredicateRunError(String),

    /// Waiting on an unresolved promise in the delegation chain.
    #[error("Waiting on promise: {0}")]
    WaitingOnPromise(String),

    /// Internal error (e.g., lock poisoned).
    #[error("Internal error: {0}")]
    InternalError(String),
}

/// Result of successful UCAN verification.
///
/// Contains the verified command, subject, and issuer, which can be used
/// to authorize the requested operation and for audit logging.
#[derive(Debug, Clone)]
pub struct VerifiedInvocation {
    /// The command being invoked (e.g., `["http", "get"]`).
    pub command: Vec<String>,

    /// The verified subject (space DID).
    ///
    /// This is the DID that the invocation operates on, and the delegation
    /// chain has been verified to grant the invoker authority over this subject.
    pub subject: String,

    /// The issuer DID (operator who signed the invocation).
    ///
    /// This identifies who made the request. The delegation chain proves
    /// that this issuer has been granted authority over the subject.
    pub issuer: String,
}

/// Verify a UCAN invocation.
///
/// This performs complete verification:
/// 1. Parse the DAG-CBOR bytes into an Invocation
/// 2. Verify the Ed25519 signature
/// 3. Check that `aud` matches `sub` (invocation addressed to space)
/// 4. Validate time bounds
/// 5. Verify the delegation chain using provided proofs
///
/// # Arguments
///
/// * `cbor_bytes` - The raw DAG-CBOR encoded invocation
/// * `proof_bytes` - Array of DAG-CBOR encoded delegation proofs
///
/// # Returns
///
/// * `Ok(VerifiedInvocation)` - Verification succeeded
/// * `Err(VerificationError)` - Verification failed
///
/// # Example
///
/// ```ignore
/// let result = verify_invocation(&invocation_cbor, &[delegation_cbor]).await?;
/// assert_eq!(result.command, vec!["http", "get"]);
/// ```
pub async fn verify_invocation(
    cbor_bytes: &[u8],
    proof_bytes: &[Vec<u8>],
) -> Result<VerifiedInvocation, VerificationError> {
    // Step 1: Parse the invocation
    let invocation: Invocation<Ed25519Did> = serde_ipld_dagcbor::from_slice(cbor_bytes)
        .map_err(|e| VerificationError::ParseError(e.to_string()))?;

    // Step 2: Check invocation addressed to space (aud == sub)
    if invocation.audience() != invocation.subject() {
        return Err(VerificationError::AudienceMismatch {
            expected: invocation.subject().to_string(),
            got: invocation.audience().to_string(),
        });
    }

    // Step 3: Check time bounds
    let now = chrono::Utc::now().timestamp() as u64;

    if let Some(exp) = invocation.expiration() {
        if exp.to_unix() <= now {
            return Err(VerificationError::Expired);
        }
    }

    // Step 4: Build delegation store from proofs
    let store = build_delegation_store(proof_bytes, now)?;

    // Step 5: Full verification via rs-ucan
    // This checks:
    //   - Signature is valid (issuer signed the invocation)
    //   - Proof chain is valid (issuer->subject chain via proofs)
    //   - Commands are properly attenuated
    //   - Policy predicates pass
    invocation
        .check::<Sendable, _, _>(&store)
        .await
        .map_err(|e| {
            // Convert the library error to our error type
            match e {
                ucan::invocation::InvocationCheckError::SignatureVerification(sig_err) => {
                    VerificationError::InvalidSignature(sig_err.to_string())
                }
                ucan::invocation::InvocationCheckError::StoredCheck(stored_err) => match stored_err
                {
                    ucan::invocation::StoredCheckError::GetError(get_err) => {
                        VerificationError::ProofNotFound(get_err.to_string())
                    }
                    ucan::invocation::StoredCheckError::CheckFailed(check_err) => {
                        map_check_failed(check_err)
                    }
                },
            }
        })?;

    // Step 6: Return verified invocation data
    Ok(VerifiedInvocation {
        command: invocation.command().segments().clone(),
        subject: invocation.subject().to_string(),
        issuer: invocation.issuer().to_string(),
    })
}

/// Build an in-memory delegation store from proof bytes.
///
/// This parses each proof, validates time bounds, and inserts into the store.
/// The store is then used by `Invocation::check()` to validate the proof chain.
fn build_delegation_store(
    proof_bytes: &[Vec<u8>],
    now: u64,
) -> Result<DelegationStore, VerificationError> {
    let store: DelegationStore = Arc::new(Mutex::new(HashMap::new()));

    for (i, bytes) in proof_bytes.iter().enumerate() {
        // Parse the delegation
        let delegation: Delegation<Ed25519Did> =
            serde_ipld_dagcbor::from_slice(bytes).map_err(|e| {
                VerificationError::ParseError(format!("Failed to parse proof[{}]: {}", i, e))
            })?;

        // Check time bounds - expiration
        if let Some(exp) = delegation.expiration() {
            if exp.to_unix() <= now {
                return Err(VerificationError::ProofExpired { index: i });
            }
        }

        // Check time bounds - not before
        if let Some(nbf) = delegation.not_before() {
            if nbf.to_unix() > now {
                return Err(VerificationError::ProofNotYetValid { index: i });
            }
        }

        // Compute CID and insert into store
        let cid = delegation.to_cid();
        let arc_delegation = Arc::new(delegation);

        store
            .lock()
            .map_err(|_| VerificationError::InternalError("Store lock poisoned".to_string()))?
            .insert(cid, arc_delegation);
    }

    Ok(store)
}

/// Map rs-ucan's CheckFailed error to our VerificationError.
fn map_check_failed(err: ucan::invocation::CheckFailed) -> VerificationError {
    use ucan::invocation::CheckFailed;

    match err {
        CheckFailed::InvalidProofIssuerChain => VerificationError::InvalidIssuerChain,
        CheckFailed::SubjectNotAllowedByProof => VerificationError::SubjectNotAllowed,
        CheckFailed::RootProofIssuerIsNotSubject => VerificationError::RootIssuerNotSubject,
        CheckFailed::CommandMismatch { expected, found } => VerificationError::CommandMismatch {
            expected: expected.segments().clone(),
            found: found.segments().clone(),
        },
        CheckFailed::PredicateFailed(predicate) => {
            VerificationError::PredicateFailed(format!("{:?}", predicate))
        }
        CheckFailed::PredicateRunError(run_err) => {
            VerificationError::PredicateRunError(run_err.to_string())
        }
        CheckFailed::WaitingOnPromise(waiting) => {
            VerificationError::WaitingOnPromise(format!("{:?}", waiting))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verification_error_display() {
        let err = VerificationError::AudienceMismatch {
            expected: "did:key:z6MkSpace".to_string(),
            got: "did:key:z6MkOther".to_string(),
        };
        assert!(err.to_string().contains("Audience mismatch"));
        assert!(err.to_string().contains("did:key:z6MkSpace"));
    }

    #[test]
    fn test_verified_invocation_debug() {
        let verified = VerifiedInvocation {
            command: vec!["http".to_string(), "get".to_string()],
            subject: "did:key:z6MkSubject".to_string(),
            issuer: "did:key:z6MkIssuer".to_string(),
        };
        let debug = format!("{:?}", verified);
        assert!(debug.contains("http"));
        assert!(debug.contains("did:key:z6MkSubject"));
        assert!(debug.contains("did:key:z6MkIssuer"));
    }
}
