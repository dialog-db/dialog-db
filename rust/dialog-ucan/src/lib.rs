//! UCAN verification and authorization utilities.
//!
//! This crate provides UCAN (User Controlled Authorization Networks) verification
//! functionality that can be used by access services to validate UCAN invocations
//! and delegation chains.
//!
//! # Overview
//!
//! The UCAN verification flow:
//!
//! 1. Client sends a DAG-CBOR encoded invocation with proof delegations
//! 2. Server parses and validates the invocation signature
//! 3. Server checks that audience matches subject (invocation addressed to space)
//! 4. Server validates time bounds (not expired, not before)
//! 5. Server verifies the delegation chain using rs-ucan
//! 6. On success, returns the verified command and subject
//!
//! # Example
//!
//! ```ignore
//! use dialog_ucan::{verify_invocation, VerifiedInvocation};
//!
//! async fn handle_request(invocation_bytes: &[u8], proof_bytes: &[Vec<u8>]) {
//!     match verify_invocation(invocation_bytes, proof_bytes).await {
//!         Ok(verified) => {
//!             println!("Verified command: {:?}", verified.command);
//!             println!("Subject: {}", verified.subject);
//!             println!("Issuer: {}", verified.issuer);
//!         }
//!         Err(e) => {
//!             eprintln!("Verification failed: {}", e);
//!         }
//!     }
//! }
//! ```

pub mod error;
pub mod verify;

pub use error::{ErrorCode, ServiceError};
pub use verify::{VerificationError, VerifiedInvocation, verify_invocation};
