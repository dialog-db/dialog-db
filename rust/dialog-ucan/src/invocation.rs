//! Signed UCAN invocation.

use dialog_capability::Did;
use dialog_ucan_core::InvocationChain;
use dialog_varsig::eddsa::Ed25519Signature;

/// A signed UCAN invocation ready to be redeemed at an access service.
///
/// Contains the signed invocation chain and metadata. The chain proves
/// the invoker's authority to perform the operation. To actually execute
/// against a remote service, send the serialized chain to the access
/// service endpoint (transport-specific, handled by `dialog-remote-ucan-s3`).
#[derive(Debug, Clone)]
pub struct UcanInvocation {
    /// The signed invocation chain (invocation + delegation proofs).
    pub chain: Box<InvocationChain<Ed25519Signature>>,
    /// The subject DID this invocation acts on.
    pub subject: Did,
    /// The ability path (e.g., "/storage/get").
    pub ability: String,
}

impl UcanInvocation {
    /// Get the subject DID.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Get the ability path.
    pub fn ability(&self) -> &str {
        &self.ability
    }

    /// Get the invocation chain.
    pub fn chain(&self) -> &InvocationChain<Ed25519Signature> {
        &self.chain
    }

    /// Serialize the invocation chain to bytes.
    ///
    /// Returns the CBOR-encoded container suitable for sending to an
    /// access service endpoint.
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        self.chain.to_bytes().map_err(|e| e.to_string())
    }
}
