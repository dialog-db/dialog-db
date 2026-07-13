//! Iroh authorization material.
//!
//! The [`Iroh`](super::Iroh) site authorizes forks the same way the UCAN-S3
//! site does: by building a signed UCAN invocation chain rooted in the
//! space's subject. Where the UCAN-S3 site POSTs that chain to an HTTP
//! access service in exchange for a presigned URL, the iroh transport sends
//! it to the peer itself, which verifies the chain and performs the effect
//! directly against its local replica.
//!
//! Because the chain is rooted in the *subject* (not addressed to a
//! specific peer), the same authorization can be redeemed at any peer that
//! replicates the space — the property the gossip block swarm relies on.

use dialog_ucan::UcanInvocation;

/// Iroh authorization material — the signed UCAN invocation chain sent to
/// the serving peer alongside the request.
#[derive(Debug, Clone)]
pub struct IrohAuthorization {
    invocation: UcanInvocation,
}

impl IrohAuthorization {
    /// Wrap a signed invocation as authorization material.
    pub fn new(invocation: UcanInvocation) -> Self {
        Self { invocation }
    }

    /// The signed invocation chain.
    pub fn invocation(&self) -> &UcanInvocation {
        &self.invocation
    }
}

impl From<UcanInvocation> for IrohAuthorization {
    fn from(invocation: UcanInvocation) -> Self {
        Self::new(invocation)
    }
}
