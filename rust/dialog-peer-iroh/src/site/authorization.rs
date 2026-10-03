//! What authorizes an invocation at a peer.

/// A `ctn-v1` container: the signed invocation, its proofs, and the
/// blocks its arguments name.
///
/// One opaque artifact rather than a struct of parts, because parts
/// invite disagreement. The command, the subject and the arguments are
/// all inside the signed invocation, and the payload is addressed by a
/// hash the invocation commits to, so there is nothing here for a second
/// field to contradict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IrohAuthorization(Vec<u8>);

impl IrohAuthorization {
    /// Wrap already-assembled container bytes.
    pub fn new(container: Vec<u8>) -> Self {
        Self(container)
    }

    /// The container, to be sent as it is.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// The container, consumed.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}
