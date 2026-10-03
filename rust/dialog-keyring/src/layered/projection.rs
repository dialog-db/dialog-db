//! What a node's envelope records about it, read from its plaintext.
//!
//! The structure and range regions mirror a node's links: each child's
//! address and structure key, and each child's separator, in the order the
//! node lists them. The structure region also lists the node's attachments,
//! the sealed values it refers to, in the order its [`Attach`] hook names
//! them. Sealing reads all of this off the plaintext node, and a reader with
//! content access reads the same lists to learn where each child's envelope
//! and each value lives.

use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{Key, NodeBody, PersistentNode, Value};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Strategy;
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;

use crate::KeyringError;

/// Names the values a node refers to without holding them, by their
/// plaintext hashes, in a deterministic order: what the tree's value type
/// spills out of its nodes. Sealing seals each named value beside the node,
/// and opening the node learns where each one lives.
pub trait Attach<K, V> {
    /// The values `node` refers to without holding them.
    ///
    /// # Errors
    ///
    /// [`KeyringError::Node`] if the node's entries do not decode.
    fn attachments(node: &PersistentNode<K, V>) -> Result<Vec<Blake3Hash>, KeyringError>;
}

/// The [`Attach`] hook for a tree whose nodes hold every value they name.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoAttachments;

impl<K, V> Attach<K, V> for NoAttachments {
    fn attachments(_: &PersistentNode<K, V>) -> Result<Vec<Blake3Hash>, KeyringError> {
        Ok(Vec::new())
    }
}

/// A node's children as its links name them, and the values it refers to.
/// Children are empty for a leaf.
pub(crate) struct Projection {
    /// Each child's content identity, as the node's links record it.
    pub(crate) children: Vec<Blake3Hash>,
    /// Each child's separator.
    pub(crate) separators: Vec<Vec<u8>>,
    /// Each value the node refers to, by plaintext hash.
    pub(crate) attachments: Vec<Blake3Hash>,
}

/// Read the children and attachments of the node whose bytes are `plain`.
pub(crate) fn project<K, V, A>(plain: &Buffer) -> Result<Projection, KeyringError>
where
    K: Key,
    V: Value,
    A: Attach<K, V>,
    V::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    let node = PersistentNode::<K, V>::try_from(plain.clone())
        .map_err(|error| KeyringError::Node(error.to_string()))?;
    let attachments = A::attachments(&node)?;
    let (children, separators) = match node.body() {
        NodeBody::Segment(_) => (Vec::new(), Vec::new()),
        NodeBody::Index(index) => index
            .links()
            .map_err(|error| KeyringError::Node(error.to_string()))?
            .into_iter()
            .map(|link| (link.node, link.separator))
            .unzip(),
    };
    Ok(Projection {
        children,
        separators,
        attachments,
    })
}
