//! What a node's envelope records about it, read from its plaintext.
//!
//! The structure and range regions mirror a node's links: each child's
//! address and structure key, and each child's separator, in the order the
//! node lists them. Sealing reads them off the plaintext node, and a reader
//! with content access reads the same list to learn where each child's
//! envelope lives.

use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{Key, NodeBody, PersistentNode, Value};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Strategy;
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;

use crate::KeyringError;

/// A node's children as its links name them: identities and separators, in
/// child order. Empty for a leaf.
pub(crate) struct Projection {
    /// Each child's content identity, as the node's links record it.
    pub(crate) children: Vec<Blake3Hash>,
    /// Each child's separator.
    pub(crate) separators: Vec<Vec<u8>>,
}

/// Read the children of the node whose bytes are `plain`.
pub(crate) fn project<K, V>(plain: &Buffer) -> Result<Projection, KeyringError>
where
    K: Key,
    V: Value,
    V::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    let node = PersistentNode::<K, V>::try_from(plain.clone())
        .map_err(|error| KeyringError::Node(error.to_string()))?;
    match node.body() {
        NodeBody::Segment(_) => Ok(Projection {
            children: Vec::new(),
            separators: Vec::new(),
        }),
        NodeBody::Index(index) => {
            let links = index
                .links()
                .map_err(|error| KeyringError::Node(error.to_string()))?;
            let (children, separators) = links
                .into_iter()
                .map(|link| (link.node, link.separator))
                .unzip();
            Ok(Projection {
                children,
                separators,
            })
        }
    }
}
