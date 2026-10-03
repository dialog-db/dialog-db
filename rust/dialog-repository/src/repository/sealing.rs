//! Sealed lines: branches whose trees the archive holds only as layered
//! envelopes.
//!
//! A branch opened with a [`TreeSpace`] seals what each commit persists
//! (`dialog_keyring::layered`): every node becomes an envelope stored under
//! the hash of its own bytes, and every value a node spills becomes a
//! sealed value beside it. The archive, the remotes a push reaches, and
//! everything that copies blocks between them see only those. Reads open
//! them again through the same space.
//!
//! Above the archive nothing changes. A revision still names its tree by
//! the plaintext root, which is what the node cache, the live spine, diffs
//! and every reader key by; a sealed line's revision also carries where
//! that root's envelope lives ([`SealedTree`]), and the space learns where
//! each node below it lives as each parent opens. So a party reaches
//! exactly what descends from the heads it reads.
//!
//! # What stays in the clear
//!
//! - The head. Its branch, issuer, edition and causal context, as on any
//!   line; its plaintext root, which lets whoever reads the head confirm
//!   an exact guess of the root node and nothing more; and its sealed
//!   root, whose structure key lets whoever reads the head walk the tree's
//!   shape (how many nodes, how they link, how large each envelope is)
//!   and copy it, which is what a remote has to do.
//! - Assets marked [`plaintext`](dialog_artifacts::Asset::plaintext), and
//!   only those. Every other asset is sealed in pieces
//!   ([`layered::asset`](dialog_keyring::layered::asset)) and
//!   stored as its sealed copy, under the hash of the copy's bytes; the
//!   fact recording where that copy lives sits in the sealed tree. What a
//!   copy's length says of the asset's length is the one thing it gives
//!   away.
//! - Sync records, which stay on the replica that keeps them.

use std::collections::BTreeSet;
use std::fmt::Display;

use dialog_artifacts::{Datum, Key, ShipmentRef, State, shipment_ref};
use dialog_common::{Blake3Hash, Buffer};
use dialog_effects::archive::ArchiveError;
use dialog_keyring::KeyringError;
use dialog_keyring::layered::{Access, Attach, LayeredRoot, Sealing, Space, StructureKey, Writer};
use dialog_search_tree::{Delta, NodeBody, NoveltyOp, PersistentNode, into_owned};

use crate::{Revision, SealedTree, TreeReference};

/// One party's keys for the sealed trees of a space, and where each node
/// and spilled value it has reached lives. See the [module](self) docs.
///
/// Clones share what has been learned, so every branch of a space opened
/// with clones of one space reaches nodes the others have read or written.
pub type TreeSpace = Space<Key, State<Datum>, SpilledValues>;

/// A space that reads with `access` and seals new commits under
/// `writer`'s generations.
pub fn writer_space(writer: Writer, access: Access) -> TreeSpace {
    Space::writer(writer, access)
}

/// A space that reads with `access` and cannot commit.
pub fn reader_space(access: Access) -> TreeSpace {
    Space::reader(access)
}

/// How a repository tree's nodes name the values they spill: what sealing
/// seals beside each node.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpilledValues;

impl Attach<Key, State<Datum>> for SpilledValues {
    fn attachments(
        node: &PersistentNode<Key, State<Datum>>,
    ) -> Result<Vec<Blake3Hash>, KeyringError> {
        spilled_values(node)
    }
}

/// The spilled values a node's entries refer to, by plaintext hash, sorted
/// and without repeats: the values sealing seals beside the node. The same
/// entries a push ships spilled values for, stored in a segment or buffered
/// as asserts in an index.
///
/// # Errors
///
/// Returns [`KeyringError::Node`] if an entry does not decode.
pub fn spilled_values(
    node: &PersistentNode<Key, State<Datum>>,
) -> Result<Vec<Blake3Hash>, KeyringError> {
    let failed = |error: &dyn Display| KeyringError::Node(error.to_string());
    let mut entries: Vec<(Key, State<Datum>)> = Vec::new();
    match node.body() {
        NodeBody::Segment(segment) => {
            segment
                .for_each_entry::<Key, _>(|key, value| {
                    entries.push((Key::from(key.to_vec()), into_owned(value)?));
                    Ok(())
                })
                .map_err(|error| failed(&error))?;
        }
        NodeBody::Index(index) => {
            for entry in index.all_novelty::<Key>().map_err(|error| failed(&error))? {
                if let NoveltyOp::Assert(value) = entry.op {
                    entries.push((Key::from(entry.key), value));
                }
            }
        }
    }
    let mut references = BTreeSet::new();
    for (key, value) in entries {
        if let Some(ShipmentRef::SpilledValue(reference)) =
            shipment_ref(&key, &value, false).map_err(|error| failed(&error))?
        {
            references.insert(reference);
        }
    }
    Ok(references.into_iter().map(Blake3Hash::from).collect())
}

/// Tell `space` where the tree `revision` names starts, when the line is
/// sealed and the revision says. Every read of a sealed line goes through
/// here before it asks for the root.
pub(crate) fn admit(space: Option<&TreeSpace>, revision: &Revision) {
    if let (Some(space), Some(sealed)) = (space, revision.sealed.as_ref()) {
        admit_root(space, &revision.tree, sealed);
    }
}

/// Tell `space` that the tree rooted at `tree` starts at `sealed`.
pub(crate) fn admit_root(space: &TreeSpace, tree: &TreeReference, sealed: &SealedTree) {
    space.admit(
        Blake3Hash::from(*tree.hash()),
        &LayeredRoot {
            address: Blake3Hash::from(sealed.address),
            structure: StructureKey::from_bytes(sealed.structure),
        },
    );
}

/// Open the node `identity` from the envelope bytes found where `space`
/// locates it.
///
/// A plain function over the concrete tree types, so the archive
/// validation bound `Space::open` carries is proved here, outside the
/// async reads that call it: proved inside one, its higher-ranked lifetime
/// leaks into the future's `Send` check.
pub(crate) fn open_node(
    space: &TreeSpace,
    identity: &Blake3Hash,
    bytes: &[u8],
) -> Result<Buffer, KeyringError> {
    space.open(identity, bytes)
}

/// Seal what a persist staged for the tree rooted at `root`. A plain
/// function for the same reason as [`open_node`].
pub(crate) fn seal_line(
    space: &TreeSpace,
    blocks: &Delta<Blake3Hash, Buffer>,
    values: &Delta<Blake3Hash, Buffer>,
    root: &Blake3Hash,
) -> Result<Sealing, KeyringError> {
    space.seal(blocks, Some(values), root)
}

/// What a revision records about where a sealed tree starts.
pub(crate) fn sealed_tree(root: &LayeredRoot) -> SealedTree {
    SealedTree {
        address: *root.address.as_bytes(),
        structure: *root.structure.as_bytes(),
    }
}

/// Why a read of a sealed line failed: the archive could not serve the
/// block, or the block did not open for the line's keys.
#[derive(Debug, thiserror::Error)]
pub enum SealedReadError {
    /// The archive failed or refused the read.
    #[error(transparent)]
    Archive(#[from] ArchiveError),
    /// The block did not open: a generation this party does not hold, a
    /// block that does not match where it was found, or tampering.
    #[error(transparent)]
    Keyring(#[from] KeyringError),
}

impl From<SealedReadError> for ArchiveError {
    fn from(error: SealedReadError) -> Self {
        match error {
            SealedReadError::Archive(error) => error,
            SealedReadError::Keyring(error) => {
                ArchiveError::Storage(format!("sealed block did not open: {error}"))
            }
        }
    }
}

pub(crate) mod asset;

#[cfg(test)]
mod tests;

#[cfg(all(
    test,
    any(feature = "integration-tests", feature = "web-integration-tests")
))]
mod remote_tests;
