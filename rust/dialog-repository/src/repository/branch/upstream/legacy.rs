//! The upstream cell as releases before peers wrote it.
//!
//! A branch's upstreams used to live in its `upstream` cell, naming each
//! remote by its local name. Only the [upgrade](crate::Upgrade) reads
//! this now, to carry them over into facts.

use crate::TreeReference;
use serde::{Deserialize, Serialize};

/// A branch's upstream tracking state, as it was persisted.
///
/// Stored in the branch's `upstream` cell. The `tree` field captures
/// the upstream's tree root at the time of last sync, used as the
/// divergence base for three-way merge; an upstream never synced was
/// written with the all-zero reference, which reads as `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Upstream {
    /// A local branch upstream.
    Local {
        /// Branch name.
        branch: String,
        /// Tree root at last sync point, if any sync has happened.
        #[serde(deserialize_with = "super::sync_base::deserialize")]
        tree: Option<TreeReference>,
    },
    /// A remote branch upstream.
    Remote {
        /// Remote name (e.g., "origin").
        remote: String,
        /// Branch name on the remote.
        branch: String,
        /// Tree root at last sync point, if any sync has happened.
        #[serde(deserialize_with = "super::sync_base::deserialize")]
        tree: Option<TreeReference>,
    },
}

impl Upstream {
    /// Returns the tree root at the last sync point, if any sync has
    /// happened.
    pub fn tree(&self) -> Option<&TreeReference> {
        match self {
            Self::Local { tree, .. } => tree.as_ref(),
            Self::Remote { tree, .. } => tree.as_ref(),
        }
    }
}

/// The persisted set of a branch's upstream tracking entries.
///
/// A branch can track several upstreams — e.g. a local integration branch
/// plus branches on two different remotes — and pull from or push to any of
/// them. Entries are ordered: the first is the *default* upstream, the one
/// a bare [`pull`](super::Branch::pull) / [`push`](super::Branch::push) /
/// [`fetch`](super::Branch::fetch) targets. Every entry carries its own
/// last-sync tree, so divergence bases are tracked per target.
///
/// Serialized as a plain sequence of [`Upstream`]s; cells written before
/// multi-upstream support hold a single bare `Upstream` map, which
/// deserialization accepts as a one-entry set.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "StoredUpstreams")]
pub struct Upstreams(Vec<Upstream>);

/// Accepts both persisted shapes of the upstream cell: the current
/// sequence-of-entries and the historical single bare entry.
#[derive(Deserialize)]
#[serde(untagged)]
enum StoredUpstreams {
    Many(Vec<Upstream>),
    One(Upstream),
}

impl From<StoredUpstreams> for Upstreams {
    fn from(stored: StoredUpstreams) -> Self {
        match stored {
            StoredUpstreams::Many(entries) => Self(entries),
            StoredUpstreams::One(entry) => Self(vec![entry]),
        }
    }
}

impl Upstreams {
    /// Iterate over every tracking entry, default first.
    pub fn iter(&self) -> impl Iterator<Item = &Upstream> {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use anyhow::Result;
    use dialog_storage::{CborEncoder, Encoder as _};

    fn remote(name: &str, seed: u8) -> Upstream {
        Upstream::Remote {
            remote: name.into(),
            branch: "main".into(),
            tree: Some(TreeReference::from([seed; 32])),
        }
    }

    /// Releases before the sentinel-free empty tree wrote "never synced"
    /// as the all-zero tree reference; it reads back as no sync base, so
    /// nothing is carried over as a sync that never happened.
    #[dialog_common::test]
    async fn it_reads_the_zero_reference_as_never_synced() -> Result<()> {
        #[derive(Debug, Serialize)]
        enum Stored {
            Remote {
                remote: String,
                branch: String,
                tree: TreeReference,
            },
        }
        let legacy = Stored::Remote {
            remote: "origin".into(),
            branch: "main".into(),
            tree: TreeReference::from([0u8; 32]),
        };
        let (_, bytes) = CborEncoder.encode(&legacy).await?;
        let decoded: Upstream = CborEncoder.decode(&bytes).await?;
        assert_eq!(decoded.tree(), None, "the zero reference reads as None");

        let synced = remote("origin", 7);
        let (_, bytes) = CborEncoder.encode(&synced).await?;
        assert_eq!(CborEncoder.decode::<Upstream>(&bytes).await?, synced);
        Ok(())
    }

    /// Cells written before multi-upstream support hold a single bare
    /// [`Upstream`]; they must decode as a one-entry [`Upstreams`].
    #[dialog_common::test]
    async fn it_decodes_legacy_single_upstream_cells() -> Result<()> {
        // Not seed 0: the all-zero reference reads as no sync base.
        let single = remote("origin", 17);
        let (_, bytes) = CborEncoder.encode(&single).await?;
        let decoded: Upstreams = CborEncoder.decode(&bytes).await?;
        assert_eq!(decoded.iter().collect::<Vec<_>>(), vec![&single]);

        // ... and the sequence shape decodes as its entries, in order.
        let many = vec![single.clone(), remote("backup", 1)];
        let (_, bytes) = CborEncoder.encode(&many).await?;
        let decoded: Upstreams = CborEncoder.decode(&bytes).await?;
        assert_eq!(decoded.iter().cloned().collect::<Vec<_>>(), many);

        Ok(())
    }
}
