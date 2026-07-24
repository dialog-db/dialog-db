use crate::{Branch, RemoteBranch, TreeReference};
use serde::{Deserialize, Serialize};

/// The persisted form of a branch's upstream tracking state.
///
/// Stored in the branch's `upstream` cell. The `tree` field captures
/// the upstream's tree root at the time of last sync, used as the
/// divergence base for three-way merge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Upstream {
    /// A local branch upstream.
    Local {
        /// Branch name.
        branch: String,
        /// Tree root at last sync point.
        tree: TreeReference,
    },
    /// A remote branch upstream.
    Remote {
        /// Remote name (e.g., "origin").
        remote: String,
        /// Branch name on the remote.
        branch: String,
        /// Tree root at last sync point.
        tree: TreeReference,
    },
}

impl Upstream {
    /// Returns the branch name of this upstream.
    pub fn branch(&self) -> &str {
        match self {
            Self::Local { branch, .. } => branch,
            Self::Remote { branch, .. } => branch,
        }
    }

    /// Returns the tree root at the last sync point.
    pub fn tree(&self) -> &TreeReference {
        match self {
            Self::Local { tree, .. } => tree,
            Self::Remote { tree, .. } => tree,
        }
    }

    /// Returns a new upstream with the tree updated to the given value.
    pub fn with_tree(self, tree: TreeReference) -> Self {
        match self {
            Self::Local { branch, .. } => Self::Local { branch, tree },
            Self::Remote { remote, branch, .. } => Self::Remote {
                remote,
                branch,
                tree,
            },
        }
    }

    /// Whether two upstream entries track the same target — the same local
    /// branch, or the same branch on the same remote — regardless of their
    /// recorded sync bases.
    pub fn same_target(&self, other: &Upstream) -> bool {
        match (self, other) {
            (Self::Local { branch: a, .. }, Self::Local { branch: b, .. }) => a == b,
            (
                Self::Remote {
                    remote: a_remote,
                    branch: a_branch,
                    ..
                },
                Self::Remote {
                    remote: b_remote,
                    branch: b_branch,
                    ..
                },
            ) => a_remote == b_remote && a_branch == b_branch,
            _ => false,
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
    /// The default upstream — the target of a bare pull/push/fetch — if any
    /// upstream is configured.
    pub fn default_upstream(&self) -> Option<&Upstream> {
        self.0.first()
    }

    /// The tracking entry for the given target, if present.
    pub fn find(&self, target: &Upstream) -> Option<&Upstream> {
        self.0.iter().find(|entry| entry.same_target(target))
    }

    /// Iterate over every tracking entry, default first.
    pub fn iter(&self) -> impl Iterator<Item = &Upstream> {
        self.0.iter()
    }

    /// Whether no upstream is configured.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The name of the first remote-kind entry, if any: the remote that
    /// reads fall back to for blocks that haven't been replicated locally.
    pub fn remote_name(&self) -> Option<&str> {
        self.0.iter().find_map(|entry| match entry {
            Upstream::Remote { remote, .. } => Some(remote.as_str()),
            Upstream::Local { .. } => None,
        })
    }

    /// Insert `upstream`, or replace the entry tracking the same target in
    /// place (keeping its position — a new entry appends at the end,
    /// leaving the default unchanged).
    pub fn upsert(&mut self, upstream: Upstream) {
        match self.0.iter_mut().find(|entry| entry.same_target(&upstream)) {
            Some(entry) => *entry = upstream,
            None => self.0.push(upstream),
        }
    }

    /// Make `upstream` the default. A target already tracked keeps its
    /// recorded sync base and just moves to the front; a new target is
    /// inserted at the front as given.
    pub fn upsert_default(&mut self, upstream: Upstream) {
        let entry = match self.0.iter().position(|entry| entry.same_target(&upstream)) {
            Some(index) => self.0.remove(index),
            None => upstream,
        };
        self.0.insert(0, entry);
    }
}

/// The input shape for [`Branch::set_upstream`](super::Branch::set_upstream).
///
/// Wraps a loaded local or remote branch handle. Convertible into
/// [`Upstream`] (the persisted form) by extracting the names; the
/// stored tree starts at [`TreeReference::default`] (empty) since the
/// divergence point is "anything in the upstream from now on."
///
/// Construct via the `From<&Branch>` and `From<&RemoteBranch>` impls;
/// `branch.set_upstream(&local_or_remote)` invokes them implicitly.
pub enum UpstreamBranch {
    /// A local branch upstream.
    Local(Branch),
    /// A remote branch upstream.
    Remote(RemoteBranch),
}

impl From<&Branch> for UpstreamBranch {
    fn from(branch: &Branch) -> Self {
        UpstreamBranch::Local(branch.clone())
    }
}

impl From<Branch> for UpstreamBranch {
    fn from(branch: Branch) -> Self {
        UpstreamBranch::Local(branch)
    }
}

impl From<&RemoteBranch> for UpstreamBranch {
    fn from(branch: &RemoteBranch) -> Self {
        UpstreamBranch::Remote(branch.clone())
    }
}

impl From<RemoteBranch> for UpstreamBranch {
    fn from(branch: RemoteBranch) -> Self {
        UpstreamBranch::Remote(branch)
    }
}

impl From<UpstreamBranch> for Upstream {
    fn from(source: UpstreamBranch) -> Self {
        match source {
            UpstreamBranch::Local(branch) => Upstream::Local {
                branch: branch.name().to_string(),
                tree: TreeReference::default(),
            },
            UpstreamBranch::Remote(branch) => Upstream::Remote {
                remote: branch.repository().site().name().to_string(),
                branch: branch.name().to_string(),
                tree: TreeReference::default(),
            },
        }
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
            tree: TreeReference::from([seed; 32]),
        }
    }

    /// Cells written before multi-upstream support hold a single bare
    /// [`Upstream`]; they must decode as a one-entry [`Upstreams`].
    #[dialog_common::test]
    async fn it_decodes_legacy_single_upstream_cells() -> Result<()> {
        let single = remote("origin", 0);
        let (_, bytes) = CborEncoder.encode(&single).await?;
        let decoded: Upstreams = CborEncoder.decode(&bytes).await?;
        assert_eq!(decoded.default_upstream(), Some(&single));
        assert_eq!(decoded.iter().count(), 1);

        // ... and the modern sequence shape round-trips.
        let mut many = Upstreams::default();
        many.upsert(single);
        many.upsert(remote("backup", 1));
        let (_, bytes) = CborEncoder.encode(&many).await?;
        let decoded: Upstreams = CborEncoder.decode(&bytes).await?;
        assert_eq!(decoded, many);

        Ok(())
    }

    #[dialog_common::test]
    fn it_upserts_by_target_and_promotes_defaults() {
        let mut upstreams = Upstreams::default();
        upstreams.upsert(remote("origin", 1));
        upstreams.upsert(remote("backup", 2));

        // Same target replaces in place, keeping its position.
        upstreams.upsert(remote("origin", 3));
        assert_eq!(upstreams.iter().count(), 2);
        assert_eq!(upstreams.default_upstream(), Some(&remote("origin", 3)));

        // Promoting an existing target keeps its recorded sync base.
        upstreams.upsert_default(remote("backup", 9));
        assert_eq!(upstreams.default_upstream(), Some(&remote("backup", 2)));
        assert_eq!(upstreams.iter().count(), 2);

        // A local entry never matches a remote one.
        let local = Upstream::Local {
            branch: "main".into(),
            tree: TreeReference::default(),
        };
        assert!(!local.same_target(&remote("origin", 0)));
        upstreams.upsert(local.clone());
        assert_eq!(upstreams.iter().count(), 3);
        assert_eq!(upstreams.find(&local), Some(&local));
        assert_eq!(upstreams.remote_name(), Some("backup"));
    }
}
