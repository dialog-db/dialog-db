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
        /// Whether this entry is a sync base only, never the branch's
        /// default. Maintained by [`Upstreams`]; see
        /// [`Upstream::is_tracking_only`].
        #[serde(default, skip_serializing_if = "not")]
        tracking_only: bool,
    },
    /// A remote branch upstream.
    Remote {
        /// Remote name (e.g., "origin").
        remote: String,
        /// Branch name on the remote.
        branch: String,
        /// Tree root at last sync point.
        tree: TreeReference,
        /// Whether this entry is a sync base only, never the branch's
        /// default. Maintained by [`Upstreams`]; see
        /// [`Upstream::is_tracking_only`].
        #[serde(default, skip_serializing_if = "not")]
        tracking_only: bool,
    },
}

/// Whether a flag is unset, for `skip_serializing_if`.
///
/// A default-eligible entry therefore serializes exactly as it did
/// before the flag existed, so a binary that predates it reads current
/// cells unchanged.
fn not(flag: &bool) -> bool {
    !flag
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

    /// Whether this entry is a sync base and nothing more — recorded by a
    /// [`pull`](super::Branch::pull) or [`push`](super::Branch::push)
    /// against an explicitly named target, and never eligible to become
    /// the branch's [default](Upstreams::default_upstream).
    ///
    /// The distinction exists because those two things are not the same
    /// question. "Where have I synced with this target up to" is
    /// bookkeeping that makes the next sync incremental; "what does a
    /// bare pull or push target" is a decision the operator made. Merging
    /// one branch into another is not a statement that the first should
    /// start pushing to the second — but the entry still has to be
    /// recorded, because the tracked set is also what push attribution
    /// reasons over, and it may only ever grow
    /// (`notes/version-control.md`, invariant 3).
    ///
    /// Absent from cells written before the flag existed, where position
    /// alone encoded the default; those entries decode as default-eligible,
    /// which is what the old rule meant.
    pub fn is_tracking_only(&self) -> bool {
        match self {
            Self::Local { tracking_only, .. } | Self::Remote { tracking_only, .. } => {
                *tracking_only
            }
        }
    }

    /// Returns a new upstream with the tree updated to the given value.
    pub fn with_tree(self, tree: TreeReference) -> Self {
        match self {
            Self::Local {
                branch,
                tracking_only,
                ..
            } => Self::Local {
                branch,
                tree,
                tracking_only,
            },
            Self::Remote {
                remote,
                branch,
                tracking_only,
                ..
            } => Self::Remote {
                remote,
                branch,
                tree,
                tracking_only,
            },
        }
    }

    /// Returns a new upstream with [`is_tracking_only`](Self::is_tracking_only)
    /// set as given.
    fn tracking(self, tracking_only: bool) -> Self {
        match self {
            Self::Local { branch, tree, .. } => Self::Local {
                branch,
                tree,
                tracking_only,
            },
            Self::Remote {
                remote,
                branch,
                tree,
                ..
            } => Self::Remote {
                remote,
                branch,
                tree,
                tracking_only,
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
/// them. Every entry carries its own last-sync tree, so divergence bases
/// are tracked per target.
///
/// The set is **append-only**: push attribution reasons over it to decide
/// whose content it holds by reference, on the assumption that an entry
/// once recorded never disappears (`notes/version-control.md`, invariant
/// 3). Nothing here removes one.
///
/// So the *default* — what a bare [`pull`](super::Branch::pull) /
/// [`push`](super::Branch::push) / [`fetch`](super::Branch::fetch)
/// targets — cannot be "whichever entry happens to be first": a sync
/// against an explicitly named target has to record its base, and would
/// otherwise decide the default by being the only entry there. It is
/// [`Upstream::is_tracking_only`] that keeps the two apart, and the
/// default is the first entry without that flag.
///
/// Serialized as a plain sequence of [`Upstream`]s. Two older shapes
/// decode: a single bare `Upstream` map (before multi-upstream support),
/// and a sequence whose entries carry no `tracking_only` — both read as
/// default-eligible, which is what "the first entry is the default"
/// meant.
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
    /// The default upstream — the target of a bare pull/push/fetch — if
    /// one has been set.
    ///
    /// The first entry that is not [tracking-only](
    /// Upstream::is_tracking_only). A branch may therefore hold tracking
    /// entries and still have no default, which is a branch somebody
    /// pulled from something without ever saying where it belongs.
    pub fn default_upstream(&self) -> Option<&Upstream> {
        self.0.iter().find(|entry| !entry.is_tracking_only())
    }

    /// The tracking entry for the given target, if present.
    pub fn find(&self, target: &Upstream) -> Option<&Upstream> {
        self.0.iter().find(|entry| entry.same_target(target))
    }

    /// Iterate over every tracking entry, default first.
    ///
    /// Tracking-only entries are included: what a branch has synced with
    /// is exactly what push attribution must reason over, whether or not
    /// any of it is the default.
    pub fn iter(&self) -> impl Iterator<Item = &Upstream> {
        self.0.iter()
    }

    /// Whether the branch tracks nothing at all.
    ///
    /// Not the same question as having no [default](
    /// Self::default_upstream): a branch that was pulled from once tracks
    /// that target without a bare pull having anywhere to go.
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

    /// Record a sync base for `upstream` without touching the default.
    ///
    /// A target already tracked keeps its position and its
    /// default-eligibility, and only its recorded base advances — a pull
    /// from the branch's own upstream must not demote it. A target not yet
    /// tracked is appended as [tracking-only](
    /// Upstream::is_tracking_only), so recording it cannot decide what a
    /// bare pull or push targets.
    ///
    /// This is the path a [`pull`](super::Branch::pull) or
    /// [`push`](super::Branch::push) against an *explicitly named* target
    /// takes. Use [`upsert_default`](Self::upsert_default) for what the
    /// operator asked to track.
    pub fn upsert(&mut self, upstream: Upstream) {
        match self.0.iter_mut().find(|entry| entry.same_target(&upstream)) {
            Some(entry) => *entry = upstream.tracking(entry.is_tracking_only()),
            None => self.0.push(upstream.tracking(true)),
        }
    }

    /// Make `upstream` the default. A target already tracked keeps its
    /// recorded sync base and just moves to the front; a new target is
    /// inserted at the front as given. Either way it stops being
    /// [tracking-only](Upstream::is_tracking_only) — this is the operator
    /// saying where the branch belongs.
    pub fn upsert_default(&mut self, upstream: Upstream) {
        let entry = match self.0.iter().position(|entry| entry.same_target(&upstream)) {
            Some(index) => self.0.remove(index),
            None => upstream,
        };
        self.0.insert(0, entry.tracking(false));
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
    /// A local branch upstream. Both variants are boxed: the handles
    /// are large and this enum is a short-lived constructor argument.
    Local(Box<Branch>),
    /// A remote branch upstream.
    Remote(Box<RemoteBranch>),
}

impl From<&Branch> for UpstreamBranch {
    fn from(branch: &Branch) -> Self {
        UpstreamBranch::Local(Box::new(branch.clone()))
    }
}

impl From<Branch> for UpstreamBranch {
    fn from(branch: Branch) -> Self {
        UpstreamBranch::Local(Box::new(branch))
    }
}

impl From<&RemoteBranch> for UpstreamBranch {
    fn from(branch: &RemoteBranch) -> Self {
        UpstreamBranch::Remote(Box::new(branch.clone()))
    }
}

impl From<RemoteBranch> for UpstreamBranch {
    fn from(branch: RemoteBranch) -> Self {
        UpstreamBranch::Remote(Box::new(branch))
    }
}

impl From<UpstreamBranch> for Upstream {
    /// The entry a caller names a target with, before [`Upstreams`] decides
    /// whether recording it makes it the default: default-eligible as
    /// built, which is what [`upsert_default`](Upstreams::upsert_default)
    /// keeps and [`upsert`](Upstreams::upsert) overrides.
    fn from(source: UpstreamBranch) -> Self {
        match source {
            UpstreamBranch::Local(branch) => Upstream::Local {
                branch: branch.name().to_string(),
                tree: TreeReference::default(),
                tracking_only: false,
            },
            UpstreamBranch::Remote(branch) => Upstream::Remote {
                remote: branch.repository().site().name().to_string(),
                branch: branch.name().to_string(),
                tree: TreeReference::default(),
                tracking_only: false,
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

    /// A default-eligible remote entry, as `set_upstream` builds one.
    fn remote(name: &str, seed: u8) -> Upstream {
        Upstream::Remote {
            remote: name.into(),
            branch: "main".into(),
            tree: TreeReference::from([seed; 32]),
            tracking_only: false,
        }
    }

    /// A default-eligible local entry.
    fn local(name: &str) -> Upstream {
        Upstream::Local {
            branch: name.into(),
            tree: TreeReference::default(),
            tracking_only: false,
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
        many.upsert_default(single);
        many.upsert(remote("backup", 1));
        let (_, bytes) = CborEncoder.encode(&many).await?;
        let decoded: Upstreams = CborEncoder.decode(&bytes).await?;
        assert_eq!(decoded, many);

        Ok(())
    }

    /// A sequence whose entries carry no `tracking_only` — every cell
    /// written before the flag existed — reads under the rule those cells
    /// were written under: the first entry is the default.
    #[dialog_common::test]
    async fn it_reads_a_flagless_entry_as_default_eligible() -> Result<()> {
        let legacy = vec![remote("origin", 1), remote("backup", 2)];
        let (_, bytes) = CborEncoder.encode(&legacy).await?;
        let decoded: Upstreams = CborEncoder.decode(&bytes).await?;

        assert_eq!(decoded.default_upstream(), Some(&remote("origin", 1)));
        assert!(decoded.iter().all(|entry| !entry.is_tracking_only()));
        Ok(())
    }

    /// A default-eligible entry must serialize exactly as it did before the
    /// flag existed, so a binary that predates it still reads current
    /// cells. Only a tracking-only entry carries the extra key.
    #[dialog_common::test]
    async fn it_writes_no_flag_for_a_default_eligible_entry() -> Result<()> {
        let mut upstreams = Upstreams::default();
        upstreams.upsert_default(remote("origin", 1));
        let (_, with_default) = CborEncoder.encode(&upstreams).await?;
        let (_, flagless) = CborEncoder.encode(&vec![remote("origin", 1)]).await?;
        assert_eq!(with_default, flagless, "the default entry gains no bytes");

        upstreams.upsert(remote("backup", 2));
        let (_, with_tracking) = CborEncoder.encode(&upstreams).await?;
        let (_, both_flagless) = CborEncoder
            .encode(&vec![remote("origin", 1), remote("backup", 2)])
            .await?;
        assert_ne!(
            with_tracking, both_flagless,
            "a tracking-only entry is distinguishable on the wire"
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_upserts_by_target_and_promotes_defaults() {
        let mut upstreams = Upstreams::default();
        upstreams.upsert_default(remote("origin", 1));
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
        let main = local("main");
        assert!(!main.same_target(&remote("origin", 0)));
        upstreams.upsert(main.clone());
        assert_eq!(upstreams.iter().count(), 3);
        assert_eq!(upstreams.find(&main).map(Upstream::branch), Some("main"));
        assert_eq!(upstreams.remote_name(), Some("backup"));
    }

    /// Recording a sync base must never decide what a bare pull or push
    /// targets — not even when it is the only entry there. The entry is
    /// still recorded, because push attribution reads the tracked set and
    /// the set may only grow.
    #[dialog_common::test]
    fn it_records_a_sync_base_without_making_it_the_default() {
        let mut upstreams = Upstreams::default();
        upstreams.upsert(local("draft"));

        assert!(!upstreams.is_empty(), "the branch tracks draft");
        assert_eq!(
            upstreams.default_upstream(),
            None,
            "but a bare push still has nowhere to go"
        );
        assert!(upstreams.find(&local("draft")).is_some());
        assert!(
            upstreams
                .iter()
                .all(|entry| entry.is_tracking_only() && entry.branch() == "draft")
        );
    }

    /// A pull from the branch's own upstream advances that entry's base.
    /// It must not demote what the operator set.
    #[dialog_common::test]
    fn it_keeps_the_default_when_a_sync_advances_its_base() {
        let mut upstreams = Upstreams::default();
        upstreams.upsert_default(remote("origin", 1));
        upstreams.upsert(remote("origin", 7));

        assert_eq!(upstreams.iter().count(), 1);
        assert_eq!(upstreams.default_upstream(), Some(&remote("origin", 7)));
    }

    /// Setting an upstream on a branch that had only tracking entries
    /// gives it a default without disturbing the bases it had recorded.
    #[dialog_common::test]
    fn it_promotes_a_tracked_target_the_operator_later_names() {
        let mut upstreams = Upstreams::default();
        upstreams.upsert(remote("origin", 4));
        assert_eq!(upstreams.default_upstream(), None);

        upstreams.upsert_default(remote("origin", 0));
        assert_eq!(
            upstreams.default_upstream(),
            Some(&remote("origin", 4)),
            "promotion keeps the recorded base"
        );
        assert_eq!(upstreams.iter().count(), 1);
    }
}
