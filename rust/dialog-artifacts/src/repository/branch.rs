use dialog_capability::{Did, Subject};
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_storage::Blake3Hash;

use std::fmt::{Debug, Formatter, Result as FmtResult};

use crate::artifacts::selector::Constrained;
use crate::artifacts::{ArtifactSelector, Datum};
use crate::repository::RemoteSelector;
use crate::{Key, State};

/// Branch state, identifiers, and upstream descriptors.
pub mod state;

mod commit;
mod fetch;
#[cfg(all(test, feature = "integration-tests"))]
mod integration_tests;
mod load;
mod novelty;
mod open;
mod pull;
mod push;
mod reset;
mod select;
mod selector;
mod set_upstream;

pub use commit::Commit;
pub use fetch::Fetch;
pub use load::LoadBranch;
pub use open::OpenBranch;
pub use pull::{Pull, PullLocal};
pub use push::Push;
pub use reset::Reset;
pub use select::Select;
pub use selector::*;
pub use set_upstream::SetUpstream;

use super::archive::Archive;
use super::cell::Cell;
use super::memory::{Memory, Trace};

pub use super::occurence::Occurence;
use super::revision::Revision;
pub use state::{BranchName, UpstreamState};

/// Type alias for the prolly tree index.
pub type Index = Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash>;

/// A branch represents a named line of development within a repository.
///
/// Holds a `Trace` (scoped to `trace/{branch}/local`) plus separate cells
/// for revision and upstream state.
pub struct Branch {
    subject: Did,
    memory: Memory,
    trace: Trace,
    revision: Cell<Option<Revision>>,
    upstream: Cell<Option<UpstreamState>>,
}

impl Debug for Branch {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Branch")
            .field("name", self.trace.name())
            .finish_non_exhaustive()
    }
}

impl Branch {
    /// Returns the branch name.
    pub fn name(&self) -> &BranchName {
        self.trace.name()
    }

    /// Returns the current revision of this branch, or `None` if the branch
    /// has no commits yet (equivalent to an orphan branch in git).
    pub fn revision(&self) -> Option<Revision> {
        self.revision.get().flatten()
    }

    /// Returns the upstream state.
    pub fn upstream(&self) -> Option<UpstreamState> {
        self.upstream.get().flatten()
    }

    /// Returns the subject DID.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Returns the trace capability for this branch.
    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    /// Logical time on this branch, or `None` if the branch has no commits.
    pub fn occurence(&self) -> Option<Occurence> {
        self.revision().map(Into::into)
    }

    /// Pre-attenuated archive capability for this branch's subject.
    pub fn archive(&self) -> Archive {
        Archive::new(Subject::from(self.subject.clone()))
    }

    /// Get a sibling branch reference by name.
    pub fn branch(&self, name: impl Into<BranchName>) -> BranchRef<'_> {
        BranchRef {
            subject: self.subject.clone(),
            memory: &self.memory,
            name: name.into(),
        }
    }

    /// Get a remote reference by name.
    pub fn remote(&self, name: impl Into<super::remote::RemoteName>) -> RemoteSelector {
        let name = name.into();
        let space = self.memory.space(&format!("remote/{}", name.as_str()));
        use super::memory::Site;
        RemoteSelector(Site::from(space))
    }

    /// Create a command to commit instructions to this branch.
    pub fn commit<I>(&self, instructions: I) -> Commit<'_, I> {
        Commit::new(self, instructions)
    }

    /// Create a command to select artifacts from this branch.
    pub fn select(&self, selector: ArtifactSelector<Constrained>) -> Select {
        Select::new(self.subject().clone(), self.revision(), selector)
    }

    /// Create a command to reset the branch to a given revision.
    pub fn reset(&self, revision: Revision) -> Reset<'_> {
        Reset::new(self, revision)
    }

    /// Pull from the configured upstream.
    ///
    /// Reads the branch's upstream and dispatches to local or remote
    /// pull logic automatically.
    pub fn pull(&self) -> Pull<'_> {
        Pull::new(self)
    }

    /// Merge an explicit upstream revision into this branch.
    pub fn merge(&self, upstream_revision: Revision) -> PullLocal<'_> {
        PullLocal::new(self, upstream_revision)
    }

    /// Create a command to fetch the upstream branch's current revision.
    ///
    /// Does NOT modify local state — only reads from upstream.
    pub fn fetch(&self) -> Fetch<'_> {
        Fetch::new(self)
    }

    /// Create a command to push local changes to the upstream branch.
    ///
    /// Reads the upstream configuration from branch state and dispatches
    /// to local or remote push logic.
    pub fn push(&self) -> Push<'_> {
        Push::new(self)
    }

    /// Create a command to set the upstream for this branch.
    ///
    /// Accepts both `UpstreamState` and `RemoteBranch` directly via
    /// `impl Into<UpstreamState>`.
    pub fn set_upstream(&self, upstream: impl Into<UpstreamState>) -> SetUpstream<'_> {
        SetUpstream::new(self, upstream.into())
    }
}

/// A reference to a named branch, obtained from another branch.
pub struct BranchRef<'a> {
    subject: Did,
    memory: &'a super::memory::Memory,
    name: state::BranchName,
}

impl BranchRef<'_> {
    /// Open the branch (create if missing).
    pub fn open(self) -> OpenBranch {
        let trace = self.memory.trace(self.name);
        OpenBranch::new(self.subject, self.memory.clone(), trace)
    }

    /// Load the branch (error if missing).
    pub fn load(self) -> LoadBranch {
        let trace = self.memory.trace(self.name);
        LoadBranch::new(self.subject, self.memory.clone(), trace)
    }
}
