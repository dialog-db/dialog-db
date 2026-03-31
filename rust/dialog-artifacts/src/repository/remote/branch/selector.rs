//! Selector for navigating to a remote branch.

use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::session::RemoteBranch;
use crate::RemoteAddress;
use crate::repository::branch::BranchName;
use crate::repository::cell::{Cell, Retain};
use crate::repository::error::RepositoryError;
use crate::repository::remote::repository::RemoteRepository;
use crate::repository::revision::Revision;

/// A reference to a named branch at a remote repository.
///
/// Call `.open()` or `.load()` to get a command, then `.perform(&env)`.
pub struct RemoteBranchSelector<'a> {
    repository: &'a RemoteRepository,
    cell: Cell<Revision>,
}

impl RemoteBranchSelector<'_> {
    /// The branch name, derived from the cell path.
    pub fn name(&self) -> BranchName {
        let cell_name = self.cell.name();
        cell_name
            .strip_prefix("branch/")
            .and_then(|s| s.strip_suffix("/revision"))
            .unwrap_or(cell_name)
            .into()
    }

    /// Open the remote branch (resolves, no error if missing).
    pub fn open(self) -> OpenRemoteBranch {
        OpenRemoteBranch {
            cell: self.cell,
            address: self.repository.retain_address(),
        }
    }

    /// Load the remote branch (error if not found).
    pub fn load(self) -> LoadRemoteBranch {
        LoadRemoteBranch {
            cell: self.cell,
            address: self.repository.retain_address(),
        }
    }
}

/// Command to open a remote branch.
pub struct OpenRemoteBranch {
    cell: Cell<Revision>,
    address: Retain<RemoteAddress>,
}

impl OpenRemoteBranch {
    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteBranch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        self.cell.resolve(env).await?;
        Ok(RemoteBranch {
            revision: self.cell,
            address: self.address,
        })
    }
}

/// Command to load an existing remote branch.
pub struct LoadRemoteBranch {
    cell: Cell<Revision>,
    address: Retain<RemoteAddress>,
}

impl LoadRemoteBranch {
    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteBranch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>,
    {
        self.cell.resolve(env).await?;
        if self.cell.get().is_none() {
            let name = self
                .cell
                .name()
                .strip_prefix("branch/")
                .and_then(|s| s.strip_suffix("/revision"))
                .unwrap_or(self.cell.name());
            return Err(RepositoryError::BranchNotFound { name: name.into() });
        }

        Ok(RemoteBranch {
            revision: self.cell,
            address: self.address,
        })
    }
}

impl RemoteRepository {
    /// Get a branch selector at this remote repository.
    pub fn branch(&self, name: impl Into<BranchName>) -> RemoteBranchSelector<'_> {
        let name = name.into();
        let cell = Cell::from_capability(self.capability().attenuate(memory_fx::Cell::new(
            format!("branch/{}/revision", name.as_str()),
        )));
        RemoteBranchSelector {
            repository: self,
            cell,
        }
    }
}
