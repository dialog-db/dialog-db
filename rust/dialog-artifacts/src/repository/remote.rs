//! Remote repository navigation and operations.
//!
//! ```text
//! repo.remote("origin").load().perform(&env)  → RemoteRepository
//!   └── .branch("main")                    → RemoteBranchSelector
//! ```

/// Serializable remote address configuration.
pub mod address;
/// Remote branch cursor with resolve/publish/upload operations.
pub mod branch;
/// Command to create a new remote.
mod create;
/// Command to load an existing remote.
mod load;
/// Remote repository cursor.
pub mod repository;
/// Selectors for navigating remote sites, repositories, and branches.
mod selector;
/// Persisted remote configuration state.
pub mod state;

pub use address::*;
pub use branch::*;
pub use create::*;
pub use load::*;
pub use repository::RemoteRepository;
pub use selector::*;
pub use state::*;
