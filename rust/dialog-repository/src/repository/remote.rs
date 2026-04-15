//! Remote repository navigation and operations.
//!
//! ```text
//! repo.remote("origin").load().perform(&env)  → RemoteRepository
//!   └── .branch("main")                    → RemoteBranchReference
//! ```

/// Serializable remote address configuration.
pub mod address;
/// Remote archive operations (upload blocks).
pub mod archive;
/// Remote branch cursor with resolve/publish/upload operations.
pub mod branch;
/// Command to create a new remote.
mod create;
/// Command to load an existing remote.
mod load;
/// Remote name newtype.
pub mod name;
/// Selectors for navigating remote sites, repositories, and branches.
mod reference;
/// Remote repository cursor.
pub mod repository;

pub use address::*;
pub use create::*;
pub use load::*;
pub use name::*;
pub use reference::*;
pub use repository::RemoteRepository;
