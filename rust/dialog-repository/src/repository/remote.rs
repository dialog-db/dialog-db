//! Remote repository navigation and operations.
//!
//! ```text
//! repo.remote("origin").load().perform(&env)  → RemoteRepository
//!   └── .branch("main")                       → RemoteBranchReference
//! ```

mod address;
mod archive;
mod branch;
mod create;
mod load;
mod reference;
mod repository;

pub use address::*;
pub use archive::*;
pub use branch::*;
pub use create::*;
pub use load::*;
pub use reference::*;
pub use repository::*;
