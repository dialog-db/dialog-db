//! Remote repository navigation and operations.
//!
//! ```text
//! repo.remote("origin").load().perform(&env)  → RemoteRepository
//!   └── .branch("main")                       → RemoteBranchReference
//! ```

mod address;
pub use address::*;

mod archive;
pub use archive::*;

mod branch;
pub use branch::*;

mod create;
pub use create::*;

mod load;
pub use load::*;

mod reference;
pub use reference::*;

mod repository;
pub use repository::*;
