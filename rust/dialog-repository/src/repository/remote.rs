//! Repositories held at peers, and their branches.
//!
//! ```text
//! repo.peer("origin").connect()             → PeerConnection
//!   └── .repository(did)                    → PeerRepository
//!         └── .branch("main").open()        → RemoteBranch
//! ```

mod address;
pub use address::*;

mod archive;
pub use archive::*;

mod branch;
pub use branch::*;

mod repository;
pub use repository::*;
