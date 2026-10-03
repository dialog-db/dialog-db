//! Replicas on connected peers, and their branches.
//!
//! ```text
//! contact("origin").connect()               → ContactConnection
//!   └── .repository(did)                    → PeerReplica
//!         ├── .open()                       → ConnectedReplica
//!         └── .branch("main").open()        → ConnectedBranch
//! ```

mod address;
pub use address::*;

mod archive;
pub use archive::*;

mod branch;
pub use branch::*;

mod connection;
pub use connection::*;

mod repository;
pub use repository::*;
