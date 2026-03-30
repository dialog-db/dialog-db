//! Remote site / repository / branch cursor hierarchy.
//!
//! Provides a navigation pattern for addressing remote repositories:
//!
//! ```text
//! repo.site("origin").load().perform(&env)
//!   └── .repository(subject_did)  → RemoteRepository
//!         └── .branch("main")     → RemoteBranch
//! ```
//!
//! [`RemoteBranch`] provides remote operations: [`resolve`](RemoteBranch::resolve),
//! [`publish`](RemoteBranch::publish), and [`upload`](RemoteBranch::upload).
//!
//! `From<RemoteBranch> for UpstreamState` enables ergonomic `set_upstream`.

/// Serializable remote address configuration.
pub mod address;
/// Remote branch cursor with resolve/publish/upload operations.
pub mod branch;
/// Remote repository cursor (site + subject).
pub mod repository;
/// Remote site configuration (add/load).
pub mod site;
/// Persisted remote configuration state.
pub mod state;

mod selector;

pub use address::*;
pub use branch::*;
pub use selector::*;
pub use site::*;
pub use state::*;
