//! Remote site / repository / branch cursor hierarchy.
//!
//! Provides a navigation pattern for addressing remote repositories:
//!
//! ```text
//! repo.load_remote("origin").perform(&env)
//!   └── .repository(subject_did)  → RemoteRepository
//!         └── .branch("main")     → RemoteBranch
//! ```
//!
//! [`RemoteBranch`] provides remote operations: [`resolve`](RemoteBranch::resolve),
//! [`publish`](RemoteBranch::publish), and [`upload`](RemoteBranch::upload).
//!
//! `From<RemoteBranch> for UpstreamState` enables ergonomic `set_upstream`.

/// Remote branch cursor with resolve/publish/upload operations.
pub mod branch;
/// Remote repository cursor (site + subject).
pub mod repository;
/// Remote site configuration (add/load).
pub mod site;
/// Persisted remote configuration state.
pub mod state;

pub use branch::RemoteBranch;
pub use repository::RemoteRepository;
pub use site::RemoteSite;
pub use state::SiteName;

use super::branch::UpstreamState;
