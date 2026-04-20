//! Re-export of [`dialog_network::Network`] for backwards compatibility.
//!
//! The composite `Network` site (and its `NetworkAddress`,
//! `NetworkAuthorization`, `NetworkFork` companions) lives in the
//! `dialog-network` crate.

pub use dialog_network::{Network, NetworkAddress, NetworkAuthorization, NetworkFork};
