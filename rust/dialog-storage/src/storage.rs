#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) mod idb;

/// Capability-based storage providers.
pub mod provider;
