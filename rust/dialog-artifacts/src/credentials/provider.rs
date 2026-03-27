//! Provider implementations for credential commands.

#[cfg(not(target_arch = "wasm32"))]
mod native;
mod volatile;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod web;
