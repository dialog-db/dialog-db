//! Browser devtools extension for inspecting Dialog databases.
//!
//! `dialog-inspector` provides introspection tooling for [`dialog_artifacts::Artifacts`]
//! instances stored in browser IndexedDB. It is designed to integrate with browser
//! developer tools as a web extension panel.
//!
//! # Architecture
//!
//! The crate is structured around three layers:
//!
//! - **Discovery**: Enumerate all IndexedDB databases that look like dialog-db instances
//! - **Inspection**: Open a database read-only and inspect its revision, facts, and tree
//!   structure
//! - **Handler**: A fetch-like request handler that can be bound to a service worker,
//!   a web extension background script, or any other request-response context
//!
//! The Leptos-based UI lives in [`components`] and renders the devtools panel. It
//! communicates with the inspection layer which accesses IndexedDB directly (both
//! the UI and IndexedDB live in the same origin context).
//!
//! # Entry Points
//!
//! The crate compiles to two targets:
//!
//! - **`panel`** binary: The devtools panel UI, mounted to the DOM via Leptos CSR
//! - **Library**: Core inspection logic, usable from a service worker or any WASM
//!   context

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod discovery;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod inspect;

pub mod handler;

pub mod components;
