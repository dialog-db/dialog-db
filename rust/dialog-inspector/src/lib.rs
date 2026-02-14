//! Browser devtools extension for inspecting Dialog databases.
//!
//! `dialog-inspector` provides introspection tooling for [`dialog_artifacts::Artifacts`]
//! instances stored in browser IndexedDB. It is a Manifest V3 Chrome extension
//! that adds a "Dialog DB" panel to the browser developer tools.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────┐         ┌──────────────────────────┐
//! │ Extension panel          │         │ Content script            │
//! │ (extension origin)       │         │ (host page origin)        │
//! │                          │         │                           │
//! │  UI ── bridge (message)──│──msg──▸ │  dispatch ──▸ IDB         │
//! │        ▲                 │         │       │                   │
//! │        └─────────────────│◂──msg── │───────┘                   │
//! └─────────────────────────┘         └──────────────────────────┘
//! ```
//!
//! The extension injects a content script into the host page.  The panel
//! runs in the extension's origin and cannot access host IDB directly, so
//! [`bridge::send`] serializes requests as JSON and sends them via
//! `chrome.tabs.sendMessage`.  The content script calls [`dispatch`] and
//! replies.
//!
//! # Modules
//!
//! - **[`handler`]**: Serde-serializable [`Request`](handler::Request) /
//!   [`Response`](handler::Response) protocol.
//! - **[`bridge`]**: Sends requests from the panel to the content script
//!   via `chrome.tabs.sendMessage`.
//! - **[`dispatch`]**: Executes requests against IndexedDB (via
//!   [`discovery`] and [`inspect`]).
//! - **[`components`]**: Leptos CSR UI panel.
//!
//! # Entry points
//!
//! - **`panel`** binary: mounts the Leptos UI to the DOM.
//! - **`content`** binary: injected into the host page by the extension;
//!   listens for messages and calls [`dispatch`].

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod discovery;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod inspect;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod dispatch;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod bridge;

pub mod handler;

pub mod components;
