//! Browser devtools extension for inspecting Dialog databases.
//!
//! `dialog-inspector` provides introspection tooling for [`dialog_artifacts::Artifacts`]
//! instances stored in browser IndexedDB. It is designed to integrate with browser
//! developer tools as a web extension panel, but also works as a standalone page
//! when served from the same origin as the inspected application.
//!
//! # Architecture
//!
//! ```text
//!                    ┌──────────────────────────────────────────┐
//!                    │ Standalone (same origin)                 │
//!                    │  panel ──bridge──dispatch──▸ IndexedDB   │
//!                    └──────────────────────────────────────────┘
//!
//!  ┌─────────────────────┐           ┌──────────────────────────┐
//!  │ Extension panel      │           │ Content script            │
//!  │ (extension origin)   │           │ (host page origin)        │
//!  │                      │           │                           │
//!  │  UI ── bridge ───────│──message─▸│  dispatch ──▸ IndexedDB   │
//!  │        ▲             │           │       │                   │
//!  │        └─────────────│◂─message──│───────┘                   │
//!  └─────────────────────┘           └──────────────────────────┘
//! ```
//!
//! - **[`handler`]**: Serde-serializable [`Request`](handler::Request) /
//!   [`Response`](handler::Response) protocol.
//! - **[`bridge`]**: Sends requests — direct dispatch in standalone mode,
//!   `chrome.tabs.sendMessage` in extension mode.
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
