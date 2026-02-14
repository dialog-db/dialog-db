//! Browser devtools extension for inspecting Dialog databases.
//!
//! `dialog-inspector` provides introspection tooling for [`dialog_artifacts::Artifacts`]
//! instances stored in browser IndexedDB. It is designed to integrate with browser
//! developer tools as a web extension panel, but also works as a standalone page
//! or via a service worker route.
//!
//! # Architecture
//!
//! Three deployment modes, all sharing the same [`handler::Request`] /
//! [`handler::Response`] protocol:
//!
//! ## 1. Standalone (same origin)
//!
//! ```text
//! ┌──────────────────────────────────────────────┐
//! │ Panel served at host origin                   │
//! │  UI ── bridge (direct) ── dispatch ──▸ IDB    │
//! └──────────────────────────────────────────────┘
//! ```
//!
//! The simplest mode: the panel page is served from the same origin as the
//! application.  [`bridge::send`] calls [`dispatch::dispatch`] directly.
//!
//! ## 2. Service worker plugin (extension-assisted)
//!
//! ```text
//! ┌───────────────────────────────────────────────────────┐
//! │ Host application's existing service worker            │
//! │  includes: import { initDialogInspector }             │
//! │            from './sw-plugin.js';                      │
//! │                                                       │
//! │  content script ──msg──▸ SW: "here's my extension id" │
//! │                          │                             │
//! │                          ▼                             │
//! │                   import('chrome-extension://<id>/     │
//! │                     dialog_inspector_worker.js')       │
//! │                          │                             │
//! │                   SW (WASM) ── dispatch ──▸ IDB        │
//! │                     ▲                                  │
//! │  Panel ─ fetch ─────┘                                  │
//! └───────────────────────────────────────────────────────┘
//! ```
//!
//! The host app's service worker includes `sw-plugin.js` (one-line opt-in).
//! When the extension's content script is injected, it sends the extension
//! ID to the SW via `postMessage`.  The SW dynamically imports the inspector
//! WASM module from the extension's `web_accessible_resources`.  The panel
//! uses [`bridge::send`] in fetch mode, or the host app can also deploy the
//! WASM locally (no extension needed) by passing `{ wasmUrl }` to the plugin.
//!
//! ## 3. Extension (content script bridge)
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
//! - **[`bridge`]**: Sends requests — auto-detects mode (direct, fetch, or
//!   message) based on the runtime context.
//! - **[`dispatch`]**: Executes requests against IndexedDB (via
//!   [`discovery`] and [`inspect`]).
//! - **[`components`]**: Leptos CSR UI panel.
//!
//! # Entry points
//!
//! - **`panel`** binary: mounts the Leptos UI to the DOM.
//! - **`content`** binary: injected into the host page by the extension;
//!   listens for messages and calls [`dispatch`].
//! - **`worker`** binary: compiled to WASM via wasm-pack; loaded by the
//!   service worker JS shell to handle API requests.

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
