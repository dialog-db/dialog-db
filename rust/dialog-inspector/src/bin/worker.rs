//! Service worker entry point for dialog-inspector.
//!
//! This binary is compiled to WASM (via `wasm-pack`) and loaded by the JS
//! service worker shell (`service_worker.js`).
//!
//! It exports two functions to JS:
//!
//! - [`activate`]: called once during SW activation.
//! - [`handle_request`]: called for each intercepted `fetch` event
//!   whose URL matches the inspector's API prefix.  Receives the
//!   request path + query as a JSON string ([`handler::Request`]),
//!   returns the response as a JSON string ([`handler::Response`]).
//!
//! # Intended usage
//!
//! A host application that uses dialog-db can register this service
//! worker to expose an inspector endpoint at its own origin:
//!
//! ```js
//! navigator.serviceWorker.register("/dialog-inspector-sw.js");
//! ```
//!
//! The SW then serves the panel UI at `/dialog-inspector/` and handles
//! API requests at `/dialog-inspector/api/*`.  Because it runs in the
//! host page's origin it has full IndexedDB access.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod wasm {
    use wasm_bindgen::prelude::*;

    /// Called once when the service worker activates.
    #[wasm_bindgen]
    pub async fn activate() {
        console_error_panic_hook::set_once();
    }

    /// Handle an API request from the service worker fetch event.
    ///
    /// `request_json` is a JSON-encoded [`handler::Request`].
    /// Returns a JSON-encoded [`handler::Response`].
    #[wasm_bindgen]
    pub async fn handle_request(request_json: String) -> String {
        use dialog_inspector::handler::{Request, Response};

        let request: Request = match serde_json::from_str(&request_json) {
            Ok(r) => r,
            Err(e) => {
                let err = Response::Error {
                    message: format!("Invalid request JSON: {e}"),
                };
                return serde_json::to_string(&err).unwrap_or_default();
            }
        };

        let response = dialog_inspector::dispatch::dispatch(request).await;
        serde_json::to_string(&response).unwrap_or_default()
    }
}

fn main() {
    // Service workers don't have a DOM to mount. On wasm32, the exported
    // functions (activate, handle_request) are called by the JS shell.
}
