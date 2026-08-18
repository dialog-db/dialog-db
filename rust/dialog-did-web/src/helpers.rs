//! Cross-target test helpers for `did:web` fetching.
//!
//! Exposes a local HTTP server, provisioned natively via the
//! `#[dialog_common::provider]` seam, that the fetch integration tests point a
//! real [`ReqwestFetch`](crate::ReqwestFetch) at. The server side is
//! native-only (it binds a TCP socket); the test *client* runs on both native
//! and wasm against the provisioned endpoint, which is what exercises the
//! browser `fetch` path the resolver uses in a browser.

#[cfg(not(target_arch = "wasm32"))]
mod server;

#[cfg(not(target_arch = "wasm32"))]
pub use server::did_web_server;

pub use address::{Behavior, DidWebServerAddress, DidWebServerSettings};

mod address {
    use serde::{Deserialize, Serialize};

    /// What the local test server should do for every request.
    ///
    /// Serializable so it can ride in the settings and (on the wasm path) be
    /// reconstructed in the inner test process.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub enum Behavior {
        /// Serve `bytes` with a 200.
        Serve(Vec<u8>),
        /// Reply `302` redirecting to `location`.
        Redirect(String),
        /// Reply `200` with a `Content-Length` of `declared` and a body of
        /// `declared` bytes. Exercises the pre-read (declared-length) check.
        Sized {
            /// The declared `Content-Length`, which is also the body length.
            declared: u64,
        },
        /// Reply `200` with NO `Content-Length` (body delimited by connection
        /// close) and `actual` body bytes. Exercises the post-read check, the
        /// backstop for a host that declares no length.
        Unsized {
            /// The number of body bytes sent.
            actual: usize,
        },
    }

    impl Default for Behavior {
        fn default() -> Self {
            // A small, well-formed document body: the happy path.
            Self::Serve(br#"{"id":"did:web:localhost"}"#.to_vec())
        }
    }

    /// The endpoint a provisioned did:web test server listens on.
    ///
    /// Passed to `#[dialog_common::test]` bodies; serializable so the wasm
    /// runner receives it from the native provider.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DidWebServerAddress {
        /// The `did.json` URL on the running server.
        pub url: String,
    }

    /// Settings selecting the server's [`Behavior`].
    #[derive(Debug, Clone, Default)]
    pub struct DidWebServerSettings {
        /// What the server does for each request.
        pub behavior: Behavior,
    }
}
