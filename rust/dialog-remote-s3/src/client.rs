//! The HTTP client shared by every request this crate issues.

/// A handle on the shared HTTP client.
///
/// A [`reqwest::Client`] owns a connection pool, so building one per
/// request throws away pooled connections and repeats TLS setup. The
/// client is cheap to clone — clones share the same pool.
///
/// The client is held per thread rather than in a process-wide static.
/// On wasm the client is not `Send`, so a static cannot hold it. On
/// native, a pooled connection is driven by a task on the tokio runtime
/// that opened it, so a process-wide pool would let a request on one
/// runtime reuse a connection owned by another and fail with "dispatch
/// task is gone" when the owning runtime shuts down — which is how
/// parallel `#[tokio::test]` tests poison each other. A thread never
/// hosts two live runtimes at once, so a per-thread pool keeps
/// connection reuse without crossing runtimes.
pub fn http_client() -> reqwest::Client {
    thread_local! {
        static CLIENT: reqwest::Client = reqwest::Client::new();
    }

    CLIENT.with(reqwest::Client::clone)
}
