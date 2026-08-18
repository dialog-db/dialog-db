//! Fetch-layer integration tests against a real, locally-provisioned HTTP
//! server.
//!
//! The redirect-refusal and body-size-cap hardening in
//! [`ReqwestFetch`](crate::ReqwestFetch) act on live responses, so a `Fetch`
//! double cannot reach them: only a server that issues a 3xx or an oversized
//! body proves the guard fires. Following the S3 / UCAN pattern, the server is
//! provisioned natively (via `#[dialog_common::provider]`) while the *client*
//! body runs on both native and wasm against the provisioned endpoint — the
//! wasm run is what validates the browser `fetch` path the resolver uses in a
//! browser.
//!
//! These are **native-only**. The client talks to a server on `127.0.0.1`,
//! which a browser cannot reach (cross-origin / mixed content), so running the
//! client on wasm is not meaningful: every request fails before a status is
//! seen. `web-integration-tests` therefore does not enable `helpers`,
//! mirroring how `dialog-remote-s3` keeps its server-backed tests out of the
//! wasm run.
//!
//! They live in `src/` rather than `tests/` because the
//! `#[dialog_common::test]` macro spawns its inner test with `--lib`, so a test
//! in a separate integration-test target is never found.
//!
//! The server, its `Behavior`, and the byte cap are provisioning concerns:
//! they appear only inside the `#[dialog_common::test(...)]` attribute, which
//! the macro expands into the native-only provider setup. The test *bodies*
//! touch only [`DidWebServerAddress`] and the client, so nothing server-side
//! reaches the wasm build.

use super::DidWebServerAddress;
use crate::{Fetch, ReqwestFetch};

/// A well-formed document served with a 200 is returned verbatim.
#[dialog_common::test]
async fn serves_a_document_body(addr: DidWebServerAddress) -> anyhow::Result<()> {
    let body = ReqwestFetch::new().get(&addr.url).await?;
    assert_eq!(body, br#"{"id":"did:web:localhost"}"#);
    Ok(())
}

/// A redirect is refused, not followed. A resolver that chased it would read
/// the DID document from a host the DID never named.
#[dialog_common::test(
    behavior = crate::helpers::Behavior::Redirect("http://evil.example/did.json".to_string())
)]
async fn refuses_a_redirect_instead_of_following_it(
    addr: DidWebServerAddress,
) -> anyhow::Result<()> {
    let outcome = ReqwestFetch::new().get(&addr.url).await;
    let Err(error) = outcome else {
        panic!("a 3xx must be refused, not followed: got {outcome:?}");
    };

    // Naming the redirect specifically is the point: a bare "returned status
    // 302" reads as an ordinary server error and leaves the operator to work
    // out that our own redirect policy is what refused it. This also pins that
    // the redirect branch is reachable at all — checked before the generic
    // non-success branch, which would otherwise shadow it entirely.
    let message = error.to_string();
    assert!(
        message.contains("redirected"),
        "the refusal must name the redirect, got: {message}"
    );
    Ok(())
}

/// An over-cap `Content-Length` is refused before the body is read, so a
/// hostile host cannot make the client buffer an unbounded response.
#[dialog_common::test(
    behavior = crate::helpers::Behavior::Sized {
        declared: (crate::MAX_DOCUMENT_BYTES as u64) + 1
    }
)]
async fn refuses_over_cap_declared_length(addr: DidWebServerAddress) -> anyhow::Result<()> {
    let outcome = ReqwestFetch::new().get(&addr.url).await;
    assert!(
        outcome.is_err(),
        "an over-cap declared length must be refused: got {outcome:?}"
    );
    Ok(())
}

/// An over-cap body with NO declared length is refused by the post-read check,
/// the backstop for a host that omits `Content-Length`.
#[dialog_common::test(
    behavior = crate::helpers::Behavior::Unsized {
        actual: crate::MAX_DOCUMENT_BYTES + 1
    }
)]
async fn refuses_over_cap_unsized_body(addr: DidWebServerAddress) -> anyhow::Result<()> {
    let outcome = ReqwestFetch::new().get(&addr.url).await;
    assert!(
        outcome.is_err(),
        "an over-cap body with no declared length must be refused: got {outcome:?}"
    );
    Ok(())
}
