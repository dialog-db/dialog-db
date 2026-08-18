//! Fetch-layer integration tests against a real, locally-provisioned HTTP
//! server.
//!
//! The redirect-refusal and body-size-cap hardening in
//! [`ReqwestFetch`](dialog_did_web::ReqwestFetch) act on live responses, so a
//! `Fetch` double cannot reach them: only a server that issues a 3xx or an
//! oversized body proves the guard fires. Following the S3 / UCAN pattern, the
//! server is provisioned natively (via `#[dialog_common::provider]`) while the
//! *client* body runs on both native and wasm against the provisioned endpoint
//! — the wasm run is what validates the browser `fetch` path the resolver uses
//! in a browser.
//!
//! The server, its `Behavior`, and the byte cap are provisioning concerns: they
//! are referenced only inside the `#[dialog_common::test(...)]` attribute, which
//! the macro expands into the native-only provider setup. The test *bodies*
//! touch only [`DidWebServerAddress`] and the client, so nothing server-side
//! reaches the wasm build. That is why the attribute expressions are
//! fully-qualified rather than imported.

#![cfg(feature = "helpers")]

use dialog_did_web::helpers::DidWebServerAddress;
use dialog_did_web::{Fetch, ReqwestFetch};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// A well-formed document served with a 200 is returned verbatim.
#[dialog_common::test]
async fn serves_a_document_body(addr: DidWebServerAddress) -> anyhow::Result<()> {
    let body = ReqwestFetch::new().get(&addr.url).await?;
    assert_eq!(body, br#"{"id":"did:web:localhost"}"#);
    Ok(())
}

/// A redirect is refused, not followed.
#[dialog_common::test(
    behavior = dialog_did_web::helpers::Behavior::Redirect("http://evil.example/did.json".to_string())
)]
async fn refuses_a_redirect_instead_of_following_it(
    addr: DidWebServerAddress,
) -> anyhow::Result<()> {
    let outcome = ReqwestFetch::new().get(&addr.url).await;
    assert!(
        outcome.is_err(),
        "a 3xx must be refused, not followed: got {outcome:?}"
    );
    Ok(())
}

/// An over-cap `Content-Length` is refused before the body is read.
#[dialog_common::test(
    behavior = dialog_did_web::helpers::Behavior::Sized {
        declared: (dialog_did_web::MAX_DOCUMENT_BYTES as u64) + 1
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
    behavior = dialog_did_web::helpers::Behavior::Unsized {
        actual: dialog_did_web::MAX_DOCUMENT_BYTES + 1
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
