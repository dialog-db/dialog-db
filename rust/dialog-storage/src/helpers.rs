use dialog_capability::{Did, Subject};
use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
use dialog_varsig::Principal;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use dialog_common::time;

#[cfg(not(target_arch = "wasm32"))]
mod fs;
#[cfg(not(target_arch = "wasm32"))]
pub use fs::*;

mod flaky;
pub use flaky::*;

/// Returns a name unique within this process, formed as `{prefix}-{ts}-{seq}`.
///
/// Uses a high-resolution timestamp combined with a process-local counter so
/// tests can run concurrently without colliding on database / directory names.
pub fn unique_name(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = timestamp_nanos();
    format!("{prefix}-{ts}-{seq}")
}

#[cfg(not(target_arch = "wasm32"))]
fn timestamp_nanos() -> u128 {
    time::now()
        .duration_since(time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(target_arch = "wasm32")]
fn timestamp_nanos() -> u128 {
    // js_sys::Date::now returns ms since the epoch. Multiply to nanos so the
    // value matches the native side's resolution scale.
    (js_sys::Date::now() as u128) * 1_000_000
}

/// Generate a fresh ed25519 signer and return its DID.
pub async fn unique_did() -> Did {
    let signer = Ed25519Signer::generate().await.unwrap();
    Principal::did(&signer)
}

/// Build a `Subject` whose DID embeds a unique-per-process token.
///
/// Useful for tests that need distinct subjects without spinning a
/// new ed25519 signer.
pub fn unique_subject(prefix: &str) -> Subject {
    let did: Did = format!("did:{}", unique_name(&format!("test:{prefix}")))
        .parse()
        .expect("synthesized did:test URI must parse");
    Subject::from(did)
}

/// Build a fresh signer-backed credential.
pub async fn test_credential() -> Credential {
    let signer = Ed25519Signer::generate().await.unwrap();
    Credential::Signer(SignerCredential::from(signer))
}
