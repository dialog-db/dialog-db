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
    name_of(prefix, timestamp_nanos(), process_part(), seq)
}

/// The name [`unique_name`] forms from its parts: the clock reading
/// `timestamp`, the part naming this process, `process`, and the
/// process-local `sequence`.
pub fn name_of(prefix: &str, timestamp: u128, _process: &str, sequence: u64) -> String {
    format!("{prefix}-{timestamp}-{sequence}")
}

/// The part of a [`unique_name`] that names this process.
pub fn process_part() -> &'static str {
    ""
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Two processes that read one clock value as their first name draw
    /// it with the same sequence, 0: the test runner starts a process per
    /// test. The process part is what names them apart.
    #[dialog_common::test]
    fn names_drawn_at_one_instant_by_two_processes_differ() {
        assert_ne!(name_of("vault", 42, "a", 0), name_of("vault", 42, "b", 0));
    }

    /// The marker a child process prints its name between.
    const MARKER: &str = "process-part:";

    /// Run only as a child of
    /// [`two_child_processes_name_apart_at_one_instant`]: prints the name
    /// this process forms at a fixed clock reading.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    #[ignore = "a child of two_child_processes_name_apart_at_one_instant"]
    fn print_a_name_at_a_fixed_instant() {
        println!("{MARKER}{}", name_of("vault", 42, process_part(), 0));
    }

    /// Two processes forming a name at one fixed clock reading, each its
    /// first, form two names. Native only: a browser test cannot start a
    /// process. On wasm the process part is random bytes drawn once per
    /// module, which
    /// [`names_drawn_at_one_instant_by_two_processes_differ`] covers by
    /// its parts.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn two_child_processes_name_apart_at_one_instant() -> std::io::Result<()> {
        let name = || -> std::io::Result<String> {
            let output = std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "helpers::tests::print_a_name_at_a_fixed_instant",
                    "--include-ignored",
                    "--nocapture",
                ])
                .output()?;
            let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
            Ok(stdout
                .lines()
                .find_map(|line| line.strip_prefix(MARKER))
                .map(str::to_owned)
                .unwrap_or_else(|| format!("no name printed: {stdout}")))
        };
        let (first, second) = (name()?, name()?);
        assert!(first.starts_with("vault-42-"), "{first}");
        assert_ne!(first, second);
        Ok(())
    }
}
