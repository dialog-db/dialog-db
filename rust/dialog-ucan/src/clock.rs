//! When an invocation says it was issued, corrected by what the services
//! that judge it said their clocks read.
//!
//! A service refuses an invocation that says it was issued further from
//! its own clock than its freshness window, as
//! [`AuthorizeError::Stale`](dialog_capability::access::AuthorizeError::Stale),
//! and says what its clock read. A client whose clock is off by more than
//! the window would have every invocation refused, so it learns the
//! difference from that refusal ([`correct`]) and stamps the invocations
//! it mints for the same subject with its own time moved by it
//! ([`issued_at`]).
//!
//! The difference is kept per subject: a subject's operations are judged
//! by the service that holds it, and two services need not agree with
//! each other. It lives for the process, so a correction is learned once.
//! A service can only move the time of invocations for its own subjects,
//! which only it judges.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, PoisonError};

use dialog_capability::Did;
use dialog_common::time::{self, UNIX_EPOCH};
use dialog_ucan_core::time::Timestamp;
use dialog_ucan_core::time::timestamp::Duration;

/// The seconds to add to this clock for each subject whose service's clock
/// was found to differ.
fn corrections() -> &'static Mutex<HashMap<Did, i64>> {
    static CORRECTIONS: OnceLock<Mutex<HashMap<Did, i64>>> = OnceLock::new();
    CORRECTIONS.get_or_init(Mutex::default)
}

/// This clock, in Unix seconds.
fn local() -> i64 {
    time::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| i64::try_from(since.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

/// When an invocation for `subject` minted now says it was issued: this
/// clock, moved by what the subject's service was found to read.
pub fn issued_at(subject: &Did) -> Timestamp {
    let correction = corrections()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(subject)
        .copied()
        .unwrap_or_default();
    let corrected = u64::try_from(local().saturating_add(correction)).unwrap_or_default();
    Timestamp::new(dialog_ucan_core::time::timestamp::UNIX_EPOCH + Duration::from_secs(corrected))
        .unwrap_or_else(|_| Timestamp::now())
}

/// The service that judges `subject`'s invocations said its clock read
/// `service_at` (Unix seconds) as it refused one as stale: invocations
/// minted for `subject` from now on say they were issued by that clock.
pub fn correct(subject: &Did, service_at: u64) {
    let service_at = i64::try_from(service_at).unwrap_or(i64::MAX);
    let correction = service_at.saturating_sub(local());
    let mut corrections = corrections().lock().unwrap_or_else(PoisonError::into_inner);
    if correction == 0 {
        corrections.remove(subject);
    } else {
        corrections.insert(subject.clone(), correction);
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    fn subject(name: &str) -> Did {
        format!("did:key:z{name}").parse().expect("a DID")
    }

    /// An invocation for a subject whose service reads ten minutes ahead
    /// says it was issued ten minutes ahead, and one for another subject
    /// says what this clock reads.
    #[dialog_common::test]
    fn it_stamps_by_the_clock_of_the_subjects_service() {
        let ahead = subject("ClockAhead");
        let other = subject("ClockOther");
        correct(&ahead, u64::try_from(local() + 600).unwrap());

        let stamped = i64::try_from(issued_at(&ahead).to_unix()).unwrap();
        assert!((stamped - (local() + 600)).abs() <= 2, "{stamped}");
        let unmoved = i64::try_from(issued_at(&other).to_unix()).unwrap();
        assert!((unmoved - local()).abs() <= 2, "{unmoved}");

        correct(&ahead, u64::try_from(local()).unwrap());
        let back = i64::try_from(issued_at(&ahead).to_unix()).unwrap();
        assert!((back - local()).abs() <= 2, "{back}");
    }
}
