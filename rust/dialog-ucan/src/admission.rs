//! Whether a verified invocation may be performed now: it is fresh, and
//! it was not presented before.
//!
//! Verifying a chain proves who asked for what, under whose authority, and
//! for how long that authority holds. It says nothing about whether this
//! request is the one its issuer just made or a copy of it. A captured
//! invocation verifies just as well the second time, for as long as its
//! delegations last. So a responder that performs what it verifies also
//! asks two questions of its own, after the chain checks out:
//!
//! - **Is it fresh?** An invocation is good when it is made. One that says
//!   it was issued further from the responder's clock than [`FRESHNESS`]
//!   is refused as [`AuthorizeError::Stale`], which says what the clock
//!   read, so a client whose clock is off can correct it
//!   ([`crate::clock::correct`]) and ask again.
//! - **Was it presented before?** An invocation carries a nonce, so one
//!   made again is never the same as one made before. One presented twice
//!   was copied, and is refused as [`AuthorizeError::Replayed`]. It need
//!   only be remembered while it is fresh: after that it is stale anyway.
//!
//! [`Admission`] asks both, for the access service and for a peer that
//! answers invocations alike.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use dialog_capability::access::AuthorizeError;
use dialog_common::time::{self, UNIX_EPOCH};
use dialog_common::{ConditionalSend, ConditionalSync};

/// How long after it was issued an invocation is accepted, and how far
/// ahead of the responder's clock its issue time may be: an invocation is
/// good when it is made, and the window allows for its trip and for
/// clocks that disagree.
pub const FRESHNESS: Duration = Duration::from_secs(30);

/// Whether an invocation must say when it was issued.
///
/// One that says is refused once it is older than the freshness window,
/// whichever way it arrives. One that does not say cannot be aged, so
/// where a captured invocation would grant lasting access (a watch,
/// which follows a cell for as long as its chain holds) saying is
/// required, and elsewhere it is not, so clients that predate it are
/// still served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Issuance {
    /// An invocation that does not say when it was issued is refused.
    Required,
    /// An invocation that does not say when it was issued is accepted.
    Optional,
}

/// The invocations a responder accepted lately, remembered so that one
/// presented again is refused.
///
/// An invocation carries a nonce, so one made again is never the same as
/// one made before: an invocation presented twice was copied. It need
/// only be remembered while it is fresh, since after that it is refused
/// as stale anyway.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Presented: ConditionalSync {
    /// Remember `invocation`, its content identifier, until `until` (Unix
    /// seconds), answering `false` when it is remembered already. `now` is
    /// what the responder's clock reads, by which what was remembered until
    /// before now may be forgotten.
    async fn record(&self, invocation: &str, until: u64, now: u64) -> bool;
}

/// What a responder's clock reads, in Unix seconds: the time it judges how
/// long ago an invocation was issued by.
///
/// The system clock by default ([`Admission::with_clock`] to change it).
pub trait Clock: ConditionalSend + ConditionalSync {
    /// What the clock reads now, in Unix seconds.
    fn now(&self) -> u64;
}

impl<F> Clock for F
where
    F: Fn() -> u64 + ConditionalSend + ConditionalSync,
{
    fn now(&self) -> u64 {
        self()
    }
}

/// The system clock, in Unix seconds.
pub fn system_now() -> u64 {
    time::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default()
}

/// Presented invocations remembered in the memory of one process: what
/// a responder that runs in one process, or keeps one instance per space,
/// needs. A responder spread over many processes shares a record they all
/// reach instead.
#[derive(Debug, Default)]
pub struct RecentInvocations(Mutex<HashMap<String, u64>>);

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Presented for RecentInvocations {
    async fn record(&self, invocation: &str, until: u64, now: u64) -> bool {
        let mut presented = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        presented.retain(|_, remembered| *remembered >= now);
        if presented.contains_key(invocation) {
            return false;
        }
        presented.insert(invocation.to_string(), until);
        true
    }
}

/// The freshness window, the record of what was presented, and the clock
/// a responder judges invocations by.
///
/// One `Admission` is one record: a responder that answers on many
/// connections keeps one, so a copy presented on another connection is
/// refused as a copy presented on the same one is.
#[derive(Clone)]
pub struct Admission {
    freshness: Duration,
    presented: Arc<dyn Presented>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for Admission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admission")
            .field("freshness", &self.freshness)
            .finish_non_exhaustive()
    }
}

impl Default for Admission {
    fn default() -> Self {
        Self::new()
    }
}

impl Admission {
    /// Accept an invocation for [`FRESHNESS`] after it was issued,
    /// remembering what was presented in this process, by the system
    /// clock.
    pub fn new() -> Self {
        Self {
            freshness: FRESHNESS,
            presented: Arc::new(RecentInvocations::default()),
            clock: Arc::new(system_now),
        }
    }

    /// Judge time by `clock` rather than the system clock: an embedder
    /// whose platform keeps time its own way, or a test of a responder
    /// whose clock differs from its clients'.
    pub fn with_clock(mut self, clock: impl Clock + 'static) -> Self {
        self.clock = Arc::new(clock);
        self
    }

    /// Accept an invocation for `freshness` after it was issued (see
    /// [`FRESHNESS`]).
    pub fn with_freshness(mut self, freshness: Duration) -> Self {
        self.freshness = freshness;
        self
    }

    /// Remember the invocations accepted in `presented`: an embedder that
    /// builds a responder per request keeps one record across them.
    pub fn with_presented(mut self, presented: Arc<dyn Presented>) -> Self {
        self.presented = presented;
        self
    }

    /// What the clock reads, in Unix seconds.
    pub fn now(&self) -> u64 {
        self.clock.now()
    }

    /// Accept a verified invocation, identified by `invocation` (its
    /// content identifier) and saying it was issued at `issued_at` (Unix
    /// seconds), only when it is fresh and was not presented before.
    ///
    /// Ask this after the chain verifies, so an invocation is only ever
    /// refused as stale or as replayed when it is authentic.
    pub async fn admit(
        &self,
        issued_at: Option<u64>,
        invocation: &str,
        issuance: Issuance,
    ) -> Result<(), AuthorizeError> {
        let at = self.clock.now();
        let window = self.freshness.as_secs();
        let fresh = match issued_at {
            Some(issued) => {
                issued.saturating_add(window) >= at && issued <= at.saturating_add(window)
            }
            None => issuance == Issuance::Optional,
        };
        if !fresh {
            return Err(AuthorizeError::Stale { issued_at, at });
        }
        let until = issued_at.unwrap_or(at).saturating_add(window);
        if !self.presented.record(invocation, until, at).await {
            return Err(AuthorizeError::Replayed {
                invocation: invocation.to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    const NOW: u64 = 1_000_000;

    fn admission() -> Admission {
        Admission::new().with_clock(|| NOW)
    }

    #[dialog_common::test]
    async fn it_admits_a_fresh_invocation_once() {
        let admission = admission();
        assert!(
            admission
                .admit(Some(NOW), "a", Issuance::Optional)
                .await
                .is_ok()
        );
        let again = admission.admit(Some(NOW), "a", Issuance::Optional).await;
        assert!(
            matches!(&again, Err(AuthorizeError::Replayed { invocation }) if invocation == "a"),
            "{again:?}"
        );
    }

    #[dialog_common::test]
    async fn it_refuses_an_invocation_issued_outside_the_window() {
        let admission = admission();
        let window = FRESHNESS.as_secs();
        for issued in [NOW - window - 1, NOW + window + 1] {
            let refused = admission.admit(Some(issued), "b", Issuance::Optional).await;
            assert!(
                matches!(
                    refused,
                    Err(AuthorizeError::Stale { issued_at: Some(at_issue), at: NOW })
                        if at_issue == issued
                ),
                "{refused:?}"
            );
        }
        // Refused as stale, it was never recorded, so a fresh copy of it
        // is admitted.
        assert!(
            admission
                .admit(Some(NOW), "b", Issuance::Optional)
                .await
                .is_ok()
        );
    }

    #[dialog_common::test]
    async fn it_requires_an_issue_time_only_where_asked() {
        let admission = admission();
        let refused = admission.admit(None, "c", Issuance::Required).await;
        assert!(
            matches!(
                refused,
                Err(AuthorizeError::Stale {
                    issued_at: None,
                    at: NOW
                })
            ),
            "{refused:?}"
        );
        assert!(admission.admit(None, "c", Issuance::Optional).await.is_ok());
    }

    #[dialog_common::test]
    async fn it_shares_its_record_across_clones() {
        let admission = admission();
        let other = admission.clone();
        assert!(
            admission
                .admit(Some(NOW), "d", Issuance::Optional)
                .await
                .is_ok()
        );
        let again = other.admit(Some(NOW), "d", Issuance::Optional).await;
        assert!(
            matches!(again, Err(AuthorizeError::Replayed { .. })),
            "{again:?}"
        );
    }
}
