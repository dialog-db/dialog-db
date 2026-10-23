//! Seeded entropy for a measurement harness that must replay a run.
//!
//! Production code draws every random byte through
//! [`random::fill`](crate::random::fill): keys, nonces, and the fresh
//! identifiers a branch's first commit mints for its line. Those reach what
//! a replica stores, so two runs of one scenario write different trees. A
//! harness that compares runs (the soak harness gates request counts that a
//! tree's leaf seams decide) needs them to be the same.
//!
//! [`seed`] makes them the same, on the current thread, for as long as the
//! returned [`Seeded`] guard lives: [`random::fill`](crate::random::fill)
//! then reads a stream derived from the seed instead of the system's
//! source. Nothing else switches it on. A build without the `helpers`
//! feature has no such stream at all, and a build with it (the feature
//! unifies across a workspace's test builds) draws from the system unless a
//! caller seeded the thread.
//!
//! ```
//! use dialog_common::helpers::entropy;
//!
//! let _seeded = entropy::seed(7);
//! let mut first = [0u8; 32];
//! dialog_common::random::fill(&mut first).unwrap();
//! // Every run that seeds 7 and draws in the same order reads `first`.
//! ```

use std::cell::RefCell;

thread_local! {
    static STREAM: RefCell<Option<blake3::OutputReader>> = const { RefCell::new(None) };
}

/// The context the seed's stream is derived under.
const CONTEXT: &str = "dialog-common helpers entropy v1";

/// Draw the current thread's random bytes from a stream derived from
/// `seed` until the returned guard drops.
///
/// Seeding a thread that is already seeded replaces its stream; the guard
/// restores the stream it replaced.
pub fn seed(seed: u64) -> Seeded {
    let mut hasher = blake3::Hasher::new_derive_key(CONTEXT);
    hasher.update(&seed.to_le_bytes());
    let stream = hasher.finalize_xof();
    let previous = STREAM.with(|slot| slot.borrow_mut().replace(stream));
    Seeded { previous }
}

/// While alive, the current thread's random bytes come from a seed. See
/// [`seed`].
#[must_use = "the thread draws from its seed only while the guard lives"]
pub struct Seeded {
    previous: Option<blake3::OutputReader>,
}

impl Drop for Seeded {
    fn drop(&mut self) {
        let previous = self.previous.take();
        STREAM.with(|slot| *slot.borrow_mut() = previous);
    }
}

impl std::fmt::Debug for Seeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Seeded").finish_non_exhaustive()
    }
}

/// Fill `bytes` from the current thread's seeded stream, if it has one.
/// Whether it did.
pub(crate) fn draw(bytes: &mut [u8]) -> bool {
    STREAM.with(|slot| match slot.borrow_mut().as_mut() {
        Some(stream) => {
            stream.fill(bytes);
            true
        }
        None => false,
    })
}

#[cfg(test)]
mod tests {
    use super::seed;
    use crate::random::fill;

    fn draw() -> [u8; 32] {
        let mut bytes = [0u8; 32];
        fill(&mut bytes).unwrap();
        bytes
    }

    #[dialog_macros::test]
    async fn it_replays_the_same_bytes_for_the_same_seed() {
        let first = {
            let _seeded = seed(7);
            (draw(), draw())
        };
        let second = {
            let _seeded = seed(7);
            (draw(), draw())
        };
        assert_eq!(first, second);
        assert_ne!(first.0, first.1, "the stream advances between draws");
    }

    #[dialog_macros::test]
    async fn it_draws_different_bytes_for_different_seeds() {
        let seven = {
            let _seeded = seed(7);
            draw()
        };
        let eight = {
            let _seeded = seed(8);
            draw()
        };
        assert_ne!(seven, eight);
    }

    #[dialog_macros::test]
    async fn it_returns_to_the_system_source_when_the_guard_drops() {
        let seeded = {
            let _seeded = seed(7);
            draw()
        };
        let after = draw();
        assert_ne!(after, seeded);
        assert_ne!(draw(), after, "the system source is fresh per draw");
    }

    #[dialog_macros::test]
    async fn it_restores_the_stream_a_nested_seed_replaced() {
        let expected = {
            let _seeded = seed(7);
            (draw(), draw())
        };
        let _outer = seed(7);
        let first = draw();
        {
            let _inner = seed(8);
            draw();
        }
        assert_eq!((first, draw()), expected);
    }
}
