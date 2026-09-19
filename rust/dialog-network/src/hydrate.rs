//! The [`Hydrate`] command: on-demand replication of one
//! content-addressed block from a remote into the local archive, as an
//! effect of the environment.
//!
//! The command lives in this crate — beneath both the repository layer
//! that performs it and the operator layer that provides it — so the
//! two sides name one shared type. Only the vocabulary is here: the
//! plain implementation providers delegate to is the repository layer's
//! `hydrate`, and the operator's provider runs it through a process-wide
//! [`HydrationScheduler`].

use std::sync::Arc;

use dialog_capability::{Capability, Command, Did};
use dialog_common::{Blake3Hash, Priority, Scheduler};
use dialog_effects::archive::{ArchiveError, Catalog};

use crate::NetworkAddress;

/// Command: replicate one block from a remote into the local archive,
/// returning its bytes.
///
/// Hydration is an effect of the *environment*, not of the store that
/// asks for it: the caller resolves the routing (which remote a miss
/// hydrates from, which catalog it writes back into) per read, says how
/// urgently it wants the block, and performs this command, borrowing
/// the env for exactly the duration of the perform. Where the sharing
/// and the ordering live is the provider's business — an operator runs
/// every hydration through a [`HydrationScheduler`] built from its own
/// internals, so every evaluation path (queries, subscriptions,
/// transaction queries, pull) shares in-flight work and competes for
/// the site's window by priority with zero wiring. A plain environment
/// simply delegates to the repository layer's `hydrate`.
///
/// Shared work must carry fetch AND local write-back, so a joiner can
/// never observe "fetched but not yet hydrated" (the re-download race
/// of bead dialog-db-81), and every joiner must poll the shared work
/// itself, so nothing waits on progress it cannot drive (a leader
/// parked in some other scan's unpolled read-ahead set deadlocks any
/// design that waits on notifications instead of co-driving; that
/// design was tried and reverted). Hydration is content-addressed, so
/// every joiner's answer is identical regardless of which caller's
/// route ran.
pub struct Hydrate;

impl Command for Hydrate {
    type Input = HydrationRequest;
    type Output = Result<Option<Arc<Vec<u8>>>, ArchiveError>;
}

/// One [`Hydrate`] job, fully routed by the caller: the remote the
/// block hydrates from, the local catalog it writes back into, the
/// block itself, and how urgently it is wanted.
#[derive(Clone, Debug)]
pub struct HydrationRequest {
    /// The site the block hydrates from.
    pub address: NetworkAddress,
    /// The subject (repository DID) at that site.
    pub subject: Did,
    /// The local catalog the fetched block writes back into.
    pub catalog: Capability<Catalog>,
    /// The block to hydrate.
    pub digest: Blake3Hash,
    /// How the read ranks against the site's other reads: a demand read
    /// goes before speculative warming.
    pub priority: Priority,
}

/// The per-site priority window and digest-keyed single-flight a
/// sharing [`Hydrate`] provider runs concurrent hydrations through.
///
/// Held weakly by the provider (an operator field): the strong shared
/// futures live only in active joiners — `.perform` calls currently
/// borrowing the env — so in-flight work makes progress exactly while
/// someone drives it, drops with its last joiner, and never keeps the
/// env's own handle alive through the provider's own field. Errors are
/// shared as their rendering; nothing is cached, so retry semantics are
/// unchanged.
pub type HydrationScheduler =
    Scheduler<NetworkAddress, Blake3Hash, Result<Option<Arc<Vec<u8>>>, String>>;
