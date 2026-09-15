use std::sync::Arc;

use crate::RemoteSite;
use async_trait::async_trait;
use dialog_capability::Fork;
use dialog_capability::{Capability, Provider};
use dialog_common::{Buffer, ConditionalSync, ScopedFlight};
use dialog_effects::archive::prelude::{ArchiveExt, ArchiveSubjectExt, CatalogExt};
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_storage::{Blake3Hash, DialogStorageError, Encoder, StorageBackend};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::{Debug, Display};

use super::local::LocalIndex;
use crate::RemoteRepository;

/// In-flight remote hydrations, joined by digest, scoped to the driven
/// preload jobs of one query evaluation.
///
/// This closes the re-download window of bead dialog-db-81 for the case
/// that produced it: a burst of preload jobs racing one another to cold
/// blocks, where a job that passed its local check before a peer's
/// hydration landed re-downloaded bytes the archive already held. The
/// shared future carries the fetch AND the local write-back, so a
/// joiner can never observe "fetched but not yet hydrated" — and every
/// joiner polls the shared work itself, so nothing waits on progress it
/// cannot drive (a leader parked in some other scan's unpolled
/// read-ahead set deadlocks any design that waits on notifications
/// instead of co-driving; this one was tried and reverted).
///
/// The futures borrow the evaluation's env for `'a`: nothing owns the
/// env, and the holder becomes invariant in `'a` — which is why this
/// hangs off [`Driven`](crate::repository::fetch) (whose lifetime
/// nothing shrinks) and deliberately NOT off `QueryEnv`, whose
/// covariance in `'a` is load-bearing for the `Provider<Select>`
/// lifetime unification. Demand reads therefore do not join this
/// flight; they are protected by the local re-check below and the
/// transport's own `'static` flight.
///
/// Errors are shared as their rendering; nothing is cached, so retry
/// semantics are unchanged, and hydration is content-addressed so every
/// joiner's answer is identical.
pub type HydrationFlight<'a> = ScopedFlight<'a, Blake3Hash, Result<Option<Arc<Vec<u8>>>, String>>;

/// The remote half of a [`NetworkedIndex`]: what a local read miss means.
///
/// On a partial replica a locally absent block is routine — by-reference
/// regions hydrate on demand through the tracked remote. That makes the
/// *unavailability* of the tracked remote a load-bearing fact: a caller
/// that swallows a failed remote load and quietly falls back to
/// local-only turns every by-reference read into a bare "Blob not found"
/// with the actual cause (the remote could not be loaded, and why)
/// erased. [`Unavailable`](RemoteFallback::Unavailable) keeps that cause
/// attached: reads that the local archive can satisfy still succeed, and
/// the first read that *needs* the remote fails naming it.
#[derive(Clone)]
pub enum RemoteFallback {
    /// No remote is tracked; a local miss is an ordinary `None`.
    None,
    /// Local misses fetch through this remote and cache locally.
    Remote(RemoteRepository),
    /// A remote is tracked but could not be loaded. Reads served by the
    /// local archive succeed; a local miss is an error naming the
    /// remote and the reason it is unavailable.
    Unavailable {
        /// The tracked remote's name.
        remote: String,
        /// Why loading it failed.
        reason: String,
    },
}

impl RemoteFallback {
    /// Fold a remote load result into a fallback: a loaded remote serves
    /// misses; a failed load is carried as [`Unavailable`](Self::Unavailable)
    /// so the failure surfaces on the first read that needed the remote,
    /// instead of being erased into a bare not-found.
    pub fn from_load(
        remote: impl Into<String>,
        result: Result<RemoteRepository, impl Display>,
    ) -> Self {
        match result {
            Ok(loaded) => Self::Remote(loaded),
            Err(reason) => Self::Unavailable {
                remote: remote.into(),
                reason: reason.to_string(),
            },
        }
    }
}

impl From<Option<RemoteRepository>> for RemoteFallback {
    fn from(remote: Option<RemoteRepository>) -> Self {
        match remote {
            Some(remote) => Self::Remote(remote),
            None => Self::None,
        }
    }
}

impl From<RemoteRepository> for RemoteFallback {
    fn from(remote: RemoteRepository) -> Self {
        Self::Remote(remote)
    }
}

/// Content-addressed index with on-demand remote replication.
///
/// Wraps a [`LocalIndex`] and adds transparent remote fallback: reads
/// that miss locally are fetched from the remote and cached. Writes
/// always go to the local index only.
///
/// When no remote is configured, behaves identically to [`LocalIndex`].
/// When the tracked remote failed to load ([`RemoteFallback::Unavailable`]),
/// local hits still succeed and the first miss fails loudly with the
/// load failure as context.
pub struct NetworkedIndex<'a, Env> {
    local: LocalIndex<'a, Env>,
    remote: RemoteFallback,
    flight: Option<Arc<HydrationFlight<'a>>>,
}

impl<Env> Clone for NetworkedIndex<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            remote: self.remote.clone(),
            flight: self.flight.clone(),
        }
    }
}

impl<'a, Env> NetworkedIndex<'a, Env> {
    /// Create a networked index. With [`RemoteFallback::Remote`] (or a
    /// `Some(remote)`), reads that miss locally fall back to the remote
    /// and cache the result; see [`RemoteFallback`] for the other modes.
    pub fn new(
        env: &'a Env,
        index: Capability<Catalog>,
        remote: impl Into<RemoteFallback>,
    ) -> Self {
        Self {
            local: LocalIndex::new(env, index),
            remote: remote.into(),
            flight: None,
        }
    }

    /// Join this index's remote hydrations through `flight`: concurrent
    /// readers of one digest share a single fetch-and-write-back, so a
    /// reader can never re-download a block a peer's completed fetch
    /// already hydrated. Indexes serving one query evaluation should
    /// share one flight (the [`HydrationFlight`] docs carry the race
    /// this closes).
    pub fn with_flight(mut self, flight: Arc<HydrationFlight<'a>>) -> Self {
        self.flight = Some(flight);
        self
    }
}

/// Raw block access for the search tree, with the same transparent
/// remote fallback as the content-addressed `read`: reads that miss
/// locally are fetched from the remote and cached, writes go to the
/// local index only. Node buffers pass through without the CBOR encoder.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> StorageBackend for NetworkedIndex<'_, Env>
where
    Env:
        Provider<Get> + Provider<Put> + Provider<Fork<RemoteSite, Get>> + ConditionalSync + 'static,
{
    type Key = Blake3Hash;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        StorageBackend::set(&mut self.local, key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        if let Some(bytes) = StorageBackend::get(&self.local, key).await? {
            return Ok(Some(bytes));
        }

        let remote = match &self.remote {
            RemoteFallback::Remote(remote) => remote,
            RemoteFallback::None => return Ok(None),
            // The block is not local and the tracked remote — the only
            // place it could hydrate from — could not be loaded. Failing
            // here, with the cause, is the contract: silently returning
            // `None` would surface downstream as a bare "Blob not found"
            // that reads like data loss instead of what it is.
            RemoteFallback::Unavailable { remote, reason } => {
                let key = dialog_common::Blake3Hash::from(*key);
                return Err(DialogStorageError::Storage(format!(
                    "block {key} is not in the local archive and the tracked \
                     remote \"{remote}\" it would hydrate from is unavailable: \
                     {reason}"
                )));
            }
        };

        let address = remote.address();

        // Re-check local before paying the remote round trip: between the
        // miss above and this point, a concurrent reader of the same block
        // may have completed its fetch, hydrated, and moved on — a window
        // in which a naive fetch re-downloads bytes the archive already
        // holds. The recheck costs one local read on genuine cold misses;
        // the overlapping remainder of the race is closed by the
        // hydration flight below (and, without one, narrowed at the
        // transport's own flight).
        if let Some(bytes) = StorageBackend::get(&self.local, key).await? {
            return Ok(Some(bytes));
        }

        let env = self.local.env();
        let local_catalog = self.local.catalog().clone();

        match &self.flight {
            // Concurrent readers of one digest share one
            // fetch-and-hydrate: nobody can observe "fetched but not
            // yet written back", which is the re-download window this
            // flight exists to close (see [`HydrationFlight`]).
            Some(flight) => {
                let digest = *key;
                let outcome = flight
                    .join(digest, move || async move {
                        match hydrate(env, &address, local_catalog, digest).await {
                            Ok(bytes) => Ok(bytes.map(Arc::new)),
                            Err(error) => Err(error.to_string()),
                        }
                    })
                    .await;
                match outcome {
                    Ok(bytes) => Ok(bytes.map(|bytes| bytes.as_ref().clone())),
                    Err(message) => Err(DialogStorageError::Storage(message)),
                }
            }
            None => hydrate(env, &address, local_catalog, *key).await,
        }
    }
}

/// Fetch one block from the tracked remote and write it back into the
/// local archive before returning it — hydration is part of the read, so
/// a caller that observes the bytes can rely on the next local read
/// hitting.
async fn hydrate<Env>(
    env: &Env,
    address: &crate::RemoteAddress,
    local_catalog: Capability<Catalog>,
    key: Blake3Hash,
) -> Result<Option<Vec<u8>>, DialogStorageError>
where
    Env: Provider<Fork<RemoteSite, Get>> + Provider<Put> + ConditionalSync + 'static,
{
    let remote_catalog = address.subject.clone().archive().catalog("index");
    let remote_result = remote_catalog
        .get(key)
        .fork(&address.address)
        .perform(env)
        .await
        .map_err(DialogStorageError::from)?;

    match remote_result {
        Some(bytes) => {
            // Every hydration is one remote round trip (two, behind a
            // UCAN remote whose permit was not cached); this event is
            // what lets a slow first read be attributed to on-demand
            // replication rather than local work.
            tracing::debug!(
                target: "dialog::sync::hydrate",
                block = %dialog_common::Blake3Hash::from(key),
                bytes = bytes.len(),
                "hydrated block from remote"
            );
            let cache = local_catalog.put(Buffer::from(bytes.as_slice()));
            // A failed write-back is not a failed read, but it silently
            // turns every future read of this block into another remote
            // round trip — worth a trace, never worth failing the read.
            if let Err(error) = cache.perform(env).await {
                tracing::debug!(
                    target: "dialog::sync::hydrate",
                    block = %dialog_common::Blake3Hash::from(key),
                    %error,
                    "failed to cache hydrated block locally"
                );
            }
            Ok(Some(bytes))
        }
        None => Ok(None),
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Encoder for NetworkedIndex<'_, Env>
where
    Env: ConditionalSync + 'static,
{
    type Bytes = Vec<u8>;
    type Hash = Blake3Hash;
    type Error = DialogStorageError;

    async fn encode<T>(&self, block: &T) -> Result<(Self::Hash, Self::Bytes), Self::Error>
    where
        T: Serialize + ConditionalSync + Debug,
    {
        self.local.encoder().encode(block).await
    }

    async fn decode<T>(&self, bytes: &[u8]) -> Result<T, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
        self.local.encoder().decode(bytes).await
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_common::Buffer;
    use dialog_operator::helpers::test_operator_with_profile;
    use dialog_storage::StorageBackend as _;

    use super::{NetworkedIndex, RemoteFallback};
    use crate::RepositoryArchiveExt as _;
    use crate::helpers::test_repo;

    /// A tracked remote that failed to load must not silently degrade the
    /// index to local-only. A read the local archive satisfies still
    /// succeeds — a full replica keeps working offline — but a miss is
    /// the exact case that needed the remote, and it must fail carrying
    /// the load failure as its cause, not surface downstream as a bare
    /// "Blob not found" with the cause erased.
    #[dialog_common::test]
    async fn it_fails_a_miss_loudly_when_the_tracked_remote_is_unavailable() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let mut index = NetworkedIndex::new(
            &operator,
            branch.archive().index(),
            RemoteFallback::Unavailable {
                remote: "origin".into(),
                reason: "no credential saved for the site".into(),
            },
        );

        // A locally held block reads back: unavailability of the remote
        // must not cost a replica anything it already holds.
        let held = b"locally held block".to_vec();
        let held_key = *Buffer::from(held.as_slice()).blake3_hash().as_bytes();
        index.set(held_key, held.clone()).await?;
        assert_eq!(
            index.get(&held_key).await?,
            Some(held),
            "a local hit succeeds regardless of the remote's availability"
        );

        // A miss is the read that needed the remote: it fails naming the
        // remote and why it is unavailable.
        let absent_key = *Buffer::from(&b"never stored"[..]).blake3_hash().as_bytes();
        let error = index
            .get(&absent_key)
            .await
            .expect_err("a miss with an unavailable tracked remote must fail loudly");
        let message = error.to_string();
        assert!(
            message.contains("origin") && message.contains("no credential saved"),
            "the failure carries the remote and the load failure as cause: {message}"
        );

        // The same miss under no tracked remote stays an ordinary `None`:
        // only the *unavailable* state escalates.
        let local_only =
            NetworkedIndex::new(&operator, branch.archive().index(), RemoteFallback::None);
        assert_eq!(
            local_only.get(&absent_key).await?,
            None,
            "an untracked branch's miss is not an error"
        );

        Ok(())
    }
}
