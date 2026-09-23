use dialog_effects::archive::prelude::CatalogExt as _;
use dialog_effects::archive::prelude::GetBlockExt as _;
use std::sync::Arc;

use crate::RemoteSite;
use async_trait::async_trait;
use dialog_capability::Fork;
use dialog_capability::Provider;
use dialog_common::{Buffer, ConditionalSync, Priority};
use dialog_effects::archive::prelude::ArchiveExt;
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_storage::{Blake3Hash, DialogStorageError, Encoder, StorageBackend};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::{Debug, Display};

pub use dialog_network::{Hydrate, HydrationRequest, HydrationScheduler};

use super::local::LocalIndex;
use crate::ConnectedReplica;
use dialog_effects::MethodExt as _;
use dialog_effects::archive::prelude::CatalogScope;

/// The remote half of a [`NetworkedIndex`]: what a local read miss means.
///
/// On a partial replica a locally absent block is routine — by-reference
/// regions hydrate on demand through the tracked remote. That makes the
/// *unavailability* of the tracked remote a load-bearing fact: a caller
/// that swallows a failed remote load and quietly falls back to
/// local-only turns every by-reference read into a bare "Block not found"
/// with the actual cause (the remote could not be loaded, and why)
/// erased. [`Unavailable`](RemoteFallback::Unavailable) keeps that cause
/// attached: reads that the local archive can satisfy still succeed, and
/// the first read that *needs* the remote fails naming it.
#[derive(Clone)]
pub enum RemoteFallback {
    /// No remote is tracked; a local miss is an ordinary `None`.
    None,
    /// Local misses fetch through this remote and cache locally.
    Remote(ConnectedReplica),
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
        result: Result<ConnectedReplica, impl Display>,
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

impl From<Option<ConnectedReplica>> for RemoteFallback {
    fn from(remote: Option<ConnectedReplica>) -> Self {
        match remote {
            Some(remote) => Self::Remote(remote),
            None => Self::None,
        }
    }
}

impl From<ConnectedReplica> for RemoteFallback {
    fn from(remote: ConnectedReplica) -> Self {
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
    priority: Priority,
}

impl<Env> Clone for NetworkedIndex<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            remote: self.remote.clone(),
            priority: self.priority,
        }
    }
}

impl<'a, Env> NetworkedIndex<'a, Env> {
    /// Create a networked index. With [`RemoteFallback::Remote`] (or a
    /// `Some(remote)`), reads that miss locally fall back to the remote
    /// and cache the result; see [`RemoteFallback`] for the other modes.
    pub fn new(env: &'a Env, index: CatalogScope, remote: impl Into<RemoteFallback>) -> Self {
        Self {
            local: LocalIndex::new(env, index),
            remote: remote.into(),
            priority: Priority::Demand,
        }
    }

    /// Rank this index's hydrations as `priority` at the remote site.
    ///
    /// An index is [`Priority::Demand`] unless told otherwise: its reads
    /// are what some evaluation is blocked on. A speculative walker (the
    /// preload queue's jobs) reads through an index ranked at its hint's
    /// likelihood, so its warming never goes before a demand read of
    /// the same site.
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
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
    Env: Provider<Get> + Provider<Put> + Provider<Hydrate> + ConditionalSync + 'static,
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
            // `None` would surface downstream as a bare "Block not found"
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

        // The routing is resolved here; the fetch, the re-check that
        // guards it, the local write-back, and any sharing of the work
        // with concurrent readers of the same digest are the env's own
        // effect (see [`Hydrate`]).
        let (local, priority) = (&self.local, self.priority);
        let digest = dialog_common::Blake3Hash::from(*key);
        let hydrated = remote
            .reach(|route| {
                let request = HydrationRequest {
                    address: route.address,
                    subject: route.subject,
                    catalog: local.catalog().clone(),
                    digest: digest.clone(),
                    priority,
                };
                Provider::<Hydrate>::execute(local.env(), request)
            })
            .await?;
        Ok(hydrated.map(|bytes| bytes.as_ref().clone()))
    }
}

/// Fetch one block from the tracked remote and write it back into the
/// local archive before returning it — hydration is part of the read, so
/// a caller that observes the bytes can rely on the next local read
/// hitting. This is the whole substance of a [`Hydrate`] perform;
/// providers wrap it in whatever sharing they own.
///
/// The local re-check comes first: between a caller's miss and this
/// point, a concurrent reader of the same block may have completed its
/// fetch, hydrated, and moved on — a window in which a naive fetch
/// re-downloads bytes the archive already holds. Inside a shared flight
/// the re-check runs once for every set of joiners, so the window is
/// closed rather than narrowed.
pub async fn hydrate<Env>(
    env: &Env,
    request: HydrationRequest,
) -> Result<Option<Arc<Vec<u8>>>, ArchiveError>
where
    Env:
        Provider<Get> + Provider<Put> + Provider<Fork<RemoteSite, Get>> + ConditionalSync + 'static,
{
    let HydrationRequest {
        address,
        subject,
        catalog,
        digest,
        priority: _,
    } = request;

    if let Some(bytes) = catalog.clone().get(digest.clone()).perform(env).await? {
        return Ok(Some(Arc::new(bytes)));
    }

    let remote_catalog = subject.reader().archive().catalog("index");
    let remote_result = remote_catalog
        .get(digest.clone())
        .fork(&address)
        .perform(env)
        .await?;

    match remote_result {
        Some(bytes) => {
            // Every hydration is one remote round trip (two, behind a
            // UCAN remote whose permit was not cached); this event is
            // what lets a slow first read be attributed to on-demand
            // replication rather than local work.
            tracing::debug!(
                target: "dialog::sync::hydrate",
                block = %digest,
                bytes = bytes.len(),
                "hydrated block from remote"
            );
            let cache = catalog.put(Buffer::from(bytes.as_slice()));
            // A failed write-back is not a failed read, but it silently
            // turns every future read of this block into another remote
            // round trip — worth a trace, never worth failing the read.
            if let Err(error) = cache.perform(env).await {
                tracing::debug!(
                    target: "dialog::sync::hydrate",
                    block = %digest,
                    %error,
                    "failed to cache hydrated block locally"
                );
            }
            Ok(Some(Arc::new(bytes)))
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

    use crate::helpers::connect;
    use std::sync::Arc;

    use anyhow::Result;
    use dialog_capability::{Command, Provider};
    use dialog_common::{Buffer, ConditionalSend, ConditionalSync, Priority};
    use dialog_effects::archive::{Get, Put};
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_storage::StorageBackend as _;
    use parking_lot::Mutex;

    use super::{Hydrate, HydrationRequest, NetworkedIndex, RemoteFallback};
    use crate::helpers::test_repo;

    /// An env that answers every hydration with "not found" and keeps the
    /// priority each request carried.
    struct Recording<P> {
        inner: P,
        priorities: Arc<Mutex<Vec<Priority>>>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl<P> Provider<Get> for Recording<P>
    where
        P: Provider<Get> + ConditionalSync,
    {
        async fn execute(&self, input: <Get as Command>::Input) -> <Get as Command>::Output {
            Provider::<Get>::execute(&self.inner, input).await
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl<P> Provider<Put> for Recording<P>
    where
        P: Provider<Put> + ConditionalSync,
    {
        async fn execute(&self, input: <Put as Command>::Input) -> <Put as Command>::Output {
            Provider::<Put>::execute(&self.inner, input).await
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl<P> Provider<Hydrate> for Recording<P>
    where
        P: ConditionalSend + ConditionalSync,
    {
        async fn execute(&self, request: HydrationRequest) -> <Hydrate as Command>::Output {
            self.priorities.lock().push(request.priority);
            Ok(None)
        }
    }

    /// The priority an index is built with reaches the env's hydration
    /// request, which is where the scheduler reads it: an index is a
    /// demand reader unless told otherwise, and a speculative walker's
    /// index carries its hint's likelihood so its warming never takes a
    /// site's slot from a read someone is waiting on.
    #[dialog_common::test]
    async fn it_ranks_its_hydrations_at_its_priority() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let site = dialog_remote_s3::Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("bucket")
            .build()?;
        let origin = connect(&repo, "origin", site, repo.did(), &operator).await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let env = Recording {
            inner: operator,
            priorities: Arc::new(Mutex::new(Vec::new())),
        };

        let absent = *Buffer::from(&b"never stored"[..]).blake3_hash().as_bytes();
        let demand = NetworkedIndex::new(
            &env,
            branch.archive().index(),
            RemoteFallback::Remote(origin.clone()),
        );
        assert_eq!(demand.get(&absent).await?, None);
        let speculative = NetworkedIndex::new(
            &env,
            branch.archive().index(),
            RemoteFallback::Remote(origin),
        )
        .with_priority(Priority::Maybe);
        assert_eq!(speculative.get(&absent).await?, None);

        assert_eq!(
            env.priorities.lock().clone(),
            [Priority::Demand, Priority::Maybe],
            "each index hydrates at its own priority, demand by default"
        );
        Ok(())
    }

    /// A tracked remote that failed to load must not silently degrade the
    /// index to local-only. A read the local archive satisfies still
    /// succeeds — a full replica keeps working offline — but a miss is
    /// the exact case that needed the remote, and it must fail carrying
    /// the load failure as its cause, not surface downstream as a bare
    /// "Block not found" with the cause erased.
    #[dialog_common::test]
    async fn it_fails_a_miss_loudly_when_the_tracked_remote_is_unavailable() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
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
