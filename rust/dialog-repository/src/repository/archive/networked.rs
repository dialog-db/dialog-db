use crate::RemoteSite;
use async_trait::async_trait;
use dialog_capability::Fork;
use dialog_capability::{Capability, Provider};
use dialog_common::{Buffer, ConditionalSync};
use dialog_effects::archive::prelude::{ArchiveExt, ArchiveSubjectExt, CatalogExt};
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_storage::{Blake3Hash, DialogStorageError, Encoder, StorageBackend};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::{Debug, Display};

use super::local::LocalIndex;
use crate::RemoteRepository;

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
}

impl<Env> Clone for NetworkedIndex<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            remote: self.remote.clone(),
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
        }
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
        let remote_catalog = address.subject.clone().archive().catalog("index");

        let env = self.local.env();
        let remote_result = remote_catalog
            .clone()
            .get(*key)
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
                    block = %dialog_common::Blake3Hash::from(*key),
                    bytes = bytes.len(),
                    "hydrated block from remote"
                );
                // Cache locally
                let cache = self
                    .local
                    .catalog()
                    .clone()
                    .put(Buffer::from(bytes.as_slice()));
                let _: Result<(), _> = cache.perform(self.local.env()).await;
                Ok(Some(bytes))
            }
            None => Ok(None),
        }
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
