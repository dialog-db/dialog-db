use dialog_effects::archive::prelude::CatalogExt as _;
use dialog_effects::archive::prelude::GetBlockExt as _;
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, ReadBlobExt as _};
use std::sync::Arc;

use crate::RemoteSite;
use async_trait::async_trait;
use dialog_artifacts::{DialogArtifactsError, LoadBlob};
use dialog_capability::Fork;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer, ConditionalSync, Priority};
use dialog_effects::archive::prelude::ArchiveExt;
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_effects::blob::{BlobError, BlobWriter, Import as BlobImport, Read as BlobRead};
use dialog_search_tree::{DialogSearchTreeError, LoadBlock};
use std::fmt::Display;

use dialog_capability::Did;
use dialog_network::NetworkAddress;
pub use dialog_network::{Hydrate, HydrationLane, HydrationRequest, HydrationScheduler};

use super::local::{LocalIndex, archive_error, read_all};
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
pub struct NetworkedIndex<'a, Env: ?Sized> {
    local: LocalIndex<'a, Env>,
    remote: RemoteFallback,
    priority: Priority,
}

impl<Env: ?Sized> Clone for NetworkedIndex<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            remote: self.remote.clone(),
            priority: self.priority,
        }
    }
}

impl<'a, Env: ?Sized> NetworkedIndex<'a, Env> {
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

impl<Env> NetworkedIndex<'_, Env>
where
    Env: ?Sized + Provider<Get> + Provider<Hydrate> + ConditionalSync + 'static,
{
    /// The block stored under `hash`: the local archive's copy, or else the
    /// tracked remote's, hydrated into the local archive as it is read.
    /// Unverified; readers outside the crate load through [`LoadBlock`] or
    /// [`LoadBlob`], which check it.
    pub(crate) async fn load(&self, hash: &Blake3Hash) -> Result<Option<Buffer>, ArchiveError> {
        if let Some(block) = self.local.load(hash).await? {
            return Ok(Some(block));
        }
        self.hydrate(hash, HydrationLane::Block).await
    }

    /// Fetch `hash` from the tracked remote through the env's [`Hydrate`],
    /// into `lane`'s local store.
    async fn hydrate(
        &self,
        hash: &Blake3Hash,
        lane: HydrationLane,
    ) -> Result<Option<Buffer>, ArchiveError> {
        let remote = match &self.remote {
            RemoteFallback::Remote(remote) => remote,
            RemoteFallback::None => return Ok(None),
            // The block is not local and the tracked remote — the only
            // place it could hydrate from — could not be loaded. Failing
            // here, with the cause, is the contract: silently returning
            // `None` would surface downstream as a bare "Block not found"
            // that reads like data loss instead of what it is.
            RemoteFallback::Unavailable { remote, reason } => {
                return Err(ArchiveError::Storage(format!(
                    "block {hash} is not in the local archive and the tracked \
                     remote \"{remote}\" it would hydrate from is unavailable: \
                     {reason}"
                )));
            }
        };

        // The routing is resolved here; the fetch, the re-check that
        // guards it, the local write-back, and any sharing of the work
        // with concurrent readers of the same digest are the env's own
        // effect (see [`Hydrate`]).
        let route = remote.address();
        let request = HydrationRequest {
            address: route.address,
            subject: route.subject,
            catalog: self.local.catalog().clone(),
            digest: hash.clone(),
            lane,
            priority: self.priority,
        };
        #[cfg(not(target_arch = "wasm32"))]
        {
            eprintln!(
                "dialog: hydrating block {hash} on lane {lane:?}\n{}",
                std::backtrace::Backtrace::force_capture()
            );
        }
        let hydrated = Provider::<Hydrate>::execute(self.local.env(), request)
            .await
            .map_err(|error| {
                ArchiveError::Storage(format!(
                    "block {hash} is not in the local archive and could not hydrate on \
                     lane {lane:?}: {error}"
                ))
            })?;
        Ok(hydrated.map(|bytes| Buffer::from(bytes.as_ref().clone())))
    }
}

impl<Env> NetworkedIndex<'_, Env>
where
    Env:
        ?Sized + Provider<Get> + Provider<BlobRead> + Provider<Hydrate> + ConditionalSync + 'static,
{
    /// The spilled value stored under `hash`: the local copy (blob store,
    /// then the block catalog for values spilled before they moved to
    /// blobs), or else the tracked remote's, hydrated into the local blob
    /// store as it is read.
    pub async fn load_blob(&self, hash: &Blake3Hash) -> Result<Option<Buffer>, ArchiveError> {
        if let Some(blob) = self.local.load_blob(hash).await? {
            return Ok(Some(blob));
        }
        self.hydrate(hash, HydrationLane::Blob).await
    }
}

/// Tree nodes load from the local archive, falling back to the tracked
/// remote.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<LoadBlock> for NetworkedIndex<'_, Env>
where
    Env: ?Sized + Provider<Get> + Provider<Hydrate> + ConditionalSync + 'static,
{
    async fn execute(
        &self,
        LoadBlock { hash }: LoadBlock,
    ) -> Result<Option<Buffer>, DialogSearchTreeError> {
        self.load(&hash)
            .await
            .map_err(|error| DialogSearchTreeError::Storage(error.into()))
    }
}

/// Spilled values load from the local blob store (falling back to the
/// block catalog for values spilled before they moved to blobs), and
/// hydrate from the tracked remote's blob store.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<LoadBlob> for NetworkedIndex<'_, Env>
where
    Env:
        ?Sized + Provider<Get> + Provider<BlobRead> + Provider<Hydrate> + ConditionalSync + 'static,
{
    async fn execute(
        &self,
        LoadBlob { hash }: LoadBlob,
    ) -> Result<Option<Buffer>, DialogArtifactsError> {
        Ok(self.load_blob(&hash).await?)
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
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<BlobRead>
        + Provider<BlobImport>
        + Provider<Fork<RemoteSite, BlobRead>>
        + ConditionalSync
        + 'static,
{
    let HydrationRequest {
        address,
        subject,
        catalog,
        digest,
        lane,
        priority: _,
    } = request;
    if lane == HydrationLane::Blob {
        return hydrate_blob(env, address, subject, catalog, digest).await;
    }

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

/// The [`HydrationLane::Blob`] half of [`hydrate`]: a spilled value, re-checked
/// locally (blob store, then the block catalog for values spilled before
/// they moved to blobs), then fetched from the remote the same way, and
/// written back into the local blob store.
async fn hydrate_blob<Env>(
    env: &Env,
    address: NetworkAddress,
    subject: Did,
    catalog: CatalogScope,
    digest: Blake3Hash,
) -> Result<Option<Arc<Vec<u8>>>, ArchiveError>
where
    Env: Provider<Get>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<BlobRead>
        + Provider<BlobImport>
        + Provider<Fork<RemoteSite, BlobRead>>
        + ConditionalSync
        + 'static,
{
    let local = LocalIndex::new(env, catalog.clone());
    if let Some(blob) = local.load_blob(&digest).await? {
        return Ok(Some(Arc::new(blob.into_vec())));
    }

    let remote_blob = subject
        .clone()
        .reader()
        .archive()
        .blob()
        .read(digest.clone())
        .fork(&address)
        .perform(env)
        .await;
    let bytes = match remote_blob {
        Ok(reader) => read_all(reader).await.map_err(archive_error)?,
        Err(BlobError::NotFound(_)) => {
            match subject
                .reader()
                .archive()
                .catalog("index")
                .get(digest.clone())
                .fork(&address)
                .perform(env)
                .await?
            {
                Some(bytes) => bytes,
                None => return Ok(None),
            }
        }
        Err(error) => return Err(archive_error(error)),
    };

    tracing::debug!(
        target: "dialog::sync::hydrate",
        blob = %digest,
        bytes = bytes.len(),
        "hydrated spilled value from remote"
    );
    // The import verifies the bytes against the digest; a failed write-back
    // is not a failed read (the reader verifies what it loads), only a
    // future remote round trip, so it is traced rather than raised.
    if let Err(error) = write_blob(env, &catalog, &digest, &bytes).await {
        tracing::debug!(
            target: "dialog::sync::hydrate",
            blob = %digest,
            %error,
            "failed to cache hydrated spilled value locally"
        );
    }
    Ok(Some(Arc::new(bytes)))
}

/// Write `bytes` into `catalog`'s archive's blob store under `digest`,
/// verified against it.
pub(crate) async fn write_blob<Env>(
    env: &Env,
    catalog: &CatalogScope,
    digest: &Blake3Hash,
    bytes: &[u8],
) -> Result<(), BlobError>
where
    Env: Provider<BlobImport> + ConditionalSync + 'static,
{
    let sink = catalog
        .archive()
        .blob()
        .import(digest.clone(), bytes.len() as u64)
        .perform(env)
        .await?;
    fill_import(sink, digest, bytes).await
}

/// Write `bytes` through an import `sink` opened for `digest` and commit
/// it. The store verifies the bytes against the declared digest and keeps
/// nothing on a mismatch; the digest the sink commits is checked here too,
/// so a store that answers with another one is refused rather than trusted.
pub(crate) async fn fill_import(
    mut sink: BlobWriter,
    digest: &Blake3Hash,
    bytes: &[u8],
) -> Result<(), BlobError> {
    sink.write_all(bytes).await?;
    let committed = sink.finish().await?;
    if &committed != digest {
        return Err(BlobError::DigestMismatch {
            expected: digest.to_string(),
            actual: committed.to_string(),
        });
    }
    Ok(())
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
        let origin = connect("origin", site, repo.did(), &operator).await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let env = Recording {
            inner: operator,
            priorities: Arc::new(Mutex::new(Vec::new())),
        };

        let absent = Buffer::from(&b"never stored"[..]).blake3_hash().clone();
        let demand = NetworkedIndex::new(
            &env,
            branch.archive().index(),
            RemoteFallback::Remote(origin.clone()),
        );
        assert_eq!(demand.load(&absent).await?, None);
        let speculative = NetworkedIndex::new(
            &env,
            branch.archive().index(),
            RemoteFallback::Remote(origin),
        )
        .with_priority(Priority::Maybe);
        assert_eq!(speculative.load(&absent).await?, None);

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

        let index = NetworkedIndex::new(
            &operator,
            branch.archive().index(),
            RemoteFallback::Unavailable {
                remote: "origin".into(),
                reason: "no credential saved for the site".into(),
            },
        );

        // A locally held block reads back: unavailability of the remote
        // must not cost a replica anything it already holds.
        let held = Buffer::from(b"locally held block".to_vec());
        branch
            .archive()
            .index()
            .put(held.clone())
            .perform(&operator)
            .await?;
        assert_eq!(
            index.load(held.blake3_hash()).await?,
            Some(held),
            "a local hit succeeds regardless of the remote's availability"
        );

        // A miss is the read that needed the remote: it fails naming the
        // remote and why it is unavailable.
        let absent_key = Buffer::from(&b"never stored"[..]).blake3_hash().clone();
        let error = index
            .load(&absent_key)
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
            local_only.load(&absent_key).await?,
            None,
            "an untracked branch's miss is not an error"
        );

        Ok(())
    }
}
