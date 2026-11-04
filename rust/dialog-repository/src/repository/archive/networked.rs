use dialog_effects::archive::prelude::CatalogExt as _;
use dialog_effects::archive::prelude::GetBlockExt as _;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, ReadBlobExt as _};
use std::sync::Arc;

use crate::RemoteSite;
use async_trait::async_trait;
use dialog_artifacts::tree::ArtifactNodeCache;
use dialog_artifacts::{Datum, DialogArtifactsError, Key, LoadBlob, State};
use dialog_capability::Fork;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer, ConditionalSync, Priority};
use dialog_effects::archive::prelude::ArchiveExt;
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_effects::blob::{BlobError, BlobWriter, Import as BlobImport, Read as BlobRead};
use dialog_search_tree::{DialogSearchTreeError, LoadBlock, Nodes};
use std::fmt::Display;

use dialog_capability::Did;
use dialog_network::NetworkAddress;
pub use dialog_network::{Hydrate, HydrationLane, HydrationRequest, HydrationScheduler};

use super::local::{LocalIndex, archive_error, read_all};
use super::routes::{HydrationRoute, Routes};
use crate::ConnectedReplica;
use dialog_capability::Subject;
use dialog_common::Holds;
use dialog_effects::MethodExt as _;
use dialog_effects::archive::Load;
use dialog_effects::archive::prelude::CatalogScope;
use dialog_effects::archive::prelude::{ArchiveScope, GetExt as _};
use dialog_effects::blob::prelude::BlobReadExt as _;
use dialog_storage::provider::load::{load_block, load_spill};

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
#[derive(Clone, Debug)]
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

/// Content-addressed index that reads through the environment's loads.
///
/// Reads [`Load`] and [`BlobLoad`] on the environment it borrows, which
/// answers from the local archive and, on a miss, hydrates from the routes
/// the subject's open branches registered (see [`Routes`](crate::Routes)).
/// It never writes: a commit writes what its batch staged.
pub struct NetworkedIndex<'a, Env: ?Sized> {
    local: LocalIndex<'a, Env>,
    priority: Priority,
    /// Whether reads see only the local archive (see
    /// [`locally`](Self::locally)).
    locally: bool,
}

impl<Env: ?Sized> Clone for NetworkedIndex<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            local: self.local.clone(),
            priority: self.priority,
            locally: self.locally,
        }
    }
}

impl<'a, Env: ?Sized> NetworkedIndex<'a, Env> {
    /// Create a networked index over `index`. Tree nodes read through it
    /// are kept in `nodes` (see [`LocalIndex::new`]).
    pub fn new(env: &'a Env, index: CatalogScope, nodes: ArtifactNodeCache) -> Self {
        Self {
            local: LocalIndex::new(env, index, nodes),
            priority: Priority::Demand,
            locally: false,
        }
    }

    /// Read only what the local archive holds: every read performs `Get`
    /// and never hydrates. For a reader whose answer is what this replica
    /// holds, such as an export that does not download.
    pub fn locally(mut self, locally: bool) -> Self {
        self.locally = locally;
        self
    }

    /// Rank the hydrations this index's reads cause as `priority` at the
    /// remote site.
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

    /// The local index this reads first, and writes through.
    pub(crate) fn local(&self) -> &LocalIndex<'a, Env> {
        &self.local
    }
}

/// What a [`NetworkedIndex`] reads through, as one trait object.
///
/// The tree code a read or a commit runs is generic over its store, and
/// the store names the environment it reads through. An index built over
/// each concrete environment therefore copies that tree code once per
/// environment type, in every crate that names one: a peer's storage and
/// mode, and each wrapper an embedder puts around it. Built over this
/// object instead, the copies collapse into one.
///
/// The index's own impls do not ask `Env: 'static`, and must not. The
/// object's lifetime is a region, which rustc erases when it checks that
/// a future holding the index is `Send` for every lifetime it borrows;
/// an outlives bound on it then cannot be shown, and every provider that
/// commits stops being `Send` (rust-lang/rust#100013).
pub(crate) trait Reads:
    Provider<Get> + Provider<BlobRead> + Provider<Load> + Provider<BlobLoad> + ConditionalSync
{
}

impl<T> Reads for T where
    T: Provider<Get> + Provider<BlobRead> + Provider<Load> + Provider<BlobLoad> + ConditionalSync
{
}

impl<'a> NetworkedIndex<'a, dyn Reads> {
    /// [`NetworkedIndex::new`], reading through `env` as a [`Reads`]
    /// object, so every environment shares one copy of the tree code
    /// that reads through the index.
    pub(crate) fn erased<Env>(env: &'a Env, index: CatalogScope, nodes: ArtifactNodeCache) -> Self
    where
        Env: Reads + 'static,
    {
        Self::new(env, index, nodes)
    }
}

impl<Env> NetworkedIndex<'_, Env>
where
    Env: ?Sized + Provider<Get> + Provider<Load> + ConditionalSync,
{
    /// The block stored under `hash`: the local archive's copy, or else the
    /// one the environment hydrates from where the subject's blocks live.
    /// Unverified; readers outside the crate load through [`LoadBlock`] or
    /// [`LoadBlob`], which check it.
    pub(crate) async fn load(&self, hash: &Blake3Hash) -> Result<Option<Buffer>, ArchiveError> {
        if self.locally {
            return self.local.load(hash).await;
        }
        Load::new(self.local.catalog().get(hash.clone()))
            .with_priority(self.priority)
            .perform(self.local.env())
            .await
    }
}

impl<Env> NetworkedIndex<'_, Env>
where
    Env: ?Sized + Provider<Get> + Provider<BlobRead> + Provider<BlobLoad> + ConditionalSync,
{
    /// The spilled value stored under `hash`: the local copy (blob store,
    /// then the block catalog for values spilled before they moved to
    /// blobs), or else the one the environment hydrates from where the
    /// subject's blobs live.
    pub async fn load_blob(&self, hash: &Blake3Hash) -> Result<Option<Buffer>, ArchiveError> {
        if self.locally {
            return self.local.load_blob(hash).await;
        }
        BlobLoad::new(self.local.catalog().archive().blob().read(hash.clone()))
            .with_priority(self.priority)
            .perform(self.local.env())
            .await
    }
}

/// The block `load` names: from `env`'s local archive, or else hydrated
/// through `env`'s [`Hydrate`] from the routes registered for its subject
/// (see [`Routes`](crate::Routes)), and written back locally first.
///
/// This is an environment's [`Load`]: a peer answers it with this, so
/// every reader that loads a block through the peer hydrates the same way,
/// sharing in-flight work through the peer's [`Hydrate`]. With no route
/// registered for the subject, a local miss is an ordinary `None`. A route
/// whose remote could not be loaded fails the miss naming it: silently
/// returning `None` would surface downstream as a bare "Block not found"
/// that reads like data loss instead of what it is.
pub async fn load_or_hydrate<Env>(env: &Env, load: Load) -> Result<Option<Buffer>, ArchiveError>
where
    Env: Provider<Get> + Provider<Hydrate> + Holds + ConditionalSync + ?Sized,
{
    let digest = load.capability.digest().clone();
    let catalog = CatalogScope::new(
        Subject::from(load.capability.subject().clone()),
        load.capability.catalog(),
    );
    let priority = load.priority;
    if let Some(block) = load_block(env, load).await? {
        return Ok(Some(block));
    }
    hydrate_through(env, catalog, digest, HydrationLane::Block, priority).await
}

/// The spilled value `load` names: from `env`'s local blob store (or the
/// block catalog, for a value spilled before values moved to blobs), or
/// else hydrated from the routes registered for its subject, as
/// [`load_or_hydrate`] does for a block.
pub async fn load_blob_or_hydrate<Env>(
    env: &Env,
    load: BlobLoad,
) -> Result<Option<Buffer>, ArchiveError>
where
    Env: Provider<Get> + Provider<BlobRead> + Provider<Hydrate> + Holds + ConditionalSync + ?Sized,
{
    let digest = load.capability.digest().clone();
    let catalog = ArchiveScope::new(Subject::from(load.capability.subject().clone())).index();
    let priority = load.priority;
    if let Some(blob) = load_spill(env, load).await? {
        return Ok(Some(blob));
    }
    hydrate_through(env, catalog, digest, HydrationLane::Blob, priority).await
}

/// Fetch `hash` through `env`'s [`Hydrate`] into `lane`'s local store,
/// from the first route registered for `catalog`'s subject that holds it.
async fn hydrate_through<Env>(
    env: &Env,
    catalog: CatalogScope,
    hash: Blake3Hash,
    lane: HydrationLane,
    priority: Priority,
) -> Result<Option<Buffer>, ArchiveError>
where
    Env: Provider<Hydrate> + Holds + ConditionalSync + ?Sized,
{
    let routes = Routes::of(env).to(catalog.subject());
    let mut unavailable = None;
    let mut failure = None;
    for route in routes {
        let (address, subject) = match route {
            HydrationRoute::Remote { address, subject } => (address, subject),
            HydrationRoute::Unavailable { remote, reason } => {
                unavailable.get_or_insert((remote, reason));
                continue;
            }
        };
        // The routing is resolved here; the fetch, the re-check that
        // guards it, the local write-back, and any sharing of the work
        // with concurrent readers of the same digest are the env's own
        // effect (see [`Hydrate`]).
        let request = HydrationRequest {
            address,
            subject,
            catalog: catalog.clone(),
            digest: hash.clone(),
            lane,
            priority,
        };
        match Provider::<Hydrate>::execute(env, request).await {
            Ok(Some(bytes)) => return Ok(Some(Buffer::from(bytes.as_ref().clone()))),
            Ok(None) => {}
            Err(error) => {
                failure.get_or_insert(ArchiveError::Storage(format!(
                    "block {hash} is not in the local archive and could not hydrate on \
                     lane {lane:?}: {error}"
                )));
            }
        }
    }
    if let Some(failure) = failure {
        return Err(failure);
    }
    // The block is not local and a remote it could hydrate from could not
    // be loaded: fail with the cause rather than read as absent.
    if let Some((remote, reason)) = unavailable {
        return Err(ArchiveError::Storage(format!(
            "block {hash} is not in the local archive and the tracked \
             remote \"{remote}\" it would hydrate from is unavailable: \
             {reason}"
        )));
    }
    Ok(None)
}

/// Tree nodes load from the local archive, falling back to the tracked
/// remote.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<LoadBlock> for NetworkedIndex<'_, Env>
where
    Env: ?Sized + Provider<Get> + Provider<Load> + ConditionalSync,
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

/// Tree nodes are read through the node cache the local index holds, then
/// the local archive, then the tracked remote.
impl<Env> Nodes<Key, State<Datum>> for NetworkedIndex<'_, Env>
where
    Env: ?Sized + Provider<Get> + Provider<Load> + ConditionalSync,
{
    fn kept(&self) -> Option<&ArtifactNodeCache> {
        Some(self.local.nodes())
    }
}

/// Spilled values load from the local blob store (falling back to the
/// block catalog for values spilled before they moved to blobs), and
/// hydrate from the tracked remote's blob store.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<LoadBlob> for NetworkedIndex<'_, Env>
where
    Env: ?Sized + Provider<Get> + Provider<BlobRead> + Provider<BlobLoad> + ConditionalSync,
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
    let local = BlobLoad::new(catalog.archive().blob().read(digest.clone()));
    if let Some(blob) = load_spill(env, local).await? {
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

    use crate::helpers::{connect, test_repo};
    use std::sync::Arc;

    use anyhow::Result;
    use dialog_capability::{Command, Provider, Subject};
    use dialog_common::{Buffer, ConditionalSend, ConditionalSync, Held, Holds, Priority};
    use dialog_credentials::Credential;
    use dialog_effects::archive::{ArchiveError, Get, Load, Put};
    use dialog_effects::credential::prelude::*;
    use dialog_effects::storage::{Directory, Location};
    use dialog_peer::helpers::{test_session_with_peer, unique_name};
    use dialog_peer::{Peer, Session};
    use dialog_remote_fs::FsAddress;
    use dialog_storage::provider::FileSystem;
    use dialog_storage::provider::storage::VolatileSpace;
    use dialog_storage::resource::Resource as _;
    use parking_lot::Mutex;

    use super::{Hydrate, HydrationRequest, NetworkedIndex, RemoteFallback};
    use crate::{Branch, Repository, Resolved, Route, SiteAddress, Tracking};

    /// An env that answers every hydration with "not found" and keeps the
    /// priority each request carried, answering a load the way a peer
    /// does: through its own `Get` and `Hydrate`.
    struct Recording<P> {
        inner: P,
        priorities: Arc<Mutex<Vec<Priority>>>,
    }

    impl<P: Holds> Holds for Recording<P> {
        fn held(&self, key: &str) -> Option<Held> {
            self.inner.held(key)
        }

        fn hold(&self, key: String, handle: Held) {
            self.inner.hold(key, handle)
        }

        fn held_or(&self, key: &str, make: &dyn Fn() -> Held) -> Held {
            self.inner.held_or(key, make)
        }
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
    impl<P> Provider<Hydrate> for Recording<P>
    where
        P: ConditionalSend + ConditionalSync,
    {
        async fn execute(&self, request: HydrationRequest) -> <Hydrate as Command>::Output {
            self.priorities.lock().push(request.priority);
            Ok(None)
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl<P> Provider<Load> for Recording<P>
    where
        P: Provider<Get> + Holds + ConditionalSync,
    {
        async fn execute(&self, load: Load) -> <Load as Command>::Output {
            super::load_or_hydrate(self, load).await
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
        let _loan = super::super::routes::lend(
            branch.routes(),
            branch.of(),
            &RemoteFallback::Remote(origin),
        );
        let env = Recording {
            inner: operator,
            priorities: Arc::new(Mutex::new(Vec::new())),
        };

        let absent = Buffer::from(&b"never stored"[..]).blake3_hash().clone();
        let demand = NetworkedIndex::new(&env, branch.archive().index(), branch.node_cache());
        assert_eq!(demand.load(&absent).await?, None);
        let speculative = NetworkedIndex::new(&env, branch.archive().index(), branch.node_cache())
            .with_priority(Priority::Maybe);
        assert_eq!(speculative.load(&absent).await?, None);

        assert_eq!(
            env.priorities.lock().clone(),
            [Priority::Demand, Priority::Maybe],
            "each index hydrates at its own priority, demand by default"
        );
        Ok(())
    }

    /// A repository whose blocks are also held in a directory a remote
    /// serves, and a branch of it tracking that remote: a partial replica,
    /// as a branch that adopted a head it never read is.
    struct Tracked<Env> {
        env: Env,
        repo: Repository<Credential>,
        branch: Branch,
        /// The site holding what the replica does not.
        site: FileSystem,
    }

    async fn tracked() -> Result<Tracked<Peer<VolatileSpace, Session>>> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let location = Location::new(Directory::Temp, unique_name("tracked-remote"));
        let site = FileSystem::open(&location).await?;
        repo.did()
            .credential()
            .key("self")
            .save(repo.credential().clone())
            .perform(&site)
            .await?;
        let origin = connect(
            "origin",
            SiteAddress::Fs(FsAddress::new(location)),
            repo.did(),
            &operator,
        )
        .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let remote = origin.branch("main").open().perform(&operator).await?;
        branch.set_upstream(remote).perform(&operator).await?;
        Ok(Tracked {
            env: operator,
            repo,
            branch,
            site,
        })
    }

    /// A block the local archive lacks and the tracked remote holds is
    /// loaded from the remote through `Load`, and written back locally
    /// first: the next plain read finds it.
    #[dialog_common::test]
    async fn it_hydrates_a_missing_block_from_the_remote_a_branch_tracks() -> Result<()> {
        let Tracked {
            env,
            repo,
            branch,
            site,
        } = tracked().await?;
        let block = Buffer::from(b"a block only the remote holds".to_vec());
        let index = branch.archive().index();
        index.put(block.clone()).perform(&site).await?;
        let get = || index.get(block.blake3_hash().clone());
        assert_eq!(get().perform(&env).await?, None, "the replica lacks it");

        let loaded = Load::new(get()).perform(&env).await?;

        assert_eq!(loaded, Some(block.clone()), "the load hydrates it");
        assert_eq!(
            get().perform(&env).await?.map(Buffer::from),
            Some(block),
            "hydration wrote it back locally"
        );
        drop(repo);
        Ok(())
    }

    /// A `Get` reads only the local archive, whatever the branches of its
    /// subject track: the block the remote holds is not fetched, and
    /// nothing is written locally by asking.
    #[dialog_common::test]
    async fn it_never_hydrates_a_get() -> Result<()> {
        let Tracked {
            env, branch, site, ..
        } = tracked().await?;
        let block = Buffer::from(b"a block a get must not fetch".to_vec());
        let index = branch.archive().index();
        index.put(block.clone()).perform(&site).await?;
        let get = || index.get(block.blake3_hash().clone());

        assert_eq!(get().perform(&env).await?, None, "a get does not hydrate");
        assert_eq!(
            get().perform(&env).await?,
            None,
            "and wrote nothing locally on the way"
        );
        assert_eq!(
            Load::new(get()).perform(&env).await?,
            Some(block),
            "the remote does hold it, so only the get declined to fetch"
        );
        Ok(())
    }

    /// With no branch of the subject tracking a remote, a miss is a miss:
    /// the load answers as the local read does.
    #[dialog_common::test]
    async fn it_reads_a_missing_block_as_absent_when_nothing_is_tracked() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let absent = Buffer::from(&b"never stored"[..]).blake3_hash().clone();
        let loaded = Load::new(branch.archive().index().get(absent))
            .perform(&operator)
            .await?;

        assert_eq!(loaded, None, "an untracked branch's miss is not an error");
        Ok(())
    }

    /// A tracked remote that failed to load must not silently degrade the
    /// read to local-only. A read the local archive satisfies still
    /// succeeds — a full replica keeps working offline — but a miss is
    /// the exact case that needed the remote, and it fails naming the
    /// remote and why it is unavailable, not as a bare "Block not found"
    /// with the cause erased.
    #[dialog_common::test]
    async fn it_fails_a_miss_loudly_when_the_tracked_remote_is_unavailable() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let target = dialog_artifacts::Entity::new()?;
        let reason = "no credential saved for the site";
        branch
            .tracking()
            .checkpoint()
            .publish(
                Tracking {
                    resolved: Some(Resolved {
                        at: None,
                        pulls: vec![Route::Unreachable {
                            target: target.clone(),
                            reason: reason.into(),
                        }],
                        pushes: vec![],
                    }),
                    synced: vec![],
                },
                &operator,
            )
            .await?;

        // A locally held block reads back: unavailability of the remote
        // must not cost a replica anything it already holds.
        let index = branch.archive().index();
        let held = Buffer::from(b"locally held block".to_vec());
        index.put(held.clone()).perform(&operator).await?;
        assert_eq!(
            Load::new(index.get(held.blake3_hash().clone()))
                .perform(&operator)
                .await?,
            Some(held),
            "a local hit succeeds regardless of the remote's availability"
        );

        // A miss is the read that needed the remote: it fails naming the
        // remote and why it is unavailable, and writes nothing.
        let absent = Buffer::from(&b"never stored"[..]).blake3_hash().clone();
        let error = Load::new(index.get(absent.clone()))
            .perform(&operator)
            .await
            .expect_err("a miss with an unavailable tracked remote must fail loudly");
        let expected = format!(
            "block {absent} is not in the local archive and the tracked remote \"{target}\" \
             it would hydrate from is unavailable: {reason}"
        );
        assert!(
            matches!(&error, ArchiveError::Storage(message) if *message == expected),
            "the failure carries the remote and the load failure as cause: {error}"
        );
        assert_eq!(index.get(absent).perform(&operator).await?, None);
        Ok(())
    }

    /// The store an untracked test subject reads from answers a load as the
    /// read, and a put through the operator lands where it reads.
    #[dialog_common::test]
    async fn it_loads_a_local_block_as_the_store_holds_it() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let index = repo
            .branch("main")
            .open()
            .perform(&operator)
            .await?
            .archive()
            .index();
        let block = Buffer::from(b"local".to_vec());
        Provider::<Put>::execute(&operator, index.put(block.clone())).await?;
        assert_eq!(
            Load::new(index.get(block.blake3_hash().clone()))
                .perform(&operator)
                .await?,
            Some(block)
        );
        let _ = Subject::from(repo.did());
        Ok(())
    }
}
