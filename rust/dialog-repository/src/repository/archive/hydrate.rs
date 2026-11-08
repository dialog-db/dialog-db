//! How a read the local archive misses hydrates: from the routes the
//! subject's open branches registered (see [`Routes`](crate::Routes)),
//! through the environment's shared [`Hydrate`].
//!
//! A peer answers [`Load`] and [`BlobLoad`] with [`load_or_hydrate`] and
//! [`load_blob_or_hydrate`], so every reader that loads through it, a
//! tree reading a node above all, hydrates the same way.

use dialog_effects::archive::prelude::CatalogExt as _;
use dialog_effects::archive::prelude::GetBlockExt as _;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, ReadBlobExt as _};
use std::sync::Arc;

use crate::RemoteSite;
use dialog_capability::Fork;
use dialog_capability::{Provider, Resume};
use dialog_common::{Blake3Hash, Buffer, ConditionalSync, Priority};
use dialog_effects::archive::prelude::ArchiveExt;
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_effects::blob::{BlobError, BlobWriter, Import as BlobImport, Read as BlobRead};
use std::fmt::Display;

use dialog_capability::Did;
use dialog_network::NetworkAddress;
pub use dialog_network::{Hydrate, HydrationLane, HydrationRequest, HydrationScheduler};

use super::routes::{HydrationRoute, Routes};
use crate::ConnectedReplica;
use dialog_artifacts::tree::{
    ArtifactNode, ArtifactNodeCache, ArtifactPreload, ArtifactRead, ArtifactWrite,
};
use dialog_common::Holds;
use dialog_effects::MethodExt as _;
use dialog_effects::archive::Load;
use dialog_effects::archive::prelude::CatalogScope;
use dialog_search_tree::DialogSearchTreeError;
use dialog_search_tree::nodes::decode as decode_node;
use dialog_storage::provider::load::{archive_error, load_block, load_spill, read_all};

/// Where a branch's local read miss hydrates from: what a miss means.
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
/// that reads like data loss instead of what it is. A
/// [local](Load::locally) load is answered from the local archive alone.
pub async fn load_or_hydrate<Env>(env: &Env, load: Load) -> Result<Option<Buffer>, ArchiveError>
where
    Env: Provider<Get> + Provider<Hydrate> + Holds + ConditionalSync + ?Sized,
{
    if let Some(block) = load_block(env, &load).await? {
        return Ok(Some(block));
    }
    let Load {
        catalog,
        digest,
        priority,
        local,
    } = load;
    // A local load asks for what the archive holds and nothing more.
    if local {
        return Ok(None);
    }
    hydrate_through(env, catalog, digest, HydrationLane::Block, priority).await
}

/// The node `read` names: from the node cache `env` holds for the catalog's
/// subject, else from `store`, else hydrated through `env`'s [`Hydrate`]
/// from the routes registered for the subject, as [`load_or_hydrate`] does
/// for a block, and decoded; what was read is kept in the cache.
///
/// This is an environment's answer to a node read ([`ArtifactRead`]): a peer answers it
/// with this over its storage. The store is the environment's node layer:
/// one that keeps nodes in a form of its own answers in that form, one that
/// keeps blocks answers through the translation layer
/// ([`dialog_search_tree::nodes`]). What arrives from a remote is a block,
/// written back into the local archive by the hydration and decoded here. A
/// [local](dialog_search_tree::nodes::Read::locally) read is answered from
/// the cache and the store alone.
pub fn read_node_or_hydrate<'a, Env, Store>(
    env: &'a Env,
    store: &'a Store,
    read: ArtifactRead,
) -> Resume<'a, Result<Option<ArtifactNode>, DialogSearchTreeError>>
where
    Env: Provider<Hydrate> + Holds + ConditionalSync + ?Sized,
    Store: Provider<ArtifactRead> + ConditionalSync + ?Sized,
{
    let cache = ArtifactNodeCache::of(env, read.catalog.subject());
    // A hit answers at once, before any of the hydrating machinery is set
    // up: nothing is allocated for it.
    if let Some(cache) = &cache
        && let Some(node) = cache.get_cached(&read.name)
    {
        return Resume::now(Ok(Some(node)));
    }
    Resume::later(async move {
        let fetch = async |name: &Blake3Hash| {
            if let Some(node) = store.execute(read.clone().locally()).await? {
                return Ok(Some(node));
            }
            if read.local {
                return Ok(None);
            }
            let block = hydrate_through(
                env,
                read.catalog.clone(),
                name.clone(),
                HydrationLane::Block,
                read.priority,
            )
            .await
            .map_err(|error| DialogSearchTreeError::Storage(error.into()))?;
            block.map(|block| decode_node(block, name)).transpose()
        };
        match cache {
            Some(cache) => cache.get_or_fetch(&read.name, fetch).await,
            None => fetch(&read.name).await,
        }
    })
}

/// Makes the node `preload` names cheap to read: reads it as
/// [`read_node_or_hydrate`] would, into the cache `env` holds, dropping the
/// result. An environment holding no cache has nowhere for it to land, so
/// nothing is read; a node the cache holds is done with at once.
pub fn preload_node_or_hydrate<'a, Env, Store>(
    env: &'a Env,
    store: &'a Store,
    preload: ArtifactPreload,
) -> Resume<'a, ()>
where
    Env: Provider<Hydrate> + Holds + ConditionalSync + ?Sized,
    Store: Provider<ArtifactRead> + ConditionalSync + ?Sized,
{
    if ArtifactNodeCache::of(env, preload.catalog.subject()).is_none() {
        return Resume::now(());
    }
    match read_node_or_hydrate(env, store, preload.read()).into_now() {
        Ok(_) => Resume::now(()),
        Err(reading) => Resume::later(async move {
            let _ = reading.await;
        }),
    }
}

/// Writes the nodes `write` carries through `store`, then keeps them in the
/// node cache `env` holds for the catalog's subject, so the next edit opens
/// them without reading them back. A write that fails keeps nothing.
pub fn write_nodes<'a, Env, Store>(
    env: &'a Env,
    store: &'a Store,
    write: ArtifactWrite,
) -> Resume<'a, Result<(), DialogSearchTreeError>>
where
    Env: Holds + ?Sized,
    Store: Provider<ArtifactWrite> + ConditionalSync + ?Sized,
{
    let cache = ArtifactNodeCache::of(env, write.catalog.subject());
    let nodes = write.nodes.clone();
    write.perform(store).map(move |written| {
        written?;
        if let Some(cache) = cache {
            for node in nodes {
                // A node the cache already holds under this hash is the same
                // bytes with whatever it has derived since: it stays.
                if cache.get_cached(node.hash()).is_none() {
                    cache.insert(node.hash().clone(), node);
                }
            }
        }
        Ok(())
    })
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
    if let Some(blob) = load_spill(env, &load).await? {
        return Ok(Some(blob));
    }
    let BlobLoad {
        archive,
        digest,
        priority,
        local,
    } = load;
    if local {
        return Ok(None);
    }
    hydrate_through(env, archive.index(), digest, HydrationLane::Blob, priority).await
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
    let local = BlobLoad::new(catalog.archive(), digest.clone());
    if let Some(blob) = load_spill(env, &local).await? {
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
    Env: Provider<BlobImport> + ConditionalSync + ?Sized + 'static,
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

    use super::{Hydrate, HydrationRequest, RemoteFallback};
    use crate::{Branch, Index, Repository, Resolved, Route, SiteAddress, Tracking};
    use dialog_effects::blob::Load as BlobLoad;
    use dialog_search_tree::DialogSearchTreeError;

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

    #[dialog_capability::provider]
    impl<P> Provider<Get> for Recording<P>
    where
        P: Provider<Get> + ConditionalSync,
    {
        async fn execute(&self, input: <Get as Command>::Input) -> <Get as Command>::Output {
            Provider::<Get>::execute(&self.inner, input).await
        }
    }

    #[dialog_capability::provider]
    impl<P> Provider<Hydrate> for Recording<P>
    where
        P: ConditionalSend + ConditionalSync,
    {
        async fn execute(&self, request: HydrationRequest) -> <Hydrate as Command>::Output {
            self.priorities.lock().push(request.priority);
            Ok(None)
        }
    }

    #[dialog_capability::provider]
    impl<P> Provider<Load> for Recording<P>
    where
        P: Provider<Get> + Holds + ConditionalSend + ConditionalSync,
    {
        async fn execute(&self, load: Load) -> <Load as Command>::Output {
            super::load_or_hydrate(self, load).await
        }
    }

    dialog_search_tree::reads_nodes_through_blocks!(
        impl[P,] Recording<P>
        where [P: Provider<Get> + Holds + ConditionalSend + ConditionalSync,]
    );

    /// The priority a read is made at reaches the env's hydration request,
    /// which is where the scheduler reads it: a load is a demand read
    /// unless told otherwise, and a tree ranked for a speculative walk
    /// carries its hint's likelihood to every load it performs, so its
    /// warming never takes a site's slot from a read someone is waiting on.
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
        let demand = Load::new(branch.archive().index(), absent.clone())
            .perform(&env)
            .await?;
        assert_eq!(demand, None);
        let speculative = Index::at(branch.archive().index(), absent.clone())
            .ranked(Priority::Maybe)
            .manifest(&env)
            .await;
        assert!(
            matches!(speculative, Err(DialogSearchTreeError::Node(_))),
            "the root is nowhere, so the read fails as a missing node: {speculative:?}"
        );

        assert_eq!(
            env.priorities.lock().clone(),
            [Priority::Demand, Priority::Maybe],
            "each read hydrates at its own priority, demand by default"
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

        let loaded = Load::new(index.clone(), block.blake3_hash().clone())
            .perform(&env)
            .await?;

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
            Load::new(index.clone(), block.blake3_hash().clone())
                .perform(&env)
                .await?,
            Some(block),
            "the remote does hold it, so only the get declined to fetch"
        );
        Ok(())
    }

    /// A load made local reads only the local archive, whatever the
    /// branches of its subject track, for both lanes: what only the remote
    /// holds reads as absent, nothing is written locally by asking, and the
    /// same load not made local does fetch it.
    #[dialog_common::test]
    async fn it_never_hydrates_a_local_load() -> Result<()> {
        let Tracked {
            env, branch, site, ..
        } = tracked().await?;
        let block = Buffer::from(b"a block a local load must not fetch".to_vec());
        let index = branch.archive().index();
        index.put(block.clone()).perform(&site).await?;
        let digest = block.blake3_hash().clone();
        let load = || Load::new(index.clone(), digest.clone());
        let spill = || BlobLoad::new(index.archive(), digest.clone());

        assert_eq!(load().locally().perform(&env).await?, None);
        assert_eq!(spill().locally().perform(&env).await?, None);
        assert_eq!(
            index.get(digest.clone()).perform(&env).await?,
            None,
            "a local load writes nothing on the way"
        );
        assert_eq!(
            load().perform(&env).await?,
            Some(block),
            "the remote does hold it, so only the local load declined to fetch"
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
        let loaded = Load::new(branch.archive().index(), absent)
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
            Load::new(index.clone(), held.blake3_hash().clone())
                .perform(&operator)
                .await?,
            Some(held),
            "a local hit succeeds regardless of the remote's availability"
        );

        // A miss is the read that needed the remote: it fails naming the
        // remote and why it is unavailable, and writes nothing.
        let absent = Buffer::from(&b"never stored"[..]).blake3_hash().clone();
        let error = Load::new(index.clone(), absent.clone())
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
            Load::new(index.clone(), block.blake3_hash().clone())
                .perform(&operator)
                .await?,
            Some(block)
        );
        let _ = Subject::from(repo.did());
        Ok(())
    }
}
