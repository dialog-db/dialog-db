//! The operator's own [`Load`] and [`Hydrate`] providers: on-demand
//! replication as an effect of the environment.
//!
//! A reader performs [`Load`] (or [`BlobLoad`] for a spilled value) on the
//! peer. The peer answers from its storage, and on a miss routes the read
//! to the remotes the subject's open branches registered (see
//! [`Routes`](dialog_repository::Routes)) and performs [`Hydrate`] on
//! itself, borrowing itself for exactly the duration of the perform. With
//! no route registered for the subject, a miss is a miss. [`Get`] is the
//! storage's alone and never hydrates. The sharing lives
//! here: concurrent hydrations of one digest anywhere in the process —
//! queries, subscriptions, transaction queries, pull — join one shared
//! fetch-and-write-back through the operator's digest-keyed flight.
//!
//! The shared future is built inside this impl from the operator's own
//! Arc-backed internals (a self handle cloned by the env's own
//! implementation, not handed to any component): the same move the Fs
//! transport's `Get` makes with its transport-level `Flight`, one layer
//! up. The scheduler holds the work weakly, so the strong shared futures
//! live only in active joiners — work makes progress exactly while some
//! `.perform` drives it (every joiner polls the shared future itself,
//! preserving the co-driving liveness rule), drops with its last
//! joiner, and can never keep the operator alive through its own field.
//!
//! The ordering lives here too: each request names its site and its
//! priority, and the scheduler admits a site's reads through one window
//! by priority (demand before speculative warming), then arrival. The
//! readers keep no widths of their own.

use dialog_artifacts::{Datum, Key as ArtifactKey, State};
use dialog_effects::blob::Load as BlobLoad;
use dialog_search_tree::nodes::{Ready, held_node};
use std::sync::Arc;

use dialog_capability::{Fork, Provider};
use dialog_common::{Buffer, Holds};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::Load;
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_repository::{
    Hydrate, HydrationRequest, RemoteSite, hydrate, load_blob_or_hydrate, load_or_hydrate,
};

use super::{Mode, Peer};
use dialog_artifacts::tree::{ArtifactNode, ArtifactPreload, ArtifactRead, ArtifactWrite};
use dialog_search_tree::DialogSearchTreeError;
use dialog_storage::provider::storage::Storage;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<Hydrate> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: Provider<Get>
        + Provider<Put>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<BlobRead>
        + Provider<BlobImport>
        + Provider<Fork<RemoteSite, BlobRead>>
        + Clone
        + ConditionalSend
        + ConditionalSync
        + 'static,
{
    async fn execute(
        &self,
        request: HydrationRequest,
    ) -> Result<Option<Arc<Vec<u8>>>, ArchiveError> {
        let digest = request.digest.clone();
        let site = request.address.clone();
        let priority = request.priority;
        // Hydration is content-addressed, so every joiner's answer is
        // identical regardless of whose route runs; errors are shared
        // as their rendering and never cached, so retry semantics are
        // unchanged.
        let outcome = self
            .hydration()
            .join(site, digest, priority, move || {
                let env = self.clone();
                async move {
                    hydrate(&env, request)
                        .await
                        .map_err(|error| error.to_string())
                }
            })
            .await;
        outcome.map_err(ArchiveError::Storage)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<Load> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: Provider<Get> + Provider<Hydrate> + Holds + ConditionalSync,
{
    async fn execute(&self, load: Load) -> Result<Option<Buffer>, ArchiveError> {
        load_or_hydrate(self, load).await
    }
}

// The node layer: a tree read through the peer asks for nodes, and the
// peer answers from the node cache it holds, its storage (the mounted
// space's archive, in whatever form it keeps nodes), or a hydration from
// the routes the subject's branches registered.
/// A node the peer's cache holds is at hand.
impl<S: Clone, M: Mode> Ready<ArtifactKey, State<Datum>> for Peer<S, M> {
    fn ready(&self, read: &ArtifactRead) -> Option<ArtifactNode> {
        held_node(self, read)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<ArtifactRead> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<ArtifactRead> + ConditionalSync,
    Self: Provider<Hydrate> + Holds + ConditionalSync,
{
    async fn execute(
        &self,
        read: ArtifactRead,
    ) -> Result<Option<ArtifactNode>, DialogSearchTreeError> {
        dialog_repository::read_node_or_hydrate(self, &self.storage, read).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<ArtifactPreload> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<ArtifactRead> + ConditionalSync,
    Self: Provider<Hydrate> + Holds + ConditionalSync,
{
    async fn execute(&self, preload: ArtifactPreload) {
        dialog_repository::preload_node_or_hydrate(self, &self.storage, preload).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<ArtifactWrite> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<ArtifactWrite> + ConditionalSync,
    Self: Holds + ConditionalSync,
{
    async fn execute(&self, write: ArtifactWrite) -> Result<(), DialogSearchTreeError> {
        dialog_repository::write_nodes(self, &self.storage, write).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<BlobLoad> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: Provider<Get> + Provider<BlobRead> + Provider<Hydrate> + Holds + ConditionalSync,
{
    async fn execute(&self, load: BlobLoad) -> Result<Option<Buffer>, ArchiveError> {
        load_blob_or_hydrate(self, load).await
    }
}
