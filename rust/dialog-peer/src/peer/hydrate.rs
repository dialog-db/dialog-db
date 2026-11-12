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

use dialog_effects::blob::Load as BlobLoad;
use dialog_search_tree::nodes;
use std::sync::Arc;

use dialog_capability::{Fork, Provider, Resume};
use dialog_common::{Buffer, Holds};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::Load;
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_repository::{
    Hydrate, HydrationRequest, RemoteSite, hydrate, load_blob_or_hydrate, load_or_hydrate,
};

use super::{Mode, Peer};
use dialog_corpus::tree::CorpusNode;
use dialog_search_tree::DialogSearchTreeError;
use dialog_storage::provider::storage::Storage;

#[dialog_capability::provider]
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

#[dialog_capability::provider]
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
/// A node the peer's cache holds is answered at once.
impl<S, M: Mode> Provider<nodes::Read<CorpusNode>> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<nodes::Read<CorpusNode>> + ConditionalSync,
    Self: Provider<Hydrate> + Holds + ConditionalSync,
{
    fn execute(
        &self,
        read: nodes::Read<CorpusNode>,
    ) -> Resume<'_, Result<Option<CorpusNode>, DialogSearchTreeError>> {
        dialog_repository::read_node_or_hydrate(self, &self.storage, read)
    }
}

impl<S, M: Mode> Provider<nodes::Preload<CorpusNode>> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<nodes::Read<CorpusNode>> + ConditionalSync,
    Self: Provider<Hydrate> + Holds + ConditionalSync,
{
    fn execute(&self, preload: nodes::Preload<CorpusNode>) -> Resume<'_, ()> {
        dialog_repository::preload_node_or_hydrate(self, &self.storage, preload)
    }
}

impl<S, M: Mode> Provider<nodes::Write<CorpusNode>> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<nodes::Write<CorpusNode>> + ConditionalSync,
    Self: Holds + ConditionalSync,
{
    fn execute(
        &self,
        write: nodes::Write<CorpusNode>,
    ) -> Resume<'_, Result<(), DialogSearchTreeError>> {
        dialog_repository::write_nodes(self, &self.storage, write)
    }
}

#[dialog_capability::provider]
impl<S, M: Mode> Provider<BlobLoad> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: Provider<Get> + Provider<BlobRead> + Provider<Hydrate> + Holds + ConditionalSync,
{
    async fn execute(&self, load: BlobLoad) -> Result<Option<Buffer>, ArchiveError> {
        load_blob_or_hydrate(self, load).await
    }
}
