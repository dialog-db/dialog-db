use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use dialog_capability::{Capability, Provider, Subject, did};
use dialog_common::{Blake3Hash, Buffer, Held, Holdings, Holds};
use dialog_effects::archive::prelude::{ArchiveScope, CatalogScope};
use dialog_effects::archive::{ArchiveError, Get, Import, Load};
use dialog_effects::blob;
use dialog_storage::provider::Volatile;
use futures_util::FutureExt as _;
use parking_lot::Mutex;

use crate::{Delta, NodeCache, PersistentNode};

/// The catalog the trees of this crate's tests, benches and examples live
/// in: the default catalog of a fixed test subject.
pub fn test_catalog() -> CatalogScope {
    ArchiveScope::new(Subject::from(did!("key:zSearchTreeTest"))).index()
}

/// Blocks of one catalog in a [`Volatile`] store: the archive the tests,
/// benches and examples of this crate read their trees through.
///
/// A tree asks for its nodes ([`crate::nodes`]); these answer through the
/// translation layer over the volatile store, as a bare store would, and
/// answer [`Load`], [`Get`] and [`Import`] from it too. They hold nothing,
/// so every node read reaches the blocks; [`Holding`] gives them a node
/// cache.
///
/// Writing is synchronous here for the tests' convenience: a volatile
/// write completes the moment it is performed. Clones share the blocks.
#[derive(Clone, Debug)]
pub struct Blocks {
    store: Volatile,
    catalog: CatalogScope,
}

impl Default for Blocks {
    fn default() -> Self {
        Self::new()
    }
}

impl Blocks {
    /// No blocks yet, in [`test_catalog`].
    pub fn new() -> Self {
        Self::in_catalog(test_catalog())
    }

    /// No blocks yet, in `catalog`.
    pub fn in_catalog(catalog: CatalogScope) -> Self {
        Self {
            store: Volatile::new(),
            catalog,
        }
    }

    /// The catalog these blocks are written to.
    pub fn catalog(&self) -> &CatalogScope {
        &self.catalog
    }

    /// The volatile store underneath.
    pub fn volatile(&self) -> &Volatile {
        &self.store
    }

    /// Keeps `block` under its content hash.
    pub fn store(&self, block: Buffer) {
        self.catalog
            .put(block)
            .perform(&self.store)
            .now_or_never()
            .expect("a volatile write completes when performed")
            .expect("a volatile write does not fail");
    }

    /// Keeps the block of every node `delta` holds, emptying it.
    pub fn flush<Key, Value>(&self, delta: &mut Delta<Blake3Hash, PersistentNode<Key, Value>>) {
        let blocks: Vec<Buffer> = delta
            .flush()
            .map(|(_, node)| node.buffer().clone())
            .collect();
        if blocks.is_empty() {
            return;
        }
        self.catalog
            .import(blocks)
            .perform(&self.store)
            .now_or_never()
            .expect("a volatile write completes when performed")
            .expect("a volatile write does not fail");
    }

    /// The block stored under `hash`, if any.
    pub fn get(&self, hash: &Blake3Hash) -> Option<Buffer> {
        self.catalog
            .get(hash.clone())
            .perform(&self.store)
            .now_or_never()
            .expect("a volatile read completes when performed")
            .expect("a volatile read does not fail")
            .map(Buffer::from)
    }
}

/// Holds nothing: a tree read through these has no node cache.
impl Holds for Blocks {
    fn held(&self, _key: &str) -> Option<Held> {
        None
    }

    fn hold(&self, _key: String, _handle: Held) {}
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Load> for Blocks {
    async fn execute(&self, load: Load) -> Result<Option<Buffer>, ArchiveError> {
        load.perform(&self.store).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Get> for Blocks {
    async fn execute(&self, get: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        get.perform(&self.store).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Import> for Blocks {
    async fn execute(&self, import: Capability<Import>) -> Result<(), ArchiveError> {
        import.perform(&self.store).await
    }
}

crate::reads_nodes_through_blocks!(Blocks);
crate::writes_nodes_through_blocks!(Blocks);

/// A spilled value is read from the volatile store's blobs, else from the
/// catalog's blocks, where these keep a value written as a block.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<blob::Load> for Blocks {
    async fn execute(&self, load: blob::Load) -> Result<Option<Buffer>, ArchiveError> {
        load.perform(&self.store).await
    }
}

/// `env`, holding handles of its own: the node cache it answers node reads
/// from ([`NodeCache::of`]) is made on the first read and kept, so a node is
/// checked once and read from memory after that, and a write through it
/// ([`flush`](Self::flush)) keeps the nodes it stores.
///
/// For a block store that holds nothing (tests, benches, tools). Clones
/// share what is held.
pub struct Holding<'a, Env: ?Sized> {
    env: &'a Env,
    holdings: Holdings,
}

impl<Env: ?Sized> Clone for Holding<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            env: self.env,
            holdings: self.holdings.clone(),
        }
    }
}

impl<'a, Env: ?Sized> Holding<'a, Env> {
    /// `env`, holding nothing yet.
    pub fn new(env: &'a Env) -> Self {
        Self {
            env,
            holdings: Holdings::default(),
        }
    }

    /// The environment this reads through.
    pub fn env(&self) -> &'a Env {
        self.env
    }

    /// The node cache trees of [`test_catalog`] read through this keep
    /// their nodes in.
    pub fn cache<Key, Value>(&self) -> NodeCache<Key, Value>
    where
        Key: dialog_common::ConditionalSend + dialog_common::ConditionalSync + 'static,
        Value: dialog_common::ConditionalSend + dialog_common::ConditionalSync + 'static,
    {
        NodeCache::of(self, test_catalog().subject()).expect("holdings keep what they are given")
    }

    /// Writes every node `delta` holds into [`test_catalog`] through this
    /// ([`nodes::Write`](crate::nodes::Write)), emptying it: the nodes land
    /// in the blocks underneath and in the cache this holds.
    pub async fn flush<Key, Value>(
        &self,
        delta: &mut Delta<Blake3Hash, PersistentNode<Key, Value>>,
    ) -> Result<(), crate::DialogSearchTreeError>
    where
        Key: crate::Key + dialog_common::ConditionalSend + dialog_common::ConditionalSync + 'static,
        Value: crate::Value + dialog_common::ConditionalSync + 'static,
        Env: Provider<Import> + dialog_common::ConditionalSync,
    {
        let nodes = delta.flush().map(|(_, node)| node).collect();
        crate::nodes::Write::new(test_catalog(), nodes)
            .perform(self)
            .await
    }
}

impl<Env: ?Sized> std::ops::Deref for Holding<'_, Env> {
    type Target = Env;

    fn deref(&self) -> &Env {
        self.env
    }
}

impl<Env: ?Sized> Holds for Holding<'_, Env> {
    fn held(&self, key: &str) -> Option<Held> {
        self.holdings.held(key)
    }

    fn hold(&self, key: String, handle: Held) {
        self.holdings.hold(key, handle)
    }

    fn held_or(&self, key: &str, make: &dyn Fn() -> Held) -> Held {
        self.holdings.held_or(key, make)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<Load> for Holding<'_, Env>
where
    Env: Provider<Load> + dialog_common::ConditionalSync + ?Sized,
{
    async fn execute(&self, load: Load) -> Result<Option<Buffer>, ArchiveError> {
        self.env.execute(load).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<Get> for Holding<'_, Env>
where
    Env: Provider<Get> + dialog_common::ConditionalSync + ?Sized,
{
    async fn execute(&self, get: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        self.env.execute(get).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<Import> for Holding<'_, Env>
where
    Env: Provider<Import> + dialog_common::ConditionalSync + ?Sized,
{
    async fn execute(&self, import: Capability<Import>) -> Result<(), ArchiveError> {
        self.env.execute(import).await
    }
}

crate::reads_nodes_through_blocks!(
    impl['a, Env,] Holding<'a, Env>
    where [Env: Provider<Load> + dialog_common::ConditionalSync + ?Sized,]
);
crate::writes_nodes_through_blocks!(
    impl['a, Env,] Holding<'a, Env>
    where [Env: Provider<Import> + dialog_common::ConditionalSync + ?Sized,]
);

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<blob::Load> for Holding<'_, Env>
where
    Env: Provider<blob::Load> + dialog_common::ConditionalSync + ?Sized,
{
    async fn execute(&self, load: blob::Load) -> Result<Option<Buffer>, ArchiveError> {
        self.env.execute(load).await
    }
}

/// [`Blocks`] that journal every read, in order, while journaling is on.
///
/// The tree specs switch the journal off while they build and inspect a
/// tree, so what it holds is exactly the reads the code under test made.
/// A [`Load`] and a [`Get`] are journaled alike: both are a read of the
/// block.
#[derive(Clone)]
pub struct JournaledBlocks {
    blocks: Blocks,
    loads: Arc<Mutex<Vec<Blake3Hash>>>,
    journaling: Arc<AtomicBool>,
}

impl Default for JournaledBlocks {
    fn default() -> Self {
        Self {
            blocks: Blocks::default(),
            loads: Arc::default(),
            journaling: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl JournaledBlocks {
    /// An empty set of blocks, journaling.
    pub fn new() -> Self {
        Self::default()
    }

    /// Journals reads of `blocks`, which other handles may share, into a
    /// journal of its own.
    pub fn over(blocks: Blocks) -> Self {
        Self {
            blocks,
            ..Self::default()
        }
    }

    /// The blocks underneath, which read without journaling.
    pub fn blocks(&self) -> &Blocks {
        &self.blocks
    }

    /// Keeps `block` under its content hash.
    pub fn store(&self, block: Buffer) {
        self.blocks.store(block);
    }

    /// Keeps the block of every node `delta` holds, emptying it.
    pub fn flush<Key, Value>(&self, delta: &mut Delta<Blake3Hash, PersistentNode<Key, Value>>) {
        self.blocks.flush(delta);
    }

    /// The block stored under `hash`, if any, without journaling the read.
    pub fn get(&self, hash: &Blake3Hash) -> Option<Buffer> {
        self.blocks.get(hash)
    }

    /// Every read journaled since the last [`clear_journal`](Self::clear_journal).
    pub fn get_reads(&self) -> Vec<Blake3Hash> {
        self.loads.lock().clone()
    }

    /// How many reads were journaled.
    pub fn read_count(&self) -> usize {
        self.loads.lock().len()
    }

    /// Forgets every journaled read.
    pub fn clear_journal(&self) {
        self.loads.lock().clear();
    }

    /// Stops journaling reads.
    pub fn disable_journal(&self) {
        self.journaling.store(false, Ordering::SeqCst);
    }

    /// Resumes journaling reads.
    pub fn enable_journal(&self) {
        self.journaling.store(true, Ordering::SeqCst);
    }

    fn journal(&self, hash: &Blake3Hash) {
        if self.journaling.load(Ordering::SeqCst) {
            self.loads.lock().push(hash.clone());
        }
    }
}

/// Holds nothing, so every node read is a journaled read.
impl Holds for JournaledBlocks {
    fn held(&self, _key: &str) -> Option<Held> {
        None
    }

    fn hold(&self, _key: String, _handle: Held) {}
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Load> for JournaledBlocks {
    async fn execute(&self, load: Load) -> Result<Option<Buffer>, ArchiveError> {
        self.journal(&load.digest);
        load.perform(&self.blocks).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Get> for JournaledBlocks {
    async fn execute(&self, get: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        use dialog_effects::archive::prelude::GetExt as _;
        self.journal(get.digest());
        get.perform(&self.blocks).await
    }
}

crate::reads_nodes_through_blocks!(JournaledBlocks);
