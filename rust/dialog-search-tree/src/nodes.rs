//! The node layer: what a tree asks its environment for.
//!
//! A tree talks in nodes. It performs three requests on the environment it
//! is read or written through, and everything beside them (where a node is
//! kept, in what form, whether it is remembered) belongs to whoever answers:
//!
//! - [`Read`]: the node a name names, from anywhere the environment can
//!   reach, or from this replica alone;
//! - [`Preload`]: make a node cheap to read soon;
//! - [`Write`]: the nodes a persist sealed, stored and kept readable.
//!
//! Beneath the node layer sit two others. The **block layer** reads and
//! writes blocks as bytes by digest ([`archive::Get`], [`Load`], [`Import`]):
//! the stores, remotes and the network. The **translation layer** turns a
//! block into a node and a node into a block, and lives here, as the
//! functions an environment whose store keeps bytes answers the requests
//! with ([`Read::through_blocks`], [`Preload::through_blocks`],
//! [`Write::through_blocks`]). A store that keeps nodes in a form of its
//! own answers the requests at the node layer directly and still answers
//! block requests by encoding; a node it lacks arrives from a remote as a
//! block, through translation.
//!
//! Caching is a concern of whoever answers: an environment that keeps a
//! [`NodeCache`] consults it before its blocks and keeps what it reads and
//! writes. The tree performs no lookup of its own.
//!
//! A request's `perform` first asks the environment what it can answer at
//! once ([`Ready`]) and performs the command only for the rest. Performing
//! a command through [`Provider`] boxes a future, which a node the
//! environment already holds does not need; an environment that cannot
//! answer anything at once says so and every read goes the long way.
//!
//! The requests are commands, not effects, for the reason [`Load`] is: a
//! node read may hydrate from a remote, which no one can do on another's
//! behalf, and a node is never forked to a remote as itself; blocks are.

use std::marker::PhantomData;

use dialog_capability::{Command, Provider};
use dialog_common::{Blake3Hash, Buffer, ConditionalSend, ConditionalSync, Holds, Priority};
use dialog_effects::archive::prelude::CatalogScope;
use dialog_effects::archive::{self, Import, Load};
use dialog_storage::DialogStorageError;
use rkyv::{
    bytecheck::CheckBytes,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{DialogSearchTreeError, NodeCache, PersistentNode};

mod stores;

/// Command: the node `name` names in `catalog`, from the local archive, or
/// else from wherever the environment knows the subject's blocks live. The
/// node layer's read.
///
/// `None` means the environment can reach no such node. An environment
/// that keeps a node cache answers a node it holds from there.
pub struct Read<Key, Value> {
    /// The catalog the node's block lives in.
    pub catalog: CatalogScope,
    /// The name the node is asked for by: the hash of its block.
    pub name: Blake3Hash,
    /// How a hydration this read causes ranks against the site's other
    /// reads.
    pub priority: Priority,
    /// Whether the read sees only what this replica holds, never
    /// hydrating (see [`locally`](Self::locally)).
    pub local: bool,
    types: PhantomData<fn() -> (Key, Value)>,
}

impl<Key, Value> std::fmt::Debug for Read<Key, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Read")
            .field("catalog", &self.catalog)
            .field("name", &self.name)
            .field("priority", &self.priority)
            .field("local", &self.local)
            .finish()
    }
}

impl<Key, Value> Clone for Read<Key, Value> {
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            name: self.name.clone(),
            priority: self.priority,
            local: self.local,
            types: PhantomData,
        }
    }
}

impl<Key, Value> Read<Key, Value> {
    /// Read the node `name` names in `catalog`, as a read someone is
    /// waiting on, from anywhere.
    pub fn new(catalog: CatalogScope, name: Blake3Hash) -> Self {
        Self {
            catalog,
            name,
            priority: Priority::Demand,
            local: false,
            types: PhantomData,
        }
    }

    /// Rank a hydration this read causes as `priority`.
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Read only what this replica holds: a node the replica lacks reads
    /// as absent wherever else the subject's blocks live. For a reader
    /// whose answer is what this replica holds, such as an export that
    /// does not download, or a difference bounded by local knowledge.
    pub fn locally(mut self) -> Self {
        self.local = true;
        self
    }

    /// The load of the block this read's node is encoded as, asked the
    /// way this read is: what the translation layer performs on a miss.
    pub fn load(&self) -> Load {
        let load = Load::new(self.catalog.clone(), self.name.clone()).with_priority(self.priority);
        if self.local { load.locally() } else { load }
    }

    /// Perform this read on `env`: what `env` can answer at once
    /// ([`Ready`]), else the command performed.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError>
    where
        Env: Provider<Self> + Ready<Key, Value> + ConditionalSync + ?Sized,
    {
        if let Some(node) = env.ready(&self) {
            return Ok(Some(node));
        }
        env.execute(self).await
    }
}

/// What an environment can answer a node [`Read`] with at once, without
/// waiting on anything: the node it holds in memory, or nothing.
///
/// The request's [`perform`](Read::perform) asks this first, so a node the
/// environment holds costs a lookup and no boxed future; a read the
/// environment cannot answer at once is performed as the command, which may
/// wait on a store or a remote. The answer must be a node the environment
/// would give the command too: an environment holds only what its archive
/// has, so this is never a shortcut around what a read means, only around
/// how it is dispatched.
pub trait Ready<Key, Value> {
    /// The node `read` names, if this environment holds it already.
    fn ready(&self, read: &Read<Key, Value>) -> Option<PersistentNode<Key, Value>>;
}

/// A borrowed environment answers at once what the environment does.
impl<T, Key, Value> Ready<Key, Value> for &T
where
    T: Ready<Key, Value> + ?Sized,
{
    fn ready(&self, read: &Read<Key, Value>) -> Option<PersistentNode<Key, Value>> {
        (**self).ready(read)
    }
}

impl<Key, Value> Command for Read<Key, Value> {
    type Input = Self;
    type Output = Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError>;
}

impl<Key, Value> Read<Key, Value>
where
    Key: crate::Key,
    Value: crate::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Answers this read over `env`'s blocks: from `cache` when it holds the
    /// node, else the block the name names, [loaded](Load) as this read
    /// asks, checked against the name and decoded, then kept in `cache`.
    ///
    /// The translation layer's read, for an environment whose store keeps
    /// bytes. Bytes that do not hash to the name fail the read rather than
    /// reading as absent, so `None` means the block is genuinely not
    /// reachable; bytes that do not decode as a node fail it too, and
    /// neither is kept.
    pub async fn through_blocks<Env>(
        self,
        env: &Env,
        cache: Option<&NodeCache<Key, Value>>,
    ) -> Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError>
    where
        Env: Provider<Load> + ConditionalSync + ?Sized,
    {
        self.through_loader(async |load| load.perform(env).await, cache)
            .await
    }

    /// [`through_blocks`](Self::through_blocks), with the block read by
    /// `load` instead of by a [`Load`] performed on an environment: for an
    /// environment that answers loads some other way (a test harness
    /// tallying what it reads through its own hydration, say).
    pub async fn through_loader<Loader>(
        self,
        load: Loader,
        cache: Option<&NodeCache<Key, Value>>,
    ) -> Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError>
    where
        Loader: AsyncFnOnce(Load) -> Result<Option<Buffer>, archive::ArchiveError>,
    {
        // A hit answers before any of the loading machinery is set up: the
        // common case of a warm read costs the lookup and nothing else.
        if let Some(cache) = cache
            && let Some(node) = cache.get_cached(&self.name)
        {
            return Ok(Some(node));
        }
        let fetch = async |name: &Blake3Hash| {
            let block = load(self.load())
                .await
                .map_err(|error| DialogSearchTreeError::Storage(error.into()))?;
            block.map(|block| decode(block, name)).transpose()
        };
        match cache {
            Some(cache) => cache.get_or_fetch(&self.name, fetch).await,
            None => fetch(&self.name).await,
        }
    }

    /// [`through_blocks`](Self::through_blocks), with the node cache `env`
    /// holds for the catalog's subject ([`NodeCache::of`]), if it holds
    /// one.
    pub async fn through_held_blocks<Env>(
        self,
        env: &Env,
    ) -> Result<Option<PersistentNode<Key, Value>>, DialogSearchTreeError>
    where
        Env: Provider<Load> + Holds + ConditionalSync + ?Sized,
        Key: ConditionalSend + ConditionalSync + 'static,
        Value: ConditionalSend + ConditionalSync + 'static,
    {
        let cache = NodeCache::of(env, self.catalog.subject());
        self.through_blocks(env, cache.as_ref()).await
    }
}

/// `block` as the node `name` names: the translation layer's decode. A
/// block of other bytes is a corrupt read, not an absent one, and bytes
/// that are not a node's are an encoding error.
pub fn decode<Key, Value>(
    block: Buffer,
    name: &Blake3Hash,
) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError>
where
    Key: crate::Key,
    Value: crate::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    if block.blake3_hash() != name {
        return Err(DialogSearchTreeError::Storage(
            DialogStorageError::Verification(
                "Retrieved bytes did not match the provided hash".to_string(),
            ),
        ));
    }
    PersistentNode::try_from(block)
}

/// Command: make the node `name` names in `catalog` cheap to read soon.
/// The node layer's read-ahead.
///
/// Nothing observes its outcome: a node that is missing or fails to load
/// is left to the read that needs it. The future is the work, and it
/// advances only while its owner polls it, so a reader that needs the node
/// meanwhile reads for itself rather than waiting on it: only the owner
/// can join it, by polling it to completion. An environment with nowhere
/// to keep the node does nothing.
pub struct Preload<Key, Value> {
    /// The catalog the node's block lives in.
    pub catalog: CatalogScope,
    /// The name of the node to make cheap.
    pub name: Blake3Hash,
    /// How a hydration this causes ranks against the site's other reads.
    pub priority: Priority,
    types: PhantomData<fn() -> (Key, Value)>,
}

impl<Key, Value> std::fmt::Debug for Preload<Key, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Preload")
            .field("catalog", &self.catalog)
            .field("name", &self.name)
            .field("priority", &self.priority)
            .finish()
    }
}

impl<Key, Value> Clone for Preload<Key, Value> {
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            name: self.name.clone(),
            priority: self.priority,
            types: PhantomData,
        }
    }
}

impl<Key, Value> Preload<Key, Value> {
    /// Preload the node `name` names in `catalog`.
    pub fn new(catalog: CatalogScope, name: Blake3Hash) -> Self {
        Self {
            catalog,
            name,
            priority: Priority::Demand,
            types: PhantomData,
        }
    }

    /// Rank a hydration this causes as `priority`.
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// The read this preload makes ahead of time.
    pub fn read(&self) -> Read<Key, Value> {
        Read::new(self.catalog.clone(), self.name.clone()).with_priority(self.priority)
    }

    /// Perform this preload on `env`: nothing, for a node `env` already
    /// holds ([`Ready`]), else the command performed.
    pub async fn perform<Env>(self, env: &Env)
    where
        Env: Provider<Self> + Ready<Key, Value> + ConditionalSync + ?Sized,
    {
        if env.ready(&self.read()).is_some() {
            return;
        }
        env.execute(self).await
    }
}

impl<Key, Value> Command for Preload<Key, Value> {
    type Input = Self;
    type Output = ();
}

impl<Key, Value> Preload<Key, Value>
where
    Key: crate::Key,
    Value: crate::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Answers this preload over `env`'s blocks: reads the node into
    /// `cache` as [`Read::through_blocks`] would, dropping the result. With
    /// no cache there is nowhere for the node to land, so nothing is read.
    pub async fn through_blocks<Env>(self, env: &Env, cache: Option<&NodeCache<Key, Value>>)
    where
        Env: Provider<Load> + ConditionalSync + ?Sized,
    {
        if cache.is_some() {
            let _ = self.read().through_blocks(env, cache).await;
        }
    }

    /// [`through_blocks`](Self::through_blocks), with the node cache `env`
    /// holds for the catalog's subject, if it holds one.
    pub async fn through_held_blocks<Env>(self, env: &Env)
    where
        Env: Provider<Load> + Holds + ConditionalSync + ?Sized,
        Key: ConditionalSend + ConditionalSync + 'static,
        Value: ConditionalSend + ConditionalSync + 'static,
    {
        let cache = NodeCache::of(env, self.catalog.subject());
        self.through_blocks(env, cache.as_ref()).await
    }
}

/// Command: store the nodes a persist sealed into `catalog`, and keep them
/// readable. The node layer's write.
///
/// The nodes are durable when this returns: a read of any of them through
/// the environment finds it. An environment that keeps a node cache keeps
/// them there too, so the next edit opens them without reading them back;
/// a write that fails keeps nothing, so a scope holds only what its archive
/// has.
pub struct Write<Key, Value> {
    /// The catalog the nodes belong in.
    pub catalog: CatalogScope,
    /// The nodes to store.
    pub nodes: Vec<PersistentNode<Key, Value>>,
}

impl<Key, Value> std::fmt::Debug for Write<Key, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Write")
            .field("catalog", &self.catalog)
            .field("nodes", &self.nodes.len())
            .finish()
    }
}

impl<Key, Value> Write<Key, Value> {
    /// Write `nodes` into `catalog`.
    pub fn new(catalog: CatalogScope, nodes: Vec<PersistentNode<Key, Value>>) -> Self {
        Self { catalog, nodes }
    }

    /// Perform this write on `env`.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), DialogSearchTreeError>
    where
        Env: Provider<Self> + ConditionalSync + ?Sized,
    {
        env.execute(self).await
    }
}

impl<Key, Value> Command for Write<Key, Value> {
    type Input = Self;
    type Output = Result<(), DialogSearchTreeError>;
}

impl<Key, Value> Write<Key, Value> {
    /// Answers this write over `env`'s blocks: [imports](Import) each
    /// node's block into the catalog, then keeps the nodes in `cache`. A
    /// failed import keeps nothing.
    ///
    /// The translation layer's write, for an environment whose store keeps
    /// bytes.
    pub async fn through_blocks<Env>(
        self,
        env: &Env,
        cache: Option<&NodeCache<Key, Value>>,
    ) -> Result<(), DialogSearchTreeError>
    where
        Env: Provider<Import> + ConditionalSync + ?Sized,
    {
        let Self { catalog, nodes } = self;
        if nodes.is_empty() {
            return Ok(());
        }
        catalog
            .import(nodes.iter().map(|node| node.buffer().clone()))
            .perform(env)
            .await
            .map_err(|error| DialogSearchTreeError::Storage(error.into()))?;
        if let Some(cache) = cache {
            for node in nodes {
                // A node the cache already holds under this hash is the
                // same bytes with whatever it has derived since: it stays.
                if cache.get_cached(node.hash()).is_none() {
                    cache.insert(node.hash().clone(), node);
                }
            }
        }
        Ok(())
    }

    /// [`through_blocks`](Self::through_blocks), with the node cache `env`
    /// holds for the catalog's subject, if it holds one.
    pub async fn through_held_blocks<Env>(self, env: &Env) -> Result<(), DialogSearchTreeError>
    where
        Env: Provider<Import> + Holds + ConditionalSync + ?Sized,
        Key: ConditionalSend + ConditionalSync + 'static,
        Value: ConditionalSend + ConditionalSync + 'static,
    {
        let cache = NodeCache::of(env, self.catalog.subject());
        self.through_blocks(env, cache.as_ref()).await
    }
}

/// An environment a tree reads through: it answers the node layer's reads.
///
/// The bound every tree operation that reads puts on its environment.
/// Nothing here asks `'static` of the environment, so a `&dyn Nodes<K, V>`
/// collapses the copies of tree code read through many environments into
/// one.
pub trait Nodes<Key, Value>:
    Provider<Read<Key, Value>> + Provider<Preload<Key, Value>> + Ready<Key, Value> + ConditionalSync
{
}

impl<T, Key, Value> Nodes<Key, Value> for T where
    T: Provider<Read<Key, Value>>
        + Provider<Preload<Key, Value>>
        + Ready<Key, Value>
        + ConditionalSync
        + ?Sized
{
}

/// The node `read` names if the cache `env` holds for the catalog's subject
/// holds it ([`NodeCache::held_for`]): how an environment that keeps its
/// cache in its handles answers [`Ready`].
pub fn held_node<Env, Key, Value>(
    env: &Env,
    read: &Read<Key, Value>,
) -> Option<PersistentNode<Key, Value>>
where
    Env: Holds + ?Sized,
    Key: ConditionalSend + ConditionalSync + 'static,
    Value: ConditionalSend + ConditionalSync + 'static,
{
    NodeCache::held_for(env, read.catalog.subject())?.get_cached(&read.name)
}

#[doc(hidden)]
pub use rkyv as __rkyv;

/// Makes a type that answers [`Load`] and holds handles ([`Holds`]) answer
/// the node layer's reads ([`Read`], [`Preload`], [`Ready`]) through the
/// translation layer, with the node cache it holds for the catalog's
/// subject.
///
/// For a non-generic environment: `reads_nodes_through_blocks!(MyEnv);`.
/// For a generic one, its generics and where clause in brackets, each
/// ending in a comma when not empty:
/// `reads_nodes_through_blocks!(impl['a, Env,] MyEnv<'a, Env> where [Env: Provider<Load> + Holds + ConditionalSync,]);`.
#[macro_export]
macro_rules! reads_nodes_through_blocks {
    ($ty:ty) => {
        $crate::reads_nodes_through_blocks!(impl[] $ty where []);
    };
    (impl[$($g:tt)*] $ty:ty where [$($w:tt)*]) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<$($g)* Key, Value> dialog_capability::Provider<$crate::nodes::Read<Key, Value>>
            for $ty
        where
            Key: $crate::Key + dialog_common::ConditionalSend + dialog_common::ConditionalSync + 'static,
            Value: $crate::Value + dialog_common::ConditionalSync + 'static,
            <Value as $crate::nodes::__rkyv::Archive>::Archived:
                for<'__a> $crate::nodes::__rkyv::bytecheck::CheckBytes<
                    $crate::nodes::__rkyv::rancor::Strategy<
                        $crate::nodes::__rkyv::validation::Validator<
                            $crate::nodes::__rkyv::validation::archive::ArchiveValidator<'__a>,
                            $crate::nodes::__rkyv::validation::shared::SharedValidator,
                        >,
                        $crate::nodes::__rkyv::rancor::Error,
                    >,
                >,
            $($w)*
        {
            async fn execute(
                &self,
                read: $crate::nodes::Read<Key, Value>,
            ) -> Result<Option<$crate::PersistentNode<Key, Value>>, $crate::DialogSearchTreeError>
            {
                read.through_held_blocks(self).await
            }
        }

        impl<$($g)* Key, Value> $crate::nodes::Ready<Key, Value> for $ty
        where
            Key: dialog_common::ConditionalSend + dialog_common::ConditionalSync + 'static,
            Value: dialog_common::ConditionalSend + dialog_common::ConditionalSync + 'static,
            $($w)*
        {
            fn ready(
                &self,
                read: &$crate::nodes::Read<Key, Value>,
            ) -> Option<$crate::PersistentNode<Key, Value>> {
                $crate::nodes::held_node(self, read)
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<$($g)* Key, Value> dialog_capability::Provider<$crate::nodes::Preload<Key, Value>>
            for $ty
        where
            Key: $crate::Key + dialog_common::ConditionalSend + dialog_common::ConditionalSync + 'static,
            Value: $crate::Value + dialog_common::ConditionalSync + 'static,
            <Value as $crate::nodes::__rkyv::Archive>::Archived:
                for<'__a> $crate::nodes::__rkyv::bytecheck::CheckBytes<
                    $crate::nodes::__rkyv::rancor::Strategy<
                        $crate::nodes::__rkyv::validation::Validator<
                            $crate::nodes::__rkyv::validation::archive::ArchiveValidator<'__a>,
                            $crate::nodes::__rkyv::validation::shared::SharedValidator,
                        >,
                        $crate::nodes::__rkyv::rancor::Error,
                    >,
                >,
            $($w)*
        {
            async fn execute(&self, preload: $crate::nodes::Preload<Key, Value>) {
                preload.through_held_blocks(self).await
            }
        }
    };
}

/// Makes a type that answers [`Import`] and holds handles ([`Holds`])
/// answer the node layer's [`Write`] through the translation layer, keeping
/// the nodes in the cache it holds for the catalog's subject. Same syntax
/// as [`reads_nodes_through_blocks!`].
#[macro_export]
macro_rules! writes_nodes_through_blocks {
    ($ty:ty) => {
        $crate::writes_nodes_through_blocks!(impl[] $ty where []);
    };
    (impl[$($g:tt)*] $ty:ty where [$($w:tt)*]) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<$($g)* Key, Value> dialog_capability::Provider<$crate::nodes::Write<Key, Value>>
            for $ty
        where
            Key: $crate::Key + dialog_common::ConditionalSend + dialog_common::ConditionalSync + 'static,
            Value: $crate::Value + dialog_common::ConditionalSync + 'static,
            $($w)*
        {
            async fn execute(
                &self,
                write: $crate::nodes::Write<Key, Value>,
            ) -> Result<(), $crate::DialogSearchTreeError> {
                write.through_held_blocks(self).await
            }
        }
    };
}
