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
//! A request is written at the node it is about ([`Node`]): a tree over
//! keys and values reads `Read<PersistentNode<Key, Value>>`, and a crate
//! that fixes the keys and values names the node once and writes
//! `Read<ItsNode>`. The tree crate does not care what the keys and values
//! are; the crate above it does, and its bounds should say so.
//!
//! Caching is a concern of whoever answers: an environment that keeps a
//! [`NodeCache`] consults it before its blocks and keeps what it reads and
//! writes. The tree performs no lookup of its own.
//!
//! A provider answers a request with a [`Resume`]: the node at once when
//! it holds it ([`Resume::now`], nothing allocated), else the future that
//! reads it ([`Resume::later`]). The tree awaits either the same way. The
//! translation layer answers a cache hit at once, so an environment that
//! answers through it pays for a boxed future only on a miss.
//!
//! The requests are commands, not effects, for the reason [`Load`] is: a
//! node read may hydrate from a remote, which no one can do on another's
//! behalf, and a node is never forked to a remote as itself; blocks are.

use std::marker::PhantomData;

use dialog_capability::{Command, Provider, Resume};
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

mod sealed {
    pub trait Sealed {}
    impl<Key, Value> Sealed for crate::PersistentNode<Key, Value> {}
}

/// A node the node layer's requests are about: what a tree reads by name
/// and writes when it persists.
///
/// The one node is [`PersistentNode`], a block decoded. The trait names a
/// node's key and value types, so a request can be written at the node
/// rather than at the pair, and how a block becomes one, which is what the
/// translation layer needs. It is sealed: a node of another form would be
/// another shape of [`PersistentNode`], not another implementor.
pub trait Node: sealed::Sealed + Clone + ConditionalSend + ConditionalSync + 'static {
    /// The tree's key type.
    type Key: crate::Key + ConditionalSend + ConditionalSync + 'static;
    /// The tree's value type.
    type Value: crate::Value + ConditionalSync + 'static;

    /// `block` as the node `name` names: the translation layer's decode. A
    /// block of other bytes is a corrupt read, not an absent one, and bytes
    /// that are not a node's are an encoding error.
    fn decode(block: Buffer, name: &Blake3Hash) -> Result<Self, DialogSearchTreeError>;

    /// The hash of the node's block: the name it is read by.
    fn hash(&self) -> &Blake3Hash;

    /// The node's block.
    fn buffer(&self) -> &Buffer;

    /// The node as the persistent node a cache keeps.
    fn into_persistent(self) -> PersistentNode<Self::Key, Self::Value>;

    /// The node a cache kept, as this node.
    fn from_persistent(node: PersistentNode<Self::Key, Self::Value>) -> Self;
}

impl<Key, Value> Node for PersistentNode<Key, Value>
where
    Key: crate::Key + ConditionalSend + ConditionalSync + 'static,
    Value: crate::Value + ConditionalSync + 'static,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    type Key = Key;
    type Value = Value;

    fn decode(block: Buffer, name: &Blake3Hash) -> Result<Self, DialogSearchTreeError> {
        if block.blake3_hash() != name {
            return Err(DialogSearchTreeError::Storage(
                DialogStorageError::Verification(
                    "Retrieved bytes did not match the provided hash".to_string(),
                ),
            ));
        }
        PersistentNode::try_from(block)
    }

    fn hash(&self) -> &Blake3Hash {
        PersistentNode::hash(self)
    }

    fn buffer(&self) -> &Buffer {
        PersistentNode::buffer(self)
    }

    fn into_persistent(self) -> PersistentNode<Key, Value> {
        self
    }

    fn from_persistent(node: PersistentNode<Key, Value>) -> Self {
        node
    }
}

/// `block` as the node `name` names ([`Node::decode`]).
pub fn decode<N: Node>(block: Buffer, name: &Blake3Hash) -> Result<N, DialogSearchTreeError> {
    N::decode(block, name)
}

/// Command: the node `name` names in `catalog`, from the local archive, or
/// else from wherever the environment knows the subject's blocks live. The
/// node layer's read.
///
/// `None` means the environment can reach no such node. An environment
/// that keeps a node cache answers a node it holds from there.
pub struct Read<N> {
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
    node: PhantomData<fn() -> N>,
}

impl<N> std::fmt::Debug for Read<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Read")
            .field("catalog", &self.catalog)
            .field("name", &self.name)
            .field("priority", &self.priority)
            .field("local", &self.local)
            .finish()
    }
}

impl<N> Clone for Read<N> {
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            name: self.name.clone(),
            priority: self.priority,
            local: self.local,
            node: PhantomData,
        }
    }
}

impl<N> Read<N> {
    /// Read the node `name` names in `catalog`, as a read someone is
    /// waiting on, from anywhere.
    pub fn new(catalog: CatalogScope, name: Blake3Hash) -> Self {
        Self {
            catalog,
            name,
            priority: Priority::Demand,
            local: false,
            node: PhantomData,
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

    /// Perform this read on `env`.
    pub fn perform<'a, Env>(
        self,
        env: &'a Env,
    ) -> Resume<'a, Result<Option<N>, DialogSearchTreeError>>
    where
        Env: Provider<Self> + ?Sized,
    {
        env.execute(self)
    }
}

impl<N> Command for Read<N> {
    type Input = Self;
    type Output = Result<Option<N>, DialogSearchTreeError>;
}

impl<N: Node> Read<N> {
    /// Answers this read over `env`'s blocks: from `cache` when it holds the
    /// node, else the block the name names, [loaded](Load) as this read
    /// asks, checked against the name and decoded, then kept in `cache`.
    ///
    /// The translation layer's read, for an environment whose store keeps
    /// bytes. A node `cache` holds is answered at once ([`Resume::now`]);
    /// only a miss builds the future that loads. Bytes that do not hash to
    /// the name fail the read rather than reading as absent, so `None`
    /// means the block is genuinely not reachable; bytes that do not
    /// decode as a node fail it too, and neither is kept.
    pub fn through_blocks<'a, Env>(
        self,
        env: &'a Env,
        cache: Option<&'a NodeCache<N::Key, N::Value>>,
    ) -> Resume<'a, Result<Option<N>, DialogSearchTreeError>>
    where
        Env: Provider<Load> + ConditionalSync + ?Sized,
    {
        self.through_loader(move |load| load.perform(env), cache)
    }

    /// [`through_blocks`](Self::through_blocks), with the block read by
    /// `load` instead of by a [`Load`] performed on an environment: for an
    /// environment that answers loads some other way (a test harness
    /// tallying what it reads through its own hydration, say).
    pub fn through_loader<'a, Loader, Loading>(
        self,
        load: Loader,
        cache: Option<&'a NodeCache<N::Key, N::Value>>,
    ) -> Resume<'a, Result<Option<N>, DialogSearchTreeError>>
    where
        Loader: FnOnce(Load) -> Loading + ConditionalSend + 'a,
        Loading: Future<Output = Result<Option<Buffer>, archive::ArchiveError>> + ConditionalSend,
    {
        // A hit answers before any of the loading machinery is set up: the
        // common case of a warm read costs the lookup and nothing else.
        if let Some(cache) = cache
            && let Some(node) = cache.get_cached(&self.name)
        {
            return Resume::now(Ok(Some(N::from_persistent(node))));
        }
        Resume::later(async move {
            let fetch = async |name: &Blake3Hash| {
                let block = load(self.load())
                    .await
                    .map_err(|error| DialogSearchTreeError::Storage(error.into()))?;
                block
                    .map(|block| N::decode(block, name).map(N::into_persistent))
                    .transpose()
            };
            let node = match cache {
                Some(cache) => cache.get_or_fetch(&self.name, fetch).await?,
                None => fetch(&self.name).await?,
            };
            Ok(node.map(N::from_persistent))
        })
    }

    /// [`through_blocks`](Self::through_blocks), with the node cache `env`
    /// holds for the catalog's subject ([`NodeCache::of`]), if it holds
    /// one.
    pub fn through_held_blocks<'a, Env>(
        self,
        env: &'a Env,
    ) -> Resume<'a, Result<Option<N>, DialogSearchTreeError>>
    where
        Env: Provider<Load> + Holds + ConditionalSync + ?Sized,
    {
        match NodeCache::of(env, self.catalog.subject()) {
            None => self.through_blocks(env, None),
            Some(cache) => {
                if let Some(node) = cache.get_cached(&self.name) {
                    return Resume::now(Ok(Some(N::from_persistent(node))));
                }
                Resume::later(async move { self.through_blocks(env, Some(&cache)).await })
            }
        }
    }
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
pub struct Preload<N> {
    /// The catalog the node's block lives in.
    pub catalog: CatalogScope,
    /// The name of the node to make cheap.
    pub name: Blake3Hash,
    /// How a hydration this causes ranks against the site's other reads.
    pub priority: Priority,
    node: PhantomData<fn() -> N>,
}

impl<N> std::fmt::Debug for Preload<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Preload")
            .field("catalog", &self.catalog)
            .field("name", &self.name)
            .field("priority", &self.priority)
            .finish()
    }
}

impl<N> Clone for Preload<N> {
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            name: self.name.clone(),
            priority: self.priority,
            node: PhantomData,
        }
    }
}

impl<N> Preload<N> {
    /// Preload the node `name` names in `catalog`.
    pub fn new(catalog: CatalogScope, name: Blake3Hash) -> Self {
        Self {
            catalog,
            name,
            priority: Priority::Demand,
            node: PhantomData,
        }
    }

    /// Rank a hydration this causes as `priority`.
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// The read this preload makes ahead of time.
    pub fn read(&self) -> Read<N> {
        Read::new(self.catalog.clone(), self.name.clone()).with_priority(self.priority)
    }

    /// Perform this preload on `env`.
    pub fn perform<'a, Env>(self, env: &'a Env) -> Resume<'a, ()>
    where
        Env: Provider<Self> + ?Sized,
    {
        env.execute(self)
    }
}

impl<N> Command for Preload<N> {
    type Input = Self;
    type Output = ();
}

impl<N: Node> Preload<N> {
    /// Answers this preload over `env`'s blocks: reads the node into
    /// `cache` as [`Read::through_blocks`] would, dropping the result. With
    /// no cache there is nowhere for the node to land, so nothing is read;
    /// a node the cache holds already is done with at once.
    pub fn through_blocks<'a, Env>(
        self,
        env: &'a Env,
        cache: Option<&'a NodeCache<N::Key, N::Value>>,
    ) -> Resume<'a, ()>
    where
        Env: Provider<Load> + ConditionalSync + ?Sized,
    {
        let Some(cache) = cache else {
            return Resume::now(());
        };
        match self.read().through_blocks(env, Some(cache)).into_now() {
            Ok(_) => Resume::now(()),
            Err(reading) => Resume::later(async move {
                let _ = reading.await;
            }),
        }
    }

    /// [`through_blocks`](Self::through_blocks), with the node cache `env`
    /// holds for the catalog's subject, if it holds one.
    pub fn through_held_blocks<'a, Env>(self, env: &'a Env) -> Resume<'a, ()>
    where
        Env: Provider<Load> + Holds + ConditionalSync + ?Sized,
    {
        let Some(cache) = NodeCache::of(env, self.catalog.subject()) else {
            return Resume::now(());
        };
        if cache.get_cached(&self.name).is_some() {
            return Resume::now(());
        }
        Resume::later(async move {
            let _ = self.read().through_blocks(env, Some(&cache)).await;
        })
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
pub struct Write<N> {
    /// The catalog the nodes belong in.
    pub catalog: CatalogScope,
    /// The nodes to store.
    pub nodes: Vec<N>,
}

impl<N> std::fmt::Debug for Write<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Write")
            .field("catalog", &self.catalog)
            .field("nodes", &self.nodes.len())
            .finish()
    }
}

impl<N> Write<N> {
    /// Write `nodes` into `catalog`.
    pub fn new(catalog: CatalogScope, nodes: Vec<N>) -> Self {
        Self { catalog, nodes }
    }

    /// Perform this write on `env`.
    pub fn perform<'a, Env>(self, env: &'a Env) -> Resume<'a, Result<(), DialogSearchTreeError>>
    where
        Env: Provider<Self> + ?Sized,
    {
        env.execute(self)
    }
}

impl<N> Command for Write<N> {
    type Input = Self;
    type Output = Result<(), DialogSearchTreeError>;
}

impl<N: Node> Write<N> {
    /// Answers this write over `env`'s blocks: [imports](Import) each
    /// node's block into the catalog, then keeps the nodes in `cache`. A
    /// failed import keeps nothing; a write of no nodes is done at once.
    ///
    /// The translation layer's write, for an environment whose store keeps
    /// bytes.
    pub fn through_blocks<'a, Env>(
        self,
        env: &'a Env,
        cache: Option<&'a NodeCache<N::Key, N::Value>>,
    ) -> Resume<'a, Result<(), DialogSearchTreeError>>
    where
        Env: Provider<Import> + ConditionalSync + ?Sized,
    {
        let Self { catalog, nodes } = self;
        if nodes.is_empty() {
            return Resume::now(Ok(()));
        }
        Resume::later(async move {
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
                        cache.insert(node.hash().clone(), node.into_persistent());
                    }
                }
            }
            Ok(())
        })
    }

    /// [`through_blocks`](Self::through_blocks), with the node cache `env`
    /// holds for the catalog's subject, if it holds one.
    pub fn through_held_blocks<'a, Env>(
        self,
        env: &'a Env,
    ) -> Resume<'a, Result<(), DialogSearchTreeError>>
    where
        Env: Provider<Import> + Holds + ConditionalSync + ?Sized,
    {
        if self.nodes.is_empty() {
            return Resume::now(Ok(()));
        }
        let cache = NodeCache::of(env, self.catalog.subject());
        Resume::later(async move { self.through_blocks(env, cache.as_ref()).await })
    }
}

/// The node `read` names if the cache `env` holds for the catalog's subject
/// holds it ([`NodeCache::held_for`]): what an environment that keeps its
/// cache in its handles answers a read with at once ([`Resume::now`]),
/// before it asks anything beneath it.
pub fn held_node<Env, N>(env: &Env, read: &Read<N>) -> Option<N>
where
    Env: Holds + ?Sized,
    N: Node,
{
    NodeCache::held_for(env, read.catalog.subject())?
        .get_cached(&read.name)
        .map(N::from_persistent)
}

/// Makes a type that answers [`Load`] and holds handles ([`Holds`]) answer
/// the node layer's reads ([`Read`], [`Preload`]) through the translation
/// layer, with the node cache it holds for the catalog's subject.
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
        impl<$($g)* N> dialog_capability::Provider<$crate::nodes::Read<N>> for $ty
        where
            N: $crate::nodes::Node,
            $($w)*
        {
            fn execute(
                &self,
                read: $crate::nodes::Read<N>,
            ) -> dialog_capability::Resume<'_, Result<Option<N>, $crate::DialogSearchTreeError>>
            {
                read.through_held_blocks(self)
            }
        }

        impl<$($g)* N> dialog_capability::Provider<$crate::nodes::Preload<N>> for $ty
        where
            N: $crate::nodes::Node,
            $($w)*
        {
            fn execute(
                &self,
                preload: $crate::nodes::Preload<N>,
            ) -> dialog_capability::Resume<'_, ()> {
                preload.through_held_blocks(self)
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
        impl<$($g)* N> dialog_capability::Provider<$crate::nodes::Write<N>> for $ty
        where
            N: $crate::nodes::Node,
            $($w)*
        {
            fn execute(
                &self,
                write: $crate::nodes::Write<N>,
            ) -> dialog_capability::Resume<'_, Result<(), $crate::DialogSearchTreeError>> {
                write.through_held_blocks(self)
            }
        }
    };
}
