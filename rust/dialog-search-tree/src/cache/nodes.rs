//! The node cache: checked nodes by content hash, bounded by the bytes they
//! hold, and shared between scopes that each see only what they hold.

use std::sync::Arc;

use dialog_common::Blake3Hash;
use hashbrown::HashTable;
use parking_lot::Mutex;

use crate::PersistentNode;

/// The bytes of nodes a cache holds when no budget is named: what the
/// former bound of 2,048 nodes came to at the 64 KiB a node is paced toward.
pub const NODE_CACHE_BUDGET: usize = 128 * 1024 * 1024;

/// How many independently locked parts a cache splits its nodes between, so
/// readers on different threads rarely meet.
const SHARDS: usize = 16;

/// A cache smaller than this is not split: a part of it would hold too
/// little to keep a node.
const SHARDED_FROM: usize = 16 * 1024 * 1024;

/// No slot.
const NIL: u32 = u32::MAX;

/// Whose nodes a [`NodeCache`] handle reads and writes.
///
/// Two repositories may hold the same node, and a cache they share keeps it
/// once. But a read must only be answered with a node the reader's own
/// repository holds: a block that turned up because another repository had
/// it would be there one moment and gone the next, and nothing downstream
/// could count on finding it in the archive. So every node records the
/// scopes that hold it, and a handle answers only for its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Scope(u64);

impl Scope {
    /// The scope named by `name`: the same name is always the same scope.
    pub fn named(name: &[u8]) -> Self {
        let [b0, b1, b2, b3, b4, b5, b6, b7, ..] = *Blake3Hash::hash(name).as_bytes();
        Self(u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, b7]))
    }
}

/// The scopes that hold a node. Nearly every node has one.
#[derive(Debug)]
enum Holders {
    One(Scope),
    Many(Vec<Scope>),
}

impl Holders {
    fn holds(&self, scope: Scope) -> bool {
        match self {
            Holders::One(holder) => *holder == scope,
            Holders::Many(holders) => holders.contains(&scope),
        }
    }

    fn add(&mut self, scope: Scope) {
        match self {
            Holders::One(holder) if *holder == scope => {}
            Holders::One(holder) => *self = Holders::Many(vec![*holder, scope]),
            Holders::Many(holders) => {
                if !holders.contains(&scope) {
                    holders.push(scope);
                }
            }
        }
    }

    /// Removes `scope`, answering whether anyone still holds the node.
    fn remove(&mut self, scope: Scope) -> bool {
        match self {
            Holders::One(holder) => *holder != scope,
            Holders::Many(holders) => {
                holders.retain(|holder| *holder != scope);
                !holders.is_empty()
            }
        }
    }
}

/// One cached node, linked into its shard's queue.
struct Slot<Key, Value> {
    hash: Blake3Hash,
    node: PersistentNode<Key, Value>,
    holders: Holders,
    /// The bytes the node is charged for.
    weight: usize,
    /// Whether the node was read since the hand last passed it.
    visited: bool,
    /// The slot admitted after this one, toward the head of the queue.
    newer: u32,
    /// The slot admitted before this one, toward the tail.
    older: u32,
}

/// One independently locked part of a cache: a SIEVE queue of nodes, newest
/// at the head, with a hand that walks from the tail toward the head looking
/// for a node nobody read since it last passed.
struct Shard<Key, Value> {
    table: HashTable<u32>,
    slots: Vec<Option<Slot<Key, Value>>>,
    free: Vec<u32>,
    head: u32,
    tail: u32,
    hand: u32,
    bytes: usize,
    count: usize,
}

/// The table hash of a node's content hash: its leading eight bytes, which
/// are as uniform as any hash of them would be.
fn table_hash(hash: &Blake3Hash) -> u64 {
    let [b0, b1, b2, b3, b4, b5, b6, b7, ..] = *hash.as_bytes();
    u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, b7])
}

impl<Key, Value> Shard<Key, Value> {
    fn new() -> Self {
        Self {
            table: HashTable::new(),
            slots: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
            hand: NIL,
            bytes: 0,
            count: 0,
        }
    }

    fn slot(&self, at: u32) -> &Slot<Key, Value> {
        self.slots[at as usize]
            .as_ref()
            .expect("a linked slot is occupied")
    }

    fn slot_mut(&mut self, at: u32) -> &mut Slot<Key, Value> {
        self.slots[at as usize]
            .as_mut()
            .expect("a linked slot is occupied")
    }

    fn find(&self, hash: &Blake3Hash) -> Option<u32> {
        self.table
            .find(table_hash(hash), |at| self.slot(*at).hash == *hash)
            .copied()
    }

    /// Unlinks the slot at `at` and gives its node back.
    fn take(&mut self, at: u32) -> Slot<Key, Value> {
        let hash = table_hash(&self.slot(at).hash);
        if let Ok(entry) = self.table.find_entry(hash, |found| *found == at) {
            entry.remove();
        }
        let slot = self.slots[at as usize]
            .take()
            .expect("a linked slot is occupied");
        match slot.newer {
            NIL => self.head = slot.older,
            newer => self.slot_mut(newer).older = slot.older,
        }
        match slot.older {
            NIL => self.tail = slot.newer,
            older => self.slot_mut(older).newer = slot.newer,
        }
        if self.hand == at {
            self.hand = slot.newer;
        }
        self.free.push(at);
        self.bytes -= slot.weight;
        self.count -= 1;
        slot
    }

    /// Evicts one node: the first the hand finds unread since its last
    /// pass, clearing the mark of each read node it steps over.
    fn evict(&mut self) -> bool {
        if self.tail == NIL {
            return false;
        }
        let mut at = if self.hand == NIL {
            self.tail
        } else {
            self.hand
        };
        while self.slot(at).visited {
            let slot = self.slot_mut(at);
            slot.visited = false;
            let newer = slot.newer;
            at = if newer == NIL { self.tail } else { newer };
        }
        // The hand rests on the node after the one it took, so the next
        // pass carries on from there instead of starting over at the tail.
        let next = self.slot(at).newer;
        self.take(at);
        self.hand = next;
        true
    }

    /// Links a new slot at the head of the queue.
    fn admit(&mut self, slot: Slot<Key, Value>) {
        let hash = table_hash(&slot.hash);
        let weight = slot.weight;
        let at = match self.free.pop() {
            Some(at) => {
                self.slots[at as usize] = Some(slot);
                at
            }
            None => {
                self.slots.push(Some(slot));
                (self.slots.len() - 1) as u32
            }
        };
        self.slot_mut(at).older = self.head;
        self.slot_mut(at).newer = NIL;
        match self.head {
            NIL => self.tail = at,
            head => self.slot_mut(head).newer = at,
        }
        self.head = at;
        let slots = &self.slots;
        self.table.insert_unique(hash, at, |found| {
            table_hash(
                &slots[*found as usize]
                    .as_ref()
                    .expect("a linked slot is occupied")
                    .hash,
            )
        });
        self.bytes += weight;
        self.count += 1;
    }
}

/// The nodes every handle of one cache shares.
struct Store<Key, Value> {
    shards: Box<[Mutex<Shard<Key, Value>>]>,
    /// The bytes each shard may hold.
    budget: usize,
}

impl<Key, Value> Store<Key, Value> {
    fn shard(&self, hash: &Blake3Hash) -> &Mutex<Shard<Key, Value>> {
        // A byte the table hash does not read, so the nodes of one shard
        // still spread over its table.
        &self.shards[hash.as_bytes()[8] as usize % self.shards.len()]
    }
}

/// A cache of a tree's nodes by content hash.
///
/// It holds [`PersistentNode`]s rather than their bytes. A node exists only
/// once its bytes passed the archive check, so a node read from the cache
/// needs no check, and bytes that fail it never enter the cache. A node also
/// carries whatever it has derived from its bytes (decoded keys, key hashes,
/// its summary), so holding the node holds those too.
///
/// The cache is bounded by the bytes of the nodes it holds, not by how many
/// there are: nodes differ in size by orders of magnitude, so a count bounds
/// nothing. What is charged is a node's stored bytes; what it has derived
/// from them is not counted.
///
/// A cache may be shared by trees that do not hold the same nodes. Each
/// handle has a [`Scope`] and sees only the nodes its scope holds
/// ([`scoped`](Self::scoped)). A node two scopes hold is kept once.
///
/// Clones share the cache and the scope.
pub struct NodeCache<Key, Value> {
    store: Arc<Store<Key, Value>>,
    scope: Scope,
}

impl<Key, Value> Clone for NodeCache<Key, Value> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            scope: self.scope,
        }
    }
}

impl<Key, Value> Default for NodeCache<Key, Value> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Key, Value> std::fmt::Debug for NodeCache<Key, Value> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeCache")
            .field("nodes", &self.len())
            .field("bytes", &self.bytes())
            .field("scope", &self.scope)
            .finish()
    }
}

impl<Key, Value> NodeCache<Key, Value> {
    /// A cache holding up to [`NODE_CACHE_BUDGET`] bytes of nodes. Nothing
    /// is allocated until a node is kept.
    pub fn new() -> Self {
        Self::with_budget(NODE_CACHE_BUDGET)
    }

    /// A cache holding up to `budget` bytes of nodes. A node larger than
    /// the cache can keep is served to its reader and not kept.
    pub fn with_budget(budget: usize) -> Self {
        let shards = if budget >= SHARDED_FROM { SHARDS } else { 1 };
        Self {
            store: Arc::new(Store {
                shards: (0..shards).map(|_| Mutex::new(Shard::new())).collect(),
                budget: budget / shards,
            }),
            scope: Scope::default(),
        }
    }

    /// A handle on the same cache for `scope`: it sees the nodes `scope`
    /// holds and no others, and what it keeps, `scope` holds.
    pub fn scoped(&self, scope: Scope) -> Self {
        Self {
            store: self.store.clone(),
            scope,
        }
    }

    /// The scope this handle reads and writes for.
    pub fn scope(&self) -> Scope {
        self.scope
    }

    /// The cached node at `hash`, if this handle's scope holds it, with no
    /// IO.
    ///
    /// Lets a caller follow a read path as far as memory already covers it,
    /// so a read-ahead can name the node a descent would actually stop at
    /// instead of guessing.
    pub fn get_cached(&self, hash: &Blake3Hash) -> Option<PersistentNode<Key, Value>> {
        let mut shard = self.store.shard(hash).lock();
        let at = shard.find(hash)?;
        let slot = shard.slot_mut(at);
        if !slot.holders.holds(self.scope) {
            return None;
        }
        slot.visited = true;
        Some(slot.node.clone())
    }

    /// Keeps `node` under `hash` for this handle's scope, answering whether
    /// the cache did not hold it before.
    pub fn insert(&self, hash: Blake3Hash, node: PersistentNode<Key, Value>) -> bool {
        self.keep(hash, node).1
    }

    /// Keeps `node` under `hash` for this handle's scope and answers with
    /// the node the cache holds there, and whether it is new to the cache.
    ///
    /// A node the cache already holds under `hash` is the same bytes, with
    /// whatever it has derived from them since, so it is the one kept and
    /// returned: this scope is recorded as holding it too.
    fn keep(
        &self,
        hash: Blake3Hash,
        node: PersistentNode<Key, Value>,
    ) -> (PersistentNode<Key, Value>, bool) {
        let weight = node.size();
        let mut shard = self.store.shard(&hash).lock();
        if let Some(at) = shard.find(&hash) {
            let slot = shard.slot_mut(at);
            slot.holders.add(self.scope);
            slot.visited = true;
            return (slot.node.clone(), false);
        }
        if weight > self.store.budget {
            return (node, true);
        }
        while shard.bytes + weight > self.store.budget && shard.evict() {}
        shard.admit(Slot {
            hash,
            node: node.clone(),
            holders: Holders::One(self.scope),
            weight,
            visited: false,
            newer: NIL,
            older: NIL,
        });
        (node, true)
    }

    /// Retrieves a node from the cache, or fetches it using the provided
    /// function.
    ///
    /// The fetch is the caller's own: it runs inside this future and
    /// advances exactly when the caller polls, so the caller never depends
    /// on anyone else's progress. A fetch that fails leaves nothing behind.
    ///
    /// A node another scope holds is not a hit: the fetch runs, which is
    /// what establishes that this scope holds the node too. The node the
    /// cache already had is then the one returned.
    pub async fn get_or_fetch<F, E>(
        &self,
        hash: &Blake3Hash,
        fetcher: F,
    ) -> Result<Option<PersistentNode<Key, Value>>, E>
    where
        F: AsyncFnOnce(&Blake3Hash) -> Result<Option<PersistentNode<Key, Value>>, E>,
    {
        if let Some(node) = self.get_cached(hash) {
            return Ok(Some(node));
        }
        Ok(fetcher(hash)
            .await?
            .map(|node| self.keep(hash.clone(), node).0))
    }

    /// Forgets that this handle's scope holds anything. A node no other
    /// scope holds is dropped.
    pub fn release(&self) {
        for shard in self.store.shards.iter() {
            let mut shard = shard.lock();
            let mut at = shard.tail;
            while at != NIL {
                let slot = shard.slot_mut(at);
                let next = slot.newer;
                let held = slot.holders.remove(self.scope);
                if !held {
                    shard.take(at);
                }
                at = next;
            }
        }
    }

    /// How many nodes the cache holds, for every scope.
    pub fn len(&self) -> usize {
        self.store
            .shards
            .iter()
            .map(|shard| shard.lock().count)
            .sum()
    }

    /// Whether the cache holds no node.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes of the nodes the cache holds, for every scope.
    pub fn bytes(&self) -> usize {
        self.store
            .shards
            .iter()
            .map(|shard| shard.lock().bytes)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;

    use super::{NodeCache, Scope};
    use crate::{Delta, Entry, Manifest, PersistentNode, TransientNode, TransientSegment};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    type Node = PersistentNode<[u8; 4], Vec<u8>>;
    type Nodes = NodeCache<[u8; 4], Vec<u8>>;

    /// A leaf holding one entry whose value is `size` bytes of `fill`, so
    /// the node is a little over `size` bytes and distinct per `fill`.
    fn node(fill: u8, size: usize) -> Result<Node> {
        let entries = vec![Entry::new([fill; 4], vec![fill; size])];
        Ok(
            TransientNode::Segment(TransientSegment::new(entries, Vec::new())).persist(
                &mut Delta::zero(),
                &Manifest::default(),
                &Nodes::new(),
            )?,
        )
    }

    fn keep(cache: &Nodes, node: &Node) {
        cache.insert(node.hash().clone(), node.clone());
    }

    /// The bound is the bytes held, whatever the count: nodes are kept
    /// until their bytes pass the budget, and then the oldest goes.
    #[dialog_common::test]
    fn it_holds_nodes_up_to_its_byte_budget() -> Result<()> {
        let cache = Nodes::with_budget(10_000);
        let (a, b, c) = (node(1, 4_000)?, node(2, 4_000)?, node(3, 4_000)?);

        keep(&cache, &a);
        keep(&cache, &b);
        assert_eq!(cache.len(), 2);

        keep(&cache, &c);
        assert_eq!(cache.len(), 2, "a third node does not fit the budget");
        assert!(cache.bytes() <= 10_000);
        assert!(cache.get_cached(a.hash()).is_none(), "the oldest went");
        assert!(cache.get_cached(b.hash()).is_some());
        assert!(cache.get_cached(c.hash()).is_some());
        Ok(())
    }

    /// A node read since the hand last passed is stepped over once: the
    /// next oldest unread node goes in its place.
    #[dialog_common::test]
    fn it_keeps_a_node_that_was_read_over_one_that_was_not() -> Result<()> {
        let cache = Nodes::with_budget(10_000);
        let (a, b, c) = (node(1, 4_000)?, node(2, 4_000)?, node(3, 4_000)?);
        keep(&cache, &a);
        keep(&cache, &b);
        assert!(cache.get_cached(a.hash()).is_some());

        keep(&cache, &c);

        assert!(cache.get_cached(a.hash()).is_some(), "the read node stays");
        assert!(cache.get_cached(b.hash()).is_none(), "the unread one went");
        assert!(cache.get_cached(c.hash()).is_some());
        Ok(())
    }

    /// A node larger than the cache can keep is still handed to its reader.
    #[dialog_common::test]
    async fn it_serves_a_node_it_cannot_keep() -> Result<()> {
        let cache = Nodes::with_budget(1_000);
        let large = node(1, 4_000)?;

        let read = cache
            .get_or_fetch(large.hash(), async |_| Ok::<_, ()>(Some(large.clone())))
            .await
            .expect("the fetch succeeds");

        assert!(read.is_some());
        assert!(cache.is_empty());
        Ok(())
    }

    /// A handle sees only what its own scope holds. A node another scope
    /// keeps is not a hit: the read fetches, and the fetch is what makes
    /// this scope a holder.
    #[dialog_common::test]
    async fn it_answers_a_scope_only_with_nodes_that_scope_holds() -> Result<()> {
        let cache = Nodes::new();
        let ours = cache.scoped(Scope::named(b"ours"));
        let theirs = cache.scoped(Scope::named(b"theirs"));
        let shared = node(1, 100)?;

        keep(&theirs, &shared);
        assert!(theirs.get_cached(shared.hash()).is_some());
        assert!(
            ours.get_cached(shared.hash()).is_none(),
            "a node another scope holds must not answer this one"
        );

        let mut fetched = false;
        let read = ours
            .get_or_fetch(shared.hash(), async |_| {
                fetched = true;
                Ok::<_, ()>(Some(node(1, 100).expect("the node builds")))
            })
            .await
            .expect("the fetch succeeds")
            .expect("the node is found");

        assert!(fetched, "the read must go to this scope's own source");
        assert!(ours.get_cached(shared.hash()).is_some());
        assert_eq!(cache.len(), 1, "a node two scopes hold is kept once");
        assert_eq!(
            read.buffer().as_ref().as_ptr(),
            shared.buffer().as_ref().as_ptr(),
            "the node the cache already had is the one handed back"
        );
        Ok(())
    }

    /// Releasing a scope forgets what it held: a node only it held is
    /// dropped, and one another scope holds too stays for that scope.
    #[dialog_common::test]
    fn it_drops_what_only_a_released_scope_held() -> Result<()> {
        let cache = Nodes::new();
        let ours = cache.scoped(Scope::named(b"ours"));
        let theirs = cache.scoped(Scope::named(b"theirs"));
        let (only_ours, both) = (node(1, 100)?, node(2, 100)?);
        keep(&ours, &only_ours);
        keep(&ours, &both);
        keep(&theirs, &both);

        ours.release();

        assert!(ours.get_cached(only_ours.hash()).is_none());
        assert!(ours.get_cached(both.hash()).is_none());
        assert!(theirs.get_cached(both.hash()).is_some());
        assert_eq!(cache.len(), 1);
        Ok(())
    }

    /// The queue survives being emptied and refilled: slots are reused and
    /// the bytes held come back to exactly what is kept.
    #[dialog_common::test]
    fn it_accounts_for_exactly_the_nodes_it_holds() -> Result<()> {
        let cache = Nodes::with_budget(9_000);
        let nodes: Vec<Node> = (0..40u8)
            .map(|fill| node(fill, 1_000 + usize::from(fill) * 50))
            .collect::<Result<_>>()?;

        for node in &nodes {
            keep(&cache, node);
        }

        let held: Vec<&Node> = nodes
            .iter()
            .filter(|node| cache.get_cached(node.hash()).is_some())
            .collect();
        assert_eq!(held.len(), cache.len());
        assert_eq!(
            held.iter()
                .map(|node| node.buffer().as_ref().len())
                .sum::<usize>(),
            cache.bytes()
        );
        assert!(cache.bytes() <= 9_000);

        cache.release();
        assert!(cache.is_empty());
        assert_eq!(cache.bytes(), 0);
        Ok(())
    }
}
