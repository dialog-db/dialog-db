# Partial Replication

Dialog is a local-first database. The query engine operates as if all data
were available locally, but the storage layer fetches missing data on demand.
This chapter explains how partial replication works and how query evaluation
drives it.

## The Abstraction

From the query engine's perspective, the prolly tree is a local data
structure. It calls `tree.stream_range(start..end)` and gets back a stream of
entries. The query engine has no knowledge of replication.

But in practice, the tree may be **sparse**: some nodes exist locally while
others exist only on a remote peer or blob store. When the tree traversal
reaches a node that isn't in local storage, the storage backend fetches it
transparently.

```
Query evaluation
    |
    v
tree.stream_range(start..end)
    |
    v  traverse branch node
    |
    v  load child by hash
storage.read(hash)
    |
    +-- Local cache hit -> return immediately
    |
    +-- Cache miss -> fetch from remote
        |
        v
    Remote blob store (S3, R2, peer)
        |
        v
    Store locally + return
```

## Content-Addressed Transparency

This transparency works because of content addressing:

1. **Every node is identified by its content hash.** A branch node contains
   references to children as `(upper_bound, hash)` pairs.

2. **Reading a node is just `storage.read(hash)`.** The storage backend
   decides whether to serve from local cache or fetch remotely.

3. **The tree traversal doesn't care where the data comes from.** It just
   needs the bytes for a given hash.

This means replication is driven by **query access patterns**:
- A query that reads `person/name` for entity `alice` will fetch only the
  tree nodes along the path to that specific entry.
- A query that scans all `person/name` entries will fetch the nodes covering
  that attribute range.
- Nodes for unqueried attributes or entities remain unfetched.

## Layered Storage Backends

The storage system uses composable backends:

```
+-------------------------+
|  Cache Backend          |  In-memory LRU for hot nodes
+-------------------------+
|  Compression Backend    |  Compress/decompress on the fly
+-------------------------+
|  Journal Backend        |  Write-ahead log for durability
+-------------------------+
|  Primary Backend        |  Filesystem or IndexedDB
+-------------------------+
|  Transfer Backend       |  Fetch from remote on cache miss
+-------------------------+
|  Remote Backend         |  S3, R2, or peer connection
+-------------------------+
```

Each layer implements `ContentAddressedStorage`:

```rust
pub trait ContentAddressedStorage {
    async fn read<T>(&self, hash: &Hash) -> Result<Option<T>, Error>;
    async fn write<T>(&mut self, block: &T) -> Result<Hash, Error>;
}
```

The transfer layer intercepts `read()` calls: if the primary backend returns
`None`, it fetches from the remote backend and caches the result locally.

## Differential Synchronization

For bulk synchronization (not query-driven), the prolly tree provides a
**differential sync** algorithm:

```rust
impl Tree {
    pub fn differentiate(&self, other: &Self)
        -> impl Stream<Item = Result<Change<K, V>>>;

    pub async fn integrate<Changes>(&mut self, changes: Changes)
        -> Result<(), Error>;
}
```

### The Algorithm

1. **Start with roots**: Compare the two trees' root hashes. If equal, they're
   identical, no sync needed.

2. **Expand differences**: Where hashes differ, load both sides and compare
   children. Matching children (same hash) are skipped entirely.

3. **Collect changes**: Entries present in one tree but not the other are
   yielded as `Add` or `Remove` changes.

4. **Apply changes**: The receiving side integrates changes into its tree.

### Sparse Trees

The differential algorithm uses `SparseTree`, a lazy representation that
starts with just root references and expands nodes on demand:

```rust
pub struct SparseTree {
    nodes: Vec<SparseTreeNode>,    // current frontier
    expanded: Vec<Node>,           // newly loaded branches
    storage: &Storage,
}
```

- **`expand(range)`**: Load branch nodes in a range, populating `expanded`
- **`prune(other)`**: Remove nodes present in both trees (same hash)
- After pruning, only **novel nodes** (unique to one tree) remain

This minimizes data transfer: only the subtrees that actually differ are
loaded and compared.

## How Indexes Enable Efficient Replication

The composite key layout (EAV, AEV, VAE) interacts with replication in an
important way:

- **EAV keys** group claims by entity. Replicating all data for one entity
  means fetching a contiguous range of tree nodes, likely just one or two
  segments.

- **AEV keys** group claims by attribute. Querying one attribute type across
  all entities fetches a contiguous range of the AEV portion of the tree.

- Different index portions occupy different parts of the key space (different
  tag bytes), so fetching EAV data doesn't require fetching AEV or VAE data.

## Implications for Query Design

Since queries drive replication, the cost of a query includes potential network
I/O:

1. **First query is expensive**: Fetching nodes from remote storage adds
   latency. Subsequent queries benefit from the local cache.

2. **Selective queries are cheaper**: A query constrained by entity fetches
   fewer nodes than a full attribute scan.

3. **The planner doesn't optimize for replication**: Cost estimation reflects
   tree traversal complexity, not network latency. This is intentional. The
   planner produces a good logical order, and the storage layer handles the
   physical I/O.

4. **Background sync reduces first-query cost**: Applications can pre-populate
   the local cache by syncing specific subtrees before queries arrive.

## Convergence

Because the prolly tree is deterministic (same data = same structure) and
content-addressed (same content = same hash), all replicas that receive the
same set of claims converge to the same tree. This is a property needed by the
CRDT-based conflict resolution model.
