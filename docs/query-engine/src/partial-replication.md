# Partial Replication

Dialog is local-first. The query engine operates as if all data were local, but
the storage layer fetches missing data on demand.

## Transparent Fetching

The search tree may be **sparse**: some nodes exist locally, others only on a
remote peer or blob store. When traversal reaches a missing node, the storage
backend fetches it transparently:

```
tree.stream_range(start..end)
    → load child by hash
    → storage.read(hash)
        ├── local cache hit → return immediately
        └── cache miss → fetch from remote → store locally → return
```

This works because every node is identified by its content hash. The tree
traversal doesn't care where bytes come from.

Replication is driven by **query access patterns**: a query for `person/name`
of entity `alice` fetches only tree nodes along that path. Nodes for unqueried
attributes remain unfetched.

## Layered Storage

```
Cache Backend          In-memory LRU for hot nodes
Compression Backend    Compress/decompress on the fly
Journal Backend        Write-ahead log for durability
Primary Backend        Filesystem or IndexedDB
Transfer Backend       Fetch from remote on cache miss
Remote Backend         S3, R2, or peer connection
```

The transfer layer intercepts `read()` calls: if the primary returns `None`,
it fetches from remote and caches locally.

## Differential Synchronization

For bulk sync (not query-driven), the search tree provides a diff algorithm:

1. Compare root hashes — if equal, trees are identical
2. Where hashes differ, load both sides and compare children
3. Matching children (same hash) are skipped entirely
4. Yield additions/removals, apply to the receiving side

This minimizes transfer: only differing subtrees are loaded.

## Index Layout and Replication

- **EAV keys** group by entity — replicating one entity fetches a contiguous
  range, usually one or two segments
- **AEV keys** group by attribute — querying one attribute fetches a contiguous
  range of that portion
- Different index portions occupy different key-space regions (different tag
  bytes), so fetching EAV data doesn't require AEV or VAE data

## Convergence

The search tree is deterministic (same data = same structure) and
content-addressed (same content = same hash). All replicas receiving the same
claims converge to the same tree.
