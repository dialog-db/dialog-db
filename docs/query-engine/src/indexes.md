# Indexes and the Search Tree

This chapter explains how claims are stored and indexed, and how the query
engine finds matching claims.

## The Storage Stack

```
┌───────────────────────────────────────────┐
│  Query Engine (dialog-query)              │
│  AttributeQuery → ArtifactSelector        │
├───────────────────────────────────────────┤
│  Artifacts (dialog-artifacts)             │
│  Index management, selector routing       │
├───────────────────────────────────────────┤
│  Search Tree (dialog-prolly-tree)         │
│  Content-addressed B-tree, range queries  │
├───────────────────────────────────────────┤
│  Storage Backend (dialog-storage)         │
│  Memory, filesystem, S3, IndexedDB        │
└───────────────────────────────────────────┘
```

## The Probabilistic Search Tree

A content-addressed B-tree where the shape is determined by the data, not
insertion order. The same set of key-value pairs always produces the same tree.

Each key gets a **rank** (1–254) from a geometric distribution over its Blake3
hash. Keys with higher ranks cause the tree to branch. This produces a balanced
tree without explicit rebalancing.

Every node is serialized and stored by its Blake3 hash:

- **Identity = content** — identical subtrees share the same hash
- **Structural sharing** — common subtrees are stored once
- **Integrity** — any modification changes the hash

## The Composite Key

All claims are stored using a **162-byte composite key**:

```
┌─────┬──────────┬─────────────┬───────────┬────────────────┐
│ Tag │ Entity   │ Attribute   │ ValueType │ ValueReference │
│ 1B  │ 64B      │ 64B         │ 1B        │ 32B            │
└─────┴──────────┴─────────────┴───────────┴────────────────┘
```

## Three Indexes, One Tree

The same claim is stored **three times** under different key layouts. The tag
byte determines sort order:

| Index | Key order | Optimized for |
|-------|-----------|---------------|
| **EAV** | Entity → Attribute → Value | "What does entity X have?" |
| **AEV** | Attribute → Entity → Value | "Which entities have attribute Y?" |
| **VAE** | Value → Attribute → Entity | "Who has value Z?" |

### Index Selection

```rust
if selector.entity().is_some() {
    scan_eav(...)      // Entity known → EAV
} else if selector.attribute().is_some() {
    scan_aev(...)      // Attribute known → AEV
} else {
    scan_vae(...)      // Only value known → VAE
}
```

Priority is EAV > AEV > VAE: entity scans are most constrained, VAE scans
least.

## From Query to Scan

1. Resolve bound variables into constants
2. Build an `ArtifactSelector` with known constraints
3. Choose index (EAV, AEV, or VAE)
4. Construct start/end keys for range scan
5. Stream matching entries from the tree
6. Decode composite keys back into claims

## Performance

| Access pattern | Index | Cost |
|----------------|-------|------|
| Entity + Attribute + Value | EAV | `SEGMENT_READ_COST` (100) |
| Entity + Attribute | EAV | `SEGMENT_READ_COST` (100) |
| Attribute only | AEV | `RANGE_SCAN_COST` (1,000) |
| Value + Attribute | VAE | `RANGE_SCAN_COST` + penalty |
| Value only | VAE | `INDEX_SCAN` (5,000) |
