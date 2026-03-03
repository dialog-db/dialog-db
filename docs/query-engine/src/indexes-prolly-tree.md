# Indexes and the Prolly Tree

This chapter explains how claims are stored and indexed, and how the query
engine reaches into the storage layer to find matching claims.

## The Storage Stack

```
┌─────────────────────────────────────────────────────┐
│  Query Engine (dialog-query)                        │
│  RelationQuery → ArtifactSelector → select()        │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Artifacts (dialog-artifacts)                       │
│  Index management, claim storage, selector routing  │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Prolly Tree (dialog-prolly-tree)                   │
│  Content-addressed B-tree with range queries        │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Storage Backend (dialog-storage)                   │
│  Memory, filesystem, S3, IndexedDB                  │
└─────────────────────────────────────────────────────┘
```

## The Prolly Tree

A **probabilistic B-tree** (prolly tree) is a content-addressed tree structure
where the shape is determined by the data, not by insertion order. This makes
it **deterministic** — the same set of key-value pairs always produces the
same tree, regardless of how they were inserted.

### How It Works

Each key gets a **rank** (1–254) computed from its Blake3 hash using a
geometric distribution:

```rust
fn rank(key: &Key) -> Rank {
    let hash = blake3::hash(key.bytes());
    compute_geometric_rank(hash.as_bytes(), BRANCH_FACTOR)
}
```

Keys with higher ranks cause the tree to branch. This produces a balanced
tree without explicit rebalancing:

- **Segments (leaves)**: Contain entries grouped by rank. Most entries have
  rank 1 and are stored together in segments.
- **Branches (interior nodes)**: Contain references to child nodes. A branch
  is created when a key's rank exceeds the current level.

### Content Addressing

Every node (branch or segment) is serialized and stored by its Blake3 hash:

```
Node → serialize (CBOR) → hash (Blake3) → store(hash, bytes)
```

This means:
- **Identity = content**: Two identical subtrees have the same hash
- **Structural sharing**: Common subtrees are stored once
- **Integrity**: Any modification changes the hash, detectable immediately

### Tree Operations

- **`get(key)`**: Navigate from root to leaf following the key's path
- **`set(key, value)`**: Insert or update, creating new nodes along the path
- **`stream_range(start..end)`**: Iterate over entries in key order

Range queries are the primary access pattern for the query engine.

## The Composite Key

All claims are stored in a single prolly tree using a **162-byte composite
key**:

```
┌─────┬──────────┬─────────────┬───────────┬─────────────────┐
│ Tag │ Entity   │ Attribute   │ ValueType │ ValueReference   │
│ 1B  │ 64B      │ 64B         │ 1B        │ 32B              │
└─────┴──────────┴─────────────┴───────────┴─────────────────┘
```

- **Tag** (1 byte): Identifies the index variant (EAV, AEV, VAE)
- **Entity** (64 bytes): Entity identifier, padded
- **Attribute** (64 bytes): Attribute in `domain/name` format, padded
- **ValueType** (1 byte): Type discriminant (String, Int, Bool, etc.)
- **ValueReference** (32 bytes): Blake3 hash of the serialized value

## Three Indexes, One Tree

The same claim is stored **three times** in the tree, each under a different
key layout. The tag byte determines the sort order:

### EAV Index (Entity → Attribute → Value)

```
Key: [EAV_TAG, Entity, Attribute, ValueType, ValueRef]
```

Optimized for: "What properties does entity X have?"

Claims for the same entity are contiguous. Within an entity, claims are
sorted by attribute, then by value.

### AEV Index (Attribute → Entity → Value)

```
Key: [AEV_TAG, Attribute, Entity, ValueType, ValueRef]
```

Optimized for: "Which entities have attribute Y?"

Claims for the same attribute are contiguous. This is the most common access
pattern for queries like `(person/name, ?person, ?name)` where only the
attribute is known.

### VAE Index (Value → Attribute → Entity)

```
Key: [VAE_TAG, ValueRef, Attribute, Entity, ...]
```

Optimized for: "Which entities have value Z for attribute Y?"

Used when only the value is known. Less common but necessary for reverse
lookups.

## Index Selection

The `Artifacts::select()` method chooses the best index based on what's
constrained in the selector:

```rust
fn select(&self, selector: ArtifactSelector<Constrained>) -> impl Stream {
    if selector.entity().is_some() {
        Self::scan_eav(...)      // Entity known → use EAV
    } else if selector.attribute().is_some() {
        Self::scan_aev(...)      // Attribute known → use AEV
    } else {
        Self::scan_vae(...)      // Only value known → use VAE
    }
}
```

The priority is EAV > AEV > VAE because:
- EAV scans are most constrained (entity narrows to a small set)
- AEV scans cover all entities for one attribute
- VAE scans are the least constrained (value hashes are evenly distributed)

## From Query to Scan

When a `RelationQuery` is evaluated:

1. **Resolve terms**: Replace bound variables with their values from the answer
2. **Build selector**: Create an `ArtifactSelector` with known constraints
3. **Choose index**: The artifacts layer picks EAV, AEV, or VAE
4. **Build range**: Construct start/end keys for the range scan
5. **Stream results**: `tree.stream_range(start..end)` yields matching entries
6. **Decode entries**: Parse composite keys back into `(the, of, is, cause)` claims

### Range Construction

The composite key layout enables efficient range queries. For example, to find
all claims for entity `alice` with attribute `person/name`:

```
Start: [EAV_TAG, alice, person/name, 0x00, 0x00...]
End:   [EAV_TAG, alice, person/name, 0xFF, 0xFF...]
```

This scans exactly the segment of the tree containing matching claims.

## Write Path

When claims are committed through a transaction:

1. **Build instructions**: Each assertion/retraction becomes an `Instruction`
2. **Compute keys**: Generate EAV, AEV, and VAE keys for each claim
3. **Apply to tree**: Insert (assert) or mark as removed (retract) in the
   prolly tree
4. **Persist**: New/modified nodes are written to the storage backend
5. **Update root**: The tree root hash changes, reflecting the new state

Because the prolly tree is content-addressed, only modified nodes need to be
written — unchanged subtrees keep their existing hashes and aren't touched.

## Performance Characteristics

| Access pattern | Index | Cost level |
|----------------|-------|-----------|
| Entity + Attribute + Value | EAV | `SEGMENT_READ_COST` (100) |
| Entity + Attribute | EAV | `SEGMENT_READ_COST` (100) |
| Attribute only | AEV | `RANGE_SCAN_COST` (1,000) |
| Value + Attribute | VAE | `RANGE_SCAN_COST` + penalty |
| Value only | VAE | `INDEX_SCAN` (5,000) |

These cost levels directly correspond to the planner's cost constants, ensuring
that the planner's ordering decisions reflect actual I/O patterns.
