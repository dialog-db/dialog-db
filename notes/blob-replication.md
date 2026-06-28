# Blobs: storage, replication, and access

Status: design record

## Goal

Store binary blobs in the database, keep them whole and hash-addressable, and
have them replicate to a remote through the same mechanism that already
replicates the rest of the data — without inlining blob bytes into the index and
without a bespoke side protocol for "which blobs need uploading."

Blobs must also be usable as self-contained resources: a blob should be loadable
behind an `<img>`/`<video>` source or a CSS `url(...)`, which means it must be
retrievable as a single contiguous byte range, range-readable, with no assembly
step on the read path.

## Identity

A blob is identified by the BLAKE3 root hash of its complete byte content. This
hash is a BAO root (BLAKE3's verified-streaming Merkle root), so verified
streaming and verified range reads are intrinsic properties of the identity and
become available without changing it.

A blob is referenced everywhere by this hash:

- as an entity (`blob:<hash>`), so ordinary facts can attach extrinsic metadata
  (content type, file name, captions, …) to it through the regular triple store;
- as a key in the blob index (below), which carries intrinsic, content-derived
  metadata and drives replication.

These two are complementary: extrinsic, user-asserted metadata lives in facts;
intrinsic, content-derived metadata lives in the blob index.

## The blob index

The triple store maintains three index orderings over one prolly tree,
distinguished by a leading key tag: EAV (`0`), AEV (`1`), VAE (`2`). Two further
tags are allocated after them:

- `HISTORY = 3` — reserved for a future history index; unused for now.
- `BLOB = 4` — the **blob index** (the fifth index).

The blob index keys on the blob hash under the `BLOB` tag and fits the existing
fixed key layout (tag byte, blob hash in the value-reference field, remainder
zeroed). Its value is a **versioned, extensible record**. Today it holds the
blob size; the version prefix leaves room to add fields (e.g. an outboard
reference, a kind discriminant) without migration.

Because the blob index is an ordinary part of the tree, the set of referenced
blobs is a contiguous, deduplicated key range — one entry per distinct blob —
and any change to it is visible to the tree differential. The blob index value
being the size lets a size query be answered from the index alone, with no blob
fetch.

The blob bytes themselves never enter the tree. Only the hash (as a key) and the
small intrinsic record (as a value) do, so index nodes stay small regardless of
blob size.

## Layered architecture

The design separates a logical layer (replicated, identical across peers) from a
physical layer (per-peer storage representation).

| Layer | Concern | Contents |
|---|---|---|
| 1. Identity & references | logical, replicated | blob hash; `blob:<hash>` entity; blob-index entry `hash → {version, size, …}` |
| 2. Physical storage | per-peer | one contiguous content-addressed object per blob; transfer state (outboard, coverage bitmap, partial/complete) |
| 3. Transfer | protocol | ingest (hash-discovered); replication write (hash-known, verified); read (hash-addressed, ranged, lazy) |
| 4. Access & serving | presentation | query predicates; resource URLs resolved from a hash to bytes |

The logical layer is what the differential sees and what a revision points at.
The physical layer can differ between peers (one peer has a blob complete,
another not yet) without changing any logical state.

## Blob lifecycle and transfer

Bytes move through three operations, distinguished by whether the hash is known
and by direction: **write** (ingest, hash discovered), **import** (replication
of a known blob into a store), and **read**.

### Write — ingest, hash discovered

Content enters the system through `write`, where the hash is **discovered**, not
supplied. The caller provides a byte source (a forward read stream); the
implementation persists it while hashing in a single pass and returns the blob
hash `H`. The content is written to a temporary location during the pass and
then placed at its `H` address, so a forward stream is sufficient and no full
copy is held in memory.

`write` is a **local** operation: the blob lands in the local blob store and a
blob-index entry is added. Because the hash is not known ahead of time, it is not
a hash-addressed wire operation; its authorization is the coarse "may add blobs,"
not "may write content `H`." Remote providers do not implement `write` — content
reaches a remote by being written locally and then imported.

### Import — replication of a known blob

By the time a blob propagates to a remote, `H` is already known (from `write`)
and recorded in the blob index. `import` is therefore **hash-ahead**: a
capability invocation commits to `H`, the size, and the per-part hashes, and the
bytes move directly to the store. The client splits the blob into ordered parts;
each part is uploaded under a checksum pinned to its hash, in parallel, then the
object is completed. A single-part blob degenerates to one direct upload.

Integrity is anchored at both ends: each part is validated by the store against
the per-part hash the capability authorized, and the assembled object is
addressed by `H`, which a reader verifies against the content. The destination is
not trusted to assemble correctly — a mis-assembly yields an object that fails to
verify as `H`.

These transfer parts are an upload concern, distinct from any verification
chunking (e.g. BAO) used on the read side.

Partial/complete state is confined to the physical layer. Combined with the
invariant that a revision is published only after the blobs it references are
complete, partial blobs are never observable through a published revision.

### Read

Reads are hash-addressed and lazy: a blob is fetched by `H` on demand, and a
peer that has the index but not a blob's bytes retrieves them from the remote
when needed. Stored blobs are contiguous and seekable, so range reads (for media
seeking, for slicing, for verified ranges) are served directly without assembly.

## Replication

Push diffs the tree once. The node-level view of the difference replicates the
tree blocks, as it does for the other indexes. The entry-level view of the same
difference, restricted to the `BLOB` tag, names exactly the blobs newly
referenced relative to the synchronization checkpoint — i.e. the blobs the remote
does not yet have under fast-forward. Push uploads those blobs (by the transfer
protocol above) before publishing the revision that references them.

The archive is append-only. Blobs are never deleted from a remote; reclaiming
unreferenced blobs is a local concern and is out of scope here. Consequently the
blob index, from the archive's perspective, only grows, and there is no
cross-peer deletion to converge.

## Capability surface

The archive holds two kinds of content-addressed, append-only store. They are
distinct capability surfaces — their effects differ — so they are distinct types
under `Archive`, not one type parameterized by a catalog name. (Effects bind to
their store by an associated type, so the store kind must be a type.)

```
Subject
  └── Archive                         /archive
        ├── Block                     /archive/block   (whole-buffer blocks)
        │     ├── Get { digest }          → /archive/block/get    → Option<Buffer>
        │     ├── Put { block }           → /archive/block/put    → ()
        │     └── Import { blocks }       → /archive/block/import → ()
        └── Blob                      /archive/blob    (streaming, hash-addressed blobs)
              ├── Write                   → /archive/blob/write   → BlobSink    (local only)
              ├── Import { digest,
              │            chunks, size } → /archive/blob/import  → transfer grant
              └── Read { digest, range }  → /archive/blob/read    → BlobReader
```

The signed capability carries only hash-addressed metadata (`digest`, per-part
`chunks`, `size`, `range`); blob bytes never travel inside an effect. A blob
effect's *output* is a transfer handle (`BlobReader`/`BlobSink`) or a set of
upload grants, and bytes flow against that handle.

- **`Write`** — ingest (above). Local providers only; coarse, content-free
  authorization because no hash exists yet.
- **`Import { digest, chunks, size }`** — content-bound replication write. The
  `chunks` are the per-part hashes; their count selects the transfer:
  `chunks.len() == 1` is a single direct upload, more is a multipart upload.
- **`Read { digest, range }`** — content-bound; ranged.

### Import over presigned S3

A remote backed by S3 (behind a UCAN access service that holds the credentials)
realizes `import` as a single authorization that hands the client everything it
needs to move the bytes itself:

1. The client redeems one `import(digest, chunks, size)` invocation.
2. The service verifies the capability, then for a multipart upload makes the one
   unavoidable S3 call — `CreateMultipartUpload` (metadata only) — to obtain the
   upload id. A single-part upload needs no S3 call at all.
3. The service presigns the upload URLs — one per part, each with the part's hash
   pinned as a signed `x-amz-checksum-sha256` header so S3 rejects bytes that do
   not match — plus a presigned completion URL, and returns them.
4. The client uploads the parts directly to S3 in parallel and completes the
   upload itself.

The byte path never crosses the service; its only S3 interaction is the lone
`CreateMultipartUpload`, and completion is client-driven, so no separate
completion capability is needed. Resumption re-presigns and re-uploads only the
missing parts. (Per-part hashes are the upload checksums; the object's identity
remains the blake3/BAO `digest`, which S3 does not itself verify against the
assembled bytes.)

The exact effect shapes and the `BlobSink`/`BlobReader`/grant types are firmed up
in the write PR.

## Query surface

- `blob/size(hash) → size` — answered from the blob-index value; no blob fetch.
- `blob/slice(hash, start, end) → blob` — returns an opaque blob handle (a view
  over a range of the parent), mirroring `Blob.slice`. Reading bytes from a
  handle is a streaming/serving concern, not a query binding.

Reading blob bytes directly within a query (binding bytes into a result row) is
not a current need and is deliberately left open; the query surface binds only
small values (a hash, a size, a handle).

## Serving blobs as resources

A blob is addressed by hash, so turning `…/<hash>` into bytes for a browser is a
presentation layer on top of the storage model. A service worker can resolve such
a request from local storage and lazily hydrate missing bytes (verified) from the
archive, giving self-contained resource URLs without a dedicated server. The
storage model is agnostic to which mechanism serves the bytes; this layer is out
of scope for the initial work.

## Load-bearing decisions

Two decisions must be made correctly at the outset because later capabilities
depend on them; both are cheap to honor immediately.

1. **A blob's identity is the standard BLAKE3/BAO root of its complete bytes.**
   No bespoke digest. This is what makes verified streaming and verified ranges
   available later without changing identities or references.
2. **The blob-index value is a versioned, extensible record** (holding the size
   initially). New intrinsic fields can be added without migration.

Given these, the remaining choices (single-part versus multipart transfer, BAO
verification, serving mechanism, slice handle semantics) can change later without
disturbing the logical model or stored identities.

## Delivery plan

1. **Tree support.** Allocate the `HISTORY` and `BLOB` tags; add the blob index
   (`hash → {version, size}`) with local read/write and membership. No network or
   capability surface.
2. **Replication.** Push ships newly-referenced blobs to the remote blob store
   via `import`, driven by the entry-level difference over the `BLOB` tag; lazy
   read-back on the remote. Single-part import is sufficient here and is
   forward-compatible with multipart (identity is the same `H` either way).
3. **Query engine.** `blob/size` and `blob/slice`, plus the read/ingest surface.

Deferred, and explicitly not blocked by the above: BAO verified streaming and
verified ranges; blob composition and verified sub-blob addressing; reading blob
bytes within queries; service-worker resource serving.
