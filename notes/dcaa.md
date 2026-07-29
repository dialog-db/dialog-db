# Dialog Content-Addressed Archive

## Overview

A Dialog Content-Addressed Archive (DCAA) is a single-file store where the
address of every value is derived from the value itself, specifically the
BLAKE3 hash of the blob. This has two important consequences:

- **Write-once**: if the same content is written twice, the address is
  identical and the second write is a no-op
- **Self-verifying**: any retrieved value can be verified against its address
  without trusting the storage medium

DCAA files use the `.dialog` extension.

The design priority is simplicity and a strict append-only invariant: **no
byte in the file is ever mutated after it is written**. A small fixed header
is written once at file creation for format identification. All subsequent
writes are pure appends. The current committed state is always found at the
tail of the file.

## Dependencies

### BLAKE3

BLAKE3 produces a 32-byte digest and internally builds a binary Merkle tree
over 1024-byte chunks; the root hash is what BLAKE3 normally returns.
Reference: https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf

### BAO Outboard Encoding

BAO (BLAKE3 Verified Streaming) stores the internal Merkle tree separately
from the content:

1. **The outboard tree**: an 8-byte little-endian length header followed by
   all parent nodes in pre-order; each parent is 64 bytes (two 32-byte
   child chaining values). Leaf chunks are not included.
2. **The content**: the raw blob bytes, unchanged.

```
outboard_size(n) = 8 + 64 * num_parent_nodes(n)
num_parent_nodes(n) = if n <= 1024 { 0 } else { 2 * next_power_of_two(ceil(n / 1024)) - 2 }
```

The BLAKE3 hash of the content is identical to the BAO root hash derived
from the outboard tree: the address in the DCAA index is simultaneously the
blob hash and the verification root for any byte range, verified in
O(log(n / 1024)). Reference: https://github.com/oconnor663/bao; recommended
crate: `bao-tree` (https://github.com/n0-computer/bao-tree).

## Goals

- Strictly append-only: no byte is ever overwritten after being written
- O(log N) lookup via binary search over a sorted flat index
- Verified reads via BAO outboard encoding, no full rehash on retrieval
- Redaction: entries can be marked redacted without deleting content
- Single file with `.dialog` extension, portable across platforms
- Crash-safe: a crash at any point leaves the last committed state intact
- Simple enough to implement without a page manager, buffer cache, or WAL

## Non-Goals

- Multiple concurrent writers
- Physical deletion of content
- Multiple tables or namespaces
- Network replication (handled by the Dialog search tree layer above)

## File Layout

```
Header (8 bytes, written once at creation)
Commit 0: blob records, index, footer (40 bytes)
Commit 1: blob records, index, footer
...
```

Each commit appends zero or more blob records, a complete merged index, and
a 40-byte footer — the commit point.

## Header

8 bytes at offset 0, written once: magic `"DCAA"` (4), version u16 = 1 (2),
reserved zero (2).

## Entries

An entry is a `(hash, offset)` pair. `offset > 0` points at a blob record;
`offset = 0` is the redaction sentinel (safe because the header occupies
bytes 0..8). Redaction does not remove bytes; it records that the hash must
be treated as absent by readers and rejected by writers.

## Blob Record

```
[ blob_len     : u64 LE ]  8 bytes   raw blob length
[ outboard_len : u64 LE ]  8 bytes   BAO outboard tree length
[ outboard     : bytes  ]            BAO outboard tree
[ blob         : bytes  ]            raw blob bytes
```

The index offset points at the first byte of the record.

## Index

A flat array of 40-byte records (32-byte hash + u64 LE offset), sorted
lexicographically by hash, covering all entries ever committed including
redacted ones. Each commit writes a complete merged index (prior index plus
this commit's entries, new entry wins on collision); prior index bytes
become unreachable and are reclaimed by compaction.

## Footer

40 bytes, always the last bytes of a commit:

```
0   4  magic         "DCAA"
4   2  version       1
6   2  reserved      0
8   8  entry_count   total committed entries (incl. redacted)
16  8  index_offset  offset of this commit's index
24  8  index_count   number of 40-byte index records
32  8  checksum      BLAKE3 hash of footer bytes 0..32
```

A footer is valid iff the magic matches and the checksum verifies.

## Writing Algorithm

Create: write the 8-byte header, flush. Commit a transaction of pending
inserts and redactions:

1. Load the prior footer's index (empty if none).
2. Resolve pending entries: duplicate blob inserts are no-ops; inserting a
   redacted address fails with `Redacted` (raised by `insert`, before
   commit); redactions of already-redacted addresses are no-ops.
3. Append each new blob record (hash, outboard, bytes).
4. Build the merged sorted index (prior + new; new wins per hash).
5. Append the merged index; note its offset.
6. Append the footer (counts, offset, checksum).
7. fsync. The footer is the new committed state.

## Opening and Reading Algorithm

Open: verify header magic/version; an 8-byte file is an empty store;
otherwise find the last valid footer (see Crash Recovery) and load its
index (`index_count * 40` bytes at `index_offset`).

Lookup: binary search the index; missing → `NotFound`; offset 0 →
`Redacted`; otherwise an `Entry { address, offset, blob_len, outboard_len }`.

Read: seek to the record, read outboard then blob, verify against the
address via BAO; failure → `Corrupt`. Range read: use the outboard tree to
verify only the chunks overlapping the range, reading only the needed tree
nodes and content bytes.

## Crash Recovery

All writes are appends, so bytes belong either to a complete prior commit or
an incomplete tail. Find the last valid footer: check `file_len - 40`; if
invalid, scan backwards for `"DCAA"` candidates and checksum-verify each;
truncate the file after the first valid footer. In practice the scan
terminates within a few bytes; the checksum eliminates false magic matches.
No valid footer anywhere → empty store or error by policy.

## Compaction

Rewrite into a temporary file: header, then every live entry in hash order
(redacted entries keep `(hash, 0)` with no bytes), new index, footer, flush,
then atomically `rename` over the original. Readers holding the old fd are
unaffected; writers pause during the swap.

## Rust Interface

`Address = [u8; 32]`. Errors: `Io`, `Format`, `Corrupt(Address)`,
`NotFound(Address)`, `Redacted(Address)`, `NoValidCommit`.

- `CasRead`: `get(&Address) -> Result<Entry>` (no bytes loaded until
  `entry.read()` / `entry.read_slice(range)`), `contains`, `len`, `is_empty`.
- `CasWrite::begin() -> Transaction`; a dropped transaction is discarded.
- `CasTransaction`: `insert(&[u8]) -> Result<Address>` (dedup no-op;
  `Err(Redacted)` if redacted), `redact(&Address)`, `commit(self)` — one
  atomic durable append.

## Future Direction: Whole-File BAO Encoding

Treat the entire `.dialog` file as a BAO stream so the file's own root hash
content-addresses the archive; a replication layer can then fetch and verify
arbitrary byte ranges (an index region, one blob record) from an untrusted
peer with the root as trust anchor — Hypercore's useful property, derived
from BAO. Left to the search tree layer; the format needs no change.

## Complexity Summary

| Operation    | Complexity      | Notes                                       |
|--------------|-----------------|---------------------------------------------|
| get          | O(log N)        | binary search; no content loaded            |
| read         | O(blob_len)     | full BAO-verified read                      |
| read_slice   | O(log blob_len) | BAO path verification only                  |
| contains     | O(log N)        | binary search                               |
| insert       | O(blob_len)     | BAO tree computed; appended on commit       |
| redact       | O(1)            | index entry only                            |
| commit       | O(N)            | merged index written once per batch         |
| recovery     | O(1) practical  | backward scan terminates after a few bytes  |
| compaction   | O(N)            | full rewrite, atomic rename                 |

Index is ~40 MB per million entries; binary search is ≤20 comparisons.

## Review notes (2026-07-28, performance work)

Two amendments proposed from the measured workloads
(`notes/sqlite-baseline-results.md`); to be validated by the DCAA spike:

1. **Per-commit index deltas.** The O(N) merged-index rewrite per commit
   fights the headline small-commit workload (SE dataset: 50k commits, p50
   one fact): at 100k entries a 1-fact commit writes ~4 MB of index, and
   integrated over an SE-scale history the index churn dominates the file.
   Amendment: each footer references the newest MERGED index plus the chain
   of per-commit DELTA indexes since; lookup searches deltas newest-first,
   then the base; compaction (or a periodic fold) merges the chain. Commit
   cost becomes O(blocks written); `get` becomes O(d + log N) with d =
   commits since the last fold. Requires only footer fields, not a record
   format change.
2. **Outboard policy for small blocks.** A 64 KiB tree node carries ~8 KiB
   of outboard (~12%) but is always read whole, where plain BLAKE3
   verification suffices; range reads matter for large spilled blobs.
   Writer policy: `outboard_len = 0` below a threshold (whole-read
   verification), full outboard above it. No format change — the record
   already stores `outboard_len` explicitly.
