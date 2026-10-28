# Single-file archive exploration

The native `dialog_storage::provider::SingleFile` is an opt-in archive provider.
Use `TreeFrameCodec<dialog_artifacts::Key, dialog_artifacts::State<dialog_artifacts::Datum>>`
for artifact trees. Place it in `Space.archive`; existing providers can continue
serving memory cells, credentials, certificates and external assets. A file is
one mounted space's archive, just as a filesystem provider is one space's directory.

```rust,ignore
use dialog_search_tree::TreeFrameCodec;
use dialog_storage::provider::SingleFile;
use dialog_artifacts::{Key, State, Datum};
let archive = SingleFile::open("replica.dlg", TreeFrameCodec::<Key, State<Datum>>::new()).await?;
```

Arena columns become complete component tokens, dictionary columns become their
full distinct tokens. Each distinct byte token is recorded once in the file;
frames refer to earlier token-record offsets. Relative pointers, value payloads,
and other node bytes remain verbatim. Decoding uses the existing front-coding
algorithm, then checks the original wire block hash. Opaque and noncanonical
blocks remain lossless. Tree layout, identity, commit hashes and replication
block boundaries do not change; this is a provider, not a new tree format.

An exclusive OS file lock prevents independently opened writers. Clone a handle
for concurrent callers; blocking I/O runs off the async executor and operations
serialize. Record headers protect lengths with their complement, and payloads
carry BLAKE3 checksums. Reopen discards only an incomplete final record; complete
corrupt records fail without truncation. A failed append poisons the handle until
reopen. Like the current filesystem archive, ordinary writes do not fsync; use
`sync()` before publishing a root when power-loss durability is needed.

`archive.snapshot()` streams the completed file prefix into a BAO outboard. It
returns the immutable prefix's root and length. `snapshot.prove(offset, length)`
serves a verified-range encoding; `RangeSnapshot::verify` checks it against an
independently trusted root and requested range. Later appends do not invalidate
that prefix. Snapshot construction is linear in file size, and its in-memory
outboard costs about 6.25% of the file; extraction reads the requested chunks
and authentication path. It is not a claim that the live growing file retains
one root. This follows the [BAO outboard and slice API](https://docs.rs/bao/0.13.1/bao/encode/index.html).

## Measurement commands

```sh
cargo run --release -p dialog-baseline --example single_file -- 2048
cargo run --release -p dialog-baseline --example single_file -- 4096
cargo run --release -p dialog-baseline --example single_file -- 1024 --wide
```

Set `DIALOG_SE_CSV` to a transformed Stack Exchange dump for real data. Otherwise
these are explicitly synthetic SE-shaped commits. `--wide` is a separate shaped
workload: an initial 1000 entities carrying approximately 2 KB bodies, followed
by small repeated updates. It does not purport to reproduce the unavailable
production 2 GB replica.

The harness runs actual repository commits over each archive at equal durability,
with the same seeded identities. It asserts identical block hash sets and every
historical root. It checks complete query rows on a cold reopened branch, walks
historical roots after reopening the file, and verifies every retained block.
Disk sizes are logical file lengths (not filesystem allocation). Read comparisons
visit the same sorted catalog/hash sequence, with OS cache effects uncontrolled;
these are process-cold provider reads, not hardware cold-cache measurements.

For the existing instruction/counter scenarios:

```sh
cargo build --release -p dialog-perf --features single-file-archive
DIALOG_PERF_ARCHIVE=fs target/release/perf sweep --out-dir target/perf-fs
DIALOG_PERF_ARCHIVE=single-file target/release/perf sweep --out-dir target/perf-single-file
target/release/perf compare target/perf-fs target/perf-single-file --base --threshold 10
```

Run these on Linux with callgrind (the Nix environment supplies it). The opt-in
perf adapter calls `get_blocking` / `put_blocking` on the same log and codec so
encoding, realization, hashing and dictionary operations run on the thread
inside the Callgrind measurement marker. Calling the normal async provider here
would offload that CPU work to unmarked worker threads and undercount it. This
instruction comparison intentionally excludes SingleFile worker scheduling;
the repository wall-clock harness uses its normal async provider. Before citing
instruction results, inspect Callgrind attribution and confirm nonzero costs for
`TreeFrameCodec` encode/decode and the single-file log on exercised paths.
Default
builds do not compile the override, and the existing volatile perf baselines
are unchanged. Without an explicit selection the opt-in build chooses `fs`.
Do not describe ordinary CI's default perf run as proof of the swapped provider.

## Limits to evaluate with the results

- Archive-only: cells, assets and replica root publication stay external.
- The initial hash and token indexes are in memory, rebuilt by scanning on open.
  Tokens share one allocation across indexes. The harness reports payload bytes
  and a resident-index lower bound; allocator/hash-table overhead and process RSS
  are additional. This is not yet a disk-resident dictionary tree.
- No compaction or deletion; every historical block/frame remains available.
- Maximum record payload is 256 MiB. Oversized blocks fail explicitly.
- Realized blocks are not cached in the provider; the existing tree/branch caches
  still apply. The provider verifies each reconstructed block.
- The measured ratios, not the feature's existence, determine whether this
  exploration meets the idea's adoption thresholds. Full-scale measurements and
  paired instruction gates must be reported before recommending adoption.
