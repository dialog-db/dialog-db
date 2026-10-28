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
and other node bytes are reconstructed verbatim. Block frames use bounded fast
DEFLATE compression when it saves bytes; shared token records stay uncompressed. Decoding uses the existing front-coding
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

The initial uncompressed-frame prototype failed the space bar: at 2048 synthetic
SE commits it used 198,803,387 bytes against the filesystem's 358,178,816 bytes
(1.802x). Only 7,796,353 bytes were shared token payloads; frame metadata dominated.
That evidence motivated compressing frames, rather than weakening the threshold.

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

The separate `Single-file archive comparison` workflow runs on pull requests
that change its provider, codec, perf adapter or CI script, and supports manual
dispatch. It uploads both full sweeps, annotated codec costs,
the exact comparison exit code, candidate SHA and toolchain. A negative adoption
comparison is explicitly reported in the job summary; build/sweep or missing
codec-attribution failures fail the job. Ordinary PR gates remain unchanged.

Run these on Linux with callgrind (the Nix environment supplies it). The opt-in
perf adapter calls `get_blocking` / `put_blocking` on the same log and codec so
encoding, realization, hashing and dictionary operations run on the thread
inside the Callgrind measurement marker. Calling the normal async provider here
would offload that CPU work to unmarked worker threads and undercount it. This
instruction comparison intentionally excludes SingleFile worker scheduling;
the repository wall-clock harness uses its normal async provider. Before citing
instruction results, inspect the complete self-cost/callee Callgrind annotation and confirm nonzero costs for
`ProfiledCodec` encode/decode boundaries around the actual `TreeFrameCodec` and the single-file log on exercised paths.
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

## Local results, 2026-10-28

[All raw samples and exclusions](measurements/single-file-archive.json) were
recorded on an Apple M3 Pro, arm64 macOS, Rust 1.99.1, with the repository's
release profile. Each row below is the median of three sequential samples. One
4096 sample and an extra wide probe deliberately overlapped another replay;
they remain in the raw data but are excluded from timing medians. The shared
host was nearly full and running other development work. In particular, 4096
filesystem commit times ranged from 13.8 to 105.7 seconds. These are noisy sanity
checks, not substitutes for the Linux instruction gate.

Sizes are decimal MB. “Live tree” is the reachable tree at the last root;
“live facts” counts the current modeled facts after picks in the input log.

| Workload | Live facts | Filesystem MB | Single-file MB | Reduction | Live tree MB | Single-file bytes/live fact |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Synthetic SE, 2048 commits | 3,755 | 358.18 | 33.02 | 10.85x | 9.90 | 8,795 |
| Synthetic SE, 4096 commits | 7,212 | 1,021.58 | 84.37 | 12.11x | 19.72 | 11,698 |
| Wide updates, 1024 commits | 1,000 | 282.77 | 24.43 | 11.58x | 12.84 | 24,429 |

| Workload | FS commits s | File commits s | FS all-block reads s | File all-block reads s | File reopen s | Resident index lower bound MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SE 2048 | 10.879 | 12.232 | 2.053 | 1.949 | 0.500 | 9.60 |
| SE 4096 | 68.027 | 44.036 | 7.073 | 4.440 | 1.506 | 22.09 |
| Wide 1024 | 1.661 | 2.031 | 0.782 | 1.043 | 0.181 | 10.42 |

Filesystem open does not scan an index and rounded to zero milliseconds in
these runs. Single-file token payloads were 7.80, 18.11 and 9.64 MB respectively.
Whole-process maximum RSS over included samples was 358, 459 and 389 MB; that
includes fixtures, both sequential providers and repository caches, so it must
not be presented as archive-only resident memory. BAO outboards added about
2.06, 5.27 and 1.53 MB when explicitly requested.

Space clears the order-of-magnitude target on these synthetic workloads.
Compression does not eliminate historical frames: doubling SE commits grew the
file 2.55x while the live tree grew 1.99x. It defers reclamation rather than
proving growth solely proportional to live facts.

The 2048 write median is 12.4% slower and the wide write median 22.3% slower;
the wide read median is 33.4% slower. These costs are reported, not waived by
the space win. Other medians favor the file but the timing spread is too large
for a speed claim. The paired Linux instruction/counter comparison remains
required, and no adoption recommendation follows from the local results alone.
