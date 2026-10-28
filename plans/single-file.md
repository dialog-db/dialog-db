# Single-file archive spike

Base: becde743. Branch: codex/idea-single-file.

## Contract and checkpoints

1. Native archive provider: append-only checksummed records, global token dictionary,
   block-hash lookup, catalog isolation, exclusive writer, torn-tail recovery.
   Test put/get/import, reopen, corrupt data, incomplete append, and lock exclusion.
2. Search-tree column codec: store arena/dictionary column values as shared tokens;
   reconstruct the exact original wire bytes (including legacy/raw fallback).
   Existing engine and wire hashes remain unchanged. Test real tree round trips.
3. Workload measurement and documentation: compare filesystem bytes and timings,
   preserve existing performance baselines, report scale and limitations honestly.
4. Fresh-context review and correction cycle coordinated by parent.

Archive-only is explicitly allowed by the idea. Memory and assets remain external.
The initial token and hash indexes are rebuilt in memory on open; an on-disk index
and verified BAO ranges remain separate concerns to assess in the result.
No production 2 GB replica has been located; use a reproducible shaped workload
and state clearly which measurements are synthetic versus Stack Exchange replay.

## Status

- Inspected provider composition, current columnar node codec and CI.
- Implementing checkpoint 1.

## Proven checkpoints

- `0d140966`: native archive with shared token frames, five original recovery /
  namespace / concurrency checks (four at first commit, framing regression next).
- `0436afa8`: complemented lengths reject corruption without truncation; token
  bytes shared between indexes; explicit BAO immutable-prefix snapshots and range
  verification. Six focused storage tests pass, including append-after-snapshot,
  wrong root, truncated/tampered proof and range overflow.
- TreeFrameCodec reconstructs full arena and dictionary values, shares them as
  tokens, and rebuilds exact original byte ranges. Focused test passes; opaque
  and noncanonical blocks fall back losslessly.
- Repository replay uses actual Get/Put/Import capabilities with a seeded peer.
  Both providers must produce identical retained wire hash sets and roots.
  Cold branch queries compare complete rows; reopened historical roots are walked.
  The 12-commit integration test passes.

## Remaining validation and explicit limits

Run controlled 2048/4096 synthetic SE and wide repeated-update measurements;
`DIALOG_SE_CSV` selects a real transformed dump, otherwise label synthetic.
No production 2 GB replica is available. Indexes are memory-resident and rebuilt
by scanning on open; stats report only a lower bound, not allocator RSS. BAO
snapshots stream the completed prefix once, keep an outboard in memory, and serve
range proofs against that immutable prefix; they are not a moving live-file root.
The 256 MiB record limit is explicit; memory cells/assets remain external.

Feature `dialog-perf/single-file-archive` swaps existing scenarios to a disk
provider via `DIALOG_PERF_ARCHIVE=fs|single-file`. Default build stays unchanged;
no existing baselines are edited. Compare paired disk sweeps with the existing
counter and instruction tolerance, and report any failure without rebasing it.
