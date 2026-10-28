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
