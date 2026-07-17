# Format-epoch codec: measured tradeoffs (M1, M2)

Same machine, same deterministic workloads throughout. Three points compared:
- **base**: `feat/inductive-self-negation` (full-key upper-bound links,
  key-rank coin, `[u8; 162]` inline keys, `Vec<Entry>` segments).
- **M1**: `feat/truncated-separators` tip `9e778b78` (separator links,
  separator-rank coin; segments still store full 162-byte keys per entry).
- **M2**: M1 plus the front-coded suffix-table codec (this section, added
  2026-07-17): leaf keys front-coded (node prefix + varint deltas + restarts
  every 16), index nodes a prefix + separator-suffix table; `Key` no longer
  rkyv-archived (reconstructed from bytes via `Key::try_from_bytes`).

## Columnar headline (2026-07-17): component dictionaries dominate

Same 100k-fact 162-byte-key workload. A key type exposing its EAV component
schema (`examples/columnar_tradeoffs.rs`) stores each component columnar:
entity + value reference in arenas, tag + attribute + value-type in per-leaf
content-derived dictionaries.

| codec                         | bytes/fact | vs base | vs M2 flat |
|-------------------------------|-----------:|--------:|-----------:|
| base (upper-bound, [u8;162])  | 357.1      | --      | --         |
| M1 (separator links)          | 356.8      | -0.1%   | --         |
| M2 (flat front-coded)         | 294.1      | -18%    | --         |
| **columnar (component dicts)**| **92.7**   | **-74%**| **-68%**   |

The attribute (20 distinct names, recurring non-adjacently across entities in
EAV order) is stored once per leaf instead of once per fact; the tag and
value-type collapse to one dictionary index each. Flat front coding could
only dedup the one component that sorts into adjacency (entity), so it left
~200 bytes/fact on the table. Read-back verified (the columnar decode /
reconstruct path is exercised, not just the write path). This is still
pre-M3: the value payload still duplicates the fact and keys are still fixed
162B; M3 (datum-as-key) shrinks the payload to state+cause on top of this.

## M2 headline (2026-07-17): front coding pays on the fixed-key workload

`examples/tradeoffs.rs`, 100k EAV facts, 162-byte keys, batches of 100. M2
vs M1 on the same machine, same run:

| metric              | M1 (sep links) | M2 (front-coded) | delta   |
|---------------------|---------------:|-----------------:|--------:|
| **live bytes/fact** | 356.8          | **294.1**        | **-17.6%** |
| live tree total     | 34.0 MiB       | 28.0 MiB         | -17.6%  |
| segment bytes       | 34.0 MiB       | 28.0 MiB         | -17.6%  |
| write amplification | 854.1 MiB      | 706.4 MiB        | -17.3%  |
| index bytes         | 40.8 KiB       | 39.3 KiB         | -3.6%   |
| bytes per link      | 107.4          | 103.4            | -3.7%   |
| point get           | 13.7 us        | 10.2 us          | -26%    |
| build throughput    | 53.0k/s        | 56.7k/s          | +7%     |
| single-change diff  | 4 reads        | 4 reads          | flat    |
| entity scan reads   | 0.75           | 0.75             | flat    |

The win is in the *segments* (99.8% of the tree): the fixed 162-byte keys
carry long shared runs (a URI scheme prefix on the entity, the entity's
zero-padded tail, the attribute's zero padding), and node-local front coding
stores each shared run once per leaf instead of once per entry. That drops
~63 bytes/fact from the payload. Reads did not regress: the encode work is
persist-time only, and decode is a linear cursor over contiguous bytes, so
point-get latency improved (better locality than the old 162-byte strides).
M2 micro-benchmarks (criterion `--quick`, insert/get/batch) are in the same
band as M1; no per-op regression. This is still pre-M3: keys remain fixed
162 bytes and the payload still duplicates the fact; datum-as-key (M3) drops
the padding entirely and shrinks the payload to state+cause.

## M1 report (2026-07-16): separator links alone

Date: 2026-07-16. Branches compared: `feat/truncated-separators` (separator
links, separator-rank coin) vs `feat/inductive-self-negation` (current:
full-key upper-bound links, key-rank coin). Same machine, same workloads.

## Workload A: dialog-shaped report (`examples/tradeoffs.rs`)

100k facts, EAV-shaped 162-byte keys (urn-prefixed entity, padded attribute
names, hash value refs), 120-248 B values, transactions of 100 facts.

| metric                     | current (upper-bound) | separators | delta |
|----------------------------|----------------------:|-----------:|------:|
| build throughput           | 57.4k facts/s | 54.3k facts/s | -5% |
| nodes written (all batches)| 6625 | 6178 | -7% |
| bytes written (write amp)  | 867.7 MiB | 854.1 MiB | -1.6% |
| live tree total            | 34.1 MiB | 34.0 MiB | -0.3% |
| live bytes/fact            | 357.1 | 356.8 | ~0 |
| index nodes / bytes        | 3 / 74.1 KiB | 1 / 40.8 KiB | -45% bytes |
| bytes per link             | 194.1 | 107.4 | **-45%** |
| tree depth                 | 3 | 2 | -1 level |
| point get (cold-ish)       | 11.5 us | 12.6 us | ~noise |
| entity scan reads          | 0.76 | 0.75 | ~0 |
| single-change diff reads   | 6 | **4** | -33% |
| 1000-delete batch          | 46.0 ms | 49.9 ms | +8% |

## Workload B: criterion suite (16-byte random keys, `--quick`)

- insert (sequential/random, per-op), get, delete, range_query: all within
  noise (±5%, p > 0.05 across every size 10..10000).
- batched inserts: **+8% (100), +18% (1000), ~+12% (10000 mixed)** after
  the orphan-append hashing fix (was +25..37% before it). Absolute scale:
  41 vs 33 us per 100-key batch. This is the price of the new per-edit
  detection work (min-move check, seam-rank hashing during regroup);
  amortized batched edits are so fast that the constant-factor work shows.

## Reading the numbers

1. **The claimed win is real but narrow at today's key size**: links shrink
   45% (194 -> 107 B) and index levels get shallower/wider, but with fixed
   162-byte keys the index is ~0.2% of the tree, so TOTAL storage is flat.
   The separator encoding is the enabler for the epoch (variable-length
   keys, front coding, datum-as-key); it does not pay by itself on
   fixed-size keys. The plan predicted exactly this (M1 is plumbing; M2/M3
   is where segments shrink: keys dominate at ~357 B/fact and front coding
   plus padding-drop attacks that directly).
2. **Reads improve where it matters for sync**: single-change diff is 4
   reads vs 6 (hash-based pruning plus a shallower tree), and write
   amplification per transaction drops ~7% in nodes because one fewer
   index level rewrites per batch.
3. **Nothing user-visible regressed**: get/scan/delete/insert per-op are
   flat; the only regression is the amortized in-memory batched-edit path
   (+8..18% at microsecond scale), with known further headroom (the fast
   path re-hashes keys the detection already hashed; ranks could be
   threaded through).
4. **Caveat on shapes**: the coin scheme changed, so the two trees have
   different (both canonical) shapes; at 100k facts the separator tree
   happened to come out one level shallower with a single wide root
   (41.8 KiB). Root width is governed by the same branch-factor dial as
   before; nothing structural changed about it.

## How to reproduce

- `cargo run --release -p dialog-search-tree --example tradeoffs` on each
  branch (same file, public API only).
- `cargo bench -p dialog-search-tree --features helpers --bench insert
  --bench get ... -- --quick --save-baseline current` on the base branch,
  then `--baseline current` on the separator branch.
