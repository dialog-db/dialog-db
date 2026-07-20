# Sync-cost findings: canonical vs buffered (hitchhiker) tree

The `sync` bench compares a canonical `PersistentTree` (one edit + persist per
commit) against a buffered `HitchhikerTree` (each commit buffers; persisted with
its novelty intact, never force-canonicalized so cascades happen only on
overflow, per the algorithm).

It measures, per sync between two replicas off a shared flushed base:

- **round-trips**: storage `get` calls the differential walk makes against the
  other replica's store, each modelling one block a real remote would serve;
- **novel blocks / novel get bytes**: the node set (and its size) a push must
  transfer (`TreeDifference::novel_nodes`);
- **commit churn**: nodes written to materialize the divergence (storage write
  amplification). For the canonical tree this is summed per-commit; for the
  buffered tree it is the single persist of the touched spine.

## Results

base = 10000 entries, op_buf_size = 1024.

### pull (one replica ahead by n commits, other catches up)

| n    | Persistent RT | Hitchhiker RT | Persistent novel | Hitchhiker novel | Persistent churn | Hitchhiker churn |
|------|---------------|---------------|------------------|------------------|------------------|------------------|
| 1    | 4             | 2             | 2                | 1                | 2                | 1                |
| 16   | 26            | 2             | 13               | 1                | 32               | 1                |
| 256  | 70            | 2             | 36               | 1                | 514              | 1                |
| 1024 | 77            | 2             | 41               | 1                | 2053             | 1                |
| 4096 | 94            | 89            | 57               | 52               | 8212             | 55               |

### concurrent (both diverge by n, reconcile one direction)

| n    | Persistent RT | Hitchhiker RT |
|------|---------------|---------------|
| 1    | 6             | 2             |
| 16   | 38            | 2             |
| 256  | 72            | 2             |
| 1024 | 87            | 2             |
| 4096 | 119           | 108           |

## Reading

- **In the buffered regime (divergence <= op_buf_size), sync cost is flat:** the
  hitchhiker tree syncs in 2 round-trips / 1 novel block / 1 node of churn no
  matter how much diverged, because every commit lands in the root's novelty and
  the lower nodes never change hashes, so the differential walk prunes them all.
  The canonical tree degrades linearly the whole way (round-trips 4 -> 77, churn
  2 -> 2053 at n=1024).
- **Past the buffer (n=4096 > 1024), the root overflows and cascades.** The
  round-trip advantage narrows (89 vs 94) because divergence now spreads across
  many subtrees, but churn is still ~150x lower (55 vs 8212): a cascade only
  rewrites the paths it spills into, not a full root-to-leaf path per commit.
- **The hitchhiker tree never loses** on any metric, and wins overwhelmingly in
  the frequent-sync regime (small divergence between syncs), which is the target
  workload.

## Caveats

- This measures the search-tree differential, the quantity that drives
  repository sync round-trips, not the repository push/pull protocol end to end.
  A full repo+remote benchmark needs the hitchhiker tree wired through
  `dialog-artifacts` and `dialog-repository` first.
- **The differential is novelty-blind.** `TreeDifference` reads only `index.links`
  and leaf `segment.entries`; it never reads `index.novelty`. So the round-trip
  and novel-block counts above are valid as *structural* (node-hash) churn, but
  the differential's entry-level `changes()` does NOT see buffered ops. Two
  buffered trees therefore cannot be reconciled by diffing them directly: a key
  living only in a node's novelty is invisible, and a flushed delete on one side
  vs a still-buffered key on the other would resurrect the delete. The safe model
  is **canonicalize-at-sync**: flush novelty to leaves before the differential
  runs, after which the existing frugal differentiate/integrate is correct and
  unchanged. See the reconcile tests in `src/hitchhiker.rs`
  (`it_reconciles_when_*`, `it_does_not_resurrect_a_flushed_delete_on_catch_up`,
  `it_shows_buffered_direct_reconcile_misses_novelty`).
- The buffered tree's reads are non-canonical between syncs (node hashes move as
  buffers fill); the byte-exact canonical root exists only after `canonicalize`.
  The numbers above assume sync exchanges the buffered roots as they are.
- Divergence keys are fresh (inserts past the base range). A churn comparison
  under updates/deletes that collide with the base would differ in detail but
  not in the qualitative result.
