# The tree reads nodes beside the capability system: claims

| claim | kind | backing | status |
|---|---|---|---|
| A tree operation reads nodes through an `Accessor`, which takes the node cache from the environment's `Holds` (`NodeCache::of(env, subject)`) and answers cached nodes itself | fact | `rust/dialog-search-tree/src/accessor.rs` on main `20f0a45` (`Accessor::new`, `load_node`) | holds |
| On a cache miss the tree performs `Load`, which returns the block's bytes (`Option<Buffer>`), then checks them against the hash, validates and decodes them | fact | `rust/dialog-effects/src/archive/load.rs` (`type Output = Result<Option<Buffer>, ArchiveError>`); `accessor.rs` `fetch_node`, `checked` | holds |
| `Holds` is a map of type-erased values under string keys, chosen by the code that puts a value in and takes it out | fact | `rust/dialog-common/src/holds.rs` | holds |
| The node cache is owned by the environment: one per space, released through the peer | fact | `rust/dialog-repository/src/repository/caches.rs`; `Peer::caches` in `rust/dialog-peer/src/peer.rs` | holds |
| #733 added a `NodeSource` kept through `Holds`, asked before every load; its lookup cost up to 16% more instructions than main on the perf scenarios, all of it formatting the key and looking it up | fact | `dialog-perf` sweep and callgrind diff, [research journal 2026-11-06](../../research/single-file-format/journal.md) | holds; mitigated by a lookup on miss only |
| `Holds` keeps more than the node cache: the formula cache, the repository's caches, the network's routes | fact | `held_key` users on main: `dialog-query/src/formula/cache.rs`, `dialog-repository/src/repository/caches.rs`, `dialog-network/src/routes.rs` | holds |
| The tree should ask for what it needs through the capability system, with no side channels; caching, reading and decoding belong to providers | opinion | the maintainer, 2026-11-07 | - |
| `dialog-storage` (where the `fs` archive lives) does not depend on `dialog-search-tree` | fact | `rust/dialog-storage/Cargo.toml` | holds; whether it *must not* is not yet asked |
| Reading nodes in place (rows) beats building a block on a miss, for cold reads and for the memory cached nodes take | hypothesis | measured once before the six-member bench (cold reads level with main only in place; rows nodes about a third of the memory). Confirmed if the six-member bench, cold reads and memory included, shows the same with both variants; refuted if building blocks on a miss is within the run-to-run spread | open |
