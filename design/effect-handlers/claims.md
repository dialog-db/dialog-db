# Claims

| claim | kind | backing | status |
|---|---|---|---|
| A tree operation names its environment's type through trait bounds, and `perform` is bound to a provider impl at compile time | fact | `rust/dialog-search-tree/src/nodes.rs` (`Nodes<Key, Value>`, `Read::perform`), `rust/dialog-capability/src/provider.rs` | holds |
| Nothing in the current dispatch suspends the performer for a handler to resume: the only suspension is the task yielding to the executor when it awaits a `Resume::later` | fact | `rust/dialog-capability/src/provider.rs` (`Resume`, its `Future` impl) | holds |
| One boxed future per command performed cost 2 to 4% of the point-read scenarios; answering a hit at once took that back | fact | `design/tree-node-reads/journal.md`, the three Building entries' tables | holds |
| The capability system was meant to work like effing-mad or gozala/actor: a handler sync or async without the performer caring, and `perform` standing in for `yield` | opinion | the maintainer, 2026-11-08 | - |
| `Resume` is a compromise: sync-or-async indifference without the inversion | opinion | the maintainer, 2026-11-08; the agent agrees and said so | - |
| An inversion that performs as well as or better than `Resume` is plausible on stable Rust | hypothesis | confirmed by a prototype of the tree's node reads under a polling handler measuring within noise of #755 on `read-point`, `rule-point` and `commit-batch` under callgrind; refuted by a cost that no arrangement of the handler loop removes | open |
| The integration's read-ahead (`open_pending`) is a hand-written form of what a handler holding suspended requests could do by batching them | hypothesis | confirmed if a handler that gathers pending node requests reproduces the soak's round-trip shape (`join-none`, every phase) without `open_pending`; refuted if the handler cannot see far enough ahead, since the window is made of changes not yet applied | open |
