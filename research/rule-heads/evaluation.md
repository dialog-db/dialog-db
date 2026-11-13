# Rule heads: what is measured

One axis decides this, with a second beside it.

| axis | workload | reports | harness | status |
|---|---|---|---|---|
| the cost of a commit that writes on one condition | 500 to-dos, each on one of 50 lists, a list with an owner, an owner with a team. One transaction dispatches a `move` command a to-do, carrying a list, a mover and a time. Rules write two or four facts a to-do | instructions under callgrind; `span.resolve_rules`, `span.resolve_bundle`; archive puts and bytes | `rust/dialog-perf/src/scenario/heads.rs`, `perf sweep --only heads-…` | not run |
| that the three write the same | the same, at 20 to-dos | the facts under every attribute written, compared across the three | the tests in the same file | not run |

The data is made in the process from an index, as every `dialog-perf` scenario's is, so there is no dataset file: an entity is `id:<tag>-<index>`.

Not measured: a query that reads what was written, which is the same for the three; a rule of several heads itself, which does not exist; wall time.
