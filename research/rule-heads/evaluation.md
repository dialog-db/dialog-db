# Rule heads: what is measured

One axis decides this, with a second beside it.

| axis | workload | reports | harness | status |
|---|---|---|---|---|
| the cost of a commit that writes on one condition | 500 to-dos, each on one of 50 lists, a list with an owner, an owner with a team. One transaction dispatches a `move` command a to-do, carrying a list, a mover and a time. Rules write two or four facts a to-do | instructions under callgrind; `span.resolve_rules`, `span.resolve_bundle`; archive puts and bytes | `rust/dialog-perf/src/scenario/heads.rs`, `perf sweep --only heads-…` | [2026-11-12](results/2026-11-12.md): `named` 22% to 39% over `wide`, `repeat` 61% to 148% over |
| the cost of a commit of one command | the same store. 100 commits, each dispatching one `move` command, after one commit off the record. Beside the three shapes: the command with no rule, and a transaction that asserts the facts itself | instructions under callgrind over the 100 commits, gross and net of the commit with no rule; the same at 10 and 300 commits, over 5000 to-dos, and for the first commit after the rules are installed | the same file, `heads-each-…`, `perf sweep --size` | [2026-11-12](results/2026-11-12-one-command.md): `named` 17% to 28% over `wide`, `repeat` 27% to 87% over |
| what the engine could recover | the corner cells of the two axes above, with each of four prototype changes to induction on, and with three of them together | instructions under callgrind; how often a rule is decoded, analyzed and planned; the gated scenarios with the prototypes on against off | a build of `spike/induce-cost`, `DIALOG_SPIKE=…` | [2026-11-13](results/2026-11-12-induce-cost.md): `repeat` 1% to 7% over `wide`, `named` 21% to 43% over |
| the prototypes built to land | the corner cells of the first two axes, on main and on each of three branches off it: one body evaluated once (#790), a seed that binds its entity (#791), a rule's plans kept (#792), and the first two together | instructions under callgrind, each variant interleaved with main | a build of each branch with this topic's harness added | [2026-11-13](results/2026-11-13-sound.md): `repeat` 0.8% to 5.7% over `wide` with #790 alone, 0.9% to 6.3% with #791 as well |
| that the three write the same | the same, at 20 to-dos, a commit of all and a commit a command | the facts under every attribute written, compared across the three and with the facts asserted by hand | the tests in the same file | they agree |

The data is made in the process from an index, as every `dialog-perf` scenario's is, so there is no dataset file: an entity is `id:<tag>-<index>`.

Not measured: a query that reads what was written, which is the same for the three; a rule of several heads itself, which does not exist; wall time.
