# Where things stand, 2026-11-19

A handoff note, written so the work can be picked up from the files alone. It is a snapshot; the [journal](journal.md) and the [ledger](claims.md) are the record.

## Decided, by the maintainer

- Two kinds. Rules and concepts give rows and never fold. A query nests and folds, and no rule reads one. ([design-queries.md](design-queries.md), and `research/notation/design.md` on `docs/notation`, #728.)
- A query is `(query (name ?entity … is: ?answer) condition… (is ?answer shape))`; a view says `(view ?answer template)` there. Asking is `select`.
- A collection's field is one entry, `block: {?k ?b}`. A set is a map with no names, `{: 1 : 2}`, stored in a record as a list.
- The whole of a field is written on the read. The word is `distinct` (Datomic's); it always gives a set, of values or of whole entries, and a map is made where the answer is built, `{... ?entries}`. The build still says `collect` and gives a map directly.
- The engine carries the fold, in a form of its own that no rule reads: an application over an inner query and a reduce. An authored rule still cannot fold (`open_rule_error` stays). The engine gets an aggregator for the set of values. The notation is an interface to the engine and evaluates nothing itself. (#830, `design/named-fold`.)
- Keeping a fold current under a subscription is a gap to close. (#837, `design/fold-maintenance`.)
- A query's definition is kept and synced as a concept's, a rule's and a macro's are.

## Built and pushed on `feat/notation-poc` (#773)

- Stage 1 and 2: queries, `collect`, `select`, entries, sets, views as queries with exact patches (`76a234e`, `6fea503`). Evaluated in the notation crate in memory, which was the wrong place.
- The cost of that, measured (`examples/cost.rs`, `0c9af72`): one change among 1000 to-dos takes 110 to 145 ms against under 1 ms for a plain subscription, and `select` of one card 57 s. Both are the build's own doing: a made-up row entity for every layer, and answering in memory.

## Running when this was written

One thread, building: the engine's folding query (`rust/dialog-query/src/query/fold.rs`), the aggregator for a set of values, a subscription of a fold (`dialog-repository/.../subscription/fold.rs`), the notation lowered onto them, and the cost measured again. It was unfinished, and not through fmt, clippy or tests. A snapshot of it is the branch `wip/engine-fold` (`6e37807`, one commit over `c3959fe`). If `feat/notation-poc` has `query/fold.rs`, the thread finished and the snapshot is stale. Either way the engine's part must move to a pull request of its own below #773.

Reported and filed on #837 (`design/fold-maintenance/`): how a subscription is kept current today (`subscriptions-as-built.md`), how tonk keeps a view current (`tonk-as-built.md`), and what the literature asks of each fold when facts are retracted (`literature.md`). Here: [definitions on the branch](definitions-on-the-branch.md).

## Open, in the order to take them

1. The numbers from the build thread, against the bars in the ledger.
2. #837: keeping a fold current. A problem statement is proposed and not confirmed. The hypothesis (`direction.md` there): a folding query's subscription is a subscription of its inner query, whose poll already says what stopped and started holding, with a state for each group beside it. It holds for `count`, integer `sum` and a set of values; `min`, `max` and anything else fold the touched group again. Open there: whether a float sum folded from changes equals one folded from the rows.
3. Definitions on the branch, for concepts, macros and queries together ([definitions-on-the-branch.md](definitions-on-the-branch.md)). Decided by the maintainer: a name is an attribute pointing at the definition's entity, as in tonk and roughly as in Unison. He had taken the build to be option C already and it is not; he is fine starting with A. A is built with every definition keyed by its content and the name pointing at it, not as the report wrote it. What is kept is the compiled definition: every reference is resolved and inlined when it is compiled, so what is stored holds no name and no pointer to follow. A source map beside it records, for each reference, the name it was written under and the entity it resolved to (decided 2026-11-11 and 2026-11-12, said again 2026-11-19). A name bound again changes nothing stored.
4. Rows of one head: Datomic gives a row for each; the maintainer leaned to aggregating. Not decided.
5. What two entries of one key leave when a set of entries becomes a map.
6. A bar for "lightning fast": proposed as not growing with the page and within 2 times a plain subscription on a release build. Not answered.
7. No reader round has seen any of the query forms.

## Other pull requests

- #835, `fix/blank-entry-read`: a stored rule reading an entry with key and value blank did not read back. Off `main`, green, draft.
- tonk-labs/tonk#1338: the notation in tonk, pinned to a revision older than all of the above.

## To build here

Every cargo call went through one lock, one job, one target directory, on 8 GB. On a machine without those limits the build thread's work, definitions on the branch and the maintenance work can each have a worktree.
