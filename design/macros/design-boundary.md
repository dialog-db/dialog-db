# The crate's boundary: what it takes in place of a `Branch`

**For:** the maintainer, 2026-11-26. He said the notation "should not be messing with branches at all except perhaps for resolution", through a trait. The audit (`audit-2026-11-26.md`, Part 2) lists where the crate holds a `Branch` today. Not built: this note is what building it has to settle.

**Given,** decided: definitions are facts. A definition is written into the primary by default, and into `session:` when the document names it. A bare name resolves over both, and the session's shadows the primary's.

## Four traits, three of them the engine's

| trait | what it does | `Branch` today | `Stack` today |
|---|---|---|---|
| `Read` | rows of a `ConceptQuery`, or of a `Fold`, at the current state, with a rule laid for this read alone | `query().with(rule)` | `query().with(rule)`, every layer |
| `Watch` | the same, kept current, as deltas | `subscribe`, `subscribe_fold`; no rule, so `Live` leases `overlay()` | `subscribe` (shared, `settle`), `fold_subscription`; no rule |
| `Stage` | assert, retract, dispatch; read through what is staged | `Transaction` | `StackTransaction` |
| `Resolve` | the definitions in force for the names a document uses, at one state | none: the `Analyzer`'s memory | none |

`Stage` is `apply.rs:133` as it is: sealed, so only the engine's two transactions implement it, and the caller commits.

`Resolve` is not an engine trait. Definitions are facts, so resolving is a `Read` of the definition attributes (`definitions-plan.md`, (1)) over `memory:` and `session:`, the session's row kept where both name the same thing. It lives in the crate, generic over `Read`. It needs no engine change, only the facts to exist.

The analyzer stays synchronous. `Resolve` is asked once per document, before analysis, for every name the document uses, and gives a snapshot. The crate has no walk that collects those names yet: `check.rs:358` collects only derived attributes. It is a walk of the read trees, done before expansion. The `Expander` analyzes against that snapshot as it analyzes against its memory today. A definition the document makes is added to the snapshot as the document is analyzed, as it is added to memory today. The alternative, an analyzer that awaits each name, makes `check` async and its order of reads the order of names. Nothing needs that.

## What each module would call

| module | today | after |
|---|---|---|
| `parse`, `form`, `syntax`, `pattern`, `data`, `print` | nothing | nothing |
| `expand`, `analyze`, `check`, `declare` | the `Analyzer`'s memory | a `Resolve` snapshot. The memory remains, as a cache of it |
| `compile`, `view/plan`, `view/markup` | nothing | nothing |
| `apply::stage` | `Stage` | `Stage` |
| `apply::apply` | `Branch::transact`, `refresh` | stays a `Branch` convenience, or moves to the embedder: the commit and its retry are the caller's |
| `apply::answer`, `apply::rows` | `Branch::query` | `Read` |
| `live` (`Live`, `Standing`, `Watched`) | `Branch::query`, `subscribe`, `subscribe_fold`, `overlay` | `Read` and `Watch` |
| `select` | rows `live` gives it | the same, and `Read`/`Watch` of a `Fold` for what moves (below) |
| `view/tree` | rows `live` gives it | the same. Whether it belongs to the renderer is a separate decision |

## What needs an engine change

1. **A subscription that takes its rule.** `Stack::subscribe(query)` and `Stack::fold_subscription(groups, inputs)` take no rule. `Read` has one (`QueryLayer::with`), and `Watch` needs the same. With it, `Live`'s overlay lease goes: the `HOLDERS` and `LAYING` statics (`live.rs:161`, `170`), the `dialog.n.live/*` facts and about 245 lines. This is what the audit found, and it is the one change everything waits on.
2. **Rules held in `session:`.** The decided naming puts a rule defined `in: :session` in the session layer. The stack routes every write under `dialog.` to the primary whatever layer is selected (`stack.rs:683-685`), and I found no read of rules from another layer. Open: whether a rule stored as facts in `session:` is evaluated at all. If not, either the engine assembles a stack's rules from every layer it reads, or a session definition is resolved from `session:` and its rule laid per read (change 1 again).
3. **Not needed to move:** a query that nests (#837). Without it, the evaluator keeps nesting (below).

## The order, each step building with its tests green

1. **The traits, implemented for `Branch` and `Stack`, called by nothing.** `Watch` on `Branch` keeps today's overlay lease inside its implementation. `Watch` on `Stack` leases the primary's overlay too, which a stack reads as `memory:` (`stack.rs:107-112`). The bookkeeping rows show there, as they would today.
2. **`answer` and `rows` take `Read`.** Tests pass a `Branch` unchanged. A new test on a stack: a document that writes `in: :session` then selects it answers its own write (it cannot today).
3. **`Live` takes `Read + Watch`.** The overlay code moves from `live.rs` into the `Branch` and `Stack` implementations of `Watch`, and `Live` names no `Branch`. `tests/live.rs` and `tests/view.rs` run on both implementations.
4. **Definitions written as facts** (`definitions-plan.md`, commits a to d). `Resolve` over `Read` gives the snapshot, and `Analyzer::open` builds one. `tests/model.rs`: an analyzer opened from the facts gives the same `Checked` as the in-memory one on every document it runs.
5. **`session:` definitions** (plan commit e), once engine change 2 is answered. Tests: a bare name defined in both resolves to the session's; one in the primary alone resolves to it.
6. **Engine change 1 lands.** Each `Watch` implementation drops its lease and passes the rule. The statics and `dialog.n.live/*` go. Every earlier test stands.
7. **`select` hands what it can to folds** (below). The standing-select tests gain a counter of how often an answer is rebuilt whole.
8. **The `repository` feature holds the implementations alone.** `Branch` is named only there and in tests.

Steps 1 to 3 need nothing from the engine and nothing from the maintainer. Step 4 needs the plan's open questions answered: who owns `db.*`, and what is written for `pick`. Step 5 needs change 2.

## What `select`'s evaluator keeps

My reply to the audit corrected its F5. Here it is as an estimate. Not measured: it is what the code's shapes say, and the line counts are guesses from the evaluator, `select.rs:586-1112` by the audit's count.

- **Moves to engine folds:** a select whose answer gathers one level, a count or a `(distinct ?x)` per row of its conditions. That is one `Fold`, with the conditions as its group query, the collected read as its input query, and `Count`, `CountDistinct` or `Distinct` as its fold (`reduce.rs:122-153`). Several collected reads of one answer are several `Fold`s over the same group query, joined on the group's fields. Standing, each is a fold subscription, maintained per group, in place of today's rebuild of the whole answer whenever any row moves (`live.rs:700-737`).
- **Stays:** an answer that nests, a collected record whose own fields are collections (`{:title ?t :tags (distinct ?g)}` gathered per list). A `Fold` folds the values of one input query per group. It has no group within a group, so the evaluator builds the inner sets and places them in the outer ones. It also keeps the join of several folds by group key, and building the answer's records. Estimate: about half of the 520 lines stays, and nesting stays whole-answer until #837.
- **Goes either way:** the `count`-only refusal (`select.rs:1106-1111`). `Fold` has the six folds a rule head already takes.

## Not decided here

- Whether `view/tree` and the HTML checks move to the renderer (audit, Part 3).
- Whether `apply::apply` stays in the crate as a `Branch` convenience.
- What a definition in another repository's layer means (deferred by the maintainer).
