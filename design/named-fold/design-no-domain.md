# A derivation with no domain

**Decision:** what a getter is in the engine, so that what it derives has a name and nothing stored can stand under that name.
**For:** the problem below, restated around the cause the maintainer holds.

## The problem, restated

Proposed to replace the Problem paragraph of [named-fold.md](../named-fold.md); that file is not edited here.

> An author cannot name a value computed over many facts and read it where a read stands. Every name a rule concludes today is an attribute, and an attribute is a place claims are stored: a computed value concluded there is read together with whatever is stored under the same name, which any writer or a merge can put there, and nothing written through the name reaches the facts the value is computed from. A computed value needs a name no claim can stand under, and a place where a fold may be stated to fill it.

The maintainer, 2026-11-18, in substance: a query, a getter and a view derive, and what they derive is not stored, so nothing competes with it; they could be something with no domain, which is why it could not be stored. What follows reads that against the code. All of it is read, not run, unless a test is named.

## What the code has

On `feat/notation-poc` at `62b1f10` (main at `54b1fc4` underneath), spike at `de5ee74`, #827 at `de0029a`.

| | fact | where |
|---|---|---|
| 1a | No query can state a fold. Every read is `select`/`subscribe` of an `Application`, and no implementor folds: `ConceptQuery`, the attribute queries, `OptionalAttributeQuery`, `ResolverQuery`, `Located` | `query/application.rs:43`; impls in `concept/descriptor.rs:801`, `resolver.rs:1074`, `attribute/query/*.rs`, `optional.rs:222`; `dialog-repository/src/stack.rs:2327` |
| 1b | A query of several premises is a deductive rule laid over one query (`QueryLayer::with`), never stored. The notation lowers `(report …)` that way, into a relation `report.q<site>`. A fold there is still `DeductiveRule::with_reduce`, refused | `dialog-repository/src/repository/branch/session.rs:234`; `dialog-notation/src/compile.rs:1493`; `examples/notebook/session.rs:124` |
| 1c | A subscription takes any `Application` (`Branch::subscribe`, `Stack::subscribe`), so it folds only where a query does: nowhere. Over a reducing rule (spike) it asks again: `it_updates_a_subscription_over_a_fold_as_its_groups_change`, 5 recomputes, 0 maintenances | `subscription.rs:443`, `stack/subscribe.rs:317`; `Restriction::Unsupported` default `application.rs:62` |
| 1d | An inductive rule cannot fold. `InductiveRule::new` takes no `reduce`; `Compile::compile` passes an empty one; `InductiveRuleDescriptor` has no `reduce` field and does not deny unknown fields, so a `reduce` key in a stored or authored inductive rule would be dropped without a word | `rule/inductive.rs:87`, `rule.rs:170`, `rule/inductive/descriptor.rs:29-53` |
| 1e | `DeductiveRule::with_reduce` is the one constructor of a fold in a rule, and it is the one refused. Every path reaches it: the descriptor (`descriptor.rs:119`), the head (`head.rs:147`), a rename (`deductive.rs:407`). The fold itself is public, `Reduce::fold(selection)`, but a repository read hands back conclusions, not a `Selection` (`QueryEnv` is `pub(crate)`) | `rule/deductive.rs:115`, `rule.rs:190-205`; `reduce.rs:360`, `lib.rs:145`; `session.rs:564`, `session.rs:675` |
| 1f | So the notes' "closed places, a query, a subscription, an inductive rule" (`notes/attribute-heads.md:176`) are places a fold is not refused, not places it can be stated. The same note says so at `:445`: the plumbing "stands in the code until inductive rules take `reduce`". The notation's message says the opposite (`compile.rs:1365`) | |
| 2a | A read of a derived attribute is a union. `ConceptRules::plan` always puts the implicit rule, the stored scan, first in the disjunction; reducing rules are folded beside it (`reduce_rows`); `relation` gathers candidates from the scan, every head and every fold, and elects per entity | `concept/query/rules.rs:398`; `concept/query.rs:730-733`, `:984` |
| 2b | The one read that skips the stored scan is the exact path: one source rule, no fold, and `stored_absent` true. A reducing rule rules it out | `concept/query.rs:745-757`, `:1288`; `session.rs:2591`, `:2624` |
| 2c | Every scan of a read goes through `QueryEnv::select`, and one attribute is answered there without the store: `dialog.rule/quarantined`, "Nothing stored under the attribute is read". No test pins that a stored claim under it is ignored | `session.rs:1351`, `:2215-2245`; `transaction/quarantine.rs:232` reads it |
| 3a | A rule's conclusion is a `ConceptDescriptor`, attributes keyed by field. Its identity spells the head under `deduce` by concept reference; the body (format 4) is the canonical descriptor; installing writes `derives` and `reads` per attribute, `on:<domain>/<name>`; resolution finds rules by attribute; the analysis's node for an attribute concept is that `on:` entity | `concept/descriptor.rs:57`; `rule/reference.rs:46`, `rule/canonical.rs:84`; `rule/body.rs:40`; `rule/statement.rs:236`, `:320`; `dialog-repository/src/rules.rs:1-35`; `session/dependencies.rs:756` |
| 3b | A body in a later format is refused by name; a body that does not hydrate is skipped, so an older replica holding a folding rule reads as if it were not there | `rule/body.rs:82`; `session.rs:2359` |
| 4a | `dialog.rule/*` is not refused. The application write gate refuses `dialog.*` and carves out `dialog.rule/`, `dialog.concept/`, `dialog.layer/`, `dialog.command/`, `dialog.provider/`. A rule claim is kept harmless by content address: the body must hydrate and be `stored_as` the entity it is under, else it is skipped | `dialog-corpus/src/tree.rs:1548-1561`; `session.rs:1939`, `:2359`; `transaction/induce.rs:787` |
| 4b | A merge has no attribute screen: it diffs key ranges (`data_scope`, `history_scope`). The gate stops a local write only | `dialog-corpus/src/merge.rs:105`, `:186`; `repository/branch/merge.rs:167` |
| 4c | A derived-only relation already exists: the built-in `schema::Revision` concludes `dialog.revision/*`, which the gate refuses, with the stored scan still in its plan. Nothing is found because nothing local can write there | `dialog-repository/src/rules.rs:186-201`, `:947`; `schema.rs:432` |
| 5a | The notation lowers `list/count-open` to the relation `concept.list.count-open`, so the attribute `concept.list.count-open/is`. To the engine it is an ordinary attribute: `concept.` is not reserved | `dialog-notation/src/expand.rs:307-330`, `syntax.rs:60`, `kernel.rs:339` |
| 5b | The notation refuses `assert` and `retract` of it; nothing else does. YAML, the Rust API or a merge can store under it, and a read unions the claim with what rules conclude | `kernel.rs:440`; `tree.rs:1551` |
| 5c | A fold there is refused by the notation itself before the engine sees it | `compile.rs:1355-1369` |

```
 read of (list/count-open ?l ?n)
   │
   ▼
 ConceptRules::plan ──┬─ implicit: stored scan ──► QueryEnv::select ──► store ◄── any writer, a merge
                      ├─ installed heads ───────► their bodies
                      └─ reducing() ─ reduce_rows (refused today; the spike lifts it)
   │
   ▼
 relation(): every candidate, elected per entity        stored 5 over folded 150 (spike)

 QueryEnv::select(dialog.rule/quarantined) ──► the analysis, never the store       session.rs:1351
```

## Alternatives

- **Inline.** A getter is a name for a body. Where it is read, its conditions and its fold stand in its place. No relation exists in the engine.
- **A reserved namespace.** A derived-only relation is an attribute under a prefix that `QueryEnv::select` answers with nothing stored, as it answers `dialog.rule/quarantined`; a fold is admitted only into a head under it, and a cycle through it is quarantined as on the spike.
- **A head with no attribute.** A new kind of conclusion: a named relation of columns, no `Relation`, so no claim can name it.
- **Status quo with the spike.** The refusal lifted, a cycle through a fold quarantined, a folded value beside the stored claims of its attribute.

Inline needs a place a fold can be stated, which fact 1 says does not exist. Two ways in: a query type that folds (an `Application` over an inner query and a `Reduce`, folding the unrestricted body and joining the caller after, as `reduce_rows` does), or a fold admitted in a rule laid over one query and refused when a rule is installed. The matrix takes the first.

| criterion \ approach | Inline | A reserved namespace | A head with no attribute | Status quo with the spike |
|---|---|---|---|---|
| what the name is in the engine | nothing; the name is the notation's | an attribute, `domain/name`, under a domain nobody owns | a new type beside `ConceptDescriptor` | an attribute |
| read in a deductive rule's condition | 🔴 the reader then holds a fold and is refused | read as any attribute concept | read once a premise can name it: a new `Proposition` | read as any attribute concept |
| read in a query, a subscription, a view's conditions | once the fold query exists | yes | once a query type for it exists | yes |
| a claim stored or merged under the name | there is no name to store under | 🟡 never read; it sits in the tree, and if the gate refuses the prefix nothing local can retract it. `Branch::claims().select` reads the index by its own `Select` and would list it | 🟢 cannot exist: a claim needs a `Relation`, and a `Relation` needs a domain (`relation.rs:116`) | 🔴 read over the folded value: stored `5`, folded `150`, read `5` |
| two replicas of different releases | nothing synced | 🟡 the older skips the folding rule (fact 3b) and reads stored claims under the prefix as an ordinary attribute | 🟡 the older refuses the format 5 body by name and skips it | 🟡 the older skips the folding rule and reads only stored claims |
| stored rule format | none, unless getter definitions sync; where they live is open | format 4 as it is | 🟡 a new format, a new head in the identity, a new install index (`derives` is per attribute) | format 4 as it is |
| recursive | no: a body put in its own place does not end. A getter of getters, acyclic, is fine | yes, a cycle through the fold quarantined | yes, once the analysis has a node for it | yes, a cycle through the fold quarantined |
| under a subscription | asked again (`restrict` defaults to `Unsupported`) | asked again (spike) | ? nothing keyed by entity applies as it is | asked again (spike) |
| what changes | 🟢 one new type in dialog-query and its export; the notation's expansion and its report lowering. 3 to 4 files | 🟢 no new type. `QueryEnv::select` (one test, as for `quarantined`), `open_rule_error` narrowed to heads outside the prefix, the spike's two lines in `dependencies.rs`, the gate in `tree.rs` if writes are to be refused, `DERIVED` in the notation. 5 to 6 files | 🔴 the conclusion's every reader: rule types, identity, canonical form, body, install indexes, resolution, the analysis, the planner and fixpoint (rows keyed by `this`), a premise and a query. ? more than 15 files, not counted | 10 files on the spike, two of them the change |
| serves the notation's derived attribute ("concluded only", read where a read stands, "a stored `5` can never beat a count") | 🟡 except in a rule | yes | yes, and rows without an entity (design Open) | 🔴 the stored `5` beats the count |
| a check that depends on the rule alone | yes: no rule folds | yes: the head's prefix is in the rule | yes | yes |
| what it leaves undone | a getter in a rule that derives; a getter's definition across replicas | maintenance of a fold; "nothing can be written through it" stays, by design; whether the domain is one prefix or the concept's | all of it to build | the competition |

Inline and the reserved namespace are not exclusive: a getter read in a query or a view can be inlined, one read by a rule can be a relation under the prefix.

## Hypotheses

| | hypothesis | confirmed by | refuted by |
|---|---|---|---|
| N1 | No query, subscription or inductive rule on the stack can state a fold; the view note's "one of the places Dialog-DB already lets a fold stand" (`design/macros/design-view-maintenance.md`) is not so | a test that an `InductiveRuleDescriptor` with a `reduce` key compiles to a rule with no fold, and a search of every `Application` | any public call that runs a `Reduce` inside a query or a subscription |
| N2 | Answering a reserved prefix at `QueryEnv::select` hides a stored claim from every read: a concept, an attribute, a selecting rule, the exact path, a transaction's query, a subscription, the stack | the test in [claims](claims.md) (`a` installs the fold under the prefix, `b` holds a claim there by machinery or merge, both read the fold) run through each of those reads | a read that reaches the store another way; `Branch::claims().select` is one to look at |
| N3 | A fold query (an `Application` over an inner query and a `Reduce`) is all inline needs in the engine, and a subscription over it gives the right rows by asking again | the spike's fold and subscription tests rewritten over such a type, no rule installed, passing | a caller's binding that restricts the folded body, or a subscription that misses a retraction |
| N4 | A head with no attribute and a reserved namespace give the same reads; they differ only in what a raw listing of claims and an older replica show | the N2 test run against both, with a raw claim listing and a replica a release behind | a read the namespace cannot give that the new head can, or the reverse |
| N5 | No rule that derives in tonk's library reads a count; counts are read by views and queries only | a read of tonk's 74 views and its rules | one deductive rule over a count, which inline cannot serve |
| N6 | A replica a release behind reads a getter differently from one ahead, in every column but inline | a test decoding a reducing body on main (`DeductiveRule::decode` errors) and reading the getter on both | the older release keeping the rule |

## Open

- Whether a getter must be readable in a rule that derives. N5 would say; the maintainer may already know.
- Where a getter's definition is kept so another replica finds it, for inline. The view note has the same question for views.
- Whether the derived-only domain is one prefix or the concept's own, as the notation's design has it ("the concept's hash and the name"). A relation is at most 64 bytes (`relation.rs:108`).
- Maintenance of a fold under a subscription, in every column. #827's `e9712a9` maintained a picking fold per entity.
- Whether the write gate should also refuse the prefix, given that the app then cannot retract what a merge brought.
