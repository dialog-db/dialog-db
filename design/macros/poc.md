# The proof of concept on Dialog-DB

`rust/dialog-notation` runs the kernel and the prelude of [kernel.md](kernel.md) on Dialog-DB as it is. It is a proof of concept: it shows which parts of the design Dialog-DB already runs and which it cannot.

```
text ──read──▶ forms ──expand──▶ kernel rules ──compile──▶ descriptors ──▶ Dialog-DB
      form.rs        expand.rs                  compile.rs
                     prelude.clj
```

- `form.rs` reads the notation. A symbol's kind is told from its spelling: a domain has a `.`, an attribute a `/`, an entity a `:`, an operator is `+-*/<>=` only, and anything else is a name. So syntax can ask `(:domain ?x)` and `(:name ?x)`.
- `expand.rs` is the walker. It keeps the rules about syntax and rewrites a form until every head and premise applies a relation or a formula. The prelude is the 65 lines of `prelude.clj`.
- `compile.rs` turns a kernel rule into what Dialog-DB has. It evaluates nothing.
- `syntax.rs` is the syntax tree: what a form says, apart from how it was written. `data.rs` is what the tree is kept in: dialog's composite value, with how a name is spelled as a symbol. Expanding and compiling do not read the tree yet. See [design-forms.md](design-forms.md).

| kernel | Dialog-DB |
|---|---|
| `(assert! A)`, `(retract! A)`, no premise | a `ConceptStatement`, asserted or retracted |
| `(assert A)`, no premise | a `ConceptStatement`, dispatched |
| a bare head, with premises | a `DeductiveRule` |
| `(assert! A)`, `(retract! A)`, with premises | an `InductiveRule` |
| `(report A)`, one read as its premise | a `ConceptQuery` |
| `(report A)`, any other premises | a `DeductiveRule` laid over the branch for the query, and a `ConceptQuery` of what it derives |
| `(xyz.todo ?t text: (:last (:text ?x)))` | a concept of one attribute, `{the: xyz.todo/text, as: text:, pick: last:}` |
| `(assert A)`, with premises | an `InductiveRule` over a concept marked `Transient` |
| `(:id domain label: term …)` | a chain of `entity/derive`, from the domain and the sorted labels through each value |
| `(+ ?a : ?b)` where a term stands | a `math/sum` premise and a variable for its value |
| `(< ?a : ?b)`, `(:value ?a is: ?b)`, `(:text ?x)` | the `<`, `==` and `type` constraints |

## What runs

`tests/notation.rs` submits documents to a branch of a real repository. Each document is one transaction.

- A concept defined, written through by a rule, and read back through itself.
- A command issued at the top level. It is a dispatched transient: the rule reading it fires and the command is not on the branch afterwards.
- `increment`, defined nowhere but in the rule that reads it. A misspelling of it is a warning and nothing else.
- `cargo run -p dialog-notation --example notebook -- file.clj …` runs documents on a repository kept in memory and prints the report and the warnings.
- A rule and the command it reads in one document. The rule fires in the transaction that installs it, so finding 4 of [kernel.md](kernel.md) does not arise.
- A backlink concept, read from either side and written through.
- A concept of two reads over two domains, and `unless` over it.
- A derived relation, a relation derived from itself, and a query of several premises whose rows keep their values together.
- One rule with an `assert!` head and a `retract!` head.
- A rule that issues a command another rule reads in the round after: archiving a list completes what is on it.
- An entity made from what a rule binds. A rule and a top-level write that say the same thing name one entity.
- Two commands in one document that write one `last` field. The cell holds both claims and elects one by the hash of the value, the same in either order.

- A struct as a field's value, kept and read back, and taken apart by a pattern in a query. This is on the patterns work (#778), merged into this branch.

## What Dialog-DB cannot run yet

Each of these comes back as `Unsupported`, with the reason. None is approximated.

- **A formula inside `unless`.** Lifted out it would have to hold, which changes what the rule says.
- **The absence of a concept that spans entities.** `unless` negates one proposition, and one proposition reads one entity.
- **A field left out of a concept twice inside `unless`.** Such a field is blank, and two blanks are not the same value.

What is refused, as not allowed:

- **A write that leaves a field out.** A concept written in part is not the concept: a read through it would not find what was written. A rule that changes one field writes the relation, `(assert! (xyz.todo ?t state: (:last (:entity :done))))`.
- **A variable in `unless` that nothing in `when` binds.** A negation binds nothing (`rust/dialog-query/src/negation.rs`); `_` says anything at all.

## What dialog gained

`entity/derive` (`rust/dialog-query/src/formula/entity.rs`): the entity two values name, `id:` and the base58 of their references as a list under the type `id:`. It has two cells and an output, as every formula has fixed cells, so `(:id domain a: ?x b: ?y)` is a chain:

```
seed = derive("domain", "a b")        worked out when compiling
?e1  = derive(seed, ?x)               one premise a value, labels sorted
this = derive(?e1, ?y)
```

Where every value is a constant the chain is worked out when compiling, by the same function.

This is a stand-in. Decided the same day: `entity/derive` takes one record, the record of what the entity is derived from ([design-forms.md](design-forms.md)). The chain exists because a rule cannot build a record yet.

What the crate does in place of Dialog-DB, and should not in the end:

- **Syntax is not stored.** The walker holds the rules about syntax in memory and matches patterns itself. Dialog-DB has no value a form can be and no premise that takes one apart, so `expand` and `syntax` cannot be premises and heads of stored rules. This is the part of "macros as rules" that is not shown.
- **A rule has one head.** To write on one condition twice is two rules. They read the same round, but they are stored and triggered apart, and each waits only on its own premises.
- **Descriptions.** A rule's `description:` reaches the Dialog-DB rule. A concept's is read and dropped, since nothing stores the concept.
- **Types are written, not inferred.** A field with no `(:type …)` is an attribute with no type.

## What it showed

1. **A query reports the relations queried.** `(query (todo ?t text: ?x))` reports `(xyz.todo todo:1 text: "Buy milk")`. Decided 2026-11-11. A row holds the fields the query named; for a concept of two reads it is a row a relation.
2. **A write says every field.** Decided 2026-11-11: no partial write through a concept.
3. **The pick is part of what is read.** `(xyz.counter ?c count: ?n)` reads every count ever written and `count: (:last ?n)` the current one. With no pick written the crate reads all, since a relation is a set. A concept hides this; a bare read does not.
4. **A command is its content.** Its entity is the hash of what it says, so issuing the same command twice in one document issues it once.
5. **Negation is narrower than the notation.** Every "cannot run yet" above is about `unless`.
6. **A document is one round, not a sequence.** `(add (todo todo:2 …))` and `(complete (todo todo:2))` in one document both fire against the same view, in either order. The state then holds `:open` and `:done` as two claims, and `(:last …)` elects one as it does between any concurrent claims (#752). Written in two documents, the second follows the first. `examples/todo.clj` and `examples/done.clj` are split for this reason.

Not measured: what expansion and a per-query rule cost. The claim that this is as fast as today is still open.
