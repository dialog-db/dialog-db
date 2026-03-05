# Dialog Query Engine Internals

This book documents the inner workings of Dialog's query engine, tracing a
query from expression through planning, evaluation, and storage.

## What Dialog Is

Dialog is an embeddable, local-first database built on an append-only store of
**claims** — immutable `(the, of, is, cause)` tuples. It uses a Datalog-inspired
query engine to interpret claims at read time, so there is no fixed schema at
write time. Different applications can impose their own models on the same data.

The query engine takes a set of **premises**, plans an execution order, evaluates
them against the store, and produces a stream of **matches** — sets of variable
bindings that satisfy all premises simultaneously.

## Key Concepts

| Concept | Role |
|---------|------|
| **Claim** | An immutable `(the, of, is, cause)` fact in the store |
| **Term** | A query position: either a constant or a variable |
| **Premise** | A single pattern or constraint in a query |
| **Match** | A set of variable bindings produced by evaluation |
| **Planner** | Reorders premises by cost estimate |
| **Conjunction** | The ordered execution plan |
| **Adornment** | Bitfield encoding which parameters are bound vs free |

## How to Read This Book

The book follows a query's lifecycle:

1. **From Syntax to Query** — How domain models map to query patterns.
2. **Planning** — How premises are reordered and cached by binding pattern.
3. **Evaluation** — How matches flow through the premise pipeline.
4. **Storage and Replication** — How the search tree indexes claims and how
   partial replication works.
5. **Reference** — Relation to magic sets and glossary.

## Crate Map

```
dialog-query       Core query engine (planner, evaluation, types)
dialog-artifacts   Claim storage, triple indexes, artifact selectors
dialog-prolly-tree Content-addressed probabilistic B-tree
dialog-storage     Storage backends (memory, filesystem, S3, IndexedDB)
dialog-macros      Derive macros for Attribute, Concept, Formula
```
