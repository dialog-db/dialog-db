# Dialog Query Engine Internals

This book documents the inner workings of Dialog's query engine, aimed at
contributors who want to understand, debug, or extend the system. It traces a
query from its initial expression through planning, evaluation, and storage,
explaining each stage and the design decisions behind it.

## What Dialog Is

Dialog is an embeddable, local-first database built on an append-only store of
**claims** — immutable `(the, of, is, cause)` tuples that describe entities and
their properties. It uses a Datalog-inspired query engine to interpret these
claims at read time, meaning there is no fixed schema enforced at write time.
Different applications can impose their own schemas on the same underlying data.

The query engine sits at the heart of this system. It takes a set of
**premises** (patterns to match against claims), plans an efficient execution
order, evaluates them against the store, and produces a stream of **answers** —
sets of variable bindings that satisfy all the premises simultaneously.

## Key Concepts at a Glance

| Concept | What it does |
|---------|-------------|
| **Claim** | An immutable `(the, of, is, cause)` fact in the store |
| **Term** | A query building block: either a constant value or a variable placeholder |
| **Premise** | A single pattern or constraint in a query |
| **Answer** | A set of variable bindings produced by evaluating premises |
| **Planner** | Reorders premises for efficient execution based on cost estimates |
| **Conjunction** | The ordered execution plan produced by the planner |
| **Adornment** | A compact bitfield encoding which parameters are bound vs. free |
| **Prolly Tree** | The content-addressed index structure that stores claims |

## How to Read This Book

The book follows the lifecycle of a query:

1. **From Syntax to Query** — How domain models are defined (via Rust macros or
   JSON notation) and how query patterns are constructed from them.

2. **Planning** — How the planner reorders premises, estimates costs, and
   caches plans by binding pattern.

3. **Evaluation** — How answers flow through the premise pipeline, expanding
   when new matches are found and being eliminated when unification fails.

4. **Storage and Replication** — How the prolly tree indexes claims and how
   partial replication works transparently during query evaluation.

5. **Reference** — How Dialog's approach relates to the magic set transformation
   and a glossary of terms.

## Crate Map

The query engine spans several crates in the workspace:

```
dialog-query       The core query engine (planner, evaluation, types)
dialog-artifacts   Claim storage, triple indexes, artifact selectors
dialog-prolly-tree Content-addressed probabilistic B-tree
dialog-storage     Storage backends (memory, filesystem, S3, IndexedDB)
dialog-macros      Derive macros for Attribute, Concept, Formula
dialog-common      Shared utilities and cross-platform abstractions
```

Most of this book focuses on `dialog-query`, reaching into `dialog-artifacts`
and `dialog-prolly-tree` when discussing storage and replication.
