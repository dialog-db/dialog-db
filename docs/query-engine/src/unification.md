# Unification and Answers

This chapter explains how variable bindings accumulate during query evaluation,
how new matches expand the answer set, and how conflicts eliminate answers.
This is the core mechanism that makes Datalog-style pattern matching work.

## What Is an Answer?

An `Answer` is a set of variable bindings accumulated during query evaluation.
It maps variable names to their bound values, along with provenance information
tracking *how* each binding was obtained.

```rust
pub struct Answer {
    conclusions: HashMap<String, Factors>,
    facts: HashMap<RelationQuery, Arc<Claim>>,
}
```

- **`conclusions`** — Named variable bindings. Each variable maps to a
  `Factors` value that records both the bound value and its provenance.
- **`facts`** — Tracks which claims were matched by which relation queries,
  enabling change detection and incremental re-evaluation.

## The Evaluation Pipeline

Query evaluation is a **pipeline of premises**, where each premise receives a
stream of answers from the previous one and produces a (possibly larger or
smaller) stream of answers:

```
Empty answer
    │
    ▼
┌──────────┐    ┌──────────┐    ┌──────────┐
│ Premise 1 │───▶│ Premise 2 │───▶│ Premise 3 │───▶ Results
└──────────┘    └──────────┘    └──────────┘
```

Each premise acts as a **filter-and-expander**:
- For each incoming answer, it produces **zero or more** expanded answers
- An answer with **zero** expansions is eliminated (filtered out)
- An answer with **multiple** expansions is multiplied (one copy per match)

This is implemented via the `Answers` trait (a `Stream` of `Result<Answer>`):

```rust
pub trait Answers:
    Stream<Item = Result<Answer, EvaluationError>> + 'static
{
    fn try_flat_map<S, F>(self, f: F) -> impl Answers
    where S: Answers, F: FnMut(Answer) -> S;
}
```

## How Answers Expand

When a premise finds a match, it **clones the incoming answer and merges new
bindings into it**:

```
Incoming answer: { ?person → Entity(alice) }
                        │
Premise: (person/age, ?person, ?age)
Matched claim: (person/age, alice, 30)
                        │
                        ▼
Expanded answer: { ?person → Entity(alice), ?age → 30 }
```

If the premise matches multiple claims, each match produces a separate
expanded answer:

```
Incoming: { }  (empty)
                │
Premise: (person/name, ?p, ?name)
Matches: alice→"Alice", bob→"Bob"
                │
          ┌─────┴─────┐
          ▼           ▼
{ ?p→alice,       { ?p→bob,
  ?name→"Alice" }   ?name→"Bob" }
```

## How Answers Get Eliminated

Answers are eliminated in three ways:

### 1. Unification Failure

If a premise tries to bind a variable to a value **different from its existing
binding**, the answer is discarded:

```
Incoming: { ?person → Entity(alice) }
                    │
Premise: (person/city, ?person, "NYC")
No claim: (person/city, alice, "NYC")
                    │
                    ▼
              (eliminated — no match)
```

Even if there *is* a claim `(person/city, alice, "Boston")`, it won't match
because `"Boston" ≠ "NYC"`.

When `Answer::assign()` detects a conflict, it returns an error:

```rust
if factors.content() != factor.content() {
    return Err(EvaluationError::Assignment { ... });
}
```

This error is caught by the evaluation pipeline and the answer is dropped.

### 2. No Matches

If a relation query finds no matching claims for the given constraints, the
incoming answer simply produces no expanded answers — it's filtered out by the
flat-map.

### 3. Negation

An `Unless` premise inverts the logic: if the inner proposition produces *any*
match, the answer is eliminated. If it produces *no* matches, the answer
passes through unchanged:

```
Incoming: { ?person → alice }
                │
Unless: (person/retired, ?person, true)
                │
    ┌───────────┴───────────┐
    │ Match found           │ No match
    ▼                       ▼
(eliminated)         { ?person → alice }
                     (passes through)
```

## Evidence and Provenance

Every binding in an answer carries **provenance** — a record of how the value
was obtained. This is tracked through the `Factor` enum:

```rust
pub enum Factor {
    Selected {
        selector: Selector,           // which component: The, Of, Is, or Cause
        application: Arc<RelationQuery>,
        fact: Arc<Claim>,
    },
    Derived {
        value: Value,
        from: HashMap<String, Factors>,  // input factors
        formula: Arc<FormulaQuery>,
    },
    Parameter {
        value: Value,
    },
}
```

- **`Selected`** — The value came from a specific claim matched by a specific
  relation query.
- **`Derived`** — The value was computed by a formula, with references to the
  input factors used.
- **`Parameter`** — The value was provided externally as a query parameter.

### Factors (Multi-Evidence)

A variable can accumulate multiple factors that all agree on the same value.
This happens when the same variable is bound by multiple premises:

```rust
pub struct Factors {
    primary: Factor,
    alternates: HashSet<Factor>,
}
```

If a second premise binds `?name` to `"Alice"` when it's already bound to
`"Alice"`, the new factor is added as an **alternate** — confirming the binding
from a different source. If it tries to bind `?name` to `"Bob"`, that's a
unification failure and the answer is eliminated.

### Evidence

The `Evidence` enum is the input format for creating factors:

```rust
pub enum Evidence<'a> {
    Relation {
        application: &'a RelationQuery,
        fact: &'a Claim,
    },
    Derived {
        term: &'a Term<Any>,
        value: Box<Value>,
        from: HashMap<String, Factors>,
        formula: &'a FormulaQuery,
    },
    Parameter {
        term: &'a Term<Any>,
        value: &'a Value,
    },
}
```

When a relation query matches a claim, it creates `Evidence::Relation` and
calls `answer.merge(evidence)`. This produces four `Factor::Selected` values
(one per component of the claim) and assigns each to its corresponding
variable.

## Unification in Detail

Unification is the process of combining variable bindings across premises. It
operates through a simple rule:

> **A variable can be bound to at most one value per answer. If two premises
> bind the same variable to the same value, the bindings are consolidated. If
> they bind it to different values, the answer is eliminated.**

This is what makes joins work without explicit join syntax. Consider:

```
Premise 1: (person/name, ?person, ?name)
Premise 2: (person/age,  ?person, ?age)
```

The shared variable `?person` acts as a join key. When premise 1 binds
`?person` to `Entity(alice)`, premise 2 must also match `Entity(alice)` — any
claim with a different entity would fail unification.

### Comparison with Traditional Datalog

In traditional Datalog implementations (as described in the
[InstantDB essay on Datalog](https://www.instantdb.com/essays/datalogjs)),
query evaluation works similarly:

1. Start with an empty context (no bindings)
2. For each pattern, find matching triples
3. If a variable is new, bind it (expand the context)
4. If a variable is already bound, check that the value matches (unify)
5. Failed matches return null and are filtered out

Dialog follows the same logic, but adds **provenance tracking** (factors) and
**streaming evaluation** (answers flow through an async pipeline rather than
being collected eagerly).

## The Selector

When a relation query matches a claim, the `Selector` enum records which
component of the claim contributed each binding:

```rust
pub enum Selector {
    The,    // the attribute (e.g., person/name)
    Of,     // the entity
    Is,     // the value
    Cause,  // the provenance hash
}
```

This enables precise tracking of which part of which claim produced each
variable binding.

## Seeding Evaluation

Every query starts with a single empty answer, called the **seed**:

```rust
Answer::new().seed()  // Stream containing one empty answer
```

This seed flows into the first premise, which expands it into concrete
answers. Each subsequent premise further refines or expands those answers.

If the seed were absent, no answers would flow through the pipeline and the
query would produce no results.
