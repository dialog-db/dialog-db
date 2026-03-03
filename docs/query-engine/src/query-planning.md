# Query Planning

The planner decides the order in which premises execute. Order matters because
binding a variable in one premise reduces the cost of subsequent premises that
use that variable. The planner's job is to find a cheap ordering.

## The Planner State Machine

The `Planner` is a two-state machine defined in `dialog-query/src/planner.rs`:

```
         ┌──────────────────────────────┐
         │  Idle { premises: Vec }      │
         └──────────┬───────────────────┘
                    │  first call to top()
                    ▼
         ┌──────────────────────────────┐
         │  Active { candidates: Vec }  │◄─── subsequent top() calls
         └──────────────────────────────┘     update candidates in-place
```

- **Idle**: Holds raw, unanalyzed premises. On the first call to `top()`,
  each premise is wrapped in a `Candidate` and scored.
- **Active**: Holds scored `Candidate` values. Each call to `top()` selects
  the cheapest viable one, removes it, and re-scores the rest.

## The Planning Algorithm

```
plan(premises, outer_scope) → Conjunction:

    bound ← clone(outer_scope)
    steps ← []
    cost  ← 0

    while premises remain:
        step ← top(bound)          // pick cheapest viable candidate
        cost ← cost + step.cost
        bound ← bound ∪ step.binds // new variables flow into scope
        steps.push(step)

    binds ← bound \ outer_scope    // variables new to this plan
    return Conjunction { steps, cost, binds, env: outer_scope }
```

The key insight is that **each selected step enriches the environment**,
potentially reducing the cost of remaining candidates and unblocking previously
blocked ones.

## Candidates

A `Candidate` wraps a premise with planning metadata. It has two states:

### Viable

All prerequisites are satisfied — the premise can execute now.

```rust
Candidate::Viable {
    premise,     // the premise to execute
    cost,        // estimated execution cost
    binds,       // variables this premise will produce
    env,         // variables already bound when this was scored
    schema,      // cached parameter schema
    params,      // cached parameters
}
```

### Blocked

One or more required variables are missing.

```rust
Candidate::Blocked {
    premise,
    cost,
    binds,
    env,
    requires,    // variables that must be bound first
    schema,
    params,
}
```

### How Candidates Are Created

When a `Candidate` is created from a premise:

1. Extract the premise's `Schema` (parameter metadata) and `Parameters` (term
   bindings).

2. For each parameter, check its `Requirement` from the schema:
   - **`Required(None)`** — must be externally bound. If the parameter is a
     variable not in the environment, add it to `requires`.
   - **`Required(Some(group))`** — part of a choice group. If *any* member of
     the group is bound (constant or in env), the whole group is satisfied.
     Otherwise, add to `requires`.
   - **`Optional`** — can be derived. If unbound, add to `binds` (this premise
     will produce it).

3. If `requires` is empty → `Viable`. Otherwise → `Blocked`.

### Choice Groups

A **choice group** ties together parameters that are interchangeable inputs.
For a `RelationQuery`, the `(the, of, is)` parameters form a choice group:
if any one of them is bound, the others become outputs rather than
prerequisites.

This is because the store maintains multiple indexes (EAV, AEV, VAE) — knowing
any one component is enough to constrain the scan.

### Incremental Updates

When the planner selects a step and extends the environment, remaining
candidates are updated via `candidate.update(&new_scope)`:

```
For each parameter in schema:
  if param entered scope    → move from requires/binds to env
  if param left scope       → move from env back to requires/binds
  (supports both growth and replanning)

Re-estimate cost with updated env.
If requires is now empty → transition Blocked → Viable.
```

This is **bidirectional**: the update handles both the normal case (scope
grows as the planner proceeds) and the replanning case (scope differs when
re-optimizing a cached plan for a new adornment).

## Plans

Once a viable candidate is selected, it becomes a `Plan`:

```rust
pub struct Plan {
    pub premise: Premise,
    pub cost: usize,
    pub binds: Environment,   // variables this step produces
    pub env: Environment,     // variables bound when this was planned
}
```

Plans drop the cached schema and params — they're not needed during execution.

## Conjunctions

A `Conjunction` is the complete, ordered execution plan:

```rust
pub struct Conjunction {
    pub steps: Vec<Plan>,
    pub cost: usize,
    pub binds: Environment,   // all new variables produced
    pub env: Environment,     // outer scope
}
```

### Evaluation

During evaluation, each step receives the output of the previous step:

```
seed answer → Step 1 → answers → Step 2 → answers → ... → final answers
```

Each step acts as a filter-and-expander: it takes each incoming answer,
matches it against the store, and produces zero or more expanded answers with
new bindings.

### Re-planning

A conjunction can be re-planned against a different scope:

```rust
conjunction.plan(&new_scope) → new Conjunction
```

This extracts the premises from the existing steps, creates a new `Planner`,
and runs the planning algorithm with the new scope. The result may have a
different step order and different costs. This is used by the adornment
caching system (covered in the [Adornment Caching](./adornment-caching.md)
chapter).

## Worked Example

Consider three premises for querying people with their cities and ages:

```
P1: (person/name, ?person, ?name)     — 1 constant, 2 variables
P2: (person/city, ?person, "NYC")     — 2 constants, 1 variable
P3: (person/age,  ?person, ?age)      — 1 constant, 2 variables
```

### Planning in Empty Scope

| Step | Candidates | Chosen | Why |
|------|-----------|--------|-----|
| 1 | P1: 1000, P2: 100, P3: 1000 | **P2** (cost 100) | 2 constants → direct lookup |
| 2 | P1: 100, P3: 100 | **P1** (cost 100) | `?person` now bound from P2 |
| 3 | P3: 100 | **P3** (cost 100) | `?person` still bound |

Total cost: 300. The planner chose P2 first because it had the most constants,
making it the cheapest scan. After P2 bound `?person`, P1 and P3 dropped from
1000 to 100.

### Without Planning (Original Order)

| Step | Cost |
|------|------|
| P1 first | 1000 (scan all names) |
| P2 second | 100 (lookup by person) |
| P3 third | 100 (lookup by person) |

Total cost: 1200. Four times more expensive.
