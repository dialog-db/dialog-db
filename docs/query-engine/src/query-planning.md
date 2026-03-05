# Query Planning

The planner decides the order in which premises execute. Order matters because
binding a variable in one premise reduces the cost of subsequent premises that
use it.

## The Algorithm

```
plan(premises, outer_scope) → Conjunction:

    bound ← clone(outer_scope)
    steps ← []

    while premises remain:
        step ← pick cheapest viable candidate given bound
        bound ← bound ∪ step.binds
        steps.push(step)

    return Conjunction { steps, cost, binds: bound \ outer_scope }
```

Each selected step enriches the environment, potentially reducing cost of
remaining candidates and unblocking previously blocked ones.

## Candidates

A `Candidate` wraps a premise with planning metadata:

- **Viable** — all prerequisites satisfied, ready to execute. Has a `cost` and
  a set of variables it will `bind`.
- **Blocked** — one or more required variables are missing. Lists the
  `requires` set.

When a candidate is created from a premise, each parameter's `Requirement`
is checked:

- **`Required(None)`** — must be externally bound.
- **`Required(Some(group))`** — part of a choice group. Satisfied if *any*
  member of the group is bound.
- **`Optional`** — will be produced by this premise.

### Choice Groups

A choice group ties together interchangeable inputs. For an `AttributeQuery`,
`(the, of, is)` form a choice group — binding any one suffices because the
store has indexes for each (EAV, AEV, VAE).

### Incremental Updates

After each step, remaining candidates are updated: parameters that entered
scope move from `requires` to `env`, costs are re-estimated, and blocked
candidates may become viable.

## Conjunctions

A `Conjunction` is the complete ordered execution plan. During evaluation, each
step receives the output of the previous step:

```
seed match → Step 1 → matches → Step 2 → matches → ... → final matches
```

A conjunction can be **re-planned** against a different scope, producing a new
step order. This is used by the adornment caching system.

## Worked Example

Three premises:

```
P1: (person/name, ?person, ?name)     — 1 constant, 2 variables
P2: (person/city, ?person, "NYC")     — 2 constants, 1 variable
P3: (person/age,  ?person, ?age)      — 1 constant, 2 variables
```

| Step | Chosen | Why | Cost |
|------|--------|-----|------|
| 1 | **P2** | 2 constants → cheapest | 100 |
| 2 | **P1** | `?person` now bound | 100 |
| 3 | **P3** | `?person` still bound | 100 |

Total: 300. Without planning (P1 first): 1,200.
