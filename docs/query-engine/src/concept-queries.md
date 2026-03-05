# Concept Queries

A `ConceptQuery` matches entities that satisfy a concept — a named collection
of attributes. It expands into multiple attribute queries (one per attribute),
coordinated by the planner and cached by adornment.

## Structure

```rust
pub struct ConceptQuery {
    pub terms: Parameters,
    pub predicate: ConceptDescriptor,
}
```

## Evaluation Flow

### 1. Acquire Rules

The session provides `ConceptRules` — the implicit rule (fetch all attributes)
plus any user-installed deductive rules:

```rust
let concept_rules = source.acquire(&predicate)?;
```

### 2. Compute Adornment and Get Plan

Derive the adornment (which parameters are bound) and look up the cached plan:

```rust
let adornment = Adornment::derive(&terms, &current_match);
let plan = concept_rules.plan(&terms, &current_match);
```

### 3. Scoped Evaluation

Before evaluating the plan, the engine maps **user variable names** to
**internal parameter names** via `extract_parameters`. This prevents name
collisions between the outer query and the concept's internal evaluation.

After evaluation, `merge_parameters` maps results back to user names.

### 4. Evaluate Plan

The extracted match seeds a `Disjunction` of `Conjunction`s — one per rule.
All alternatives are evaluated and their streams merged.

## The Implicit Rule

Every concept has an implicit rule that fetches attributes directly from the
store. For `Employee` with `name` and `role`:

```
Employee(?this, ?name, ?role) :-
    (employee/name, ?this, ?name),
    (employee/role, ?this, ?role).
```

This ensures entities with the right attributes match even without deductive
rules.

## Deductive Rules

Users install additional rules via `session.install(rule_fn)`:

```rust
fn employee_from_person(employee: Query<Employee>) -> impl When {
    (
        Query::<Person> {
            this: employee.this.clone(),
            name: employee.name.clone(),
            title: employee.role.clone(),
        },
    )
}

session.install(employee_from_person)?;
```

Multiple rules form a disjunction: an entity matches if it satisfies the
implicit rule OR any installed rule.

## Nested Concepts

Concept queries can nest arbitrarily deep, each level maintaining its own
adornment cache.

## Cost

A concept's cost is the sum of its attribute lookup costs plus
`CONCEPT_OVERHEAD` (1,000). When the entity is unbound, the planner finds the
cheapest lead attribute and estimates the rest as if the entity were bound.
