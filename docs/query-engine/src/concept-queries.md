# Concept Queries

A `ConceptQuery` matches entities that satisfy a concept — a named collection
of attributes. Under the hood, it expands into multiple relation queries (one
per attribute), coordinated by the planner and cached by adornment.

## Structure

```rust
pub struct ConceptQuery {
    pub terms: Parameters,
    pub predicate: ConceptDescriptor,
}
```

- **`terms`** — Maps parameter names (`this`, `name`, `role`, ...) to user
  terms (constants, variables, or blanks).
- **`predicate`** — The concept descriptor, listing required attributes with
  their types and cardinalities.

## Evaluation Flow

Concept evaluation is the most complex part of the query engine. Here's the
step-by-step flow:

### 1. Acquire Rules

The session provides `ConceptRules` for the concept:

```rust
let concept_rules = source.acquire(&predicate)?;
```

This returns the concept's implicit rule (fetch all attributes) plus any
user-installed deductive rules.

### 2. Compute Adornment and Get Plan

For each incoming answer, derive the adornment (which parameters are bound)
and look up the cached plan:

```rust
let adornment = Adornment::derive(&terms, &answer);
let plan = concept_rules.plan(&terms, &answer);
```

If no cached plan exists for this adornment, one is compiled and cached.

### 3. Extract Parameters

Before evaluating the plan, the engine maps **user variable names** to
**internal parameter names**:

```rust
fn extract_parameters(source: &Answer, terms: &Parameters) -> Answer
```

This creates a fresh answer scoped to the concept's parameters. For example:

```
User query:  PersonMatch { this: ?employee, name: ?n }
Parameters:  { "this": var("employee"), "name": var("n") }

If incoming answer has: { ?employee → alice, ?n → "Alice" }

Extracted answer: { ?this → alice, ?name → "Alice" }
                    ↑ internal names    ↑ from user bindings
```

This scoping prevents variable name collisions between the outer query and the
concept's internal evaluation.

### 4. Evaluate Plan

The extracted answer is used as the seed for evaluating the concept's plan
(a `Disjunction` of `Conjunction`s):

```
Extracted answer → Rule 1 plan → answers
                   Rule 2 plan → answers
                   ...
                   (all merged via disjunction)
```

Each rule's `Conjunction` executes its premises in the planned order, expanding
answers as relation queries match claims.

### 5. Merge Parameters Back

After evaluation, the engine maps **internal parameter names** back to **user
variable names**:

```rust
fn merge_parameters(base: &Answer, result: &Answer, terms: &Parameters) -> Answer
```

This reintegrates the concept's results into the outer query's answer:

```
Rule result:    { ?this → alice, ?name → "Alice", ?role → "Engineer" }
Merged back:    { ?employee → alice, ?n → "Alice", ?role_var → "Engineer" }
                  ↑ restored to user names
```

### 6. Yield Answers

The merged answers flow to the next premise in the outer query.

## Lazy Rule Loading

Concept rules are loaded **on demand**. The `Source::acquire()` method:

1. Checks the `RuleRegistry` for installed rules
2. Always includes the implicit rule (direct attribute lookup)
3. Returns `ConceptRules` with an empty plan cache

Plans are then compiled lazily on first query via adornment caching.

## The Implicit Rule

Every concept has an **implicit rule** that fetches attributes directly from
the store. For a `Person` concept with `name` and `role` attributes, the
implicit rule is equivalent to:

```
Person(?this, ?name, ?role) :-
    (person/name, ?this, ?name),
    (person/role, ?this, ?role).
```

This rule is always present and ensures that entities with the right attributes
match the concept even if no deductive rules are installed.

## Deductive Rules

Users can install additional rules that derive concept instances from other
data:

```rust
session.rule::<Person>(
    PersonMatch::default(),
    vec![
        // premises that derive a Person from other facts
    ],
);
```

Multiple rules form a **disjunction**: an entity matches the concept if it
satisfies the implicit rule OR any installed deductive rule.

## Nested Concepts

Concept queries can appear as premises in other concept's rules, creating
nested evaluation:

```
Employee(?e, ?name, ?dept) :-
    Person(?e, ?name, ?role),          ← nested concept query
    (department/member, ?dept, ?e).
```

When the `Person` concept query is encountered during `Employee` evaluation:
1. A new `ConceptQuery` is created for `Person`
2. Its adornment is derived from the current answer (e.g., `?e` might be bound)
3. A cached plan is retrieved or compiled
4. Parameters are extracted, the plan is evaluated, and results are merged back

This nesting can go arbitrarily deep, with each level maintaining its own
adornment cache.

## Cost Estimation

A concept's cost is the sum of its attribute lookup costs plus `CONCEPT_OVERHEAD`:

```rust
pub fn estimate(&self, env: &Environment) -> Option<usize> {
    let mut total = CONCEPT_OVERHEAD;

    if entity_bound {
        // Sum costs of all attribute lookups with entity constrained
        for attr in self.predicate.with() {
            total += attr.estimate_with_entity_bound();
        }
    } else {
        // Find cheapest lead attribute, then add others with entity bound
        let (lead_cost, remaining_cost) = find_best_lead_strategy();
        total += lead_cost + remaining_cost;
    }

    Some(total)
}
```

The `CONCEPT_OVERHEAD` (1,000) reflects the potential cost of:
- Looking up rules in the registry
- Checking the adornment cache
- Evaluating deductive rules (if any)
