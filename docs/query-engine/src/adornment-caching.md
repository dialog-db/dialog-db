# Adornment Caching

When a concept query is executed, the engine needs a plan for evaluating the
concept's rules. Different callers may provide different bound variables,
requiring different plan orderings. The **adornment caching** system ensures
each binding pattern is planned only once.

## What Is an Adornment?

An `Adornment` is a compact `u64` bitfield where each bit represents one of a
concept's parameters (sorted alphabetically). A set bit means the parameter is
**bound**; a clear bit means it is **free**.

```
Concept: Person { this, name, role }
Sorted parameters: [name, role, this]

Adornment when name is bound:     0b001  (bit 0 = name)
Adornment when this is bound:     0b100  (bit 2 = this)
Adornment when name+this bound:   0b101
Adornment when nothing bound:     0b000
```

A parameter is considered bound if it is either:
- A **constant** in the query terms
- A **named variable** that the incoming answer already contains

### Why Alphabetical Order?

Sorting by parameter name ensures the adornment is deterministic regardless of
the insertion order of the `Parameters` map. Two callers with the same binding
pattern always produce the same adornment.

## How Adornments Drive Planning

The adornment captures the **calling pattern**, which variables are known when
the concept is queried. This determines which premises should run first:

```
Adornment 0b001 (name bound):
  -> Plan: look up person/name first (cheap, name is constrained)
  -> Then: look up person/role (entity now bound from step 1)

Adornment 0b100 (this bound):
  -> Plan: look up person/name first (cheap, entity is constrained)
  -> Then: look up person/role (entity already bound)

Adornment 0b000 (nothing bound):
  -> Plan: scan person/name (expensive, no constraints)
  -> Then: look up person/role (entity now bound)
```

Each adornment produces a different optimal plan. The adornment serves as the
cache key.

## The ConceptRules Cache

The `ConceptRules` struct manages per-concept rule storage and plan caching:

```rust
pub struct ConceptRules {
    implicit: DeductiveRule,
    installed: Vec<DeductiveRule>,
    plans: Arc<RwLock<HashMap<Adornment, Arc<Disjunction>>>>,
}
```

- **`implicit`**: The default rule that fetches all attributes directly from
  the store. Every concept has one.
- **`installed`**: User-defined deductive rules registered via
  `session.rule::<Concept>(...)`.
- **`plans`**: A read-write locked cache mapping adornments to compiled
  disjunctions.

### Cache Lookup Flow

```
concept_rules.plan(terms, answer):

  1. Derive adornment from terms and answer
  2. Read lock: check cache for this adornment
     |-- Hit -> return cached Arc<Disjunction>
     +-- Miss -> continue to step 3
  3. Convert adornment -> Environment
  4. Re-plan all rules (implicit + installed) for this environment
  5. Combine plans into Disjunction
  6. Write lock: insert into cache
  7. Return Arc<Disjunction>
```

Most queries hit the cache at step 2 with only a read lock.

## Disjunctions

A `Disjunction` represents the OR of multiple rule plans:

```rust
pub struct Disjunction {
    alternatives: Vec<Conjunction>,
}
```

Each `Conjunction` is one rule's plan optimized for the adornment. During
evaluation, all alternatives are evaluated and their answer streams are merged.
An entity matches the concept if any one alternative produces answers for it.

## Re-planning

When a cache miss occurs, each rule's pre-compiled `Conjunction` is
re-planned for the new scope:

```rust
// In ConceptRules::plan():
let scope = adornment.into_environment(&terms);

let plans: Vec<Conjunction> = rules
    .iter()
    .map(|rule| rule.join.plan(&scope))
    .collect();
```

The `conjunction.plan(&scope)` method:

1. Extracts the premises from the existing steps
2. Creates a fresh `Planner`
3. Runs the planning algorithm with the new scope
4. Returns a new `Conjunction` with potentially different ordering and costs

This means a rule compiled with no bound variables can be re-optimized when
called with specific bindings.

## Cache Invalidation

The plan cache is cleared when new rules are installed:

```rust
pub fn install(&mut self, rule: DeductiveRule) {
    self.installed.push(rule);
    self.plans.write().clear();  // invalidate all cached plans
}
```

This is correct because a new rule changes the set of alternatives in the
disjunction. The cache repopulates lazily on the next query.

## Relation to Magic Sets

The adornment system is directly inspired by the **magic set transformation**
from deductive databases (Bancilhon et al., 1986). In classical magic sets:

1. Each rule is annotated with an adornment string (b/f markers)
2. "Magic" predicates are generated to push selections into recursive rules
3. The result is goal-directed evaluation instead of bottom-up computation

Dialog's approach is similar but simplified for its current scope:

- Adornments serve the same role as magic set adornment strings
- Plan re-optimization serves the same role as generating specialized rules
- The plan cache serves the same role as tabling/memoization

The difference is that Dialog currently handles **non-recursive** rules only.
For non-recursive rules, magic sets reduce to pushing selections into joins,
which is what the planner does when given a richer environment.

The adornment infrastructure also positions Dialog for future support of
**recursive rules** (fixpoint evaluation), where adornments would serve as
cache keys for tabling (Tekle & Liu, 2011).

See the [Relation to Magic Sets](./magic-sets.md) chapter for more.

## Worked Example

```rust
// Define concept
#[derive(Concept)]
struct Employee {
    this: Entity,
    name: employee::Name,
    salary: employee::Salary,
}

// Query 1: find all employees (nothing bound)
let q1 = EmployeeMatch::default();  // all variables
// Adornment: 0b000

// Query 2: find employee by name (name bound)
let q2 = EmployeeMatch {
    name: Term::from("Alice".to_string()),
    ..Default::default()
};
// Adornment: 0b010

// Query 3: find employee by entity (this bound via answer)
// (called from within another query that already bound the entity)
// Adornment: 0b100
```

Each of these queries produces a different adornment, hitting the cache or
triggering a re-plan. Once planned, subsequent queries with the same binding
pattern reuse the cached plan.
