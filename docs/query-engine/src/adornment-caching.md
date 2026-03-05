# Adornment Caching

When a concept query executes, the engine needs a plan for evaluating its
rules. Different callers provide different bound variables, requiring different
plan orderings. The adornment cache ensures each binding pattern is planned
only once.

## What Is an Adornment?

An `Adornment` is a `u64` bitfield where each bit represents one of a concept's
parameters (sorted alphabetically). Set = bound, clear = free.

```
Concept: Employee { this, name, salary }
Sorted:  [name, salary, this]

name bound:          0b001
this bound:          0b100
name + this bound:   0b101
nothing bound:       0b000
```

A parameter is bound if it is a constant or a named variable already in the
incoming match.

## How Adornments Drive Planning

Each adornment produces a different plan:

- **name bound** → look up `employee/name` first (cheap), then look up
  `employee/salary` (entity now bound)
- **this bound** → look up any attribute first (entity constrains everything)
- **nothing bound** → scan the cheapest attribute, then look up the rest

The adornment is the cache key.

## The ConceptRules Cache

```rust
pub struct ConceptRules {
    implicit: DeductiveRule,
    installed: Vec<DeductiveRule>,
    plans: Arc<RwLock<HashMap<Adornment, Arc<Disjunction>>>>,
}
```

### Lookup Flow

1. Derive adornment from terms and incoming match
2. **Read lock**: check cache → hit returns cached `Arc<Disjunction>`
3. **Cache miss**: convert adornment → environment, re-plan all rules, combine
   into `Disjunction`, write to cache

Most queries hit the cache at step 2.

## Re-planning

On cache miss, each rule's `Conjunction` is re-planned for the new scope. The
premises are extracted, a fresh `Planner` runs with the new environment, and a
new `Conjunction` with potentially different ordering is produced.

## Cache Invalidation

The cache is cleared when new rules are installed, since a new rule changes the
set of alternatives in the disjunction. It repopulates lazily on the next query.
