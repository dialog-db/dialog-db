# Relation to Magic Sets

Dialog's query planning draws from the **magic set transformation** (Bancilhon,
Maier, Sagiv & Ullman, 1986), a classical optimization from deductive databases
that avoids computing all possible derivations when only a subset is needed.

## The Core Idea

Magic sets push query constraints into rules via **adornments** — `b`/`f`
markers indicating which parameters are bound:

```
ancestor^bf(alice, ?who)  — first param bound, second free
```

Each adornment generates a specialized version of the rule that only computes
reachable results.

## How Dialog Relates

| Aspect | Magic Sets | Dialog |
|--------|-----------|--------|
| Binding pattern | Adornment string (`bf`) | `Adornment(u64)` bitfield |
| Specialization | Rewrite rules | Re-plan premises |
| When | Before evaluation | On demand (lazy) |
| Caching | Tabling (results) | Plan cache (strategies) |
| Recursion | Full support | Non-recursive only |
| Join ordering | Optimal per adornment | Greedy per adornment |

## Key Differences

**Non-recursive rules only.** For non-recursive rules, the magic set
transformation reduces to pushing selections into joins — which is what
Dialog's planner does when given a richer environment. No "magic" predicates
are needed.

**No program transformation.** Classical magic sets rewrite the Datalog program.
Dialog re-plans existing premises for each adornment. Rules stay the same; only
execution order changes.

**Plan cache vs tabling.** Magic sets use tabling (result memoization) for
termination of recursive rules. Dialog caches plans (execution strategies), not
results. The `Adornment` type already serves as a natural cache key for future
result-level caching if recursive rules are added.

## Future: Recursive Rules

The adornment infrastructure positions Dialog for future recursive rule support:
adornments identify calling patterns, plan caching can extend to result caching
(tabling), and stratification (already used for negation) would determine
evaluation order.
