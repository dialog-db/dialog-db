# Design decision: how the planner counts a pattern's variables

**Problem it serves:** a parameter has no way to hold more than one variable (the topic's [problem](../patterns.md)). The planner orders premises by which variables each binds and which it needs, one variable a slot (`categorize`, `rust/dialog-query/src/planner/feasibility.rs`). A slot that holds a pattern holds several.

**Decision:** when a slot that holds a map, a list or a tuple counts as bound, what it binds, and what it requires.

## Facts the matrix rests on

- `categorize` is the one definition of feasibility. The planner orders by it and the rule analyzer builds its dependency graph from it.
- A slot is `Required`, `Required` in a choice group, or `Optional`. A choice group is satisfied when any member is a constant or bound, and its other members are then bound by the premise. An equality's two sides are one group.
- A negation never binds. Its slots are requirements, blanks excepted (`Negation::schema`).
- [design-parameters.md](design-parameters.md) decided that a concept premise holding a pattern is planned as the concept over a whole and an equality between the whole and the pattern. So the slot a planner meets a pattern in is an equality's side, and a negated concept's.

```
 ?card == {title: ?t, body: ?b}        one choice group: this, is

 bound: t, b      ->  the map is a value: feasible, binds card      (built)
 bound: card      ->  feasible, binds t and b                       (taken apart)
 bound: card, t   ->  feasible, binds b; t is matched
 bound: t         ->  not feasible: needs card and b
```

| Decision: a slot that holds a pattern, for: a premise feasible by the variables in its patterns |
|---|---|---|---|
| criterion \ approach | A: as one variable, the whole | B: bound when any variable inside is bound | C: bound when every variable inside is bound; binds or requires each that is not |
| a record is built only when it can be | 🔴 the whole is bound by nothing, so nothing is ever built | 🔴 a map with one of two variables bound counts as a value and is not one | 🟢 a pattern is a value exactly when all its variables are bound |
| a variable inside is known to be bound afterwards | 🔴 the planner does not know the premise binds `t`, so a head that needs `t` is refused | holds | 🟢 holds |
| what a refusal names | the whole, a name nobody wrote | holds | 🟢 the variables inside that nothing binds, and not the ones bound already |
| a blank inside | n/a | n/a | never bound, so the pattern is never a value and can only be taken apart; it is not required either, as a blank slot is not |

## Decided

C. A slot that holds a shape is bound when `Pattern::is_bound`: every variable inside it is, and a blank never is. Where the premise binds the slot it binds each variable inside that is not bound yet. Where it requires the slot it requires each of those.

- The equality's own cost stays 1 (`Equality::estimate`, now over `Pattern::is_bound`), so the planner takes it as soon as it is feasible: a record is built before the premise that is read by it, and a row is taken apart right after the premise that yielded it.
- A rule whose pattern can never be bound fails to plan with `TypeError::RequiredBindings`, which names the variables inside the pattern that nothing binds (`it_refuses_a_rule_whose_pattern_can_never_be_bound`).
- An equality with neither side bound names both sides' variables, though binding either side would do. That is what it does for two terms today.

The claim that a premise can be planned by the variables inside its patterns is confirmed by `it_takes_a_record_apart_in_either_order_of_premises` and `it_builds_a_record_in_either_order_of_premises`.
