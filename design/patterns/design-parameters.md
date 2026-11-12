# Design decision: how `Parameters` holds a pattern

**Problem it serves:** a parameter has no way to hold more than one variable (the topic's [problem](../patterns.md)). The plan says `Parameters` maps a name to a pattern and every premise that reads a parameter as a variable or a constant keeps working for the leaves.

**Decision:** what `Parameters::get` answers for a name that holds a map, a list or a tuple, and where a premise that reads terms meets a pattern.

## Facts the matrix rests on

- `Parameters` is read at about four hundred places in the workspace, nearly all as `get(name) -> Option<&Term<Any>>` or `iter()` over terms. Tonk reads it too.
- Only `ConceptQuery` stores a `Parameters` (`terms`). Every other premise is a struct of typed terms and makes a `Parameters` when asked (`Equality::parameters`, a formula's, an attribute query's), so it could not hold a pattern in a field without that field changing type.
- A rule's body is evaluated by more than the plan: a recursive component joins its occurrences from tables by `occurrence.terms` (`rust/dialog-query/src/concept/query/fixpoint.rs`), commit-time induction reads `query.terms` (`rust/dialog-repository/src/repository/branch/transaction/induce.rs`), and a concept evaluates its own `terms` along five paths (`ConceptQuery::evaluate_unsorted`).
- `Equality` already runs both ways: with one side bound it infers the other (`rust/dialog-query/src/constraint/equality.rs`).
- A scope's rows do not see the caller's names (`Match::within`), so a name has to be unique within one rule body only.

```
 written      note/payload { this: ?note, is: {title: ?t, ...?rest} }

 held         Parameters   this -> ?note
                           is   -> whole: ?"{title: ?t, ...?rest}"   what get("is") answers
                                   shape: {title: ?t, ...?rest}      what shape("is") answers

 planned      note/payload { this: ?note, is: ?whole }
              ?whole == {title: ?t, ...?rest}
```

| Decision: what a premise that reads terms meets, for: a pattern in `Parameters` without breaking every reader |
|---|---|---|---|
| criterion \ approach | A: `get` answers a `&Pattern`, and every reader takes the leaf out | B: `get` answers the term, and for a shape a variable that stands for the whole value; the shape is beside it | C: a third case of `Term` |
| readers that keep compiling | 🔴 none of about four hundred, here and in tonk | 🟢 all; one that must look inside asks `shape` or `leaves` | 🔴 every exhaustive match on `Term`, and `Term<T>` for typed `T` gains a case it cannot hold |
| a reader that does not know patterns | cannot compile until it decides | 🟡 sees a variable it treats as any other, which is right for running the premise and wrong for asking which variables it mentions; those readers are listed below | cannot compile until it decides |
| where a pattern is evaluated | in each premise | 🟢 in one premise, an equality, and around a concept run by itself | in each premise |

| Decision: where the pattern is applied, for: a body evaluated by more than the plan |
|---|---|---|
| criterion \ approach | D: fused. Each plan step builds before its premise and takes apart after it | E: lowered. A concept premise is planned as the concept over the whole and an equality between the whole and the pattern |
| the recursive evaluator, induction and the concept's own paths | 🔴 each joins `terms` itself and would bind the whole and drop the pattern, without an error | 🟢 each meets terms only, and an equality, which all of them already run |
| the planner's cost | each premise's `estimate` has to learn that a shape is bound when its variables are | 🟢 the equality costs 1 and is taken as soon as it is feasible, so a value is built before a lookup and a row is dropped right after a scan |
| a negated premise | 🟢 one step asks whether any fact matches | 🔴 two premises cannot say "no fact matches" |
| a query run with no plan | 🟢 works | has no plan to lower in |

## Decided

B and E, with D where E cannot go.

- `Parameters` maps a name to a term and, for a map, a list or a tuple, the pattern beside it. `get` and `iter` answer the term: the leaf, or the whole. `shape`, `pattern`, `shapes`, `leaves` and `addressed` look inside. `insert` takes a term as before and `set` takes a pattern. A pattern with no variable is the value it builds and is held as that constant.
- The whole is a variable named by how the pattern is written, so two premises holding one pattern share it: with its variables bound it is one value in both. A pattern that holds a blank is not one value, and its whole is numbered within the body.
- `pattern::lower` rewrites a positive concept premise that holds a shape into the concept over the whole and an equality (`Equality::between`). It runs when a rule compiles and when a conjunction is planned. `Equality`'s two sides are patterns; an equality between two terms is the leaf case and runs as it did.
- A negated concept premise is left whole, and a concept takes its own shapes apart when it is evaluated (`pattern::build`, `pattern::take_apart` around `ConceptQuery::evaluate_application`). That is D, for the two cases the lower rows are red on: a negation, and a query run by itself.
- Readers that ask which variables a premise mentions were moved to `leaves`: the rename of a body's variables, the canonical labeling, and the bulk anti-join. Readers that rebuild a `Parameters` were moved to `map_terms` and `rekeyed`, which keep a shape.
- A premise that is a struct of typed terms cannot hold a shape. A notation that writes one in a formula's slot lowers it as `lower` does: a variable in the slot and an equality beside it.

`Equality`'s fields changed type from `Term<Any>` to `Pattern`. `Equality::new` takes terms as before, and a `Pattern` compares with a `Term<Any>` and answers `name` and `is_constant`, so code that reads the two sides of an equality between terms keeps compiling.
