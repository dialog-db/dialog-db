# Design decision: what a pattern with nothing left over means

**Problem it serves:** a rule cannot say a value by its shape (the topic's [problem](../patterns.md)). The direction says one pattern does both: where the value is bound it takes it apart, and where its variables are bound it is the value. It also says a map pattern may bind the entries it did not name, and a list pattern a run.

**Decision:** what `{title: ?t}` matches when the map holds more than a title, and what `[?a, ?b]` matches when the list holds more than two.

## Facts the matrix rests on

- The planner picks the cheapest feasible premise at each round (`Planner::plan`, `rust/dialog-query/src/planner.rs`). The author does not choose which premise runs first, so does not choose whether a pattern is built or taken apart.
- A pattern whose variables are bound builds one value: for `{title: ?t}`, the map with a title and nothing else.
- A blank is never bound (`Term::is_bound`).

```
 {title: ?t}              the map of exactly a title
 {title: ?t, ...?rest}    a map with a title; the others are ?rest, a map
 {title: ?t, ..._}        a map with a title; the others are not looked at

 built:        ?t = "Buy milk"            ->  {title: "Buy milk"}
 taken apart:  {title: "Buy milk"}        ->  ?t = "Buy milk"
               {title: "Buy milk", done}  ->  no match, unless a rest takes `done`
```

| Decision: a pattern with no rest, for: one pattern, both ways |
|---|---|---|
| criterion \ approach | A: open. It matches any map that holds its entries, as destructuring does in most languages | B: closed. It is the map of exactly its entries; a rest, named or blank, takes the others |
| a rule means one thing whichever premise runs first | 🔴 built, the pattern is the map of a title alone; taken apart, it is any map with a title. A rule that reads a stored record by it finds one set of facts when the record is scanned and another when it is looked up by the built value | 🟢 the pattern is one relation between its variables and a value, whichever is bound first |
| reading a field out of a larger record | 🟢 `{title: ?t}` | `{title: ?t, ..._}`: four more characters |
| building a record with the rest carried along | not sayable without a rest either | `{title: ?t, ...?rest}`, and the rest must not hold a title |

## Decided

B, by the builder. Step 3 asks that the tests pass in either order of premises, and under A they cannot in general.

- A map pattern with no rest matches a map of exactly its entries. With a rest it matches a map that holds them and binds the others to the rest, as a map. A blank rest takes the others and binds nothing.
- A list pattern with no run matches a list of exactly its items. With a run it matches a list that starts with them and binds the ones after to the run, as a list. A run stands after the items and nowhere else.
- A tuple pattern matches a tuple of exactly its items. A list is not a tuple.
- A constant inside a pattern matches the value it reads as, as a scan matches a constant (`Value::reads_as`).
- A pattern that holds a blank cannot be built. Built from a rest that is not a map, or one that holds a key the pattern names, it is no value, and the row is dropped.
- Matched against a record under a version this release does not read, a pattern is refused by the number (`EvaluationError::RecordVersion`), since the record may match and this release cannot tell. A record of before is a value of no shape and does not match.

The cost of B is the second row. Whether a notation writes `{title: ?t}` for the open reading and lowers it to a blank rest is the notation's to decide; dialog's pattern says which it is.
