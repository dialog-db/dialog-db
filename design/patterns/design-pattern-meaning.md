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
- A constant inside a map or a list matches a part only where it is the part: a number by the number it is, whatever type it was given under (`Value::same_number`), since that is the value the pattern builds. A constant that is the whole pattern matches as a scan's constant does (`Value::reads_as`).
- A pattern that holds a blank cannot be built. Built from a rest that is not a map, or one that holds a key the pattern names, it is no value, and the row is dropped.
- Which keys of a map pattern are one is the map's to say: the natural `3`, the integer `3` and the float `3.0` are one key, and a pattern that names two of them is refused (`MapPatternError::DuplicateKey`, `it_refuses_two_keys_a_map_holds_as_one`).
- Opaque record bytes (`Value::Record`) are a value of no shape and match no map or list pattern; a variable takes them whole.

The cost of B is the second row. Whether a notation writes `{title: ?t}` for the open reading and lowers it to a blank rest is the notation's to decide; dialog's pattern says which it is.

## The edges, at head

Each of these is pinned by a test in `rust/dialog-query/src/pattern/tests.rs`, named in its paragraph.

**A key that holds several values.** A map holds a key once for each value it holds under it (`Composite::map`). A map pattern names a key for one value, so naming a key that holds several never matches, whether the value is taken apart or the pattern is built, and a rest takes such a key whole, every value under it, as a blank rest passes it by (`it_matches_no_key_that_holds_several_values_and_a_rest_takes_it_whole`). This is consistent but it is a gap, not a decision: the notation's own maps refuse a key given twice, so it cannot write a pattern for such a key, and a map that holds one can only be read through a rest. Whether a map pattern should be able to match a key of several values, by a set pattern or by matching each, is on the [journal](journal.md)'s agenda.

**A record.** A record is a map whose keys are attributes, held as symbols (`Value::Symbol`). A map pattern reads one through symbol keys alone: the attribute's name written as text is another key and does not match. A map pattern of symbol keys builds what `Composite::is_record` says is a record, and one of text keys a map (`it_reads_a_record_through_symbol_keys`).

**A variable named twice.** A variable is one variable wherever a pattern names it: `[?a ?a ?b]` matches `[1 1 2]`, binding `?a` once, and does not match `[1 2 2]` (`it_binds_a_variable_named_twice_once`).

**Empty.** `{}` matches the empty map alone and `[]` the empty list alone: neither matches the other kind, nor a composite that holds anything, and each builds its own kind (`it_matches_an_empty_composite_of_its_own_kind_alone`).

**No variable.** A map pattern with no variable is a constant: it is the value it builds, it matches that value alone and binds nothing (`it_is_a_constant_when_it_holds_no_variable`).

**Both ways.** A pattern used to build, its variables bound, and used to take apart, its value bound, round trips: what it builds it takes apart to the same bindings, and what it takes apart it builds again. That is a property test over generated patterns and values, with variables typed and constants of other number types (`it_builds_what_it_takes_apart_and_takes_apart_what_it_builds`, `rust/dialog-query/src/pattern/property_tests.rs`).
