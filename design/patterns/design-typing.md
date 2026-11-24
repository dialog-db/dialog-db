# Design decision: how a pattern is typed

**Problem it serves:** a rule cannot say a value by its shape (the topic's [problem](../patterns.md)). Inference types each variable by the slots it stands in (`TypeEnv::infer`, `rust/dialog-query/src/rule/types.rs`). A slot that holds a pattern holds no one variable.

**Decision:** what a pattern contributes to inference.

## Facts the matrix rests on

- Inference unifies each named variable with the kind of every slot it stands in. A slot's kind comes from the premise's schema; a slot with none admits any present value.
- A kind is a set of primitive types, with or without `Nothing` (`rust/dialog-query/src/type_system.rs`). There is no kind of a map of text to a number: a composite map is one primitive and a composite list another (#843).
- A term carries a descriptor: `Term::<String>::var("t")` is text, `Term::<Any>::var("t")` says nothing.
- A value of a shape is a `Value::Composite`: a map pattern's a composite map, a list pattern's a composite list. A rest is a map and a run is a list.
- A variable inference narrows to a kind is stamped with it wherever a concept premise binds it (`apply_types`, `rust/dialog-query/src/planner/plan.rs`). A concept field that declares no type and yields a value of another kind then fails the query: `merge_parameters` returns the `KindMismatch` as `Fact store: Variable ?x of kind String cannot bind UnsignedInt(7)`. Seen with an untyped read of a field holding a text and a number, and `?x.text()` beside it. An attribute scan filters such a row; a concept premise does not.

| Decision: what a pattern contributes, for: a pattern's type follows from its shape and its leaves' types |
|---|---|---|---|
| criterion \ approach | A: nothing. The whole is a variable of the slot's kind | B: the shape makes the slot a composite of its kind, and each leaf contributes its descriptor | C: a kind for each shape: a map of its entries' kinds, a list of its items' |
| a text bound inside a map and added to a number is refused | 🔴 the leaf is never seen | 🟢 the leaf is text and the sum wants a number: a conflict, naming the variable | 🟢 |
| a map where a field reads text is refused | 🔴 | 🟢 the field's kind and a composite map have no meet | 🟢 |
| a map of the wrong keys where a record is read | not seen | not seen: every map pattern is a composite map | 🟢 seen |
| what it adds to the type system | nothing | nothing: the composite kinds and the descriptors exist | 🔴 kinds that hold kinds, through the unifier, the wire form of a type and every place a kind is a bit set |

## Decided

B.

- A slot that holds a shape has to read a composite of its kind: a map pattern a map, a list pattern a list (`TypeEnv::infer_shape`). A concept field that reads only text refuses a map standing in it.
- A leaf keeps the descriptor a term has: a variable written with a type contributes it wherever else the rule mentions the variable, and one written with none contributes any present value.
- A variable an equality sets equal to a shape, a rest and a run each hold a composite map or list. That is checked once every premise has contributed, and not asserted: a rule in which another premise makes such a variable text or a number is refused, naming it (`TypeEnv::check_records`), and the variable is not narrowed to a composite kind. Narrowed, it would be stamped on the concept premise that reads it, and by the last fact above a field that declares no type would fail the query on its first value that is not a composite, where a pattern is to drop the row (`it_passes_over_a_value_of_no_shape`).

`it_refuses_adding_a_number_to_a_text_bound_inside_a_map` pins the first row beside the rule it is refused like, and `it_types_a_pattern_by_its_shape` the others.

C is what a declared composite shape may want, once a record's shape can be declared. Nothing here stands in its way: B types the leaves a pattern names, and a declared shape would type the ones it does not.

The last fact is not about patterns. A variable a premise types, read from a concept field that declares no type, fails the query where a scan would drop the row. It is on the [journal](journal.md)'s agenda as found, and is not changed here.
