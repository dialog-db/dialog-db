# A typed variable over a field of no declared type fails the query

**Phase: Building.** Reproduced on main, decided, and fixed in #782. One question about a variable that may be absent is left open below.

## Description

A concept field can declare no type, and then holds values of any. A premise can still type the variable it reads that field into, `?x.text()` say. When the field holds a value of another type for some entity, the paths a read can take met the same mismatch and did not do the same thing with it.

```
 a row whose value is not of the variable's type

   attribute scan                  AttributeQueryAll::admits      the row is passed over
   concept read, recursive         fixpoint::join                 the row is passed over
   concept read, a rule derives it merge_candidate → join         the row is passed over
   concept read, plain             merge_parameters  bind(…)?     the query failed
```

Run on main (`rust/dialog-query/src/typed_read_tests.rs`): a text, a natural, a negative integer and a boolean under `mixed/value`, read into a variable typed text, natural and boolean. The scan and the recursive read gave the values of the variable's type. The plain concept read gave

```
Fact store: Variable ?value of kind String cannot bind Boolean(true) (Boolean)
```

It was first seen by the agent building patterns (#778), whose first build narrowed a variable an equality sets equal to a map to `record:`.

## Problem

Which of a concept's rows a caller's terms match depends on which way the read is evaluated, and the caller cannot see which way that is. One value of an unexpected type under a field fails every plain query that types the variable it reads the field into.

## Approach

Decided by the maintainer, 2026-11-11: a variable of a type reads the values of that type, so only those are selected.

`Match::bind` already says so: a value a type does not read is a mismatch, "and callers that filter rows treat it as a non-match". `merge_parameters` was the one caller that did not filter. It now gives no row for a result the caller's terms do not match, by type or because one variable written twice would have to hold two values, and each of its three callers passes over such a row.

## Open: a variable that may be absent

A set-widened read binds its variable absent for an entity nothing matched. For an entity whose value is of another type than the variable reads, the paths differ, by reading the code:

| path | an entity with a value the variable does not read |
|---|---|
| plain concept read, as fixed here | no row: a result arrived, so the entity has a fact |
| concept read a rule derives (`relation`) | no row: survivors that do not merge are dropped |
| recursive concept read | a row with the variable absent: `matched` is set only when a row joins |

The scan's tests say what absent means: "no fact for this attribute", so a value that disagrees is no row (`it_does_not_emit_absent_on_optional_value_mismatch`). The fix follows that, and pins it in `it_gives_no_row_for_a_value_an_optional_variable_does_not_read`. The recursive path is left as it is and is not pinned.

Not decided: whether "the entity has no text" should read as absent instead, which is the other thing a typed optional variable could mean. Whichever it is, the recursive path and the others should agree.
