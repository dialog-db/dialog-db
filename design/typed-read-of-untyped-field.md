# A typed variable over a field of no declared type fails the query

**Phase: Building.** Reproduced on main, decided, and fixed in #782.

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

Decided by the maintainer:

- 2026-11-11: a variable of a type reads the values of that type, so only those are selected.
- 2026-11-12: an attribute's type is part of which attribute it is. An attribute of the same name and another type is another attribute. So an entity that holds a number, read into a variable that is a text or nothing, is the same as an entity that holds nothing: one row, with the variable absent.

`Match::bind` already says the first: a value a type does not read is a mismatch, "and callers that filter rows treat it as a non-match". `merge_parameters` was the one caller that did not filter. It now gives no row for a result the caller's terms do not match, and each of its three callers passes over such a row.

The second is one check, made before anything is decided about an entity (`lens`, `Type::reads`): a value the caller's variable does not read is dropped as it arrives. It is then not elected among, and it does not make the entity one a set-widened read matched.

```
 an entity's values, read into ?x: text?

   "hello"          ──▶  ?x = "hello"
   7                ──▶  ?x absent        a number is a fact of another attribute
   "hello" and 7    ──▶  ?x = "hello"     under max: too, where 7 would win of all
   nothing          ──▶  ?x absent
```

What stays as it was: a value of the variable's type that disagrees with what the variable is already bound to is no row, and not absent, as a scan has it (`it_does_not_emit_absent_on_optional_value_mismatch`). The entity has a fact of the attribute; it is not the one asked for.

| path | a value the variable does not read | pinned by |
|---|---|---|
| attribute scan | passed over | `it_reads_the_values_of_its_type_through_an_untyped_field` |
| plain concept read | passed over; absent when it may be | the same, and `it_reads_a_value_of_another_type_as_absent_when_it_may_be` |
| plain concept read under an electing pick | not elected among; absent when it may be | `it_reads_a_value_of_another_type_as_absent_when_it_may_be` (`max:`) |
| concept read a rule derives | not elected among; absent when it may be | `it_elects_among_the_values_of_its_type_in_a_derived_field` |
| recursive concept read | passed over; absent when it may be | `it_reads_the_values_of_its_type_through_a_recursive_untyped_field`, and the fixpoint case of the absent test |

Not looked at: a recursive concept read whose optional variable an earlier premise bound to another value of its own type. By reading `fixpoint::join` and `widened`, it would try to bind the bound variable absent. No test here reaches it.
