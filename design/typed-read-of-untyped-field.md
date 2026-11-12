# A typed variable over a field of no declared type fails the query

**Phase: Describing.** Nobody has picked this up. What follows is the situation, not yet a problem statement.

## Description

A concept field can declare no type, and then holds values of any. A premise can still type the variable it reads that field into, `?x.text()` say. When the field holds a value of another type for some entity, three places meet the same mismatch and do not do the same thing with it.

```
 a row whose value is not of the variable's type

   concept read, non-recursive     merge_parameters   merged.bind(…)?          the query fails
   concept read, recursive         fixpoint::join     bind(…).is_err() → None  the row is dropped
   attribute read                  resolver::project  KindMismatch → None      the row is dropped
```

Read on main, in `rust/dialog-query/src`:

- `concept/query.rs`, `merge_parameters`: each value a concept's row holds is bound to the term the caller wrote with `merged.bind(user_param, value.clone())?`. A bind that fails is the read's error.
- `concept/query/fixpoint.rs`, `join`: the same bind, and a failure of any kind is "a non-match", in the words of its comment.
- `resolver.rs`, `project`: an `Assignment` or a `KindMismatch` filters the row, "the membership-test semantics shared with formulas"; any other failure is an error.

Seen by the agent building patterns (#778), on that branch, and not run again for this note: an untyped read of `mixed/value` over two entities, one holding a text and one the natural 7, with `?x.text()` beside it, planned and evaluated, and gave

```
Fact store: Variable ?x of kind String cannot bind UnsignedInt(7) (UnsignedInt)
```

It was found because the first build of patterns narrowed a variable an equality sets equal to a map to `record:`. That stamped the type on the concept premise binding it, and a test over a field of no declared type failed where it was to pass over a row. Patterns now check and do not narrow (`it_passes_over_a_value_of_no_shape`), so #778 does not depend on this.

Not known:

- Whether it reproduces on main as written above. Nothing in the code read says it would not, and no test on main pins either behaviour.
- Which of the two is meant. A caller that asks for texts and gets the texts, and a caller told its field holds something it did not expect, are both things someone could want.
- Whether anything relies on the error: a test, tonk, a rule that types a variable to find a bad write.
- Whether the same field read through a rule of the concept and read as a stored fact take the same path, or one fails and the other drops.
