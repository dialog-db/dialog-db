# A concept read is moved as 17 KB

**Phase: Describing.** Nobody has picked this up. What follows is the situation, not yet a problem statement.

## Description

A concept read is one stream, made by `ConceptQuery::evaluate_erased` (`rust/dialog-query/src/concept/query.rs`). It is returned as `impl Selection`, a value and not a pointer, and it is moved where it is made and again where `Application::perform` takes it.

Measured while the Perf gate's failure on patterns (#778) was traced, by the agent building it, and not run again for this note:

- The stream is 16,896 bytes for a query environment (`-Zprint-type-sizes` on `dialog-repository`), on main as on #778.
- Under callgrind each move of it is about 16.4 thousand instructions. Over the 200 reads of the `read-point` scenario that is 3.28 and 3.39 million instructions, of about 21.8 million the scenario counts: close to a third of a point read is moving the stream that does the reading.
- #778 first wrapped the stream in a second one. That moved it once more and cost 3.5 million instructions on `read-point`, which is how this was seen.

Why it is that large, from reading the code and not from a measurement:

```
 evaluate_erased        try_stream!   holds, in line, whichever of these it is waiting on
   ├─ the rules of the concept                Provider::<SelectRules>::execute(…).await
   ├─ evaluate_unsorted try_stream!
   │    ├─ a recursive concept's fixpoint     fixpoint::evaluate(…).await, continuation.rows(…).await
   │    ├─ each reducing rule's rows          reduce_rows(rule, env).await
   │    └─ the planned rules, joined          stream_select!(…)
   └─ the buffer a sorted read fills
```

A `try_stream!` body and an `async fn` are each one value that has room for every local alive across a place it can stop, and for the largest thing it waits on, laid out inside it unless that thing is boxed. So the read is as large as its deepest path, whichever path a given read takes. A point read of a stored attribute takes none of the fixpoint or the reduce, and carries the room for them.

Asked of the linter, `clippy::large_futures` with a threshold of 2 KB over `dialog-query` and `dialog-corpus` (the command and the table are in #781):

- It sees a future where it is awaited, and a stream is polled. In the read it finds one future above 2 KB, of 2,104 bytes. So most of the 16,896 is in what the stream holds across its stops or in the streams inside it.
- The commit path has futures of 11 to 12 KB of its own, in `dialog-corpus/src/buffered.rs`: `seal`, `settle`, `insert_deferred`, `delete_deferred`.

The brief for whoever picks this up is #781.

Not known:

- Which of the paths is the 17 KB. `-Zprint-type-sizes` says, field by field.
- What the stream costs to make and to poll apart from being moved.
- Whether the same holds for an attribute read and a formula, which are streams of their own.
- Whether the commit path's futures are moved often enough to cost what a scenario counts.
