# Design decision: what a premise of terms alone costs

**Problem it serves:** the four steps of [the plan](plan.md) made every query slower, a query that holds no pattern included. The Perf gate failed on seven scenarios at 43d49a5, by 7.6% to 12.4% of instructions, with every counter unchanged. The maintainer's requirement, 2026-11-11: a premise whose parameters are all leaves costs what it did before a parameter could hold a pattern.

**Decision:** where a concept read by itself keeps its patterns, how `Parameters` holds one, and how large a `Pattern` is.

## Facts the matrix rests on

- A concept read is one stream, made by `ConceptQuery::evaluate_erased`. For a query environment it is 16,896 bytes (`-Zprint-type-sizes` on `dialog-repository`), and it is moved once where it is made and once where `Application::perform` takes it. Under callgrind each move of it is about 16.4 thousand instructions: 3,284,800 and 3,388,800 over the 200 reads of `read-point`.
- At 43d49a5 `evaluate_application` returned one of two streams: the read, or a stream around the read that took each row apart. A value that is one of two types is as large as the larger and is moved once more. On `read-point` that was 3.49 million of the 4.20 million instructions added: `memcpy` went from 8.86 to 12.35 million.
- Every `?` in a `try_stream!` body is a place the stream can stop, since the error is yielded. A value alive across two such places is kept in the stream for its whole life. Two values alive at once, each across one such place, cost one of them the same. So a second `?` in a loop that had one made the stream 96 bytes larger, twice, with no field added (the layouts are in the [journal](journal.md)).
- `number-point` allocates 905,482 times, the base and every fix alike. With the shapes of `Parameters` kept beside the terms in the same allocation, 833 of those allocations are 8 bytes larger (40 bytes became 48) and no other size moves. With one map whose slot is a term or a boxed shape, every size is the base's.
- One binary's instruction count moves between two runs of it: over five runs of the base, `read-point` was 24.59 to 24.93 million and `subscribe-repoll` 13.513 to 13.547 million. The calls to the allocator are the same in number every run, and what they cost inside is not: in `number-point` the allocator's own instructions were 7.30 to 7.46 million over five runs of the base. The instructions outside the allocator and outside `memcpy` are steady, 18.324 to 18.332 million there, so that is where a change is read.

| Decision: where a concept read by itself keeps its patterns, for: a read of terms alone moved as what it was |
|---|---|---|---|
| criterion \ approach | A: a stream around the read, for a query that holds a pattern (43d49a5) | B: a field in the read's stream, and each row taken apart where it is yielded | C: the application kept as an option, and each row taken apart in the place it arrived in |
| the stream of a read of terms alone | 🔴 one of two types, as large as the larger, moved once more | 🔴 about 300 bytes larger: the field, and what is held across two more places a row is yielded from | 🟢 16,896 bytes, the layout the base has, field for field |
| instructions on `read-point`, against the base | 🔴 +16.3% here, +12.4% in the gate | 🟡 +1.6% and +2.0% in two runs, `memcpy` +128 thousand | 🟢 within what the base moves between two runs of itself |
| a read that holds a pattern | rows leave as they are found | rows leave as they are found | 🟢 rows leave as they are found; the application is cloned once for the read |
| what the reader of the code has to know | nothing | nothing | 🟡 why the loop writes a row back into the place it came in: a second place costs every read |

| Decision: how `Parameters` holds a map or a list, for: parameters of terms alone being what they were |
|---|---|---|---|
| criterion \ approach | D: each entry a term and an optional shape (43d49a5) | E: the map of terms it was, and the shapes in a list beside it | F: one map whose slot is a term or a boxed shape, the size of a term |
| entries and nodes of the map | 🔴 each entry 8 bytes larger, so every node is | 🟢 what they were | 🟢 what they were: the slot is the size of a term |
| the allocation behind `Parameters` | what it was | 🟡 8 bytes larger: 833 allocations of 40 bytes became 48 in `number-point` | 🟢 what it was: the same sizes, the same number of each |
| asking whether any parameter holds a shape | 🔴 a walk of the map | 🟢 one test | 🔴 a walk of the map, about 90 to 110 instructions, once a concept read and once more wherever a query is printed |
| reading a term | what it was | 🟢 what it was | 🟡 one test of which the slot is, at every read |
| instructions outside the allocator and `memcpy`, over the base, five runs each | not measured apart | 🟢 `number-point` +34 thousand, `rule-point` +57 thousand | 🟡 `number-point` +36 thousand, `rule-point` +122 thousand |

| Decision: how large a `Pattern` is, for: an equality between two terms being what it was |
|---|---|---|
| criterion \ approach | G: a map pattern holds its rest, and a list its run, in line (43d49a5) | H: the rest and the run are boxed |
| a pattern against a term | 🔴 larger: a list of entries and an optional term, where a term is 96 bytes | 🟢 the size of a term, asserted when it compiles |
| an `Equality`, and every premise, plan step and stream that holds one | 🔴 larger by twice the difference | 🟢 192 bytes, two terms |
| a pattern with a rest | no allocation for it | one allocation for it, when the pattern is written |

## Decided

C, E and H, by the builder, under the maintainer's requirement. F was built and measured too: it allocates exactly what the base does and runs more instructions, twice as many in a rule's read, and the gate counts instructions.

- `ConceptQuery::evaluate_erased` is the one stream of a concept read, for a query that holds a pattern too, and `evaluate_application` returns it for both, with no box around it. The application is held in it as `Option<ConceptQuery>`, which is the size of the application. A read of terms alone takes it out and gives it to its evaluation, as the base did. A read that holds a pattern leaves it in, reads through a copy of it, whose terms hold the wholes where the patterns stand, and takes each row apart by the terms it kept (`pattern::take_apart`), a picked read's rows too. The row is written back into the place it arrived in, so the loop has the places to stop it had.
- `ConceptQuery::canonical` renames the operands of a one-field read. Where its terms hold a pattern it renames with `Parameters::rekeyed`, which keeps the pattern under the new name; terms alone are moved one by one, as the base moves them.
- `Parameters` is the map of terms it was, and beside it in the same allocation one place for the patterns, which is empty unless a name holds one. `get`, `iter` and `insert` read and write the map of terms as before, the whole standing where a name holds a pattern.
- A `MapPattern`'s rest and a `ListPattern`'s run are boxed, and `Pattern` is the size of `Term<Any>`.
- An `Equality` between two terms runs the body it ran, in line, and `Pattern`'s questions (`is_bound`, `bind`, `is_constant`, `has_blank`) answer a leaf without collecting its terms.

## What is left

Measured on the fix, against the base, five runs of each. The instructions outside the allocator and `memcpy`:

| scenario | base | fix | more |
|---|---|---|---|
| `number-point` | 18.324 to 18.332 million | 18.360 to 18.365 million | 34 thousand, 0.10% of the scenario |
| `read-point` | 10.053 to 10.057 million | 10.090 to 10.100 million | 38 thousand, 0.15% |
| `read-sibling` | 15.504 to 15.511 million | 15.540 to 15.548 million | 36 thousand, 0.11% |
| `rule-point` | 24.289 to 24.303 million | 24.344 to 24.363 million | 57 thousand, 0.10% |
| `subscribe-repoll` | 6.838 to 6.842 million | 6.855 to 6.857 million | 16 thousand, 0.12% |

Each of these scenarios reads 200 times, so that is 80 to 290 instructions a read. By function, in `number-point`:

- `Parameters::insert` tests whether the name held a pattern, and the map's own insert is now called where it was in line: 6 instructions an insert, 9.5 thousand.
- The read's stream takes the application out of its option and tests it once a row: 35 instructions a read, 7 thousand.
- `evaluate_application` asks whether the terms hold a pattern: 26 instructions a read, 5 thousand.
- A query layer has one more field, for the rules it holds as they are ([design-rule-identity.md](design-rule-identity.md)): 28 instructions a query to drop it, and it is 8 bytes more to move.
- `Box::pin`, `canonical` and the planner's reading of a slot: 13, 12 and 5 instructions a read.

And one allocation in a thousand is 8 bytes larger.

The total of a scenario moves more than this between two runs of one binary, because of the allocator: the medians of five runs of the fix are within 1% of the base's on every scenario, above on some and below on others. So the gate does not see what is left. It is named here so that nobody has to find it again, and so the maintainer can say whether a test of "does this hold a pattern" in these places is a cost.

## Measured again, 2026-11-23

f86d6983b, porting the branch to the composite API, put a box around every concept read and a second stream around a read that holds a pattern, approach A above, and dropped the branch of `ConceptQuery::canonical` that renames a one-field read's patterns. The one stream of C is restored, and `canonical` renames with `Parameters::rekeyed` only where the terms hold a pattern.

The comment on `evaluate_application` about the recursive type still applies: it is why the caller's stream is erased (`Box::pin(selection)`) before the read, which main does too. It is not why the read itself was boxed; that box only gave the two ways one type, which `evaluate_erased` now gives by taking `apart`.

The Perf gate's read and rule scenarios, under callgrind, the toolchain the flake pins (1.99.0), each side built from its own worktree:

```
cargo +1.99.0 run -p dialog-perf --release -- sweep --out-dir <dir> --only read-point,read-scan,read-sibling,rule-dispatch,rule-install,rule-overlay,rule-point,rule-ranked,rule-recursive,rule-scan,rule-session,concept-optional,query-join,query-member,first-query
```

Main is `dcb72a5`; head before is `876288c`, the branch with main merged; head after is `a459c30`. Instructions outside the allocator, and the change against main:

| scenario | main | head before | head after | before | after | allocation calls, main / before / after |
|---|---:|---:|---:|---:|---:|---|
| `concept-optional` | 133,459,513 | 133,545,168 | 133,617,868 | +0.06% | +0.12% | 82,633 / 82,632 / 82,633 |
| `first-query` | 1,360,987 | 1,361,375 | 1,362,353 | +0.03% | +0.10% | 978 / 978 / 978 |
| `query-join` | 49,508,215 | 49,554,648 | 49,547,905 | +0.09% | +0.08% | 49,292 / 49,291 / 49,291 |
| `query-member` | 86,152,857 | 86,148,421 | 85,947,537 | -0.01% | -0.24% | 82,495 / 82,495 / 82,495 |
| `read-point` | 17,066,679 | 17,075,532 | 17,084,119 | +0.05% | +0.10% | 22,919 / 22,919 / 22,919 |
| `read-scan` | 20,066,479 | 20,053,157 | 20,005,882 | -0.07% | -0.30% | 18,781 / 18,781 / 18,781 |
| `read-sibling` | 22,505,136 | 22,501,563 | 22,511,643 | -0.02% | +0.03% | 30,689 / 30,689 / 30,689 |
| `rule-dispatch` | 483,464,020 | 483,592,529 | 482,399,898 | +0.03% | -0.22% | 449,130 / 449,131 / 449,133 |
| `rule-install` | 119,245,132 | 119,270,240 | 119,292,819 | +0.02% | +0.04% | 160,768 / 160,766 / 160,766 |
| `rule-overlay` | 183,342,997 | 183,366,261 | 183,063,860 | +0.01% | -0.15% | 326,607 / 326,608 / 326,608 |
| `rule-point` | 38,963,081 | 38,962,269 | 38,987,315 | -0.00% | +0.06% | 53,892 / 53,893 / 53,894 |
| `rule-ranked` | 37,234,622 | 37,249,076 | 37,051,826 | +0.04% | -0.49% | 45,775 / 45,775 / 45,783 |
| `rule-recursive` | 588,007,220 | 588,096,556 | 585,419,599 | +0.02% | -0.44% | 845,161 / 845,161 / 845,159 |
| `rule-scan` | 58,108,868 | 58,117,942 | 58,151,492 | +0.02% | +0.07% | 61,652 / 61,652 / 61,651 |
| `rule-session` | 49,441,158 | 49,440,057 | 49,427,062 | -0.00% | -0.03% | 71,184 / 71,184 / 71,183 |

Neither head moves a scenario past what one binary moves between two runs of itself (the fifth fact above): the box f86d6983b added cost at most 0.09% here, and the head after is within -0.49% to +0.12% of main, with the calls to the allocator the same within eight. So the restoration is for the decision C records, not a saving the gate sees. The full gate at `a459c30` (`dialog-perf compare perf/baseline`, all 31 scenarios) reports no regression, every counter unchanged.

Main moved while this was measured: #806 and #784 boxed the inner evaluation of a concept read and the paths a plain read does not run, and cut `read-point` by about 30%. Merged with them (`af4f0ef`), against main at `9b43ebc`, with the same command:

| scenario | main | this branch | change | allocation calls, main / branch |
|---|---:|---:|---:|---|
| `concept-optional` | 133,413,646 | 133,423,254 | +0.01% | 82,465 / 82,465 |
| `first-query` | 1,331,776 | 1,333,248 | +0.11% | 960 / 960 |
| `query-join` | 49,544,289 | 49,569,510 | +0.05% | 49,263 / 49,263 |
| `query-member` | 85,979,810 | 85,854,844 | -0.15% | 82,463 / 82,464 |
| `read-point` | 11,865,501 | 11,854,075 | -0.10% | 21,508 / 21,508 |
| `read-scan` | 19,965,622 | 19,998,443 | +0.16% | 18,763 / 18,763 |
| `read-sibling` | 17,300,433 | 17,297,342 | -0.02% | 29,676 / 29,676 |
| `rule-dispatch` | 443,953,961 | 442,382,009 | -0.35% | 437,053 / 437,054 |
| `rule-install` | 118,327,741 | 118,343,907 | +0.01% | 158,083 / 158,082 |
| `rule-overlay` | 171,124,743 | 170,852,359 | -0.16% | 317,498 / 317,499 |
| `rule-point` | 28,338,732 | 28,316,695 | -0.08% | 50,402 / 50,404 |
| `rule-ranked` | 37,014,867 | 36,827,381 | -0.51% | 45,522 / 45,530 |
| `rule-recursive` | 434,326,647 | 431,188,641 | -0.72% | 814,998 / 814,996 |
| `rule-scan` | 57,861,072 | 57,949,689 | +0.15% | 61,345 / 61,346 |
| `rule-session` | 38,821,340 | 38,789,185 | -0.08% | 67,893 / 67,893 |

Within what one binary moves between two runs, as above. The full gate at `af4f0ef` against `perf/baseline` reports no regression, every counter unchanged, and main's `it_keeps_a_read_small_enough_to_move` holds the read's stream under its 2,048 bytes with the patterns' option in it.

## Backtracked

[design-parameters.md](design-parameters.md) decided that a concept takes its own shapes apart around `evaluate_application`, and that `Parameters` holds a term and, for a shape, the pattern beside it. Both were built as the first thing that worked, a stream around the stream and a second field in every entry, and both were what the gate measured. What they decided stands: a reader gets a term, and a concept run by itself takes its own patterns apart. How it is held changed.
