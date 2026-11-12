# Design decision: naming a record that nests deep

**Problem it serves:** a composite is named by its parts, and a part that is a composite by its own parts, down to a depth (`NAMED_DEPTH`, 128). The test that says naming a deeper record "does not exhaust the stack" failed on wasm, in release, on 43d49a5 and on 7ee8e5e: `RuntimeError: memory access out of bounds`. It passed on every native target. A record is data from anywhere, so one that cannot be named is one a replica cannot hold.

**Decision:** how a record nested in records is named so that the claim holds on every target.

## Facts the matrix rests on

- The trap is inside the naming. The stack of the failing run is `Value::reference_within`, `Composite::reference_within`, `Type::list`, over and over (`Tests (test:web:release)`, job 131034817262).
- It is the stack, not memory. The same body in release on a native thread, by the smallest stack it runs in:

  | nesting | stack needed |
  |---|---|
  | 16 | 180 thousand bytes |
  | 64 | 686 thousand |
  | 128 and deeper | 1.36 million |

  That is 10.5 thousand bytes a level, and the depth stops it growing at 128. On a stack of 1 MiB a nesting of 96 is named and one of 112 overflows. A wasm module built for `wasm32-unknown-unknown` has a stack of 1 MiB, by how rustc links that target, and the stack is laid first in memory, so running past it is an access below the memory: the trap the run reports. Running out of memory aborts instead.
- The heap is not near anything. Naming the test's record, nested 512 deep in 7,150 bytes, holds 815 thousand bytes at its most and allocates 2.4 million in all.
- A native test passes because its thread has 2 MiB. It was 700 thousand bytes from failing too.
- Where the ten thousand bytes go: `Type::list` keeps a kilobyte of parts and a hasher on its stack and asks for each part while it holds them, and the part's name is the call that recurses. Naming the parts before hashing them, still by a call a level, needs 714 thousand bytes for 128 levels, 5.5 thousand a level: what is left is the reading of the record at each level.
- A level reads everything nested in it again, to parse it and to check it is written as a composite is written. So the work of naming grows with the bytes times the depth followed, and the depth is what bounds it.
- No release has written a composite. The names of deeply nested records are in no stored data yet.

| Decision: naming a record nested in records, for: a name on every target |
|---|---|---|---|
| criterion \ approach | A: no call a level. The composites a part is nested in wait in a list on the heap | B: a call a level, and a lesser `NAMED_DEPTH` | C: a call a level, the parts named before they are hashed |
| stack, by how deep a record nests | 🟢 the same at any nesting: the test's record is named on the smallest stack a thread can be given, and a nesting of 4,000 on the same | 🟡 10.5 thousand bytes a level here: 32 levels would be about 340 thousand. What a level costs is the compiler's and the target's to change | 🔴 714 thousand bytes at 128 levels here, under 1 MiB by a third, on one compiler and one target |
| the name of a record | 🟢 what it was: every nesting tried, 0 to 512, has the name the calls gave | 🔴 a record nested deeper than the new depth has another name: what was named by its parts below that depth is named by its bytes | 🟢 what it was |
| what a name depends on | the parts, down 128 levels | 🔴 the bytes sooner: a layout of a record's bytes would move the name of anything nested past the lesser depth, which is what naming by parts is for | the parts, down 128 levels |
| the claim on a target not measured | 🟢 holds by how it is built: nothing grows with the nesting but the heap | holds only while a level stays under the stack divided by the depth | 🔴 unknown |
| the work of naming a hostile record | bounded by the depth, as before | bounded sooner | bounded by the depth, as before |

## Decided

A, by the builder. It leaves every name as it is and makes the claim true by construction, where B buys the claim with the names of deep records and C only moves the edge.

- `Composite::reference_within` names a composite with a loop. The composite being named keeps the names of its parts so far. When a part is a composite to be followed, the one it is in is put on a list and the part is named first; when a composite's parts are all named it is named and its name goes to the one that waited (`Naming`, `rust/dialog-corpus/src/model/composite.rs`).
- `NAMED_DEPTH` stays 128, and now bounds the work and not the stack. Its comment says so, and says that changing it renames whatever is nested deeper than the lesser of the two depths.
- A scalar is named as it was, by the same code.

No name changed and no stored byte changed: `it_names_a_nested_record_by_its_parts_down_to_a_depth` pins the names the calls gave at nestings 1, 2, 127, 128, 129 and 512, and computes each from its parts by hand.

## How it is pinned

- `it_names_a_record_nested_past_any_depth` names a record nested 512 deep on the stack a test is given, on every target. On wasm that is the module's megabyte, which is the run that failed.
- `it_names_a_deep_record_on_a_small_stack` names it on a thread given 256 KiB, a fifth of what the calls needed. Native only, since a wasm test has no thread to give a stack to; the test above is its cover there.
- `it_names_composites_nested_in_composites_by_their_parts` names a tuple of a list of maps of tuples and compares each with its name computed from its parts.

## Not measured

Wasm was not run on this machine: the wasm target cannot be installed here. The cause was confirmed on a native thread in release, and by what the failing run's own stack and trap say. That the fix holds on wasm is for the web test jobs to say.
