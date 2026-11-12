# The plan

For whoever builds it. In order; each step lands with its tests before the next starts.

## 1. A composite value

In `dialog-corpus`.

- A value made of values: a map (a value to a value, no key twice), a list, a tuple. Nested as deep as written.
- Kept as `Value::Record`: one version byte, then dag-cbor. A reader refuses a version it does not know, by its number. Bytes that do not start with a version byte are a record of before, such as a revision's, and read as they do today.
- Named by `merkle_reference`: a map under `Type::MAP`, a list under `Type::LIST`, each part by its own reference. `Value::reference` for a composite is that, not a hash of its bytes. A record of before keeps the name it has.
- One byte string a value: entries of a map in one order, whatever order they were given in.
- Equality, ordering and the key encoding (`model/ordvalue.rs`) go on treating a record as its bytes.

Done when: a composite round-trips through a branch as a claim's value; two maps given in two orders are one value, one name and one byte string; a revision record written before reads and verifies as before.

## 2. A pattern

In `dialog-query`.

- `Pattern`, with `Term`'s two cases as its leaves and a map, a list and a tuple above them. A map pattern may bind the entries it did not name, as a map. A list pattern may bind a run, as a list.
- `Parameters` maps a name to a pattern. Every premise that reads a parameter as a variable or a constant keeps working for the leaves.
- Matching a pattern against a value binds its variables or fails. A pattern whose variables are all bound is a value.

Done when: a query takes a stored record apart by a map pattern, with and without the rest; a rule builds a record from two bound variables and concludes a fact holding it.

## 3. The planner

- A premise is feasible by the variables in its patterns. A parameter the premise binds may be a pattern: what it yields is matched, and a row that does not match is dropped.
- A parameter the premise requires may be a pattern whose variables are bound: it is built before the premise runs.

Done when: the two tests of step 2 pass in either order of premises, and a rule whose pattern can never be bound is refused when it compiles, naming the variable.

## 4. Inference

- A pattern's type follows from its shape and its leaves' types. A leaf keeps the descriptor a `Term` has.

Done when: a rule that binds a text from inside a map and adds a number to it is refused as one that adds a number to a text is today.

## What is not decided here

- **What installing a rule that holds a pattern does.** Format 3 has no way to write one. Refuse it with that reason, and bring the question back; do not add a format.
- **The words of the wire form.** None is needed while only the new notation makes patterns, through Rust.

## What to leave alone

- The stored rule format, the perf and soak baselines, and the record values topic's files.
- `rust/dialog-notation`: it is on #773 and will take patterns up when they land.
