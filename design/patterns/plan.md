# The plan

For whoever builds it. In order; each step lands with its tests before the next starts.

## 1. The composite value (prerequisite, implemented)

The pattern work uses `Value::Composite` from [#843](https://github.com/dialog-db/dialog-db/pull/843), with Chronicle token sharing from [#849](https://github.com/dialog-db/dialog-db/pull/849). A composite is a canonical map or list; its sorted header table contains each scalar atom across all nested composites once. Chronicle interns those atoms with the tokens for standalone values and reconstructs the canonical table and body when it realizes a value. The composite's identity is computed from its parts, not from its bytes or token IDs. See [the composite design](../composite-value.md). Pattern code must not add a parallel `Value::Record` encoding.

## 2. A pattern

In `dialog-query`.

- `Pattern`, with `Term`'s two cases as its leaves and a map and a list above them. A map pattern may bind the entries it did not name, as a map. A list pattern may bind a run, as a list.
- `Parameters` maps a name to a pattern. Every premise that reads a parameter as a variable or a constant keeps working for the leaves.
- Matching a pattern against a `Value::Composite` binds its variables or fails. A pattern whose variables are all bound builds a `Value::Composite`.

Done when: a query takes a stored composite apart by a map pattern, with and without the rest; a rule builds a composite from two bound variables and concludes a fact holding it.

## 3. The planner

- A premise is feasible by the variables in its patterns. A parameter the premise binds may be a pattern: what it yields is matched, and a row that does not match is dropped.
- A parameter the premise requires may be a pattern whose variables are bound: it is built before the premise runs.

Done when: the two tests of step 2 pass in either order of premises, and a rule whose pattern can never be bound is refused when it compiles, naming the variable.

## 4. Inference

- A pattern's type follows from its shape and its leaves' types. A leaf keeps the descriptor a `Term` has.

Done when: a rule that binds a text from inside a map and adds a number to it is refused as one that adds a number to a text is today.

## What is not decided here

- **What installing a rule that holds a pattern does.** The stored rule format had no way to write one, and it was refused at commit, with that reason. Rule body format 5, built after this plan, stores it; what it still cannot write is refused as before (see [rule identity](design-rule-identity.md), "Format 5, as built").
- **The words of the wire form.** None is needed while only the new notation makes patterns, through Rust.

## What to leave alone

- The stored rule format, the perf and soak baselines, and the composite value implementation and layout in #843 and #849.
- `rust/dialog-notation`: it is on #773 and will take patterns up when they land.
