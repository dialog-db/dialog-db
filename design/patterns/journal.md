# Journal

|  | Understanding | Activity |
|---|---|---|
| **Status** | A parameter holds a variable or a constant, and dialog has no value made of values. The direction is decided: a pattern in place of a term, over a composite value kept as a versioned record. | **Designing.** Direction decided by the maintainer. Nothing built. |
| **Agenda** | How the planner judges a pattern feasible. How a pattern is typed. What installing a rule that holds one does. | Build in the order of [the plan](plan.md), one implementation decision at a time, each written down. |

## 2026-11-11, Directing: a pattern in place of a term

**Asked.** In the [macros](../macros.md) topic: how dialog evaluates a struct with a variable in it, what a record is, and how to stop waiting on the layout of its bytes.

**Learned.**

- A struct with a variable in it is a destructuring pattern. The same pattern, with its variables bound, is the value. Macros, an entity derived from a row, and a command's row all need it.
- The layout of a record's bytes has been the thing everything waited on. A name that is a merkle reference of the parts does not depend on it.
- A revision's record is a bare dag-cbor map (`RevisionRecord::to_bytes`), so a version byte in front of a new record tells the two apart.

**Decided.** By the maintainer, Irakli:

- `Term` becomes the leaf of a `Pattern`, which is also a map, a list or a tuple of patterns.
- The value first: a composite kept as `Value::Record`, dag-cbor under a version byte now, another layout later.
- The planner and inference are part of it. A stored format is not: only the new notation makes patterns.
- It is a piece of work of its own, for an agent of its own.
