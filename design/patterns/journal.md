# Journal

|  | Understanding | Activity |
|---|---|---|
| **Status** | A parameter holds a variable, constant, map pattern or list pattern. Patterns build and take apart `Value::Composite`; nested atoms use the shared canonical table and Chronicle tokens from #849. | **Implementation.** Pattern planning, typing and rule handling are implemented in #778 and are being ported to the current composite API. |
| **Agenda** | Verify pattern query and rule behavior against the current branch stack. | Run the pattern tests, then the full test and performance gates. |

## 2026-11-11, Directing: a pattern in place of a term

**Asked.** In the [macros](../macros.md) topic: how dialog evaluates a struct with a variable in it, what a record is, and how to stop waiting on the layout of its bytes.

**Learned.**

- A struct with a variable in it is a destructuring pattern. The same pattern, with its variables bound, is the value. Macros, an entity derived from a row, and a command's row all need it.
- The layout of a record's bytes has been the thing everything waited on. A name that is a merkle reference of the parts does not depend on it.
- A revision's record is a bare dag-cbor map (`RevisionRecord::to_bytes`), so a version byte in front of a new record tells the two apart.

**Decided.** By the maintainer, Irakli:

- `Term` becomes the leaf of a `Pattern`, which is also a map or list of patterns.
- Patterns use `Value::Composite`, whose canonical map/list encoding and shared tokens are implemented in #843 and #849.
- The planner and inference are part of it. A stored format is not: only the new notation makes patterns.
- It is a piece of work of its own, for an agent of its own.

## 2026-11-21, Updating: patterns use the canonical composite value

**Learned.** The composite prerequisite is now implemented in #843 and #849. A `Value::Composite` map or list has canonical bytes with a shared table of scalar atoms across all nested composites. Chronicle interns those atoms through the token dictionary it also uses for standalone values, then rebuilds canonical bytes when it realizes the composite. Its identity comes from its parts and does not depend on its layout or token IDs.

**Decided.** The pattern design and implementation use this composite representation directly. They do not introduce a second record encoding, version byte, or per-pattern token table. The revision record remains a separate bare dag-cbor map.

**Implementation note, 2026-11-21.** The pattern matcher now constructs and inspects `Composite` values directly. A pattern does not encode nested composites itself: it delegates canonical ordering, shared scalar atoms and composite identity to the corpus implementation.
