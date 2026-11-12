# Journal

|  | Understanding | Activity |
|---|---|---|
| **Status** | Dialog has a value made of values, kept as a record under a version byte and named by its parts. A parameter holds a pattern: a term, or a map, a list or a tuple of terms. A concept premise that holds one is planned as the concept over the whole value and an equality between the whole and the pattern, and the equality runs both ways ([design-parameters.md](design-parameters.md)). A rule that holds one has an identity, runs, and cannot be stored ([design-rule-identity.md](design-rule-identity.md)). | **Building.** Steps 1 and 2 of [the plan](plan.md) are built and tested. Step 2 took the planner's reading of a pattern with it. Steps 3 and 4 are not landed. |
| **Agenda** | For the maintainer: what installing a rule that holds a pattern should do; today it is refused by a panic that says why, since an assertion returns nothing. Whether a pattern with no rest is the map of exactly its entries, as built. Whether a composite keeps a number under the narrowest type that holds it, as built. Whether a tuple's name is its own type, `tuple:`, as built. Whether any embedder has stored a record that starts with `0x01` to `0x17`. Then: how a pattern is typed. | Step 3: the tests in either order of premises and the refusal that names the variable. Then step 4. |

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

## 2026-11-11, Building: a composite value

**Asked.** Which byte says a record is versioned, and what a reader makes of a first byte. How the parts are written under it. What makes two keys of a map one key.

**Learned.**

- The one record dialog writes starts with `0xa7`. Every other writer of `Value::Record` carries bytes a caller gave, and in this repository only tests give any. So the hypothesis that a version byte tells the two apart holds for what dialog writes, and is open for what an embedder stored.
- A small integer in front makes two CBOR items in a row, which no dag-cbor document is. That is a stronger reason for `0x01` than "a map starts with `0xa0` to `0xbf`".
- `Value`'s own serde form cannot hold every value in dag-cbor: a natural above 2^64-1 and an infinite float are refused, and bytes are written as an array of integers. The history index already writes a claim's value as the byte of its type and its bytes, which holds every value.
- A fresh tree keys numbers in one band and reads a number back under the narrowest type that holds it. A composite that kept the type a number was given in would have one name and two byte strings, and a record built from a bound number would miss a stored one.
- The direction named `Type::MAP` and `Type::LIST`. A tuple needs a name too, and a list's would make one name of two values.
- A rule's identity hashes a record constant by its bytes, and says in its doc that it agrees with `Value::reference`. Step 2 has to keep that true.

**Decided.** By the builder, each in its file:

- Version 1 is the byte `0x01`, and `0x02` to `0x17` are later versions. A reader refuses one it does not know by its number when it looks in, and carries the record as bytes otherwise ([design-version-byte.md](design-version-byte.md)).
- A composite is `{"map:": …}`, `{"list:": …}` or `{"tuple:": …}`, a part is `[type, bytes]`, a nested composite is a record, a number is kept under the narrowest type that holds it, and a map's entries are in the order of their keys as written ([design-composite-bytes.md](design-composite-bytes.md)).
- A tuple is named under `tuple:`.

Two of these rest on the builder's opinion and are on the agenda: the number's type, and the tuple's name.

## 2026-11-11, Building: a pattern in a parameter

**Asked.** What `Parameters::get` answers for a name that holds a map. Where a premise that reads terms meets a pattern. What a pattern with nothing left over means. Whether a rule that holds a pattern can run without a stored form.

**Learned.**

- `Parameters` is read as terms at about four hundred places, and only a concept premise stores one. So a reader gets a term whatever the parameter holds, and the few that must look inside ask for the shape.
- A rule's body is evaluated by more than its plan: a recursive component joins its occurrences from tables, and induction reads a premise's terms. A pattern applied by each plan step would be dropped by those, with no error. Lowered to an equality, they meet a premise they already run.
- The planner, not the author, picks which premise runs first. So a pattern has to say one thing built and taken apart, and `{title: ?t}` is the map of a title alone.
- A rule runs only when registered, and is registered only with an identity, which is a reference of its wire form. So step 2's rule could not be tested until a pattern had a reference. It has one by its parts, as the value it builds has, and still no wire form.
- A rule that holds a pattern does not compile until feasibility looks inside the pattern. The plan's step 3 starts there, so step 2 took that much of it.
- Asserting a rule returns nothing, so a refusal to install can only be a panic or a check the caller runs first. Both are there.

**Decided.** By the builder, each in its file:

- `Parameters` holds a term and, for a shape, the pattern beside it; a concept premise is lowered to the concept over a whole and an equality; `Equality`'s sides are patterns ([design-parameters.md](design-parameters.md)).
- A pattern with no rest is the map of exactly its entries, a list pattern with no run the list of exactly its items ([design-pattern-meaning.md](design-pattern-meaning.md)).
- A pattern is named by its parts and has no wire form. A rule that holds one has an identity and no stored body, and installing it is refused with that reason ([design-rule-identity.md](design-rule-identity.md)).

The first rests on facts in the code. The second and the manner of the third's refusal are the builder's opinion, and on the agenda.
