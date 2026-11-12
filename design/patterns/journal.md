# Journal

|  | Understanding | Activity |
|---|---|---|
| **Status** | Dialog has a value made of values, kept as a record under a version byte and named by its parts. A parameter holds a pattern: a term, or a map, a list or a tuple of terms. A concept premise that holds one is planned as the concept over the whole value and an equality between the whole and the pattern, and the equality runs both ways ([design-parameters.md](design-parameters.md)). A rule that holds one has an identity, runs, is laid over a branch for a query, and cannot be stored ([design-rule-identity.md](design-rule-identity.md)). A pattern is typed by its leaves, and has to stand where a record can ([design-typing.md](design-typing.md)). A premise of terms alone is moved and allocated as it was, and runs 80 to 290 instructions more a read ([design-leaf-cost.md](design-leaf-cost.md)). | **Building.** The six steps of [the plan](plan.md) are built and tested, in pull request #778. |
| **Agenda** | For the maintainer: what installing a rule that holds a pattern should do; today it is refused by a panic that says why, since an assertion returns nothing, and a stored form for it is the rule format topic's (#779). Whether the instructions left on a read of terms alone, each a test of whether a pattern is held, are a cost. Whether a pattern with no rest is the map of exactly its entries, as built. Whether a composite keeps a number under the narrowest type that holds it, as built. Whether a tuple's name is its own type, `tuple:`, as built. Whether any embedder has stored a record that starts with `0x01` to `0x17`. Found on the way, not about patterns: a variable a premise types, read from a concept field that declares no type, fails the query on a value of another type, where a scan drops the row; and one binary's instruction count moves by about 1% between two runs on the read scenarios, all of it inside the allocator, so the Perf gate cannot see less. | The maintainer's answers to the agenda. Then what the notation (#773) needs of patterns that it does not have. |

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

## 2026-11-11, Building: the planner

**Asked.** When a slot that holds a pattern counts as bound. What a refusal names.

**Learned.**

- With the pattern lowered to an equality, the planner needs one new rule and no new step: a slot that holds a shape is bound when every variable inside it is. The equality costs 1, so it is taken as soon as it is feasible, which builds a record before the premise read by it and takes a row apart right after the premise that yielded it.
- The same premise plans both ways. With nothing bound the concept runs first and the equality takes apart. With the pattern's variables bound the equality runs first and the concept is read by the built value.
- A negated premise is not lowered, so the anti-join that reads the negated relation in bulk has to key on the variables inside the pattern. It keyed on the terms and would have missed them.

**Decided.** By the builder: a shape is bound when all its variables are, binds or requires each that is not, and a blank inside it is neither ([design-feasibility.md](design-feasibility.md)). The hypothesis that a premise can be planned by the variables inside its patterns is confirmed.

## 2026-11-11, Building: inference

**Asked.** What a pattern contributes to inference.

**Learned.**

- A kind is a set of primitive types. There is no kind of a map of text to a number, so a shape's own type is `record:` and what a pattern adds is its leaves' descriptors.
- Narrowing a variable to `record:` because an equality sets it equal to a map turned a dropped row into a failed query. A narrowed variable is stamped on the concept premise that binds it, and a concept field that declares no type fails on the first value of another kind. The same happens with no pattern at all: `?x.text()` beside an untyped read of a field that holds a number. It is a fact about the concept boundary, written in [claims](claims.md).

**Decided.** By the builder: a leaf keeps the descriptor a term has; a slot that holds a shape has to read records; what an equality sets equal to a shape, a rest and a run are checked to be able to hold a record and are not narrowed ([design-typing.md](design-typing.md)).

**Backtracked within the step.** The first build narrowed those variables to `record:`. A test over a field that declares no type failed, and the narrowing became a check.

## 2026-11-11, Building: what a premise of terms alone costs

**Asked.** Why every query ran more instructions after the four steps, one that holds no pattern too. What it takes for such a query to cost what it did.

**Learned.**

- The Perf gate failed on 43d49a5, on seven scenarios, with every counter unchanged:

  | scenario | base | 43d49a5 | more |
  |---|---|---|---|
  | `commit-derived` | 116,210,026 | 129,087,538 | 11.1% |
  | `number-point` | 32,398,236 | 35,551,367 | 9.7% |
  | `read-point` | 21,797,631 | 24,500,838 | 12.4% |
  | `read-sibling` | 29,249,097 | 31,486,018 | 7.6% |
  | `rule-point` | 49,128,084 | 54,147,373 | 10.2% |
  | `rule-recursive` | 779,111,706 | 859,568,403 | 10.3% |
  | `rule-session` | 66,552,691 | 71,723,950 | 7.8% |

- Nearly all of it was one thing. A concept read is a stream of 16,896 bytes that is moved twice on its way to being polled. `evaluate_application` had come to return one of two streams, the read or a stream around the read that takes rows apart, and a value that is one of two types is as large as the larger and is moved once more. On `read-point`, `memcpy` went from 8.86 to 12.35 million instructions, 3.49 of the 4.20 million added.
- The rest was `Parameters`. Each entry had gained a field for a shape, so every node of every map of terms was larger, and asking whether any parameter held a shape walked the map.
- A stream can be made larger without a field. Every `?` in a `try_stream!` body is a place it can stop. A second `?` in a loop that had one kept the row in the stream for its whole life, 96 bytes, and the result being unwrapped too, 96 more. The compiler's layout of the stream says so (`-Zprint-type-sizes`), and nothing else does.
- A `Pattern` held its rest in line and was larger than a term, so an equality between two terms was larger than it had been, and so was everything that holds one.
- The gate's number is not steady enough to judge the last percent by. Five runs of the base binary gave `read-point` from 24.59 to 24.93 million. The allocator is called the same number of times each run and costs a different number of instructions. The instructions outside the allocator and `memcpy` are steady to a few thousand, so the fix was judged by those, by the layout of the stream, and by a count of every allocation by size.

**Decided.** By the maintainer, Irakli: a premise whose parameters are all leaves costs what it did before a parameter could hold a pattern. It is step 5 of the plan.

By the builder, in [design-leaf-cost.md](design-leaf-cost.md):

- One stream for a concept read. The application is held in it as an option the size of the application. A read of terms alone takes it out, as before. A read that holds a pattern keeps it and takes each row apart in the place the row arrived in.
- `Parameters` is the map of terms it was, with one place beside it for the patterns.
- A pattern's rest and run are boxed, and a pattern is the size of a term.

Measured here, one sweep of each binary, with the base and 43d49a5 built from the same checkout:

| scenario | base | 43d49a5 | the fix |
|---|---|---|---|
| `commit-derived` | 127,967,064 | 145,826,056 (+14.0%) | 127,775,114 (-0.1%) |
| `rule-recursive` | 874,598,497 | 982,882,115 (+12.4%) | 875,916,957 (+0.2%) |
| `rule-session` | 72,896,893 | 80,047,317 (+9.8%) | 72,996,317 (+0.1%) |

And the median of five runs of each, with the lowest and highest:

| scenario | base | the fix |
|---|---|---|
| `number-point` | 35,327,686 (35.23 to 35.39 million) | 35,651,189 (35.46 to 35.88 million), +0.9% |
| `read-point` | 24,800,272 (24.59 to 24.93 million) | 24,905,003 (24.60 to 24.98 million), +0.4% |
| `read-sibling` | 32,230,676 (32.01 to 32.40 million) | 32,186,624 (32.06 to 32.64 million), -0.1% |
| `rule-point` | 55,571,670 (55.26 to 56.28 million) | 55,573,725 (55.33 to 55.96 million), 0.0% |

What moves those medians is the allocator. Outside it and `memcpy`, the fix runs 34 to 57 thousand instructions more over 200 reads, 0.10% to 0.15% of a scenario. What they are is listed in the decision file.

**Backtracked.** Two things [design-parameters.md](design-parameters.md) decided were built in the first way that worked, and the gate measured both: a stream around the read for a query that holds a pattern, and a second field in every entry of `Parameters`. On the way to the fix, four more were built and measured and put back: a field for the shapes in the read's stream, the shapes read from the kept application at each of two yields, a second map for the shapes whose empty value still cost a drop, and one map whose slot is a term or a boxed shape, which allocates exactly what the base does and runs more instructions.

## 2026-11-11, Building: a rule that holds a pattern, laid over a branch

**Asked.** By the notation (#773): `Branch::with(rule)` panics for a rule that holds a pattern, though nothing would be stored. Whether the rule's identity can only come from its stored spelling.

**Learned.**

- It does not. The rule has had a name since step 2, by its parts.
- What stopped it is that an overlay is the same batch of facts a transaction commits. A rule laid over a branch was asserted as facts, its body as bytes in format 3, and the query decoded the body back from those bytes. So a rule that was never stored still went through the stored format.
- An assertion is generic over its target and cannot tell a batch that will be committed from one laid over a line for a query.

**Decided.** By the builder: the target of an assertion may hold a thing as it is, and a query layer's does ([design-rule-identity.md](design-rule-identity.md)). `Update::hold` gives the thing back unless a target says otherwise, so a transaction refuses the rule as before. A layer keeps the rule under the name its index facts give it, and a query that finds those facts with no body beside them takes the rule from the layer. Nothing stored is touched, and no format is added. It is step 6 of the plan.

## 2026-11-11, Building: what a written form of a pattern would have to say

For the rule format topic (#779), which decides it. This is what building patterns showed, and decides nothing.

- **Where a pattern stands in a rule's body.** A rule is lowered when it compiles (`pattern::lower`): a positive concept premise that holds a pattern becomes the concept over a variable and an equality between that variable and the pattern. The canonical spelling, which is what a body writes, is of the lowered premises. So a written rule holds a pattern in two places only: a side of an equality, and a parameter of a negated concept premise, which is not lowered. Every other premise is a struct of terms and cannot hold one.
- **The variable that stands for the whole.** It is not the author's. It is named by how the pattern is written, and numbered within the body where the pattern holds a blank. The canonical labeling renames it like any local, so a body would write a label.
- **A map.** Entries, and an optional rest. A key is a value of any type, so entries cannot be the keys of a dag-cbor map, which are text; a composite's bytes write a part as its type and its bytes for the same reason ([design-composite-bytes.md](design-composite-bytes.md)). One pattern has to have one spelling, so the entries go in the order a map of them is kept in, by the type and the bytes of the key, and a key written twice is refused. The rest is a term: a variable, a blank, which takes the other entries and binds nothing, or a constant map.
- **A list.** Items in order, and an optional run, a term as a rest is. **A tuple.** Items in order.
- **A leaf.** A term, written as format 3 writes one.
- **What it must not be mistaken for.** A pattern with no variable is the value it builds and is already held and written as a record constant (`Parameters::set`). So in parameters a pattern always holds a variable or a blank. An equality built by hand can still hold a pattern of constants, which a form would write as a pattern or fold to the constant it is. Either way a form for a pattern has to be told from a record constant by more than its payload.
- **What the canonical spelling does with one today.** It names a pattern by its parts (`pattern_reference`): a leaf as the term it is, a map, a list or a tuple as a map from `map:`, `list:` or `tuple:` to its parts' references, with the rest or the run beside them. It orders a premise that holds one by that reference, where it orders every other premise by its dag-cbor, and it labels the variables inside one at their places. So the identity of a rule that holds a pattern is already decided by the parts and not by any bytes, and a format that writes patterns would not move it.
- **A rule that holds no pattern.** Its identity is a reference of its canonical spelling, and patterns did not change that spelling for it: a term in an equality is written as the term, and parameters of terms as the map of terms. The one exception is a rule that holds a record constant which is a composite, named by the composite's parts since step 1; no earlier release could write that constant. Its body is the dag-cbor of that spelling in an envelope that names the format, `{"format": 3, "rule": …}`. So under a later format its entity stays, and its bytes stay too if a writer stamps a body with the lowest format that can write it, or change in the format number alone if every body is stamped with the latest. A release that reads up to format 3 refuses a later body by its number.

