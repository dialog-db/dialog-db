# A stored rule says less than a rule can: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | The approach is decided: format 4 writes a pattern, a release writes it always and reads the earlier ones for one release cycle, with `Branch::upgrade_rules` as the migration. Patterns are the one addition: a rule has one head, records ask nothing, a rule about syntax is not stored. A pattern is named by its parts already, so no name moves. | **Building**, on `feat/rule-format-4` over the patterns build. |
| **Agenda** | The maintainer's word on the two choices the build of `match` made without him ([the form of a pattern](design-pattern-form.md#as-built-2026-11-16)). What one entity holds once two releases have each written its body. | The build, its changelog entry with the compatibility note, and the notation storing a rule that holds a struct. |

## Candidate problems

"Format 4" is a feature. These are what it could be an answer to, each a hypothesis in the [ledger](claims.md).

1. **A rule that runs cannot be kept.** A rule that holds a pattern compiles and runs and is refused when installed. So it is not synced, not there after a restart, and a rule that writes cannot hold one at all, since a rule that writes runs only from the branch.
2. **Nothing says what a new body does to a replica that cannot read it.** A release skips a rule it cannot read, silently. Whether a rule it could have read is among them depends on how a writer stamps.
3. **Four pieces of work wait on one number.** Each needs the body to say more, and a format has so far been one step for everything.

## 2026-11-12, Diagnosing: what stamping decides, and what is already pinned

**Asked.** By the agent, of the code: does a rule that holds no pattern keep its name and its bytes once a pattern can be held? What does a release do with a body in a later format? What moves if every body is stamped with the latest format?

**Learned.**

- A rule's name and bytes are now written out in a test (`it_keeps_the_name_and_the_bytes_of_a_rule_format_3_wrote`, two rules: one premise with a constant, and a join). It passes on main and, unchanged, on `feat/patterns` at eb07e88. So patterns in memory moved no rule of terms. Nothing pinned either before: the migration fixture pins what the release before format 3 wrote.
- The format's number is the last byte of a body: dag-cbor orders `rule` before `format`. Stamping a rule of terms with 4 changes that byte and nothing else, and leaves its name.
- A release that cannot read a body skips the rule. Three of the four load paths drop it and go on (`session.rs` resolving a concept's rules, `induce.rs` twice); one returns the error (`session.rs`, reading one body by entity). The concept derives nothing from the rule and no one is told.
- A release reads past an envelope key it does not know (`it_reads_past_an_envelope_key_it_does_not_know`). So naming what a body uses beside the number tells a release of today nothing: only a later number makes it refuse.
- The two branches were first built into one target directory, and cargo took one branch's built crate for the other's: a build failed on a method the sources had. The results here are from each branch rebuilt from its own sources.
- From building patterns ([patterns journal](../patterns/journal.md), 2026-11-11): a written rule holds a pattern in two places only, a side of an equality and a parameter of a negated concept premise, because a positive premise is lowered. A rule that holds one is already named by its parts, so writing it moves no name.

```
 the same rule of terms, written by a release that can also write patterns

   stamped with the latest      …6d6174 04     same name, one byte moved, an older release skips it
   stamped with the lowest      …6d6174 03     same name, same bytes, an older release reads it
```

**Decided.** Not decided. The stamping policy is the maintainer's. The [matrix](stamping.md) stands on problem 2 above, which he has not cut.

## 2026-11-14, Building: the new format always, and the words of a pattern

**Asked.** On 2026-11-13, which number a writer stamps. The maintainer did not follow the question, and answered what he wants: "I think we should switch to a new format and maintain old one for backwrards compatibility for one release cycle while offering migration. In version after we'll remove support for old formulas."

**Learned.**

- The question was put as a column to pick and did not say the case the columns differ in. Said again as two peers on one space, one upgraded, the answer is the first column of the matrix: the latest.
- The scope is patterns alone. Since the description was written a rule has one head, and a rule about syntax is held by the notation and not stored.
- The patterns build already names a pattern by its parts, spells a rule that holds one canonically, and orders its premises. What was missing is small: a written form for a pattern and for parameters that hold one, and the number.
- The plan of the patterns topic left the words of that form undecided. Decided by the agent as a [matrix](design-pattern-form.md): a word for the shape, which is not shaped as the name of a type.

**Decided.** By the maintainer: the approach. By the agent, until said otherwise: the problem's words, and the words of a pattern.

## 2026-11-14, Designing: a pattern is written as what it is, under `match`

**Asked.** The agent asked whether its words, `map`, `rest`, `list`, `run`, stand. The maintainer, by voice: is this JSON; "is there any reason why you wouldn't just use a map as it is", an array for a list, and `...` as a key for the rest.

**Learned.**

- The agent's one reason for a word a shape: a map with one entry is already a variable (`?`) or a constant (the name of a type), so a bare map where a parameter stands could be read as either. The maintainer took a wrapper for that, "to stay consistent", and named it: `match`, over `pattern` and `#`.
- Shown `"..."` as a bare string among a list's items, he refused it. `...` is a key in both shapes, and in a list it is the one entry of the last item.
- The form is drawn in [design-pattern-form.md](design-pattern-form.md).
- Building it, the agent found what the conversation did not say: the form writes a key as text, and a map pattern holds a key of any type. The notation keys `{author: ?who}` by a symbol. So the form as decided cannot write every pattern #778 can hold, and the agent's remark in the conversation, that only a key ending in `:` is at stake, was short of it.
- A symbol cannot be `?`, `...` or end in `:`. Were a map's key a name, the form would have no case to refuse.

**Decided.** By the maintainer: `match`, a map as a map, a list as an array, `...` as a key. Not decided, and his: what a map's key is. Nothing of the form is built until it is, since the answer is either a change to #778 or a change to the form.

#799 writes the agent's words until then and stays a draft. The topic stays in Building: this is the decision the build opened, moved.

## 2026-11-16, Building: `match`, as built

The maintainer was away and asked for the build to go on without him.

**Asked.** Nothing of him. The form he settled on 2026-11-14 waited on what a map's key is, which is text since 2026-11-15, and a record is rebuilt on it (#778).

**Learned.** Building the form met three things the drawing does not say ([design-pattern-form.md](design-pattern-form.md#as-built-2026-11-16)):

- Refusing a key ending in `:` only where it is alone in a map makes whether a key is allowed depend on how many entries stand beside it. It is refused always, with `?` and `...`.
- A constant alone is written bare as JSON, and bytes bare are an array of numbers, which under `match` is a list. Found by a test: a run written `[1]` was read as the bytes `01`. So inside `match` a constant is under its type, as the drawing has it, and what takes the rest is refused when it is an array.
- `[?a {... ?r}]` and `[?a ... ?r]` are one array as drawn. The notation meets the first: a struct read is open, so `[{}]` in a premise is a list ending in a map of a rest.

**Decided.** By the agent, each until he says otherwise: the three keys are refused always; a list that ends in a map of a rest and has no run says `{ "...": [] }` after it. Nothing of the maintainer's form moved.

#799 writes `match`. A rule's entity does not move by any of this: it is named by its parts, and the names of the parts (`rest`, `run`) are not the words of the stored form.
