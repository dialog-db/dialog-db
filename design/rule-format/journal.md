# A stored rule says less than a rule can: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | Four pieces of work each reach something format 3 cannot write. A rule's name does not hold the format's number. Adding patterns in memory moved neither the name nor the bytes of a rule that holds none, pinned by a test. A release skips a rule whose body it cannot read and says nothing. | **Diagnosing.** A problem is proposed and not cut. The stamping decision is drawn as a [matrix](stamping.md) ahead of its phase, because it is what the patterns work waits on. |
| **Agenda** | Whether the proposed problem is the problem. Which stamping policy. What one entity holds when two releases write one rule with two bodies. Whether several heads can be added without respelling a rule of one. | The maintainer cuts the problem statement and decides stamping. Then a writer that stamps a body, built with patterns as the first addition. |

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
