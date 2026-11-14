# A stored rule says less than a rule can

**Phase: Building.** The maintainer decided the approach on 2026-11-13, and format 4 is being built on top of the patterns build. The [journal](rule-format/journal.md) has how it got here, the [ledger](rule-format/claims.md) what is known, [stamping](rule-format/stamping.md) the matrix the decision was read off, and [the form of a pattern](rule-format/design-pattern-form.md) the one decision the build opened.

## Description

A rule is kept on a branch as a body in format 3: the dag-cbor of its canonical spelling under `{ "format": 3, "rule": … }` (`rust/dialog-query/src/rule/body.rs`). The body holds exactly what the rule's identity holds, so a rule is named by the bytes it is stored as. Formats 0 to 2 are still read, `Branch::upgrade_rules` rewrites them, and a body in a later format than a release reads is refused by its number.

Four pieces of work have each reached something format 3 cannot write:

```
 patterns (#777, #778)     a parameter that holds a map or a list of terms
 records (#748)            a composite value, as a constant and as what a pattern builds
 rule heads (#776)         a rule of several heads, asserts and retracts mixed
 macros (#772, #773)       a rule about syntax: `expand` among its premises, `syntax` as its head
```

Irakli, 2026-11-11: format 4 is the natural next step, and it depends on all four.

Where it shows today:

- A rule that holds a pattern compiles and runs, and cannot be installed: `storable()` refuses it and `transaction.assert` panics with the reason (`rust/dialog-query/src/rule/statement.rs`, on #778).
- The same rule cannot be laid over a branch for one query with `Branch::with`, though nothing would be stored: a rule laid over is named as a stored one is. Found from the notation (#773): a query of several premises with a struct in one of them does not run.
- `entity/derive` over a record was built in the notation's proof of concept and taken out. With it every rule that derives an entity from what it binds holds a pattern, so none can be installed, and a rule and a top-level write could no longer name one entity.
- A rule of several heads is stored as a rule a head, each repeating the body ([rule-heads](https://github.com/dialog-db/dialog-db/pull/776)).
- A rule about syntax is held in memory by the notation's walker and stored nowhere.

What a format has cost before, from the code and the changelog:

- A rule's entity is made of its canonical spelling, and since format 3 the body is that spelling. So a format that spells a rule another way would name it another entity, unless a rule format 3 can write keeps the spelling it has.
- The format's number is not in the name. A rule's entity is the reference of its canonical conclusion, premises and reduce (`DeductiveRule::try_this`, `rule/reference.rs`); the number is only on the envelope the body is stored in (`rule/body.rs`).
- `Branch::upgrade_rules` rewrote every body of an earlier format into format 3. Until an embedder runs it, an earlier body is read as before.
- A release refuses a format later than its own, so a rule written in a new format is inert on an older replica until it upgrades.

What each of the four would ask of a body, as read so far and not as a plan:

| | asks for | touches a rule that uses none of it? |
|---|---|---|
| patterns | a written form for a pattern where a parameter stands | ? |
| records | nothing new where a composite is a constant, which format 3 writes as a record's bytes; a layout the bytes will not leave (#748) | no |
| rule heads | several heads where there is one, a polarity a head | ? it depends on how one head is spelled among several |
| macros | a head that is not a concept, and a premise that takes a form apart | ? |

A hypothesis this opens, the agent's, 2026-11-11: **a format that only adds leaves every rule it does not touch where it is.** If a rule that holds no pattern, one head and no syntax keeps its canonical parts, it keeps its entity, and its body can go on being written as format 3. Then each of the four can be added when it is ready, a rule is written in the lowest format that can say it, and nothing is migrated. Confirmed by a test that a rule of terms alone has one entity and one body before and after a pattern can be written. The first half is run, 2026-11-12: `it_keeps_the_name_and_the_bytes_of_a_rule_format_3_wrote` writes out the entity and the body of two rules of terms and passes on main and, unchanged, on `feat/patterns`, where a pattern can be held and not yet written. Refuted if the canonical spelling of an old rule has to change to make room, as it might for several heads.

A stored format changes here, so whatever is built is a breaking change with a compatibility note (`.claude/rules/versioning-and-changelog.md`).

## Problem

A rule that holds a map or a list of terms runs where it is built and cannot be kept. It has a name and no bytes, so it is not installed on a branch, not synced, and not there the next time the branch is opened.

Of the four pieces of work in the description, patterns are the one left. Records ask nothing new of a body. A rule has one head since 2026-11-13, so several heads are no addition. A rule about syntax is not stored yet, and is not part of this.

## Approach

Format 4 writes what format 3 wrote, and a map or a list of terms where a parameter stands.

```
 a release       writes      reads               migrates
 this one        4, always   4, 3, 2, 1, 0       Branch::upgrade_rules rewrites what it finds in 4
 the one after   4           4                   -
```

- **The new format always** (Irakli, 2026-11-13): "switch to a new format and maintain old one for backwrards compatibility for one release cycle while offering migration. In version after we'll remove support" for the old ones. It is the first column of the [stamping matrix](rule-format/stamping.md).
- A rule that holds no pattern is written as it was, but for the number: one byte of its body moves, and its name does not.
- A rule that holds a pattern keeps the name it has had since it could be built: it is named by its parts, and the bytes are new.
- What the other direction costs, said to the maintainer and taken as accepted for one cycle: a release that reads up to 3 skips every rule this one writes, a rule of plain terms too, and says nothing. It lasts until every peer of a space has upgraded.
- A pattern is written under the word for its shape: [the form of a pattern](rule-format/design-pattern-form.md).
