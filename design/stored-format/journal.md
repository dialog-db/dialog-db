# One stored format for rules, concepts and attributes: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | What is stored and read, by file. Identities are keyed by nothing a user fact uses; only dialog's own index facts and markers name them, inside dialog. Formats 4 and 5 are unreleased, and every identity already moved since 0.5.0, so a cut-over before the next tag costs a live space one migration. Main's upgrade would strand 88 of tonk-team's 89 transient markers. A 0.5.0 replica meets a newer body under the same index keys and fails the query (F1, run). | Directing. Six approaches and a matrix; F recommended by the agent (E before tonk's answers). |
| **Agenda** | Tonk's answers to T12 to T15 ([blast radius](blast-radius.md)), T13 first: whether tonk can reinstall without retracting the previous install. The maintainer's answers to open questions 1 to 4 in the top story; 5 is answered.  | The maintainer reads the matrix and chooses. Then step 1 of the [plan](plan.md): tests pinning today's behaviour. |

## 2026-11-24, Describing to Directing: what "format 5 for everything" would take

**Asked.** The maintainer's brief, in seven questions: what is stored
and read; who names a rule or concept by entity; what two releases in one
space do; what format 5 for everything removes; whether concepts and
attributes need a number of their own; approaches and a matrix; a step
order. Not asked in a sitting: this entry is the agent's alone, from the
code, a probe and the tests named below.

**Learned.**

- Every tag up to 0.5.0 writes format 3. Formats 4 and 5 exist only on
  main, so "format 4 legacy" is code, not data in a live space, unless
  a tonk client runs an untagged revision (T6).
- Every identity already moved between 0.5.0 and main. The maintainer's
  "their identities will change" is therefore not a new cost if it lands
  before the next tag; after the tag it is a second migration.
- The format number is not in any identity. Rewriting a body in format 5
  moves no identity unless the identity function changes. Written as
  open question 1, since the brief says identities change without saying
  which part of the hash moves.
- Facts are keyed by relation, so no user fact moves with an identity.
  Inside dialog a rule entity is named only by its own index facts. A
  concept entity is named by index facts, the transient marker, the
  provider rows of `session:` and a command's `answered` row.
- The tonk-team fixture in `dialog-baseline` was run through main with a
  probe outside the repository: 126 of 135 bodies decode, 9 are refused
  and rewritten; 135 of 135 concluded concepts moved identity; 1 of 89
  transient markers is on a concept a rule concludes, so the upgrade on
  main moves 1. The test that covers marker moves marks a concluded
  concept, and does not see this.
- The rule-format ledger's "three of four load paths skip silently" no
  longer describes main (all skip, and the branch lists them) and never
  described 0.5.0 exactly: there two of five paths propagate the decode
  error (`session.rs:1815`, `:1962` at v0.5.0), and the changelog says a
  query "used to fail with `rule hydrate`".
- The rule and marker attributes 0.5.0 reads are a fixed list, and the
  carve-out of `dialog.rule/` and `dialog.concept/` admits other names.
  That is approach E: two vocabularies that never meet.
- Contradicting the brief: `dialog-migration` holds the single-file
  archive migration only and reads no rule, concept or attribute
  spelling. `identity.rs` says a concept's identity maps `optional` and
  `conforms`; it maps `pick` too.

**Decided.** Nothing; the maintainer decides. The agent recommends E on
the opinion that a 0.5.0 replica should meet nothing it cannot read, and
on F1, whose run is recorded below.

## 2026-11-24, the runs

F1, on a v0.5.0 worktree outside the repository, one uncommitted test
in its `transaction/migration.rs`: `org/salary` of `id:alice` read
`[700]`; after committing a `{ "format": 4 }` body under `rule:later`
with `dialog.rule/derives on:org/salary` and a conclusion, the same read
returned `Err("Fact store: rule hydrate: the rule body is in format 4,
and this release reads up to 3")`. Confirmed: any rule body a peer on
main writes, format 4 included, fails a 0.5.0 replica's queries over
the relations it derives. That holds for main as it stands (approach
A), not only for format 5.

On main, the tests the topic leans on, run 2026-11-24, all pass:
`rule::body::tests::it_reads_every_format_up_to_its_own`,
`it_refuses_a_later_format`,
`rule::reference::tests::it_keeps_the_name_and_the_bytes_of_a_rule_that_holds_no_pattern`
(dialog-query), and in dialog-repository the six of
`transaction::pick_migration`, `it_leaves_a_body_in_a_later_format_inert`
and `it_rewrites_only_bodies_below_format_4_on_upgrade`. 11 passed, 0
failed. None of them covers a marker on a concept no rule concludes.

## 2026-11-24, Directing: the order of a listed `as`

**Asked.** Relayed by the coordinator from the maintainer: reverse the
listed `as` from format 4 to 5 so it reads left to right, `top` becoming
`max`. The coordinator asked which stored lists had an order that meant
anything, and whether to reverse every list or only those a `top` reads.

**Learned.** In this repository 4 listed attributes are read by `top`
and 5 are not; one of the 5 is written least first. tonk-team's fixture
holds none. A reversal is not idempotent and a list does not say its
order, so the format number is what lets the upgrade reverse once; a
definition outside a body has no number, and the embedder reverses its
own in the same release (T11). Main's `rewrite_body` and its doc
disagree on whether a `top` list is reversed.

**Decided.** By the maintainer: reverse every list ("No lets reverse all
please"), overruling the coordinator's opinion that an unranked list may
already be least first. It keeps one relation to one order. Recorded in
the ledger and [listed order](listed-order.md); it holds in every
approach of the matrix.

## 2026-11-24, Directing: `top` is deprecated this version, removed in the next

**Asked.** Relayed by the coordinator, the maintainer: "We can keep top
just deprecate it and not have it in notation, we can remove top in the
next version allowing anyone wishing to migrate to do so".

**Learned.** Meanwhile a list can be read by a deprecated `top` and by a
`max` field: `top` is `min` over the same list, so the read is defined,
and over a list still best first `max` picks the worst value. If the
reversal waits for the next version's upgrade, every attribute with a
listed `as` and everything embedding it moves identity a second time.

**Decided.** By the maintainer: `top` stays this version, deprecated,
not in the new notation, removed in the next; nothing reversed
automatically now. Plan steps 4 and 7 reworked. Not decided: whether the
reversal is a step of this version's upgrade or the next one's (open
question 5); the coordinator reads the maintainer's words as "by a
migration the owner runs before the next version", and that reading is
the coordinator's. Open question 1 restated as what an identity is a
hash of once there is one format.

## 2026-11-24, Directing: tonk's answers, and the rewrite moves to the embedder

**Asked.** T1 to T11, answered by a read-only session on tonk @ b125f0d
and relayed by the coordinator, who asked what changes: who rewrites
tonk's facts and what dialog must export; what undetectable mixing does
to B and E; how a dialog-side rewrite must appear in history for tonk's
uninstall by replay; the smallest plan safe for tonk.

**Learned.**

- Tonk runs exactly 0.5.0 (the tag's commit, pinned by revision).
- "No user fact moves with an identity" holds of dialog and not of tonk:
  tonk writes dialog identities as values in its own facts and moves
  them with its own `rekey`. It knows earlier identities only through
  dialog's v0 functions.
- Tonk reinstalls its library by replaying the previous install's
  retractions. A rewrite dialog makes outside an install commit is never
  retracted by that replay, so it outlives the next library change.
- Versions mix in a space and nobody can tell. Any step keyed on "every
  device has upgraded" keys on something unobservable.
- Tonk's three listed attributes are all ranked and best first; one of
  them has two descriptors already.

**Backtracked.** The recommendation, not the phase: from E to F. E's
format and vocabulary stay; what moves is who rewrites. Dialog rewrites
nothing an embedder installs, and exports each generation's identity
functions and a generation constant. The plan is reordered so the
exports land first, which every approach needs.

**Decided.** Nothing by the maintainer in this entry. Recorded as the
agent's opinion: F. Open with tonk: T12 to T15.

## 2026-11-24, Directing: the reversal rides a rule migration

**Asked.** Open question 5, answered by the maintainer and relayed: "If
we have to migrate rules we can just as well reverse, if we don't then
we keep them as is". The coordinator asked whether a 0.5.0 space has to
have its rules migrated, which decides which branch applies.

**Learned.** It does. Every 0.5.0 body is format 3, every identity moved
since, and a body under an entity it does not hash to is inert; on main
all 135 of tonk-team's rules derive and fire nothing until rewritten.

**Decided.** By the maintainer: the reversal and `top` becoming `max`
are a step of a rule migration, never a migration of their own. For a
0.5.0 space that is this version's migration (tonk's reinstall under F,
`rewrite_body` for other rules). Replaces the coordinator's reading.
Plan steps 3 and 7 carry it.
