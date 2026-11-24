# One stored format for rules, concepts and attributes: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | What is stored and read, by file. Identities are keyed by nothing a user fact uses; only dialog's own index facts and markers name them, inside dialog. Formats 4 and 5 are unreleased, and every identity already moved since 0.5.0, so a cut-over before the next tag costs a live space one migration. Main's upgrade would strand 88 of tonk-team's 89 transient markers. A 0.5.0 replica meets a newer body under the same index keys and fails the query (F1, run). | Directing. Five approaches and a matrix; E recommended by the agent. |
| **Agenda** | Tonk's answers T1 to T11 ([blast radius](blast-radius.md)), T1, T2 and T9 first. The maintainer's answers to the four open questions in the top story.  | The maintainer reads the matrix and chooses. Then step 1 of the [plan](plan.md): tests pinning today's behaviour. |

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
