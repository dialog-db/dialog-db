# One stored format for rules, concepts and attributes: direction

Decision: how a release comes to read only the format it writes for
rules, concepts and attributes, and what a live space and an older
replica go through on the way.

For the problem in the [top story](../stored-format.md#problem): every
release carries readers for what earlier ones stored, main writes two
rule formats, and removing the readers means rewriting what live spaces
hold while peers on the older release keep syncing.

Throughout, O is a replica on 0.5.0, the latest tag, and N one on the
next release. Facts in cells are backed in the
[inventory](inventory.md), the [blast radius](blast-radius.md), the
[mixed fleets](mixed-fleets.md) note and the [ledger](claims.md).

## The approaches

- **A. Status quo.** Main as it stands, released: format 4 for a rule
  of terms, 5 for a rule that holds a pattern; every format from 0 read
  on every read path; `Branch::upgrade_rules` run by the embedder,
  retracting what it rewrites.
- **B. One format, cut over once.** N writes format 5 for every rule
  and reads format 5 only, on every read path. The readers of formats
  0 to 4 and of 0.5.0 identities move into the upgrade and nowhere else.
  The upgrade asserts the new facts beside the old ones and keeps the
  old until the embedder prunes them. The release after N deletes the
  upgrade's readers.
- **C. Read old, write new, for one cycle.** N writes format 5 and reads
  formats 3 and 5 (4 was never tagged) on every read path, and rewrites
  what a pull brings, as a pull aligns a tree an older release laid out
  (`repository/branch/migrate.rs:266` to `:281`). The release after N
  reads 5 only.
- **D. Fence, then migrate.** B, and the migrated space carries a mark
  0.5.0 refuses by name: a critical (odd) tree manifest field, which
  0.5.0 already refuses (`dialog-search-tree/src/manifest.rs:62` at
  v0.5.0). O cannot open the space until it upgrades.
- **E. A new vocabulary.** B, with format 5 rules and the transient
  marker stored under attribute names no release up to 0.5.0 reads
  (new names inside the `dialog.rule/` and `dialog.concept/` carve-out,
  `dialog-corpus/src/tree.rs:1526` to `:1560`). O never meets a format 5
  body; N never reads the old names but in the upgrade. Old and new sit
  side by side until pruned.

## Decided for every approach

The maintainer decided on 2026-11-24 that the move to format 5 reverses
every listed `as` to least first and makes every `top` a `max`
([listed order](listed-order.md)). It is the same in every column: the
upgrade reverses a list only in a body below format 5, and an embedder
reverses the definitions it ships. Under C a format 3 body is read on
every read path for a release, so C also reverses on every read, not
only in the upgrade.

## The matrix

| criterion \ approach | A. Status quo | B. One format, cut over once | C. Read old, write new, one cycle | D. Fence, then migrate | E. A new vocabulary |
|---|---|---|---|---|---|
| what a live tonk space goes through | one identity move from 0.5.0, already on main; rules rewritten and old facts retracted | 🟢 the same one move, if it lands before the next tag; old facts kept until pruned | the same move, spread over every pull of the cycle | the same as B, and every device locked out until it upgrades | 🟢 the same as B |
| what O sees once a peer upgrades | 🔴 its own rules retracted; a format 4 or 5 body indexed under a relation it reads is met on a query, and ? fails the query (mixed fleets, F1) | 🔴 keeps its own rules; meets format 5 bodies under the relations they derive, the same as A | 🔴 the same as B | 🟢 cannot open the space; nothing it writes reaches N | 🟢 keeps its own rules and markers; never meets a format 5 body |
| transient markers | 🔴 1 of tonk-team's 89 moved; the rest stranded on 0.5.0 identities | 🟡 moved for every concept the embedder hands the upgrade; O keeps its own | 🟡 the same as B, on every pull | 🟡 the same as B | 🟢 new marker beside the old; O reads the old, N the new |
| a rule fires twice | the same rule meaning under two entities can only be read by one release each; what both write is deduplicated by the novelty check, as today (F4) | the same as A | 🟡 N reads both the format 3 and the format 5 body of one rule while the old is kept; two rules, one meaning, both evaluated | as B | as A |
| a rule retracted under a replica that still needs it | 🔴 yes: the upgrade retracts O's rule facts (`upgrade.rs:297` to `:312`) | 🟢 not until the embedder prunes | 🔴 as A, if the rewrite on pull retracts | 🟢 O is locked out anyway | 🟢 as B |
| a rule comes back after being retracted | 🟡 yes, when O asserts it again (T4); inert on N, listed `older:` | 🟡 the same, after a prune | 🟡 read again by N for the cycle | 🟢 O cannot write | 🟡 the same as B |
| code deleted in N | none | 🟢 formats 0 to 4 and every older spelling off every read path; `TERMS_FORMAT`, `format_for`, the per-format branches of `invalid_reason` | 🟡 formats 0 to 2 and 4 only; 3 stays on read paths for a release | 🟢 as B | 🟢 as B, and N's read paths name only the new attributes |
| code deleted the release after | 🔴 nothing planned | 🟢 the upgrade's readers and `dialog_query::migration`'s v0, v1 | 🟢 format 3 | 🟢 as B | 🟢 as B, and the old names |
| code added | none | an upgrade that keeps and prunes, and takes concepts | 🔴 a rewrite in the pull path, and a reader of two formats on every read path | B, and a manifest field and its migration step | B, and renamed attributes in every place that names them (`statement.rs`, `rules.rs`, `induce.rs`, `tonk_dataset.rs`) |
| what can go wrong | stranded markers turn commands into durable facts with nothing reporting it; O's queries ? fail | O's queries ? fail on format 5 bodies; a stranded marker for a concept the embedder did not hand over | a rewrite on pull racing a local commit; two formats on every read path for a release | a device left on 0.5.0 cannot open its space: loud, and a support cost | O authoring a rule after the cut-over: N does not see it until the upgrade runs again |
| how it is detected | `dialog.rule/invalid` on N lists `forged:` and `older:`; nothing lists a stranded marker | `dialog.rule/invalid` on N; the upgrade reports what it kept and what it could not map | the same as B | O refuses by name | the upgrade reports what it found under the old names that it had not migrated |
| what tonk has to do | run the upgrade; move its own records (T2) | run the upgrade with its concepts; prune when every device has upgraded (T9); move its own records | run nothing but its own records | run the upgrade; ship every client before it runs | as B |

## Reading the matrix

- No column is all green. E and D are the only ones where O meets
  nothing it cannot read; D buys it by locking O out, E by keeping two
  vocabularies until a prune.
- A, B and C differ on the first row only in timing. The identity move
  a live space goes through is already on main (inventory,
  "What tonk-team holds": 135 of 135 concluded concepts moved). Folding
  format 4 into 5, and moving identities again if the maintainer wants
  to, costs a live space nothing more if it lands before the next tag,
  and a second migration if it lands after.
- The second row decides between B and E. If F1 holds (a 0.5.0 query
  fails on a body it cannot read), B and C hand every O replica a
  failing query the first time a peer upgrades. E does not, because O
  never looks under the new names.
- C is the one column that keeps an old format on every read path for
  a release, which is what the maintainer has said not to do
  (2026-11-18, and 2026-11-24 in his words in the top story).

## Recommendation

E, landed before the next tag, as the [plan](plan.md) orders it. It is
the agent's recommendation, not a decision.

Why: it is B's cut-over with the one hazard B cannot remove taken out.
O and N never read each other's rule facts, so neither skips, fails or
retracts anything of the other's; the code N carries on its read paths
is format 5 only; the old vocabulary is read in one place, the upgrade,
and deleted the release after. The cost is renaming the rule and marker
attributes, which is mechanical, and a prune the embedder decides when
to run.

What would change it:

- if F1 is refuted (0.5.0 skips rather than fails), B is as good as E for
  O's reads, and cheaper;
- if tonk can guarantee every device upgrades before any writes (T9), D
  is simplest: one vocabulary, nothing kept, nothing pruned;
- if tonk's records name concepts by dialog identity (T2), every
  approach adds a migration on tonk's side, and the choice here does not
  change it.

## Scope

Not taken on here: the removal of `top` and the order of a listed type
(`feat/listed-type-order`); a rule holding a composite constant reading
back (#855); the tree manifest's legacy layout, the number band, the
repository layout version and upstream cells, which are storage and not
rules, concepts or attributes.
