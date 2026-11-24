# The last migration of every rule, and what this plan takes from it

## What happened

Reported facts, backed "tonk history, PR numbers as given", from a
session that reconstructed tonk-labs/tonk's history and pull requests,
relayed by the coordinator on 2026-11-24. Not read in this session.

Tonk #1058 (merged 2026-10-14) moved tonk from dialog `tonk-2026-10-03`
to `v0.3.0`: merkle-reference identities for attributes, concepts and
rules, canonical rule bodies, and the number band, under which dialog
refuses to commit into a tree an earlier release laid out. It added
`BranchReference::migrate_once`: once per branch per process, under the
transactor lock, gated on the head's storage layout, it runs
`Branch::migrate_storage`, then tonk's `upgrade_definitions` (attributes,
concepts, names, routes, views), then `Branch::upgrade_rules`. Every
commit through the reactor and every direct `Branch` writer runs it
first, and pages show an "upgrading" frame. #1058 also added `rekey` and
`rekey_sites` with dialog's `*_v0` functions, because after the upgrade a
space rendered "Model not found".

What went wrong afterwards:

- **#1113** (2026-10-19): every CLI command failed with "The branch must
  be migrated before it is written: its tree keys values under encoding
  0", because opening a profile writes the profile state branch outside
  every migration gate. A first fix migrated storage and rules only, and
  later storage-gated calls would then have skipped the definitions.
- **#1304** (2026-11-05): spaces a v0.6.17 build wrote never opened on
  v0.7.0-rc.1, because opening a repository writes its `meta` registry
  branch in the old layout. Fixed in dialog (#747). Tagged users got it
  three weeks after the merge, the rollout being staggered.
- **#1081** (open): two deployments on different builds kept replacing
  each other's library in one space.
- **Never fixed:** rules arriving by sync after a branch migrated stay
  inert until the next process start; the profile library reconcile does
  not rekey; rc-seeded spaces were not rekeyed. Never exercised: a space
  synced from a replica still on the shipping release; a partial replica
  whose rule bodies the upgrade has to fetch.
- **Not addressed in #1113:** a crash between the storage rewrite and the
  definitions step strands the definitions, since every gate checks the
  storage encoding only. "Migrated" was inferred from the encoding; no
  record says it. Each step is idempotent, a half-run is not safe.
- **No test opens a space an older binary wrote.** Tests synthesize the
  old state with current code; old-binary checks were manual. The only
  old-binary fixtures were deleted (#743).

Tonk has an open draft, **#1329**, for the next migration, "from dialog
0.4": it extends `upgrade_definitions` and `upgrade_rules`, and lists the
same gaps (inert rules arriving by sync; "no public 0.4 identity for a
ranked attribute on its own").

## Each lesson against the plan

| lesson | answered by | how |
|---|---|---|
| one gated entry point before every write path, direct writers included (#1113, #1304) | plan step 3 | the gate is in dialog's commit path, which every writer goes through: a commit on a tree whose fence is not the current generation fails with `NeedsMigration` only if the tree holds rule facts or markers; a tree that holds none is fenced by that commit and nothing fails. So the profile state, `meta` and every branch with no rules never fail; only a branch with rules needs the embedder's entry point |
| an "upgrading" signal | tonk | the entry point is tonk's; `NeedsMigration` names the step a write is waiting for |
| record completion explicitly; never infer it from storage | plan step 3 | the fence is that record, and it is written last, in the same publish as everything the migration changes (below) |
| a half-run must be safe and resumable | plan step 3 | one publish: nothing is visible before it, everything after it. A crash before the publish leaves the head as it was, unfenced, and the next run starts over |
| versioned identity functions exported by dialog, and a rekey in the same publish | plan step 1, and step 3 | step 1 exports each generation (in flight on `feat/identity-generations`); it must also export the 0.5.0 identity of an attribute read under a ranking pick, `aggregated_reference_v1`, which is private today (`migration.rs:182`), the gap #1329 names. Tonk's rekey goes in the migration's one publish |
| rules arriving by sync after the migration stay inert (#1058, #1329) | plan step 4 | designed below; not deferred |
| a partial replica whose rule bodies the upgrade has to fetch | plan step 3 | `upgrade_rules` selects `dialog.rule/source` through the branch, which fetches from the remote (`upgrade.rs:231` to `:237`). Offline, the migration fails and the gate keeps refusing writes to that branch, by name; reads go on. Unanswered: whether a partial replica should fetch the `dialog.rule/` range before it migrates, to migrate offline later |
| two deployments replacing each other's library (#1081) | partly, by the fence | a device on 0.5.0 cannot sync a fenced space, so 0.5.0 and the next release no longer fight over one. Two builds of the next release that ship different libraries still do: that is tonk's to decide (one library per lineage) |
| a staggered rollout with no version gate | the fence | the fence is the version gate, on the data, where 0.5.0 already refuses it |
| an end-to-end check against data the shipping binary wrote, with before and after query counts | plan step 2 | a fixture written by the v0.5.0 binary itself (below), and every later step tested against it |
| the profile library reconcile does not rekey; rc-seeded spaces not rekeyed | tonk, and the gate | a branch whose library was not reinstalled still holds old rule facts, so the gate refuses its next write rather than letting it run half-migrated. Who reinstalls it is tonk's |

## When the fence is set, and what reads it

`Branch::migrate` stages, in one transaction and in this order: the
embedder's changes (tonk's reinstall: retract the previous install,
assert the new library, write its record; its rekey), then
`upgrade_rules` over what the branch then holds, then the fence: a
critical manifest field whose value is the current identity generation
from step 1. It publishes once.

- Before the publish, nothing has happened; a crash leaves the head as it
  was, and the next run starts over. After it, all of it has.
- The gate reads the fence where a commit already reads the base tree's
  manifest for the number band (`repository/branch/commit.rs:776` to
  `:787`). A fence below the current generation counts as no fence, so
  when the generation moves again the same gate asks for the next
  migration.
- `upgrade_rules` runs over the staged view, after the embedder's
  reinstall, so it rewrites only what no install commit holds, and tonk's
  replay stays exact.
- Reads are not gated: a reader on an unfenced tree sees its rules inert
  and listed, as on main today.

## Every kind of branch, who migrates it, and what a skip does

| branch | holds rule facts or markers? | migrated by | if skipped |
|---|---|---|---|
| space content (the primary of a space) | yes: library rules, rules evaluated into it (T12), markers | tonk's entry point: `Branch::migrate` with the reinstall and rekey staged | the gate refuses its next write, by name; reads see its rules inert and listed |
| a durable layer of a stack (its own tree) | no definitions (T5); possibly rules a user evaluated into it | dialog: fenced at its first write if it holds none; else as space content | as space content, for one that holds rules |
| profile state | no, as reported | dialog: fenced at its first write | nothing fails (#1113 cannot recur) |
| profile library | yes: the library | tonk's entry point, as space content | the gate refuses its write; the reconcile has to go through the entry point |
| `meta` registry | no (`registry.rs:346` to `:353`: "it has no rules") | dialog: fenced at its first write | nothing fails (#1304 cannot recur) |
| custody, account, roster | ? not known here; if any holds rules or markers, as space content | dialog if none; else tonk's entry point | if it holds rules: the gate refuses, by name |
| `session:`, overlays, memory layers | rows naming concepts (tabs, intent answers); never synced, not trees a peer reads | tonk's `rekey_sites`; no fence | a tab names a concept nothing reads: what #1058 saw as "Model not found" |

The table is for checking, not for code: the gate does not know a
branch's kind. It asks one question of the tree, whether it holds a
`dialog.rule/*` or `dialog.concept/transient` fact, once per branch per
process.

## Rules that arrive by sync after a branch migrated

An N replica pulls a head an O device pushed after the space was fenced
(O cannot pull the fenced head, but it can still push its own). What
arrives is what O wrote since the common ancestor: data, and any rule or
marker O wrote, in format 3 under 0.5.0 identities.

The pull rewrites them in the merge it commits:

- A pull already screens every incoming fact before it integrates it
  (`dialog-corpus/src/merge.rs:1` to `:40`). An incoming
  `dialog.rule/source` in a format below the current one is run through
  `rewrite_body`; the merge retracts the incoming rule facts and asserts
  the rewritten rule under its identity, with its indexes.
- An incoming marker on a concept that an incoming or a stored rule
  concludes is moved, as the upgrade moves one; any other is left and
  listed.
- The merge's tree is the local one, fenced; the arrivals are written
  into it in the current format, so the fence stays true.
- The rewrite is a function of the bytes, so two N replicas pulling the
  same O head write the same facts and converge.
- A body the rewrite cannot map (a chain, an unknown pick) is left inert
  and listed in `dialog.rule/invalid`, as today.

What this does not do: run tonk's library reinstall. A library rule O
re-asserts arrives as a rule like any other and is rewritten; it is in no
install commit, which is the case tonk's replay already has for rules
evaluated into a space. Whether O re-asserting its old library into a
migrated space is a case tonk wants at all is #1081's question.

This is the number band's answer to the same hole: a pull aligns a tree
an older release wrote (`repository/branch/migrate.rs:266` to `:281`),
here for rule facts rather than keys.

## A fixture the real v0.5.0 binary wrote

What #1058 lacked, and what every later step is tested against.

- **Produced by** a capture program run from a checkout of the `v0.5.0`
  tag, as `transaction/fixtures/README.md` already does for `main` at
  120fba8: the program's source is in the fixture's README, it is added
  as a workspace member of the old checkout only for the run, and is
  never part of this repository's build. It opens a space with the fs
  provider and writes, through 0.5.0's own API: deductive and inductive
  rules spelling `last`, `all`, `max`, `min`, a listed `as` with no pick
  (read as `top`) and a chain; a transient marker on a concept a rule
  concludes and on one no rule concludes; a provider row; a fact whose
  value is a concept identity, as tonk writes; and a second replica's
  head that diverged from the first after a shared commit.
- **What is committed** is what 0.5.0 wrote to disk, the space's
  directory, not facts replayed through current code: the test opens it
  with the release under test, as a user's device would. A JSON listing
  of the same facts sits beside it for review. Signing keys are fixed
  seeds; the clock is not, so the bytes are not identical between runs,
  and the committed bytes are the fixture, with the README's program as
  the way to make an equivalent one.
- **Lives** beside the tests that read it, under
  `rust/dialog-repository/src/repository/branch/fixtures/v0.5.0/`,
  committed with the tests of plan step 2. It is test input, as
  `v0.4.0.json` is, not a research artifact; nothing goes to
  `research-data`. Its size is to be kept to tens of kilobytes; if it is
  larger, that is the reason to move it.
- **Checked against it, at every step:** before migration, every rule
  listed inert and the count of what each relation derives; after
  `Branch::migrate`, the same counts, every rule read, the fence set, a
  second run doing nothing; a migration interrupted before its publish
  leaving the head unfenced and resumable; the diverged head pulled
  after the fence, its rules rewritten; and v0.5.0 itself, from the same
  checkout, refusing the fenced tree by name.

## What tonk #1329 implies

It is the tonk half of this plan: the same move, from 0.5.0 to the next
release, through tonk's `upgrade_definitions` and `upgrade_rules`. What
has to be agreed with it before either side builds:

1. **One entry point, one publish.** Tonk's definitions step, its rekey
   and dialog's `upgrade_rules` and fence go into one `Branch::migrate`
   publish, the fence last. #1329 should not commit its own steps
   separately.
2. **The gate is the fence, not the storage encoding.** `migrate_once`
   should ask the fence (through `NeedsMigration` or a read of it), since
   a 0.5.0 space needs no storage rewrite and the encoding says nothing.
3. **What dialog exports**, step 1: every generation's attribute and
   concept identity, including the 0.5.0 identity of an attribute read
   under a ranking pick on its own, and the generation constant.
4. **The reversal** of listed `as` and `top` to `max` happens in #1329's
   reinstall for tonk's three lists, and in `rewrite_body` for the rest,
   with explicit `pick: "max"` where tonk relied on the implied `top`
   (T15).
5. **Sync after migration is dialog's**, in the pull (above). #1329 can
   drop the gap from its list once step 4 lands.
6. **Tested against** the v0.5.0 fixture on dialog's side, and a tonk
   space the shipping tonk binary wrote on tonk's.
