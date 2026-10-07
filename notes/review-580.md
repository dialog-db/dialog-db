# Review of dialog-db #580: rules derive attributes, select policies, overlay, policy writes

Reviewed at head `6c59c91`. Every claim below is pinned by a test on branch
`claude/pr-580-review-assessment-jkgwxr` (PR head plus test commits only). Each test states
a claim from `notes/attribute-heads.md`, the changelog or the PR body, in its doc comment.

| Where | Tests | Fail on head | Pass |
|---|---|---|---|
| dialog-query `concept/query/invariants.rs` | 9 | 7 | 2 |
| dialog-query `rule/invariants.rs` | 3 | 3 | 0 |
| dialog-repository `transaction/invariants.rs` | 10 | 10 | 0 |
| dialog-repository `transaction/migration.rs` | 8 | 5 | 3 |
| dialog-repository `subscription/invariants.rs` | 1 | 0 | 1 |
| dialog-artifacts `history/invariants.rs` | 2 | 2 | 0 |
| dialog-perf `compare.rs` | 4 | 4 | 0 |

A passing test pins a claim that holds, so a later change cannot break it silently. The
migration tests replay facts captured from a `main` build (`transaction/fixtures/`), not
this release's encoding of the same rules, which is what the PR's own legacy tests use.

```
cargo test -p dialog-query      --features dialog-peer/helpers --lib invariants
cargo test -p dialog-repository --features dialog-peer/helpers --lib -- transaction::invariants transaction::migration subscription::invariants
cargo test -p dialog-artifacts  --lib history::invariants
cargo test -p dialog-perf       --lib
```

## Fix status

Fixed on this branch, each with its test now passing:

| Defect | Fix |
|---|---|
| 1, 3: rules the previous release installed | Permanent legacy path removed (conclusion index, legacy scan, commit-time indexing, byte-hash acceptance). `Branch::upgrade_rules` re-installs them; idempotent, convergent, re-runnable when an old replica reintroduces them. |
| 2: attribute and concept identities | The identity hashes `{domain, name, cardinality, type}` as `main` did, adding `select`, `then`, `among` only when they say more. Plain attributes, concepts and transient markers keep their identities. |
| 4: covering rule under `last` | The covering rule answers attribute concepts only, whose rows it elects; a multi-field concept selects and joins. |
| 6 (part): recursive relation under `last` | The component's exit elects under `last` too, so a reader sees one value. |
| 7: bulk anti-join | The key covers every variable any candidate binds; a candidate missing one falls back to per-candidate evaluation. |
| 8: plan cache | A head's spelling pairs each field name with its relation. |
| 9: forged rule facts on the query path | Every hydration checks the content address, on reads as at commit. |
| 11: held losing value | Tree and transactor elect over every claim, the held one included. A write that does not re-elect the held claim succeeds the winner and folds the commit's version into the held claim. |
| 13: batch order | `Changes::associate` drops an earlier `last` write only when it is the cell's only assertion in the batch. |
| 14: transactor tie-break | A staged claim stands with no cause, as every reader sees it. |
| 16: overlay in a join | An overlay row stands past every joined line's head. |
| 17: same-batch history record | A claim succeeded within its batch gets a retraction record; the supersession it carried survives the fold. |
| 19: perf gate | Counters are compared over both reports' keys, a zero baseline has no slack, a missing baseline or scenario fails, and every commit path counts a `commit` step; the commit-side settlement counts `settle`. Counter baselines refreshed; instruction baselines need the CI toolchain's sweep. |

Re-diagnosed: **18** is the perf harness, not the engine. Opening a repository mints a
fresh account key, its encrypted secret and a delegation into the tree; the facts the
scenario's commits write are identical run to run. The harness needs a seeded account key.

Open, each needing a decision:

- **12, transaction reads differ from the commit.** Needs one settlement engine. Proposed:
  settle the log incrementally on first read or at commit, extending a memoized settled
  prefix with only the new writes, against the line plus that prefix: the commit's own
  algorithm, so reads and commit share one result and each write is settled once. It
  replaces the PR's per-cell read settlement; tonk's bootstrap is the cost to measure.
- **15, a transaction retracting an overlay row.** Either the commit also retracts the row
  from the session overlay, or the transaction refuses a retraction only the overlay holds.
- **5, absence tests inside a cycle.** Options: refuse per component and report; evaluate
  the negation against stored facts and lower strata only; a well-founded semantics.
- **6 (rest), standings through the fixpoint.** A derived fact's standing must be the
  maximum over its derivations, propagated as its own fixpoint, before `last` can pick the
  newest candidate of a recursive relation; until then it picks by value.
- **10, field names in identity.** Not blocking.

## Decided since the first review

Rules derive attributes and concepts select them. A multi-field head is a set of
independent attribute facts sharing a body, so a concept over derived attributes reads the
join, cross product included. The test `a_concept_read_under_all_joins_the_attributes_a_rule_derives`
pins that and passes. Defect 3 below is restated against that decision: the covering-rule
path is the bug, not the attribute semantics.

## Corrections to the first review

- A rule the previous release installed **does** feed commit-time induction: the deductive
  body is not dropped from the closure (`a_rule_the_previous_release_installed_feeds_induction`
  passes). The inductive rule is the one that goes inert.
- A cardinality-one attribute read and a `last` concept read elect the same claim among
  equal standings (`every_last_read_of_a_cell_elects_the_same_claim` passes). The claim
  that `only.rs` breaks ties differently was wrong. The transactor's tie-break (defect 9)
  stands.
- Not every defect is blocking. Defects 11 and 14 and the gaps list are not; the table in
  "Blocking" says which are.

## Defects, ranked

Each entry names the failing test. "Blocking" means it loses data, returns wrong rows, or
breaks a replica on upgrade.

### Migration (blocking: none of this can reach a replica with existing data)

**1. An inductive rule the previous release installed never fires again.**
`migration::a_rule_the_previous_release_installed_fires_at_commit`, and
`rule::invariants::a_rule_stored_by_the_older_release_is_recognised_under_its_stored_identity`.
`stored_as` (dialog-query `rule/deductive.rs:456`, `rule/inductive.rs:171`) accepts the
legacy identity as the hash of *this release's re-encoding* of the decoded rule. `main`'s
bytes spell `cardinality: one` and `description: ""` on every field (see the fixture); this
release's re-encoding spells neither. The hash never matches, and commit-time hydration
(`induce.rs:655`) drops the rule as forged. Fix: hash the stored bytes at the hydration site.

**2. Every attribute and concept identity changed, and a transient concept becomes durable.**
`migration::an_attribute_and_a_concept_keep_the_identities_the_previous_release_gave_them`,
`migration::a_concept_the_previous_release_marked_transient_stays_transient`.
The identity hash moved from `{domain, name, cardinality, type}` to
`{domain, name, then, type, select, among}` under a comment that still says identities are
preserved. A `dialog.concept/transient` marker `main` wrote sits under the old concept
entity, so the command concept's facts land on the branch after upgrade.

**3. Retracting a rule the previous release installed does not uninstall it.**
`migration::retracting_a_rule_the_previous_release_installed_uninstalls_it`.
`transaction().retract(&rule)` writes under the canonical identity; `main` installed under
the byte hash. The installed facts stay and the rule keeps deriving. tonk's reconcile
retracts by recorded provenance and is unaffected; any other caller is not.

### Rules and concepts

**4. Under `last`, a concept over attributes one rule derives returns two values per entity
until any fact lands under one of those attributes.** (blocking)
`concept::query::invariants::a_concept_read_under_last_elects_one_value_per_attribute`,
`...a_fact_about_another_entity_does_not_change_a_derived_concepts_rows_under_last`.
With attribute heads decided, `Member { group, role }` for alice under `last` is one row:
each attribute's newest value, `(g1, viewer)`. While nothing is stored under either
attribute, the covering rule (`concept/query.rs:565`) answers instead, runs no election for
a multi-field concept, and returns the body's two rows. A stored `member/role` for *bob*
flips alice to the election path (`stored_absent` is an entity-free scan, `query.rs:1069`).
The covering rule is a performance shortcut that is only valid when it yields what the
election yields; it should run the per-field election over its rows, or be dropped.

**5. The cycle policy deletes a negated premise, stored facts included, and an unrelated
install changes a rule's meaning.** (blocking)
`...a_negation_inside_a_cycle_still_sees_stored_facts`,
`...a_stratified_negation_keeps_its_meaning_when_another_rule_closes_a_cycle`.
`p(x) :- q(x), unless r(x)` with a *stored* `r(b)` and `r(x) :- p(x)` derives `p(b)`.
With `p` alone, `p = {a}`; installing `s :- p` and `r :- s` changes it to `{a, b}`.

**6. A recursive relation read under `last` returns every candidate.** (blocking)
`...a_recursive_relation_read_under_last_yields_one_value_per_entity`,
`...a_derived_value_stands_by_the_fact_that_bound_it_through_a_fixpoint`.
The exit skips the election under `last` (`query.rs:525`) and the fixpoint drops standings
(`fixpoint.rs:332`), so a cardinality-one field reads all ancestors, and a self-reading rule
makes a `last` read return the stored and the derived value both.

**7. The bulk anti-join keys on the first candidate alone.** (blocking)
`...a_bulk_negation_keys_on_the_variables_each_candidate_binds`. With an optional premise
before `unless` and the first of twenty candidates absent there, one banned nickname
excludes everyone.

**8. The plan cache hands one spelling of a rule another spelling's plan.** (blocking)
`rule::invariants::a_plan_is_cached_by_the_working_spelling_it_was_planned_for`.
`spelling()` hashes sorted operand names only, so two working spellings that pair those
names with different attributes share a plan that binds the wrong premise.

**9. Rule facts under an entity that is not their address derive on the query path.**
`migration::rule_facts_under_an_entity_that_is_not_their_address_derive_nothing`.
The commit path checks the content address; the query path hydrates without it. The note
says such bytes "stay inert". Pre-existing on `main` per the sub-review; not re-checked
there. Blocking only if rule facts can arrive from an untrusted writer.

**10. Premise field names are part of a rule's identity.** (not blocking)
`rule::invariants::a_premise_field_name_does_not_distinguish_a_rule`.

### Writes and transactions

**11. A choosing write of a value the cell holds does nothing, even when that value is a
loser.** (blocking)
`transaction::invariants::a_last_write_of_a_value_the_cell_holds_reads_back_as_the_newest`,
`...a_transaction_reads_back_its_own_last_write_of_a_held_value`,
`...a_max_write_of_a_held_value_succeeds_the_claim_a_max_read_returns`.
See "The held-value question" below.

**12. A transaction does not read what its commit leaves.** (blocking)
`...a_transaction_reads_a_last_write_as_succeeding_the_stored_claim_under_a_rule`,
`...a_transaction_reads_what_its_commit_leaves_when_a_derived_input_follows_the_write`,
`...a_write_succeeds_what_a_reader_after_the_earlier_writes_observes`.
Two settlement engines over two views. Where any rule derives the relation, the read feeds
the write's own staged value back as a derived candidate and retracts nothing; the commit
retracts. The author confirmed the split.

**13. An integrated `Changes` batch lands differently from the same writes asserted in
order.** (blocking)
`...an_integrated_batch_lands_as_the_same_writes_asserted_in_order`,
`history::invariants::a_batch_lands_a_cells_writes_in_the_order_they_were_recorded`.
`Changes::associate` under `Last` drops the earlier `last` write and appends the new one.

**14. The transactor breaks `last` ties by fact hash; every reader uses value bytes.**
(blocking) `...a_last_write_succeeds_the_claim_a_last_read_returns_among_equals`.
Four of eight value pairs retract the claim a reader would not have returned.

**15. A transaction's retraction of an overlay row reads as gone and commits as nothing.**
(blocking) `...a_transaction_that_retracts_an_overlay_row_reads_what_its_commit_leaves`.

**16. In a join, a line's overlay row stands below that line's own commits.** (blocking for
joins) `...an_overlay_row_stands_above_its_own_lines_commits_in_a_join`. The overlay's
standing comes from the first line's head (`session.rs:608`, `:823`).

**17. A claim succeeded within its own batch leaves a standing history record.** (not
blocking) `history::invariants::a_claim_succeeded_within_its_own_batch_leaves_no_standing_record`.

### Perf gate and determinism

**18. Identical commits from a fresh repository mint different trees.** (blocking for the
gate; worth knowing for sync) `compare::counted::the_same_commits_mint_the_same_head`.
Six runs of the same four commits produced three different tree roots. The PR says "two
runs write the same records". Whether `main` does the same was not checked.

**19. The gate cannot see what it was written for.** (not blocking for the engine)
`compare::tests::a_step_the_baseline_never_took_is_a_regression`,
`compare::tests::a_read_that_starts_writing_is_a_regression`,
`compare::counted::a_branch_commit_counts_a_commit_step`. A counter the baseline lacks is
never gated, a zero baseline gets the block slack, and a branch commit counts no `commit`
step. The job is also not a required check.

## Claims pinned that hold

- A concept over derived attributes reads their join under `all`.
- `main`'s deductive rule derives on the query path, feeds commit-time induction, and a
  choosing write beside its derived candidate retracts nothing.
- A rule landing on a subscribed attribute wakes the subscription (the PR deleted the only
  earlier test of this).
- Every `last` read of a cell elects the same claim among equal standings.

## The held-value question

The intent: under `last`, a write succeeds the winner, and every challenger the writer saw
is causally behind the new fact, so it cannot win. The PR breaks that in one case. The tree
keys a claim by `(entity, attribute, value)`. When the written value already exists as a
live claim, both the tree (`tree.rs:1496`) and the transactor (`succession.rs:586`) return
before the election and write nothing.

Concretely: replica A writes 100, replica B concurrently writes 200, they merge, and both
claims are live; 200 wins on standing. A writes 100 again under `last`. Per the intent, 200
is retracted and 100 becomes the newest fact. In the PR nothing happens and 200 keeps
winning. The same shortcut applies under `max`, `min` and `top`.

To honour the intent, the existing claim of 100 must carry the new write's version, so its
standing moves past 200, with 200's versions recorded as its cause. The tree datum already
carries a *set* of versions (`current_element.versions()`), so this is adding one version
to an existing claim plus a history record. The PR's comment says it avoided this so as not
to "fork the claim's lineage". Answering "may a re-assertion add a version to a claim it
already holds?" decides defect 11. Under the stated intent the answer is yes.

## Gaps: confirmed by reading, not pinned

- A keyed collection read under a choosing policy elects one entry per entity, not per key.
  Whether that is wrong depends on what a choosing policy over a collection means, which
  the note does not say.
- A same-commit uninstall that retracts `source` but not `conclusion` leaves a `derives`
  fact that answers "derived" for that relation forever.
- `Changes::iter` walks hash maps, so the cross-cell order of an integrated batch is
  random; it matters only when the batch installs a rule beside a write to its relation.
  Not pinned because a nondeterministic test would be flaky by design.
- `negate()` now buffers the whole candidate set.
- Dangling intra-doc links (`Snapshot::commit`, `DeductiveRule::variants`); dialog-perf uses
  ```text fences against the repo's doc-example rule; dialog-perf carries an unused
  `futures-util`.
- `last` over a multi-valued cell is excluded from the artifact fuzz on purpose.

## Design assessment

Attribute heads are a sound primitive for dialog, and the user confirms they were the
intent. They make derived facts behave like stored ones. A rule wanting a pairing kept
derives an entity carrying both parts, as the note says. The implementation does not yet
follow that decision consistently: the covering rule (defect 4) and the one-body memo
exist to recover tuple behaviour and are where the semantics leak. With attribute heads
fixed, the covering rule can only be an optimisation whose output equals the election's.

Two decisions remain open, and the code should follow them rather than lead:

- **May a re-assertion refresh a held claim's standing?** Under the stated intent, yes
  (see above). That removes defect 11 and tonk's retract-everything-else workaround.
- **What does an absence test inside a recursive component mean?** The cycle policy as
  landed deletes the premise, stored facts and all, and lets an unrelated install change a
  rule's meaning. Options that keep merge-determinism: refuse per component and report it;
  evaluate the negation against stored facts and lower strata only; or a real well-founded
  semantics.

The transaction contract ("a commit not yet flushed") is keepable only with one settlement
engine: settle each write when it is made, against the line and the staged prefix, and let
reads and the commit share that result.

The author's recommendation, accepted here: do not merge #580 or tonk#1058 until the
migration defects are fixed against real `main` fixtures and the two open questions above
are decided.
