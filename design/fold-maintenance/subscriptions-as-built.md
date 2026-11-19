# How a subscription is kept current today

A reading of the code on `main` at `8ce5f37` and of `origin/feat/notation-poc`, made on 2026-11-19 by a research thread for this topic. Nothing was built or run. Every statement is tagged READ (the code that does it was read), TESTED (a named test pins it) or INFERRED (a reading that could be wrong). Paths are under `rust/`; `sub.rs` is `dialog-repository/src/repository/branch/subscription.rs`.

## 1. Surface
- `Branch::subscribe<Q: Application>(&self, query: Q) -> Subscription<Q>` (sub.rs:443). `Stack::subscription(query) -> Subscription<Q>` (sub.rs:464) is the unshared one. Both go through `Subscription::over(branch, Option<Stack>, query)` (sub.rs:472). READ
- `Stack::subscribe<Q>(&self, query) -> Subscriber<Q>` (`dialog-repository/src/stack/subscribe.rs:317`) is a registry plus delivery layer over that same `Subscription`.
- It is keyed by `QueryHash::of(&query)` and `TypeId` (subscribe.rs:324-338). It needs `Q: Serialize`.
- It is polled once per `Stack::settle` (subscribe.rs:411).
- `Subscriber::poll() -> Vec<Delta<Q::Conclusion>>` (subscribe.rs:459). The first delivery is the whole set as `asserted` (subscribe.rs:32-38). READ. Test: `it_shares_one_evaluation_among_the_readers_of_a_query`, `it_hands_a_late_reader_the_result_set`. TESTED
- `Subscription::poll(&mut self, env) -> Result<Option<Delta<Q::Conclusion>>, EvaluationError>` (sub.rs:761). It needs `Q::Conclusion: Conclusion + PartialEq + Clone` (sub.rs:708). `Conclusion` has only `fn this(&self) -> &Entity` (`dialog-query/src/concept.rs:382`). READ
- One poll returns a change, not the whole answer (sub.rs:371-377). READ:
```rust
pub struct Delta<T> { pub asserted: Vec<T>, pub retracted: Vec<T> }
```
- `Ok(None)` means known unchanged.
- The first poll asserts every row (sub.rs:742-751).
- A row whose field changed shows up as a retraction of the old row plus an assertion of the new one. TESTED by `it_transitions_optional_max_between_present_and_absent` and `it_updates_depth_two_consumer_subscriptions`.

## 2. State (sub.rs:388-430, READ)
- `revision: Option<Revision>`, the pinned head.
- `overlay_sequence: u64`, the session overlay's ring position.
- `memory_sequences: Vec<u64>` and `durable_heads: Vec<Option<Revision>>`, used only through a stack.
- `demand: Demand`: disjoint `BTreeMap<Key,Key>` ranges for facts and, separately, for rules. It also holds the manifest the ranges are keyed under and a `head` flag for reads of `dialog.branch/tree|edition|revision` (sub.rs:120-150, 156-160).
- `results: Vec<Q::Conclusion>`: the whole last answer as a flat Vec. There is no index, by entity or otherwise.
- `fixpoint: Arc<Mutex<Option<InMemoryAnswerTable>>>`, only for a recursive concept.
- `recomputes` and `maintenances` counters.
- `window` = 16 (sub.rs:113).
- Nothing is kept per rule, per premise, per group or per derived relation. INFERRED from that field list.

## 3. Deciding what to re-read
The common gate in `poll` (sub.rs:789-845), READ:
- Nothing moved: `None`.
- A durable layer moved, or the head moved and the query read a head attribute: full recompute.
- Otherwise the poll classifies the change as a `Touched`:
- **Tree:** `touched()` (sub.rs:993-1108) runs `differentiate_within(&next, &demand.ranges())`. It reconstructs each datum's `Fact` (fetching spilled values), dedups across the three index orders, and returns `Touched::Rules` at the first key inside a rule range (sub.rs:1065).
- **Overlay and memory layers:** `touched_by()` (sub.rs:505-553) filters the instants' facts by their E/A/V keys against the cover.
- What each classification does:
- `Nothing`: the pins advance silently.
- `Rules`: full recompute.
- `Facts{subjects, asserted, retracted}`: `maintain()` (sub.rs:1136).

Inside `maintain`:
- **Non-concept query** (`query.concept()` is None): the entities are the changed subjects (sub.rs:1263). Each one is `restrict`ed (sub.rs:1280-1286). READ
- **Concept query** (sub.rs:1176-1261):
1. `SelectRules` for the concept.
2. `elected_attributes` (`dialog-query/src/concept/query/affected.rs:101`).
3. `widened` (sub.rs:564).
4. If `rules.recursion()`, the fixpoint continuation runs. Otherwise `affected_entities(concept, asserted, retracted, after_env, || before_env)` (affected.rs:294) runs.
5. Then `restrict` each entity, evaluate the restrictions together (a window of 16 at a time), `adopt` the rows, and diff them against the retained rows that share `this()`.
- `Restriction` (`dialog-query/src/query/application.rs:17`) is `Scoped(Q) | Unaffected | Unsupported`. The default is `Unsupported` (application.rs:63). READ

Case by case:
- **(a) Plain concept query** (one concept, implicit rule, entity-local): `affected_entities` takes the changed subjects whose attribute some premise reads (affected.rs:377-392, `reading_attribute` at affected.rs:473). `ConceptQuery::restrict` pins `this` (`dialog-query/src/concept/descriptor.rs:813-841`). The macro-derived `Query<C>` does the same (`dialog-macros/src/query/concept.rs:423`). READ. Tests: `it_maintains_covered_writes_without_recompute`, `it_maintains_derived_concept_subscriptions`, `it_scopes_maintenance_to_touched_entities`. TESTED
- **(b) A rule joining two concepts:**
- Not `is_entity_local` (that test admits no concept premise): goes to `rule_heads` (affected.rs:503).
- Each concept premise binds its `this` from the target's already-computed `Heads`, bottom-up over the acyclic concept graph (affected.rs:355-405, 573-603).
- Each attribute premise binds the changed fact.
- Then `sideways_heads` plans the rest of the body and projects `?this`. It short-circuits when the bound premise already binds `?this` (affected.rs:652). The join runs on the `Before` env (pinned revision plus rewound overlay, sub.rs:935) or the `After` env, by side (affected.rs:16-34, 555-563).
- READ. Tests: `it_maintains_cross_entity_conformance`, `it_maintains_deeply_nested_conformance`, `it_retracts_a_row_when_two_supporting_facts_go_together`, `it_reaches_a_head_two_retracted_facts_supported` (affected.rs:783). TESTED
- **(c) `unless`:** the same path. A negated premise flips the side, through `Bind::Exact{negated:true}` and `Side::derivation` (affected.rs:211, 460-465, 581). READ. Tests: `it_reaches_a_head_two_asserted_facts_negated` (affected.rs:854), and the randomized `a_maintained_subscription_matches_a_fresh_evaluation_after_random_changes` (`subscription/invariants.rs:1604`, covers `Unvouched`). TESTED
- **(d) `QueryLayer::with` (a rule laid over one query):**
- Neither main nor the stack has a subscribe on `QueryLayer`. A subscription builds its own `QueryLayer::from(&self.branch)` at every evaluation (sub.rs:1181, 1373). READ
- The stack's `rust/dialog-notation/tests/view.rs:6-13` says so and asserts the rule into the session overlay instead.
- A rule fact landing in the overlay inside a rule range is `Touched::Rules`, which recomputes (sub.rs:521). READ. Test: `a_rule_installed_through_the_overlay_moves_the_subscription` (invariants.rs:1895). TESTED
- **(e) Electors (`last`, ranked picks):**
- `elected_attributes` gathers the cardinality-one attributes that reachable rules read. It returns empty when every rule reads only `?this` and nothing recurses, because re-deriving the entity re-elects (affected.rs:97-163).
- For each touched cell under an elected attribute, `widened` reads the whole cell `(the, of)` through the after env. It counts every current claim as asserted and every prior claim as retracted, so re-derivation settles which claim the pick elects (sub.rs:555-615). READ
- Tests: `it_updates_an_electing_subscription_on_asserts`, `..._on_retractions`, `it_maintains_a_ranked_choice_flip` (asserts `recomputes()==1`, `maintenances()==1`), `retracting_a_committed_winner_restores_the_value_it_outvoted`. TESTED
- **(f) Optional reads:**
- An optional premise gets `Bind::Optional`. It is joined on both sides, binding the subject only and re-reading the value on each side (affected.rs:28-31, 422-433, 454-459). READ. Tests: `it_reaches_a_head_an_optional_field_of_a_target_moves` (affected.rs:975), `it_reaches_a_head_an_optional_premise_over_another_entity_moves` (affected.rs:1058). TESTED
- A top-level `OptionalAttributeQuery` has no `restrict` (`dialog-query/src/optional.rs:222-235`), so every covered change recomputes. READ
- **(g) Keyed collection read:**
- A `ConceptQuery` whose field reads a variable `the` under a domain prefix (`ordered_query`, sub.rs:2259). Its implicit rule reads `of ?this`, so it is entity-local and the affected entities are the subjects. INFERRED
- The cover excludes the other half of the domain. TESTED by `it_ignores_the_other_half_of_a_mixed_domain`, `it_maintains_a_collection_subscription`, `it_retracts_a_member_from_a_collection_subscription`. These tests do not assert the recompute or maintenance counts (`baseline` is unused, sub.rs:2372, 2412).

## 4. What stopped holding
- A retraction or succession arrives as a `Fact` in `retracted`, or in both lists after `widened`.
- The subscriber learns a row stopped only by re-derivation:
```rust
let before: Vec<_> = self.results.iter().filter(|row| row.this() == entity).cloned().collect();
... for row in &before { if !after.contains(row) { retracted.push(row.clone()); } }
self.results.retain(|row| row.this() != entity); self.results.extend(after);
```
(sub.rs:1309-1330). READ
- So it never reads the whole answer, but it always re-evaluates the touched head entity's restricted query, which reads that entity's slice through every rule. For a non-local rule it also runs a sideways join on the before state to find the head. READ
- Under cardinality one, the whole cell is read as well (`widened`). READ
- No signed row comes out of the engine. `affected_entities` returns entities whose `before`/`after` sides are merged by `into_union` (affected.rs:245-249, 407). The sign is recovered by set difference against the retained rows of the same `this()`. READ
- A recompute or recursive poll diffs the whole old Vec against the whole new one with `contains` (sub.rs:851-863, 1220-1232). READ

## 5. Cost of one changed fact, and every give-up
Cost, INFERRED from READ code:
- The diff walk is bounded by changed subtrees inside the cover (sub.rs:977-983), plus a spilled-value fetch per datum.
- `SelectRules` is called per reachable concept, twice (`elected_attributes` and `affected_entities`).
- For each touched cell under an elected attribute, one cell scan.
- For each non-local rule × change × matching premise × side, one planned sideways join; its cost is that of the rule body with what the change bound.
- For each affected entity, one restricted evaluation, which costs that entity's slice through the rule's body.
- In memory, an O(|results|) scan per entity (sub.rs:1312-1317, 1328) plus quadratic `contains` within the entity's rows.
- The answer size enters only through those Vec scans, except on a recompute.

Full re-evaluation is triggered when:
1. A durable stack layer moved (sub.rs:805; `durables_moved`).
2. The head moved and `demand.depends_on_head()` (sub.rs:805). TESTED: `it_refires_head_dependent_subscriptions_on_commit`.
3. `Touched::Rules`:
 - a tree diff key inside a rule range (sub.rs:1065);
 - an overlay or memory fact inside a rule range (sub.rs:521);
 - the overlay ring no longer reaches the pin (sub.rs:1117-1118; `LOG_CAPACITY` 1024 in `ephemeral.rs`). TESTED: `it_recomputes_when_the_session_ring_is_exhausted`;
 - a memory ring no longer reaches its pin (sub.rs:963).
4. `maintain` returns `None` (sub.rs:837):
 - `Restriction::Unsupported` for any entity (sub.rs:1284). This happens when `ConceptQuery`'s `this` shares a variable with another field (descriptor.rs:820-829), or `DynamicAttributeQuery`'s `of` shares one (`dialog-query/src/attribute/query/dynamic.rs:256-270`). It also always happens for `AttributeQueryAll`, `AttributeQueryOnly`, `StaticAttributeQuery`, `OptionalAttributeQuery` and `ResolverQuery`, which have no `restrict` (grep: only `dynamic.rs`, `descriptor.rs` and the macro implement it).
 - `affected_entities` returns `None` when:
   - a reachable concept recurses (affected.rs:329);
   - **any reachable bundle has a reducing rule** (affected.rs:338);
   - the graph is not ready (affected.rs:370);
   - no before-state exists (affected.rs:556, 594): `before()` is `None` with no pinned revision or a ring past its pin (sub.rs:935-953). TESTED: `it_recomputes_when_a_join_needs_the_state_before_an_unborn_branch`;
   - a target is missing from `affected` (affected.rs:574);
   - a concept premise has no `this` term (affected.rs:577);
   - the sideways plan fails (affected.rs:656);
   - a joined row leaves `?this` unbound or not an entity (affected.rs:669).
5. **Recursive root concept:** `maintain` always returns `Some` and counts a maintenance (sub.rs:831, 1208-1234). Yet it performs the whole unrestricted query (sub.rs:1453), and `Continuation::rows` rebuilds the whole fixpoint table when `retract` or `extend` decline (`dialog-query/src/concept/query/fixpoint.rs:1767-1810`). They decline on a reducing member (fixpoint.rs:1251, 1411), a negated or optional premise a change matches, or a non-local nested concept (fixpoint.rs:1225-1229, 1385-1391). So `maintenances` does not mean "not re-evaluated whole" for recursive concepts.

## 6. Folds
- `Reduce { groups: Vec<String>, entries: Vec<ReduceEntry> }` (`dialog-query/src/reduce.rs:330`).
- `Reduce::fold(selection) -> Vec<Match>` (reduce.rs:360-410) consumes the whole selection. It keeps a `BTreeMap<dag-cbor key bytes, Group{key, columns: Vec<Vec<Value>>}>`, buffering every present input value per entry. Then `fold_column` folds each column (reduce.rs:442; sum, avg and extremum at reduce.rs:462, 507, 551). There is no incremental state and no inverse operation. READ
- `DeductiveRule::reducer()` groups by `this` plus every non-reduced head field (`dialog-query/src/rule/deductive.rs:237-252`). So a folded row's `this()` is a group-key component. READ
- How a fold is evaluated:
- Reducing rules are left out of the plan disjunction (`dialog-query/src/concept/query/rules.rs:347, 402`).
- `evaluate_unsorted` calls `reduce_rows(rule, env)` for each reducing rule (`dialog-query/src/concept/query.rs:636-637`). That function plans the body at empty scope, folds it, projects it, and the caller's bindings join against the result (concept/query.rs:1549-1567):
  > "the body plans and evaluates at *empty* scope (the fold must see the full relation, never a caller-restricted slice) ... Recomputed per query — incremental maintenance is milestone A5."
- READ. Consequence, INFERRED: a `restrict`ed query pinning `this` to one group still folds every group.
- In a recursive component, a seed reducing rule folds its body once at the start (fixpoint.rs:1030-1039).
- On main and on the stack, an authored reducing deductive rule is refused: `open_rule_error` returns `ReduceInOpenRule` (`dialog-query/src/rule.rs:190-203`, applied at rule.rs:262-267). `with_reduce` is authored (deductive.rs:115-129). READ
- The spike (branch `spike/named-fold`, `0c41ee8` and `de5ee74` over `54b1fc4`):
- It lifts that refusal and quarantines a cycle through a fold. Its new `transaction/fold.rs` tests installation and quarantine only.
- Its `subscription.rs` diff adds tests only, no logic.
- `it_updates_a_subscription_over_a_fold_as_its_groups_change` (a group gains a row, a new group appears, a group loses a row, a group empties) asserts `(recomputes, maintenances) == (5, 0)`, "every poll re-derived the whole query". `it_updates_a_subscription_over_a_reader_of_a_fold` asserts only the deltas. TESTED, in the spike.
- The path, INFERRED from READ code: a `Staffed` write lands in the cover (the empty-scope body scan recorded whole attribute ranges), giving `Touched::Facts` → `maintain` → `affected_entities` returns `None` at affected.rs:338 → full recompute → `reduce_rows` folds the whole relation → the Vec diff gives the old group row as retracted and the new as asserted.
- Where a fold would have to hook (INFERRED): there is no operator state anywhere, and maintenance works by entity re-derivation. The only places that see changes are:
- `affected_entities` (affected.rs:332-340), which knows only changed base facts and head entities;
- `maintain`'s per-entity loop (sub.rs:1279-1330);
- `Continuation` (fixpoint.rs:1727), the one existing precedent of state kept across polls, held in the `Subscription` and attached through the query env (sub.rs:1383-1386, 1444-1449).
- The fold itself needs the body's rows that changed, signed, per group key. None of the current layers produces that: `affected_entities` yields entities, and re-derivation yields whole rows for an entity.

## 7. Derived rows
- Changes to rows a rule derived are never materialized as changes. Within `affected_entities`, a lower concept's effect travels as `Heads { before: BTreeSet<Entity>, after: BTreeSet<Entity> }` (affected.rs:227-231). That is a candidate entity with the side its derivation is on ("may have lost / may have gained"), not a row and not a weight. READ
- Only the subscribed root's rows are compared, by set difference.
- Under a subscription, conclusions are `Q::Conclusion` values held whole in `results`. For concept queries that is `ConceptConclusion { this, terms, source: Match }`, put into the open query's shape by `adopt` (descriptor.rs:854-867). READ
- Inside recursion, derived rows live in the `InMemoryAnswerTable`. The continuation's retract and extend seed from changed base facts (`with_changes(additions, deletions)`, fixpoint.rs:1757). READ

## 8. Evidence
**Tests in sub.rs** (start lines):
- 2003 `it_evaluates_on_first_poll_and_idles_after`
- 2046 `it_ignores_writes_outside_the_demand_cover`
- 2107 `it_ignores_the_other_half_of_a_mixed_domain`
- 2160 `it_emits_a_new_member_of_the_demanded_half`
- 2207 `it_does_not_trip_the_head_gate_on_a_domain_scan`
- 2354 / 2420 collection tests
- 2642 `it_emits_deltas_for_writes_inside_the_cover`
- 2714 `it_invalidates_absence_reads`
- 2764 `it_maintains_covered_writes_without_recompute`
- 2842 `it_rederives_surviving_rows_on_partial_retraction`
- 2895 `it_scopes_maintenance_to_touched_entities`
- 3158 `it_maintains_derived_concept_subscriptions`
- 3216 `it_maintains_cross_entity_conformance`
- 3314 / 3388 `it_retracts_a_row_when_two_supporting_facts_go_together[_in_the_overlay]`
- 3612 `it_recomputes_when_a_join_needs_the_state_before_an_unborn_branch`
- 3700 `it_ranks_many_candidates_by_relation`
- 3767 `it_maintains_a_ranked_choice_flip`
- 3826 `it_grows_the_demand_cone_with_recursive_rules`
- 3992 `it_maintains_deeply_nested_conformance`
- 4134 / 4217 electing asserts / retractions
- 4305 `it_refires_head_dependent_subscriptions_on_commit`
- 4374 `it_transitions_optional_max_between_present_and_absent` (an elector `max:`, not a fold)
- 4496 `it_updates_depth_two_consumer_subscriptions` (also an elector)
- 4578 `it_elects_over_a_recursive_relation_at_its_exit`
- 4707 – 5048 overlay and session maintenance
- 5135 `it_agrees_with_a_fresh_evaluation_through_overlay_churn`
- 5266 `it_maintains_rows_in_the_shape_a_recompute_gives_them`
- 5405 `it_maintains_a_window_of_entities_as_it_does_one_at_a_time`
- 5623 `it_keeps_its_results_when_a_rederivation_fails`

**Other tests:**
- `subscription/invariants.rs`: 47, 136, 1604 (randomized against a fresh evaluation; seeds through `DIALOG_SUBSCRIPTION_SEEDS`), 1660, 1702, 1744, 1824, 1895, 1936.
- `affected.rs`: 783, 854, 975, 1058, 1134.
- `stack/subscribe.rs`: 575 – 817.
- No test on main subscribes over a reducing rule. The aggregation note's "Subscription lifecycle (A3 acceptance)" items (`notes/aggregation.md:159-167`) exist only as the spike's two tests.

**Perf** (`dialog-perf/src/scenario/subscribe.rs`, registered at `dialog-perf/src/scenario.rs:300-322`):
- `subscribe-repoll` (size 1000): four status rules (`scenario/rule.rs:189`). A commit promotes 10 accounts, then one poll. The outcome records asserted, retracted, recomputes and maintenances. The baseline is `perf/baseline/subscribe-repoll.json`: 10, 10, 1 recompute, 1 maintenance.
- `subscribe-fanout` (200): one subscription per note plus one to the list. A commit adds a note, then every subscription is polled.
- `subscribe-pulled` (200): the same on an adopter, after a pull of a cold commit.
- Soak: `dialog-soak/src/catchup.rs` (poll-k after k upstream commits) and the `subscribe` phase of `dialog-soak/src/join.rs:566-581`.
- None of these covers folds.

## 9. Main against the stack
The merge base of `origin/feat/notation-poc` is `54b1fc4`, older than main's `8ce5f37`. A two-dot diff therefore mixes in main's own later changes (#829, #832, #833, #836), which touch only `session.rs` among these files. The stack's own changes (three-dot diff):
- `subscription.rs`, `invariants.rs`, `stack/subscribe.rs`, `affected.rs`, `reduce.rs`, `application.rs`, `descriptor.rs`, `rules.rs`, `deductive.rs`, `fixpoint.rs`, the macros and the perf scenarios: **no change**.
- `dialog-query/src/concept/query.rs` (+143): parameters may hold a map or list pattern. `evaluate_erased(.., apart)` builds and takes apart rows (`pattern::build` / `take_apart`). Serializing such a query errors, so a `Stack::subscribe` of one would get no `QueryHash` and register unshared (INFERRED). The reduce path is unchanged.
- `dialog-query/src/rule.rs` (+10): `pattern::lower(premises)`; constants are checked through `.leaves()`. `ReduceInOpenRule` still refuses.
- `session.rs` (+96) and `ephemeral.rs` (+34): a `QueryLayer` and an `Ephemeral` can hold `Bodies` (rules held whole, by entity, with their `dialog.rule/*` index facts among the line's facts). Rule resolution reads them. A subscription still builds its layer from the branch alone, so a held body reaches it only through the branch overlay (INFERRED).
- The notation's own subscriptions: `dialog-notation/tests/view.rs` and `examples/cost.rs` use `Branch::subscribe` and count `recomputes()`.

## What a design must respect
- A subscription holds one query of one `Application` type, over one branch (optionally through a stack). It keeps a flat `Vec<Conclusion>` and diffs by `PartialEq`. A row has no identity beyond its whole value; a changed field is a retraction plus an assertion.
- Retained rows are grouped only by `Conclusion::this()`, and maintenance replaces everything under one `this` at a time. For a fold, `this` is one component of the group key; the other components are the non-reduced head fields.
- At poll time, what is known:
- the changed base facts inside the cover, signed and deduplicated (tree, overlay instants, memory instants), with no cause ordering beyond what `widened` reads;
- the pinned and current revisions, with a before-state only while the overlay and memory rings reach the pin.
- What is not known at poll time: derived rows' changes, per-premise or per-group state, and the previous fold inputs. Re-derivation is the only way a row is found to have stopped holding.
- The cover is the gate. A reducing rule's body is scanned at empty scope, so its cover is the whole body relation (INFERRED). A rule-range change always recomputes, and an overlay-laid rule is a rule-range change.
- Recompute is the always-sound fallback, and every partial path must be all-or-nothing before `results` changes (sub.rs:1275-1278; `it_keeps_its_results_when_a_rederivation_fails`).
- The architecture is stated as pull-driven with no retained operator state (sub.rs:59-64, `notes/incremental-subscriptions.md:39-52`). The fixpoint `Continuation` is the one exception, kept in the subscription and attached per concept through the query env.
- The poll future must stay `Send` on native (sub.rs:757-760, `it_keeps_the_poll_future_send_general`). Evaluations are boxed for that reason.
- Fold semantics to preserve:
- groups exist only from rows;
- absent inputs are skipped;
- `count`/`sum` have identities, while `min`/`max`/`avg` go absent;
- `sum` is checked `i128`; integers and floats do not mix;
- output must not depend on row order: canonical ordering, the dag-cbor tie rule (reduce.rs:19-60).
- Authored reducing deductive rules are refused on main and on the stack. Only the spike lets one fold.
- `maintenances` is not a reliable "not recomputed" signal for recursive concepts (§5 item 5).

## What I could not determine
- Whether `SelectRules` results are cached across a poll's three calls. `span.resolve_rules: 9` in the repoll baseline suggests resolution happens several times; I did not trace the cache.
- What a fold subscription costs in reads. Nothing measures it, and I ran nothing.
- Whether the empty-scope body scan of a reducing rule records ranges that also make unrelated attributes' writes "touched". This depends on the body's selectors, which I did not trace.
- Whether `elected_attributes` and `widened` behave correctly when a reducing rule is reachable. They are never reached past affected.rs:338 today.
- Whether the collection tests are maintained or recomputed. They do not assert the counts.
