# Scalar associative layer: move optionality up to the semantic layer

> Investigation note. Maps the boundary for the restructure: the associative (raw EAV / triple) layer
> should operate on **scalars only** — `the(of, is)` with a present value — and all optionality
> (`Option`, `Absent`, set-widening) belongs in the **semantic** (concept) layer, composed from scalar
> scans. This is the real root cause behind the #348 bug and the unplannable standalone-optional query.

## The problem (root cause)

Optionality leaked *down* into the associative `AttributeQuery`. The concept layer pushes a set-widened
(`Option`-kinded) `is` term into a raw triple scan, and the scan itself implements the `Absent`
fallback. That entanglement causes:

- **#348:** an optional scan with an unbound entity suppresses its `Absent` fallback (correctly — "absent
  for whom?" needs a known entity) but still leads the scan, silently dropping entities lacking the fact.
- **The unplannable query:** `the!("person/name").of(?this).maybe(?name)` standalone — a scalar lookup
  is being asked to express "every entity, set-widened," which a triple scan cannot soundly do.
- **Feasibility awkwardness:** "can an optional scan lead?" is a question that only exists because the
  associative layer knows about optionality. Remove optionality from it and the question vanishes.

**The fix is layering, not a planner heuristic:** the associative layer is scalar; the semantic layer
set-widens.

## What the associative layer carries today (to remove)

`Resolution` (28 refs across 4 files) + the Absent-fallback machinery (~30 refs), all in
`src/attribute/query/`:

- `Resolution` enum (`Required` / `Optional`) — `attribute/query.rs`.
- `resolution()` derived from `is.is_optional()` — `all.rs`, `only.rs`, `dynamic.rs`.
- The `Absent` fallback in evaluation (`all.rs` ~L266–307): `entity_known = of.is_constant()`,
  `bind_absent(is)`, `bind_absent(cause)`, the `!produced && is_optional && entity_known && …` guard.
- The optional-widening of the `is` (and `cause`) schema/content-type for optional queries.
- The `of`-is-required change I just made for #348 — a symptom patch that should be reverted; with a
  scalar layer there is no optional scan to special-case.

After: `AttributeQuery` is a scalar triple lookup. `is` is always a present value (or a variable bound
to one). No `Resolution`, no `Absent`, no `entity_known` guard. The schema's `is`/`of`/`cause` are plain
required/grouped slots — the feasibility model is uniform (every attribute scan is a normal scan).

## What the semantic (concept) layer takes over

The set-widening currently emitted in `From<&ConceptDescriptor> for DeductiveRule` (deductive.rs L141–167):

- **Today:** an optional field emits one `AttributeQuery` with an `Option`-kinded `is` (→ `Resolution::Optional`).
- **Target:** an optional field emits a **scalar** `AttributeQuery` (required, like every field) plus a
  *left-join / coalesce* at the projection: for the bound entity `this`, run the scalar scan; if it
  yields no row, supply `Absent` for that field. The entity is always known here (the concept binds
  `this` before — or the required fields do), so "absent for whom?" is always answerable.

This is sound precisely because the concept *always has a `this`*: required fields bind it; an optional
field's left-join is evaluated per known entity. The associative layer never has to guess.

Mechanism options for the concept-layer set-widening (to decide when we implement):
1. **A projection operator** that wraps the scalar scan: run it for each input row's `this`; emit the
   Present row(s), or one `Absent` row if none. Essentially the current `all.rs` fallback logic, lifted
   out of `AttributeQuery` and into the concept projection where `this` is guaranteed bound.
2. **Coalesce composition:** scalar scan into a fresh var, then a `Coalesce` (which already exists) to
   set-widen into the field var with an `Absent` fallback. Reuses existing machinery; the projection
   emits `scan(this, ?tmp_present)` + `coalesce(?tmp_present → ?field, else Absent)`. Needs the scan to
   be a left-join (emit nothing vs. emit absent) — so option 1's left-join is still needed underneath.

Option 1 (a per-entity left-join projection operator in the concept layer) is the clean target: the
associative scan stays pure-scalar, the concept layer owns "optional field = left-join + Absent."

## Blast radius

- `src/attribute/query/{all,only,dynamic,mod}.rs` — remove `Resolution`, the Absent fallback, the
  optional schema widening. ~58 refs.
- `src/rule/deductive.rs` `From<&ConceptDescriptor>` — emit scalar scans + a left-join wrapper for
  optional fields instead of optional `is` terms.
- The optional-field concept tests (`it_executes_concept_with_optional_field`,
  `it_set_widens_optional_field_sorted_before_required`) — should still pass (same observable: optional
  field → Absent when missing), now via the concept-layer left-join.
- The standalone-optional planner tests (`it_preserves_local_optionality…`, the optional `nickname` in
  `it_plans_coalesce_constraint`) — these become *invalid* (a scalar layer has no optional scan). Rewrite
  them: optionality is exercised only through concepts now, not raw attribute queries.
- Type-narrowing / inference: `is_optional()` on the attribute `is` term goes away; the narrowing logic
  (`apply_types`, the `String | Nothing` widening) was *about* the optional attribute term — re-home or
  remove. Needs care (this is what the type-inference branch added).

## Relationship to the operator-IR / analyze→plan work

This is **orthogonal** to the planner restructure we've been landing (analyze→plan, feasibility/cost,
SIPS). It's a layering fix in the *attribute* and *concept* layers, not the planner. The planner gets
*simpler* afterward (no optional-scan feasibility special case). It should be its own chapter, sequenced
after — and the #348 `of`-required patch reverted, since it's a symptom treatment this restructure
removes.

## Open questions for the design (before implementing)
- Left-join operator vs. Coalesce composition (option 1 vs 2 above).
- What happens to the optional `is`-term *type* machinery (`Term<Option<U>>`, `Kind::optional`,
  the inference narrowing) — does the value-layer keep any notion of optional, or is `Option`/`Absent`
  purely a concept-projection (semantic) construct with no presence in attribute terms at all?
- Cardinality-many optional fields (a `maybe`+`many` field) — how the left-join interacts with multiple
  rows.

## Decisions (as built)

The restructure landed on `feat/operator-ir`; every open question above
is settled:

- **Option 1 taken.** The left-join is a first-class construct:
  `OptionalAttributeQuery` (premise level: `Proposition::OptionalAttribute`, plan level:
  `Plan::OptionalScan`) wraps a *scalar* `DynamicAttributeQuery`. Its schema
  hard-requires the entity slot and set-widens the `is`/`cause`
  content types, so feasibility and inference need no special cases.
  Concept lowering emits a plain scan per required field and a
  `OptionalAttributeQuery` per optional field.
- **The `of`-required symptom patch was reverted** once the schema
  contract moved into `OptionalAttributeQuery`; attribute schemas are uniform
  again.
- **Term-level `Option` fate.** The `Nothing` bit lives in the type
  system and in the schemas that can deliver `Absent` (`OptionalAttributeQuery`,
  a concept's optional fields). Attribute terms never carry it:
  `AttributeQueryAll::new` strips a `Nothing`-bearing kind at
  construction. `Term<Option<T>>` remains the *declaration* surface
  (concept fields, coalesce sources).
- **Narrowing demotes.** When rule inference proves a sibling premise
  guarantees presence, `apply_types` demotes the `Maybe` to its inner
  scalar scan (`OptionalAttributeQuery::into_query`), preserving the
  fallback-suppression optimization the old `is`-term narrowing
  provided.
- **Cardinality::Many** rides the inner dispatch: every fact extends
  the row; a miss still yields exactly one `Absent` row (set-widening
  is per entity, not per fact).
- The remaining row-multiplicity guards (`saw_fact`, `entity_known`)
  were deleted with `Resolution`; their semantics live in
  `OptionalAttributeQuery::evaluate`'s four-case contract. User-facing semantics
  are documented in `notes/guide.md`.
