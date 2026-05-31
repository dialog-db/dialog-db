# Rule Compilation Pipeline

A deductive rule moves from user-supplied premises to an executable
plan in three phases:

```
   ┌──────────┐    ┌──────────┐    ┌──────────┐
   │  Parse   │ -> │ Analyze  │ -> │  Plan    │
   └──────────┘    └──────────┘    └──────────┘
        |               |               |
   Descriptor       AnalyzedRule    Conjunction
```

## Parse

`DeductiveRuleDescriptor` is the serialized form: a conclusion
(`ConceptDescriptor`) plus `when`/`unless` premise lists with
user-supplied terms. Parsing yields `Vec<Premise>` plus the
conclusion. No type information beyond what the user wrote at the
term layer (e.g. `Term<Option<String>>` carries `String | Nothing`;
`Term<Any>::var("x")` carries nothing).

## Analyze

`rule::analyzer::analyze(conclusion, &steps) -> Result<AnalyzedRule, AnalysisError>`.

Three sub-steps:

1. **Type inference** (`rule::types::TypeEnv::infer`).
   For every named variable referenced by any positive premise's
   slots, unify the slot kinds. Negation premises don't
   contribute — they filter on already-bound values rather than
   introducing them. Untyped slots contribute their *requirement
   shape*: a `Required` slot says "any present value"
   (`Primitive::ALL`), an `Optional` slot says "any present or
   absent" (`Primitive::ANY`). Output: name → inferred `Kind`.
   Errors: `Conflict { variable, reason }` when slots disagree on
   the kind for a given variable.

2. **Required-head check**.
   For each conclusion variable, if the inferred kind admits
   `Nothing`, raise `RequiredHeadFromOptional { variable }`. The
   rule can't produce `Absent` in a required slot.

3. **Coalesce contract validation**.
   Every `Constraint::Coalesce` runs against a fresh unifier
   context. The contract says: source is `Optional<α>`, fallback
   and is both unify with `α`. Errors:
   `CoalesceTypeMismatch { reason }`.

The output, `AnalyzedRule`, carries:
- `conclusion: ConceptDescriptor`
- `premises: Vec<Premise>` in planned order
- `types: Arc<TypeEnv>` — the inferred environment, shared
- `graph: DependencyGraph` — per-premise `binds`/`needs` plus
  precomputed `requires[]` edges for ordering

Analysis is rule-scoped; the result is immutable. Errors are pre-
rule (they don't reference a `DeductiveRule`); `DeductiveRule::new`
wraps them in the corresponding `TypeError::*` variants for display.

## Plan

`Planner::from(premises).plan(&scope) -> Result<Conjunction, TypeError>`.

The planner does:

1. Greedy cost-based ordering: repeatedly pick the cheapest viable
   premise, remove it from candidates, advance the bound-vars set.
   Existing `Candidate`/`Schema`/`Parameters` walk.
2. Run `TypeEnv::infer` on the ordered steps. Failures surface as
   `TypeError::TypeInference`.
3. **Narrow each step's premise** via `apply_types(premise, &types)`.
   The rewrite replaces variable terms (currently only
   `AttributeQuery::is`) with copies carrying the inferred kind.
   Negated propositions are walked too — a negation over an
   optional attribute picks up the same narrowing.
4. Stamp the rewritten premises into `Plan` values; assemble the
   `Conjunction`.

The rewrite happens **once at plan time**, not on every evaluation.
The user-supplied `Premise` values stay untouched; only the in-flight
working copy in the `Plan` reflects rule-level narrowing.

## Why narrow at plan time

The evaluator's behavior depends on `is.is_optional()` for the
Absent-fallback decision:

- Without narrowing, an optional attribute always emits an Absent
  row when its lookup misses — even when a sibling premise's
  required slot guarantees that variable is Present at the rule
  level. The downstream join then filters the spurious Absent rows.
- With narrowing, the optional attribute's `is` term reflects the
  rule-inferred kind. If inference stripped `Nothing` (because a
  sibling required premise narrowed it), `is_optional()` returns
  `false` and the fallback row is never emitted.

The savings are real for any rule that mixes optional and required
bindings on the same variable.

## Evaluation

`Conjunction::evaluate(selection, env)` folds the steps' evaluators
in order. Each step's premise is already narrowed; no `TypeEnv` is
threaded through evaluation. Standalone queries (top-level
`.perform()` outside any rule) plan with an empty `TypeEnv` so the
rewrite is a no-op — the user's local term kinds are the sole
source of optionality info.

## Replanning

`Conjunction::plan(&new_scope)` reruns the planner against a
different scope (adornment-based replanning for concepts whose
bindings change between callers). Type inference runs again on the
fresh order — it's idempotent (re-narrowing an already-narrowed
premise produces the same kinds) and stable across reorderings
(inference doesn't depend on step order).

## What lives where

| Location | Contents |
|---|---|
| `Premise` (user-facing) | Whatever the user wrote |
| `AnalyzedRule.types` | Rule-level inferred env, shared via `Arc` |
| `AnalyzedRule.graph` | Per-premise `binds`/`needs` + `requires[]` |
| `Plan.premise` | Premise with variable terms narrowed |
| `Conjunction.steps` | Ordered `Plan`s plus cost/binds/env |

## Errors

| Source | Variant | When |
|---|---|---|
| `InferenceError::Conflict` | `TypeEnv::infer` | Slot kinds disagree for one variable |
| `AnalysisError::Inference` | `analyze` | Wraps the above |
| `AnalysisError::RequiredHeadFromOptional` | `analyze` | Inferred head admits `Nothing` |
| `AnalysisError::CoalesceTypeMismatch` | `analyze` | Coalesce contract violated |
| `TypeError::TypeInference` | `Planner::plan` | Inference error during planning |
| `TypeError::RequiredHeadFromOptional` | `DeductiveRule::new` | Wraps analysis error with rule |
| `TypeError::CoalesceTypeMismatch` | `DeductiveRule::new` | Wraps analysis error with rule |
| `TypeError::UnboundVariable` | `DeductiveRule::new` | Head var not bound by any premise (post-plan) |
| `TypeError::RequiredBindings` | `Planner::plan` | A premise's dependencies are unsatisfiable |
