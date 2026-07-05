# Operator IR and analysis-owned planning

> Design document for the `feat/operator-ir` chapter: the goal, the architecture chosen to reach it,
> and the decisions made along the way, including the alternatives considered and why they lost.
> Companions: [`planning-adornment-and-cost.md`](./planning-adornment-and-cost.md) (the feasibility/
> cost design this realizes), [`scalar-associative-layer.md`](./scalar-associative-layer.md) (the
> optionality restructure), [`polarity-and-negation.md`](./polarity-and-negation.md) (negation typing),
> and [`query-engine-design.md`](./query-engine-design.md) (the resulting engine, described as it is).

## Goal

Three properties the engine should have, none of which it had at the chapter's start:

1. **Execution runs on a compiled form, not the AST.** A `Plan` operator IR gives later work (demand
   reification, incremental maintenance, cost redesign) a concrete structure to attach to, instead of
   re-walking syntax.
2. **A rule that exists is valid.** Analysis is the constructor: type inference, the safety checks, and
   the dependency structure all run before a rule can be obtained, so every downstream consumer holds a
   rule that is *plannable by construction*. The analysis artifacts (the SIPS) are retained as data,
   because the demand-driven incremental work consumes them.
3. **Optionality is structural.** The correctness of optional (`maybe`) fields must not depend on plan
   order, field names, or runtime guard interactions; the contracts live in the shape of the
   constructs themselves.

## Prior state, and what was wrong with it

- Evaluation walked the syntactic AST: `Premise`/`Proposition` carried `evaluate`, so there was no
  compiled artifact between "parsed rule" and "running stream".
- Rule construction ran **plan-then-analyze**: the dependency graph was computed *from the already
  ordered steps* and then discarded. Analysis could validate but produced nothing anyone consumed; the
  SIPS the magic-sets literature builds everything on existed only transiently.
- A rule stored one pre-baked `Conjunction`. Plans are scope-specific, so replanning had to
  reconstruct premises from the stored plan (an `as_premise` round-trip) and re-run everything,
  including type inference, on every replan and every concept adornment.
- Feasibility was fused into cost (`estimate(env) -> Option<usize>`): a premise that could not run was
  merely "costless", with no account of *why* or of what would unblock it: exactly the information
  demand reification needs.
- Optionality was a property of a term's *kind*, interpreted by scans through four cooperating runtime
  guards in two files. The meaning of "optional" was therefore only correct under plan orders the
  guards anticipated; the #348 family (an optional field that sorts first leads the scan and silently
  drops entities) was the observable symptom.

## Architecture

### The type hierarchy carries the guarantees

```
DeductiveRuleDescriptor ──analyze──▶ DeductiveRule ──plan(scope)──▶ Conjunction ──evaluate──▶ rows
   (wire data,                       (verified, plannable           (concrete plan
    no guarantees)                    by construction)               for one scope)
```

Analysis runs type inference, the safety checks (required-head, Coalesce contract, negated-optional),
and builds the `DependencyGraph`, all from the premises, before any execution order exists. The rule
holds the analysis (`{premises, types, graph}`); a concrete plan is produced per scope, cheaply,
because the expensive scope-independent work is never repeated. `InductiveRule` is the
assertion-shaped sibling on the same pipeline.

### The operator IR

`Plan` is a closed enum (`Scan`, `Maybe`, `Formula`, `Constraint`, `Concept`, `Negate`), each variant
carrying its lowered payload plus a `Header { cost, binds, env }`. The syntactic premise is *not*
stored; it is reconstructable from the payload (`as_premise`) for the consumers that analyze a step.
Evaluation dispatches on the variant.

### One SIPS, two halves, cost apart

The magic-sets SIPS `(≺, f)` is realized as two retained artifacts: the `DependencyGraph` (`≺`, the
dependency index: which premise binds what each premise needs) and `feasibility::categorize`/`feasible`
(`f`, the binding function: given the bound set, what a premise binds, or which variables it still
needs, named in `Infeasible::NeedsAll`). Cost is deliberately *not* part of either: the planner asks
`estimate` only of premises feasibility has approved, per the Balbin separation.

### Inference once, projected everywhere

Analysis infers the rule-wide `TypeEnv` once. Planning (`Planner::with_types`) projects it onto a
working copy of the premises per `plan(scope)` call: attribute value kinds are stamped, a `Maybe`
whose variable is proven present demotes to a plain scan, and concept parameter terms record what the
rule proved at the boundary. Projection is positive-polarity only.

### Structural optionality

Set-widening lives in exactly one construct, the `OptionalAttributeQuery` left-join, whose schema hard-requires
its entity slot and declares the widened content types. The associative layer below it is scalar.
Every ordering-sensitive correctness condition is schema-borne (the entity requirement, Coalesce's
hard-required source), so the planner cannot produce an order that changes meaning.

## Decisions, with the alternatives considered

**The dependency graph does not drive ordering.** The obvious move (feed the analysis-built graph to
the planner so it stops re-categorizing premises) was considered and rejected: choice-group
satisfaction shifts with the bound set (a group satisfied by a *bound variable* flips a slot from
required to binding), so ordering needs per-scope feasibility that a static graph cannot express
without changing plan order. The graph's actual role (clarified mid-chapter): it is the **dependency
index** (given a binding, which premises it affects/unblocks) which is precisely what demand-driven
re-planning and incremental subscriptions consume. Ordering uses the shared `categorize`; the graph is
kept for the consumers that need static structure.

**Narrowed premises are not stored on the rule.** The earlier design had analysis bake narrowing into
the stored premises, making the planner type-free. Rejected on a round-trip argument: a rule's
serialized descriptor is reconstructed from its premises, and baked-in narrowing would leak inferred
kinds into the wire form, so serialize/deserialize would not be identity. Instead the rule stores the
authored premises plus the inferred `TypeEnv`, and planning projects types onto a working copy. The
single-inference property is preserved; the wire form is untouched.

**`AnalyzedRule` was composed, not dissolved.** The design called for collapsing `AnalyzedRule` into
`DeductiveRule`. As built, `DeductiveRule { analysis: AnalyzedRule }`: composition gives the same
guarantee (the rule *is* its analysis) with less churn, and the inductive sibling shares the type.

**Stateless feasibility replaced the stateful candidate.** The old planner kept per-premise `Candidate`
state, incrementally updated as scope grew, with subtle stickiness semantics on paths real planning
never took. Characterization established that the statefulness was an optimization, not a contract
(only the output plan is observable), so the candidate machinery was deleted in favor of recomputing
`feasible` per round. (The dependency edges enable re-checking only affected premises if this ever
shows up in profiles.)

**Optionality as a structural operator, not a kind-driven scan mode.** Alternatives:
(1) keep deriving scan behavior from the value term's kind and patch the planner heuristics: rejected
because the #348 family showed the guards' meaning depended on orderings the planner is free to choose;
a type system that cannot constrain the plan cannot guarantee its own semantics.
(2) compose optional fields from a scalar scan plus `Coalesce`: rejected because the fallback still
needs left-join row semantics underneath (emit nothing vs. emit Absent), so the operator is needed
anyway and the composition adds nothing.
Chosen: a first-class `OptionalAttributeQuery` premise/plan construct wrapping a *scalar* lookup, with the
contracts in its schema. A standalone optional lookup with an unbound entity is thereby
*inexpressible* rather than mis-planned ("absent for whom?").

**`Absent` across boundaries filters; it never aborts and never silently defaults.** A scalar slot
(scan, formula input, equality against a non-widened term) matches nothing against an `Absent` claim:
the row is excluded, in both polarities. The error alternative (reject optional-into-required at
analysis, demand explicit coalescing everywhere) was rejected as call-site ceremony contrary to the
set-widening reading of optionality: in a relational language, the demanding premise *is* the evidence
of presence. `Coalesce` remains the explicit opt-in for defaults, and orders strictly after its source
so a default can never shadow a present value.

**Negation participates in typing not at all.** Negated premises neither contribute their demands to
inference (else "unless the nickname is banned" would silently strengthen to "must have a nickname,
and it must not be banned") nor receive the positive narrowing (a negated subquery is a hypothetical,
typed in its own context). The second direction is a judgment call with arguments both ways, recorded
with its alternatives in [`polarity-and-negation.md`](./polarity-and-negation.md).

**Checked kinds: filter at the data boundary, error at the contract boundary.** Attribute values are
dynamically typed in the store (one attribute may hold several value types across facts), so a typed
scan slot treats a mismatched fact as a non-match and filters it. `Match::bind` rejecting a value
outside the variable's kind is the opposite case: by then every data-dependent filter has run, so a
mismatch is a construction-path bug and surfaces as an error.

## Status and remaining design

Landed on `feat/operator-ir`, guarded throughout by characterization, plan-ordering, and end-to-end
optionality tests. Still open from the companion design (tracked on the roadmap): the richer
`Infeasible` vocabulary (`NeedsAnyOf`/`NeedsKOf`) and the serializable per-premise `Feasibility`
descriptor; propagator decomposition of multidirectional formulas (`math/sum` as adder plus
subtractors rather than k-of-n feasibility); and the cost redesign (work-class plus selectivity in
place of magic constants).
