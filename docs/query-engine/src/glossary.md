# Glossary

**Adornment** — A `u64` bitfield encoding which concept parameters are bound
vs free. Cache key for pre-planned execution strategies.

**Application** — Polymorphic trait for anything evaluable as a query.
Provides `evaluate()` for the match stream and `realize()` to convert to typed
results.

**Attribute** — A named, typed relation in `domain/name` format. Describes one
kind of association an entity can have.

**AttributeQuery** — Query matching a single `(the, of, is, cause)` claim
pattern. The most fundamental premise type. Dispatches between `All`
(Cardinality::Many) and `Only` (Cardinality::One) variants.

**Candidate** — A premise under consideration by the planner. Either `Viable`
(ready to execute) or `Blocked` (missing prerequisites).

**Cardinality** — Whether an attribute holds a single value (`One`) or multiple
values (`Many`) per entity.

**Cause** — Provenance reference establishing causal order between claims.

**Choice Group** — Schema parameters where binding any one satisfies the group.
`AttributeQuery` uses this for `(the, of, is)`.

**Claim** — An immutable `(the, of, is, cause)` tuple. The fundamental unit of
information.

**Concept** — A composition of attributes sharing an entity. Bidirectional:
assert decomposes into claims, query composes claims into conclusions.

**ConceptQuery** — Query matching entities against a concept's attribute
requirements. Expands into multiple `AttributeQuery` steps.

**Conjunction** — Ordered sequence of execution steps (the AND of premises).

**Constraint** — Equality check between two terms. Requires one operand bound;
supports bidirectional inference.

**Deductive Rule** — User-defined rule deriving concept instances from
alternative premises. Multiple rules form a disjunction (OR).

**Disjunction** — The OR of multiple conjunction plans.

**Environment** — `HashSet<String>` tracking bound variable names during
planning.

**Factor** — Provenance for a binding: `Selected` (from a claim), `Derived`
(from a formula), or `Parameter` (externally provided).

**Formula** — Pure computation reading bound variables and writing derived
values. No store access.

**Match** — A set of variable bindings accumulated during evaluation.

**Negation** — Premise that filters matches by checking a pattern does *not*
match. Never binds variables.

**Parameters** — `HashMap<String, Term<Any>>` mapping parameter names to terms.

**Plan** — A single step in a conjunction: premise, cost, bindings produced.

**Planner** — State machine that greedily selects the cheapest viable premise
at each step.

**Premise** — A query step: `Assert(Proposition)` (must match) or
`Unless(Negation)` (must not match).

**Proposition** — The content of a premise: `Attribute`, `Concept`, `Formula`,
or `Constraint`.

**Schema** — Metadata describing a premise's parameters: types, cardinalities,
and requirements.

**Search Tree** — Content-addressed probabilistic B-tree storing claims.
Deterministic structure (same data = same tree).

**Selection** — Stream trait (`Stream<Item = Result<Match>>`) for evaluation
pipelines.

**Session** — Entry point for database interaction. Combines a store with a
rule registry.

**Term** — Query building block: `Constant` (must match), named `Variable`
(joins across premises), or blank `Variable` (wildcard).

**The** — Validated attribute selector in `domain/name` format. The `the!()`
macro validates at compile time.

**Unification** — Combining bindings across premises. Same value = consolidated;
different value = match eliminated.
