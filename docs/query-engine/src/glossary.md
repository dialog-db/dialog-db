# Glossary

**Adornment**
A compact `u64` bitfield encoding which of a concept's parameters are bound
vs free. Used as a cache key for pre-planned execution strategies. Inspired
by the magic set transformation from deductive databases.

**Answer**
A set of variable bindings accumulated during query evaluation. Each binding
maps a variable name to a value with associated provenance (`Factors`).

**Application**
The polymorphic trait for anything that can be evaluated as a query. Provides
`evaluate()` to produce an answer stream and `realize()` to convert answers to
typed results.

**Artifact**
A stored claim in the database. Contains `the` (attribute), `of` (entity),
`is` (value), and `cause` (provenance reference).

**Attribute**
A named, typed relation in `domain/name` format. Describes one property an
entity can have. Examples: `person/name`, `employee/salary`.

**Bindings**
A controlled interface for reading and writing values during formula
evaluation. Tracks which inputs were read for provenance.

**Blank (variable)**
An anonymous variable (`Term::Variable { name: None, .. }`) that matches
anything but does not participate in joins across premises. Used as a wildcard.

**Candidate**
A premise under consideration by the query planner. Can be `Viable` (ready to
execute) or `Blocked` (missing prerequisites).

**Cardinality**
Whether an attribute holds a single value (`One`) or multiple values (`Many`)
per entity. Affects cost estimation and winner selection.

**Cause**
A reference to a previous claim that the current claim succeeds. Establishes
causal ordering for conflict resolution.

**Choice Group**
A set of parameters in a schema where binding any one member satisfies the
entire group. Used in `RelationQuery` schemas to express that any of
`(the, of, is)` suffices to constrain the query.

**Claim**
An immutable `(the, of, is, cause)` tuple stored in the database. The
fundamental unit of information. Claims are never modified; new claims may
succeed previous ones.

**Concept**
A named collection of attributes that share an entity. Similar to a table in
a relational database, but defined at query time. Examples: `Person`,
`Employee`.

**ConceptQuery**
A query that matches entities satisfying a concept's attribute requirements.
Expands into multiple `RelationQuery` steps internally.

**ConceptRules**
Per-concept storage of the implicit rule and any user-installed deductive
rules, with an adornment-keyed plan cache.

**Conclusion**
A typed instance produced by a concept query. The `Application::realize()`
method converts raw `Answer`s into conclusions.

**Conjunction**
An ordered sequence of execution steps produced by the planner. Represents the
AND of premises in their planned execution order.

**Constraint**
A premise that enforces equality between two terms without accessing the store.
Requires both operands to be bound.

**Deductive Rule**
A user-defined rule that derives concept instances from other data. Multiple
rules for the same concept form a disjunction (OR).

**Disjunction**
The OR of multiple conjunction plans. An entity matches a concept if any one
alternative produces answers.

**Environment**
A `HashSet<String>` tracking which variable names are currently bound during
planning. The planner extends the environment as it selects premises.

**Evidence**
Input format for creating factors. Describes how a binding should be created
before being converted to a `Factor`.

**Factor**
Provenance record for a single binding. Three variants: `Selected` (from a
matched claim), `Derived` (computed by a formula), `Parameter` (externally
provided).

**Factors**
Multiple provenance records that agree on the same value. A primary factor plus
optional alternates that confirm the binding from different sources.

**Formula**
A pure computation that reads bound variables and writes derived values. Does
not access the store. Examples: `Sum`, `Concatenate`, `Length`.

**FormulaQuery**
A type-erased formula ready for evaluation. Contains the compute function,
parameter mappings, and cost.

**Negation**
A premise that filters answers by checking that a pattern does *not* match.
Implements negation-as-failure semantics.

**Parameters**
A `HashMap<String, Term<Any>>` mapping parameter names to terms. Each premise
type has a fixed set of parameter names.

**Plan**
A single step in a conjunction. Contains the premise, its cost, the variables
it binds, and the environment it was planned in.

**Planner**
A state machine that greedily selects the cheapest viable premise at each
step, building an ordered execution plan (conjunction).

**Premise**
A single step in a query. Either `Assert(Proposition)` (must match) or
`Unless(Negation)` (must not match).

**Prolly Tree**
A probabilistic B-tree with content-addressed nodes. Provides deterministic
structure (same data = same tree) and efficient range queries.

**Proposition**
The content of a premise. One of: `Relation`, `Concept`, `Formula`, or
`Constraint`.

**RelationQuery**
A query matching a single `(the, of, is, cause)` claim pattern. The most
fundamental premise type.

**Requirement**
Whether a parameter must be externally bound (`Required`) or can be derived
(`Optional`). Parameters in the same choice group form `Required(Some(group))`.

**Schema**
Metadata describing a premise's parameters: their types, cardinalities, and
requirements. Used by the planner to classify parameters as prerequisites or
products.

**Selector**
Records which component of a matched claim (`The`, `Of`, `Is`, `Cause`)
contributed a particular binding.

**Session**
The entry point for interacting with the database. Combines a store with a
rule registry, supporting both reads and writes.

**Source**
A read-only trait for accessing facts and rules. Implemented by `Session` and
`QuerySession`.

**Statement**
A typed value that can be asserted or retracted in a transaction. Concepts
implement `Statement` by decomposing into individual attribute claims.

**Term**
A query building block. Either a `Constant` (must match exactly), a named
`Variable` (captures a binding and participates in joins), or a blank
`Variable` (matches anything, no join).

**The**
A validated attribute selector in `domain/name` format. The `the!()` macro
validates at compile time.

**Transaction**
Accumulates assertions and retractions, organized by entity and attribute.
Committed atomically to the store.

**Unification**
The process of combining variable bindings across premises. If two premises
bind the same variable to the same value, the bindings are consolidated. If
they bind it to different values, the answer is eliminated.
