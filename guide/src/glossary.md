# Glossary

**Attribute**
A named, typed relationship between an entity and a value. Defined as a Rust newtype with `#[derive(Attribute)]`. Each attribute has a selector (like `"recipe/name"`), a value type, and a cardinality.

**Cardinality**
Whether an attribute allows one value per entity (`one`, the default) or multiple values (`many`). Cardinality one means asserting a new value retracts the old one. Cardinality many means values accumulate.

**Cause**
The causal reference attached to each claim, recording which peer asserted it and the logical timestamp. Used for sync and conflict resolution.

**Claim**
An atomic statement stored in the associative layer. Contains four fields: `the` (attribute), `of` (entity), `is` (value), and `cause` (provenance). Claims are immutable and content-addressed.

**Concept**
A group of attributes queried together. Defined as a Rust struct with `#[derive(Concept)]`. A concept query is a conjunction: an entity matches only if it has all the listed attributes.

**Conjunction**
A logical AND. In Dialog, the premises of a rule and the attributes of a concept are conjunctive: all must hold.

**Disjunction**
A logical OR. In Dialog, installing multiple rules for the same concept creates disjunction: any rule can produce a match.

**Entity**
A unique identifier for a thing in the database. Entities have no inherent type or structure. Their meaning comes from the claims asserted about them.

**Fact**
See **Claim**. The guide uses "claim" to refer to atomic statements in the associative layer.

**Formula**
A pure computation integrated into the query planner. Takes bound inputs and produces derived outputs. Defined with `#[derive(Formula)]`.

**Negation (negation-as-failure)**
A pattern that succeeds when no matching claim exists. Written with `!` before a query pattern. Variables in negated patterns must be bound by preceding positive premises.

**Premise**
A condition in a rule. Multiple premises form a conjunction. A premise can be a query pattern, a formula, or a negated pattern.

**Prolly tree**
Probabilistic B-tree used as Dialog's storage structure. Two trees with the same content have the same structure, enabling efficient diffing for sync.

**Query**
A pattern with variables and constants that the engine matches against stored claims and derived rules. Returns all bindings that satisfy the pattern.

**Rule**
A function that derives a concept from a set of premises. Rules are evaluated at query time and do not store their results. Multiple rules for the same concept create disjunction.

**Selector**
The string identifier for an attribute in `"domain/name"` format (e.g., `"recipe/name"`). Derived from the Rust module and struct name.

**Session**
The entry point for interacting with Dialog. Wraps a source and a set of installed rules. Supports both querying and writing through transactions.

**Source**
The backing storage that a session operates against. Implements the `Source` trait.

**Term**
A value in a query pattern. Either a variable (`Term::var("x")`) that the engine binds, or a constant (`Term::from(value)`) that must match exactly. Variables with the same name are unified.

**Transaction**
A batch of assertions and retractions committed atomically through `session.commit(edit)`.

**Unification**
When two terms share the same variable name, the engine requires them to have the same value. This is how joins work in Dialog: shared variables across patterns.
