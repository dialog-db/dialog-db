# Optional Fields & Type System — v2 Design

This document was written as a *design contract* before
implementation. The implementation took a smaller path than the
contract proposed. The doc has been split:

- **What shipped** — the set-widening type system, the unifier-backed
  rule-level type inference, the `Resolution` policy, and the
  `Coalesce` constraint. These are now in code under
  `rust/dialog-query/`. The shipped pipeline is documented in
  [`rule-pipeline.md`](./rule-pipeline.md); the type-system shape is
  in `src/type_system.rs` and `src/type_system/unifier.rs`.

- **What didn't ship** — rank-1 polymorphic *formulas* with
  `TypeScheme`/`SchemeBody`/`SchemeType`, and the `instantiate` /
  `generalize` operations that go with them. The schemes were the
  design's answer to "generic formulas like `math/sum`"; we didn't
  have a concrete consumer that needed them, so the work was
  deferred to a follow-up.

The remainder of this document is the original design contract,
preserved so the historical intent is visible. **Names like
`TypeScheme`, `SchemeBody`, `SchemeType`, and `SchemeDefinite` do
not exist in code.** Where a section maps onto something that did
ship, an "✅ Shipped as" note points at the real type. Where a
section described an unshipped piece, a "⚠️ Not shipped" note marks
it.

---

## Motivation (still accurate)

Three concerns drove the redesign:

1. **Set-widening optionality.** Concept fields like
   `nickname: Option<Nickname>` should realize as `Some(value)`
   when the underlying fact exists and `None` when it doesn't. The
   storage layer never persists `None` — absence is realized at
   query time. `Optional<T>` is the set `T ∪ {Absent}` with the
   subtype rule `T ⊆ Optional<T>`.

2. **Generic formulas.** Today's engine has formulas like
   `math/sum`, `string/concat`, `to_string` that conceptually want
   to be polymorphic — `forall T: Numeric. (T, T) → T` — but the
   schema language has no way to express that, so they end up
   either type-erased (lossy) or one variant per concrete type
   (verbose). `Equality` was extracted into its own
   `Constraint::Equality` variant precisely because it couldn't
   fit the formula schema's "fixed input/output types" model.

3. **Range predicates and inference.** Future predicate
   constraints (`<`, `<=`, `starts_with`, etc.) want to *narrow
   the type* of a variable based on which predicate uses it.
   `starts_with` implies `String | Symbol`. The planner needs an
   inference framework that propagates type information across a
   rule body and feeds back into the storage layer (e.g. for
   index-range optimization).

The shipped work addresses (1) end-to-end and lays the unifier
groundwork for (3). Concern (2) is the part that didn't ship.

## v1 retrospective (still accurate)

v1 shipped working set-widening but accumulated debt:

1. Two parallel type taxonomies (schema-layer `Type` + descriptor
   `Option<ValueType>`).
2. Recursive `DynamicAttributeQuery::Optional` variant — `Option<
   Option<...>>` not prevented by the type signature.
3. Type-erasure loss in `From<Term<Option<T>>> for Term<Any>` —
   needed a parallel `optional_producers: HashSet<String>` to
   recover the lost info at planning time.
4. `UnwrapOr` builder type-erased at the boundary — accepted
   mismatched output types.
5. Marker traits as load-bearing structural fences rather than
   ergonomic bounds.

Each is a symptom of "the descriptor can't express optionality."
v2 fixes this at the root: the descriptor expresses optionality,
type variables, and constraint sets uniformly via the same
`Type` enum.

## v2 type system

### `Type` and `Definite`

```rust
pub enum Type {
    /// A definite shape. Subtype of `Optional(definite)` via the
    /// `T ⊆ Optional<T>` set-widening rule.
    Definite(Box<Definite>),

    /// Set-widened: `Definite ∪ {Absent}`. One level only —
    /// nested optionality is structurally impossible because the
    /// wrapped type is `Definite`, not `Type`. Optionality lives
    /// at the slot layer, not on type variables.
    Optional(Box<Definite>),
}

pub enum Definite {
    /// Atomic value type, possibly union over several primitive
    /// shapes. `PrimitiveSet::singleton(ValueType::String)` is
    /// "exactly String"; `PrimitiveSet::NUMERIC` is "any of
    /// UnsignedInt, SignedInt, Float."
    Primitive(PrimitiveSet),

    /// A type variable. Anonymous (per-site fresh) or named
    /// within a single type scheme. The variable's constraint
    /// (which primitive shapes it can take) lives in the
    /// `UnificationContext`'s constraint registry, keyed by
    /// `VarId`.
    Variable(VarId),

    // Future:
    // Record(BTreeMap<String, Type>),
    // Variant(BTreeMap<String, Definite>),
}
```

✅ **Shipped as** `type_system::Type` with variants
`Primitive(Primitive)` and `Composite(Primitive, BTreeSet<Composite>)`.
The shipped form is flatter than the proposed `Definite/Optional`
split: optionality is encoded as a `Nothing` bit in the same
`Primitive` bitfield, not as a wrapping `Optional` variant. A
shipped `Type` carrying `Nothing` *is* the set-widened thing — no
separate constructor. Nested optionality is still structurally
impossible: `Type::optional()` adds the bit; calling it again is
idempotent.

Key structural properties (mostly hold in the shipped form):

- **No `Any` variant.** ✅ The shipped form has no `Any` either.
  Untyped slots carry `None` at the `Option<Type>` boundary or
  use `Primitive::ALL` (any present value) / `Primitive::ANY` (any
  including `Nothing`).
- **Records and variants are reserved as `Composite` cases.** ✅ The
  shipped `Composite` enum has `Product(BTreeMap<String, Type>)` and
  `Variant{label, value}` — placeholders, not active in inference
  yet.

### `PrimitiveSet`

```rust
pub struct PrimitiveSet { bits: u16 }
```

✅ **Shipped as** `type_system::Primitive` (same shape, different
name). API matches the proposal closely:
`Primitive::ALL`/`NUMERIC`/`STRING_LIKE`/`COMPARABLE`, `intersect`,
`singleton`. The shipped version also exposes
`Primitive::NOTHING` and a `Primitive::ANY` (= `ALL | NOTHING`)
that the doc doesn't mention — these are what make the "Nothing
bit" encoding work.

### `VarId` and unification

```rust
pub struct VarId(u32);

pub struct UnificationContext {
    substitution: HashMap<VarId, Definite>,
    constraints: HashMap<VarId, PrimitiveSet>,
    next_id: u32,
}
```

✅ **Shipped as** `type_system::unifier::VarId` and
`type_system::unifier::Context`. The shipped `Context` also tracks
a `names: HashMap<String, VarId>` so the rule-level inference pass
can allocate one variable per named rule variable.

Operations:

- `fresh(constraint) -> VarId` — ✅ shipped.
- `unify(a, b)` Robinson unification — ✅ shipped.
- `apply(ty)` — ✅ shipped.
- `instantiate(scheme)` — ⚠️ **Not shipped.** Nothing constructs
  `TypeScheme`s, so nothing instantiates them.

Unification rules — all the rules between concrete `Type`s and
between variables and concretes are shipped. The `Definite ≡
Optional` cases collapsed into the shipped form's intersection
semantics: optionality is just a bit on the primitive set, and
intersection narrows it correctly.

### Type schemes

```rust
pub struct TypeScheme { ... }
pub enum SchemeBody { ... }
pub enum SchemeType { ... }
pub enum SchemeDefinite { ... }
```

⚠️ **Not shipped.** No type with any of these names exists in
code. Formulas keep their existing `Schema` (no quantified
variables); inference happens *over a rule's variables*, not over
a formula's. This is the biggest deviation from the design.

**Why it didn't ship:** the PR's actual mission was to make the
optional-fields case work end-to-end (stop emitting spurious
`Absent` rows when sibling premises narrow the variable). That
requires inference over a rule's variables. It does not require
polymorphic formulas. Adding the `TypeScheme` machinery would have
meant building infrastructure with no current consumer — no
shipped formula is yet declared polymorphic.

**What would need to change to ship them:** introduce a
`TypeScheme` type, attach one to each formula registration in the
formula registry, add `Context::instantiate` to allocate fresh
`VarId`s from a scheme's quantified variables, and have the
planner invoke instantiation when it picks up a formula premise.
The unifier already handles everything downstream.

### Wire format

✅ **Shipped.** Type schemes are not serialized (they're Rust-side
registry data). Concept descriptors and attribute queries serialize
their types directly via the existing JSON format. The shipped
`Type` enum has a serde representation; it's not the exact shape
the doc proposes (`{"definite": ...}` / `{"optional": ...}`) but
the underlying property — "concrete types only, no variables on
the wire" — holds.

## Rule analysis

```rust
pub struct RuleAnalysis {
    types: HashMap<String, Type>,
    producers: HashMap<String, Vec<ProducerEntry>>,
    refinements: HashMap<String, ScanHint>,
}
```

✅ **Shipped as** `rule::analyzer::AnalyzedRule` with
`types: Arc<TypeEnv>` plus a `DependencyGraph` instead of
`producers`/`refinements`. The shipped form:

- Carries the inferred environment via `Arc<TypeEnv>` (shared
  across the analyzed premises).
- Builds a `DependencyGraph` (per-premise `binds`/`needs` plus
  precomputed `requires[]` edges) — broader than the proposed
  `producers` map. Currently built but not yet consumed by the
  planner (left for a follow-up).
- Doesn't carry `refinements` — range-predicate scan hints are
  deferred along with the predicates themselves.

`RuleAnalysis::build(conclusion, premises) -> Result<Self, TypeError>`

✅ **Shipped as** `rule::analyzer::analyze(conclusion, &steps) ->
Result<AnalyzedRule, AnalysisError>`. The phases match the doc:
inference, then required-head check, then Coalesce contract
validation. Step (2) — instantiating formula schemes — isn't done
because schemes don't ship. Step (5)'s Slice-7 checks ship via
`AnalysisError::RequiredHeadFromOptional` and the unifier's
constraint-conflict errors.

## Descriptor layer

```rust
pub trait TypeDescriptor: ... {
    const KIND: Option<Type>;
    fn kind(&self) -> Type;
}
```

✅ **Shipped.** `TypeDescriptor::kind` returns `Option<Type>`
(slightly different signature than `Type` — `None` means "no
static info, leave to the unifier"). `OptionalOf<D>` ships as
described.

## Attribute query layer: `Resolution` policy

```rust
pub enum Resolution {
    Required,
    Optional,
}
```

✅ **Shipped.** With one refinement: `Resolution` is *derived* from
`self.is.is_optional()` rather than stored as a field. The shipped
`AttributeQueryAll::resolution()` returns
`Resolution::Optional` iff the `is` term's kind admits `Nothing`.
This makes the rule-level narrowing automatic — once the planner
narrows the term's kind, the resolution flips with it, without
needing a separate path.

## Macro layer

✅ **Shipped.** The `#[derive(Concept)]` macro emits
`Term<Option<<T as Attribute>::Type>>` for `Option<T>` fields.
Dispatch happens through the `ConceptField` trait with two
blanket impls (`for N` and `for Option<N>`) leveraging `Option`'s
`#[fundamental]` annotation — no syntactic detection of the
`Option` ident.

## Coalesce / `unwrap_or`

⚠️ **Partially shipped, but not as a formula.** `Coalesce` ships as
its own constraint variant (`Constraint::Coalesce`), not as a
formula with a `UNWRAP_OR_SCHEME`. Its type contract is checked at
rule-compile time via `Coalesce::validate(ctx)`, which uses the
shipped `unifier::Context` to verify
`source: Optional<α>, fallback: α, is: α` — but the contract is
hand-rolled inside `validate`, not declared as a reusable scheme.

This is consistent with the broader "no schemes" decision: without
`TypeScheme` infrastructure, Coalesce's polymorphism is expressed
ad-hoc rather than via a registered scheme.

## Slice 7 enforcement

✅ **Shipped (mostly).**

- `RequiredHeadFromOptional` — shipped as
  `AnalysisError::RequiredHeadFromOptional`. The shipped check
  reads the inferred `TypeEnv` and flags any conclusion variable
  whose inferred kind admits `Nothing`.
- `NegationOnOptional` — not shipped as a separate check. The
  shipped semantics: negations don't *contribute* to inference
  (they're filters), and `apply_types` rewrites their terms with
  the rule-level kinds — so a negation reading an optional binding
  sees the narrowed kind, not the user's local one.
- `ConceptOnlyOptionalFields` — not shipped. Captured as task #80.

## Marker traits

✅ **Shipped.** `ScalarType`/`ProductType`/`VariantType`/
`OptionalType`/`DefiniteType`-family bounds prevent
`Term<Option<Option<U>>>` and `Term<Option<Any>>` at the Rust API.

## Implementation phases

The implementation didn't follow the proposed numbered phases
exactly. The actual shipped order is captured in the commit log on
`feat/type-inference` (formerly `feat/meet-into-plan`) and
summarized in [`rule-pipeline.md`](./rule-pipeline.md).

## What's deferred to follow-up branches

- **Type schemes for polymorphic formulas** (`TypeScheme`,
  `SchemeBody`, `SchemeType`, `instantiate`). The doc's design
  intent. The largest deferred chunk.
- **Records and variants in active inference** — placeholders ship
  as `Composite::Product`/`Composite::Variant`; the unifier
  doesn't recurse into them yet.
- **Generic formulas at runtime** — formula impls that dispatch on
  the unified type (e.g. `Sum<T>` with separate addition paths
  for `u32`, `i64`, `f64`). Conditional on type schemes shipping.
- **Range predicates** — new constraint variants (`<`, `<=`,
  `starts_with`, etc.) and their contribution to scan refinements.
- **Cross-rule type inference** — propagating type variables
  across rule bodies that span multiple formulas. Within-rule
  inference ships; across-rule does not.
- **`get-some`** — variant-elimination premise. Builds on variant
  types.
- **`ConceptOnlyOptionalFields` rejection** — task #80.
- **Dependency graph as planner input** — the shipped
  `DependencyGraph` is built but unread; the planner still uses
  `Candidate::update`'s schema-walking loop.

## Acceptance criteria

The original criteria were aspirational for the full design. The
shipped subset:

- ✅ Unifier with full algorithm coverage for the cases that ship:
  identity, variable-variable, variable-concrete, occurs check,
  constraint conflict.
- ⚠️ "Instantiation independence, generalization" — not shipped
  (no schemes).
- ✅ All v1 end-to-end tests pass.
- ✅ New tests cover unification corner cases at the *rule* level
  (not formula-scheme level).
- ⚠️ "Generic formula declarations" — not shipped.
- ⚠️ "Range-predicate-style narrowing" — infrastructure ready
  (`TypeEnv`, dependency graph) but no predicates use it yet.
- ✅ Compile-fail doctests for `Term<Option<Option<U>>>` and
  `Term<Option<Any>>`.
- ✅ Workspace clippy clean, fmt clean.
- ✅ Design doc reflects the final shape — *this* split.

## Open questions to settle during implementation

The questions the doc closed during implementation:

1. **`TypeDescriptor::KIND` const evaluation.** Settled as
   non-const `kind()` returning `Option<Type>`.
2. **Anonymous variable lifetime.** Settled: per-rule
   `unifier::Context`; named variables allocated via
   `var_for_name(name)` on first reference.
3. **Error messages.** Settled: `InferenceError::Conflict`
   includes the offending variable name; the `Compile::compile`
   layer wraps it as `TypeError::TypeInference { reason }`.
