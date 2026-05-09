# Optional Fields & Type System — v2 Design

This document specifies the type-system rework on
`feat/optional-fields-v2`. It supersedes v1 (commit `d6629bf7` on
`feat/optional-fields`) and the original v2 sketch at
`5cc533ff`. The current direction is **rank-1 parametric
polymorphism with Damas-Milner type inference** — Roc/Elm style,
adapted to our Datalog-flavored query engine.

This is a design contract for the v2 branch. Implementation
follows in subsequent commits.

## Motivation

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

All three problems share a root: **the schema language can't
express type variables**. Without type variables, optionality
becomes a parallel taxonomy, generic formulas can't be declared,
and inference has nowhere to write its results.

v2 introduces type variables as first-class citizens of the type
system, plus a Damas-Milner unifier that resolves them at rule
compile time. Optionality is one specific use of the unifier
(`Optional<T>` is a slot type that admits `Definite(T) ∪
{Absent}`); generic formulas are another (`Sum<T: Numeric>` is a
formula scheme); range predicates a third (constraint set on
variables).

## v1 retrospective

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

Key structural properties:

- **`Optional` wraps `Definite`**, not `Type`. Nested optionality
  unrepresentable.
- **No `Any` variant.** What v1 called `Any` is `Definite::
  Variable(fresh)` with constraint `PrimitiveSet::ALL` —
  "anonymous variable that can take any primitive shape."
- **Records and variants are reserved as `Definite` constructors.**
  Future PRs add them; the existing recursion via `Box<Definite>`
  accommodates them without reshape.

### `PrimitiveSet`

A bitfield over `ValueType` variants. Constraints are sets, not
single values, so type variables can carry kind-level constraints
(`NUMERIC`, `STRING_LIKE`, etc.) and unification can intersect
them.

```rust
pub struct PrimitiveSet { bits: u16 }

impl PrimitiveSet {
    pub const fn singleton(vt: ValueType) -> Self;
    pub fn intersect(self, other: Self) -> Option<Self>;  // None = empty
    pub fn includes(self, other: Self) -> bool;            // superset
    pub fn as_singleton(self) -> Option<ValueType>;        // when narrowed
    pub fn iter(self) -> impl Iterator<Item = ValueType>;

    pub const ALL: Self;          // every primitive
    pub const NUMERIC: Self;      // UnsignedInt | SignedInt | Float
    pub const STRING_LIKE: Self;  // String | Symbol
    pub const COMPARABLE: Self;   // NUMERIC ∪ STRING_LIKE ∪ Entity ∪ ...
}
```

### `VarId` and unification

```rust
pub struct VarId(u32);

pub struct UnificationContext {
    /// Substitution: `VarId` → resolved type.
    substitution: HashMap<VarId, Definite>,
    /// Constraint registry: per-variable `PrimitiveSet`.
    constraints: HashMap<VarId, PrimitiveSet>,
    /// Fresh-id allocator.
    next_id: u32,
}
```

Operations:

- `fresh(constraint) -> VarId` allocates a new variable.
- `unify(a, b)` Robinson unification with constraint
  intersection. Errors on constraint conflict, occurs check, or
  primitive mismatch.
- `apply(ty)` recursively walks `ty`, replacing each
  `Variable(id)` with the substituted type.
- `instantiate(scheme)` allocates fresh `VarId`s for the
  scheme's quantified variables, returning a body type with the
  substitutions applied.

Unification rules:

- `Variable(x) ≡ Variable(y)`: bind them; intersect constraints.
- `Variable(x) ≡ Definite(p)`: check `p`'s primitive belongs to
  `x`'s constraint; substitute `x := Definite(p)`. Run occurs
  check.
- `Variable(x) ≡ Optional(p)`: an Optional cannot satisfy a
  variable that's used in a `Definite` slot. The unifier records
  the slot's optionality at the slot level (not on the variable),
  so this case is "merge constraints; the slot wrapping
  determines optional vs definite per-use."
- `Definite(a) ≡ Definite(b)`: structural unify on `a` and `b`.
- `Optional(a) ≡ Optional(b)`: structural unify on `a` and `b`.
- `Definite(a) ≡ Optional(b)`: strictest wins — narrow to
  `Definite(unify(a, b))`. The optional consumer's tolerance for
  Absent becomes dead code, but that's not an error.

### Type schemes

A formula declares a `TypeScheme` — its rank-1 polymorphic type:

```rust
pub struct TypeScheme {
    /// Quantified type variables and their constraints. Names
    /// scope-local to this scheme.
    quantified: Vec<(VarName, PrimitiveSet)>,
    /// The body type, referencing quantified variables by
    /// `VarName`. Instantiation replaces these with fresh
    /// `VarId`s.
    body: SchemeBody,
}

pub enum SchemeBody {
    /// A function-like signature: parameter names → types.
    Schema(BTreeMap<String, SchemeType>),
    /// A single value type (rare, for non-function values).
    Type(SchemeType),
}

pub enum SchemeType {
    /// Reference to a quantified variable.
    Bound(VarName),
    /// Anonymous fresh variable (used for "any" slots that don't
    /// share with other slots).
    Fresh(PrimitiveSet),
    /// Concrete shape: as `Type` but with `SchemeType` inside
    /// `Definite::Variable(VarName)` instead of `VarId`.
    Definite(Box<SchemeDefinite>),
    Optional(Box<SchemeDefinite>),
}

pub enum SchemeDefinite {
    Primitive(PrimitiveSet),
    Variable(VarName),
}
```

Schemes are **static, compile-time constants** in formula module
definitions. Example for `math/sum`:

```rust
const SUM_SCHEME: LazyLock<TypeScheme> = LazyLock::new(|| TypeScheme {
    quantified: vec![(VarName::new("T"), PrimitiveSet::NUMERIC)],
    body: SchemeBody::Schema(btreemap! {
        "left".into()  => SchemeType::Definite(Box::new(SchemeDefinite::Variable(VarName::new("T")))),
        "right".into() => SchemeType::Definite(Box::new(SchemeDefinite::Variable(VarName::new("T")))),
        "is".into()    => SchemeType::Definite(Box::new(SchemeDefinite::Variable(VarName::new("T")))),
    }),
});
```

`unwrap_or` carries `Optional<T>` for its source slot:

```rust
const UNWRAP_OR_SCHEME: LazyLock<TypeScheme> = LazyLock::new(|| TypeScheme {
    quantified: vec![(VarName::new("T"), PrimitiveSet::ALL)],
    body: SchemeBody::Schema(btreemap! {
        "source".into()  => SchemeType::Optional(Box::new(SchemeDefinite::Variable(VarName::new("T")))),
        "default".into() => SchemeType::Definite(Box::new(SchemeDefinite::Variable(VarName::new("T")))),
        "output".into()  => SchemeType::Definite(Box::new(SchemeDefinite::Variable(VarName::new("T")))),
    }),
});
```

### Wire format

Type schemes are **not serialized**. They're Rust-side metadata
attached to formula identifiers via a registry. The wire format
for a formula application stays as today:

```json
{ "assert": "math/sum", "where": { "left": ..., "right": ..., "is": ... } }
```

When the planner sees `"math/sum"`, it looks up `SUM_SCHEME` from
the formula registry, instantiates it (allocates fresh `VarId`s
for `T`), and unifies against the rule body.

For non-formula schemas (concept descriptors, attribute queries,
etc.) the wire format is the existing one. These don't have
quantifiers — they declare concrete `Type` values. The serde
representation of `Type` itself:

```json
{ "definite": { "primitive": ["Text"] } }      // exactly String
{ "definite": { "primitive": ["UnsignedInt", "SignedInt"] } }  // narrower numeric
{ "optional": { "primitive": ["Text"] } }      // String or Absent
```

`Variable` in concrete types only appears at runtime (in
descriptors carrying inferred types after unification). The wire
format for concept descriptors only emits `Primitive(...)` and
`Optional(Primitive(...))` — no variable references.

## Rule analysis

`RuleAnalysis` is the per-rule output of the unification pass:

```rust
pub struct RuleAnalysis {
    /// Type intersection per rule-level variable, with
    /// substitution applied.
    types: HashMap<String, Type>,
    /// Producers per variable: which premise (by index)
    /// declared the variable in a `Derived` slot, and what type
    /// did it declare. Useful for diagnostics and Slice-7-equivalent
    /// checks.
    producers: HashMap<String, Vec<ProducerEntry>>,
    /// Scan refinements from range predicates. Empty initially;
    /// populated as range-predicate constraints are added.
    refinements: HashMap<String, ScanHint>,
}
```

`RuleAnalysis::build(conclusion, premises) -> Result<Self,
TypeError>` walks the rule body:

1. Initialize `UnificationContext`.
2. For each premise, look up its scheme (or use its concrete
   schema for non-polymorphic premises). Instantiate fresh
   `VarId`s for any quantified variables.
3. For each parameter that's a named rule-level variable, unify
   the slot's instantiated type against the rule's accumulated
   type for that variable.
4. After all premises processed, apply the final substitution to
   every rule-level variable's type.
5. Validate Slice-7-equivalent rules (Negation can't read a slot
   that ended up `Optional`; required head fields must be
   `Definite`; concept must have at least one required `with`
   field).
6. Return `RuleAnalysis` with substituted types.

Stored on the compiled `DeductiveRule` so downstream consumers
(planner, query-time optimizer) can read inferred types.

## Descriptor layer

`TypeDescriptor` carries the new `Type` instead of v1's
`Option<ValueType>`:

```rust
pub trait TypeDescriptor: ... {
    /// Statically known type, if monomorphic.
    /// `None` means "anonymous variable" (fresh at runtime).
    const KIND: Option<Type>;

    fn kind(&self) -> Type;
}
```

Concrete descriptors (`Text`, `Boolean`, etc.) report `Type::
Definite(Primitive(singleton(vt)))`. The descriptor formerly
known as `Any` reports an anonymous variable with `ALL`
constraint at runtime — i.e., its `kind()` allocates a fresh
`VarId`. (Or we keep a special "anonymous" sentinel; design
detail to settle in implementation.)

`OptionalOf<D>` ZST handles `Term<Option<U>>`:

```rust
pub struct OptionalOf<D: TypeDescriptor>(PhantomData<D>);

impl<D: TypeDescriptor> TypeDescriptor for OptionalOf<D> {
    fn kind(&self) -> Type {
        match D::KIND {
            Some(Type::Definite(d)) => Type::Optional(d.clone()),
            // Anonymous-variable case: Optional<Variable(fresh)>
            None => Type::Optional(Box::new(Definite::Variable(fresh()))),
            Some(other) => other,  // shouldn't happen given marker bounds
        }
    }
}
```

## Attribute query layer: `Resolution` policy

v2 drops the recursive `DynamicAttributeQuery::Optional`
variant. Optionality becomes a `Resolution` field on the
existing `All`/`Only` variants:

```rust
pub enum Resolution {
    /// Standard EAV: zero rows on miss.
    Required,
    /// Yield one Absent fallback row on miss.
    Optional,
}
```

Schema reflects optionality through the `is` term's descriptor's
`kind()`. If `is: Term<Option<U>>`, `is.kind()` returns
`Type::Optional(...)`, and the schema declares it directly. No
recursive wrapper, no parallel `value_type` pin.

## Macro layer

For `Option<T>` fields on `#[derive(Concept)]`, the macro emits
`Term<Option<<T as Attribute>::Type>>`. The Rust-level
`Option<...>` wrapper is the typed signal; users get
`unwrap_or` on the field, set-widening conversions, and
compile-time rejection of nested `Option`. The realize impl
pattern-matches on `Binding`.

## Coalesce / `unwrap_or`

`Coalesce` becomes a formula with `UNWRAP_OR_SCHEME` as its
type scheme — no longer a separate `Constraint::Coalesce`
variant. The `UnwrapOr<T: DefiniteType>` Rust-side builder
constructs the formula application; the typed bound flows
through Rust generics.

When wire-format rules arrive without Rust types, the planner
applies `UNWRAP_OR_SCHEME` at unification time. Type errors
("output doesn't match source type") surface as unification
failures.

## Slice 7 enforcement

v1 had three checks:

1. `NegationOnOptional` — Negation reads optional binding.
2. `RequiredHeadFromOptional` — required head field bound by
   optional producer.
3. `ConceptOnlyOptionalFields` — concept with empty `with`.

v2 expresses (1) and (2) through `Type::accepts`-equivalent
checks during `RuleAnalysis::build`. Negation premises declare
their parameters as `Definite(...)` (not `Optional`); if any
positive producer's substituted type is `Optional(...)`,
unification fails with a clear error. Required head fields
similarly declare `Definite(...)`. Check (3) is unchanged.

## Marker traits

`ScalarType`/`ProductType`/`VariantType`/`OptionalType`/
`DefiniteType` family stays as ergonomic Rust-level bounds. They
prevent `Term<Option<Option<U>>>` and `Term<Option<Any>>` at the
Rust API.

## Implementation phases

1. **Step 1 (amend)** — `PrimitiveSet`, `VarId`, `Type`,
   `Definite`, `TypeScheme`, `UnificationContext`, `unify`,
   `apply`, `instantiate`, `generalize`, occurs check. Tests for
   the algorithm.
2. **Step 2 (committed)** — `Binding<Value>` at Match layer.
3. **Step 3** — Descriptor enriched to carry the new `Type`.
4. **Step 4** — Collapse v1's `schema::Type` into the new `Type`.
5. **Step 4.5** — `RuleAnalysis::build` with `UnificationContext`
   integration.
6. **Step 5** — `Resolution` policy on `DynamicAttributeQuery`.
7. **Step 6** — Concept projection emits `Resolution::Optional`
   for `maybe` fields.
8. **Step 7** — Macro emits typed `Term<Option<U>>` for
   `Option<T>` fields.
9. **Step 8** — Coalesce as a formula with `UNWRAP_OR_SCHEME`.
10. **Step 9** — Marker traits family.
11. **Step 10** — End-to-end tests; Slice 7 enforcement via
    unification.
12. **Step 11** — Docs, lint, fmt, full workspace test.

## What's deferred to follow-up branches

- **Records and variants** — `Definite::Record(...)`,
  `Definite::Variant(...)` constructors. The recursive `Box<
  Definite>` accommodates them without reshape.
- **Generic formulas at runtime** — formula impls that dispatch
  on the unified type (e.g. `Sum<T>` with separate addition
  paths for `u32`, `i64`, `f64`). v2 lays the type-system
  foundation; runtime monomorphization is a follow-up.
- **Range predicates** — new constraint variants (`<`, `<=`,
  `starts_with`, etc.) that carry type schemes and contribute to
  `RuleAnalysis::refinements`. The plumbing is in place; the
  predicates themselves are a separate slice.
- **Cross-formula type inference** — propagating type variables
  across rule bodies that span multiple formulas. v2 unifies
  within a single rule's `UnificationContext`; cross-rule
  inference is conceptually more.
- **`get-some`** — variant-elimination premise. Builds on
  variant types.

## Acceptance criteria

- Damas-Milner unifier with full algorithm coverage: identity,
  variable-variable, variable-concrete, occurs check, constraint
  conflict, instantiation independence, generalization.
- All v1 end-to-end tests pass under v2.
- New tests cover unification corner cases, generic formula
  declarations, range-predicate-style narrowing.
- Compile-fail doctests for `Term<Option<Option<U>>>` and
  `Term<Option<Any>>`.
- Workspace clippy clean, fmt clean, `test:native:debug` green.
- Design doc reflects the final shape.

## Open questions to settle during implementation

1. **`TypeDescriptor::KIND` const evaluation.** The `Box<
   Definite>` makes `const` tricky. Likely fall back to non-const
   `kind()` function. Decision in Step 1.
2. **Anonymous variable lifetime.** When a descriptor's `kind()`
   returns `Definite::Variable(fresh)`, who owns the `VarId`? It
   needs an `UnificationContext` to be meaningful. Likely: every
   rule-compile pass has its own context, and "anonymous"
   variables are allocated on demand during unification, not
   eagerly at descriptor read time. Decision in Step 3.
3. **Error messages.** Unification failures need source location
   ("variable `?x` in premise 2 was inferred as String but
   premise 3 demands UnsignedInt"). The unifier signature
   accepts a context argument so errors can attribute their
   source. Decision in Step 1.
