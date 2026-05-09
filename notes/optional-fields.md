# Optional Fields — v2 Design

This document specifies the second design pass for set-widening
optional concept fields in dialog-db. It supersedes a prior
implementation (commit `d6629bf7` on `feat/optional-fields`) which
shipped a working but structurally awkward solution. The lessons
from v1 are folded into v2.

This document is the design contract for `feat/optional-fields-v2`.
Implementation follows in subsequent commits.

## Motivation

Concepts model entities as records with named, typed fields. Today
every field is required: a query for a concept with field `name`
finds only entities that have a `name` fact. Some real-world
attributes are inherently optional — a `Person` may or may not have
a `nickname`, an order may or may not have a `discount_code`. Users
want to write:

```rust
#[derive(Concept)]
struct Person {
    this: Entity,
    name: Name,
    nickname: Option<Nickname>,  // optional
}
```

and have `Query::<Person>::default()` return entities that have a
`name` fact, with `nickname` populated as `Some(value)` when the
fact exists and `None` when it doesn't. The fact for a `None` field
is **never persisted** — absence is realized at query time, not
stored.

This is the same pattern Datomic calls `get-else` and Hickey
described as "set widening": `Optional<T>` is the set
`T ∪ {Absent}`, with the subtype rule `T ⊆ Optional<T>`. A `T` value
flows freely into an `Optional<T>` slot.

## v1 retrospective

v1 (commit `d6629bf7`) shipped working set-widening across nine
slices: `Binding<Value>` at the Match layer, a schema-level `Type`
enum with `Required`/`Optional`/`Any`, `AttributeQueryOptional`
streaming wrapper, `Constraint::Coalesce`, marker-trait taxonomy
with structural rejection of `Term<Option<Option<U>>>`, and end-to-end
tests proving both `Some(...)` and `None` realize correctly.

It works. The native test suite is 1361 green. The user-visible
behavior is correct.

But the implementation accumulated structural debt that motivates
v2:

1. **Two parallel type taxonomies.** A schema-layer `Type` enum
   (`Any | Required(ValueType) | Optional(ValueType)`) and a
   descriptor-layer `Option<ValueType>`. The same concept expressed
   twice in different shapes, with conversion sites between them.
2. **`AttributeQueryOptional` as a recursive `DynamicAttributeQuery`
   variant.** `Optional(Optional(...))` is structurally meaningless
   but the type permits it. Fixed at construction by `try_new`/
   `try_typed` validation, but the type signature doesn't prevent
   the case.
3. **Type-erasure loss.** `From<Term<Option<T>>> for Term<Any>`
   strips the optional tag. Once erased, the runtime can't
   introspect "this term might be Absent." The Slice 7 enforcement
   of "Negation can't read optional bindings" needed a parallel
   `optional_producers: HashSet<String>` on the side, because the
   meet algebra alone couldn't see through the erasure.
4. **`UnwrapOr` type-erased at the boundary.** `Term<Option<String>>`
   ::unwrap_or accepts `Term<Entity>` for output without complaint —
   the type system doesn't enforce source/default/output share a
   value type.
5. **Marker traits as the only structural fence.** `OptionalType`
   and `DefiniteType` exist purely to prevent
   `Term<Option<Option<U>>>` at the Rust API. The runtime descriptor
   couldn't enforce the same invariant, so the markers were
   load-bearing — and proliferated across many bound clauses.

Each of these is a symptom of the same root cause: **the descriptor
can't express optionality**, so the schema layer added an `Optional`
variant on its own, and the rest of the codebase built bridges
between the two.

v2 fixes the root: the descriptor *does* express optionality. Then
the schema-layer enum collapses, the recursive variant goes away,
the erasure preserves type info, the `UnwrapOr` typing tightens, and
the marker traits become ergonomic bounds rather than structural
fences.

## v2 type system

### `Type` enum

```rust
pub enum Type {
    /// Dynamic top — value type unknown until runtime. Used at
    /// type-erased boundaries (e.g. wire-format `Term<Any>`) and as
    /// the planner's "I don't know yet" placeholder.
    Any,

    /// A definite shape. Subtype of `Optional(definite)` via the
    /// `T ⊆ Optional<T>` set-widening rule.
    Definite(Box<Definite>),

    /// Set-widened: `Definite ∪ {Absent}`. One level only — nested
    /// optionality is structurally impossible because the wrapped
    /// type is `Definite`, not `Type`. This matches the Rust-side
    /// `T: DefiniteType` bound on `Term<Option<T>>`.
    Optional(Box<Definite>),
}

pub enum Definite {
    /// Atomic value type. Today this is the only Definite variant;
    /// future work adds Record (concept-as-value) and Variant
    /// (tagged union) without reshaping the enum.
    Primitive(ValueType),
}
```

Key properties:

- **Recursive via `Box`**, not `const`. We accept the heap-allocation
  cost so the enum can grow `Record(BTreeMap<String, Type>)` and
  `Variant(BTreeMap<String, Definite>)` without changing the shape
  of existing code.
- **No `const` anywhere in the type system.** The current codebase
  declares `const TYPE: Option<ValueType>` on `TypeDescriptor` but
  never uses it in const contexts (every read site is a runtime
  expression). v2 drops the const requirement to enable `Box`.
- **`Optional` wraps `Definite`, not `Type`.** This makes
  `Optional(Optional(...))` and `Optional(Any)` structurally
  unrepresentable at the data level. The Rust marker traits
  enforce the same invariant at the type level for typed code.
- **`Any` sits alongside `Definite` and `Optional`**, not inside
  `Definite`. `Optional(Any)` is therefore unrepresentable —
  matching the Rust-side rejection of `Term<Option<Any>>`.

### Type algebra

Two operations:

- **`meet(a, b) -> Result<Type, MeetError>`** — the lattice
  intersection. Used to combine multiple declarations of the same
  variable into a single narrowest type. Commutative, associative,
  identity at `Any`. Conflicting value types reject.
- **`accepts(consumer, producer) -> bool`** — one-way subtype
  check. `Definite(T)` accepts `Definite(T)`. `Optional(T)` accepts
  both `Definite(T)` and `Optional(T)` (set widening). `Any`
  accepts anything. `Definite(T)` does NOT accept `Optional(T)` —
  this is the rule that makes "Negation reads optional binding" a
  type error.

The v1 rule was "`Required ∧ Optional → Required` (strictest wins)
plus a parallel `optional_producers` set." v2 replaces this with
the cleaner formulation: **a consumer's declared type must
`accept` every producer's declared type.** No parallel set —
the type system itself encodes the constraint.

### Wire format

`Type` serializes as a tagged union:

```json
"any"
{ "definite": { "primitive": "Text" } }
{ "optional": { "primitive": "Text" } }
```

`Definite::Primitive(ValueType::String)` flattens to
`{ "primitive": "Text" }` reusing the existing `ValueType` wire
form. The wrapping `definite`/`optional` tag is added so future
`Definite::Record(...)` and `Definite::Variant(...)` extensions
fit without rewriting the on-disk format.

This is a wire-format break from v1's `Option<ValueType>` shape on
`Field::content_type`. Acceptable since the project is pre-1.0 and
no Schema is currently persisted across versions.

## Descriptor layer

### Descriptors carry `Type`

```rust
pub trait TypeDescriptor: Clone + Debug + Default + ... {
    /// The statically known type, if monomorphic.
    /// `None` for the dynamic-top descriptor (Any).
    const KIND: Option<Type>;

    /// The runtime type. For monomorphic descriptors equals
    /// `KIND.unwrap()`; for `Any` returns the wrapped tag.
    fn kind(&self) -> Type;
}
```

`KIND` replaces v1's `TYPE: Option<ValueType>`. The change from
`Option<ValueType>` to `Option<Type>` is the central enrichment:
descriptors now carry the full type taxonomy, not just the leaf
value type.

`const KIND` works because `Type::Definite(Box::new(Definite::
Primitive(vt)))` is constructible in const context (Rust 1.83+
supports `const fn Box::new` for some cases) — *or* we use a
non-const `KIND_FN()` approach if Box::new const stabilization
isn't available. Final choice depends on toolchain check.

If neither path works, the fallback is to give up `const` entirely
and make `KIND` a non-const associated function. The 13 read sites
in v1 are all runtime expressions; non-const has no behavior cost.

### `Any` carries `Type`, not `Option<ValueType>`

```rust
pub struct Any(pub Type);
```

`Any(Type::Any)` represents fully-erased terms (no static info).
`Any(Type::Definite(Box::new(Definite::Primitive(vt))))` represents
a term that was originally `Term<U>` for some scalar `U`.
`Any(Type::Optional(Box::new(Definite::Primitive(vt))))` represents
a term that was originally `Term<Option<U>>`.

**This is the key fix.** v1's `Any(Option<ValueType>)` lost the
`Optional` tag at the erasure boundary. v2 preserves it.

### `Term<Option<U>>` descriptor

```rust
#[derive(Default, Clone, Debug, ...)]
pub struct OptionalOf<D: TypeDescriptor>(PhantomData<D>);

impl<D: TypeDescriptor> TypeDescriptor for OptionalOf<D> {
    const KIND: Option<Type> = match D::KIND {
        Some(Type::Definite(d)) => Some(Type::Optional(d.clone())),
        // Other shapes prevented by the Rust trait bound `T:
        // DefiniteType`; const match is exhaustive.
        _ => None,
    };
    fn kind(&self) -> Type {
        match D::KIND {
            Some(Type::Definite(d)) => Type::Optional(d.clone()),
            _ => Type::Any,
        }
    }
}

impl<T: DefiniteType> Typed for Option<T> {
    type Descriptor = OptionalOf<<T as Typed>::Descriptor>;
}
```

`Term<Option<String>>` has `Descriptor = OptionalOf<Text>`. At
runtime its `kind()` returns
`Type::Optional(Definite::Primitive(ValueType::String))`. The
erasure into `Term<Any>` constructs
`Any(Type::Optional(Definite::Primitive(ValueType::String)))` —
the optionality survives.

### Schema collapse

v1's `schema::Type` enum is deleted. `Field::content_type` becomes
`Type` (the new unified one).

```rust
pub struct Field {
    description: String,
    content_type: Type,
    requirement: Requirement,
    cardinality: Cardinality,
}
```

Conversion sites — `From<&ConceptDescriptor> for Schema`,
`From<&Cells> for Schema`, `Constraint::Equality::schema`,
`AttributeQuery::schema` — populate `content_type` from the
underlying term's `descriptor().kind()`. No more
`.into()`-coercion ambiguity between `Option<ValueType>` and the
old `schema::Type` enum.

## Attribute-query layer

### `Resolution` policy on `DynamicAttributeQuery`

v1 introduced a recursive `DynamicAttributeQuery::Optional(Box<...>)`
variant wrapping an `AttributeQueryOptional` struct. v2 drops the
variant entirely. Optionality becomes a policy switch on the single
attribute-query type:

```rust
pub enum DynamicAttributeQuery {
    All(AttributeQueryAll),
    Only(AttributeQueryOnly),
    // No Optional variant.
}

// Cardinality + Resolution = behavior matrix.
pub enum Resolution {
    /// Standard EAV semantics: zero rows on miss.
    Required,
    /// One row per input — Present from the lookup if any fact
    /// exists, else Absent fallback.
    Optional,
}
```

`Resolution` lives on `AttributeQueryAll` and `AttributeQueryOnly`
as a field, not as a wrapping enum. Each existing variant carries
the policy.

### Schema reflects optionality through the descriptor

`AttributeQueryAll::schema()` declares `is`'s `content_type` from
`is.descriptor().kind()`. If `is: Term<Option<U>>`, the descriptor
reports `Type::Optional(...)` and the schema reflects that
directly. No widening logic on the wrapper. No `value_type: Option<
ValueType>` pin. The descriptor is the single source of truth.

If the user constructed the query with `Resolution::Optional` but
passed `is: Term<U>` (non-optional Rust type), the descriptor
reports `Type::Definite(...)` while the runtime evaluates with
optional semantics. That's a structural mismatch — the constructor
should reject it. We add a constructor-time check: `Resolution::
Optional` requires `is.descriptor().kind()` to be either
`Type::Optional(_)` or `Type::Any` (so wire-format
`Term<Any>` rules can flow through and have their resolution
checked at planner time).

### Concept projection emits typed Optional resolvers

`From<&ConceptDescriptor> for DeductiveRule` walks both `with` and
`maybe` attribute maps:

- `with` attributes get `Resolution::Required` queries.
- `maybe` attributes get `Resolution::Optional` queries.

The `is` term in each case is `Term::<Option<vt>>::var(...)` for
maybe fields (typed) or `Term::<vt>::var(...)` for required fields.

## Macro layer

### `Term<Option<U>>` for `Option<T>` fields

The `#[derive(Concept)]` macro:

- For required `T` fields: emits `Term<<T as Attribute>::Type>`.
  Same as today.
- For `Option<T>` fields: emits
  `Term<Option<<T as Attribute>::Type>>`. The Rust-level
  `Option<...>` wrapper is the typed signal; users get
  `unwrap_or` on the field, set-widening conversions in/out, and
  compile-time rejection of nested `Option`.

The realize impl pattern-matches on `Binding`:

```rust
nickname: match source.lookup(&Term::<Any>::from(&self.nickname))? {
    Binding::Present(value) => Some(Nickname(value.try_into()?)),
    Binding::Absent => None,
}
```

`Term::<Any>::from(&self.nickname)` is the borrow-form erasure that
preserves `Type::Optional(...)` in the descriptor (Slice 6 of v1
introduced this conversion; v2 keeps it but with type-faithful
preservation).

### `From<Term<U>> for Term<Option<U>>` set-widening

Kept from v1. A user passing `Term::<String>::var("x")` where the
macro expects `Term<Option<String>>` gets implicit widening via
`From`. The conversion is structurally trivial: descriptor changes
from `Text` to `OptionalOf<Text>`, value preserved.

## Coalesce / `unwrap_or`

### Typed `UnwrapOr<T>` builder

v1 erased source/default/output to `Term<Any>` at the builder,
losing the value-type guarantee at the call site. v2 parameterizes:

```rust
pub struct UnwrapOr<T: DefiniteType> {
    source: Term<Any>,        // erased internally, but...
    default: Term<Any>,       // ...the type T is preserved at the
    _phantom: PhantomData<T>, //    builder level for compile-time check
}

impl<T: DefiniteType> Term<Option<T>> {
    pub fn unwrap_or<D: Into<Term<T>>>(self, default: D) -> UnwrapOr<T> {
        UnwrapOr {
            source: Term::<Any>::from(self),
            default: Term::<Any>::from(default.into()),
            _phantom: PhantomData,
        }
    }
}

impl<T: DefiniteType> UnwrapOr<T> {
    pub fn is<O: Into<Term<T>>>(self, output: O) -> Premise {
        // Construct the Coalesce constraint from
        // self.source, self.default, Term::<Any>::from(output.into()).
    }
}
```

The user can no longer write
`Term::<Option<String>>::var("x").unwrap_or(...).is(Term::<Entity>::var("y"))`
— it fails at Rust-compile time because `Term<Entity>` doesn't
satisfy `Into<Term<String>>`.

### `Term<Any>::unwrap_or` for wire-format paths

Rules parsed from JSON arrive as `Term<Any>` for unspecified terms.
There is no Rust-level type to enforce. v2 adds an untyped
`Term<Any>::unwrap_or` that returns `UnwrapOr<()>` (or a separate
`UntypedUnwrapOr` builder). At rule-compile time (`DeductiveRule::
new`), the planner runs the meet algebra over the resulting Coalesce
constraint and rejects type-incoherent uses.

Both paths converge on the same runtime `Coalesce` constraint with
type-erased terms; the typed surface is purely a Rust-API
compile-time guarantee.

## Slice 7 enforcement, simplified

v1 had three checks:

1. `NegationOnOptional` — Negation reads optional binding.
2. `RequiredHeadFromOptional` — required head field bound by an
   optional producer.
3. `ConceptOnlyOptionalFields` — a concept with empty `with`.

v2 expresses (1) and (2) through `Type::accepts`. The Negation's
schema declares its parameters as `Type::Definite(_)` (not
`Optional`) — by definition, Negation needs Present values. The
required head field's schema declares `Type::Definite(_)`. If any
producer in the rule body declares `Type::Optional(_)` for the same
variable, `accepts` rejects. No parallel `optional_producers` set.

Check (3) is unchanged — it's a structural property of the concept
descriptor, not a type-meet rule.

## Marker traits

The `ScalarType`/`ProductType`/`VariantType`/`OptionalType`/
`AnyType`/`DefiniteType` family stays. Their role shifts:

- **v1 used markers as the only structural fence** preventing
  `Option<Option<U>>` at the Rust API. The runtime had no
  equivalent fence because the descriptor couldn't express
  optionality.
- **v2 uses markers as ergonomic bounds.** They're how
  `Term<Option<T: DefiniteType>>` expresses "T must be
  non-optional." The runtime descriptor *also* enforces this via
  the `Optional(Box<Definite>)` shape, but the markers exist so
  Rust users get compile-time errors instead of runtime panics.

The compile-fail doctests (`Term<Option<Option<U>>>`,
`Term<Option<Any>>` reject) carry over from v1.

## Wire format and migration

Schema is not currently persisted — it's recomputed from
descriptors on each session. So the wire-format change to `Type`
serialization affects only future-persisted schemas. No migration
needed for existing data.

`Resolution` is a new field on `AttributeQueryAll` /
`AttributeQueryOnly`. Existing serialized rules (which don't carry
this field) deserialize with `Resolution::Required` as the default.
Concept projection regenerates rules with the correct resolution at
runtime, so the wire-format default doesn't matter for
concept-derived rules.

## What's deferred

The recursive `Type` and `Definite` accommodate these without
reshape:

- **`Definite::Record(BTreeMap<String, Type>)`** for concepts as
  values. Future PR adds the variant; existing code continues to
  pattern-match exhaustively.
- **`Definite::Variant(BTreeMap<String, Definite>)`** for tagged
  unions. Variant payloads are `Definite` (no nested optionality
  in payloads — the canonical way to express
  "optional-inside-variant" is to flatten into the outer variant).
- **Storage encoding for variants** — the `domain/VariantType/Tag
  → payload` per-tag attribute scheme, with read-time deterministic
  winner selection across the tag group. Documented separately.
- **`get-some` (multi-attribute fallback)** — variant-elimination
  premise. Builds on Variant types.
- **Map types** — open question; not needed for optionality.

## Implementation phases

Tracked separately as Steps 1–11. Summary:

1. `Type`/`Definite` enums with meet/accepts/serde.
2. `Binding<Value>` at Match layer (port from v1, unchanged).
3. Descriptor enriched to carry `Type` (was `Option<ValueType>`).
4. Collapse v1's `schema::Type` into the new unified `Type`.
5. `Resolution` policy on `DynamicAttributeQuery`; remove
   `AttributeQueryOptional`.
6. Concept projection emits `Resolution::Optional` for `maybe`
   fields.
7. Macro emits typed `Term<Option<U>>` for `Option<T>` fields.
8. `Coalesce`/`UnwrapOr` with proper typed parameterization.
9. Marker-trait family (slimmer; ergonomic bounds).
10. End-to-end tests and Slice-7-equivalent enforcement via meet
    algebra.
11. Docs, lint, fmt, full workspace test.

Each step compiles and tests; the tree stays green throughout.

## Acceptance criteria

The branch is ready to merge when:

- All v1 end-to-end tests pass under v2 (cherry-picked from
  `feat/optional-fields`).
- New tests cover `Type` algebra (meet, accepts, serde
  round-trips), descriptor preservation through erasure,
  `Resolution` dispatch, typed `UnwrapOr` rejecting type-mismatched
  output, and Slice-7-equivalent rejections via meet.
- Compile-fail doctests still prove `Term<Option<Option<U>>>` and
  `Term<Option<Any>>` are rejected.
- Workspace clippy clean, fmt clean, `test:native:debug` green.
- This document and the Steps 1–11 task list reflect the final
  shape of the implementation.

## Open questions to settle during implementation

1. **`const KIND` vs non-const `kind()`**: depends on whether `Box::
   new` stabilizes in const for our toolchain. Decision deferred to
   Step 1.
2. **`UnwrapOr<()>` for the untyped path**: viable shape, or
   separate `UntypedUnwrapOr` builder? Decision deferred to
   Step 8.
3. **`Resolution::Optional` validation**: reject at constructor
   when `is` term's descriptor is `Type::Definite(_)` (mismatch)?
   Or allow as user signaling "I know what I'm doing, treat as
   optional anyway"? Decision deferred to Step 5.
