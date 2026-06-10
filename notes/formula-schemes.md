# Formula type schemes

> Design note for generic formulas: `math/sum` as "forall `N` ⊆ NUMERIC: `(of: N, with: N) → is: N`"
> rather than a fixed `u32` signature. Records the declaration surface, the runtime model, the
> inference wiring, the numeric-promotion decision, and the alternatives considered. Companion to
> [`guide.md`](./guide.md) (the "Inference in an open world" section is the user-facing account) and
> the planned range-refinement predicates (which share the lattice-typed cell prerequisite).

## Goal

Built-in and user-defined formulas should be polymorphic where their semantics are: addition works on
any numeric type, comparison on any comparable one, concatenation on string-likes. Today every formula
fixes concrete Rust types (`Sum { of: u32, with: u32, is: u32 }`), so `math/sum` over `i64` facts
matches nothing. The type system should carry the polymorphism: a scheme with a *bounded type
variable*, instantiated fresh at every use site, so that inference flows bidirectionally — a `u64`
input narrows the output to `u64`, a `Float` output requirement narrows the inputs, and a `String`
anywhere is a compile-time conflict.

## Declaration: Rust generics

```rust
#[derive(Formula)]
pub struct Sum<N: Number> {
    of: N,
    with: N,
    #[output(cost = 5)]
    is: N,
}

impl<N: Number> Sum<N> {
    fn compute(input: Input<Self>) -> Vec<Self> { ... }
}
```

The type parameter *is* the scheme variable: sharing is expressed by using `N` in several fields,
scoping and checking come from rustc, and multi-parameter schemes (`Convert<A: Number, B: Number>`)
need no extra machinery. The bound trait carries the lattice bound as an associated constant
(`<N as SchemeBound>::BOUND = Primitive::NUMERIC`), so the derive emits trait-qualified code and never
matches type names syntactically — a bound without a scheme fails to compile rather than silently
degrading.

## Runtime: one visitor over a closed type set

A registered formula must serve every instantiation at runtime, and Rust cannot call a generic
function with a runtime-chosen type without enumerating somewhere. The enumeration lives in exactly
one place: the value lattice's numeric set is *closed* (`UnsignedInt`, `SignedInt`, `Float`), so
`dialog-query` provides one hand-written visitor (`with_numeric(data_type, visitor)`) that routes to
the matching monomorphization; the derive emits a visitor impl per generic formula. Per row, the
instantiation type is determined from the bound input values, the matching `compute::<N>` runs, and
the result is a `Value` of that same type. Other bounds (COMPARABLE, STRING_LIKE) get their own
visitors when a formula needs them; each is a closed set.

## Inference: instantiate per use

When `TypeEnv::infer` walks a `Sum` premise, the scheme allocates one fresh unifier variable with the
NUMERIC constraint and contributes it as the slot type of `of`, `with`, and `is` together — the
per-use instantiation of a rank-1 scheme. The unifier's per-variable `Primitive` constraint *is* the
bounded type variable, and `unify` returning the principal meet (landed ahead of this work) is what
lets composed unifications surface their results. Consequences:

- `sum(?age, 1, ?next)` with `?age` known `u64` infers `?next : u64`.
- `?next` demanded as `Float` downstream infers `?age : Float`.
- `?age` demanded as `String` anywhere is an empty meet: compile error.
- Nothing known: all three stay bounded NUMERIC, and the bound is stamped onto the feeding scans
  (filtering non-numeric facts at the data boundary, per the narrow-on-use semantics).

## Prerequisite: cells carry lattice types

`Cell::content_type` is today a single concrete `artifact::Type`; a scheme-bounded or
range-refined slot cannot be expressed. Cells graduate to the lattice `type_system::Type` (and, for
scheme slots, to the unifier-facing variable form). This same change is the prerequisite for the
planned range-refinement predicates, where a cell's type is "String with prefix `p`".

## No implicit numeric promotion

A row whose inputs cannot share the scheme variable — `sum(2u64, 3.5f64)` — is a **non-match**, not a
promotion. Filtered, like every other type mismatch at a scalar slot. The reasons, in order of force:

1. **Promotion cannot be lossless here.** Datomic promotes safely because the JVM supplies
   BigInt/BigDecimal to widen into. Dialog's value lattice tops out at `u64`/`i64`/`f64`:
   `u64 → f64` silently loses precision above 2^53, and `u64`/`i64` have no common type covering both
   ranges. Promotion would not remove the runtime surprise, it would relocate it from "row excluded"
   to "row matched with a quietly wrong value". If promotion is ever genuinely wanted, the honest
   prerequisite is widening the value lattice (BigInt/Decimal) — a storage-format decision to take
   deliberately, not to back into.
2. **Data-dependent output types poison inference and joins.** Under promotion the output's type
   depends on sibling inputs per row, so analysis can only ever say NUMERIC — weakening exactly the
   narrowing that index-range pushdown wants — and `2u64`/`2.0f64` are distinct values with distinct
   index keys, so promoted outputs join inconsistently downstream.
3. **Consistency.** Typed scan slots filter heterogeneous facts; a formula slot that coerced instead
   would make two halves of one type system disagree.

The SQLite comparison that calibrated this: SQLite coerces everything (`'abc' + 1 = 1`) and never
errors; PostgreSQL errors at runtime; dialog filters. Filtering keeps SQLite's
"queries never die on data" ergonomics without fabricating values.

**The ergonomic release valve is literals, not data.** `sum(?age, 1, ?next)` must not die because `1`
defaulted to the wrong width: numeric literals are *polymorphic constants* carrying the NUMERIC bound,
instantiated per row to the data's type with a checked-lossless conversion (`1` fits everywhere;
`1.5` can only instantiate to `Float`; `-1` only to signed). Data-derived values stay strict, and
explicit conversion formulas (`number/to-float`, …) are the opt-in for crossing type strata — exactly
parallel to `Coalesce` being the explicit opt-in for absence.

## Alternatives considered

- **Scheme labels as field attributes** (`#[scheme(a)] of: Number`): rejected. Invents an annotation
  language rustc cannot check, handles multi-parameter schemes awkwardly, and the `Number` newtype
  hides which cells share a variable.
- **Hand-written `Cells` for generic formulas only**: rejected; gives up the derive ergonomics that
  make formulas pleasant, precisely for the formulas users touch most.
- **Implicit promotion**: rejected above; recorded with its prerequisite (a wider value lattice)
  should it ever be revisited.

## Where errors surface (summary)

Compile time: empty meets (known types misaligned), literal outside a cell's bound. Evaluation:
no type errors — rows that cannot instantiate the scheme are non-matches, and inference must be
inspectable (the planned diagnostics surface reports what narrowed each variable and what was
filtered where).
