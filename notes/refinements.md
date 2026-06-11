# Refinements: value-level constraints on the type lattice, and scan-range pushdown

> Design note for the refinement layer (dialog-db-51): how a constraint a rule
> proves about a variable's *values* (not just its types) travels from the
> predicate that proved it, through inference, to the storage boundary, where it
> becomes index key-range bounds. Companion to
> [`formula-schemes.md`](./formula-schemes.md) (the predicates that produce
> refinements) and the planned order-preserving value encoding (dialog-db-57,
> which widens what can consume them).

## Goal

`?a.starts_with("person/")` narrows `?a`'s kind to the textual members the
prefix could begin — that landed with the predicates. But the *prefix itself*
is knowledge too: a scan feeding `?a` need not read the whole attribute index
and filter; it can read only the `person/*` key range. The refinement layer
makes such constraints first-class on the type lattice so that one mechanism
carries them end to end:

```
starts-with schema  →  inference (meet)  →  planner stamp  →  selector  →  key range
```

Downstream, the same narrowed kinds are what demand covers are computed from,
so subscriptions watch less and partial replication pulls less (M5); M3's
concept-membership constraints on Entity are the same mechanism with a
different payload.

## The lattice layer

`type_system::Type` gains a third shape:

```rust
enum Type {
    Primitive(Primitive),
    Composite(Primitive, BTreeSet<Composite>),
    Refined(Primitive, Refinement),
}

struct Refinement {
    prefix: String,   // non-empty; lexical prefix over TEXTUAL members
}
```

A refined type admits a value when its membership admits the value's type
*and* the refinement admits the value itself (`Type::admits` checks both — so
every existing admits-site, scans included, enforces refinements with no new
code). The lattice operations treat a refinement as a constraint:

- **Meet** (`intersect`): the conjunction. Two prefixes are jointly
  satisfiable iff one extends the other; the meet keeps the longer. Disjoint
  prefixes are an empty meet — the ordinary known-types-misalign compile
  error. A refined side admits no composite shapes, so composites on the
  other side drop out of the meet.
- **Join** (`union`): the weakest common implication — the longest common
  prefix, or no refinement at all when joined with an unrefined side (the
  union must admit everything either side admits).
- **Inclusion**: constraint-ordered. `[prefix "did:"]` includes
  `[prefix "did:key:"]`; unrefined includes refined; never the reverse.

`Refinement` is a struct, not an enum, so numeric intervals (dialog-db-57's
consumer) and M3's Entity concept-membership extend it with fields rather
than new lattice variants; the meet/join shape stays put.

The `starts-with` schema attaches the refinement to its subject slot's content
type, and the generic schema walk plus the unifier's principal meet do the
rest — no inference changes. One unifier fix was load-bearing: variable
resolution used to *reconstruct* the resolved static from the merged primitive
set, which would have silently shed the refinement; resolution now rebuilds
around the merged membership preserving the type's structure
(`Type::with_primitive_part`).

## Transport: kinds stamped on scan terms

The planner already stamped rule-level kinds onto the scan's value term
(`with_type`). The attribute and entity terms now get the same treatment
(`with_subject_kinds`): their descriptors (`Symbol`, `EntityType`) graduated
from unit structs to carrying an `Option<Type>`, normalized to `None` whenever
the kind says no more than the static type — so unnarrowed terms compare,
hash, and serde-roundtrip exactly as before (the wire format already had a
`type` field per variable; it previously dropped on deserialize, now it is
preserved).

Alternative considered: stamping refinements onto the query struct as
plan-time decoration fields. Rejected — kinds live on terms everywhere else,
and the wire `type` slot already existed; a parallel channel would have been
a second source of truth.

## The storage boundary

`ArtifactSelector` gains prefix bounds beside its exact fields:
`the_starting_with(prefix)` / `of_starting_with(prefix)` (both produce a
`Constrained` selector — a prefix is a constraint). The scan
(`ArtifactTreeExt::scan`):

- picks the index as before by exact fields (entity / value / attribute), and
  a prefix on a leading dimension picks its index when no exact field does;
- tightens every branch's `(start, end)` keys with whatever prefix bounds the
  selector carries: the bound segment is the prefix's raw bytes followed by
  `0x00` (lower) or `0xFF` (upper) fill. Applying a bound to a non-leading
  dimension is sound (the range stays a superset; `matches_selector` filters
  per entry) and tight when every more-significant dimension is exact;
- `matches_selector` re-checks prefixes per entry, so range construction can
  over-approximate freely.

What each segment's encoding permits (the dialog-db-57 analysis):

- **Attribute** (64 bytes, raw, zero-padded): prefix ranges are *exact*. A
  prefix longer than 64 bytes matches nothing, which the per-entry check
  yields naturally.
- **Entity** (32 raw bytes + 32-byte hash of the URI tail): ranges are tight
  up to 32 bytes; a longer prefix ranges over its 32-byte truncation and the
  per-entry check confirms the remainder against the stored datum's URI
  (`ENTITY_RAW_HEAD`).
- **Value** (type tag + blake3 hash): *no* range pushdown is possible — the
  hash destroys order. Value refinements still travel (inference, `admits`
  filtering, demand-cover input) so dialog-db-57's re-encoding turns them
  into ranges with only a selector-conversion change.

## What this deliberately does not do

- No cost-model change: the planner does not yet prefer prefix-bounded scans.
  Range bounds only shrink what the chosen scan reads.
- No numeric intervals in `Refinement` yet: with the value segment hashed
  there is no consumer, and the comparison predicates' sides are not
  scheme-linked. Both land together with dialog-db-57.
- No `the!`-macro-level surface: prefixes arrive via `starts_with` premises
  on attribute/entity variables, not via new scan syntax.
