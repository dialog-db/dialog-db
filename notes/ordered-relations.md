# Ordered Relations via Deterministically Biased Fractional Indexing

Status: **core implemented** — the position primitive
(`dialog_artifacts::position`), the `dialog/position` and
`dialog/position-parts` formulas, and an end-to-end ordered-list test
(one prefix scan, sorted) have landed; see "What landed" below.
Originally: **proposed**. Evaluation of
[deterministically biased fractional indexing](https://observablehq.com/@gozala/deterministically-biased-fractional-indexing)
and the synopsys POC
([`commontoolsinc/synopsys/src/position`](https://github.com/commontoolsinc/synopsys/blob/main/src/position/lib.js))
as the basis for ordered collections in dialog, plus a surfacing
plan. The core idea
under evaluation: **the fractional index lives in the attribute name**,
so all members of an ordered collection come out of a single key-range
scan, already sorted.

## The approach, evaluated

The notebook surveys the design space for replicated ordered lists:

- **RGA** anchors each element to its predecessor; ordering requires a
  tree walk (a join per element in datalog terms), and it exhibits both
  the interleaving anomaly and unintended duplication of identical
  concurrent inserts.
- **Fugue** fixes interleaving with a binary-tree structure, but still
  duplicates, still needs a traversal to order, and — decisive for a
  partial-replica database — requires the *whole* positional tree to
  insert correctly. A replica holding a subset of a list cannot
  reliably place an element.
- **Fractional indexing** (Figma-style) stores a position per element;
  ordering is a sort, retrieval is a scan, and insertion needs only the
  two neighbors you can see — partial views degrade gracefully (worst
  case: more collisions, never wrong results). The known costs:
  interleaving of concurrent runs is accepted (fine for coarse lists,
  wrong for collaborative text — say so in the docs), and naive
  distributed use collides when two replicas derive the same position.

The notebook's contribution is two moves on top of the Figma/Wallace
scheme, and both fit dialog unusually well:

1. **Deterministic bias instead of random jitter.** Where Evan
   Wallace's variant adds random jitter to avoid same-position
   collisions without a server, here the jitter bits are drawn from the
   member's merkle reference. Consequences:
   - *Convergent idempotence*: two replicas inserting the **same**
     entity between the same neighbors derive the **same** position —
     the duplicate-"milk" problem disappears by construction, no
     coordination, no logical clocks.
   - *Dispersion*: two replicas inserting **different** entities at the
     same spot bias differently and (probably) land apart instead of
     colliding.
   This is exactly dialog's identity philosophy — content-derived
   entities, causality captured in what the reference hashes — applied
   to ordering. The notebook's "unique enough identifiers" guidance
   (derive the member entity from data that includes the list, or the
   list *state*, per the desired cardinality) is the same discipline
   dialog already asks of entity derivation.
2. **Collisions are benign because position is a relation, not an
   address.** When two distinct members do land on one position, the
   total order is `(position, member)` — the tie-break is
   deterministic, and nothing corrupts. In dialog this tie-break comes
   for free (below).

Verdict: sound, simple, and it is the only member of the design space
whose *retrieval* shape matches a triple store — no per-element joins,
no materialized tree, no order state outside the facts themselves.

## What the POC actually implements (position/*.js)

The implementation is richer than textbook fractional indexing — a
three-component hybrid that fixes fractional indexing's worst growth
mode:

```text
position = major (1 byte, base52 A–Z a–z)
         ‖ minor (capacity(major) bytes, base62)
         ‖ patch (variable, base62 — the fractional tail)
```

- **Major** encodes the *length class* of the minor: `a`/`Z` denote a
  1-digit minor, growing toward the edges (`z`/`A` = 26 digits), with
  `A–Z` the negative side and `a–z` the positive. Appending past a
  minor's range increments the major into a larger class — so
  head/tail insertion (the overwhelmingly common case) grows
  positions **logarithmically**, not linearly. This is LexoRank-style
  integer headroom fused with a fractional tail; naive fractional
  indexing grows O(n) on repeated appends.
- **Minor** is a fixed-width (per major) base62 integer;
  increment/decrement moves whole steps.
- **Patch** is the unbounded fractional tail where the **bias**
  lands: `deriveBias(item) = base62-digits(item bytes)` (the merkle
  reference re-encoded). New positions take their tail from the bias;
  when low/high patches are consecutive the bias is appended (median
  fallback when absent); when an intermediate digit exists, its
  tie-break digit is nudged to the bias's head digit when that fits
  in the gap. Same `(after, before, item)` on any replica ⇒ the same
  bytes.
- **Canonical form**: trailing minimum digits are trimmed (patch,
  then minor), so logically-equal positions are byte-identical —
  which is what makes the convergence claim exact rather than
  approximate.
- **Ordering** is plain byte order: the base62 alphabet is the byte
  ranges `0–9 < A–Z < a–z` (the parse-string order is presentational;
  digit arithmetic runs over byte ranges), so a `Uint8Array`/`&[u8]`
  lexicographic compare — i.e. dialog's key order — is the collation.
  No character in the alphabet is `/`, `NUL`, or outside printable
  ASCII.
- **Edge sentinels**: `Patch.min()`/`Patch.max()` use bytes just
  outside the alphabet (`/` = 0x2F, `{` = 0x7B) as virtual bounds,
  and `before()` at the absolute minimum returns the input position
  unchanged (no room left). A port must keep the sentinels out of
  persisted positions — `/` especially, since it is the attribute's
  namespace separator — and surface the exhaustion case to callers.

## Why "position in the attribute name" is right for dialog's keys

This is the load-bearing mechanical fact. An M3 entity-ordered key is

```text
tag ‖ entity ‖ attribute ‖ vtype ‖ value
```

Fix `entity` = the collection and give members attributes sharing a
prefix and the EAV region stores every
member of the collection in one **contiguous** range, sorted by
attribute bytes, i.e. by position, with the value slot free to carry
the member reference:

```text
[groceries  todo.item/aV  apples ]
[groceries  todo.item/an  milk   ]   ← one range scan,
[groceries  todo.item/ar  bread  ]   ← already in order
[groceries  todo.item/ax  bananas]
```

Because the position alphabet excludes `/`, the position can simply BE
the predicate under a per-collection namespace (`todo.item/<position>`)
— no extra separator character is needed, `Attribute`'s
namespace/predicate shape is satisfied as-is, and the prefix for the
range scan is `todo.item/`.

The alternatives genuinely do not have this property in dialog's
layout. Position as the *value* of a fixed attribute would sort members
by position — but then the value slot is spent and the member must live
elsewhere (a join). Position as a separate fact on the member
(`[item list/position p]`) needs a join per member plus a client sort;
the AEV region orders that attribute's facts by *entity*, not position,
and VAE mixes all lists together. The attribute-name encoding is the
one place a per-collection sort key can live such that a single scan is
both complete and ordered.

Two properties come along for free:

- **Deterministic tie-break.** Colliding positions produce the *same
  attribute*, so the colliding members are adjacent entries under it,
  ordered by value bytes — `(position, member)` total order, exactly
  the notebook's answer, enforced by the index itself.
- **Subscription soundness with zero new machinery.** Positions are
  ordinary committed facts. Prefix selectors
  (`ArtifactSelector::the_starting_with`) already exist and already
  record demand ranges; the cover-gated diff, incremental maintenance,
  and standing subscriptions treat an ordered collection like any other
  fact range. Nothing in the ordering design touches an invariant.

### Constraints found in the current code

- **`ATTRIBUTE_LENGTH = 64`** (`dialog-artifacts/src/key.rs:84`), and
  `Attribute` must contain `/` and be NUL-free — otherwise free-form.
  So the position budget is `64 − len(namespace) − 1` bytes (~50 for
  realistic namespaces). The POC's major/minor headroom makes
  head/tail insertion grow logarithmically — a 27-byte position
  covers the entire positive integer range — so the cap only
  pressures *middle* insertion patterns, where the patch tail grows
  (bias bounds it statistically). The cap still makes a **rebalance
  story** mandatory: reassigning positions is an ordinary retract/assert sweep
  an application (or later, a maintenance helper) performs when a
  collection's positions approach the cap. Alternatively the cap could
  be lifted for this use — the M3 key encoding is variable-length
  already; the cap is a type-level guard, not a format requirement.
- **Alphabet**: satisfied by the POC as-is — base62 byte ranges are
  order-preserving under attribute byte order, every character is
  attribute-legal, and `/` is excluded, so position-as-predicate
  needs no separator. The Rust port must match the POC byte-for-byte
  (major capacity map, trimming, bias nudge rules) with shared test
  vectors, and must keep the `/`‌/`{` edge sentinels out of persisted
  positions.
- **It already works at the raw layer, today.** A member scan is
  expressible with the existing pieces: an `AttributeQuery` with bound
  `of` and variable `the`, plus the existing `Term::starts_with`
  constraint, yields `(attribute, member)` rows; the selector-level
  `the_starting_with` pushdown is an optimization the premise layer
  can grow later. Nothing blocks experimentation.

## What landed

- **`dialog_artifacts::position`** — the Rust port:
  `Position` (validated, canonical, byte-ordered), `Bias::derive`,
  `insert(bias, after, before)` with explicit open bounds. Three
  deliberate departures from the JS POC, each documented in the
  module:
  1. **Bias truncation** to `BIAS_DIGITS = 6` digits (~35 bits) —
     the POC appends the full ~43-digit re-encoded reference, which
     would blow the attribute budget; six uniform digits make
     same-slot collisions negligible and truncation is deterministic,
     so convergence is unaffected.
  2. **No out-of-alphabet sentinels** — open bounds are explicit
     (`Option`), so `/` and `{` can never reach a stored position.
  3. **Exhaustion is an error** (`PositionError::Exhausted`) instead
     of silently returning the neighbor's position.
  And one genuine **bug fix over the POC**: digit arithmetic returns
  min-trimmed minors, and the POC's `create` concatenates them with a
  non-empty patch, letting patch digits occupy minor byte positions —
  `between("Zz…", "a1…")` derives `aY…`, which sorts *above* `a1…`.
  The port re-pads the minor to the major's capacity whenever a patch
  follows (trimming stays sound only for empty patches). Worth
  upstreaming to the JS POC.
- **Formulas** (`define_formulas!`): `dialog/position { member,
  after, before → is }` — derivation with empty-string open bounds,
  usable in queries, rules, and app code, deterministic per
  `(member, bounds)`; `dialog/position-parts { of → namespace,
  position }` — attribute decomposition. Both pure →
  subscription-sound with no machinery. (Learned in testing: any
  alphanumeric word starting with a letter is a *syntactically* valid
  position, so the namespace prefix — not position syntax — is what
  scopes an ordered relation.)
- **End-to-end test** (`ordered_relation_tests`): a list built by
  appends plus a wedge between neighbors, committed as
  `[list test.list/<position> member]` facts, read back with ONE
  `of(list) + the_starting_with("test.list/")` range scan — members
  arrive already in list order.

## The dictionary-scan spike (PR #338) and the typed-attribute ADR

Two prior explorations bear directly on the interface, and together
they answer the open ergonomics questions better than the plan below
originally did:

- **PR [#337](https://github.com/dialog-db/dialog-db/pull/337)
  "feat: directory style concepts"** (`feat/directory`, draft) — the
  first iteration: a `Symbol` type (lowercase letters, digits,
  hyphens, dots; MUST start with a lowercase letter; no `/`; ≤ 63
  bytes) and `Attribute` restructured as a `{domain, name}` Symbol
  pair, with the selector split into domain/name slots. Structurally
  invasive — the Attribute type change ripples through everything.
- **PR [#338](https://github.com/dialog-db/dialog-db/pull/338)
  "feat: open dictionary like concepts"** (`feat/open-record`, draft,
  stacked on `feat/type-inference-v2`) — the lesson learned from
  #337: `Attribute` stays opaque with `.split() → (Symbol, Symbol)`
  (parse-level, not structural), and the decomposition lives where it
  pays —
  - `Attribute` reshaped as a **(domain, name) `Symbol` pair** with
    `.split()`, and the selector grows two slots —
    `ArtifactSelector::with_domain(d).with_name(n)` — so *domain-only*
    selection is the first-class "all facts under this domain" scan,
    no string-prefix construction involved.
  - `Directory<T: Scalar>` — a `Symbol`-keyed collection as a concept
    **field**: content type `{"directory": "Text"}`
    (`Composite::Directory(Type)` in the type system), each matched
    row binding one `(name, value)` entry, aggregation into a
    `BTreeMap<Symbol, T>` at concept realize.
- **ADR 005** (`adr-attribute-types` branch): attributes as a variant
  — `name(utf8) | reference(0xFF ‖ digest) | position(0xFE ‖
  fractional-index)` — giving set membership and ordered relations
  their own attribute *types*, distinguishable by tag byte, opening
  storage/merge optimizations no userland prefix convention can reach.
  (It cites this very fractional-indexing design for `position`.)

### Positions and symbols are now syntactically disjoint

The load-bearing detail in both PRs: **a `Symbol` must start with a
lowercase letter**. The position port originally used majors spanning
`A–Z a–z`; restricting majors to **uppercase only** (`A–M` negative,
`N–Z` positive — 13 length classes per side, still base62^13 ≈ 2^77
integer headroom) makes the attribute's name half self-discriminating
by its first byte:

- lowercase first byte → a named field (a `Symbol`),
- uppercase first byte → a position,
- `0xFF` first byte → a reference (ADR 005's membership arm).

No tag byte, byte ordering intact, and the position/word ambiguity is
gone even in the text encoding — `person/name` cannot parse as a
position, so `dialog/position-parts` is now a true filter for ordered
members. (Landed: `MAJORS = A–M / N–Z`, origin position `N`.) The one
ask this places on #337/#338 when they land: the name slot's
validation must admit the position kind alongside `Symbol`, i.e. the
name half becomes the variant above rather than Symbol-only.

### How this reshapes the ordered-relations interface

An ordered relation **is a `Directory` whose name half is a
position**. `BTreeMap<Symbol, T>` already iterates in `Symbol` byte
order, and positions are byte-ordered by construction — so a
`Directory` field over a position-named domain realizes an ordered
collection *with no additional machinery*: the domain-only scan is the
single contiguous range, the name slot binds the position directly
(retiring `dialog/position-parts` string splitting for this path),
and the aggregation step yields members already sorted.

Concretely, the adapted plan:

- Adopt #338's `(domain, name)` decomposition and two-slot selector as
  the scan interface; the ordered-members query is then "domain-bound,
  name-free" — a first-class premise shape instead of a
  `starts_with` constraint over a joined string.
- The ordered concept field is `Directory<T>` with position-typed
  names — either directly, or as a thin `Sequence<T>` refinement that
  validates names as positions and exposes values-in-order iteration.
  Insertion sugar hangs off the same field: derive the position with
  `insert(&bias, &a..&b)` and assert under `(domain, position)`.
- ADR 005's `position` attribute *type* is the storage-level endgame:
  it removes the position/word ambiguity found while testing
  `dialog/position-parts` (any alphanumeric word starting with a
  letter is a syntactically valid position — with a `0xFE` tag the
  ambiguity vanishes), keeps user-visible names out of the position
  namespace entirely, and licenses merge/layout optimizations. The
  interim text encoding (`domain/<position>` predicates) stays
  wire-compatible with everything above and migrates mechanically
  when the tagged types land.

### What landed from the spike port

The adapted plan above is now implemented on this branch, lowered
onto the existing one-slot selector rather than the spike's two-slot
reshape (which would have forced the storage-layer `AttributePattern`
churn #338 itself walked back):

- **`Symbol`** (`dialog-artifacts/src/artifacts/symbol.rs`): the
  spike's validated identifier, unchanged rules (lowercase start;
  lowercase/digits/hyphens/dots; no `/`; ≤63 bytes) — which, combined
  with uppercase-only majors, makes the two name shapes disjoint by
  first byte.
- **`Name`** (same module): the name half of an attribute as a typed
  sum — `Symbol` (named predicate) or `Position` (ordered member),
  discriminated by the first byte with no tag. This is the interim,
  text-encoded form of ADR 005's `name | position` attribute variants.
- **`Attribute::domain()/name()/split()/compose()`**: lazy halves plus
  fallible typed decomposition (`split` declines legacy shapes like
  `person/display_name` rather than misclassify) and composition with
  the joint 64-byte budget check. `dialog/position-parts` now rides
  `split()` instead of hand-rolled string splitting.
- **Selector sugar**: `with_domain(&Symbol)` lowers onto the existing
  attribute-prefix range (`domain/` is a contiguous scan);
  `with_name(Name)` is a per-entry filter on the name half (a name
  alone is not a contiguous range, so it preserves the selector's
  constrained-state, exactly the spike's state rule) — and when a
  domain is already present the builder tightens the pair to an exact
  attribute, a point lookup.
- **`Directory<T>` / `Sequence<T>`**
  (`dialog-artifacts/src/collection.rs`): the twin keyed views over a
  domain scan — `BTreeMap<Symbol, T>` and `BTreeMap<Position, T>`,
  each with an `admit(attribute, value)` classifier so one pass over
  a mixed domain fills both. `Sequence` iterates in list order and
  exposes `first_position`/`last_position` as the bounds for the next
  `insert(&bias, last..)`.
- **Not ported — `Composite::Directory`**: the spike's type-system
  work targeted the pre-rewrite lattice (`Composite` set alongside
  Product/Variant). The current lattice (`Primitive` bitfield +
  `Refinement`) has no composite kinds, so the schema-level
  `as: {"directory": "Entity"}` story is deferred until composites
  return; `Directory`/`Sequence` live at the artifacts layer where
  they are useful today, and concept-field aggregation remains the
  realize-layer follow-up the spike itself deferred.

## Surfacing plan

Phased so each step is useful alone:

1. **The position primitive** — a Rust port of `position.js` in
   `dialog-artifacts` (`position.rs`): `between(after, before, bias)
   → position`, where `bias` is the member entity's reference bytes;
   deterministic, order-preserving, attribute-legal alphabet;
   explicit `min`/`max` boundary handling for head/tail/empty
   insertion. Property tests: order preservation, determinism,
   betweenness, growth bounds; shared vectors with the JS POC.
   Exposed through the wasm bindings so JS callers use the same
   implementation rather than a parallel one.
2. **Pure formulas** (`define_formulas!`, both trivially
   subscription-sound):
   - `dialog/order-between { after?, before?, bias → is }` — compute
     an insertion position inside a query or transaction-building
     code; the same primitive everywhere dialog runs.
   - `dialog/order-parts { of → base, position }` — decompose a
     position-bearing attribute name, so query results expose the
     position as a bindable term (for sorting, range filtering,
     neighbor lookup).
3. **Insertion through rules** — the intended end state: an insert is
   an event fact plus an *inductive rule* whose premises bind the
   neighbors and derive the position via `dialog/position`, and whose
   head asserts the membership fact. The identified gap: rule heads
   currently name attributes statically, and the membership fact's
   attribute is *computed* (`namespace ‖ position`). Ordered
   relations therefore need **dynamic-attribute heads** — a head/
   statement form whose attribute comes from a bound term (an
   attribute-composition formula like `dialog/attribute { namespace,
   predicate → is }` plus head support for attribute terms). This is
   the main engine work left.
4. **Write-side sugar** — `tx.insert(list, "todo/item", member)
   .after(x)/.before(y)/.append()`-style helpers that read the
   neighbor positions and call the primitive with `bias` = the member
   entity's bytes. (Insertion needs the neighbors' positions; on a
   partial view, whatever neighbors are visible — the algorithm's
   graceful-degradation property is precisely that this stays
   correct.)
5. **Concept integration** — superseded in shape by PR #338's
   `Directory` fields (see the dictionary-scan section above): the
   ordered field is a position-named directory, realized sorted for
   free. Original sketch kept for reference below.
   **Concept integration (original)** — the real ergonomics target: an *ordered*
   field kind in concept descriptors (`items: { the: "todo/item", as:
   entity, ordered: true }`) that compiles to the prefix scan +
   decomposition + `(position, member)` sort and realizes conclusions
   in order. Engine result streams are unordered in general — the
   single-scan shape happens to stream in order, but the guarantee
   should live in the concept's realize step (an explicit sort over
   the position binding), not in an accident of the plan.
6. **Premise pushdown** (optimization): let the scan premise carry an
   attribute prefix so the selector narrows server-side instead of
   filtering post-scan. The selector, demand recording, and key-range
   machinery already support prefixes; this is surface plumbing.

## Out of scope / accepted tradeoffs

- **Collaborative text.** Interleaving of concurrent runs is inherent
  to fractional indexing; this design targets coarse ordered
  collections (lists, columns, playlists, outlines). Say so loudly in
  the docs.
- **Fugue/RGA as derived views.** The notebook's observation that
  list CRDTs are expressible as datalog rules over anchor relations
  remains open and compatible — nothing here precludes a later
  recursive-rule ordering for text-grade cases (it wants the
  goal-directed fixpoint anyway).
- **Automatic rebalancing.** v1 documents the cap and leaves
  rebalancing to applications; a maintenance helper can come later.
