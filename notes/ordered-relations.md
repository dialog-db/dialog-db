# Ordered Relations via Deterministically Biased Fractional Indexing

Status: **proposed**. Evaluation of
[deterministically biased fractional indexing](https://observablehq.com/@gozala/deterministically-biased-fractional-indexing)
and the synopsys POC (`src/position/position.js`) as the basis for
ordered collections in dialog, plus a surfacing plan. The core idea
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

## Why "position in the attribute name" is right for dialog's keys

This is the load-bearing mechanical fact. An M3 entity-ordered key is

```text
tag ‖ entity ‖ attribute ‖ vtype ‖ value
```

Fix `entity` = the collection and give members attributes sharing a
prefix — `todo/item@<position>` — and the EAV region stores every
member of the collection in one **contiguous** range, sorted by
attribute bytes, i.e. by position, with the value slot free to carry
the member reference:

```text
[groceries  todo/item@V  apples ]
[groceries  todo/item@n  milk   ]   ← one range scan,
[groceries  todo/item@r  bread  ]   ← already in order
[groceries  todo/item@x  bananas]
```

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
  So the position budget is `64 − len(base) − 1` bytes (~50 for
  realistic bases). Fractional positions grow under pathological
  insertion patterns (always the same spot); the bias bits bound
  growth statistically, but the cap makes a **rebalance story**
  mandatory: reassigning positions is an ordinary retract/assert sweep
  an application (or later, a maintenance helper) performs when a
  collection's positions approach the cap. Alternatively the cap could
  be lifted for this use — the M3 key encoding is variable-length
  already; the cap is a type-level guard, not a format requirement.
- **Alphabet**: byte order must equal position order (standard
  fractional-index alphabets are constructed for exactly this), every
  character legal in an attribute (NUL-free — trivially satisfiable),
  and the base/position separator must be a character excluded from
  the alphabet so the prefix range is exact. The alphabet and bias
  truncation must match the synopsys `position.js` POC byte-for-byte
  if positions are to interoperate — port from it, with shared test
  vectors. (The POC was not reachable from this session; the port
  needs it open as the reference.)
- **It already works at the raw layer, today.** A member scan is
  expressible with the existing pieces: an `AttributeQuery` with bound
  `of` and variable `the`, plus the existing `Term::starts_with`
  constraint, yields `(attribute, member)` rows; the selector-level
  `the_starting_with` pushdown is an optimization the premise layer
  can grow later. Nothing blocks experimentation.

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
3. **Write-side sugar** — `tx.insert(list, "todo/item", member)
   .after(x)/.before(y)/.append()`-style helpers that read the
   neighbor positions and call the primitive with `bias` = the member
   entity's bytes. (Insertion needs the neighbors' positions; on a
   partial view, whatever neighbors are visible — the algorithm's
   graceful-degradation property is precisely that this stays
   correct.)
4. **Concept integration** — the real ergonomics target: an *ordered*
   field kind in concept descriptors (`items: { the: "todo/item", as:
   entity, ordered: true }`) that compiles to the prefix scan +
   decomposition + `(position, member)` sort and realizes conclusions
   in order. Engine result streams are unordered in general — the
   single-scan shape happens to stream in order, but the guarantee
   should live in the concept's realize step (an explicit sort over
   the position binding), not in an accident of the plan.
5. **Premise pushdown** (optimization): let the scan premise carry an
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
