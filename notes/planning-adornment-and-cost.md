# Adornment feasibility and cost in the planner

> Design note. Companion to [`incremental-subscriptions.md`](./incremental-subscriptions.md).
> Grounds the planner redesign in the magic-sets literature: separate **feasibility** (the SIPS
> adornment function) from **cost** (ranking among feasible orders), make both declarable so a rule
> loaded from a peer carries its binding requirements as data, and fix the cases the current per-slot
> `Requirement` schema cannot express.

## Where this fits

The query engine is moving the executable form off the syntactic AST onto a compiled `Plan` (the
operator IR). Planning orders premises by repeatedly asking, per premise, against the current set of
bound variables: *can this run yet, and if so what does it cost?* Today that question is answered by
two coupled pieces — the per-slot `Requirement` schema (feasibility) and `estimate(env)` (cost). This
note redesigns those two pieces to match how magic-sets actually separates them, and to remove the
expressiveness gaps in the current encoding.

## What the papers prescribe

Classic magic-sets (Balbin et al.; Alviano) separate two concerns cleanly, and dialog-db should too:

- **Adornment / SIPS = feasibility.** A SIPS for a rule fixes a total order over body literals plus,
  for each literal, the set of variables passed to it from the literals before it (Balbin Def. 10).
  The adornment `bf…` of a predicate is the derived bound/free pattern of its arguments under that
  order. Crucially the adornment is a **per-predicate** notion — "a SIPS for rule `R` and adornment
  `α`" — not a per-argument flag. It answers *can this literal be solved given these bound arguments,
  and which arguments does it then bind*.
- **Cost = which SIPS to pick.** Balcin §3.1 states the SIPS choice "is guided by factors such as the
  current and expected size of the different relations and the indexing mechanism employed… we assume
  throughout that this choice has been made." The papers deliberately delegate cost to a model they do
  not specify. dialog-db's `estimate` is exactly that model, and being richer than the papers is
  correct, not a defect.

Two further points the literature settles, both validating the current architecture:

- **Adornment is generated on demand and memoized, never enumerated.** Alviano's `Adorn`/`ProcessQuery`
  (Fig 3.2–3.4) drive compilation from a worklist `S` of adornments *seen so far* and a set `D` of
  *already-processed* ones; an adorned predicate is added "unless it has been produced previously."
  Adornments are discovered lazily as evaluation reaches predicates and cached the first time each is
  needed. Eager enumeration of all `2^n` adornments is the thing the algorithm avoids. dialog-db's
  just-in-time adornment computation with caching (`ConceptQuery::evaluate` deriving the adornment from
  the first match and caching the plan) is the demand-driven twin of this worklist — it is the
  textbook algorithm, not a shortcut.
- **There is no global adornment table to keep in sync.** Because adornment is demand-driven and
  memoized per evaluation, each peer adorns lazily for the adornments its own queries demand and caches
  locally. There is no persistent cross-peer set of adornments that could drift. For a system where
  rules arrive from different peers and are compiled during query evaluation, the lazy form is the
  correct one; eager precomputation would manufacture exactly the sync problem it must avoid.

## What dialog-db has today, and the two gaps

`Cardinality::estimate(the, of, is)` (in `schema.rs`) is a 16-arm truth table over *which of the three
slots are bound*. It already fuses feasibility and cost into one per-predicate method: `None` =
infeasible (the all-free adornment `fff`), `Some(n)` = feasible with cost `n`, where the cost reflects
which index prefix the bound slots form. So the per-predicate feasibility-as-method already exists —
the instinct behind `estimate` was right.

The per-slot `Requirement` schema (`Required(None)` / `Required(Some(group))` / `Optional`, with choice
`Group`s) is the *declarative* feasibility encoding the planner walks in `Candidate`. It exists so that
binding requirements are data — so a premise can advertise its requirements without bespoke code. Two
gaps:

1. **Feasibility is entangled with cost and is silent.** `estimate(env) -> Option<usize>` returns one
   number or `None`. It cannot say *why* a premise is infeasible (which bindings it still needs), nor
   *which variables it would bind* on success. The planner reconstructs binds/needs separately by
   re-walking the schema. The verdict, its reason, and its produced bindings should travel together.
2. **The per-slot vocabulary cannot express k-of-n.** A `Group` expresses "any one of these slots
   bound satisfies the group" — one-of-n. It cannot express:
   - **either-of-2, symmetric** — equality binds the other cell given *either* one; works as a single
     2-member group, the one case that fits.
   - **k-of-n with k > 1** — `math/sum(a, b, c)` needs *any two of three* bound to derive the third.
     No combination of per-slot `Required`/`Optional`/`Group` expresses "2 of these 3, don't care
     which." This is the propagator-style requirement the current schema fails on.

So the redesign keeps `estimate` as the cost method, keeps requirements declarative, and replaces the
per-slot flag vocabulary with a per-premise feasibility function expressive enough for k-of-n.

## Proposed shape

### Feasibility: `adorn`

Each premise advertises a feasibility function: given the set of currently bound variables, either it
can run — yielding the variables it will bind — or it cannot, with a reason naming what is still
missing.

```rust
/// What a premise binds once it runs, given an entry adornment.
struct Binds(BTreeSet<String>);

/// Why a premise cannot run yet under the current bindings.
enum Infeasible {
    /// Needs at least one of these still-unbound variables bound.
    NeedsAnyOf(BTreeSet<String>),
    /// Needs at least `k` of these bound; currently `have` are.
    NeedsKOf { k: usize, of: BTreeSet<String>, have: usize },
    /// Needs all of these still-unbound variables.
    NeedsAll(BTreeSet<String>),
}

impl Premise /* per variant */ {
    fn adorn(&self, bound: &BTreeSet<String>) -> Result<Binds, Infeasible>;
}
```

`Result` rather than `Option` so an infeasible premise reports *why* — the planner surfaces it as the
required-bindings diagnostic, and demand reification (the magic-sets step) reads it to know which
variables to demand next. `Binds` is the SIPS function `f_r^α`: the variables passed onward from this
literal.

The cases fall out per premise without a flag table:
- **Scan** (the/of/is): feasible iff at least the slots needed to form a selector are bound (today's
  `None` arms); `Binds` = the unbound slots. The 16-arm table's feasibility column.
- **Equality(a, b):** feasible iff `a` or `b` bound; binds the other. `NeedsAnyOf({a, b})` otherwise.
- **`math/sum(a, b, c)`:** feasible iff ≥2 of `{a, b, c}` bound; binds the third — *if modeled as one
  premise*. But the propagator model (below) says **don't** model it as one premise: decompose it into
  directional sub-premises, each of which is a simple one-output `Prefix`. The k-of-n case then never
  arises at the feasibility layer.

The k-of-n shape (`NeedsKOf`) is retained in `Infeasible` for premises that genuinely cannot be
decomposed, but the propagator decomposition is the preferred treatment for arithmetic/relational
constraints. See *Propagator model* below.

Keeping this **declarable** (the open requirement): `adorn` is a method, but its *data* should be a
small per-premise requirement descriptor the method interprets — e.g. an enum
`Feasibility::{ Prefix(ordered slots), AnyOf(set), KOf(k, set), All(set) }` stored on the premise — so a
rule loaded from a peer carries its feasibility as serializable data, not as opaque code. The current
`Requirement`/`Group` schema is the weaker prototype of this; `Feasibility` is its generalization.
Concretely: replace the per-slot `Requirement` with a per-premise `Feasibility` descriptor; `adorn`
evaluates it against `bound`.

## Propagator model (Radul–Sussman): decompose, don't generalize feasibility

*The Art of the Propagator* (Radul & Sussman, MIT-CSAIL-TR-2009-002) and Radul's thesis *Propagation
Networks* (MIT-CSAIL-TR-2009-053) answer the multidirectional-constraint question directly, and their
answer reshapes the feasibility design above.

**A multidirectional constraint is built from unidirectional propagators sharing cells; whichever has
enough inputs fires.** The thesis's canonical example (§3.2, Fig 3-3) is exactly our `math/sum`:

```scheme
(define (sum x y total)
  (adder x y total)        ; total <- x + y
  (subtractor total x y)   ; x <- total - y
  (subtractor total y x))  ; y <- total - x
```

"It works because whichever propagator has enough inputs will do its computation. It doesn't buzz
because the cells take care to not get too excited about redundant discoveries." So the "2-of-3,
don't-care-which" requirement is **not** expressed as a k-of-n feasibility test on one node — it is
three ordinary one-output nodes over shared variables. Each sub-node has the trivial adornment "all
inputs bound → binds the one output." The planner needs no `KOf`; it just sees three candidate
premises and runs whichever becomes feasible first.

This changes the recommendation: **prefer decomposing relational/arithmetic premises into directional
sub-premises over enriching the feasibility vocabulary with k-of-n.** `Equality(a, b)` is already this
shape (two inverse copy propagators); `sum`/`product` become three; the per-slot schema's failure to
express k-of-n stops mattering because no single premise needs k-of-n. `NeedsKOf` stays only as a
fallback for genuinely atomic non-decomposable premises.

Two further propagator ideas bear on the wider engine (not just planning), worth recording because the
incremental-subscriptions direction will want them:

- **Cells accumulate information; merge is the combinator.** A cell holds *partial information* and
  starts at `nothing` (absence of a value); adding content `merge`s the increment with the current
  content (TR §3). Merge is monotone: it returns the *more informative* result, the old value unchanged
  if the new is redundant, or a distinguished *contradiction* if they conflict.

  dialog-db's binding environment (`Match`) already implements this merge, and as a genuine three-state
  lattice — not equality-only as a naive reading suggests. `Binding` (`selection/match.rs`) has exactly
  the propagator structure:
  - **unbound** (variable absent from the map) = the cell's `nothing` / bottom.
  - **`Present(value)`** = a ground value.
  - **`Absent`** = a *distinct, more-informative-than-bottom* state: "known to have no value." This is
    itself a lattice point, not absence of information — which is why optional resolution can bind
    `Absent` and a later `Present` then *conflicts* rather than overwrites.

  `Match::bind` / `bind_absent` are the merge: binding into an unbound slot inserts (the
  `nothing → content` arm); re-binding the same `Present` value is idempotent (TR's "return the old
  information by `eq?`"); a different `Present`, or `Present` vs `Absent`, returns the contradiction
  (`EvaluationError::Assignment`). So dialog-db's binding-merge *is* a cell merge over the
  `{ unbound ⊏ Present(v) }` / `{ unbound ⊏ Absent }` lattice.

  The engine already has a propagator that *reads* this lattice rather than only writing it:
  `Coalesce` (`constraint/coalesce.rs`) branches on `Present` / `Absent` / unbound to merge a `source`
  with a `fallback` — set-widening unwrap, one row in, one row out. So dialog-db is already doing
  small-scale propagation over partial bindings; the propagator model is the principled account of what
  it is doing, not a new paradigm to import.

  The one axis where it is narrower than the propagator lattice: merging two `Present` values succeeds
  only on **equality**, never by *narrowing* (interval intersection, type/set refinement). Generalizing
  the value lattice from "ground or conflict" to "narrow toward ground" is what would let bindings carry
  partial constraints (a variable known to be in a range/type before it is fully ground) — relevant to
  the incremental-subscriptions work, out of scope for the planner.
- **Direction is chosen by available data, at run time, not fixed at compile time** (TR §4: "whichever
  one has enough inputs will do its computation"). This is the same demand-driven, just-in-time
  adornment dialog-db already does for rules — it is the propagator-network restatement of the
  magic-sets lazy `Adorn` worklist. The two literatures agree: don't enumerate directions up front;
  let the bound set select the direction when the premise is reached.

### Cost: `estimate`

Cost stays a per-premise method and keeps doing what the papers delegate to it, but is decoupled from
feasibility and made to depend on more than bound/unbound booleans. It is only ever asked of a
*feasible* premise (the planner calls `adorn` first), so it never re-derives feasibility.

```rust
impl Premise /* per variant */ {
    /// Cost of running this premise under `bound`. Only called when
    /// `adorn(bound)` is `Ok`, so it assumes feasibility.
    fn estimate(&self, bound: &BTreeSet<String>) -> Cost;
}
```

Cost should capture the distinctions the current flat `usize` already gestures at, made explicit:

- **Work class, not just a magic number.** The existing constants (`LOOKUP`, `RANGE_READ`,
  `RANGE_SCAN`, `INDEX_SCAN`, plus `VERIFICATION`, `CONCEPT_OVERHEAD`) are really an ordinal ladder:
  point lookup < bounded range read < large range scan < full index scan, with additive overheads for
  a winner-verification pass and for rule evaluation. Model cost as that ladder so the comparison the
  planner makes (cheapest feasible premise) is over a principled order, and a formula (no IO) sits
  below any scan by construction — addressing "computing a sum is far cheaper than a scan that binds
  the same variable."
- **Which bound variable matters, not just how many.** The current table already distinguishes
  entity-bound from value-bound scans (`{of}` → `RANGE_READ`/`RANGE_SCAN` vs `{is}` →
  `INDEX_SCAN`), and cardinality-one verification cost. Generalize this: cost is a function of *which*
  index prefix the bound set forms and the attribute's cardinality/selectivity — an entity-bound scan
  is narrower than a value-bound one, and a cardinality-one lookup narrower still. The right longer-term
  anchor for "which bound variable yields the narrower scan" is selectivity-driven cost
  (cf. worst-case-optimal joins); for now the index-prefix ladder the table already encodes is enough,
  lifted out of the hand-written arms onto a per-prefix cost so it is derived, not enumerated.

`Cost` need not be a single scalar forever — a `(class, tie-breaker)` pair (work class as primary,
estimated row count / selectivity as secondary) lets the planner order within a class. Start with the
scalar ladder to preserve current behavior; leave room for the selectivity tie-breaker.

## How analysis and planning consume this

- **Analysis builds the SIPS once.** The dependency graph (`DependencyGraph` in `analyzer.rs`:
  per-premise `binds`/`needs` and the `requires[i]` edges) is precisely the SIPS skeleton — the order
  plus variable flow. It is currently computed and then discarded (used only as a validation gate).
  Analysis should compute `binds`/`needs` via `adorn` (with the entry adornment) and retain the graph.
- **Planning consumes the graph + `estimate`.** The planner orders by asking `adorn` for feasibility
  (against the running bound set) and `estimate` for cost, rather than re-walking the per-slot schema
  each call. Just-in-time, memoized adornment is kept (it is the textbook algorithm); the change is
  that feasibility and cost are now distinct, expressive, and declarable.
- **The compiled `Plan` carries the graph-derived binding info** (`binds`/`needs`) per step, so neither
  replanning nor analysis needs a stored syntactic premise — the leaf payload plus the SIPS subsume it.

## Open questions

- **Decompose vs. k-of-n.** The propagator model says decompose multidirectional premises into
  directional sub-premises rather than enrich feasibility with k-of-n. Open: do we decompose at the
  *premise* level (a `sum` premise lowers to three directional `Plan` nodes over shared variables) or
  keep one premise whose `adorn` reports the directional options? Decomposition matches the papers and
  keeps each node's adornment trivial; the cost is more nodes for the planner to order.
- **Serializable `Feasibility` descriptor.** With decomposition, most premises need only `Prefix`
  (ordered inputs → one output). Whether `AnyOf` (equality) and `All` are enough, with `KOf` reserved
  for non-decomposable atoms.
- **Cost as scalar vs. `(class, selectivity)`.** Whether to land the selectivity tie-breaker now or
  keep the scalar ladder and add it when demand reification needs sharper estimates.
- **Where `Feasibility` lives on each premise** so it survives the AST→IR lowering as data on the
  `Plan` variant.
- **How far to take the cell/merge analogy.** Whether the binding environment stays equality-only or
  generalizes to a partial-information cell merge (intervals, type/set narrowing) — relevant only if we
  want constraint *narrowing*, not just ground binding. Out of scope for the planner; noted for the
  incremental-subscriptions work.

## Pointers

- Magic sets / SIPS: Balbin et al. 1991; Alviano *Dynamic Magic Sets* (the lazy `Adorn`/`ProcessQuery`
  worklist).
- Propagators: Radul & Sussman, *The Art of the Propagator* (MIT-CSAIL-TR-2009-002) — cells, merge,
  multidirectional constraints as composed mutual inverses; Radul, *Propagation Networks*
  (MIT-CSAIL-TR-2009-053), §3.2 "Propagation can Go in Any Direction" — the `sum` decomposition (Fig
  3-3) that answers the k-of-n question.
- Cost / selectivity (forward pointer): worst-case-optimal joins, for selectivity-driven scan cost.
- This repo: `rust/dialog-query/src/schema.rs` (`Cardinality::estimate` 16-arm table, `Requirement`/
  `Group`), `rust/dialog-query/src/planner/` (the implicit SIPS), `rust/dialog-query/src/rule/analyzer.rs`
  (`DependencyGraph`).
