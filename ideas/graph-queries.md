# Graph queries

## Idea

A concept's entity-valued field today is `as: entity`. The reader gets a
URI and has to know which concept to read it under. The idea is to let
the field name a concept, so a concept describes a graph, a query over
it returns the graph, and a subscription delivers changes anywhere in
it.

Views made this less obvious than it was. A view renders one concept and
mounts a nested `<tonk-display>` per reference, each choosing its own
model and facet and loading lazily. That works for rendering. It does
not help an agent: the graph is only recorded inside view templates. The
orientation trial (`tonk/plans/2026-10-05-agent-orientation-trial.md`)
dumped a small space's schema at 55 KB and the agent's output truncated.
A route maps to a concept (`guide-routes.md`), so sharing a route gives
an agent a starting point; typed edges would let it walk from there.

## What exists

- `conforms` on a concept field (`#[dialog(conforms = C)]`, `"conforms"`
  on the wire, `as: .Ingredient` in `notes/notation.md`). It conjoins the
  target concept onto the field's variable
  (`dialog-query/src/rule/deductive.rs`, `field_premises`): a filter.
- `dialog-query/src/concept/query/affected.rs` propagates subscription
  changes through nested concept premises.
- Only `this` of the target is projected. Results are flat rows with the
  target as a URI.
- Tonk notation does not expose `conforms`; the library uses
  `as: entity` throughout.

## Constraints

- **Self-reference.** A conforming field holds its target as a
  `Box<ConceptDescriptor>` and the concept's identity hashes the
  target's reference. `person.manager: person` or `block.next: block`
  cannot be expressed. Options: name the target by bookmark on cyclic
  edges, or hash mutually recursive concepts as one group (as Unison
  does).
- **Least fixpoint.** A concept is a rule. `person` requiring
  `manager: person`, with no other rule for `person`, derives nothing.
  A required cyclic edge yields an empty concept, so cyclic edges can
  only shape results, never filter them. This also keeps matching
  non-recursive.
- **Inner join.** Conformance is required and cannot be optional (the
  engine refuses absence over a derived predicate). As a graph read this
  means an issue drops out when its assignee lacks a name, and an issue
  with no conforming comments drops out instead of returning an empty
  list.

## Design

Datomic pull ([spec](https://docs.datomic.com/query/query-pull.html))
is the main reference. `:where` selects entities; `(pull ?e pattern)`
then shapes each one by walking references. Properties that carry over:

- Shaping runs after matching and cannot change which entities matched.
  In dialog terms it sits above the query's stratum, so reading absence
  there is sound. The spike should check this against
  `session/dependencies.rs`.
- Missing attributes are omitted; an empty many-valued reference is an
  empty vector; the parent is kept.
- Recursion is explicit (`{:person/manager 3}`, `...`); an entity
  already seen returns its id only.
- Reverse references (`:release/_artists`). Tonk's library points
  children at parents (`xyz.tonk.notebook.block/notebook`), so a
  notebook with its blocks needs one. The VAE index makes it a prefix
  scan.
- `*` follows component references and returns ids for others, which
  bounds the walk.
- Patterns are data and can be query inputs.

What pull lacks: refs are untyped, so the pattern author must already
know the graph; patterns carry no predicates; there are no
subscriptions.

Proposal:

1. **Schema.** A field may name a concept as its type. Possibly also
   mark the edge as owned (component) or a reference.
2. **Matching.** On a root, a required typed edge filters, as
   `conforms` does today.
3. **Shaping.** Every other edge, and every edge of a non-root entity,
   shapes only. The default pattern is the concept's typed edges,
   following owned edges and stopping at references. A query may give
   its own pattern.
4. **Receiver-side conformance.** For each entity an edge reaches, the
   server returns an attribute bag: the attributes the target concept
   names, read under their picks, whichever the entity has. The
   receiver checks the bag and keeps, drops or flags the node, with a
   reason ("`c3` is not a `comment`: `author` missing").
5. **Views.** Unchanged. A nested `<tonk-display>` is one map spec; a
   view may state a pattern to get the subgraph in one frame.

Receiver-side conformance:

- Makes server-side shaping entity-local (EAV prefix reads, no target
  premise, no fixpoint), maintained on the existing per-entity path.
- Lets the server over-approximate reachability: the receiver computes
  it from edge values in the bags, so stale nodes can be pruned lazily
  and only new reachability must be prompt.
- Gives per (entity, attribute) deltas.
- Explains non-conformance to agents and views.

Limits:

- Derived attributes are evaluated on the server and shipped in the
  bag; cheap for entity-local rules, a fixpoint for rules reading other
  entities. Only the conjunction moves to the receiver.
- The check must be one function in `dialog-query`, used by the engine
  for roots and by the receiver (tonk-display already runs Rust as
  wasm). A second implementation would drift.
- Bags for non-conforming nodes cost bytes; measure them.

## Narrowing

`<tonk-display>` narrows by entity and by equality on the model's own
fields (`model="person?name=Alice"`, `tonk-template/src/resolve.rs`).
Nothing nested. Two cases:

- **Roots by a nested attribute** ("issues assigned to Alice"). Matching;
  may drop a root. Datomic does it in `:where`. In dialog, a constant on
  a nested field pushed into the target premise, which today receives
  only `this`.
- **A nested edge** ("each issue, only Alice's comments"). Shaping;
  never drops the parent. Pull cannot express it (`:as`, `:limit`,
  `:default`, `:xform` only). GraphQL field arguments and EdgeDB nested
  `filter` can.

The notation must make the stage of a constraint clear. In both cases
the constant goes into the scan, so it narrows what is read and the
subscription's demand cover.

## Example

Today:

```yaml
concept!: &person
  with:
    name: { the: xyz.tonk.person/name, as: text }

concept!: &comment
  with:
    body:   { the: xyz.tonk.comment/body, as: text }
    author: { the: xyz.tonk.comment/author, as: entity }

concept!: &issue
  with:
    title:    { the: xyz.tonk.issue/title, as: text }
    assignee: { the: xyz.tonk.issue/assignee, as: entity }
    comment:  { the: xyz.tonk.issue/comment, as: entity, pick: :all }
```

With typed edges (spelling is a placeholder):

```yaml
concept!: &comment
  with:
    body:   { the: xyz.tonk.comment/body, as: text }
    author: { the: xyz.tonk.comment/author, as: person }

concept!: &issue
  with:
    title:    { the: xyz.tonk.issue/title, as: text }
    assignee: { the: xyz.tonk.issue/assignee, as: person, optional: true }
    comment:  { the: xyz.tonk.issue/comment, as: comment, pick: :all }
```

Queried as a root, a comment whose author is not a `person` is not
returned. Reached through `issue.comment`, it is shipped as a bag and
checked by the receiver.

The query `issue: { this: ?issue }` today returns one flat row per
tuple (`tonk-worker-api/src/evaluate.rs`). With graph queries:

```yaml
- this: did:key:…issue-1
  title: Sync stalls after resume
  assignee: { this: did:key:…alice, name: Alice }
  comment:
    - this: did:key:…c1
      body: Repro attached
      author: { this: did:key:…bob, name: Bob }
    - this: did:key:…c2
      body: Looking into it
      author: { this: did:key:…alice, name: Alice }
```

A comment `c3` with a body and no author is shipped as
`{this: c3, body: "+1"}`; the receiver drops it and reports the missing
`author`.

A custom pattern, with a reverse edge:

```yaml
issue:
  this: ?issue
  shape:
    title: .
    assignee: [name]
    xyz.tonk.issue/_blocks: [title]
```

Subscription deltas, each from the result above:

- `issue-1` retitled: change on the issue node.
- Alice renamed: one change on Alice's node, though it appears twice.
- `c1` retracted: the edge and `c1` go; Bob is no longer reachable.
- Reassigned to Bob: the `assignee` edge moves; no node changes.

## Result shape and deltas

A tree repeats shared entities and is infinite over a cycle. A
normalized result avoids both: nodes (entity and attribute bag) and
edges (the entity-valued attributes in the bags). Relay and Apollo
caches use this shape; pull falls back to it only on cycles.
`tonk-display`'s `select_rows` already folds rows by `this`. Deltas are
per node and edge; the tree is a receiver-side fold.

Path-addressed deltas (`issue-1/comment/c2/author/name`) match template
bindings but repeat a change once per path. Pick one and measure the
other where they differ (a person reachable from many issues).

Either way:

- New reachability must be delivered promptly; lost reachability can be
  lazy under receiver-side conformance, otherwise it needs DRed over
  edges.
- `ConceptConclusion` equality (entity plus values) must not see nested
  values, or every child change re-emits the parent.

## Performance

- A concept without typed edges compiles to the same rule and costs the
  same.
- Existing `conforms` queries must not read more unless asked.
- Recursive subscriptions are already incremental: a retained fixpoint
  table, semi-naive for additions, DRed for deletions
  (`dialog-query/src/concept/query/fixpoint.rs`, `extend`, `retract`).
  They rebuild when a change matches a negated or optional premise,
  meets a non-local nested concept, or a rule body consumes more than
  one deletion. Graph queries hit those shapes. Requirement: a graph
  subscription takes no full-recompute path that the same data read
  without the graph would not take.
- Comparison to make: N issues with nested `<tonk-display>` open about
  `3 + 3N` subscriptions (model, view, entity per display;
  `tonk-display/README.md`) against one graph subscription.

## Deliverable

- Tonk notation: a field names a concept; the engine enforces it on
  roots.
- A query returns the graph through the worker's evaluate path.
- A subscription delivers deltas for nested, edge and root changes.
- Matching and shaping as separate stages, with required, optional and
  many-valued edges exercised.
- Nested nodes as attribute bags, checked by one shared function, with
  reasons.
- A cyclic schema (`person.manager` or `block.next`) declared, queried
  and subscribed to without full recompute: a deep rename, an edge
  closing a cycle, an edge cutting one.
- Narrowing by a nested attribute in both stages, with the constant
  reaching the scan.
- The measurements below.

Optional: `#[derive(Concept)]` nested types; full pattern features
(map specs, reverse edges, depth, `:limit`); owned vs reference edges;
porting a tonk view.

## Goal

Find out whether concepts can describe a graph, and whether queries and
subscriptions over them can return and maintain it on the existing
engine without slowing anything that does not use it.

## Success

- The deliverable works through tonk notation.
- The `dialog-perf` gate holds unchanged (steps exact, blocks within
  two, bytes within 15%, instructions within 3%); no baseline is
  re-recorded.
- New scenarios for a graph query and subscription (nested rename, edge
  move, root change) are added with baselines.
- The `3 + 3N` comparison is reported.
- Concepts still compile to rules; subscriptions stay demand-gated and
  pull-driven.

## Failure

- An existing perf scenario regresses and cannot be recovered.
- It needs a second evaluator, retained operator state, or a change to
  how every concept compiles.
- Concepts without typed edges change identity or encoding.
- Cycles require giving up structural identity everywhere.
- Shaping cannot be kept above matching, leaving only the inner join.
- A graph subscription adds a full-recompute path.
- The receiver's check and the engine's disagree, or cannot share code.
- Non-conforming bags dominate shipped bytes.
- Nested narrowing only works as a post-read filter.

## Open questions

- Does the concept's default pattern suffice, or is a query-side
  pattern needed from the start? Test with an agent given a route.
- What bounds the default walk? Do tonk schemas have an owned/reference
  distinction, and who declares it?
- Should a bag hold only the concept's attributes, or everything the
  entity has (`*`)?
- Where does a pattern live: the query, a view, or stored and
  addressable by a route?
- Does a typed edge affect writes? Suggested: no, schema is read-side.
- Wire format: nested values in `QueryResult.fields`, or node and edge
  sets beside it?

## Prior art

- **Datomic pull**: see *Design*. Datascript and XTDB 1.x use the same
  pattern language.
- **GraphQL**: typed, possibly cyclic schema; selection sets; nullable
  fields; field arguments.
- **EdgeDB (Gel)**: typed links; nested shapes with `filter`,
  `order by`, `limit`.
- **Relay, Apollo**: normalized client caches.
- **Unison**: hashing of mutually recursive definitions.
- **This repository**: `notes/incremental-subscriptions.md`,
  `notes/optional-fields.md`, `affected.rs`, `fixpoint.rs`.
