# Graph queries

## Idea

A concept describes one entity: the attributes it must carry, read under
their picks. When one of those attributes points at another entity, the
concept says `entity` and stops there. Whoever reads the result gets a
URI and has to know, on their own, which concept to read it under next.

The original intent was always that a concept could say more: a field
can name *another concept* as its type, the way a field in a GraphQL
schema names an object type, so that a concept describes a graph and not
one node of it. Querying a concept would then return the graph, and
subscribing to it would deliver changes anywhere in that graph, a
renamed author under a comment under an issue as much as a retitled
issue.

That never got built as a whole, and once tonk's views landed it was no
longer obvious it should be. Views compose layer by layer: a view renders
a concept, and wherever a field holds a reference it mounts a nested
`<tonk-display>` over it, which picks its own model and facet, loads
lazily and updates on its own. The concept does not decide what the
reader wants from a referenced entity; the subview does. For rendering
that is the right shape.

It is the wrong shape for an agent. An agent invited into a space faces
every concept at once, with nothing that says where to start or how they
connect. The orientation trial (`tonk/plans/2026-10-05-agent-orientation-trial.md`)
dumped the full schema of a small space at 55 KB, 1434 lines, and the
agent's tool output truncated it. Sharing a route instead of a bare space
helps: a route already maps to a concept (`guide-routes.md`), so an agent
handed one has a place to start, and from the concept it can read the
schema and the rules over it. But from there it hits the same wall the
template author does. `issue.assignee` is `as: entity`. Under what
concept is the assignee to be read? A view answers that inside a template
string (`<tonk-display entity={author} model=person …>`), where an agent
reading the schema will not look. The graph exists, but only the views
know it.

So the idea resurfaces: let a concept declare its edges by naming the
concept on the other end. Then a route maps to a concept, and the concept
is the whole connected schema an agent needs, discoverable by walking it.
And a query over that concept can return the graph in one read instead
of one read per layer.

## What exists already

Part of this is built, and the spike should start from it rather than
from scratch.

- **Concept-typed fields as a filter.** dialog has `conforms` on a field
  (`#[dialog(conforms = C)]` in the derive, `"conforms": "concept:…"` on
  the wire, `as: .Ingredient` in dialog's abbreviated notation, see
  `notes/notation.md`, *Concept-typed fields*). It desugars to the target
  concept conjoined as a premise over the field's variable
  (`dialog-query/src/rule/deductive.rs`, `field_premises`): the row
  survives only when the target entity satisfies the target concept.
- **Incremental maintenance through nested concepts.**
  `dialog-query/src/concept/query/affected.rs` discovers which head
  entities a change can affect by a delta-join through concept premises,
  to arbitrary depth, bottom-up. A subscription to `Report` already
  re-derives the reports whose manager lost their badge.
- **What it does not do.** "Only `this` is projected; the target's own
  fields stay internal to the premise." The answer is still one flat row
  per tuple with the referenced entity as a URI. The target is checked,
  then thrown away.
- **Tonk does not expose it.** Nothing under `tonk-notation`,
  `tonk-schema` or the library YAML says `conforms`; every reference in
  the library is `as: entity`.

So the filter half is done. The projection half (the graph in the
result), the subscription half (deltas addressed into the graph) and the
notation half (tonk can say it) are not.

## Where the current design stops

Two limits of `conforms` decide most of what this spike has to do, and
it is better to see them before writing any code.

**A concept cannot refer to itself.** A conforming field holds its target
as a `Box<ConceptDescriptor>`, and a concept's identity hashes its
conformance targets' references (`CHANGELOG.md`, 0.3.0, *merkle
references*). `person.manager: person`, `block.next: block`,
`comment.reply-to: comment` would need a concept whose identity contains
its own identity. Real schemas are full of these cycles, two of the three
reference fields in tonk's notebook library are one. Without an answer
here "the whole graph" means "the acyclic part of the graph", which is
not the idea. Unison's hashing of mutually recursive definitions as one
cycle group is the prior art to look at; naming the target by bookmark
instead of by hash is the cheap answer and gives up structural identity
for those edges.

**Conformance is a filter, and graph reads mostly want a left join.**
Today a conforming field is required and cannot be optional
(`TypeError` "optional conformance is absence over a derived predicate,
rejected until stratification"). For a filter that is right. For a graph
it means an issue whose assignee has no name drops out of the issue
list, and retracting one person's name retracts every issue they are
assigned to. GraphQL made the other choice: a field whose object cannot
be resolved is `null`, and the parent stays. A many-valued edge
(`comment: pick all`) has the same problem one level up: an issue with
no conforming comments drops out instead of returning an empty list.
Both semantics are useful and they are different concepts, so the
notation has to be able to say which one it means, and the optional one
is exactly the absence-over-a-derived-predicate the engine currently
refuses. The spike has to either make that sound under the existing
stratification analysis or argue that graph edges can be read without
it (for example, optional edges whose target is a stored-facts-only
concept, which has no derived predicate under it).

## Concepts are the schema, the query picks the tree

GraphQL is the closest prior art, and the lesson worth taking from it is
not "nest objects in responses". It is the split between the **schema**,
which is a graph and can be cyclic, and the **query**, which selects a
finite tree from it. A GraphQL server never returns "the whole graph";
it returns what the selection set asked for, as deep as the selection
goes.

That split also dissolves the tension with views. A view is a selection:
it decides, per layer, which facet of the referenced concept it wants.
A concept with typed edges is schema: it says what is on the other end,
not how much of it to read. Both can be true at once, and they serve
different readers. An agent walks the schema; a view, or a graph query,
selects from it.

So the proposal is:

1. A concept may type an entity field with another concept. That is
   schema. It makes the graph discoverable from any concept, and it is
   what a route hands an agent.
2. A query over such a concept may return the graph below it. *How
   much* of it is a property of the query, not of the concept. The
   default the deliverable needs is "every edge the concepts declare,
   to a fixpoint", which is finite because entities are (see *Result
   shape*), but a depth bound or an explicit selection is the natural
   next knob and the spike should leave room for it.
3. Views keep working as they do. A view that wants to stop nesting
   mounts a `<tonk-display>` as it does today; a view that wants the
   subgraph in one frame can ask for it.

If the spike finds that the selection half is unnecessary, that "the
concept decides everything" works in practice, that is a result worth
reporting. It is not the default assumption.

## Example

A small issue tracker. Today, in tonk notation, every reference is
`as: entity`:

```yaml
concept!: &person
  with:
    name:
      the: xyz.tonk.person/name
      as: text

concept!: &comment
  with:
    body:
      the: xyz.tonk.comment/body
      as: text
    author:
      the: xyz.tonk.comment/author
      as: entity

concept!: &issue
  with:
    title:
      the: xyz.tonk.issue/title
      as: text
    assignee:
      the: xyz.tonk.issue/assignee
      as: entity
    comment:
      the: xyz.tonk.issue/comment
      as: entity
      pick: :all
```

With typed edges, `as` names the concept on the other end. The spelling
below is a placeholder; settling it (a concept name in `as`, a separate
`conforms:` key mirroring the wire, how a bookmark resolves) is part of
the spike:

```yaml
concept!: &comment
  with:
    body:
      the: xyz.tonk.comment/body
      as: text
    author:
      the: xyz.tonk.comment/author
      as: person               # target must be a person

concept!: &issue
  with:
    title:
      the: xyz.tonk.issue/title
      as: text
    assignee:
      the: xyz.tonk.issue/assignee
      as: person
      optional: true           # an unassigned issue is still an issue
    comment:
      the: xyz.tonk.issue/comment
      as: comment
      pick: :all               # zero or more; see "left join" above
```

The query does not change:

```yaml
issue:
  this: ?issue
```

Today it returns one flat `QueryResult` per tuple (`tonk-worker-api`,
`evaluate.rs`), with `assignee` and each `comment` as a URI, two comments
meaning two rows. With graph queries, the same query returns the issue
with its subgraph:

```yaml
- this: did:key:…issue-1
  title: Sync stalls after resume
  assignee:
    this: did:key:…alice
    name: Alice
  comment:
    - this: did:key:…c1
      body: Repro attached
      author:
        this: did:key:…bob
        name: Bob
    - this: did:key:…c2
      body: Looking into it
      author:
        this: did:key:…alice
        name: Alice
```

And a subscription to it delivers, each case starting from the result
above:

- `issue-1` retitled: a change to the issue node.
- Alice renamed: a change to the `person` node for Alice, which appears
  twice in the tree above but is one node.
- `c1` retracted: the `comment` edge from `issue-1` to `c1` goes, and
  with it `c1`. Bob's node was reachable only through `c1`, so it
  leaves the result too, though nothing about Bob changed.
- `issue-1` reassigned to Bob: the `assignee` edge moves from Alice to
  Bob. Both nodes were already in the result, so no node changes, just
  an edge.

## Result shape and deltas

The tree above is what a reader wants to look at. It is not what the
engine should produce or what a subscription should emit, and deciding
this is probably the most consequential design choice in the spike.

The same entity under the same concept can appear many times in a tree
(Alice above), and in a cyclic schema the tree is infinite. A
**normalized** result has neither problem: a set of nodes, each a
`(concept, entity)` with its scalar fields, and a set of edges, each
`(parent node, field, child node)`. That is what Relay and Apollo
converge on client side, it is what Datomic's pull avoids by being
bounded by its pattern, and it is close to what dialog already has: rows
keyed by entity, which `tonk-display`'s `select_rows` already folds by
`this`. A delta is then node rows asserted and retracted plus edges
asserted and retracted, each row change happens once however many paths
reach it, and the tree is a client-side fold.

The alternative, path-addressed deltas (`issue-1/comment/c2/author/name`),
is closer to what a template binds against and duplicates every change
by the number of paths that reach it. The spike should pick one and say
why, with the other's cost measured on at least one shape where they
differ (a person reachable from many issues).

Two things the delta has to get right whichever shape it takes:

- **Reachability.** A node leaves the result when the last edge to it
  goes, and enters when the first does. That is DRed over the edge
  relation, which `affected.rs` does for the filter today; the graph
  needs it for membership too.
- **Equality of a row.** `ConceptConclusion` equality is entity plus
  resolved values, which is what stops re-derivation emitting
  retract-and-reassert of unchanged rows. Nested values must not break
  that: if a parent's row embeds its children, every child change
  re-emits the parent. A normalized result sidesteps it.

## Performance

The hard constraint: **a query that reads no typed edges must cost what
it costs today.** Graph queries are opt-in at the schema; a concept with
no conforming field compiles to exactly the rule it does now.

Two other paths can change and the spike must measure both:

- **Existing `conforms` users.** A conforming field today projects only
  `this`. If projecting the target's fields becomes the default, every
  existing conforming query reads more. Either the projection is asked
  for, or the spike shows it costs nothing for a target already scanned
  by the filter (it should be close: the filter already reads the
  target's facts to decide, and throws them away).
- **Subscriptions.** `affected_entities` returns `None` on recursion and
  the subscription falls back to full re-evaluation. A cyclic schema is
  recursive by construction, so without work here every graph
  subscription over a cyclic concept recomputes on every touching
  commit. That is correct and it is not acceptable as the end state,
  but it is acceptable for the spike if the report says which shapes
  fall back.

The comparison that argues for graph queries, as opposed to merely
against regressions, is against what tonk does now. A directory of N
issues with an author each, rendered with nested `<tonk-display>`,
opens three subscriptions per nested display (model, view, entity;
`tonk-display/README.md`), so roughly `3 + 3N` standing queries. One
graph subscription replacing them is the win the spike can show.

## Deliverable

Graph queries in dialog, usable from tonk notation, with subscriptions:

- In tonk notation, a concept's entity field names another concept, and
  the engine enforces it.
- Querying that concept returns the graph below it: each matched entity
  with the fields of the entities its edges reach, transitively, through
  the tonk worker's evaluate path.
- Subscribing to it delivers differential updates: a change to a nested
  entity, to an edge, or to a top-level entity each arrives as a delta
  of what changed, not a re-sent result.
- At least one optional edge and one many-valued edge, with the
  semantics the spike chose for each written down.
- At least one cyclic schema (`person.manager: person` or
  `block.next: block`) that can be declared, queried and subscribed to,
  even if its subscription falls back to recompute.
- The measurements below.

Not part of the deliverable, nice to have: `#[derive(Concept)]`
producing nested Rust types from conforming fields; selection sets or
depth bounds in the query; migrating a tonk view to a graph
subscription.

## Goal

Find out whether concepts can describe a connected graph, and whether a
query and a subscription over such a concept can return and maintain
that graph, on top of the existing engine and without making anything
that does not use it slower.

## What success looks like

- The deliverable above works end to end through tonk notation.
- The `dialog-perf` gate holds as it stands: steps exact, blocks within
  two, bytes within 15%, instructions within 3%, on every existing
  scenario. No scenario's baseline is re-recorded to make room for this.
- New `dialog-perf` scenarios for a graph query and a graph subscription
  (a nested rename, an edge move, a top-level change), recorded as
  baselines for whoever comes next.
- One graph subscription is measured against the per-layer subscriptions
  tonk-display opens for the same render, and the report says which is
  cheaper and by how much.
- The change fits the existing architecture: concepts still compile to
  rules, subscriptions are still demand-gated and pull-driven, and the
  compiled rule for a concept with no typed edge is unchanged.

## What failure looks like

Any of these answers the question with a no, and that is a fine outcome
if the report shows why:

- An existing `dialog-perf` scenario regresses beyond its tolerance and
  the spike cannot get it back without dropping the feature.
- It takes a rewrite: a second evaluator, operator state retained
  between polls (the push-model `notes/incremental-subscriptions.md`
  rejects), or a change to how every concept compiles rather than to
  concepts with typed edges.
- It changes stored or synced encodings for concepts that do not use it.
  A concept with no typed edge must keep its identity.
- Cycles cannot be expressed without giving up structural concept
  identity everywhere, not just on the cyclic edges.
- The optional and many-valued edges cannot be made sound and the only
  graph left is the inner-join one, where one missing name deletes a
  parent. That is the filter dialog already has, and not worth shipping
  as "graph queries".
- Every graph subscription falls back to full recompute, so subscribing
  to a graph is strictly worse than the per-layer subscriptions it would
  replace.

## Questions the spike should answer, not assume

- Is "the concept decides the whole graph" what agents and views
  actually want, or does it need a query-side selection from day one?
  Try it with an agent: hand one a route to a concept with typed edges
  and see whether it navigates the space better than it does from the
  schema dump.
- Is the whole reachable graph the right default, given that
  `person.manager` reachability is the whole org chart? What bounds it
  if not the query?
- Does a typed edge change what an assertion through that concept means?
  Asserting an issue with `assignee: person` could check the target
  conforms, write the nested person too, or ignore the type on write.
  dialog's schema is read-side; the cheap and consistent answer is
  "ignore on write", but say so.
- Where does the graph belong in the wire format: inside
  `QueryResult.fields` as nested values, or as the normalized node and
  edge sets beside it?

## Prior art

- **GraphQL.** Schema as a typed graph, possibly cyclic; queries as
  selection sets producing finite trees; nullable fields as the answer
  to an unresolvable edge. Subscriptions there are event streams, not
  differential, so the model is the schema and query split, not the
  subscription.
- **Relay and Apollo normalized caches.** Results flattened to nodes by
  id with edges between them, the tree reassembled at read. The closest
  thing to the delta shape proposed here.
- **Datomic pull.** A pattern over entity attributes, nested through
  references, with explicit recursion limits (`{:friend 3}`, `...`).
  Pull is the selection half done over a schema that has no types on
  refs at all.
- **EdgeDB (Gel) shapes.** Object types with links to other object
  types, and queries that select a shape through links. The closest to
  "the schema declares the edge's target type".
- **Unison.** Hashes mutually recursive definitions as one cycle group,
  which is the reference for structural identity over a cyclic schema.
- **This repository.** `notes/incremental-subscriptions.md` for the
  pull-driven maintenance model graph subscriptions have to stay inside;
  `notes/optional-fields.md` for set-widening, which optional edges
  extend; `dialog-query/src/concept/query/affected.rs` for the nested
  delta-join that already exists.
