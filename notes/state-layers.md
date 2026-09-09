# Stacks: layers, scopes, and where an attribute's facts live

Status: proposal, superseding the fixed-scope model of the first
increment. What the increment built and what carries over is listed at
the end. Motivated by #483 and by tonk's overlay, transient, and
command workarounds.

## Vocabulary

- A **layer** is anything with a head and a readable store: a `Branch`
  (durable, head in a cell, replicates to its upstreams), a `Snapshot`
  (durable, head by value), and, new, an **ephemeral layer** (memory
  backed, head by value, no history, dies with the process; a
  *channel* is an ephemeral layer that replicates to a branch's peers
  through the log described below). The word is the image editor's:
  a layer is where the pixels are, layers stack, an upper layer's
  retraction occludes what is beneath it, and the composite is what
  you see. A locked layer is a read-only link, a hidden layer a
  write-only one, a linked smart object a branch captured at a
  revision, an adjustment layer a deductive rule.
- A **link** is a fact held by one layer naming another layer beneath
  it: the enclosing layer captures the enclosed layer's head whenever it
  commits. Links carry a **name**, an entity; several links from one
  layer may share a name.
- A **scope** is a name, an entity: which layers are in scope under
  it is what a stack's links say. It is what placements refer to and
  what a query selects from. Early drafts used "layer" for both the
  store and the name on the assumption they were one to one; they are
  many to many, so the store keeps the word and the name is a scope.
- A **stack** is a layer together with everything reachable through
  its links. The bottom is the slowest and widest layer, the top the
  fastest and narrowest. A stack is built by API, per replica or per
  connection, and may be registered under a name so others can find
  it.
- A **placement** maps an attribute to a scope name. It is a fact on
  the bottom layer, replicated with the schema. A repository-wide fact
  names the scope an attribute without a placement belongs to.

The split that makes this coherent: what is *shared* (which scope
names exist, which attribute goes where) lives in the replicated tree;
what is *local* (which concrete layer stands under a name on this
replica, for this tab) is a binding made at stack construction.

## Declarations, as facts on the bottom layer

```
<repository did>      dialog.attribute/default  memory:shared   # the implicit scope
attribute:ui/selected dialog.attribute/scope    memory:tab      # an override
```

Those are the only replicated declarations. Scope names are plain
entities under a `memory:` convention; nothing about them is fixed.
Their properties (durable, audience) are properties of the *layers*
bound under the name, known to the builder, not facts. A replica that
meets a placement naming a scope its stack does not bind fails the
write with a clear error rather than routing elsewhere; the fix is in
the stack, which is local, not in the schema.

Notation, as concept-level sugar the analyzer lowers to the fact
above:

```yaml
concept!: &site
  scope: memory:tab
  with:
    path: { the: xyz.tonk.site/path, as: text }
```

## Topology as facts

A stack's shape is data, not builder state. Each enclosing layer holds
one **link** per layer it encloses, in its own tree, written as
machinery in the same commit that moves its head:

```
<link> dialog.link/from      <enclosing layer>
<link> dialog.link/to        <enclosed layer>
<link> dialog.link/name      memory:shared        # the scope this link binds under
<link> dialog.link/revision  <revision of the enclosed layer, as last seen>
```

plus the enclosed layer's address (its repository DID and branch name,
or its ephemeral kind), in the `dialog.branch/*` vocabulary the
session metadata already uses at query time. The link is its own
entity rather than a fact on the enclosed layer because a composite
read unions every tree: local and gossip both link shared, at
different revisions, and only a link entity keeps the two apart.

**Wiring stays where it is made.** Each layer holds only its own links.
The whole topology is still one composite read away, since every
layer is in the composite, and an opener walks direct links from the
top. Copying every link fact upward was considered and dropped: it
amplified writes, and under placement-guided reads the copies would
have needed their own exemption from routing.

### Two identities per layer, and the descriptor blob

A layer has an **address**, where its head lives: repository DID plus
branch name for a branch, a nonce for an ephemeral layer. A branch
entity today is derived from the repository DID, the profile, and the
name, which differs per profile for the same branch, so it is not
usable as a link target; the address is.

A layer also has a **stack identity**: the hash of its **descriptor**,
a canonical dag-cbor blob in the archive, exactly the way a rule or a
concept descriptor is content-addressed and hydrated on read:

```
descriptor(layer) = { address(layer), links: sorted [(name, id(to))] }
id(layer)         = stack:<base58(blake3(descriptor))>
```

The bottom layer, linking nothing, has the descriptor `{address}` and
so the same identity on every replica. The descriptor holds the
*shape* and nothing that moves: what an encloser last saw is the
`dialog.link/revision` fact in its tree, keyed by
`link:blake3({address(from), id(to)})`, refreshed eagerly by stack
commits. `from` is
an address, not an identity, because the link is part of what defines
the encloser's identity. The descriptor names the shape; the facts
name the time.

Consequences of merkelizing the stack this way:

- **A stack is one hash.** `Stack::open("stack:...")` fetches the
  descriptor, opens each layer by its address, and recurses. A stack
  can be handed to another process as a string.
- **Cycles are unconstructible.** `id(A)` needs `id(B)`, which would
  need `id(A)`. The ladder (mutual one-rung-stale links) is gone with
  them, since it needs both directions.
- **Tampering is detected on fetch.** A blob that does not hash to
  the identity a link claims is rejected, so no cycle search and no
  separate verification pass.
- **Descriptors deduplicate.** Every replica's descriptor for
  `shared` is the same blob, and stacks with the same shape below a
  point share the blobs below it. The archive's blob replication path
  carries them, so a peer holding only a hash can resolve it.
- **`to` is audience-independent.** A stack identity is a pure
  function of addresses and structure, so every reader anywhere
  computes the same one.
- **Shape is identity.** Adding a link under local, or renaming a
  scope, changes `id(local)`, so every link *to* local is stale and
  must be rewritten. That is ordinary Merkle behaviour; it is free for
  ephemeral layers, which are rebuilt per process, and it is a
  migration for any durable layer above a re-shaped durable layer. In
  the layout below nothing durable sits above local, so it costs
  nothing today, and it is the constraint to remember when a second
  durable scope is added.

Why facts rather than a field on the revision: a rule can premise on
them. The enclosed layer's *current* head is already readable as
`dialog.branch/revision` metadata, so "local's link to shared is
behind shared's head" is an ordinary two-premise body, and stale
derivation is a rule's concern, not engine code. The audience rule
(below) guarantees a link is resolvable wherever it is readable,
because a layer only ever links layers beneath it.

### Refresh is eager, and topology decides what is captured

A link's `revision` records the head of the enclosed layer as the
encloser last saw it, and a stack commit refreshes it on every layer
above a layer that moved, bottom to top, in the same commit. After a
stack commit the top layer's head therefore transitively names the
head of every layer beneath it: the one hash for the composite. A
link whose target did not move is a no-op refresh and mints nothing,
so the refresh reaches exactly the layers above the movement.

The cost is bounded by the audience rule: a durable layer may only
link durable layers, so ephemeral churn never forces a durable commit,
and the only thing that ripples into a branch is another branch
moving. Where even that is unwanted, the topology is the knob, not
the refresh policy. Linking is capturing, so a layer that should not
record another's head does not link it:

```
shared          shared
  |               \
local     vs.      state
  |               /
state           local
```

In the chain, local commits whenever shared moves and durably records
which shared head it was consistent with. In the sibling shape, a
commit to shared refreshes only state, local never moves, and the
pairing lives only in the ephemeral top: gone after a restart, which
is the case where the chain is the right choice.

Movement that bypasses the stack, a pull on the bottom or a direct
commit to one layer, is not seen until the next stack commit; the top
hash is then stale, not wrong. Capturing that is the open question
on pulls below.

### Recovery

`Stack::open` takes a layer or a stack identity and walks descriptors
downward, each fetch verifying its hash (a visited set only spares
re-walking a diamond). The durable part of a tab stack recovers from
`main.local`; the ephemeral scopes above it are rebuilt by the
process, as they would be anyway. If that rebuild should be
data-driven too, a durable layer may hold template facts pointing up,
name plus kind and no revision, which reference no head and so do not
violate the audience rule. Templates stay builder code for now.

## Building a stack

```rust
let shared = repo.branch("main").open().perform(&env).await?;
let local = repo.branch("main.local").open().perform(&env).await?;
let gossip = Ephemeral::channel(&shared);
let state = Ephemeral::new();
let tab = Ephemeral::new();

let stack = Stack::builder()
    .layer(shared)                                    // memory:shared by the repository default
    .layer(local).link(&shared, "memory:shared")
    .layer(gossip).link(&shared, "memory:shared")
    .layer(state).link(&local, "memory:local").link(&gossip, "memory:gossip")
    .layer(tab).link(&state, "memory:state")
    .build()
    .perform(&env)
    .await?;
```

`link` declares a link from the layer just added to a layer beneath it,
under a name; `build` validates and asserts the links, and every later
commit of an enclosing layer refreshes its links' revisions. Enclosure
is explicit rather than derived from list order: the derivation would
produce the same graph here, but a rule that needs explaining must
not be the only way to say it. A flat topology, every layer linked
straight from the top, is expressible and is a bad idea for the same
reason a deep one is good: the top is the tip that names everything
beneath it, and a flat stack makes the top pay every capture.

`build` checks:

- **Every name a placement can target is bound**, including the
  repository default. A write naming a scope the stack does not bind
  fails with a clear error. A catch-all scope is a possible flag; it
  is deliberately not the default, since it turns a schema and stack
  mismatch into silent misplacement.
- **Descriptors resolve.** Cycles cannot be built (see the descriptor
  blob); a blob that does not hash to its claimed identity is a
  configuration error.
- **The audience rule.** A layer may link a layer beneath it only if
  that layer's audience contains its own. A capture is a revision
  hash; if gossip captured local, every gossip instant a peer received
  would reference a head the peer cannot resolve. So gossip (audience:
  the peers) links shared and not local (audience: this device); state
  (audience: this process) links local and gossip; tab links state:

```
|-------------------------|
|           tab           |
|-------------------------|
|          state          |
|------------|------------|
|   local    |            |
|------------|   gossip   |
|   shared   |            |
|-------------------------|
```

Audience is a property of the layer: a branch's is its peers, an
`Ephemeral::new()` is this process, a channel is the peers of the
branch it is built from.

### Several links under one name

A name may be bound by more than one link from the same layer. A write
to that name lands in every layer so bound, and a retract removes from
every one. This is
cheap in the model and allowed, with two consequences to keep in view:
the composite read must dedup on entity, attribute, and value across
same-named layers, since tree facts carry a per-layer cause; and a fact
that lives in two places has two lifetimes under one name, so "keep
locally and also broadcast" is usually better said as two attributes
or a rule that copies. Reach for it rarely.

### Naming

```rust
let stack = stack.named("tab:123");   // registers; still a handle
```

Naming is optional. A named stack is registered in a process-local
registry on the `Repository`, and the registry is exposed at query
time as metadata facts, exactly the way `dialog.session/branch` lists
the layers in scope today:

```
dialog.stack/name    of <stack>   is "tab:123"
dialog.stack/top     of <stack>   is <layer entity>    # walk its links for the rest
```

So an inspector enumerates stacks and their layers with an ordinary
query and joins their tab layers with the composite subscription that
already exists. Dropping the handle unregisters it; an anonymous
stack is just never listed. Nothing durable is written by naming.

## Using a stack

```rust
stack.select(query).perform(&env)          // composite read, all layers
stack.subscribe(query)                     // composite subscription, pins every layer
stack.transaction().assert(doc).commit().perform(&env)   // routes by placement
stack.scope(&"memory:tab".parse()?)        // the bound layer(s), for direct access
stack.revision()                           // the stack revision: see below
```

`Stack` is what `QueryLayer` already is with two additions: layers
reached through named links, and a transaction. The composite subscription built in
the first increment carries over unchanged; its vector of pins *is*
the stack revision.

### Transactions and capture

A stack holds, per layer, the head it last **published** or pulled and,
for branch layers, a **staged** chain of commits not yet published,
mirroring a branch transaction after #488: `commit` stages, `publish`
moves heads. A transaction accumulates instructions as today. At
commit, induction reads the heads the stack reads at (staged tips over
published heads), the settled batch is partitioned by each attribute's
scope, and the layers stage **bottom to top**: each branch layer extends
its staged chain (or opens one on its published head) with its own
share and its wiring at the heads as they stand once the layers beneath
it staged; an ephemeral layer is written directly, having nothing to
publish. A commit never moves a branch head and never fails because
one moved. `publish` then moves every staged layer's head to its chain
tip, bottom to top, each with one CAS against the version the stack
last published or pulled. `commit().publish()` does both.
Consequences:

- **Reads are pinned.** A stack is read at its top's head: every layer
  beneath the top is read at its staged tip or published head, not
  at its live head, so what a read sees is exactly what the top's
  hash names. A branch is read pinned through its own handle (caches,
  remote fallback, session store intact), not through a snapshot.
- **A moved layer fails to publish, never to commit.** If a layer's
  head moved outside the stack, through this handle or another, its
  chain fails the publish CAS. That chain and every chain above it
  are stale wholesale and dropped; layers beneath stay published. The
  recovery is a pull and a re-run of the transactions, never a rewrite
  of staged versions, exactly as for a branch batch.
- **Ephemeral shares land at stage time.** A transaction's ephemeral
  share is applied when the transaction stages, so if a lower layer's
  publish later fails, the ephemeral layers are ahead until the re-run,
  which re-applies the same facts. An ephemeral layer has no write API
  outside the stack, so this is the only way it gets ahead.
- **Movement enters on pull, leaves on push.** `stack.pull()`
  re-resolves every branch layer's head from storage, pulls every layer
  that tracks an upstream, bottom to top, drops everything staged,
  takes the live heads as published, and stages and publishes the
  wiring of every layer above a layer that moved. `stack.push()` pushes
  every layer with an upstream, bottom to top, so a pushed layer's
  wiring never names a head its upstream lacks; it pushes published
  heads only. Reads and subscription polls never write: until the
  pull, the stack is behind, not wrong, and `behind()` says so.
- **Partial failure.** A publish that fails at layer `i` leaves layers
  beneath it published and drops the chains from `i` up. A pull that
  fails part way leaves the published heads where they were: layers
  beneath the failure may have reconciled with their upstreams, the
  stack does not read at them until a pull completes, and a later
  push of those layers may find nothing to push. Wiring conflicts that
  a merge resolves either way are overwritten by the next capture
  with the actual head.
- **Stale derivation is a rule.** A layer whose link revision differs
  from the enclosed layer's current head is behind, and a rule can say
  so, which is the induction watermark generalized to a pair of layers.

A revision on an ephemeral layer is an identity, not a persistence
claim: a hash of its state plus its links, with no parent chain
retained. That is enough for subscription pins, diffs, and captures.

### Retracts on a linked scope

A retract of a fact on scope L removes it from L's layer. If L's layer
never held it, the retract is a tombstone over the layers below, which
is the only way a fact can appear under an attribute placed on L: a
peer without the placement wrote it to the bottom. That situation is
a placement divergence and should surface as a warning on pull, not
be silently masked. This is the one shadowing case the model admits
and it is diagnosable because placement is in the tree.

## Instants, queues, and the log

An **instant** is one induction round. A commit is a sequence of
instants; durable layers fold them into one revision because storing
intermediates costs storage. The two things folding loses are the
intermediate rounds and any fact asserted and retracted within one
commit, which is exactly what a rule-concluded transient is.

An **observer** is anything that wants to see instants rather than
folds: a state subscription (which needs the touched facts since its
last poll to maintain incrementally) or an event handler (which needs
every instant, including the ones that folded away). Observers
register with a layer when they are created and unregister when they
are dropped.

For a **local ephemeral layer** (state, tab), do not keep a shared log. Fan each
instant out at write time into per-observer queues, filtered by each
observer's demand. An instant nobody demanded costs nothing; memory is
the sum of unconsumed matched instants across observers, which is what
any event system pays. A queue has a ring bound; an observer that
falls off the ring gets a gap marker and recomputes from the fold.
"All have seen it" is not a question here: each observer owns its
queue and drains it.

For a **channel** (a replicated ephemeral layer such as gossip), the
log is the right shape and the peers are the observers. Each peer holds an offset into the layer's log; sync is
"send me instants past my offset"; retention is the minimum peer
offset with a ring bound, and a peer that falls off resyncs from the
fold. That is how presence and awareness protocols already work, with
a per-peer clock standing in for the offset. It is also what makes
that scope's `replicated` property meaningful without a tree: there is
nothing to push except the log.

With either mechanism in place, `transient:` becomes sugar: an
attribute placed on an ephemeral scope plus a sweep rule `retract! C
when C`, and the engine's transient bucket becomes an optimization it
may apply when no observer demands the attribute.

## What is built

**Increment 1** (fixed scopes, composite subscriptions): a `Scope`
enum, `Placement` as a fact on the branch, partition-after-induction
routing with the tree as the top and the session store as the only
other backed scope, and composite subscriptions over `QueryLayer`
with a pin per layer.

**Increment 2** (the ephemeral layer, `repository/ephemeral.rs`): the
session overlay is replaced by a real memory-backed store,
`Ephemeral`, which every branch and snapshot carries:

- Facts are held under the tree's own three index keys (entity,
  attribute, and value orders) in one ordered map, so a selector's
  `selector_range` applies unchanged and rows stream in exactly the
  order a tree scan produces them. The query scope's k-way merge
  interleaves the store with tree scans with no special case.
- Writes have the tree's semantics: idempotent assert, cardinality-one
  replace that supersedes the cell, exact retract. A retract of a fact
  the store does not hold is a tombstone that hides it in the layers
  beneath, so session shadowing of committed facts still works and
  the store's own facts are never shadowed by it.
- Every visible change mints an `Instant` (asserted facts, retracted
  facts, sequence, chained hash) into a bounded ring. A subscription
  pins the sequence and reads the exact delta since its pin, filtered
  by its demand cover, so a session write inside the cover is
  maintained per touched entity and one outside the cover advances
  the pin for free. Only a pin older than the ring recomputes. The
  chained hash costs the delta, never the store, which is the identity
  an ephemeral layer needs and the trade the design accepted.
- The store is read live by every read path: `QueryEnv` unions each
  layer's store stream and lifts its tombstones itself, so `QueryLayer`
  no longer snapshots session facts at construction and the
  subscription's subtract workaround is gone. Rules asserted into the
  store resolve as their own scope, read fresh.

**Increment 3** (placement by scope entity, `placement.rs`): the
fixed `Scope` enum is gone.

- A scope is an entity, conventionally `memory:<name>`, and nothing
  about the name is fixed. `dialog.attribute/scope` of an attribute
  entity is a scope entity; `dialog.attribute/default` of the
  repository DID names the scope an attribute with no placement
  belongs to, which is the tree's name. With no default declared,
  undeclared attributes reach the tree as before. A declaration
  whose value is not an entity fails the commit
  (`InvalidPlacement`).
- A layer carries local **bindings** from scope entities to its stores
  (`Branch::bind(scope, Target::Tree | Target::Session)`, shared
  across clones like the caches; a snapshot minted from a branch
  shares the branch's). The tree needs no binding: the default names
  it. A write naming a scope the layer does not bind fails the commit
  (`UnboundScope`), and binding it afterwards makes the same write
  succeed. This is the seam the stack builder drives next: a stack's
  links become bindings to other layers.
- Routing is unchanged in shape: after induction the settled batch
  is partitioned by each attribute's scope, resolved through the
  declarations and the bindings, into the tree commit and the
  ephemeral store.

**Increment 4** (the stack, `stack.rs`): layers linked under scope
names, read as one composite and written by placement.

- A `Scope` is a branch, a snapshot, or an `Ephemeral` store; a stack
  is built bottom first with `Stack::builder().layer(a).layer(b).link(&a,
  name)`. `link` binds a name to the layer it points at, so
  `local.link(&shared, "memory:shared")` routes `memory:shared` to
  `shared`. A name may be bound by several links; a write to it lands
  in every layer so bound and the composite read dedups the fact.
- `build().perform(env)` checks the shape: a link must name a layer
  already beneath the linking one, a snapshot cannot hold links, and
  the audience rule holds (a layer may link a layer beneath it only if
  the lower layer's audience contains its own, with `Process < Device
  < Peers`; a branch with an upstream is `Peers`, one without is
  `Device`, an ephemeral store is `Process`). Every check is a
  `StackError`.
- Identity is a pure function of shape. A layer's stack identity is
  `stack:<base58(blake3(dagcbor{address, links}))>` where `links` is
  the sorted `(name, id(to))` list and `address` is the layer's
  location (repository and branch name, repository and tree hash, or
  the ephemeral store's nonce entity). The bottom's identity is the
  same in every stack that holds it; renaming or re-linking changes
  only the layers above.
- Each layer holds only its own link facts.
- Links are facts held by the enclosing layer: `dialog.link/{from, to,
  name, revision}` on `link:<base58(blake3(dagcbor{from, to}))>`
  with `from` the encloser's address entity and `to` the enclosed
  layer's identity, plus the
  target's address (`dialog.link/repository` and `dialog.link/branch`,
  `dialog.link/tree`, or `dialog.link/ephemeral`). `revision` is the
  target's head as the encloser last saw it: written at build and
  refreshed by every stack commit on each layer above a layer that
  moved, so after a stack commit the top layer's head names the whole
  composite. A layer that must not capture another sits beside it
  instead of linking it. The
  `dialog.link/` prefix is carved out of the reserved-attribute gate
  like `dialog.attribute/`.
- Reads are pinned: `Stack::query()` reads every layer beneath the top
  at its staged tip or published head, through a new `Source::Pinned`
  (a branch handle read at a fixed revision, keeping its caches,
  remote fallback and session store), and the top live. `Stack::pull`
  pulls each upstream-tracking layer and captures the live heads;
  `Stack::push` pushes them bottom to top; `StackSubscription::poll`
  is a pure read, so an external commit lands as a delta on the first
  poll after a pull. Standalone- Writes stage: `Stack::transaction().commit()` induces once over the
  composite at the heads the stack reads at (`induce` takes the view
  separately from the dispatching layer, which stays the bottom
  branch: only the bottom's committed rules fire in a stack
  transaction, and an upper branch's own rules are a gap this
  increment leaves open), resolves placements from the bottom, routes
  each instruction to the layers its scope is linked under (an
  undeclared or default-scope attribute goes to the bottom; a name no
  link binds falls back to a tree binding on the bottom; a session
  binding or nothing is `UnboundScope`), then stages the layers bottom
  to top: a branch layer extends a per-layer `TransactionBatch` opened
  on its published head with a checkpoint at the version captured
  then; an ephemeral layer is written directly. `Stack::publish`
  publishes the chains bottom to top; `commit().publish()` is the
  one-shot form. A stack whose bottom is not a branch is read-only
  (`Detached`).
- `Ephemeral` has no public write API: it is written through a stack
  (or, for a layer's session store, through that layer's transaction).

Carries over unchanged from increment 1: composite subscriptions,
`Changes::cancel` and `Changes::subtract`, partition after induction,
and the routing tests.

Not yet built from the stack section: descriptor blobs in the
archive, `Stack::open` by hash, `named` and the registry metadata.

## Order of work

1. ~~Ephemeral layer.~~ Done: increment 2.
2. ~~Placement by scope entity plus the repository default fact, with
   the unbound-scope error.~~ Done: increment 3.
3. ~~`Stack` over `QueryLayer`: the builder with explicit `link`, the
   `build`-time checks, descriptor identities, `dialog.link/*` facts
   refreshed by stack commits, `transaction` with bottom-to-top
   commit.~~
   Done: increment 4. Still open from this item: descriptor blobs in
   the archive, `Stack::open` by hash, `named`, the registry
   metadata.
4. Tonk migration: `scope:state` and `scope:tab` declared in the
   library, one stack per connection with its own tab layer,
   inspector over the registry, `navigate` as a tab-scope
   conclusion.
5. Per-observer instant queues on the ephemeral layer, then
   `transient:` as sugar.
6. Channels: replicated ephemeral layers with a peer-offset log.

## Open questions

- **Per-tab stacks and rules that read across tabs.** A rule premised
  on "any tab's state" must read the join of every tab layer, which
  means induction on one stack's commit reads other stacks' layers.
  Either such rules are disallowed on tab scopes, or the registry is
  what induction joins. Decide when a rule needs it.
- **Who calls `stack.pull()`.** Movement lands only on pull, so
  something has to pull on a schedule or on a signal. If a memory
  cell ever notifies on change, that is the signal; the semantics do
  not change, only the latency.
- **Scope name convention.** `memory:shared`, `memory:local`,
  `memory:state`, `memory:tab` are used above as a convention only;
  the repository default fact and the placements are what bind them.
- **Snapshots in stacks.** A snapshot can be bound as the bottom of a
  read-only stack. Whether a transaction over such a stack should
  advance the snapshot the way `Snapshot::transaction` does today is
  unresolved.
