# Stacks: lines, layers, and where an attribute's facts live

Status: proposal, superseding the fixed-layer model of the first
increment. What the increment built and what carries over is listed at
the end. Motivated by #483 and by tonk's overlay, transient, and
command workarounds.

## Vocabulary

- A **line** is anything with a head and a readable store: a `Branch`
  (durable, head in a cell, replicates to its upstreams), a `Snapshot`
  (durable, head by value), and, new, an **ephemeral line** (memory
  backed, head by value, no history, dies with the process; a
  *channel* is an ephemeral line that replicates to a branch's peers
  through the log described below). Lines are entities.
- A **link** is a fact held by one line naming another line beneath
  it: the enclosing line captures the enclosed line's head whenever it
  commits. Links carry a **name**, an entity; several links from one
  line may share a name.
- A **layer** is a name: the set of lines a stack's links bind under
  it. It is what placements refer to.
- A **stack** is a line together with everything reachable through
  its links. The bottom is the slowest and widest line, the top the
  fastest and narrowest. A stack is built by API, per replica or per
  connection, and may be registered under a name so others can find
  it.
- A **placement** maps an attribute to a layer name. It is a fact on
  the bottom line, replicated with the schema. A repository-wide fact
  names the layer an attribute without a placement belongs to.

The split that makes this coherent: what is *shared* (which layer
names exist, which attribute goes where) lives in the replicated tree;
what is *local* (which concrete line stands under a name on this
replica, for this tab) is a binding made at stack construction.

## Declarations, as facts on the bottom line

```
<repository did>      dialog.attribute/default  memory:shared   # the implicit layer
attribute:ui/selected dialog.attribute/layer    memory:tab      # an override
```

Those are the only replicated declarations. Layer names are plain
entities under a `memory:` convention; nothing about them is fixed.
Their properties (durable, audience) are properties of the *lines*
bound under the name, known to the builder, not facts. A replica that
meets a placement naming a layer its stack does not bind fails the
write with a clear error rather than routing elsewhere; the fix is in
the stack, which is local, not in the schema.

Notation, as concept-level sugar the analyzer lowers to the fact
above:

```yaml
concept!: &site
  layer: memory:tab
  with:
    path: { the: xyz.tonk.site/path, as: text }
```

## Topology as facts

A stack's shape is data, not builder state. Each enclosing line holds
one **link** per line it encloses, in its own tree, written as
machinery in the same commit that moves its head:

```
<link> dialog.link/from      <enclosing line>
<link> dialog.link/to        <enclosed line>
<link> dialog.link/name      memory:shared        # the layer this link binds under
<link> dialog.link/revision  <revision of the enclosed line, as last seen>
```

plus the enclosed line's address (its repository DID and branch name,
or its ephemeral kind), in the `dialog.branch/*` vocabulary the
session metadata already uses at query time. The link is its own
entity rather than a fact on the enclosed line because a composite
read unions every tree: local and gossip both link shared, at
different revisions, and only a link entity keeps the two apart.

**Wiring lifts by copy.** An enclosing line also holds, verbatim, every
link fact each line it links holds: the same link entities, with
`from` still naming the line that made the link. So the top line
carries the whole stack's wiring, every edge is queryable from it
alone, and an opener needs no recursion to learn the shape. The copy
is refreshed with the original: commits go bottom to top, so a line
copies what its enclosed lines hold *after* the same stack commit,
and every captured revision of a given line agrees across the DAG,
both arms of a diamond included. Lifted links are facts, not
descriptor entries: the descriptor stays the direct shape, and
`id(local)` already covers `id(shared)`.

### Two identities per line, and the descriptor blob

A line has an **address**, where its head lives: repository DID plus
branch name for a branch, a nonce for an ephemeral line. A branch
entity today is derived from the repository DID, the profile, and the
name, which differs per profile for the same branch, so it is not
usable as a link target; the address is.

A line also has a **stack identity**: the hash of its **descriptor**,
a canonical dag-cbor blob in the archive, exactly the way a rule or a
concept descriptor is content-addressed and hydrated on read:

```
descriptor(line) = { address(line), links: sorted [(name, id(to))] }
id(line)         = stack:<base58(blake3(descriptor))>
```

The bottom line, linking nothing, has the descriptor `{address}` and
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
  descriptor, opens each line by its address, and recurses. A stack
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
  layer, changes `id(local)`, so every link *to* local is stale and
  must be rewritten. That is ordinary Merkle behaviour; it is free for
  ephemeral lines, which are rebuilt per process, and it is a
  migration for any durable line above a re-shaped durable line. In
  the layout below nothing durable sits above local, so it costs
  nothing today, and it is the constraint to remember when a second
  durable layer is added.

Why facts rather than a field on the revision: a rule can premise on
them. The enclosed line's *current* head is already readable as
`dialog.branch/revision` metadata, so "local's link to shared is
behind shared's head" is an ordinary two-premise body, and stale
derivation is a rule's concern, not engine code. The audience rule
(below) guarantees a link is resolvable wherever it is readable,
because a line only ever links lines beneath it.

### Refresh is eager, and topology decides what is captured

A link's `revision` records the head of the enclosed line as the
encloser last saw it, and a stack commit refreshes it on every line
above a line that moved, bottom to top, in the same commit. After a
stack commit the top line's head therefore transitively names the
head of every line beneath it: the one hash for the composite. A
link whose target did not move is a no-op refresh and mints nothing,
so the refresh reaches exactly the lines above the movement.

The cost is bounded by the audience rule: a durable line may only
link durable lines, so ephemeral churn never forces a durable commit,
and the only thing that ripples into a branch is another branch
moving. Where even that is unwanted, the topology is the knob, not
the refresh policy. Linking is capturing, so a line that should not
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
commit to one line, is not seen until the next stack commit; the top
hash is then stale, not wrong. Capturing that is the open question
on pulls below.

### Recovery

`Stack::open` takes a line or a stack identity and walks descriptors
downward, each fetch verifying its hash (a visited set only spares
re-walking a diamond). The durable part of a tab stack recovers from
`main.local`; the ephemeral layers above it are rebuilt by the
process, as they would be anyway. If that rebuild should be
data-driven too, a durable line may hold template facts pointing up,
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
    .line(shared)                                    // memory:shared by the repository default
    .line(local).link(&shared, "memory:shared")
    .line(gossip).link(&shared, "memory:shared")
    .line(state).link(&local, "memory:local").link(&gossip, "memory:gossip")
    .line(tab).link(&state, "memory:state")
    .build()
    .perform(&env)
    .await?;
```

`link` declares a link from the line just added to a line beneath it,
under a name; `build` validates and asserts the links, and every later
commit of an enclosing line refreshes its links' revisions. Enclosure
is explicit rather than derived from list order: the derivation would
produce the same graph here, but a rule that needs explaining must
not be the only way to say it. A flat topology, every line linked
straight from the top, is expressible and is a bad idea for the same
reason a deep one is good: the top is the tip that names everything
beneath it, and a flat stack makes the top pay every capture.

`build` checks:

- **Every name a placement can target is bound**, including the
  repository default. A write naming a layer the stack does not bind
  fails with a clear error. A catch-all layer is a possible flag; it
  is deliberately not the default, since it turns a schema and stack
  mismatch into silent misplacement.
- **Descriptors resolve.** Cycles cannot be built (see the descriptor
  blob); a blob that does not hash to its claimed identity is a
  configuration error.
- **The audience rule.** A line may link a line beneath it only if
  that line's audience contains its own. A capture is a revision
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

Audience is a property of the line: a branch's is its peers, an
`Ephemeral::new()` is this process, a channel is the peers of the
branch it is built from.

### Several links under one name

A name may be bound by more than one link from the same line. A write
to that name lands in every line so bound, and a retract removes from
every one. This is
cheap in the model and allowed, with two consequences to keep in view:
the composite read must dedup on entity, attribute, and value across
same-named lines, since tree facts carry a per-line cause; and a fact
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
the lines in scope today:

```
dialog.stack/name    of <stack>   is "tab:123"
dialog.stack/top     of <stack>   is <line entity>    # walk its links for the rest
```

So an inspector enumerates stacks and their lines with an ordinary
query and joins their tab lines with the composite subscription that
already exists. Dropping the handle unregisters it; an anonymous
stack is just never listed. Nothing durable is written by naming.

## Using a stack

```rust
stack.select(query).perform(&env)          // composite read, all lines
stack.subscribe(query)                     // composite subscription, pins every line
stack.transaction().assert(doc).commit().perform(&env)   // routes by placement
stack.layer(&"memory:tab".parse()?)        // the bound line(s), for direct access
stack.revision()                           // the stack revision: see below
```

`Stack` is what `QueryLayer` already is with two additions: lines
reached through named links, and a transaction. The composite subscription built in
the first increment carries over unchanged; its vector of pins *is*
the stack revision.

### Transactions and capture

A stack transaction accumulates instructions as today. At commit,
the stack first advances to the live heads (induction reads what the
write builds on), settles the batch against that view, partitions it
by each attribute's layer, and commits the lines **bottom to top**.
Each line's commit folds in its wiring at the heads as they stand
after the lines beneath it committed. A line with nothing of its own
to write and wiring that already names the current heads is a no-op
and keeps its head, so the refresh reaches exactly the lines above a
line that moved. Consequences:

- **Reads are pinned.** A stack is read at its top's head: every line
  beneath the top is read at the revision the wiring captured, not
  at its live head, so what a read sees is exactly what the top's
  hash names. A branch is read pinned through its own handle (caches,
  remote fallback, session store intact), not through a snapshot.
- **One identity.** After a stack commit the top line's head
  transitively names the head of every line beneath it. With
  siblings, state's wiring names both local and shared; the top
  always names everything.
- **External movement is an instant.** A pull on the bottom or a
  direct commit to one line moves a live head the stack has not
  captured. The stack is then behind, not wrong: `heads()` differs
  from `captured()`, and `advance` (a stack commit with nothing to
  write) captures it. Every stack commit advances, and a stack
  subscription advances on each poll, so the external change lands
  as that poll's delta rather than leaking in beneath the hash.
- **Stale derivation is a rule.** A line whose link revision differs
  from the enclosed line's current head is behind, and a rule can say
  so, which is the induction watermark generalized to a pair of lines.

A revision on an ephemeral line is an identity, not a persistence
claim: a hash of its state plus its links, with no parent chain
retained. That is enough for subscription pins, diffs, and captures.

### Retracts on a linked layer

A retract of a fact on layer L removes it from L's line. If L's line
never held it, the retract is a tombstone over the lines below, which
is the only way a fact can appear under an attribute placed on L: a
peer without the placement wrote it to the bottom. That situation is
a placement divergence and should surface as a warning on pull, not
be silently masked. This is the one shadowing case the model admits
and it is diagnosable because placement is in the tree.

## Instants, queues, and the log

An **instant** is one induction round. A commit is a sequence of
instants; durable lines fold them into one revision because storing
intermediates costs storage. The two things folding loses are the
intermediate rounds and any fact asserted and retracted within one
commit, which is exactly what a rule-concluded transient is.

An **observer** is anything that wants to see instants rather than
folds: a state subscription (which needs the touched facts since its
last poll to maintain incrementally) or an event handler (which needs
every instant, including the ones that folded away). Observers
register with a line when they are created and unregister when they
are dropped.

For a **local ephemeral line** (state, tab), do not keep a shared log. Fan each
instant out at write time into per-observer queues, filtered by each
observer's demand. An instant nobody demanded costs nothing; memory is
the sum of unconsumed matched instants across observers, which is what
any event system pays. A queue has a ring bound; an observer that
falls off the ring gets a gap marker and recomputes from the fold.
"All have seen it" is not a question here: each observer owns its
queue and drains it.

For a **channel** (a replicated ephemeral line such as gossip), the
log is the right shape and the peers are the observers. Each peer holds an offset into the line's log; sync is
"send me instants past my offset"; retention is the minimum peer
offset with a ring bound, and a peer that falls off resyncs from the
fold. That is how presence and awareness protocols already work, with
a per-peer clock standing in for the offset. It is also what makes
that layer's `replicated` property meaningful without a tree: there is
nothing to push except the log.

With either mechanism in place, `transient:` becomes sugar: an
attribute placed on an ephemeral layer plus a sweep rule `retract! C
when C`, and the engine's transient bucket becomes an optimization it
may apply when no observer demands the attribute.

## What is built

**Increment 1** (fixed layers, composite subscriptions): a `Layer`
enum, `Placement` as a fact on the branch, partition-after-induction
routing with the tree as the top and the session store as the only
other backed layer, and composite subscriptions over `QueryLayer`
with a pin per line.

**Increment 2** (the ephemeral line, `repository/ephemeral.rs`): the
session overlay is replaced by a real memory-backed store,
`Ephemeral`, which every branch and snapshot carries:

- Facts are held under the tree's own three index keys (entity,
  attribute, and value orders) in one ordered map, so a selector's
  `selector_range` applies unchanged and rows stream in exactly the
  order a tree scan produces them. The query layer's k-way merge
  interleaves the store with tree scans with no special case.
- Writes have the tree's semantics: idempotent assert, cardinality-one
  replace that supersedes the cell, exact retract. A retract of a fact
  the store does not hold is a tombstone that hides it in the lines
  beneath, so session shadowing of committed facts still works and
  the store's own facts are never shadowed by it.
- Every visible change mints an `Instant` (asserted facts, retracted
  facts, sequence, chained hash) into a bounded ring. A subscription
  pins the sequence and reads the exact delta since its pin, filtered
  by its demand cover, so a session write inside the cover is
  maintained per touched entity and one outside the cover advances
  the pin for free. Only a pin older than the ring recomputes. The
  chained hash costs the delta, never the store, which is the identity
  an ephemeral line needs and the trade the design accepted.
- The store is read live by every read path: `QueryEnv` unions each
  line's store stream and lifts its tombstones itself, so `QueryLayer`
  no longer snapshots session facts at construction and the
  subscription's subtract workaround is gone. Rules asserted into the
  store resolve as their own layer, read fresh.

**Increment 3** (placement by layer entity, `placement.rs`): the
fixed `Layer` enum is gone.

- A layer is an entity, conventionally `memory:<name>`, and nothing
  about the name is fixed. `dialog.attribute/layer` of an attribute
  entity is a layer entity; `dialog.attribute/default` of the
  repository DID names the layer an attribute with no placement
  belongs to, which is the tree's name. With no default declared,
  undeclared attributes reach the tree as before. A declaration
  whose value is not an entity fails the commit
  (`InvalidPlacement`).
- A line carries local **bindings** from layer entities to its stores
  (`Branch::bind(layer, Target::Tree | Target::Session)`, shared
  across clones like the caches; a snapshot minted from a branch
  shares the branch's). The tree needs no binding: the default names
  it. A write naming a layer the line does not bind fails the commit
  (`UnboundLayer`), and binding it afterwards makes the same write
  succeed. This is the seam the stack builder drives next: a stack's
  links become bindings to other lines.
- Routing is unchanged in shape: after induction the settled batch
  is partitioned by each attribute's layer, resolved through the
  declarations and the bindings, into the tree commit and the
  ephemeral store.

**Increment 4** (the stack, `stack.rs`): lines linked under layer
names, read as one composite and written by placement.

- A `Line` is a branch, a snapshot, or an `Ephemeral` store; a stack
  is built bottom first with `Stack::builder().line(a).line(b).link(&a,
  name)`. `link` binds a name to the line it points at, so
  `local.link(&shared, "memory:shared")` routes `memory:shared` to
  `shared`. A name may be bound by several links; a write to it lands
  in every line so bound and the composite read dedups the fact.
- `build().perform(env)` checks the shape: a link must name a line
  already beneath the linking one, a snapshot cannot hold links, and
  the audience rule holds (a line may link a line beneath it only if
  the lower line's audience contains its own, with `Process < Device
  < Peers`; a branch with an upstream is `Peers`, one without is
  `Device`, an ephemeral store is `Process`). Every check is a
  `StackError`.
- Identity is a pure function of shape. A line's stack identity is
  `stack:<base58(blake3(dagcbor{address, links}))>` where `links` is
  the sorted `(name, id(to))` list and `address` is the line's
  location (repository and branch name, repository and tree hash, or
  the ephemeral store's nonce entity). The bottom's identity is the
  same in every stack that holds it; renaming or re-linking changes
  only the lines above.
- Wiring lifts by copy: each line also holds every link fact its
  linked lines hold, verbatim, refreshed in the same bottom-to-top
  commit. The top holds the whole stack's wiring.
- Links are facts held by the enclosing line: `dialog.link/{from, to,
  name, revision}` on `link:<base58(blake3(dagcbor{from, to}))>`
  with `from` the encloser's address entity and `to` the enclosed
  line's identity, plus the
  target's address (`dialog.link/repository` and `dialog.link/branch`,
  `dialog.link/tree`, or `dialog.link/ephemeral`). `revision` is the
  target's head as the encloser last saw it: written at build and
  refreshed by every stack commit on each line above a line that
  moved, so after a stack commit the top line's head names the whole
  composite. A line that must not capture another sits beside it
  instead of linking it. The
  `dialog.link/` prefix is carved out of the reserved-attribute gate
  like `dialog.attribute/`.
- Reads are pinned: `Stack::query()` reads every line beneath the top
  at its captured head, through a new `Source::Pinned` (a branch
  handle read at a fixed revision, keeping its caches, remote
  fallback and session store), and the top live. `Stack::advance`
  captures the live heads; `StackSubscription::poll` advances first,
  so an external commit lands as a delta. Standalone
  ephemeral lines are first-class in the query layer now (`join` a
  `&Ephemeral`), the query env unions their streams and tombstones
  and resolves their rules, and a subscription pins each one's
  sequence beside the tree pins, maintaining from the instant ring
  exactly as it does for a line's session store.
- Writes: `Stack::transaction()` induces once over the whole
  composite (`induce` now takes the view separately from the
  dispatching line, which stays the bottom branch: only the bottom's
  committed rules fire in a stack transaction, and an upper branch's
  own rules are a gap this increment leaves open), resolves
  placements from the bottom, routes each instruction to the lines
  its layer is linked under (an undeclared or default-layer attribute
  goes to the bottom; a name no link binds falls back to the bottom's
  own bindings, so a one-line stack routes exactly as the branch
  would; anything else is `UnboundLayer`), then commits bottom to
  top through `commit_settled`, the settled half of a transaction
  commit, refreshing each committing line's links. A stack whose
  bottom is not a branch is read-only (`Detached`).

Carries over unchanged from increment 1: composite subscriptions,
`Changes::cancel` and `Changes::subtract`, partition after induction,
and the routing tests.

Not yet built from the stack section: descriptor blobs in the
archive, `Stack::open` by hash, `named` and the registry metadata.

## Order of work

1. ~~Ephemeral line.~~ Done: increment 2.
2. ~~Placement by layer entity plus the repository default fact, with
   the unbound-layer error.~~ Done: increment 3.
3. ~~`Stack` over `QueryLayer`: the builder with explicit `link`, the
   `build`-time checks, descriptor identities, `dialog.link/*` facts
   refreshed by stack commits, `transaction` with bottom-to-top
   commit.~~
   Done: increment 4. Still open from this item: descriptor blobs in
   the archive, `Stack::open` by hash, `named`, the registry
   metadata.
4. Tonk migration: `layer:state` and `layer:tab` declared in the
   library, one stack per connection with its own tab line,
   inspector over the registry, `navigate` as a tab-layer
   conclusion.
5. Per-observer instant queues on the ephemeral line, then
   `transient:` as sugar.
6. Channels: replicated ephemeral lines with a peer-offset log.

## Open questions

- **Per-tab stacks and rules that read across tabs.** A rule premised
  on "any tab's state" must read the join of every tab line, which
  means induction on one stack's commit reads other stacks' lines.
  Either such rules are disallowed on tab layers, or the registry is
  what induction joins. Decide when a rule needs it.
- **Push-based advance.** A pull is captured on the next stack commit
  or subscription poll. If a memory cell ever notifies on change, the
  stack can advance on the notification instead of on poll; the
  semantics do not change, only the latency.
- **Layer name convention.** `memory:shared`, `memory:local`,
  `memory:state`, `memory:tab` are used above as a convention only;
  the repository default fact and the placements are what bind them.
- **Snapshots in stacks.** A snapshot can be bound as the bottom of a
  read-only stack. Whether a transaction over such a stack should
  advance the snapshot the way `Snapshot::transaction` does today is
  unresolved.
