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
- A **layer** is a line placed in a stack under an optional name. The
  name is an entity; it is what placements refer to. The one unnamed
  layer receives every attribute with no placement.
- A **stack** is an ordered list of layers, **bottom first**. The
  bottom is the slowest and widest line, the top the fastest and
  narrowest. An upper layer captures the heads of the layers beneath
  it when it commits, subject to the audience rule below. A stack is
  built by API, per replica or per connection, and may be registered
  under a name so others can find it.
- A **placement** maps an attribute to a layer name. It is a fact on
  the bottom line, replicated with the schema. Undeclared attributes
  belong to the unnamed layer.

The split that makes this coherent: what is *shared* (which layer
names exist, which attribute goes where) lives in the replicated tree;
what is *local* (which concrete line stands under a name on this
replica, for this tab) is a binding made at stack construction.

## Declarations, as facts on the bottom line

```
dialog.attribute/layer  of attribute:ui/selected  is layer:tab
```

That is the only replicated declaration. Layer names are entities;
their properties (durable, audience) are properties of the *line*
bound under the name, known to the builder, not facts. A replica that
meets a placement naming a layer its stack does not bind fails the
write with a clear error rather than routing elsewhere; the fix is in
the stack, which is local, not in the schema.

Notation, as concept-level sugar the analyzer lowers to the fact
above:

```yaml
concept!: &site
  layer: layer:tab
  with:
    path: { the: xyz.tonk.site/path, as: text }
```

## Building a stack

```rust
let shared = repo.branch("main").open().perform(&env).await?;
let local = repo.branch("main.local").open().perform(&env).await?;

let stack = Stack::new()
    .layer(None, shared)                                    // durable, peers
    .layer(Some("layer:local".parse()?), local)             // durable, this device
    .layer(Some("layer:gossip".parse()?), Ephemeral::channel(&shared)) // ephemeral, peers
    .layer(Some("layer:state".parse()?), Ephemeral::new())  // ephemeral, this process
    .layer(Some("layer:tab".parse()?), Ephemeral::new())    // ephemeral, this tab
    .build()
    .perform(&env)
    .await?;
```

`build` checks three things:

- **Exactly one unnamed layer.** None means undeclared attributes have
  nowhere to go; more than one means they fan out silently, which is
  the one place fan-out must never be implicit.
- **Order is a dependency order.** Every layer is listed after every
  layer it may capture.
- **The audience rule.** A layer captures a lower layer only if that
  layer's audience contains its own. A capture is a revision hash; if
  gossip captured local, every gossip instant a peer received would
  reference a head the peer cannot resolve. So gossip (audience: the
  peers) captures shared and not local (audience: this device); state
  (audience: this process) captures shared, local, and gossip; tab
  captures state. The list above therefore builds this shape, with
  local and gossip as siblings, without anyone drawing it:

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
branch it is built from. Two layers with the same audience may
capture in list order, which is how state and tab, both process-local
in kind, are still ordered.

### Several layers under one name

A name may be bound more than once. A write to that name lands in
every line bound to it, and a retract removes from every one. This is
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
dialog.stack/layer   of <stack>   is <line entity>    # cardinality many, ordered bottom first
dialog.stack/bottom  of <stack>   is <line entity>
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
stack.layer(&"layer:tab".parse()?)         // the bound line(s), for direct access
stack.revision()                           // the stack revision: see below
```

`Stack` is what `QueryLayer` already is with two additions: ordered
named lines, and a transaction. The composite subscription built in
the first increment carries over unchanged; its vector of pins *is*
the stack revision.

### Transactions and capture

A stack transaction accumulates instructions as today. At commit,
after induction has settled the batch against the composite view, the
batch is partitioned by each attribute's layer, and the lines commit
**bottom to top**. Each upper line's revision records the heads of
the lines it captures, as they stand after their own commits:

```
Revision { tree, edition, context, captures: Vec<(line entity, Revision)> }
```

Only lines above the bottom pay this, and a capture is one hash per
captured line, so a tab line that commits on every click records the
state head it saw and nothing more. Consequences:

- **Consistency without atomicity.** A reader of the tab line knows,
  transitively, which shared revision that state was computed
  against, and a handler acting on a tab instant reads shared state
  *at that revision* rather than at "now".
- **One identity.** The top line's revision transitively names the
  whole composite. `stack.revision()` is that. With siblings, the
  first line that names both local and gossip is state; the top
  always names everything.
- **Stale derivation is detectable.** A local layer that captured
  shared head `h` and now sees the shared head at `h'` knows it is
  behind, which is the induction watermark generalized to a pair of
  lines.

A revision on an ephemeral line is an identity, not a persistence
claim: a hash of its state plus its captures, with no parent chain
retained. That is enough for subscription pins, diffs, and captures.

### Retracts on a named layer

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

## What the first increment built and what carries over

Built on the branch: a fixed `Layer` enum, `Placement` as a fact on the
branch, partition-after-induction routing with the tree as the top and
the overlay as the only other backed layer, and composite
subscriptions over `QueryLayer` with a pin per line.

Carries over unchanged: composite subscriptions (they are
`stack.subscribe`), `Changes::cancel` and `Changes::subtract`, the
`dialog.attribute/` carve-out in the write gate, partition after
induction, the tests for routing and for rule heads concluding into a
non-top layer.

Replaced: the `Layer` enum becomes a layer name (an entity) bound in
a stack; `Placement` targets that entity; the `Overlay` becomes an
ephemeral line, a real store with a head, cardinality, and a diff, so
subscriptions maintain incrementally from it instead of recomputing
on an epoch; `Transaction` gains the stack fan-out and capture.

## Order of work

1. Ephemeral line: a memory-backed `Source` with a head by value, a
   cover-scoped diff, and no history. Replace `Overlay` with it.
   Subscriptions become incremental over session changes for free.
2. Placement by layer entity, with the unbound-layer error.
3. `Stack` over `QueryLayer`: the bottom-first builder, the
   `build`-time checks (one unnamed layer, dependency order, the
   audience rule), `named`, the registry metadata, `transaction`
   with bottom-to-top commit and captures on upper revisions.
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
- **Capture on the bottom line's pull.** A pull moves the shared head
  without any upper line committing, so upper captures are briefly
  behind. That is the stale-derivation signal working as intended,
  but the first read after a pull should probably re-capture eagerly.
- **Layer entity syntax.** `layer:local` is used above because a
  bare-scheme URI such as `local:` may not pass the canonical parse;
  check before settling the convention.
- **Snapshots in stacks.** A snapshot can be bound as the bottom of a
  read-only stack. Whether a transaction over such a stack should
  advance the snapshot the way `Snapshot::transaction` does today is
  unresolved.
