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

### Two identities per line

A line has an **address**, where its head lives: repository DID plus
branch name for a branch, a nonce for an ephemeral line. A branch
entity today is derived from the repository DID, the profile, and the
name, which differs per profile for the same branch, so it is not
usable as a link target; the address is.

A line also has a **stack identity**, derived from its address and
the identities of the lines it links:

```
id(line) = blake3({ address(line), links: sorted [(name, id(to))] })
```

The bottom line, linking nothing, has `id = blake3({address})`. A
link's `to` is the enclosed line's stack identity; `from` is the
enclosing line's *address*, since the link is part of what defines
the encloser's identity and would otherwise be self-referential. The
link entity is `link:blake3({address(from), id(to)})`, so re-asserting
a link is idempotent and `revision` is a clean cardinality-one
replace.

Consequences of merkelizing the stack this way:

- **Cycles are unconstructible.** `id(A)` needs `id(B)`, which would
  need `id(A)`. The ladder (mutual one-rung-stale links) is gone with
  them, since it needs both directions.
- **Fabricated topology fails verification.** `open` recomputes each
  line's identity from its links as it walks and rejects a mismatch,
  so no cycle search is needed.
- **`to` is audience-independent.** A stack identity is a pure
  function of addresses and structure, so every reader anywhere
  computes the same one.
- **Topology is part of identity.** Adding a link under local changes
  `id(local)`, so every link *to* local is stale and must be
  rewritten. That is ordinary Merkle behaviour; it is free for
  ephemeral lines, which are rebuilt per process, and it is a
  migration for any durable line above a re-linked durable line.
  In the layout below nothing durable sits above local, so it costs
  nothing today, and it is the constraint to remember when a second
  durable layer is added.

Why facts rather than a field on the revision: a rule can premise on
them. The enclosed line's *current* head is already readable as
`dialog.branch/revision` metadata, so "local's link to shared is
behind shared's head" is an ordinary two-premise body, and stale
derivation is a rule's concern, not engine code. The audience rule
(below) guarantees a link is resolvable wherever it is readable,
because a line only ever links lines beneath it.

### Refresh is lazy

A link's `revision` records what the enclosing line saw when it last
committed, and only that line's own commits refresh it. Nothing
propagates eagerly. This is a deliberate choice against the
alternative, where a tip moving commits every line that links to it:

- an eager refresh is itself a commit, so it would cascade up every
  path, and a diamond (state over local and gossip, both over shared) would
  refresh state twice per shared commit unless propagation were
  batched in topological order;
- a refresh on a durable line is a durable commit, so every shared
  commit would cost a local commit.

Lazy refresh costs nothing, and staleness stays fully detectable at
query time through the metadata head. What "the top names the whole
composite" then means, precisely: the top's revision transitively
names the heads each layer *saw when it last committed*, which is the
consistent snapshot a handler acting on a tab instant wants. A
subscription still pins actual current heads; the two are different
questions and both stay answerable.

### Recovery

`Stack::open(&line)` walks links downward from any line, verifying
each identity as it goes (a visited set only spares re-walking a
diamond). The durable part of a tab stack recovers from
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

let stack = Stack::new()
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
- **Identities verify.** Cycles cannot be built (see Two identities
  per line); a mismatched identity is a configuration error.
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
after induction has settled the batch against the composite view, the
batch is partitioned by each attribute's layer, and the lines commit
**bottom to top**, in a topological order of the links touched. Each
enclosing line's commit folds in a replace of `dialog.link/revision`
on each of its links, naming the enclosed heads as they stand after
their own commits. Only enclosing lines pay this: one small replace
per link per commit, recorded in that line's history, and only when
that line commits for its own reasons. Consequences:

- **Consistency without atomicity.** A reader of the tab line knows,
  transitively, which shared revision that state was computed
  against, and a handler acting on a tab instant reads shared state
  *at that revision* rather than at "now".
- **One identity.** The top line's revision transitively names the
  heads each layer saw when it last committed. `stack.revision()` is
  that. With siblings, the first line whose links name both local and
  gossip is state; the top always names everything.
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
2. Placement by layer entity plus the repository default fact, with
   the unbound-layer error.
3. `Stack` over `QueryLayer`: links as `dialog.link/*` facts with
   addresses and merkelized stack identities, the builder with
   explicit `link`, the `build`-time checks (every targetable name
   bound, identities verify, the audience rule), `Stack::open` by
   walking and verifying links, `named`, the registry metadata,
   `transaction` with bottom-to-top commit and lazy link refresh.
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
- **Layer name convention.** `memory:shared`, `memory:local`,
  `memory:state`, `memory:tab` are used above as a convention only;
  the repository default fact and the placements are what bind them.
- **Snapshots in stacks.** A snapshot can be bound as the bottom of a
  read-only stack. Whether a transaction over such a stack should
  advance the snapshot the way `Snapshot::transaction` does today is
  unresolved.
