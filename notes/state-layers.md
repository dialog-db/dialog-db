# Stacks: lines, layers, and where an attribute's facts live

Status: proposal, superseding the fixed-layer model of the first
increment. What the increment built and what carries over is listed at
the end. Motivated by #483 and by tonk's overlay, transient, and
command workarounds.

## Vocabulary

- A **line** is anything with a head and a readable store: a `Branch`
  (durable, head in a cell, replicates to its upstreams), a `Snapshot`
  (durable, head by value), and, new, an **ephemeral line** (memory
  backed, head by value, no history, dies with the process). Lines are
  entities. Branch entities already exist; an ephemeral line mints one.
- A **layer** is a *role* in a stack: a named slot with declared
  properties. Layers are entities declared as facts on the top line,
  so every replica agrees on which layers exist and what each one is.
- A **stack** is an ordered binding of layers to lines. Top is the most
  stable line, bottom the most volatile. Each line captures the heads
  of the lines above it when it commits. A stack is built by API, per
  replica or per connection, and may be registered under a name so
  others can find it.
- A **placement** maps an attribute to a layer. It is a fact on the top
  line, replicated with the schema. Undeclared attributes belong to the
  top layer.

The split that makes this coherent: what is *shared* (which layers
exist, their properties, which attribute goes where) lives in the
replicated tree; what is *local* (which concrete line plays a role on
this replica, for this tab) is a binding made at stack construction.
A replica that meets a placement for a layer it has not bound
materializes the layer from its declared properties, so a name can
never dangle.

## Declarations, as facts on the top line

```
dialog.layer/name       of <layer>   is "session"
dialog.layer/durable    of <layer>   is false
dialog.layer/replicated of <layer>   is false
dialog.layer/above      of <layer>   is <layer>       # the next layer up; absent on the top
dialog.attribute/layer  of attribute:ui/selected is <layer>
```

The top layer is implicit: it is the line whose tree holds these facts,
and it needs no declaration. `above` gives a chain; loading checks it
is acyclic and total. A DAG (`captures`, cardinality many) is the
general form and can replace `above` later without changing anything
else. `replicated` is a boolean today and becomes an audience (a peer
set) when a layer needs to replicate to a subset; nothing in the
routing depends on which.

Notation, as concept-level sugar the analyzer lowers to the facts
above:

```yaml
layer!: &session
  durable: false

layer!: &local
  durable: true
  replicated: false
  above: *session      # local sits above session; shared is the implicit top

concept!: &site
  layer: *session
  with:
    path: { the: xyz.tonk.site/path, as: text }
```

## Building a stack

```rust
// Everything from declarations: every layer bound by default policy.
let stack = repo.branch("main").stack().open().perform(&env).await?;

// Explicit bindings override the defaults, e.g. one session line per
// tab. Unbound layers still materialize by policy.
let stack = repo
    .branch("main")
    .stack()
    .bind("session", repo.ephemeral(format!("tab:{id}")))
    .bind("local", repo.branch("main.local"))
    .open()
    .perform(&env)
    .await?;
```

Default policy per declared properties: `durable && replicated` is the
top line itself; `durable && !replicated` opens the branch
`<top>.<layer>` with no upstream; `!durable` creates a fresh ephemeral
line. `!durable && replicated` is unbacked until the log below exists
and fails `open` with a clear error rather than binding to something
else.

`open` verifies each bound line against its layer's properties, so a
durable branch cannot be bound to an ephemeral layer by mistake.

### Naming

```rust
let stack = stack.named("tab:123");   // registers; still a handle
```

Naming is optional. A named stack is registered in a process-local
registry on the `Repository`, and the registry is exposed at query
time as metadata facts, exactly the way `dialog.session/branch` lists
the lines in scope today:

```
dialog.stack/name   of <stack>   is "tab:123"
dialog.stack/layer  of <stack>   is <line entity>    # cardinality many, ordered
dialog.stack/top    of <stack>   is <line entity>
```

So an inspector enumerates stacks and their lines with an ordinary
query and joins their session lines with the composite subscription
that already exists. Dropping the handle unregisters it; an anonymous
stack is just never listed. Nothing durable is written by naming.

## Using a stack

```rust
stack.select(query).perform(&env)          // composite read, all lines
stack.subscribe(query)                     // composite subscription, pins every line
stack.transaction().assert(doc).commit().perform(&env)   // routes by placement
stack.layer("session")                     // the bound line, for direct access
stack.revision()                           // the stack revision: see below
```

`Stack` is what `QueryLayer` already is with two additions: ordered
lines with roles, and a transaction. The composite subscription built
in the first increment carries over unchanged; its vector of pins *is*
the stack revision.

### Transactions and capture

A stack transaction accumulates instructions as today. At commit,
after induction has settled the batch against the composite view, the
batch is partitioned by each attribute's layer, and the lines commit
**top to bottom**. Each lower line's revision records the heads of the
lines above it as they stand after their own commits:

```
Revision { tree, edition, context, captures: Vec<(line entity, Revision)> }
```

Only lines below the top pay this, and a capture is one hash per line
above, so a session line that commits on every click records the
shared and local heads it saw and nothing more. Consequences:

- **Consistency without atomicity.** A reader of the session line
  knows which shared revision that state was computed against, and a
  handler acting on a session instant reads shared state *at that
  revision* rather than at "now".
- **One identity.** The bottom line's revision transitively names the
  whole composite. `stack.revision()` is that.
- **Stale derivation is detectable.** A local durable layer that
  captured shared head `h` and now sees the shared head at `h'` knows
  it is behind, which is the induction watermark generalized to a pair
  of lines.

The ordering rule follows from lifetimes: a reference may point only
from a shorter-lived line to a longer-lived one. The top never
references anything below it.

A revision on an ephemeral line is an identity, not a persistence
claim: a hash of its state plus its captures, with no parent chain
retained. That is enough for subscription pins, diffs, and captures.

### Retracts on a non-top layer

A retract of a fact on layer L removes it from L's line. If L's line
never held it, the retract is a tombstone over the lines above, which
is the only way a fact can appear under an attribute placed on L: a
peer without the placement wrote it to the top. That situation is a
placement divergence and should surface as a warning on pull, not be
silently masked. This is the one shadowing case the model admits and
it is diagnosable because placement is in the tree.

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

For a **local ephemeral line**, do not keep a shared log. Fan each
instant out at write time into per-observer queues, filtered by each
observer's demand. An instant nobody demanded costs nothing; memory is
the sum of unconsumed matched instants across observers, which is what
any event system pays. A queue has a ring bound; an observer that
falls off the ring gets a gap marker and recomputes from the fold.
"All have seen it" is not a question here: each observer owns its
queue and drains it.

For a **replicated ephemeral line**, which is where you found the log
compelling, the log is the right shape and the peers are the
observers. Each peer holds an offset into the line's log; sync is
"send me instants past my offset"; retention is the minimum peer
offset with a ring bound, and a peer that falls off resyncs from the
fold. That is how presence and awareness protocols already work, with
a per-peer clock standing in for the offset. It is also what makes
that layer's `replicated` property meaningful without a tree: there is
nothing to push except the log.

With either mechanism in place, `transient:` becomes sugar: an
attribute placed on the session layer plus a sweep rule `retract! C
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

Replaced: the `Layer` enum becomes a layer entity with declared
properties; `Placement` targets a layer entity; the `Overlay` becomes
an ephemeral line, a real store with a head, cardinality, and a diff,
so subscriptions maintain incrementally from it instead of recomputing
on an epoch; `Transaction` gains the stack fan-out and capture.

## Order of work

1. Ephemeral line: a memory-backed `Source` with a head by value, a
   cover-scoped diff, and no history. Replace `Overlay` with it.
   Subscriptions become incremental over session changes for free.
2. Layer entities and placement by entity, with the default
   materialization policy and the `open`-time property check.
3. `Stack` over `QueryLayer`: ordered roles, `bind`, `named`, the
   registry metadata, `transaction` with top-to-bottom commit and
   captures on lower revisions.
4. Tonk migration: session layer declared in the library, one stack
   per connection bound to a per-tab ephemeral line, inspector over
   the registry, `navigate` as a session-layer conclusion.
5. Per-observer instant queues on the ephemeral line, then
   `transient:` as sugar.
6. Replicated ephemeral lines with a peer-offset log.

## Open questions

- **Per-tab stacks and rules that read across tabs.** A rule premised
  on "any tab's state" must read the join of every session line, which
  means induction on one stack's commit reads other stacks' lines.
  Either such rules are disallowed on session layers, or the registry
  is what induction joins. Decide when a rule needs it.
- **Capture on the top line's pull.** A pull moves the top head without
  any lower line committing, so lower captures are briefly behind.
  That is the stale-derivation signal working as intended, but the
  first read after a pull should probably re-capture eagerly.
- **Snapshots in stacks.** A snapshot can be bound as the top of a
  read-only stack. Whether a transaction over such a stack should
  advance the snapshot the way `Snapshot::transaction` does today is
  unresolved.
