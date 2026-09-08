# State layers: an attribute declares where its facts live

Status: first increment implemented (`placement.rs`, composite
subscriptions). Motivated by #483 (rule-concluded transients are
unobservable after the commit-time rollup) and by the pattern of
workarounds it produced in tonk: the `navigate` postMessage side
channel, the `CreateNotebookHandler` write-then-read-back dance, scoped
overlay clears, and the `Provider<C>` command registry that re-derives
what induction already computed.

## The problem, restated

Dialog had three places a fact could live and three verbs for putting
it there, and the verb was chosen at every call site:

| place | verb | observable by subscribers | survives restart | replicates |
|---|---|---|---|---|
| branch tree | `tx.assert` | yes, as settled state | yes | yes |
| transient bucket | `tx.dispatch` | no | no | no |
| session overlay | `branch.overlay().assert` | yes, as settled state | no | no |

Readers never chose: `QueryLayer::from(SourceRef)` folds the overlay
into every read, and the induction round view layers transients on top.
Writers chose every time. That asymmetry was the incoherence: the
schema knew what a fact *is*, but not where it *goes*, so every writer
had to know.

Two further gaps followed from the same root:

- A rule head could conclude into the tree or into the next round's
  transient bucket, never into the overlay. Anything a rule derived was
  durable-or-invisible (#483).
- An overlay write bypassed induction. Session facts could be read by
  rule bodies but could never trigger a rule.

And composition was read-only: a `QueryLayer` joining several lines
could be queried but not subscribed to. `subscribe` lived on `Branch`
alone.

## The model

A fact's **place** is a property of its attribute, declared once in
the schema and stored in the branch. Writers assert; the schema routes.

### Four layers, fixed and always present

The set is fixed so a declaration can never name a layer some replica
lacks. The names follow the memory taxonomy:

| layer | durable | replicated | backing |
|---|---|---|---|
| `semantic` | yes | yes | the branch tree (the default) |
| `episodic` | yes | no | not yet |
| `procedural` | no | no | the session overlay |
| `sensory` | no | yes | not yet |

Two are backed today, which is what tonk actively needs: `semantic` is
the tree and `procedural` is the overlay. A write to an attribute
placed on an unbacked layer fails the commit rather than landing
somewhere else, so adopting `episodic` or `sensory` later is a change
in what commits accept, not in what a declaration means.

The earlier draft of this note proposed user-named layers with
properties. The fixed set was chosen instead because layer wiring must
live in the database (a name that exists on one replica and not
another is exactly the bug the model is meant to remove), and a fixed
vocabulary makes that trivially true. User-defined layers remain
possible on top: they would be named bundles of the same two
properties, and the routing below would not change.

### Declaration

`dialog.attribute/layer` `of` `attribute:<namespace>/<name>` `is` the
layer name. It is a branch-level fact, deliberately outside any
concept's content address, so the same descriptor may be procedural on
one branch and semantic on another. It takes effect in the commit that
declares it, so a transaction can declare and use a placement
together. Undeclared attributes are semantic. `Placement::new(attr,
Layer::Procedural)` is the `Statement`; retracting it returns the
attribute to the tree.

### Routing

A transaction accumulates instructions as before. At commit, after
induction has settled the batch, the batch is partitioned by each
instruction's attribute: semantic instructions go to the tree commit,
procedural ones to the overlay, applied only once the tree commit has
succeeded so a failed commit leaves the session untouched. A batch
with no semantic instructions mints no revision.

Because the partition happens after induction, both directions work
with no change to the induction loop:

- A procedural write is part of the stimulus, so rules watching that
  attribute fire, and their semantic heads land in the tree.
- A rule whose head attribute is procedural folds its conclusion into
  the settled batch like any durable novelty, and the partition
  carries it to the overlay, where every subscription on the branch
  sees it. This closes #483 for state-shaped conclusions without a
  side channel and without the write amplification or crash re-fire
  that persisting intermediates on the tree would carry.

A retract of a procedural fact removes it from the overlay rather than
tombstoning it: the overlay is the store for that attribute, so there
is no tree fact to shadow.

### Concepts across layers

A concept whose attributes span layers fans out on write and joins on
read. This is deliberate: tonk already has concepts with a durable half
and a session half, and forcing them apart would push the join into
every consumer. The cost to keep in view is that a required
session-scoped attribute makes the concept absent on another device and
after a restart. That is a schema decision per concept, not a rule
the engine enforces.

### Composite subscriptions

`QueryLayer::subscribe` registers a standing query over every line the
layer joins, plus the layer's own `.with(..)` facts. Each line is
pinned separately, revision and session-overlay epoch, so a poll
re-evaluates exactly when some line moved, and the incremental path
diffs only the lines that did: each moved line's cover-scoped tree
diff yields its touched set, the sets union, and one DRed maintenance
step runs over the composite. A rule-range hit on any line forces a
recompute, as before. `Branch::subscribe` is now the single-line case
of this.

A joined line's session overlay is read live at every evaluation, not
captured when the subscription is made. The layer's constructor folds
each line's overlay into the layer's changes; the subscription lifts
those back out so a stale snapshot never shadows the moving session.

## What this removes, once tonk adopts it

- `dispatch` for state-shaped commands and every `overlay().assert`
  become plain `assert` with a declared attribute.
- The navigate side channel: a rule concludes a procedural
  `site/navigate` fact, the page's subscription sees it, the host
  performs the effect. The client id on `CommandOrigin` goes with it.
- The `CreateNotebookHandler` workaround.
- The transact route's pre-commit transient snapshot, for any command
  that is procedural state rather than a one-round transient.
- The blanket-versus-scoped overlay clear hazard, for facts that are
  session-scoped by schema: they are retracted through transactions
  like any fact.

Reconciler pairs follow the same shape: a desired value on one
attribute and one owner, an observed value on another attribute and
another owner, and a controller subscribed to both. Tonk's pause-sync
fight is one attribute doing both jobs.

## What this increment does not do

- **No instant log.** Transients still live for one induction round
  and are still swept; a transient concluded by a rule is still
  unobservable. This increment makes the *state* case observable by
  giving it a layer; the *event* case wants a per-instant log on the
  procedural store with observer offsets, which is the next step. Once
  that exists, `transient:` becomes sugar for a sweep rule
  (`retract! C when C`) plus placement on the procedural layer, and
  the transient bucket becomes an optimization, as the inductive-rules
  note already argues.
- **No episodic or sensory backing.** Episodic wants a second,
  unpushed tree per branch; the composite subscription already takes
  a vector of pins, so that is additive. Sensory wants an event record
  polarity in history so a fact can replicate without entering the
  state fold.
- **No per-connection partition of the procedural layer.** Tonk keys
  per-tab facts by entity and reaps them; an owner tag on session
  facts with partition GC would make that structural.
- **No branch-union retraction semantics.** Joining two branches that
  hold the same attribute still unions their facts, and a retraction
  on one does not hide the fact on the other. Composite subscriptions
  over disjoint entity sets, which the seed-branch case has, are
  correct today.
- **Placement changes are not migrations.** Moving an attribute
  between layers does not move its existing facts. It is the same
  class of problem as the frozen-descriptor trap, and the honest
  answer for now is that placements are declared before facts are
  written.

## Two identities

The hash with teeth is the semantic head: the only thing two replicas
must agree on. It cannot cover procedural or episodic state, which
differ between replicas by design. The identity a subscription pins is
a vector of per-line revisions and epochs. Both are deterministic over
their inputs; they answer different questions, and keeping them apart
is what lets root hashes keep their teeth.

## Path from here

1. Tonk migration: declare `xyz.tonk.site/*`, sync status, email
   status, and the other overlay concepts procedural; replace overlay
   writes with transactions; turn `navigate` into a procedural
   conclusion with a page-side subscriber; delete the notebook
   workaround; split pause-sync into a desired/observed pair.
2. The procedural instant log and observer offsets, then `transient:`
   as sugar over it.
3. Episodic backing as a second tree per branch.
4. Sensory backing as a history record polarity.
