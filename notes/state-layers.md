# State layers: place and lifetime as attribute properties

Status: proposal. Motivated by #483 (rule-concluded transients are
unobservable after the commit-time rollup) and by the pattern of
workarounds it produced in tonk: the `navigate` postMessage side
channel, the `CreateNotebookHandler` write-then-read-back dance, scoped
overlay clears, and the `Provider<C>` command registry that re-derives
what induction already computed.

## The problem, restated

Dialog has three places a fact can live and three verbs for putting it
there, and the verb is chosen at every call site:

| place | verb | observable by subscribers | survives restart | replicates |
|---|---|---|---|---|
| branch tree | `tx.assert` | yes, as settled state | yes | yes |
| transient bucket | `tx.dispatch` | no | no | no |
| session overlay | `branch.overlay().assert` | yes, as settled state | no | no |

Readers never choose: `QueryLayer::from(SourceRef)` folds the overlay
into every read, and the induction round view layers transients on top.
Writers choose every time. That asymmetry is the incoherence: the
schema knows what a fact *is*, but not where it *goes* or how long it
*lasts*, so every writer has to know both.

Two further gaps follow from the same root:

- A rule head can conclude into the tree or into the next round's
  transient bucket, never into the overlay. Anything a rule derives is
  durable-or-invisible (#483).
- An overlay write bypasses induction. Session facts can be read by rule
  bodies (the round view folds them) but can never trigger a rule.

## The model

A fact has a **place** and a **lifetime**. Both are properties of its
attribute, declared once in the schema. Writers assert; the schema
routes.

### Lifetime: `carry`

Every attribute declares whether its values carry forward to the next
instant by default.

- `carry: true` (default) — state. The value persists until retracted or
  replaced. This is the implicit Dedalus frame rule the storage layer
  already materializes.
- `carry: false` — event. The value exists at exactly one instant. It
  never enters the state fold; it is recorded in that instant's log
  entry and is visible to rule bodies evaluating at that instant.

This replaces the `dialog.concept/transient` marker. A concept-level
`transient:` in the notation lowers to `carry: false` on each of its
attributes. Placement is per attribute rather than per concept because
facts are per attribute: two concepts sharing an attribute cannot
disagree about its lifetime.

Expiry stays open-ended, as intended: an inductive `retract!` rule is
how a `carry: true` value stops carrying, and an inductive `assert!`
rule is how a `carry: false` value gets carried into a state attribute.
Nothing is consumed. An event at instant t is visible to every rule and
every observer of instant t, the way a DOM event reaches every listener.

### Place: `scope`

Every attribute declares which store holds its facts.

- `scope: shared` (default) — the replicated branch tree. Pushed and
  pulled.
- `scope: device` — durable on this replica, never pushed. Sync status,
  local preferences, drafts, must-survive-restart local work.
- `scope: session` — in memory in this process. Lost on restart. Site
  stamps, form state as typed, DOM events, navigation intents.

Scope and lifetime are orthogonal, and all six combinations mean
something:

| | `carry: true` | `carry: false` |
|---|---|---|
| `shared` | replicated state (today's tree) | replicated message: exists at the writer's commit instant and again at each receiver's pull instant |
| `device` | local durable state | local durable event: a command that must survive a restart |
| `session` | session state (today's overlay) | session event: DOM events, `tonk:load`, navigation intents (today's transients, made observable) |

The `shared` × `carry: false` cell is Dedalus's `@async`. It is the
cross-replica effect the inductive-rules note currently routes through
a host bridge or a durable obligation fact. Under the watermark model
every head advance is an instant, so a message pulled in fires the
receiver's rules natively. It costs a history record per message and is
opt-in per attribute.

### One write verb

`tx.assert` and `tx.retract` route each instruction by its attribute's
scope. `dispatch` and `overlay().assert` go away. `.with()` stays as
the query-time speculation tool; it is not a place.

A concept whose attributes span scopes fans out across stores on
assert. That is allowed but discouraged: a concept with a required
`session` attribute fails to match on another device and after a
restart. The guidance is that a concept lives in one scope, and
cross-scope relationships are joins in rules and queries. Reconciler
pairs (below) are the worked example.

### Instants, folds, and logs

A transaction runs induction to a fixpoint. Each round is an instant.
Round k's emissions route by attribute:

- `carry: true` → the state fold of the attribute's store.
- `carry: false` → round k's instant entry, visible to round k+1's
  bodies (exactly today's `transient_overlay`) and to the store's log.

At the end of the transaction each store records what it needs:

- `shared` and `device` fold every round's state changes into one
  revision, as today. Events land as history records with no index
  entries, a new record polarity beside assert, replace, and retract.
  History already records one claim record per instruction, so this is
  a record kind, not a new region.
- `session` appends one log entry per round, in memory. State changes
  update the fold and bump an epoch as the overlay does now.

The rollup is thereby demoted from a semantic rule to a storage
decision. Durable intermediates are folded because storing them costs
storage. Session intermediates are kept because they are memory and
the intermediates are the point. Nothing a rule concludes is
unobservable any more, which closes #483 without a side channel and
without the write amplification or crash re-fire that persisting
intermediates on the tree would carry.

### Two kinds of observer

A store exposes a fold and a log. Subscriptions come in two kinds.

- **State subscription** — what `Branch::subscribe` is today: the delta
  of a query result between two composite versions. The composite
  version is a vector, one component per store: `(shared head, device
  head, session epoch)`. `Subscription` already pins a `(revision,
  overlay_epoch)` pair; this generalizes that pair.
- **Event subscription** — a log tail. The observer holds an offset per
  store and receives instants in order. Each instant carries its events
  and the revision they folded into, so a handler reads durable state
  at that revision rather than at "now". Host handlers are event
  subscriptions with a typed decode. The `Provider<C>` capability gate
  stays on the handler type.

Retention is the observer's contract, not the writer's. The session log
is a bounded ring: an observer that stalls past the ring loses events
and receives a gap marker. Observers that must not lose events subscribe
to a `device` or `shared` log instead, where retention is history's and
offsets persist across restart. That gives at-least-once for local
commands like space creation, which today are lost if the handler dies
mid-run.

### Reconciler pairs

External resources — the URL bar, the document title, the sync process,
a remote service — are not facts dialog owns. Modeling them as commands
makes them edge-triggered and unobservable. Model them as two attributes
with two owners:

- a **desired** value, written by the party that wants it, usually
  `shared` or `session`, `carry: true`;
- an **observed** value, written by the system that controls the
  resource, usually `device` or `session`, `carry: true`.

A controller subscribes to both, acts when they differ, and updates the
observed side. It never writes the desired side. Tonk's pause-sync fight
is the failure mode of using one attribute for both: the system
overwrites the user's intent, and the space can no longer change it.

Navigation under this model: `tonk:load` already stamps the observed
`site/path` into the session store. A page asserts a `site/navigate`
event, `session` scoped, `carry: false`. The host listener that today
handles a postMessage becomes an event subscriber; it performs
`pushState`, the resulting load re-stamps the observed path. The
worker→client message channel goes away.

### Single writer per attribute

The model assumes each attribute has one writer. Different writers
should be different attributes, which is what reconciler pairs enforce.
Under that assumption cross-store atomicity is not a coherent ask: two
writers never share a transaction. The one case that needed it, a
single concept fanning across stores, is discouraged above. This is a
convention for now; an `owner:` declaration on the attribute is a
possible later hardening.

### Which hash has teeth

Two identities, deliberately distinct:

- The **replication hash** is the `shared` store's head. It is the only
  hash two replicas must agree on, so it cannot cover `device` or
  `session` state, which differ between replicas by design.
- The **subscription version** is the vector of store versions. It is
  deterministic over its inputs, so a root hash over it is still a root
  hash. It answers "what did this observer see", not "do we agree".

Conflating the two would make the replication hash meaningless. Keeping
them apart is what lets root hashes keep their teeth.

### Per-connection session facts

The session store is per process, shared by every client of a service
worker. Tonk keys per-tab facts by a `site:<uuid>` entity and reaps them
with `retain_entities` when the client dies. Keep that shape but make
it structural: a session-scoped assert may carry an **owner** tag (a
connection id), and the store drops an owner's partition when the
owner is gone. That removes the scoped-clear hazard where one flow's
`clear()` wiped another tab's site stamp.

## What this removes

- Three write verbs become one. `dispatch`, `overlay().assert`,
  `overlay().retract`, `overlay().clear`, and `retain_entities` become
  `assert`, `retract`, and owner-partition GC.
- Two ephemeral mechanisms become two bits on an attribute.
- The command registry becomes an event subscription. `match_transients`
  and the pre-commit transient snapshot in the transact route go away;
  handlers receive the instants induction produced.
- The `navigate` side channel and the client-id on `CommandOrigin` go
  away.
- The `CreateNotebookHandler` workaround goes away: the rule concludes a
  session event and the page's subscriber acts on it.
- Cross-replica effects need no host bridge: a `shared` event is a
  message.
- The "commands never replicate" property stops being a rule to remember
  and becomes a consequence of the attribute's scope.

## What this does not fix

- The frozen-descriptor trap (`docs/evolving-command-concepts.md`) is
  orthogonal: decode ambiguity between same-shaped concepts is a schema
  problem, and marker fields remain the answer until placement facts
  can disambiguate.
- Branch union (a seed branch joined with `main`) is a different
  problem. It is composition of two sources of the *same* scope, and it
  needs the vector-version subscription from phase 3 plus multiplicity
  semantics so a retraction on one branch does not hide a fact still
  live on another. With disjoint entity sets, which the seed case has,
  union is enough and shadowing never arises.
- `MAX_ROUNDS` is unchanged. It bounds instants per transaction; the
  difference is that a session observer can now see all of them.

## Storage of the declarations

Placement lives beside the concept, not inside the descriptor's content
address, mirroring how `dialog.concept/transient` is deliberately a
branch-level fact today:

- `dialog.attribute/scope` of `<attribute entity>` is `shared | device | session`
- `dialog.attribute/carry` of `<attribute entity>` is `true | false`

Absent means `shared` and `true`. Read through the same head-keyed
cache as the transient marker, overlay slice scanned fresh, so a
placement asserted in the same commit takes effect in that commit.

Notation:

```yaml
concept!: &site/navigate
  scope: session
  transient:            # lowers to carry: false on every attribute
  with:
    href: { the: xyz.tonk.site/navigate, as: text }

concept!: &sync/status
  scope: device
  with:
    state: { the: xyz.tonk.sync/state, as: text }
```

## Path forward

Ordered so that #483 and the tonk workarounds fall first, and each phase
is shippable on its own.

1. **Declarations.** Add the two placement facts and their cache;
   lower `transient:` to `carry: false`; keep `dialog.concept/transient`
   as a read alias for one release. No behavior change.
2. **Session store with a log.** Promote `Overlay` to a store: a fold
   with an epoch (as now) plus a bounded per-round log. Route
   session-scoped writes through the transaction so they induce. In
   `induce`, write each round's `carry: false` emissions into the
   round's log entry instead of dropping them, and have
   `TransactionCommit::perform` return the instants as the commit
   receipt the inductive-rules note lists as not yet built. This closes
   #483.
3. **Observers.** Generalize `Subscription` from `(revision, epoch)` to
   a per-store version vector; add the event subscription with offsets
   over the session log. Owner-tagged session facts and partition GC.
4. **Tonk migration.** `dispatch` → `assert` of `carry: false`
   concepts; command registry → typed event subscribers; delete the
   navigate postMessage path, the pre-commit transient snapshot, and
   the notebook workaround; split pause-sync into a desired/observed
   pair; replace overlay writes with asserts of session-scoped
   concepts.
5. **Device store.** A second, unpushed tree per branch for
   `scope: device`. Subscriptions already take a vector from phase 3,
   so this is additive. Move sync status and local preferences there.
6. **Replicated events.** The event record polarity in history, so
   `shared` × `carry: false` facts replicate and fire at the receiver's
   pull instant. Needs the batch-cancellation rule in the history writer
   to treat an event as its own polarity rather than as assert+retract.

Phases 1 through 3 are dialog-db only. Phase 4 is tonk only. Phases 5
and 6 are additive and independent of each other.

## Open questions

- **Ring size and gap semantics** for the session log. A gap marker is
  the minimum; whether an observer can request a resync from the fold
  is a UI question.
- **Per-round history for durable stores.** This proposal folds durable
  rounds into one revision and records only events per round. If a
  durable-only cascade ever needs to be observable round by round, the
  answer is to conclude an event alongside, not to stop folding.
- **Scope on rule heads.** A rule's head concept carries its own
  placement, so a rule concluding into `session` is just a rule whose
  head concept is session-scoped. Whether a rule may conclude across
  scopes in one head is the same question as concepts spanning scopes,
  and gets the same answer: allowed, discouraged.
- **`device` scope and the watermark.** The induction watermark is per
  branch. With a device store the watermark becomes a vector too, or
  the device store rides the shared store's instants. The second is
  simpler and probably right: a device write is an instant of the same
  branch.
