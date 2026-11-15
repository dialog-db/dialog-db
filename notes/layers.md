# Layers, commands, ticks

Status: the model the stack work converges on, after the review of
2026-11. Four words: layers, attributes, commands, ticks. Rules do the
rest. Each numbered sentence is an invariant; where this differs from
`state-layers.md`, this is the intent and that is the history.

## Layers

1. A **layer** is a table of facts. It persists across ticks until its
   facts are retracted. Two things about a layer are deployment, not
   semantics: what backs it (memory or disk) and whether it has
   upstreams (none, or peers it pulls from and pushes to). A session
   layer is a table in memory. A device-local layer is a table on disk
   with no upstream. A replicated layer is the same with upstreams.
   There are no kinds of layer.

2. One layer is the **primary**: the one whose schema defines the
   others. Opening a session on a branch builds, from that schema,
   every layer the branch declares beside it, and links them. Primary
   is a role, not a kind: a device branch with no upstream can be the
   primary of a session as well as a replicated one.

3. A link is **read**, **write**, or both. A write into a layer linked
   read-only is refused before anything changes; a read never includes
   a layer linked write-only.

4. The picture is an image editor's: the composite is what you read,
   every layer at once; a read-only link is a locked layer; a layer not
   linked into this session is hidden.

## Attributes

5. A row has a layer as it has a subject and a value: `in` is a
   dimension of the fact, beside `the`, `of` and `is`, and no part of
   the attribute's identity. `site/path` is one attribute wherever its
   rows lie; the same cell on two layers is two cells, each electing
   its own claim under the attribute's pick, and a read with `in`
   unbound returns both, each with its layer bound. Where a row goes
   is said by the `in` of the descriptor that writes it (Open, "`in:`
   as a term"); with none, the primary.

6. A query names the layer it reads the way a Datomic query names its
   db: left out, it reads the primary. Joining across layers, picking
   a winner between two attributes of one name, and copying a value
   from one layer to another are rules, not features of placement.

## Commands

7. A **command** is the scratch: it exists for one tick and is never
   stored. It has no `in:`, so it is dispatched into this session. A
   command for another session goes through a **channel** (see 8a):
   not stored either, but carried.

8. A command is the stimulus of the tick after the one in which it
   was dispatched. A dispatch from outside happens between ticks, so
   it starts the next one. The writes a transaction carries beside a
   command are stimulus of the same tick.

8a. A **channel** is a scratch with a transport: a layer whose facts
   live for one tick, linked write-only in the session that sends and
   read-only in the session that receives, and carried between them.
   In Bloom's terms the three collections are one grid: a table is
   durable, a scratch lives one tick, a channel lives one tick and has
   a transport. Lifetime, like store and upstreams, is a deployment
   fact of a layer. A space coordinating with the persona space that
   opened it (consent, identity, a request and its reply) uses two
   channels, one each way, the way a message port has two ends. What
   is dispatched into a channel is received once, in order, and is
   not merged with an equal dispatch from elsewhere, which a fact in a
   replicated layer would be.

## Ticks

9. A **tick** evaluates the inductive rules once, over the layers as
   they stand plus the tick's stimulus. Everything the rules assert,
   facts and commands alike, lands at the start of the next tick and
   is its stimulus. Nothing a rule writes is visible in the tick that
   wrote it.

10. Ticks chain until a tick writes nothing. Durable writes are staged
    as the ticks run and published once, when they stop. A tick that
    writes only to memory, or only dispatches, commits nothing. A
    chain longer than the cap (16 today, a guard against a command
    that re-dispatches itself) fails the dispatch.

11. A dispatch is one unit of publication: all its ticks stage on the
    heads captured when it began, publish bottom to top, and a lost
    compare-and-set re-runs the whole dispatch from the new heads and
    the original command. A tick is a function of state and stimulus,
    so a re-run is safe.

12. Each tick that changes a memory layer mints one instant there, so
    a subscriber sees the ticks in order, and a maintained
    subscription equals a fresh evaluation after any sequence of
    ticks. (The randomized invariant.)

## Driving it

13. `branch.session()` opens a session: the stack the branch's schema
    describes, with the branch as primary. `session.invoke(command)`
    runs the ticks, stages, publishes and retries. `session.transact(
    facts)` writes without a command (imports, seeds, tests).
    `session.sync()` pulls and pushes the layers with upstreams; it is
    a separate call.

## Two examples, tick by tick

**A click increments a counter.** Rule: `count ← prev + 1 when
click(c) ∧ count(c, prev)`, `count in: :space`.

| tick | stimulus | rules see | rules write |
|---|---|---|---|
| 1 | `click(c)` | click, count = 1 | count = 2 |
| 2 | count = 2 | count = 2, no click | nothing |

Two ticks, one publish. The rule never sees count = 2 beside the
click, so it fires once. (With the click visible to a second round in
the same tick, it would derive 3, then 4: the loop the earlier
`event`-layer design hit.)

**A command cascade.** Rules: `open(doc) when navigate(path) ∧
site(path, doc)` dispatches `open`; `selected ← doc when open(doc)`,
`selected in: :session`.

| tick | stimulus | rules write |
|---|---|---|
| 1 | `navigate(p)` | dispatch `open(d)` |
| 2 | `open(d)` | selected = d |
| 3 | selected = d | nothing |

Three ticks; nothing durable was written, so nothing commits; the
session layer minted two instants, and a subscription over `selected`
reports one delta.

## What this removes

Scopes as names standing for sets of layers, and the default scope;
the `event` and `queue` layer kinds and `dialog.scope/kind`;
`TransientAttribute` and the witness rounds, once commands are the
scratch everywhere; `Channel` as a type apart from a layer with
upstreams and a transport; `Branch::overlay` as a mechanism apart from
the session layer of a one-layer stack; the consumer's own
advance-and-retry loop.

## What this keeps

Placement (now attribute → one layer), guided reads and divergences,
read-only links, capture by a durable encloser, holds until publish,
`Committed::derived`, `Session::declare` for a process's own
placements, the randomized invariant, induction's rounds, which are
the ticks.

## Order

1. Done, in [#795](https://github.com/dialog-db/dialog-db/pull/795)
   and [#797](https://github.com/dialog-db/dialog-db/pull/797), rebuilt
   on `main` rather than ported from #578 and #720, and reshaped on
   the branch that follows them: `in` as a dimension of the row and a
   term of the attribute descriptor, with `Statement::at` for a write;
   `LayerDeclaration`; `branch.session()` building the memory layers
   from the primary's schema; `invoke` and `transact` with the re-run
   from a moved head; a rule head landing in the layer its descriptor
   names, with its pick; `Session::subscribe` over the composite with
   the churn invariant; handlers and the command feed (to be replaced
   by subscriptions over a channel, below). The layers every session
   has are named `memory:`, `session:` and `control:`. Induction's
   rounds are the ticks; they keep their name in the code.
2. Done, in [#803](https://github.com/dialog-db/dialog-db/pull/803),
   on [#800](https://github.com/dialog-db/dialog-db/pull/800), which
   made `in` a dimension of the row: the `session:` layer as the
   session's own, held whether or not the schema declares it, the
   store `Branch::overlay` hands out under its older name; the
   revision of every layer as its rows (`dialog.layer/revision`,
   base58, under `last`), recorded once a dispatch publishes, and the
   change in them since the last recording (the retraction of the
   recorded row, then the row as it stands) as the dispatch's
   stimulus, so a rule premised on a layer's revision fires when the
   layer moved; the selected layer as a row of `session:`
   (`dialog.session/layer`), which a write with no `in` lands in, the
   primary when none is selected, the selected cells moved out of the
   primary before anything settles; machinery under `dialog.` never
   follows the selection. Decided on the way: a row with no layer
   stamped is the primary's, `memory:`, as a write with no `in` is,
   so the common read moves nothing per row (the Perf gate caught the
   stamping).
3. Done, in [#805](https://github.com/dialog-db/dialog-db/pull/805):
   each tick that changes a memory layer mints one instant there, in
   tick order, minted at publish (point 11 holds: a refused publish
   shows in no layer), so a subscriber sees the ticks in order. The
   rounds read each layer as the ticks before them leave it, the
   link's own layer writes are the first tick and the first round's
   stimulus, and a `last` write into `session:` succeeds the row the
   layer holds within the dispatch.
4. Done, in [#807](https://github.com/dialog-db/dialog-db/pull/807):
   `control:` as a layer, holding what a dispatch writes into it for
   one tick (a command, a write `.at(control:)`, a head naming
   `in: control:`), read as rows of `control:` by a premise naming it
   or binding `in`; the `transient` marker stays, for now, as what
   marks a concept a command. Channels `control:<name>` declared with
   `Store::Control`, in memory, their rows kept until the consumer
   retracts them. Providers read what arrives
   (`Session::arrivals(layer, query)`: the layer's newly asserted
   rows through a query, each once, retractions dropped, `poll` or
   `next`) and answer by invoking. `Session::commands()`, `Handler`
   and the feed are gone; a dispatch's commands stand in `control:`
   for one instant once it has published (point 18).
4a. Done, in [#809](https://github.com/dialog-db/dialog-db/pull/809),
   from what tonk's move (tonk-labs/tonk#1318) found: a transaction a
   session hands out lands its commands when its publish wins, however
   it is published; a session opens through a read-only env
   (`OpenSessionEnv`); a whole layer is a legal scan (`in:` constant,
   all else variables; `Selector::only_in`), and `Arrivals` reads rows
   without a query (`arrivals_rows`). Still open from that move: the
   session's revision rows live in `Branch::overlay()`, which an
   embedder may sweep for its own reasons; and whether the asking tab
   becomes a field of the command so a provider can run on `next`
   outside the request.
5. Sapling and tonk move onto `invoke` and subscriptions over
   channels, and delete their wiring, registry and retry code.
6. Done, in [#808](https://github.com/dialog-db/dialog-db/pull/808):
   a durable layer is a branch the primary points at, declared
   `memory:<branch> dialog.layer/store "durable"` as a fact of the
   primary, read as a line stamped with its name, written by one
   commit per dispatch on its branch with the pointer fact
   (`dialog.layer/revision`, the tree, base58) in the primary's own
   commit, published bottom to top, no memory cells. `Session::sync`
   pulls the layers then the primary, writes a pointer where a pull
   left a layer the primary does not point at, and pushes the layers
   then the primary. Open: whether the declaration carries upstream
   facts (today upstreams are each branch's own, set per replica), and
   discovery as announcements.

## The control channel

14. A dispatch's commands, once it has published, are the session's
    **output**: every command invoked or dispatched by a rule in any
    tick, in order, once (`Session::commands()`, a feed a reader polls
    by sequence, bounded like a layer's instants). Nothing else sees a
    command: it lands on no layer, so no query and no subscription can
    observe it, and that is right, since a command is an event, not
    state. What observes commands is a **consumer** of the feed.

15. A consumer answers by invoking: a command naming the one it
    answers by that command's entity, which is the stimulus of a new
    tick, where a rule captures what it wants of the answer on the
    layer the attribute is placed on. A page learns the outcome
    through the subscription it already holds. No reply body, no side
    channel to the asking page: the asker is a field of the command.

16. One feed per session, not a channel per target. A command's
    address is the capability it names, its attribute domain, and
    optionally the subject (DID) whose capability it is; absent, this
    session's own. Routing by capability is what a consumer does, and
    what discovery (Open) answers. A channel per target would be a
    second registry.

17. Three transports carry the same feed. In process: a `Handler`
    registered on the session, run by the invoke loop after each
    publish, its answers chained until none answers. Kernel to host:
    the world's `application.send` carries the dispatches out and
    `channel.receive` brings the answers in, as the feed and an invoke
    do. Peer to peer: an invocation over a relay (#764), signed by the
    session and verified by the provider.

18. A handler runs only after the dispatch it answers has published,
    so a dispatch stays a function of head and stimulus: a lost
    compare-and-swap re-runs no effect. A handler's answer is a
    dispatch with its own compare-and-swap, re-run from the new head
    when it loses, so the answer's writes need no isolation of their
    own (tonk's `TODO(stm)` on command outcomes).

What this dissolves in tonk's worker: the pre-commit capture of
transients and `spawn_dispatch` (the feed records every dispatch
after it publishes); `CommandRegistry::match_transients` (a consumer
matches the feed's commands by attribute, as it does today, or dialog
decodes the concept for it); `CommandOrigin.repo` and `.branch` (a
session is a branch); `CommandOrigin.client` and `notify_navigate`
(the asking site is a field of the command, as `SiteSelect` already
carries, and a navigation is a fact on the site's session layer the
page subscribes to); the two vocabularies selected by origin
(handlers are registered per session, so a space's session holds a
space's and the profile's holds the profile's). What stays: the
`Provider<C>` impls, which are the effects.

## Decisions of 2026-11-15, after tonk moved onto the stack

Made by the maintainer on reading what the stack built (#795 to #810) and
what tonk's move onto it (tonk-labs/tonk#1318) found. They supersede the
Order's descriptions where they differ.

1. **The thing is a `Stack`**, not a `Session`: `branch.stack()`, a stack
   of layers. `schema::Session` (`db:session`) already names something
   else. A separate crate later, once the model below is built; the
   rename now.
2. **Every stack has three implied layers**, `memory:` (the primary),
   `session:` (its own, process-local) and `control:` (the channel it
   invokes through); `layers()` lists all three. A plain branch has no
   others. A layer's revision row exists from the moment the stack opens.
3. **Layer facts.** `dialog.layer/store` goes. A layer is described by
   `dialog.layer/type` with a bare-scheme entity as the value, and a
   branch layer names its branch entity, the same entity the meta
   branch derives from `(replica, name)`:

   ```
   ; implied, written into session: when the stack opens
   control:   dialog.layer/type    channel:
   session:   dialog.layer/type    session:
   memory:    dialog.layer/type    branch:
   memory:    dialog.layer/branch  <branch entity of the primary>

   ; declared, stored in the primary's tree
   session:tab    dialog.layer/type    session:
   memory:notes   dialog.layer/type    branch:
   memory:notes   dialog.layer/branch  <branch entity of notes>
   ```

   Everything about a branch is the branch entity's own meta facts,
   reused verbatim: `dialog.branch/revision` (the revision entity,
   `Revision::entity()`), `dialog.branch/tree`, `dialog.branch/pull`
   and `dialog.branch/push` (branch entities, cardinality-many, whose
   replica's `dialog.replica/peer` and `dialog.peer/address` say where).
   That is what `sync` reads to know where each layer pulls from and
   pushes to. Revisions are the existing revision entity, never base58
   text and never a new `revision:` scheme; a tree is the `node:` entity
   when that lands (tonk-tree component independence branch). A memory
   layer's revision row keeps the chain hash as text until an entity
   form is chosen.
4. **A durable layer has no head of its own.** `memory:notes` is at the
   tree the primary's `dialog.branch/revision` fact for its branch
   entity names, full stop. A dispatch writes the layer's new tree
   through the snapshot API and names it in the same primary commit; a
   failed primary commit leaves an orphan tree block and moves nothing.
   A pull of the primary downloads the pointed trees' intersecting
   ranges before advancing the primary's head (the offline guarantee,
   applied to the pointed trees). The stack's `session:` carries no
   revision row for a durable layer: the primary's fact is the record.
   `sync` takes one `Download` and covers the primary and its pointed
   trees. **Gap to close:** when the local layer and a pulled primary
   name different trees, `last` would pick one arbitrarily; the pull
   must replay our changes (the diff from the tree we started at to
   ours) onto their tree, write the merged tree, and point at it, as a
   branch pull rebases commits.
5. **A channel is a layer**: an ephemeral layer whose rows live one tick,
   so a simpler `Ephemeral`. `control:` is the built-in one; the stack
   creates others (`stack.channel("mail:box")`, asserting
   `mail:box dialog.layer/type channel:` into `session:`). What one end
   asserts lands on the other end's layer for one tick, commands and
   facts alike; not a Rust channel, a layer with a transport.
6. **Writes re-run on a moved head**: `stack.transact(|tx| ..)` takes a
   closure, re-run from the new head when the compare-and-swap is lost,
   so an embedder's reads inside it are repeated; `stack.invoke(command)`
   the same. The word is *invoke*; nothing is "dispatched" in the API.
7. **Effects are explicit steps.** `invoke(..).perform(&env)` returns an
   `InvocationReport`: the revision published, the ticks run, the
   commands that landed in `control:`. `report.effect().perform(&env)`
   runs the stack's registered providers (`stack.provide(p)`, a function
   from landed rows to answer statements, the shape tonk's
   `run_commands` consumes) over those commands and returns the answers'
   report; the report says whether commands remain, so the embedder
   drives the chain (tonk under `waitUntil`). Lifecycle events
   (`invoked`, `answered`, `failed`, `unhandled`, a provider's progress)
   land in `control:` as rows of the command's entity for one tick;
   rules copy what they want into `session:`. A failed provider answers
   with a failure command of the same entity. Registering a provider
   writes `session: dialog.provider/command <concept>` per command it
   serves: local discovery.
8. **`stack.backup()`** returns the embedder's rows of every `session:`
   layer (the implied one and the declared `session:*`) with the
   revision they were taken at; `branch.stack().restore(backup)`
   replays them and the stack recomputes its own rows. Dialog's
   bookkeeping lives apart from `Branch::overlay()` (a second store read
   as `session:`), so an embedder's export, clear or retain never meets
   it. `stack.layer("session:")` hands out the session layer; layers are
   created like channels; `Layer` has variants (ephemeral, durable,
   channel).
9. **Toolchain.** Tonk pins a February 2026 stable; dialog's CI runs a
   newer one. Dialog declares no `rust-version`, so a construct stable
   only in the newer compiler reached tonk unchecked (an `if let` guard,
   fixed in rc.4). Decide: `rust-version` at tonk's toolchain, or move
   tonk's overlay forward.

Order of the rework, each a PR stacked on #810 and pinned by tests:
(a) the `Stack` rename, `dialog.layer/type`, branch entities, revision
entities, `control:` in `layers()`, rows at open; (b) durable layers as
pointed trees, with the merge on pull; (c) channels as a layer type and
`stack.channel`; (d) `transact(|tx|)`, `InvocationReport`, `provide`,
`effect`, lifecycle events, discovery facts; (e) `backup`/`restore` and
the bookkeeping split.

Done, in that order: (a) [#813](https://github.com/dialog-db/dialog-db/pull/813),
(b) [#814](https://github.com/dialog-db/dialog-db/pull/814),
(c) [#815](https://github.com/dialog-db/dialog-db/pull/815),
(d) [#816](https://github.com/dialog-db/dialog-db/pull/816),
(e) [#817](https://github.com/dialog-db/dialog-db/pull/817); the tip is
`0.5.0-rc.5`. Three departures from the points above, each argued in
its PR: a durable layer's pointer is keyed by the layer entity
(`memory:notes dialog.branch/revision`, with `dialog.branch/tree`
beside it), not a branch entity, since `(replica, name)` differs per
replica (#814); the merge on pull runs as a dispatch after the pull
lands, both trees already local (#814); only `control:` echoes what it
was sent onto its outside end (#815). Point 9 is still open.

## Open

- Channels as layers, named by address. The maintainer's proposal,
  which subsumes 14 to 17 and revises 7 and 16: a command is a fact
  whose attribute is placed `in:` a channel layer, so asserting a
  command is asserting a concept, with nothing special about it. A
  channel layer's rows live one tick and it has a transport.
  `channel:self` is the loopback: what lands in it is the next tick's
  stimulus of this session, which is exactly what a transient is
  today, and a command with no `in:` defaults to it as a fact with no
  `in:` defaults to the primary. `channel:host` is the embedder, read
  by the embedder as the layer's instants (`Ephemeral::since`), which
  makes `Session::commands()` of #797 a special case of reading a
  channel layer. Layers then read as addresses: `channel:self`,
  `channel:host`, `memory:shared`, `memory:local`, `session:space`,
  `session:tab`. Several providers of one capability: either two
  attributes of one name placed on two channels, chosen by the rule
  that asserts (static), or one channel whose rows carry the
  provider's DID as an address field, Bloom's `@address`, filled by a
  rule over the announcements (dynamic). Choosing is a rule's
  decision over data, never a router's; fan-out is one row per
  provider. Leaning: adopt, and rework #797 so that `Handler` is
  sugar over a consumer of `channel:host` and the feed goes.
- `in:` as a term of the attribute descriptor. Decided. The picture
  is an image editor's: a stroke lands on the layer the tool is on,
  chosen per stroke, not per colour. `in` sits beside `as` and `pick`
  in the descriptor of each attribute a concept reads or writes, a
  constant or a variable, and a variable is a field of the concept
  like any other:

  ```clj
  (concept (todo ?i list: ?l title: ?t state: ?s at: ?where)
    (xyz.todo/title of: ?i is: ?t as: :text pick: :last in: ?where)
    (xyz.todo/state of: ?i is: ?s as: [:open :done] pick: :last in: ?where)
    (xyz.list/task of: ?l is: ?i as: :entity pick: :all in: ?where))
  ```

  Asserting a `todo` with `at:` bound writes each cell there. Reading
  with `at:` bound reads that layer alone; unbound, the read unions
  every layer and binds `?where` to the one each row came from, and a
  query narrows by binding. A descriptor with no `in` reads and writes
  the primary. A rule head needs nothing of its own: its concept's
  descriptor carries the `in`, and a variable there is Bloom's
  `@address`, bound by the rule from data. The descriptor is the only
  source of placement, so `dialog.attribute/in` of #795 goes, and
  with it the placement map, the guide's per-attribute filter and
  divergences: a read takes a row from a layer because its `in` names
  that layer, bound or constant. `dialog.layer/store` stays: a layer
  must be declared to be written. Engine shape: the descriptor and the
  attribute statement gain `in`; a cell of `Changes` carries its
  layer; a scanned row carries the layer it came from; a constant `in`
  picks streams, a variable unions them and binds.
- Durable layers. Decided: a layer that is a branch is declared in the
  primary like every layer, and the primary records its revision as a
  fact, the way a submodule pointer does, so one root hash names the
  whole composite's state. A durable layer has no memory cell of its
  own: its tree is rooted by the pointer fact, the primary's one
  compare-and-swap moves every pointer at once, and a sync of the
  primary carries the trees its pointers name. There is no second
  class of head conflict, since there is no second head. A memory
  layer records no pointer.
- Subscriptions are the mechanism for effects. `Session::subscribe`
  already maintains a query incrementally across every layer, so an
  embedder observes a channel by subscribing to the concept it
  handles, and `Session::commands()` of #797 goes. One condition
  follows, and it decides a channel's lifetime: a subscription is a
  view of state, so a row asserted and retracted between two polls is
  no delta at all. A channel a subscriber consumes therefore keeps
  its rows until the consumer retracts them, which is the
  acknowledgement; the one-tick lifetime is only for `channel:self`,
  which no subscriber reads. Order is the rows' own (a time or
  sequence field, as tonk's commands carry); "once" is the retraction;
  "exactly one of several handlers" is a claim a handler asserts
  before it acts, decided by the layer's pick.
- Command providers subscribe to the command concept, through a helper
  that drops retractions, so they see commands and not a view. The
  `transient` marker stays for this change; commands become plain
  concepts later. In process, the invoke loop polls the helper once
  per tick, so no command is missed. Out of process, a consumer that
  polls less often than ticks happen sees nothing of a row asserted
  and retracted in between, so its channel keeps rows until the
  consumer retracts them.
- Names. Decided. A layer's name says its store, and the bare prefix
  is that store's default: `memory:` is the primary, the base durable
  branch, and `memory:shared` or `memory:local` another durable layer,
  a branch the primary points at; `session:` is the layer every
  session mints, and `session:tab` another in-memory layer of the
  session's life; `effect:` is the loopback, one tick. "Memory" is the
  word the codebase already uses for what persists (`dialog-effects`'
  memory cells), not for RAM.
- The `session:` layer. Every session mints one, declared or not: it
  replaces `Branch::overlay`, the process-local store a branch has
  today. The revisions the overlay injects now (`dialog.branch/tree`,
  `dialog.branch/edition`, `dialog.branch/revision`) become rows of
  `session:`, one per layer, the session layer's own revision among
  them, written before induction runs so the tick reads them and their
  change is its stimulus like any row's. A subscription that depends
  on the head then depends on rows of a layer it reads, not on a
  special case.
- Whether a channel's one-tick lifetime is enough, or the sender needs
  to know the message was handled. A message port buffers until the
  other end runs; a channel whose receiver's session is closed drops
  the message unless the sender keeps it, and a kept message is an
  outbox: a fact of a durable layer the receiver's handler retracts
  (what 7 said before 8a). The axis is not channel versus branch but
  whether delivery must survive the receiver being away. Leaning: a
  channel is fire-and-forget; anything that must survive is a fact.
- What a channel's transport is between two sessions of one process
  (a tonk space and its persona) and between two peers: the former
  needs no replication, the latter is the peer protocol carrying a
  one-tick layer. [#764](https://github.com/dialog-db/dialog-db/pull/764)
  is the first such transport: a `Relay` performs a UCAN site's
  operations for code that has no network, and tonk's kernel world
  (`plan/persona-capabilities.md`) makes every ask of a space to its
  persona an invocation the kernel signs and the host performs. What
  travels is therefore not a fact but an invocation: a command with a
  subject, arguments, proofs and a signer, answered with a result. A
  channel's message has to be that, so the receiver can check who
  asked and with what authority, and so the reply has somewhere to
  land. In those terms `session.invoke(cmd)` and a channel are one
  API: invoke on a session whose transport is a relay. The reply is
  the stimulus of the caller's next tick, correlated by the
  invocation's identity. The host boots the receiver it relays to, so
  within one process the receiver is never away; across devices it
  can be, and that is where the first bullet bites.
- Whether a tick cap is wanted at all, and what a dispatch that hits
  it should leave behind. Leaning: fail the dispatch, publish nothing.
- How the schema names a layer that is another branch of the same
  repository (device, index), so a session can open it unaided; a
  foreign repository's branch (a profile) stays an explicit link.
