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

5. An attribute is placed on exactly one layer: `in: :session`. With
   no `in:`, the primary. The layer is part of the attribute's
   identity: `site/path in: :session` and `site/path in: :space` are
   two attributes that share a name and a type.

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

1. Done, in [#795](https://github.com/dialog-db/dialog-db/pull/795),
   rebuilt on `main` rather than ported from #578 and #720: `in:` on
   attributes (`Placement`, `LayerDeclaration`), guided reads and
   divergences, `branch.session()` building the memory layers from
   the primary's schema, `invoke` and `transact` with the re-run from
   a moved head, a rule head landing on its placed layer with its
   pick, `Session::subscribe` over the composite with the churn
   invariant. Induction's rounds are the ticks; they keep their name
   in the code.
2. A tick mints its instants in the memory layers as it ends, not at
   the end of the dispatch, so a subscriber sees the ticks in order
   (point 12). Today a dispatch lands each layer's share once, after
   the publish.
3. Sapling and tonk move onto `invoke` and delete their wiring and
   retry code. `Branch::overlay` becomes the session layer of a
   session with no declared layers.
4. `sync` on a session: pull and push every layer with upstreams.
   Layers that are other branches of the repository (device, index),
   named by the schema; layers on disk (`store: disk`).
5. Write-only links. Channels (8a): a one-tick layer with a transport,
   sapling's worker to daemon link first.

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
- `in:` as a term of the attribute descriptor. The maintainer's picture
  is an image editor's: a stroke lands on the layer the tool is on,
  chosen per stroke, not per colour. So `in` sits beside `as` and
  `pick` in the descriptor of each attribute a concept reads or
  writes, constant or a variable, and a variable is a field of the
  concept like any other:

  ```clj
  (concept (todo ?i list: ?l title: ?t state: ?s at: ?where)
    (xyz.todo/title of: ?i is: ?t as: :text pick: :last in: ?where)
    (xyz.todo/state of: ?i is: ?s as: [:open :done] pick: :last in: ?where)
    (xyz.list/task of: ?l is: ?i as: :entity pick: :all in: ?where))
  ```

  Asserting a `todo` with `at: layer:x` writes each cell there;
  querying with `at:` bound reads that layer alone, unbound unions the
  layers and binds `?where` to the one each row came from. A rule head
  needs nothing of its own: its concept's descriptor carries the `in`.
  A variable `in` on a head is Bloom's `@address`, bound by the rule
  from data. Precedence: the descriptor's `in` over the attribute's
  placement over the primary; a placement pins, so a constant `in`
  that contradicts it is refused at analysis and a variable `in` over
  a placed attribute has one candidate. The primary's own layer entity
  is the branch's subject. This replaces an "active layer" fact and
  copy rules, which make every write two steps, and it replaces any
  `in` on the transaction, which would be outside the query engine.
  Engine shape: the descriptor and the attribute statement gain `in`;
  a cell of `Changes` carries its layer; a scanned row carries the
  layer it came from (the guided read already knows it per stream);
  a constant `in` picks streams, a variable unions them and binds.
- Layers as facts of the bottom layer, not of a meta branch. A layer
  that is a branch is declared where every layer is, in the primary,
  so the set of layers is under the primary's root hash. Open whether
  the primary also records each branch layer's revision, the way a
  submodule pointer does: then one root hash names the whole
  composite's state, a subscription pins once, and a sync is one
  commit of moved pointers; but a push carries pointers a peer may not
  have pulled yet, and two peers moving one pointer conflict on the
  primary's head. A memory layer records no revision either way.
- Layers that are branches. The meta branch already keeps which
  branches exist, their revisions and their upstreams (tonk's
  `tonk.dialog.replica/active-branch`, `xyz.tonk.branch/name`). A
  layer with `dialog.layer/store "branch"` is one of them, opened as a
  source of the composite (a composite already has several sources),
  and `session.sync()` pushes and pulls the primary and every branch
  layer with its own upstreams. Such a layer is a queue when the sender
  asserts commands into it and the recipient retracts them once
  handled: order from the branch's commits, once from the retraction,
  and the retraction is the acknowledgement the sender reads on its
  next pull. The recipient's rules fire on arrival already: a pull's
  novelty is induction's stimulus through the watermark lag. This is
  the durable end of the channel question above; `channel:self` and
  `channel:host` are the one-tick end. Both are layers.
- Discovery. A provider announces what it performs as facts: the
  capability (a command concept), the provider's DID, when the
  announcement expires. Announcements are TTL'd rows of a layer, as
  mDNS records are of a scope: an in-process handler's in the session's
  own memory layer, a peer's arriving through a layer with upstreams,
  so they are queried like any fact and a page can list what it may
  ask for. An invoke of a foreign capability is a command with a
  subject; the session's routing consumer reads the announcements,
  picks a provider, carries the command over the transport that
  reaches it (17), and the answer comes back on the feed naming the
  command's entity. Whether "invoke" is a command of its own (`invoke`
  with a target and a payload) or implicit in the subject: leaning
  implicit, so one notation serves a local and a remote capability and
  only the routing consumer knows transports. Still open: how an
  announcement proves the provider may offer the capability (a
  delegation beside the row), and what the asker proves to the provider
  (its chain, which #764 already carries).
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
