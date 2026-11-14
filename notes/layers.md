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
`Committed::derived`, `Stack::declare` for a process's own
placements, the randomized invariant, induction's rounds, which are
the ticks.

## Order

1. #720 lands as the engine.
2. `round` becomes `tick` in names and docs; a tick mints its instants
   in the memory layers as it ends, not at the end of the dispatch.
3. `branch.session()`, `invoke`, `transact`, `sync`; the stack built
   from the primary's schema. Sapling and tonk move onto `invoke` and
   delete their wiring and retry code.
4. `in:` on attributes; scopes go; `Branch::overlay` becomes the
   session layer.
5. Write-only links. A transport for a layer with upstreams in memory
   (sapling's worker–daemon link first); `Channel` goes.

## Open

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
  one-tick layer.
- Whether a tick cap is wanted at all, and what a dispatch that hits
  it should leave behind. Leaning: fail the dispatch, publish nothing.
- How the schema names a layer that is another branch of the same
  repository (device, index), so a session can open it unaided; a
  foreign repository's branch (a profile) stays an explicit link.
