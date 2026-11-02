# Layers

Status: the model that the stack work (#578, #720) converges on. One
page; each numbered sentence is an invariant, and the randomized
subscription harness checks the ones a test can reach. Where this
differs from `state-layers.md`, this is the intent and that is the
history.

## The picture

The mental model is an image editor's layers. What you see is the
composite: every layer read as one, the upper occluding the lower. A
layer can be locked (a read-only link), hidden (unlinked, or not in this
stack), or a linked file that shows another document at the revision
you last saw (a durable layer linked beneath another, whose head the
encloser recorded). Where a pixel lives is a property of the pixel's
kind, not of the brush: a placement says which layer an attribute's
facts are on, and a write goes there whatever handle made it.

## One primitive

1. A **layer** holds facts and has a **kind**. There are five:

   | kind         | lives in | survives a restart | reaches peers | a fact stays until          |
   |--------------|----------|--------------------|---------------|-----------------------------|
   | `replicated` | a branch | yes                | yes           | retracted                   |
   | `durable`    | a branch | yes                | no            | retracted                   |
   | `memory`     | memory   | no                 | no            | retracted                   |
   | `queue`      | memory   | no                 | no            | retracted, by what handles it |
   | `tick`       | memory   | no                 | no            | the commit after it entered |

   A channel is a `memory`, `queue` or `tick` layer with a transport.
   A branch's session store is a `memory` layer. There is no other
   store of facts.

2. A **stack** is an ordered list of layers, each bound under one or
   more **scope** names. The bottom is the widest and slowest. Which
   layer stands under a name is wiring, local to the process, in
   memory. It is never a fact.

3. The **schema** is on the bottom and replicates with it: each
   attribute's **placement** (the scope it lives in), the repository's
   default scope, and each scope's **kind**. A stack refuses to bind a
   layer of another kind under a scope, at `link`.

4. A process may hold its own declarations in a stack
   (`Stack::declare`). They outrank the bottom's, yield to a
   transaction's own, and reach no layer.

## Reads

5. A read of a stack takes each attribute only from the layers of its
   placement. A fact found elsewhere is a divergence, reported once,
   read by nothing.

6. A durable encloser (`replicated` or `durable`) captures: it records
   the head of each layer it links as a fact of its own, so the pairing
   survives a restart and replicates with it. That is how a replica one
   pulls from or pushes to can be a layer, a pull being the advance of
   the stack, and how a profile can be a layer beneath a space, so a
   query for grants finds them all. A memory encloser records nothing
   and reads what it links live.

7. A subscription's maintained result equals a fresh evaluation,
   after any sequence of commits, memory writes and clears, on a branch
   or a stack. (The randomized invariant.)

## Writes

8. A transaction's write goes to the layer its attribute's placement
   names. A write placed on a scope the stack does not bind, or binds
   read-only, fails before any layer changes.

9. A commit induces over the whole stack to a fixpoint (at most 16
   rounds), then routes each fact, then stages one chain per branch
   layer and publishes bottom to top. Rules fire on every fact that
   **entered** any layer since the last commit, from this transaction,
   from another writer, from a pull, or over a channel, each exactly
   once.

10. A fact in a `tick` layer is offered to the rules once, in the
    commit it enters in (the next commit, for one asserted from
    outside; the same commit, for one the transaction writes), is read
    by queries and subscriptions as a stored fact while it is held, and
    is retracted by the commit after the one it entered in, whether or
    not anything handled it. (This replaces transients, dispatch and
    witnesses.)

11. A fact in a `queue` layer is offered to the rules at every commit
    until the rule (`retract!`) or writer that handles it retracts it.
    Delivery is at least once, and the entity is the idempotency key.
    (This replaces commands that need a receipt.)

11a. An input (a tick or a queue fact) reaches a rule body the way a
    dispatched transient does: in the first round of the commit only,
    and not as a stored fact of the view. So a rule over an input and
    the state it changes (a click and the count it increments) fires
    once and settles.

12. Movement between kinds of state is a rule: one attribute per
    scope, and a rule that reads one and writes another. The same
    attribute never lives in two scopes.

## Peers

13. A channel delivers a layer's state, not its history: a peer that
    fell behind reconciles against what the layer holds. The instant
    stream is an optimisation of that, never the only path.

14. A channel sends what a commit wrote only once the commit
    published; a commit that fails sends nothing.

## What this removes

Transients, `dispatch`, `TransientAttribute`, witness rounds and
`Committed::derived`, once sapling and tonk are on input layers;
`Branch::overlay` as a mechanism apart from a layer; link facts and
capture for memory enclosers (a memory encloser keeps its wiring in
memory); snapshots in stacks.

## What this keeps

Placements and guided reads, divergences, read-only links, one layer
under several scopes, capture and descriptor identities for durable
enclosers, holds until publish, the entered watch, channels, the
randomized invariant.

## Order

1. Scope kinds, `tick` and `queue` layers (dialog-db/dialog-db#720's
   follow-up). Then migrate sapling's and tonk's commands onto them,
   and delete transients.
2. `Branch::overlay` becomes a `memory` layer; `branch.query()` is a
   one-layer stack.
3. Memory enclosers read live and keep no link facts; capture stays
   for durable enclosers.
4. Channels reconcile by state; sapling's worker–daemon link first,
   iroh second.
5. A replica as a layer (pull as advance), and a profile as a layer
   beneath a space.

## Open

- A `queue` fact that no rule ever handles: a clock (a periodic
  `compact` input) retracts by age, or it stays. Leaning: it stays,
  and the inspector shows it.
- Placements from a `replicated` layer that is not the bottom (a peer's
  space above a shared one): a higher durable layer outranks a lower.
  Not built.
