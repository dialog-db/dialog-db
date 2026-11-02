# Layers

Status: the model that the stack work (#578, #720) converges on. One
page; each numbered sentence is an invariant, and the randomized
subscription harness checks the ones a test can reach. Where this
differs from `state-layers.md`, this is the intent and that is the
history.

## One primitive

1. A **layer** holds facts and has a **kind**. There are five:

   | kind         | lives in | survives a restart | reaches peers | a fact stays until          |
   |--------------|----------|--------------------|---------------|-----------------------------|
   | `replicated` | a branch | yes                | yes           | retracted                   |
   | `durable`    | a branch | yes                | no            | retracted                   |
   | `memory`     | memory   | no                 | no            | retracted                   |
   | `queue`      | memory   | no                 | no            | retracted, by what handles it |
   | `tick`       | memory   | no                 | no            | the next commit             |

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

6. A stack reads each layer at its live head. Capture, recording
   another layer's head as a fact, exists only when the encloser is
   `replicated` or `durable`, and no such encloser exists today.

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

10. A fact in a `tick` layer is retracted by the commit after the one
    it entered in. It is there to make rules and subscriptions react
    in one commit, whether or not anything handled it. (This replaces
    transients, dispatch and witnesses.)

11. A fact in a `queue` layer stays until the rule or writer that
    handles it retracts it, in the same commit. An unhandled fact
    fires again at the next commit. (This replaces commands that need
    a receipt: delivery is at least once, and the entity is the
    idempotency key.)

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

Descriptor identities and `stack:` hashes; link facts and
`advance`/capture for memory enclosers; transients, `dispatch`,
`TransientAttribute`, witness rounds and `Committed::derived`;
`Branch::overlay` as a mechanism apart from a layer; snapshots in
stacks.

## What this keeps

Placements and guided reads, divergences, read-only links, one layer
under several scopes, holds until publish, the entered watch, channels,
the randomized invariant.

## Order

1. `Branch::overlay` becomes a `memory` layer; `branch.query()` is a
   one-layer stack.
2. Live reads for stacks with no durable encloser.
3. Scope kinds as schema facts, checked at `link`.
4. `tick` and `queue` layers; migrate sapling's and tonk's commands;
   delete transients.
5. Channels reconcile by state; sapling's worker–daemon link first,
   iroh second.

## Open

- A `queue` fact that no rule ever handles: a clock (a periodic
  `compact` input) retracts by age, or it stays. Leaning: it stays,
  and the inspector shows it.
- Whether a `tick` fact is visible to a subscription poll between the
  commit it entered in and the next one, or only to rules. Leaning:
  visible, so a UI can react without a rule.
- Placements from a `replicated` layer that is not the bottom (a peer's
  space above a shared one): a higher durable layer outranks a lower.
  Not built.
