# Reconciling with the layer model

The layer model (`notes/layers.md`, #721; built in #720, #722) and the
text recommendation describe the same shape from two sides.

```
layer model                          text recommendation
───────────                          ───────────────────
memory layer + transport (channel)   live path: changes to peers now
replicated layer (branch)            durable path: changes at pauses
a rule moves state between kinds     flush: changes → batch, retract
channel delivers state, not history  lagging peer catches up from held
                                     changes, then from the branch
```

Text in layer terms:

- `text/change` placed on a `memory` scope with a channel. One fact per
  keystroke, value = a library change. Not `event`: an event is
  retracted after one commit, a change must stay until flushed.
- `text/batch` placed on the `replicated` scope. One fact per flush,
  value = the changes since the last flush, library encoded.
- A rule (invariant 13) reads `text/change`, writes one `text/batch`,
  retracts the changes it took.
- The merge engine and positional index sit outside the stack: a local
  view fed by every change that enters either layer, deduplicated by
  change id.

## Where they disagree

1. **"There is no other store of facts" (invariant 1)** against a text
   region whose entries are not facts. The layer model says the batch
   is a fact (H9). The region exists only if H9 costs too much.
2. **Commit floor.** A replicated commit costs ~66 KB median; a region
   flush ~10 KB (E1). H9 pays the first per flush.
3. **Rules per keystroke.** Every fact that enters a layer is offered to
   rules (invariant 9), so each keystroke runs a stack commit and an
   induction round. #693 cut ephemeral-only commit cost; how far is H10.
4. **State, not history (14)** fits accreting changes: the state of the
   memory layer is the unflushed changes. It does not fit a library's
   live sync protocol, which is why the channel carries changes as facts
   rather than the library's sync messages.

## From automerge-repo

- A document is a handle with a lifecycle (loading, ready, unavailable)
  and a URL; it arrives from storage or a peer, whichever is first.
- A share policy gates, per peer and document, what syncs. Dialog's
  equivalent is the partial replication range plus capabilities.
- Ephemeral broadcast is scoped to peers that have the document open
  (`DocHandle::broadcast` in samod). The live channel should be scoped
  per document the same way.
- Per-connection "they have our changes" and "we have theirs" futures:
  the signal an editor needs for saved and synced indicators.
- Its limits are what Dialog already has: sync needs the whole document
  in memory, long histories sync slowly, and authorization is unsolved.
- It does not split live from durable: changes sync continuously over a
  connection; storage is a chunk per change compacted into snapshots.
