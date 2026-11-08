# A write under an electing pick must replace every claim it means to replace

Phase: **Directing** (backtracked from it to Diagnosing for the second case and back; see the journal). Issue [#759](https://github.com/dialog-db/dialog-db/issues/759). Working material in [`pick-succession/`](pick-succession): [journal](pick-succession/journal.md), [claims](pick-succession/claims.md), [glossary](pick-succession/glossary.md), [use cases](pick-succession/use-cases.md), [direction](pick-succession/direction.md).

## Description

Two replicas write the same cell under `max`, one 5 and the other 7, without seeing each other. After they merge, a read returns 7. A replica that holds both then writes 3, meaning to replace the value. The next read returns 5.

The write retracted the claim the pick elected, 7, and left 5 live. Under `min` and `top` it goes the same way. Under `last` the read comes out right, but the losing claim stays live, and the cell never returns to one claim.

A second case was suspected and turned out not to arise: a claim that partial sync brings back after the claim that succeeded it. A merge already screens for it (`rust/dialog-artifacts/src/merge.rs`): an incoming live claim whose revision the receiver has observed is never re-applied (R1), and incoming history records that cover a claim, a replace's supersedes or a retraction's cause, remove it from the receiver's live set (R3). A claim that arrives after its successor, from a replica that is behind, is either observed already or covered by the record that arrives with the successor.

What remains is true concurrency, which is correct and should stay. 7 is written; A writes 5 over it, and B, without seeing 5, writes 3 over it. C gets 7 ← 5 from A and writes 2 over 5. When C later gets 7 ← 3 from B, 3 and 2 are concurrent: both live, and a read elects between them by the pick. The bug is what the next write on C does with them.

## Problem

Readers expect what a CRDT over every claim of a cell would give: concurrent writes stand side by side and the pick resolves among them, and a write that saw them settles them. They expect it from the operational index alone, without keeping every value and recomputing on read. A write under an electing pick retracts only the claim its pick elects, so claims concurrent with it stay live: under `max`, `min` and `top` one can win the next read, and under `last` the cell never settles. And a writer who appends under `all` expects those values to stay whatever others write under an electing pick.

## Approach

Not decided. The [direction](pick-succession/direction.md) matrix weighs six approaches. The one the maintainer has stated is column D:

- a write retires the elected claim and every live claim concurrent with it;
- it keeps claims that were written before it and never succeeded;
- claims that come back are already handled by the merge's observed-remove screen, so reads need no extra filter.

What "concurrent" is computed from, claim lineage or the revision DAG, is the first implementation decision, and the [claims](pick-succession/claims.md) show why the answer matters. What a write does when it cannot decide is the second: [design-undecided](pick-succession/design-undecided.md), where the stated direction is to fetch the missing revision records when they can be reached, and otherwise to write the value as concurrent with the undecided claim.
