# A write under an electing pick must replace every claim it means to replace

Phase: **Directing**. Issue [#759](https://github.com/dialog-db/dialog-db/issues/759). Working material in [`pick-succession/`](pick-succession): [journal](pick-succession/journal.md), [claims](pick-succession/claims.md), [glossary](pick-succession/glossary.md), [use cases](pick-succession/use-cases.md), [direction](pick-succession/direction.md).

## Description

Two replicas write the same cell under `max`, one 5 and the other 7, without seeing each other. After they merge, a read returns 7. A replica that holds both then writes 3, meaning to replace the value. The next read returns 5.

The write retracted the claim the pick elected, 7, and left 5 live. Under `min` and `top` it goes the same way. Under `last` the read comes out right, but the losing claim stays live, and the cell never returns to one claim.

A second case came up while working through the first, and it takes three replicas. A writes, B writes having seen it, then A writes again having seen B's write, which succeeds it. C syncs with A first, and gets A's last write without B's, which A had retired. Then C syncs with B, and B's write arrives after the write that succeeded it. On C it should read as succeeded, not as concurrent; under `min` or `max` today it can win the read.

```mermaid
sequenceDiagram
  participant A as Replica A
  participant B as Replica B
  participant C as Replica C
  A->>A: write 5 (min)
  A-->>B: sync
  B->>B: write 3 (min): succeeds 5
  B-->>A: sync
  A->>A: write 7 (min): succeeds 3
  A-->>C: sync brings 7
  B-->>C: sync brings 3, after 7
  Note over C: live: 7, 3. 7's cause names 3.<br/>min read today: 3. It should read 7.
```

With two replicas this cannot happen: each sync brings the other's writes in order.

```mermaid
sequenceDiagram
  participant A as Replica A
  participant B as Replica B
  participant C as Replica C
  A->>A: write 5 (max)
  B->>B: write 7 (max)
  A-->>C: sync
  B-->>C: sync
  Note over C: live: 5, 7. max read: 7
  C->>C: write 3 (max): retracts 7 only
  Note over C: live: 5, 3. max read: 5
```

## Problem

A writer who writes a cell under a pick means the next read under that pick to return what was written. A writer who writes the same cell under `all` means their values to stay. Neither holds today. A write retracts only the claim its pick elects, so claims concurrent with that claim stay live and can win the next read. A read elects among live claims without checking whether one succeeded another, so a claim that partial sync brings back after its successor can win too.

## Approach

Not decided. The [direction](pick-succession/direction.md) matrix weighs six approaches. The one the maintainer has stated is column D:

- a write retires the elected claim, every live claim concurrent with it, and every live claim it succeeded;
- it keeps claims that were written before it and never succeeded;
- reads skip a claim that another present claim's cause names;
- nothing is retired on merge.

What "concurrent" is computed from, claim lineage or the revision DAG, is the first implementation decision, and the [claims](pick-succession/claims.md) show why the answer matters. What a write does when it cannot decide is the second: [design-undecided](pick-succession/design-undecided.md), where the stated direction is to fetch the missing revision records when they can be reached, and otherwise to write the value as concurrent with the undecided claim.
