# What one poll reads

**Decision:** whether a poll reads one fixed state, and so whether several subscriptions can be polled at one revision.

**For:** a fold's two inner subscriptions, and every subscription of one view, have to agree. Read from the code at `feat/query-fold` on 2026-11-19 by a thread of its own; `sub.rs` is `rust/dialog-repository/src/repository/branch/subscription.rs`.

## What a poll reads today

| step | reads | where |
|---|---|---|
| the diff's new side | the head, overlay sequence, memory sequences and durable heads captured once at the start | `sub.rs` 795-798, 818 |
| the overlay's and memories' changes | every instant up to now, not up to the captured sequence | `sub.rs` 1142, 988; `ephemeral.rs` 841-859 |
| each touched entity derived again | whatever head stands when that evaluation starts, pinned separately for each entity, 16 at a time | `sub.rs` 1394-1404, 1316-1320; `source.rs` 495-500 |
| discovery, the new side | its own pin, taken when its env is built | `sub.rs` 1215 |
| the session overlay and memory layers | 🟡 live at each scan, in every env | `session.rs` 1400, 1417-1424 |

So a plain subscription can already report a delta of mixed revisions when a commit lands between two awaits of one poll: the touched set is of R0 to R1 and the rows derived at R2, entity by entity. The next poll diffs from R1 and derives those entities again, so it is repaired. There is no lock, retry or stamp; the doc says "re-triggers on the next poll" (`sub.rs` 763-766). `Stack::settle` polls subscriptions in turn with nothing shared between them (`stack/subscribe.rs` 411-437).

## The approaches

| criterion \ approach | Status quo: each read where the branch is | A. Stamp and retry, in the fold only (built, #839) | B. A frame captured at the start, every read against it, `poll_at(&frame)` |
|---|---|---|---|
| one poll, one revision | no; repaired on the next | usually; after 8 rounds it reports what it has | 🟢 always |
| several subscriptions agree | no | 🟡 the two of one fold; two folds of one view can still differ | 🟢 a caller polls all of them against one frame |
| the plain subscription's own tear | there | there | gone |
| what it takes | nothing | one private `stamp()` | a frame of revision, sequences and pinned durables; a `Source` that holds a branch at a given revision; the frame passed through `maintain`, `evaluate`, `Driven`; `since` bounded by the frame's sequence |
| the durable tree | | | 🟢 immutable at a revision already |
| the overlay and memory layers | | | 🟡 an `Ephemeral` is one shared store, live only. Pinning needs its facts copy-on-write (an `Arc` and `make_mut`, as its tombstones already are), one `Arc` clone for each layer in a frame; copying eagerly would cost the overlay's size on each poll |
| cannot be pinned | | | which layers a stack has, remote routes |
| the Perf gate | | none | ? `subscribe-fanout` and `subscribe-pulled` run 201 polls; a frame costs about what a poll clones already and saves a pin for each entity, unless the overlay is copied |

**Decided, the agent's:** A lands with #839. B is built next as its own pull request, with the overlay's facts copy-on-write first, since a view's subscriptions all have to agree and the driver polls many.
