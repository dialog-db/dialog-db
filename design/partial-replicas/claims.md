# A query or a click waits on blocks the replica does not hold: claims

Checked against dialog `a31b144` and tonk `bfb2d72` on 2026-11-12. Paths
without a repository are dialog's, under `rust/`. tonk's line numbers are
from that read and were gathered by a search of the tree; the rows that
rest on them say so.

## What is observed, and who says what

| claim | kind | backing | status |
|---|---|---|---|
| tonk sometimes feels unresponsive: a page load stalls, and a clicked button seems not to react | fact | the maintainer, 2026-11-12; tonk `plan/steady-interface.md` ("Fast one time, slow the next") and `plan/perf-gates.md` record the same | holds |
| The click waits because dialog is replicating "in order to be able to update the tree where the write is supposed to go" | hypothesis | the maintainer's attribution, 2026-11-12. Confirmed if, on clicks that felt stalled, the time is in the commit and the commit fetched blocks; refuted if the time is elsewhere (see the hypotheses on what a click waits on, below). The result template is in the journal's agenda | open |
| Offline, a query or a transaction that reaches a block held by reference "does not work at all" | hypothesis | the maintainer, 2026-11-12. What the code does is two things, neither of them "blocks": with the route known to be down the read fails with an error naming the remote (`dialog-repository/src/repository/archive/hydrate.rs`, `RemoteFallback::Unavailable`); with a connection that stalls, it does not return, since `dialog-remote-s3/src/client.rs` builds `reqwest::Client::new()` with no timeout. Which of the two a user offline meets is not measured | open |
| Returning what is local and leaving out the rest is useful to someone looking at a page | opinion | the maintainer, 2026-11-12, held as a question ("does this idea even have merit") | - |
| For a list or a document a partial answer is acceptable; for an authorization check or a rule with a negation it is a wrong answer | opinion | tonk `plan/steady-interface.md`, "Assumption to check" | - |

## How a replica comes to hold a tree by reference

| claim | kind | backing | status |
|---|---|---|---|
| A fast-forward pull adopts the upstream's head, tree and watermark by root: no diff, no block reads; blocks are fetched later by the reads that reach them | fact | `notes/version-control.md`, scenario 3; `dialog-repository/src/repository/branch/pull.rs` ("its tree is adopted by root"); `branch/download.rs` module doc | holds |
| A pull is two-phase: `prepare` does the network work and writes nothing, `commit` publishes the head and the sync base. A prepared revision can be downloaded before the head moves (`Download::of`), whole or only the three data orderings and the blob index (`operational`) | fact | `notes/version-control.md`, "Pulling changes"; `branch/download.rs` (`Download::of`, `operational`) | holds |
| "Seen" means in the head's ancestry, not bytes on disk: a replica that adopts head H holds H's state whichever blocks it has fetched | fact | `notes/version-control.md`, "Push from a partial replica", last paragraph | holds |
| tonk pulls a space lazily (`.pull().perform()`) and downloads only the account and profile branches before their head moves | fact | tonk `rust/dialog-reactor/src/pull.rs` (`download`, `operational` default false; `hold_root`), `rust/tonk-worker/src/router/adopt.rs` (`pull_content_on_mount`), `router/sync.rs`; from the search | holds |
| No code in tonk chooses a hydration, fallback or local-only policy for a read or a commit | fact | search of tonk's `*.rs` for the policy names, from the search; dialog decides the routes (`dialog-repository/src/repository/archive/routes.rs`) | holds |
| After a background pull moves a space's head, the next read of anything another device changed goes to the network | fact | follows from the two rows above; stated in tonk `plan/steady-interface.md` | holds |
| Before a lazy pull, the replica held every block its reads had reached at the old head: it was stale and, for what it reads, not partial. The pull makes it current and partial | hypothesis | follows from write-back on hydration (`archive/hydrate.rs`) if nothing evicts blocks from the local archive. Refuted by an eviction path, or by reads at the old head that had themselves not finished | open |

## What a read does when a block is absent

| claim | kind | backing | status |
|---|---|---|---|
| A tree reads a node with `nodes::Read`, which the environment answers from what it keeps or else from the routes registered for the subject, writing the block back locally | fact | `dialog-search-tree/src/nodes.rs` (`Read`), `dialog-repository/src/repository/archive/hydrate.rs` (`load_or_hydrate`) | holds |
| A read can be made local: `Read::locally`, `PersistentTree::locally`. An absent node then reads as `None`, wherever else the subject's blocks live | fact | `dialog-search-tree/src/nodes.rs` (`locally`), `tree.rs` (`locally`) | holds |
| The tree diff already has a policy for absent blocks, per side: `Deny` (absence is a fault) and `Boundary` (absence is information: settle the node, do not descend). Push uses `Boundary` | fact | `dialog-search-tree/src/differential.rs` (`MissingBlocks`, `MissingPolicy`); `notes/version-control.md`, "Push from a partial replica" | holds |
| A link carries the separator at the left edge of its subtree, the subtree's hash and a rough size, so a parent that is held names the key range of a child that is not | fact | `dialog-search-tree/src/link.rs` (`Link`) | holds |
| A scan made local can therefore report, beside the entries it read, the exact key ranges it could not read | hypothesis | confirmed by a scan over a tree with some children absent that returns the entries of the held children and the ranges of the absent ones, and whose ranges cover every key the full scan returns and the partial one does not; refuted if some case (a buffered op routed to an absent child, a spilled value) cannot be attributed to a range | open |
| No query result type in `dialog-query` carries anything but rows: a caller cannot tell "no rows" from "could not read" | hypothesis | confirmed by reading `dialog-query`'s output and selection types for a completeness marker; not read this sitting | open |
| A subscription records its demand cover and a poll does nothing when the branch's root has not changed | fact | `dialog-repository/src/repository/branch/subscription.rs`, module doc ("no root change → nothing to do") | holds |
| So a block that arrives later changes no root and wakes no subscription: a partial answer is not corrected by the machinery that exists | hypothesis | follows from the row above; refuted if a poll is also scheduled by a hydration landing (not found in the module doc; tonk schedules polls on commit and on pull) | open |
| The existing design holds that absence may be read only from a subtree that is fully materialized | fact | `notes/incremental-subscriptions.md`, "Negation correctness is a tree-layer property": "the covering subtree is fully materialized before reading absence" | holds |
| The same fact is stored under three orderings, and holding its block in one says nothing about the other two | fact | `notes/version-control.md`, "The picture in one page"; `dialog-corpus/src/tree.rs`, the instruction loop's comment on the entity-index probe | holds |
| A partial answer to one query therefore depends on which ordering the planner chose to read | hypothesis | follows from the row above; confirmed by one query answered two ways from the same partial replica under two plans | open |
| An election's verdict is cached forever; the one revisable outcome, part of the supersedes chain missing on a partial replica, is an error and is never cached | fact | `notes/version-control.md`, end of "Which of two writes wins?" | holds |

## What a commit reads

| claim | kind | backing | status |
|---|---|---|---|
| A commit buffers by default: its writes are appended to buffers in index nodes, starting at the root, and the published root is a buffered one | fact | `dialog-repository/src/repository/branch/commit.rs` (`Commit::canonicalize` doc; `Mint::perform`, "Writes go through the node buffers"); `dialog-corpus/src/buffered.rs` module doc | holds |
| Of an instruction's writes, those to the attribute index, the value index and history are appended without reading. The one read is in the entity index: a probe of the key for an assert that appends, a scan of the cell for an assert under a pick that elects, a read of the standing datum for a retract. On a partial replica that read hydrates | fact | `dialog-corpus/src/tree.rs`, the instruction loop: "this probe is the ONLY read the assert performs — one EAV spine, on a partial replica hydrated through the store's remote fallback. The AEV/VAE/history spines are never read by any write" | holds |
| A buffer that overflows cascades one level down: each child receives the ops routed to it, which rewrites the child. The cap is the tree's branch factor in ops, or its node size target in bytes | fact | `dialog-search-tree/src/hitchhiker.rs`, module doc and `op_buf_size` | holds |
| A cascade therefore reads children of the overflowing node, in every region that has buffered ops, whether or not anything read them before | hypothesis | follows from the row above; confirmed by counting node reads of the commit that overflows a root on a replica holding only the root; not run this sitting | open |
| Before the batch is applied, a transaction's commit runs induction: every inductive rule that watches a touched attribute has its body evaluated, as a query, against the transaction's view; and a write to a cell a rule derives is settled by reading the cell | fact | `dialog-repository/src/repository/branch/transaction/induce.rs` and `succession.rs`, module docs | holds |
| A commit also reads the tree's manifest, the parent's revision record for the skip table, and the context region when the head it builds on is another origin's | fact | `commit.rs`, `Mint::perform` (`tree.manifest`, `extend_skips`, `successor`) | holds |
| A commit holds the branch's writer lock across all of it | fact | `commit.rs`, `perform_on_branch` (`branch.writer().lock()` before `Mint`) | holds |
| Push needs none of the commit's reads to have happened: it computes what the target lacks over a tree partly held by reference, and that contract was written down when buffered writes stopped hydrating the paths they touch | fact | `notes/version-control.md`, "Push from a partial replica" and its invariants ("Worth remembering why these are written down at all") | holds |

## What a click waits on, between the click and the page changing

Each is a place the time could be. None is measured.

| claim | kind | backing | status |
|---|---|---|---|
| The path is: POST `/transact`; `mount_on_first_use` awaited; the commit; `mark_dirty` (push is not awaited); dispatch of every command provider the batch fired, joined; then the scheduled polls, each subscription on the branch in turn. The page changes when its subscription's frame arrives: there is no optimistic update | fact | tonk `rust/tonk-display/src/events/delegate.rs`, `rust/tonk-worker/src/router/transact.rs`, `router/command.rs`, `rust/dialog-reactor/src/transaction.rs`, `branch/state.rs`; from the search | holds |
| The wait is the entity-index read of the commit, hydrating | hypothesis | confirmed or refuted by the result template in the journal | open |
| It is worst for a write about an entity nobody has read: a new entity's key falls wherever its bytes sort, in a leaf of the entity index no query had a reason to fetch | hypothesis | confirmed if commits that assert facts of a newly minted entity fetch entity-index blocks and commits that change a displayed entity do not; refuted if tonk's entity keys cluster (a shared prefix per kind or per parent) | open |
| The wait is a buffer overflow cascading into children held by reference | hypothesis | would show as a commit that fetches blocks in the attribute, value or history regions, about once per buffer's worth of ops | open |
| The wait is induction: the bodies of the rules the click triggers read ranges not yet held | hypothesis | would show as fetches during `induce`, before the batch is applied | open |
| The wait is not in the commit: the click's frame is behind another subscription's poll that is waiting on the network (after a background pull moved the head), or behind a command provider doing I/O, or behind the worker lock held across a sync | hypothesis | tonk `plan/steady-interface.md`, "One global lock is held across network round trips" and "Subscription polls on a branch are sequential" (the queueing there is marked inferred, not measured). Would show as a commit that returns quickly and a frame that arrives late | open |
| tonk logs the commit's duration and each evaluate phase, and logs a subscription poll that takes 500 ms or more. Nothing counts blocks fetched per phase | fact | tonk `rust/dialog-reactor/src/transaction.rs` ("reactor commit timing"), `router/evaluate.rs` ("evaluate timing"), `branch/state.rs` ("slow subscription poll"); `plan/steady-interface.md`, "Measure it", lists the counters as still to add; from the search | holds |

## What a blind write would mean

These bear on the second requested remedy. They are the agent's, 2026-11-12,
and enter on the same terms as anyone's.

| claim | kind | backing | status |
|---|---|---|---|
| A read under `last` elects the claim with the deepest version, by edition first | fact | `dialog-corpus/src/model/update.rs` (`Pick`, `Standing`, `impl Ord for Standing`) | holds |
| A retraction covers exactly the claims its author observed. A retraction that cites no version covers nothing and does not survive a merge with a peer that still holds the fact | fact | `notes/version-control.md`, D3; `notes/novelty-buffer-port.md`, "What a buffered write must still read" | holds |
| A write made by a replica that had not observed a cell is something the system already defines: it is what a concurrent peer's write is. The screens consult only the receiver's state and the incoming delta, so replicas converge whatever the writer had read | fact | `notes/version-control.md`, "The three screen rules", "Why everyone converges", rule 4 of "Rules that must never break" | holds |
| A blind assert under `last` would read as the cell's value on every replica (it is minted above everything in its head's ancestry), and would leave the claim it should have succeeded live beside it. Retracting the new value would then bring the old one back, and each such write would add a claim to the cell | hypothesis | follows from the three rows above. Confirmed by a test that writes a cell without the scan on top of a standing claim, reads it, retracts it and reads again | open |
| A blind assert under `max`, `min` or `top` would not read as the cell's value when the standing claim outranks it, though the same write made with the scan would have retracted that claim | hypothesis | `update.rs`: a write under any pick but `all` succeeds the claim the pick returns. Confirmed by the same test under `max` | open |
| A blind assert of a value the cell already holds would not absorb the standing claim's versions, which the code says can resurrect the fact through a merge | fact | `dialog-corpus/src/tree.rs`, the instruction loop: "an insert-overwrite here silently orphaned the earlier claim, which could then resurrect the fact through a merge" | holds |
| A retract cannot be blind unless the version it withdraws comes from somewhere other than the entity index. A row a query returned carries the standing of the fact that bound it | fact | `tree.rs` (the retract reads the standing datum for the versions its record cites); `dialog-query/src/selection/match.rs` (`standing_of`) | holds |
| The read a blind write skipped is a read of the revision it was built on, which is immutable: its answer is fixed when the write is minted and can be computed whenever the blocks arrive, then recorded by a later revision that cites exactly the versions the first would have | hypothesis | follows from content addressing and from D3 (a covering record names versions, and is valid whenever it is minted). Confirmed if, for random histories, the live set after "blind write, then a covering record computed against the write's base" equals the live set after the same write made with its read; refuted by a history where a third replica's write between the two changes the outcome | open |
| The same does not hold for what induction derives. Rule bodies evaluated later against the base, and committed on a later head, are a write whose reads are older than what it builds on, which is what #758 made a transaction refuse | hypothesis | `design/query-isolation.md` (a transaction reads and commits against one revision); `branch/transaction.rs`, "A transaction reads and commits against one revision". Those two cannot both be true of one replica without saying which gives way | open |
| A cascade that stops at a child held by reference leaves ops in the parent's buffer past its cap; every commit re-encodes the root, whose size the buffer dominates | hypothesis | `hitchhiker.rs`, `op_buf_size` doc ("a commit re-encodes and re-hashes the root frame, whose size the buffer dominates"). The cost of an uncapped root over a long offline session is not measured | open |
| A fact derived from a partial answer and then written is signed, replicated and merged like any other: a rule or an application that writes "because nothing was there" writes it on every device that could not see what was there | hypothesis | follows from induction running on the transaction's view and from the merge keeping concurrent asserts (D3). Confirmed by a rule with a negated premise fired on a replica holding the premise's range by reference | open |

## Opinions the sitting produced

| claim | kind | backing | status |
|---|---|---|---|
| The two requested remedies answer four different things that the description braids together: a read that waits (latency), a read that never returns (no timeout, no offline check), data the device never held (cold), and a write that waits | opinion | the agent, 2026-11-12 | - |
| Stale and complete is a state in which every query is exactly right as of some revision, negations and aggregates included, and a write is an ordinary concurrent write. Current and partial is a state in which no revision exists at which the answers are right. For data the device already held, the first is available and the lazy pull trades it away | opinion | the agent, 2026-11-12; rests on the open hypothesis that the replica was complete for its reads before the pull | - |
| Of the two remedies, the write against a partial replica is the better founded: its meaning is already defined by the merge. A read with holes has no meaning in the engine yet, and needs the answer's type to change through the query engine, subscriptions, caches and rules | opinion | the agent, 2026-11-12 | - |
