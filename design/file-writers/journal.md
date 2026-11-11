# Journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | Direction chosen: one file, an extent per writer, native only (the web keeps IndexedDB). Writers never wait on one another's writes; claiming an extent takes a short lock once per extent filled. Two coordination points: integration, by the committer after its commit, and branch updates, under a byte-range lock per cell. Cells are slots at offsets an index names, of a capacity set at creation, written over in place; a moved head always means a replay. | Building, in the order in the top story. |
| **Agenda** | Evaluating the page size, the chunk the file grows by and the run a writer claims. Applying a commit with updated versions over a moved head, keeping its instructions until it publishes. How extents are claimed and recorded; the cell index and its two tables; what integration does to extents and per-commit node records; a cell's default capacity. Replay avoidance is its own topic. | The next implementation decision: claiming extents. |

## 2026-11-10, Describing: the topic opens

**Asked.** Irakli: design branch storage in the same file archive, or
memory cells more generally; branches contending on one file does not feel
right; perhaps each session or branch writes a file of its own and an
asynchronous job integrates it, readers considering both; perhaps main
holds another tree for memory cells.

**Learned.** The integrator is already an option the single-file design
names and leaves unstarted. Cells are a file each today, so two branches'
heads do not contend now; their blocks do, on the file's writer lock. No
run has measured two branches of one replica writing at once.

**Decided.** Not decided. Two wishes are braided in the request: writers
that never wait on one another, and cells kept in the file. Each arrived as
a remedy; the problems they answer are not stated yet.

**Asked (continued).** Who writes at once? Irakli: two processes advancing
two branches independently and concurrently. On main there is no
coordination among them at all; with the single-file archive they have to
contend for writes.

**Learned.** Checked against the code and it holds: main's `fs` archive
writes each block as its own content-addressed file with no shared lock,
and each head is its own cell. The file archive is what introduces the
shared turn. What the waiting costs a process is not stated yet.

## 2026-11-10, Delimiting: the requirement

**Asked.** What does the second process lose by waiting its turn? Irakli:
many processes run and work on separate branches; serializing them and
forcing them to coordinate would disqualify the single-file archive from
such uses, so it has to be addressed.

**Learned.** The cost of a wait is not what decides this: no wait between
processes on separate branches is acceptable. The hypothesis that the wait
matters is superseded by that requirement, not tested.

**Decided.** The requirement, on Irakli's say-so: processes on separate
branches of one space write without waiting on one another. The problem
statement in the top story says so. Open: the same branch, and the cells.

**Asked.** Irakli: is there something to learn from other databases here?

**Learned.** [prior-art.md](prior-art.md). The single-file stores take one
writer at a time. The stores that let writers run at once write each
writer's data where nobody else writes and coordinate only publication, by
the unit that conflicts (a ref, a table), then integrate later, with
readers looking in both. Git's object store is main's `fs` archive plus the
packing it lacks, and is close to the proposal in this topic. The landing
place in all of them belongs to a writer, not a branch.

**Asked.** Irakli: Dolt is probably the only contender.

**Learned.** [prior-art.md](prior-art.md), Dolt. It keeps one journal file
a database, fsynced per transaction, and merges table files in the
background. It does not let separate processes write one database: a
second process is refused writes, and concurrency is sessions inside one
server, which serialize commit-graph writes per branch. Dolt is the closest
product, but it answers the requirement by having one owning process, not
writers of their own; git is the closer precedent for separate processes.
That adds a candidate approach for Direction: one process owns the file and
every writer goes through it.

## 2026-11-10, Directing: writers write blocks, an integrator folds them in

**Asked.** May processes reach the space through one owning process, as
Dolt's server does? Irakli: the idea is to just write blocks and have an
integrator integrate them; queries consult both.

**Learned.** That is git's object store: loose objects without a lock,
packed later, read from both. Both halves exist here: main's `fs` archive
writes blocks as files of their own, and the file archive imports them.
Writers then meet only at a branch head's compare-and-swap, whether on
separate branches or one, which settles the same-branch question the way
main already answers it. A read has to look in the file, then the loose
blocks, then the file again if it grew, or it can miss a block the
integrator moved between its looks (a hypothesis in the ledger).

**Decided.** Not decided. Four approaches in [direction.md](direction.md):
the status quo, loose blocks and an integrator, a journal per writer and an
integrator, and one owning process. Open in the matrix: who integrates and
when, and the web.

**Asked.** Who runs the integrator, and when? Irakli: a scheduler. After a
commit, integration is scheduled: if an integrator holds the lock, it does
the integration; otherwise one is spawned to do it.

**Learned.** SQLite's automatic checkpoint and git's `gc --auto` work this
way: whoever commits past a threshold does the integration, or starts it.
The check "someone already holds the lock" has a lost-wakeup race: blocks
written while the holder is releasing are left until the next commit. The
holder closes it by looking for new blocks again after releasing, and
taking the lock back if there are any.

**Decided.** The scheduling, on Irakli's say-so, into the matrix's "who
integrates" row. Open: where the spawned integrator runs.

**Asked.** A task in the writer's process, or a process of its own?
Irakli: detaching into the background.

**Learned.** Git can detach because git is the binary it re-runs; a library
cannot do that alone, and on the web and iOS there is no process to launch.
So "in the background" is a task off the commit in the writer's process,
resumed by the next process if this one exits, or a detached process where
the host program gives dialog a command to run.

**Decided.** Integration runs in the background, off the commit. Open:
whether a task that dies with its process is enough, or integration has to
outlive the writer.

**Asked.** Irakli, unprompted: ideally, when only one process runs, it writes
into the archive file directly, without the added ceremony.

**Learned.** A process cannot know it is alone, but it does not need to: a
non-blocking try of the writer lock says whether anyone is writing now. A
fifth column follows: try the turn; with it, append directly as today and
fold in any loose blocks waiting; without it, write loose blocks and
schedule integration. With one process it is today's path; under
contention nobody waits; and the writer that next gets the turn does most
of the integrating, so a background integrator runs only when no writer
comes.

**Decided.** Not decided; the column is in the matrix.

**Asked.** Who integrates? Irakli: either each committer, once it has
committed, waits and performs the integration, or some deterministic
process does. What would heads in the file buy? Irakli: he likes SQLite's
approach of just a file.

**Learned.** The two answers conflict as stated: heads in the one file
make a head update take the file's turn, where branches would meet again.
SQLite itself is one file only at rest; open in WAL mode it keeps `-wal`
and `-shm` beside it. Read the same way here: at rest, one file holding
blocks and heads; while writers overlap, loose blocks, and per branch a
lock file and a pending head record, folded in under that branch's lock.

**Decided.** Integration is the committer's: once its commit is published,
it waits for the turn, off the commit, and folds in what it left loose;
closing a space waits for it; a process that dies leaves its blocks to the
next writer with the turn. A space is one file, heads included, on Irakli's
say-so. Open: whether "one file" may mean one file at rest.

**Asked.** Irakli: can the same concurrency be had on the one file, rather
than many?

**Learned.** On native platforms, largely yes, with three mechanisms. Data:
writers append to the file's tail without the turn, the kernel ordering
`O_APPEND` writes or a reservation giving each its range (PostgreSQL's WAL
inserters). Index: only indexing the tail takes the turn, by whoever next
gets it; readers scan the unindexed tail and index it in memory (SQLite's
`-shm` for its WAL). Heads: a byte-range lock per branch in the one file
(SQLite locks byte ranges of its database). The costs: a writer that dies
mid-append leaves a hole with records after it, so the framing has to let
a scan resume; readers scan the tail until it is indexed; and on the web
an OPFS file has one writing handle at a time.

**Decided.** Not decided; a sixth column in the matrix.

**Asked.** Irakli: maybe lock a range, or something like that.

**Learned.** A range lock per write does not help, but a range lock per
extent does: a writer claims an extent of the file by a byte-range lock and
writes only there while it holds it. Each extent then has one writer, so a
writer that dies leaves a torn end of its own extent, recovered as today,
instead of a hole between others' records. Integration copies nothing: the
records are already in the file, and indexing them is the whole job. The
one wait left is claiming a new extent, once per extent filled.

**Decided.** The one-file column now uses an extent per writer. Open:
whether a short wait once per extent filled is within the requirement.

**Asked.** Is a short lock once per extent filled within the requirement?
Irakli: he thinks so. And how do concurrent writers on one branch work?
Like an STM, where a writer that fails keeps retrying over the latest.

**Learned.** That is close to today: across processes a commit already
mints over the head it noted and publishes compare-and-swap, failing with
`VersionMismatch` for the caller to retry; in one process, commits to a
branch take turns on its writer mutex. An STM makes the library run the
retry. What it retries matters: replaying the same instructions over a
newer head loses an update that depended on an earlier read, which is why
an STM re-runs the transaction's body. Each failed attempt leaves
unreferenced blocks behind in the writer's extent.

**Decided.** The once-per-extent wait is acceptable, on Irakli's say-so.
Writers on one branch retry over the latest head, in a loop the library
runs. Open: replay the instructions, or re-run the caller's transaction.

**Asked.** Replay the instructions, or re-run the caller's transaction?
Irakli: failing is the current behaviour; the model is changing so that,
instead of failing, a commit retries the same operation over the new head.

**Learned.** Replaying the operation is close to re-running the
transaction in dialog: a command is an asserted event, and the rules that
derive its writes run at commit against the head being built on, so a
retry re-reads through them. Only writes the caller computed outside the
rules replay as given.

**Decided.** A commit that loses the race retries the same operation over
the new head, in a loop the library runs.

## 2026-11-10, Designing: the direction, and how a head becomes visible

**Asked.** Is the web in scope? Irakli: on the web we will likely keep
IndexedDB; leave it alone. He sees two coordination points: integration
into the archive, and updating branches. A branch update that finds the
head moved replays only if what the transaction read changed; otherwise it
updates the versions. And: how does a branch update become visible to the
next query or transaction without blocking on integration?

**Learned.** Visibility needs two things findable without the file's
index: the latest head, and the nodes it names. A slot per branch in the
header region, updated in place under the branch's byte-range lock, gives
the first in one read; a small per-commit record of node hashes and
offsets, kept in an in-memory map by each reader until integration, gives
the second. One file also orders durability: `fdatasync` makes everything
appended before a head durable with it.

**Decided.** The direction: one file, an extent per writer, native only;
the web keeps IndexedDB. Two coordination points, integration and branch
updates. A commit that loses its race replays only if what it read
changed. First implementation decision opened:
[design-branch-heads.md](design-branch-heads.md), a slot per branch as the
candidate. Open: the read set, and how many branches the slot table holds.

**Asked.** (The read set waits.) Irakli: do for memory cells what is done
for blocks. An index maps each cell to the byte offset of its slot, say
branch A at offset 6, where its value (a 32-byte hash, or whatever size)
is written. The index changes only when cells are created or deleted, as
two tables swapped in turn. A branch update writes only the cell's bytes;
a reader goes to the extents only for a cell new since the index was
written, and otherwise reads the slot at once.

**Learned.** That generalizes the branch slot to every memory cell, with no
fixed table: the index is what grows. Two things the slot has to handle: a
branch cell's value is a `Revision` of a few hundred bytes, not 32, and
other cells vary more; and a write of more than 8 aligned bytes in place can
tear, by a crash or under a reader, so a slot needs two halves written in
turn, each with a sequence and a checksum, the reader taking the newer
whole one.

**Decided.** The slot-per-cell design, with the cell index, replaces the
slot table in [design-branch-heads.md](design-branch-heads.md). Open: what
a slot holds.

**Asked.** The value in a fixed-size slot, or a pointer to a record?
Irakli: branches hold whole records today, but the slot need not point to a
record: it can point to a block, and the block holds whatever is written.

**Learned.** That matches how cells already work: under `fs`, a cell's
edition is the hash of its content. With the value as a block, the slot's
hash is the edition, and a compare-and-swap compares the slot without
reading the value. A value block is new on every update, so almost never
indexed: with its offset in the slot as well, a reader reads it straight
from the mapping.

**Decided.** A cell's slot references a block holding its value; the
block's hash is the edition. Suggested, not decided: the offset and length
beside the hash.

**Asked.** (The read set, again.) Irakli: treating it as spills is a good
call; a fast path and graceful degradation.

**Learned.** Read as the cell slot: a value inline in its slot when it
fits, spilled to a block when it does not, as dialog already spills large
values out of tree nodes. Inline is one read; spilled is a reference with
the block's hash, offset and length. The slot carries the value's hash
either way, so the edition and the compare-and-swap are unchanged.

**Decided.** Cell values inline, spilling to a block (to confirm that this
was the question answered). Open: the slot's size; the read set.

**Asked.** Was that about cells? Irakli: for cells we do not want
accretion, so inline space makes sense; calibrate it, and if more is
needed, resort to spills.

**Decided.** Cells do not accrete: an update writes over the slot. Values
are inline in a slot of a calibrated size; only a value that outgrows it
spills, leaving its previous block behind in the extent on each update.

**Asked.** (The read set waits again.) Irakli: maybe a cell gets a
capacity on creation, and resizing it is a separate operation, like
deleting the cell and creating it again.

**Learned.** That removes spilling from cells altogether. Its one cost: a
reader caches a cell's offset from the index, which learns of a resized
cell's new slot only at the next integration, so the old slot has to
forward to the new one until then.

**Decided.** A cell has a capacity fixed at creation; outgrowing it is a
resize, done as a delete and a create, leaving the old slot forwarding.
Cells never spill. This replaces the inline-or-spilled entry above.

**Asked.** Key ranges, or entities and attributes? Irakli: key ranges seem
better, being less data to hold, but he is unsure of their precision: a
scan may cover a wider range than its candidate frames, so capturing the
triples that end up bound to variables might be the better strategy.

**Learned.** Matched triples are precise for values a commit read, but
miss phantoms: a fact newly asserted into a scanned range that would now
match, which rules picking the latest value or matching on absence depend
on. Combining both answers keeps both properties: record each scan's range
and pattern, diff the trees over the ranges, and test each changed fact
against the pattern.

**Decided.** Not decided; three options in the read-set section of
[design-branch-heads.md](design-branch-heads.md).

**Asked.** Irakli: handle the read set separately; here just rerun, and
separately develop a system that captures what was considered, to avoid a
rerun when there is no overlap.

**Decided.** Scope: a commit that finds its branch's head moved always
replays its operation over the latest head. Replay avoidance is its own
topic, [replay-avoidance](../replay-avoidance.md), with the options and the
phantom analysis moved there.

**Asked.** Continue with extents, or stop? Irakli: here we just need the
ability to apply a commit with updated versions.

**Learned.** A retry is applying the same commit's instructions over the
latest head, its versions minted afresh against it. Today the mint
consumes a commit's instructions as a stream, so a failed publish leaves
nothing to apply again; the commit has to keep them until it publishes.

**Decided.** The retry is that capability: apply a commit with updated
versions over the latest head.

**Asked.** The open items, with candidates and what would decide each.
Irakli: something simple. On a cell's creation, add 20% to the size of its
content and call it the capacity; an update that needs more marks the cell
unpublished, telling readers to read it from the extents, or creates it
over; or a spill, if more capacity is needed.

**Learned.** No measurement is needed for the capacity. The two ways past
it differ in what readers do: an unpublished slot sends every reader of the
cell to the extents until integration gives it a new slot, an index
change; a spill keeps the slot and the index as they are, at one more read
for that cell.

**Decided.** Capacity is the content's size at creation plus 20%. Open:
which of the two an update past it does.

## 2026-11-11, Designing: spills, and integration without corruption

**Asked.** Unpublished, or spill? Irakli: he agrees with spilling, but has
to be sure integration handles spills without corruption, and does not see
how it does.

**Learned.** Integration never writes where a spill depends on: it indexes
records already in place and never touches a slot. What keeps a spill
whole is the order (block, then slot), the value's hash in the slot,
checked against the block, the alternating halves, and an integrator that
reads an extent only up to its writer's published end. A crash or a race
yields the previous value or the new one, whole. The one thing to keep for
later: whatever reclaims space in extents must keep every block a slot
names.

**Decided.** An update that outgrows a cell's capacity spills, on Irakli's
condition; the argument is written in
[design-branch-heads.md](design-branch-heads.md), "Spills and integration",
and its two hypotheses have tests named in the ledger.

**Asked.** Irakli: integration does move things in extents, does it not?

**Learned.** Not in what the agent wrote: it assumed writers append the
final form (frames and tokens), so that integration only indexes. That was
a choice never put to Irakli, and the spill argument rests on it. With
whole blocks in extents and integration converting them, which is what his
first idea did and what takes splitting off every commit, records move:
a slot naming a spill by offset is left pointing at freed space unless it
names it by hash or integration rewrites it, and an extent's space is
reused only after a sync point and once no reader can hold an old offset.
The ledger's "integration never writes where a spill depends on" is a
hypothesis that holds only under the first.

**Decided.** Not decided: [design-integration.md](design-integration.md).

**Asked.** Final form or whole blocks? Irakli: whole blocks. He expected
the integrated form to come out smaller, so the tail just marks the offset
where the extents begin and integration continues from there; it can move
spills left and update their offsets; and with everything integrated it
can truncate the file, unless that messes up writers.

**Learned.** Three steps as stated lose or corrupt data, and each has a
rule that keeps it safe: compact output goes only into space freed by an
earlier, durable integration, never over input still being converted; a
spill moves by new copy, sync, then slot; and truncation waits for the
last process to close, since a mapped reader touching a page past a
shorter end gets `SIGBUS`. Writers are the lesser concern: an integrator
can try an extent's lock before truncating past it.

**Decided.** Whole blocks in extents, integration converting them, records
moving left. Open: how a reader keeps integration from moving or reusing
bytes it is still reading.

**Asked.** Shared locks per extent, or epochs? Irakli: that sounds good.
And: integration comes out smaller by its nature, since it tokenizes; if it
did not, there would be no point integrating, the block being already at
its smallest. So integrating a block can be assumed to free the space the
block took.

**Learned.** It holds by construction: integration keeps a block whole when
its compact form is not smaller, so it never writes more than a block took.
What the size does not settle is when the freed space may be written over:
only once the integration that converted the block is durable, since until
then the old bytes are the only copy the durable index names.

**Decided.** Readers hold a shared byte-range lock on each extent they read;
an integrator frees an extent only once it can take that lock exclusively,
without waiting. Integrating a block frees its space, once its integration
is durable.

**Asked.** Extent size, node lookup, cell index? Irakli: no answers yet;
evaluate and see. Perhaps SQLite has an answer to borrow; otherwise append
whatever needs appending to the extents. The nuance from the last turn: a
gap is needed so integration's compact output fits, or it has to append,
which is doable but makes things move around a lot.

**Learned.** SQLite does not keep a gap: its file is fixed-size pages with
homes, a free list of freed pages reused before the file grows, growth in
chunks, shrinking only by vacuum. Borrowed: extents as runs of pages from
the free list or the file's end, integration writing into free pages and
returning an extent's pages once durable. Its WAL index also answers how a
reader finds a block not yet integrated: a map from key to newest location,
here a record per commit of hashes and offsets.

**Decided.** Pages and a free list, as SQLite, in
[design-integration.md](design-integration.md). Readers find blocks not yet
integrated through per-commit records. Open, to evaluate: page size, chunk
and claimed run.

**Asked.** Measure how many cells a space has? Irakli: it cannot be
measured; it depends on how many branches users create, one or two in our
runs, but it could be far more, so no number can be settled on.

**Learned.** Then the index has to cost the same at one cell and at
millions, and the file already has one that does: the block index, with
cells as one more kind of entry.

**Decided.** Cells are entries in the file's block index (name hash to slot
offset); creating or deleting one is an index change integration makes.

**Asked.** Irakli: go further than swapping roots, and discard or reuse any
node pruned from the index trees.

**Learned.** Format 10 already does this for index pages: any page a write
replaces is reused after the next durable sync point. In this design it now
extends to data pages, an extent's pages returning to the free list. The
search tree's own superseded nodes are not derived like index pages: they
are the space's history, kept by design; reusing them would be garbage
collection of history, a topic of its own.

**Decided.** Nothing new: noted so it is not proposed again as new.

## 2026-11-11, Building: the order

**Asked.** Irakli: we are in agreement; start implementing all of it now.

**Decided.** Building, in the order in the top story: merge main, extents,
integration, cells, retry, the last close. The page size, the chunk the
file grows by and the run a writer claims start at fixed defaults, to be
tuned by measurement.
