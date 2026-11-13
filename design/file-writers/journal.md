# Journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | Direction chosen: one file, an extent per writer, native only (the web keeps IndexedDB). Writers never wait on one another's writes; claiming an extent takes a short lock once per extent filled. Two coordination points: integration, by the committer after its commit, and branch updates, under a byte-range lock per cell. Cells are slots at offsets an index names, of a capacity set at creation, written over in place; a moved head always means a replay. | Building: all five stages built and pushed; the departures are in the last entry. |
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

## 2026-11-11, Building: the five stages built

**Built.** On the single-file branch, each stage tested, pushed and run
on CI against main:

0. Main merged (#768), `5d8c8d9b`.
1. and 2. Extents and integration, `833cfcf9`: a writer tries the turn
   without waiting; without it, it writes whole blocks into an extent of
   its process's, readable at once in every process; the next writer
   with the turn folds them into frames and retires the extent, which a
   sync point frees for the next claim. The turn is a lock on one byte.
   Format 11.
3. Cells, `61302917`: `Chronicle` provides `Resolve`, `Publish`,
   `Retract` and `List`; a cell is a record of two halves in a cells index
   of its own, updated under a lock on that cell alone, spilling to a
   block past its capacity. A repository keeps its branch heads in the
   file (`it_keeps_branch_heads_in_the_file`).
4. Retry, `9a19ebf1`: a plain branch commit that loses its race applies
   its instructions again over the winner, up to 64 times.
5. The last close, `afc44b0a`: the last process to close retires its
   extents and cuts free extents that end the file.

CI (run 13, against main): nothing slower; warm commands off the disk
1.10× to 0.99×.

**Where the build departs from the design, and why.**

- Readers check a block in an extent by its hash, not by a shared lock
  on the extent: as safe (a reused extent's bytes fail the hash, and the
  reader finds the block in the index), with no lock call per read.
- A process keeps one extent claimed while it writes, claimed in a turn
  it holds anyway: without it, the first write that found the turn taken
  waited to claim one, which a test showed.
- Creating a cell takes the turn; updates never do.
- Integration runs in the commit when the turn is free, not as a task of
  its own; compact output is appended at the end, and freed extents are
  reused by the next claims, not by frames.
- The last close cuts only free extents at the end of the file: a freed
  extent mostly sits mid-file, where reuse, not truncation, reclaims it.

**Open.** A repository test of two processes on two branches; integration
off the commit; a contact write that reads before it writes
(`remove_name`) is replayed as computed, by the registry's own retry as
before and by the commit's now.

## 2026-11-11, Verifying: a test for every case the design handles

Every concurrent case the design says it handles now has a test in
`rust/dialog-chronicle/src/archive/concurrency.rs` (the archive and its
cells), `src/archive/cells.rs` and `tests/repository.rs` (branches), and
each test was checked against the code broken on purpose: with the
guarantee it pins removed, it fails. Other processes are the test binary
run again in a role; children start together on a line from the parent;
a child that never says what it should fails the test after twenty
seconds instead of hanging it.

**Cases pinned.** A lone writer writes nothing into extents. Processes,
and threads of one process, append at once and every block reads back,
filed once, after reopening. A process that has never held the turn
writes while another holds it throughout. Another process reads blocks
still in an extent. A dead writer's extent is read, taken over without
losing its blocks, and folded in by the next writer. An extent retired
under a stale snapshot is not taken. A crash between folding and
retiring is finished by the next integration. An extent is reused only
after a sync point, and a reader that read it before reads the blocks
written over it. A frame not whole ends a walk, and is read once its
bytes arrive. A new catalog, and a full extent with none to take, wait
for the turn. Only the last process to close cuts the file. A file
reached by two names in one process is one handle. One of many creating
a cell wins; counting a cell up from three processes and three threads
loses no update; a swap and a retraction racing land one; a cell's lock
holds up that cell alone; a cell read while written is a value written,
never older than one read before. Two processes commit to two branches
while a third holds the turn throughout; three commit to one branch at
once and every commit lands.

**What the suite found, and what changed.**

1. *A process that had never held the turn waited for it on its first
   write*, and *opening the file took the turn*: the main tonk case, a
   command-line process committing while an app integrates, waited
   twice. A process holding the turn now leaves one extent listed that
   no process owns, and a process with none takes it, or a dead writer's,
   by its owner's byte; opening takes the turn only after a reboot or for
   a new file. An adopter checks the newest slot still lists what it
   took, and a claim takes the owner's byte before any slot lists it.
2. *Threads lost blocks.* An integration folding this process's extents
   took them off its idle list without marking them busy; another thread
   took one (the process's own lock does not stop its own threads) and
   wrote into it as it was retired. Every extent whose byte a process
   holds is now idle or busy, and a failed integration keeps them held.
3. *A reader could report no whole slot*: it read the file's length and
   then the slots, and two appends in between put both past the length.
   The slots are read first, and a read that catches both torn is tried
   again.
4. *A cell read could go backwards*: the first half read, written over,
   then the second read torn mid-write, returned the first's older value
   after a newer one had been read. A reader reads the first half again
   after the second and reads again if it changed, by read calls, not
   from the mapping, which the compiler may take as unchanging.
5. *Readers missed blocks*: a walk that stopped at a frame not yet whole
   recorded the whole count as read, and a reader could not tell an
   extent claimed again from the one it had read. A walk records how far
   it got; an extent carries the sequence of the slot that claimed it,
   and a reader that sees it change reads the extent again. The format is
   12.
6. *One file by two names was two handles in a process*: both would hold
   the turn, and closing either let go of every lock the process held on
   the file. Open files are known by device and inode.
7. *A transaction that lost its branch's race was refused*: the retry of
   stage four covered the repository's internal commits only, and the
   changelog claimed more. `transaction()...commit().publish()` now
   stages its writes again over the head that won; a batch its caller
   staged explicitly is still refused, as its caller read what it staged
   on.

A first counting test failed for a reason that is not a bug: a cell's
edition is its value's hash, and every memory store takes a publish of
the value a cell holds as done, so two increments to the same value were
one. The test writes values that differ.

**Open.** Integration off the commit; a write computed from a read before
the commit is applied again as computed (the claim above); more than one
process opening while the turn is held finds one extent to take, and the
next waits for the turn once.

## 2026-11-11, Building: transactions that run again, and an extent per process

Irakli, on the review of the suite: a transaction that loses its race
should run again, not reapply what it computed, through a new API, with
the old one deprecated and failing as it did; and an extent should be
left per process, not one, taken back as the process closes.

- `Branch::transact(|transaction| async { .. })` builds the transaction
  in a closure, called once per try on the head as it is then
  (`it_builds_a_transaction_again_over_the_head_that_won`: a second build
  reads what the winner wrote). `.commit().publish().perform(..)` is
  deprecated and fails with `VersionMismatch` on a lost race again
  (`it_refuses_a_one_shot_transaction_that_lost_its_race`). Code outside
  tests moved to `transact`, except the perf, soak and baseline harnesses,
  which keep measuring the one-shot commit the gates were recorded with:
  moving them is the maintainer's call.
- An open that finds the turn free leaves an extent no process owns; its
  close retires one. Four processes arriving together while the turn is
  held all write without waiting
  (`processes_arriving_together_while_the_turn_is_held_do_not_wait`), and
  ten opens and closes leave no more than were there
  (`extents_left_for_others_do_not_pile_up`). An extent left before
  blocks were appended stays mid-file when the last process closes,
  free, and is the next claimed: the file does not grow open after open
  (`it_cuts_free_extents_off_the_end_at_the_last_close`). Claims no
  longer write an extent's bytes, so an unused one is a hole.
  `Options::leave_extent` turns it off for a process that only reads.

## 2026-11-11, Verifying: power losses, played

`src/file/disk.rs` notes every write, change of length and sync the
archive makes to a watched file, and builds what a disk could hold had
the power gone after any of them: what the last sync made durable, then
each later operation dropped, kept, or (a write) kept in some of its
512-byte sectors, in any combination, which stands for the disk writing
them out of order too. `src/archive/power.rs` runs a workload on a
watched file (appends in the turn and into extents while another process
holds it, folds, a cell updated each round naming that round's blocks
and spilling every other round, a sync point every other round), then
opens 30 images for every point in what it wrote, as after a reboot.
Breaking recovery's check of what was written since the durable slot
makes it fail (blocks read back as other bytes); at three images a point
it did not, so it takes 30.

**Found and fixed.** A file whose making a power loss cut short did not
open ("not a dialog archive file", "no whole durable slot"). Opening
makes it again when it is no longer than its header and slots, and its
first bytes are unwritten, or this format's with no durable slot; any
other file is still refused.

**Holds.** The file opens, every block appended before the last sync
point reads back, no block reads back as other bytes: about 15,000
images over five seeds.

**Does not hold, open.** A cell is written in place without the turn or
a sync, so after a power loss:

- it can name blocks the file lost (190 of 378 images): its write reached
  the disk, the blocks' bytes did not, or did but the state recovery
  returns to (the durable slot) predates them. A sync before the cell's
  write is not enough, as two appends after it overwrite both rolling
  slots and recovery falls back to the durable one;
- it can hold a value older than the last sync point (1 image): the
  write to its stale half lost, the write over its synced half torn.

Both are pinned by `every_power_loss_leaves_a_cell_with_what_it_names`,
ignored until decided. Main has the first on `fs` too: block files and
the head's file are written without a sync. The candidates: a sync point
before every cell write (two syncs a commit, and the turn, which cell
writes were not to take); each half naming the slot sequence it needs,
recovery passing over a half the recovered state does not reach, with a
lock of its own for advancing the durable slot so cell writers do not
take the turn; or a sync after each cell write for the second alone.

**Windows and macOS** run in `.github/workflows/file-archive-platforms.yml`
on pushes to the crate. A file's identity on Windows is its volume and
file index (two names of one file canonicalize to two paths).

## 2026-11-11, Deciding and building: cells as SQLite keeps a commit

Irakli: copy what SQLite does. What SQLite does in WAL mode: a
transaction's pages and then its commit frame are appended to one log, a
recovery keeps the longest valid prefix of it, writers append under one
write lock, readers look for a newer commit as they start, and with
`synchronous=NORMAL` the log is synced at checkpoints, not per commit, so
a power loss can undo the last commits but never leaves a commit without
its pages.

So a cell is now a record appended per update, in the writer's turn,
after the process's own extents are folded in, and the cells' index files
the cell at its newest record; recovery's prefix (the slots, and every
record since the durable slot whole) keeps a head with what it names or
drops both. The two halves, capacities, spills and per-cell locks are gone.
A block another process holds only loose is written again into the
writer's own extent, as another's extent is folded only when it commits.
The format is 13.

What it costs is SQLite's: head updates take turns, and wait while
another process holds the turn (an integration folding a large extent
holds it longest). `it_commits_to_two_branches_from_two_processes_at_once`
pins that both branches' commits wait while a third process holds it and
land when it lets go; `a_cell_update_waits_for_the_turn_and_a_read_does_not`
pins the same for a cell. The requirement that writers of separate
branches never wait on one another is relaxed to SQLite's: they take
turns for one small append each.

**Verified.** The power-loss test now checks cells too and holds over
3,600 images a seed, five seeds; without the fold before the record, 203
of 3,600 images have a cell naming blocks the file lost.

**A bug the turn now carries found.** Counting one cell up from three
processes and three threads lost updates in a third of runs (179, once
93, of 180). `Turn` dropped this process's mutex before the file's lock:
another thread took the mutex, "took" the lock the process still held,
and the unlock that followed let it go under that thread, so another
process wrote at once. Block appends had it too, unseen, since no test
mixed threads and processes appending. The lock is now let go first;
15 runs of 15 count to 180.

## 2026-11-11, Windows and macOS: the first runs

The first run of `file-archive-platforms.yml` failed on both.

- **Opens waited for the turn** (macOS 6 tests, Windows 6): opening
  trusted the newest slot without the turn only when it was written in
  this boot, known from Linux's `boot_id` alone. The rule now needs no
  boot id: when another process holds the turn, the file is in use in this
  boot, so it was recovered since it was last left, by the first process
  to open it after, which found the turn free. Every open takes a recovery
  byte for its duration, and a repair holds it throughout, so an open never
  reads what is being repaired. All tests pass here with the boot id
  forced unknown.
- **The last close did not cut the file on Windows** (2 tests): Windows
  refuses to cut a file under a view past the cut. The closing process now
  maps only what it keeps before cutting. Dropping a torn tail in the turn
  is best effort: the next append writes from the slot's end anyway.
- **Children failed within 20 ms on Windows** (3 tests), saying why only on
  their error output, which the harness did not read. It does now; the next
  run says.

## 2026-11-11, Deciding: a head waits briefly (B), measured against A

Irakli, on waiting commits: "Maybe me saying do what SQLite does was not
well thought out"; then, given three options (A: what was built; B: a head
takes the turn only to append its record, naming the extents it depends
on; C: heads written to the writer's own extent, no turn): "At least B, C
seems better, but costs matter here".

SQLite has two ideas, and only one was needed: a commit trusted only with
everything before it in the log, and one writer at a time. B keeps the
first without folding inside the turn: a cell's record names each of the
writer's extents still holding blocks (record, epoch, bytes of frames),
and recovery takes it as whole only if every one the slot still lists in
that epoch holds whole frames that far. Built, format 14.

The power-loss test found one more case on the way: an extent's frames
reached the disk and its count, written after them, did not, so readers
did not find blocks a recovered head names. Recovery writes the count
when it takes such a record. Without the dependency check 203 of 3,600
images fail; without the count repair 3 to 9 do; with both, five seeds
pass.

**What a head waits, measured** (`measure_how_long_a_cell_update_waits`,
release build, this VM; each process appends 20 blocks of 4 KiB then
publishes its own cell, 200 times; two runs):

| publish | B, 1 process | B, 4 at once | A, 1 process | A, 4 at once |
|---|---|---|---|---|
| p50 | 44–48 µs | 0.12–0.66 ms | 87–97 µs | 0.15–0.18 ms |
| p90 | 86–88 µs | 6.2–7.9 ms | 141–170 µs | 6.6–8.9 ms |
| p99 | 4.4–5.3 ms | 11–14 ms | 4.3–4.6 ms | 16–23 ms |
| max | 5.2–5.8 ms | 13–18 ms | 5.2–5.4 ms | 21–27 ms |

B halves an uncontended publish and cuts the contended tail by a third.
The p90 under contention hardly moves: there a head waits behind the
other writers' block appends and sync points, which hold the turn for
milliseconds, not behind heads. Only C, or block appends that take the
turn less, removes that. A lone writer's p99 is a sync point's two
fsyncs inside the turn.

## 2026-11-11, Building: the turn held for laying out and writing only

Irakli agreed to the cheaper route before C. Three changes, measured on
the same harness (four processes, each 200 times: 20 blocks of 4 KiB,
then its own cell):

1. **An append prepares before its turn again.** The extents work had
   put splitting and encoding (most of an append's cost) back inside the
   turn, undoing `97abe73`. An append now prepares against the snapshot it
   has, and in the turn lays out against the newest, preparing again only
   when a node it split is held whole by then.
2. **A fold is gathered and prepared before the turn**, then laid out,
   written and retired in it.
3. **A sync point is made after the turn**: a commit that finds one due
   marks it; under a sync byte of its own (taken before the turn, never
   while holding it) the newest slot is read, the file synced, the durable
   slot written and synced; then, briefly in the turn, the newest rolling
   slot is told what pages and extents the durable slot freed.

Splitting the publish into waiting and the rest, with four writers:

| | p50 | p90 | p99 | max |
|---|---|---|---|---|
| waited for the turn | 0–0.2 ms | 1.3–2.0 ms | 2.9–5.5 ms | 4–8.5 ms |
| whole publish | 0.12–0.38 ms | 2.8–6.6 ms | 9.7–13.8 ms | 10.6–16 ms |

A writer alone never waits (p99 under 1 µs). What a publish takes beyond
its wait is mostly the writer making a sync point due after its own
turn: two syncs of about 4 ms each on this VM, in its own call.

**Found on the way.** Repository tests running together failed once in
ten with EDEADLK: the system refused a wait for the turn, seeing a cycle
of processes waiting on one another. The turn's holder waited for an
extent's owner byte when it claimed a free extent. It now only tries;
the turn's holder waits on no lock, so no cycle runs through it, and a
wait refused anyway (another file's locks, held by another thread of a
process, as POSIX locks are the process's) is waited again after a
millisecond. Fifteen runs of fifteen pass.

**Open.** A background thread for sync points would take them out of the
writer's call. C (heads written without the turn) would take the 1.3 to
2 ms p90 wait away; whether that is worth its cost is the question.

**A sync point never waits for the turn.** The first Windows run with the
change above hung. A writer that wrote into its extent because another
process held the turn then made a sync point that fell due (two seconds
had passed, which Windows' slower runners reach and the quick Linux runs
do not), and its last step waited for the turn the other process held. A
sync point now only tries the turn; when another holds it, the next commit
in the turn reads the durable slot and takes up what it freed.
`a_sync_point_due_while_another_holds_the_turn_does_not_wait` pins it,
and fails after 20 s with the wait put back.

**An extent retired twice.** `processes_append_to_one_file_at_once`
failed once on macOS: a block a writer appended, the writer closed
cleanly, did not read back. Opening with no boot id (as on macOS, which
reads the file whole on every first open and so changes the timing)
reproduced it on Linux in 2 to 3 of 300 runs. The newest slot listed
extents free twice, and one both free and in use: two writers could claim
it, and one wrote over the other's blocks. A fold gathers an abandoned
extent from the snapshot it has, which can be older than a fold another
process made of the same extent and retired; the second fold retired it
again. A fold now retires only the extents the newest slot still lists in
the epoch they were read in. Every slot a test writes is checked for an
extent listed twice: with the old retire put back, 216 of 240 runs trip
it; with the fix, 320 runs pass.

## 2026-11-12: defects a review found

A review on a branch stacked on this one
([#786](https://github.com/dialog-db/dialog-db/pull/786)) added failing
tests for seven defects. Each is fixed here, and its test now passes.

**A torn durable slot.** There was one durable slot. A sync torn by power
loss in the middle of writing it left no whole durable state, and opening
fell back to a rolling slot that may name pages the torn sync had freed.
Format 15 keeps two durable copies. A sync writes over the copy that does
not hold the newest whole state, and opening reads the newer whole copy.

**A reopen during the last close.** fcntl locks belong to the process,
and closing any descriptor of the file drops all of them. The last close
released PRESENT and then dropped the descriptor. Meanwhile a reopen in
the same process could take the shared `Inner` from the open table, and
it lost its locks when the old descriptor closed. The last close now holds
the open table until the descriptor is closed, and it stands down if a
reopen took a reference first.

**Extents piling up.** A process that left an extent on open (or one that
died) left it listed and empty. Nothing claimed it again, so every
crashed or briefly opened process added one. A process now holds a `left`
lock byte for the extent it left. An extent that is listed and empty, with
its owner byte and `left` byte both free, is an orphan. The next writer
that needs an extent takes the orphan before it claims a new one.

**Frames for a node in another layout.** With `verify` off, a frame was
written for a node whose rows realize to different bytes than the block
(a padded node, or a segment in a mixed layout, which another peer can
send). A read then served bytes that do not hash to the name. Frames are
now always checked, and `Options::verify` is gone. The test that put the
option there measured its cost as small next to the encode.

**Migration.** Three defects:

- `migrate_space` wrote a fresh file and renamed it over an existing
  `archive.dialog`, which dropped whatever a space had already written to
  it. It now writes into that file through `Chronicle` (see below for why
  not a copy).
- `unmigrated` took any `archive.dialog` to mean "migrated", but opening
  a `Chronicle` creates one. Migration now writes
  `archive.dialog.migrated` last, and that marker decides.
- With `remove_source`, the `fs` archive was deleted even when the
  transcode had left something out: a block whose bytes do not match its
  name, or a file that is not a block. The migration now fails instead,
  with the file written and the `fs` archive kept, and `strays` lists
  those files.

## 2026-11-13: the review's other findings

The review also listed findings it reasoned about without a test. Each is
addressed here, with a test except where noted.

**Durability on a timer.** A sync point was only ever made by a later
append, `Chronicle::sync` or the last close. A process that committed and
then sat idle could lose that commit to a power loss for as long as it
stayed idle. Now a thread per open file (holding the archive only while it
looks) makes a sync point once appends have waited `SYNC_AFTER` for one.
The thread and a handle being dropped take a mutex in turn, so the
thread's handle is never the last one and the last close always runs
where the user dropped the handle. Pinned by
`it_syncs_a_commit_no_append_follows`, which waits for the durable slot to
reach a lone commit's sequence.

**The slot's size against the freed-page lists.** The slot (64 KiB) lists
the index pages free to reuse and those freed since the last sync point,
4 bytes each. Measured by appending sequential 8-byte blocks:

| batch | file | reusable + pending | slot bytes |
|---|---|---|---|
| 50 000 | to 2 M blocks | under 2 700 | under 11 KB |
| 1 000 000 | 2 M | 2 993 | 12 145 |
| 1 000 000 | 4 M | 6 945 | 27 941 |
| 1 000 000 | 6 M | 9 496 | 38 145 |

With batches of 50 000 the lists stay flat as the file grows: the tree
buffers inserts, so one commit rewrites a bounded number of pages. With
batches of a million they grow with the file, and would outgrow the slot
somewhere past 12 million blocks. A commit whose slot does not fit fails
and leaves the file as it was, but an append that size would fail every
time it was tried. An append now commits at most 32 Ki blocks at a time
(`COMMIT_BLOCKS`). A fold of extents is not split: an extent holds 1 MiB,
and a fold takes what the listed extents hold.

**The table of open files held across recovery.** `open_with` held the
process-wide table while it recovered the file, which after a power loss
can read everything written since the last sync point. Every other open,
and every last close, in the process waited for it. Now opens of one file
wait for each other through a gate keyed by the file (or, for a file not
made yet, its path), and the table is taken only to look up and register.
Pinned by `it_opens_another_file_while_one_is_recovered`, which holds one
file in recovery through a test hook and opens another meanwhile.

**Blocking I/O in async providers.** `Put` and `Import` ran the append on
the caller's thread: waiting for the writer's turn another process holds,
and the syncs of a sync point. On a runtime with one thread, that stalls
every other task. When a tokio runtime runs the caller, they now append on
its blocking threads (the `fs` provider goes through `tokio::fs`, which
does the same). `Get` stays inline: it reads the mapping and takes no
lock. Pinned by `it_waits_for_the_turn_off_the_runtime`, in which another
task of a one-thread runtime must run while the import waits. It fails
after 5 s with the append put back inline.

**Locks let go by another descriptor's close.** fcntl locks belong to the
process, and closing any descriptor of the file lets go of all of them.
The archive opens a file once per process, but the migration copied an
open `archive.dialog` with `std::fs::copy`, which opens and closes a
descriptor of it. The migration now writes into an existing file through
`Chronicle`, which shares the process's handle, so nothing in the
workspace opens the file beside the archive. Pinned by
`it_migrates_into_a_file_a_process_has_open`. Linux's
open-file-description locks would not be let go of, but they would make
Linux the one platform where two handles in a process exclude each other,
and macOS has no equivalent, so the archive keeps fcntl locks.
