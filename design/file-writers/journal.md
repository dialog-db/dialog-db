# Journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | Two processes advancing two branches of one space share no lock on main and take turns on one file lock with the file archive. Irakli requires that they not wait on one another. Cells live outside the file, a file each. | Directing: a matrix of four approaches, Irakli's candidate among them. |
| **Agenda** | Whether the web is in scope for this decision (every column but the owner's is open or blocked there). Whether "one file" means one file at rest, with side files while writers overlap. How a head's compare-and-swap works against a pending head record. The `?` cells in the matrix. | Ask who integrates; then the web; then the cells question. |

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
