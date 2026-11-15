# A read follows the head it should have pinned

Phase: **Building.** See the [design index](README.md) for what the
phases mean, and the [journal](query-isolation/journal.md) for where the
topic stands and what the harness found.

## Description

A branch keeps its head in a memory cell that every handle of the branch
shares. On main (`427546f`), every read of the branch took its tree root
from that cell at the moment of the read: a `Select` asked `branch.revision()`
when it built its tree, a query environment asked it once per scan, a
transaction's query asked it once per query, and a transaction's commit
asked it once more for the base it minted on and once for the version its
publish CAS'd against. One query performs many scans (a conjunction looks
its second premise up per row of the first, a resolver re-reads a
reference per outer row); a transaction performs many queries; a commit's
settlement reads each cell a choosing write touches and its induction
reads each rule body's premises.

dialog-db/dialog-db#757 reported the symptom: a commit through another
handle of the branch, landing while a query ran, moved the root under the
query, which then read some of its premises at one revision and the rest
at another and yielded rows no revision held.

The first fix (the first two commits of #758) pinned the owned `Source` a
query environment holds, which closed `Branch::query`, subscriptions and
the reads a commit makes through a query environment. It left the
borrowed `SourceRef` serving both reading and committing, so a
transaction's two queries could still read two revisions, the commit
could take a base from a third read of the head, `Select::new` still read
live, and nothing stopped a new read path from being written against the
live head.

## Problem

The code did not distinguish *the line* (a branch or a snapshot, where
its head is now, with its caches, overlay and tracking) from *the line at
a revision*. One type, `SourceRef`, answered both "what is the head" and
"what do I read", so whether a read path was isolated depended on which
of its calls happened to go through the pinned owned form, and the
compiler could not tell a read that pinned from one that followed the
head. The unit of isolation (a query, a select, one poll of a
subscription, a transaction from open to commit) had no representation.

## Approach

Decided by the maintainer, 2026-11-09, as the review of #758: prefer the
stronger guarantee over the smaller diff, and test as much as possible.

- **Two types.** `Live<'a>` is the live handle: `&Branch` or `&Snapshot`.
  It reaches what every handle shares and reads live by design (identity,
  caches, session overlay, tracking) and no tree root. `View<'a>` is a
  `Live` pinned at a revision (`Option<Revision>`, `None` for a branch
  with no commits, which reads as the empty tree). Every read API takes a
  `View`: `Select`, the query environment, history and log, blob and
  asset reads, induction, settlement, the oracle. `Live::pin()` is the one
  place a revision is taken from a live head; a transaction builds its
  views on the revision it holds with `View::at`. `Source`, the owned
  form the query environment holds, is a `View` that owns its line, and
  borrows back as a `View` per scan without copying the revision.
- **A transaction pins when it opens.** `Transaction<L: Line>` holds
  `L::Pin`: for `&Branch`, the head cell's edition (revision and cell
  version, one read of the cell); for `&Snapshot`, its revision; for a
  `TransactionBatch`, nothing, since the batch owns its snapshot. Every
  query reads the pin, the commit settles, induces and mints on it, and a
  branch transaction's publish CAS's the cell against the version it
  read. A head that moved is refused with `VersionMismatch` at commit,
  before the settlement runs (the early word), and at publish (the
  guarantee).
- **A subscription pins per poll**, as before: a standing query reads one
  revision within a poll and the branch's current revision at the next.
- **`Snapshot` stays a kind of `Live`.** Making a snapshot a `View` that
  owns its caches would move the caches out of `Branch`'s fields into a
  `Caches` value and rework how the staged chain's snapshot mints; that
  balloons past this change. Reads treat the two lines identically
  through `View`; the `match` on the kind survives only where a branch
  has what a snapshot lacks (tracking, the induction watermark, the head
  cell).

### What the settlement and conflict detection assume about the base

Read before changing the base (`transaction/batch.rs`,
`transaction/succession.rs`, `transaction/induce.rs`, `ReadObservation`
in `staged.rs`, `QueryEnv::observation` in `session.rs`):

1. **The settlement's edition is the base's successor.** `settle` creates
   a `ReadSettlement` at `QueryEnv::pending_edition()`, the first line's
   revision's successor, and the mint versions the commit at
   `base.edition.successor()`. On main both came from live reads of the
   head at different moments; they agreed only when nothing moved in
   between. With the view, both come from the pin.
2. **The settlement memo is keyed by what the read observed.** A read of
   the transaction settles a prefix of its log and keeps the settlement
   under a `ReadObservation` (every line's head, every overlay's
   revision). The commit reuses it only when its own observation matches,
   else it settles again from scratch. On main a head that moved between
   the read and the commit made the memo miss and the commit settle
   against the new head: the commit then applied what the transaction had
   not read. With the view the commit's observation is the read's, so the
   memo hits and the commit applies what the transaction read, which is
   what the memo was for.
3. **Nothing detects a conflict by comparing the base with the head at
   commit time.** The only conflict detection is the publish CAS
   (`Checkpoint::publish` against the cell version captured at
   checkpoint), the no-op path's re-read of the cell version, and, for a
   `Commit` asked to `merge`, `merge_with_winner` on a lost CAS. None of
   them reads the base again. So pinning the base contradicts no
   assumption, provided the CAS's expected version is captured *with* the
   base: a base pinned at open with a checkpoint taken at commit would
   publish a revision built on the old head over the new one, under an
   edition the new head already took (an equivocation). `Line::pin` for
   `&Branch` therefore reads the cell's edition once, revision and
   version together, and `Cell::checkpoint_at` publishes against that
   version.
4. **The induction lag is computed against the base.** `lag_delta` diffs
   the watermark's tree against the head's. On main the head was live; now
   it is the pin. A watermark ahead of the pin (another writer induced
   past it) is the moved-head case, which the commit refuses before it
   computes the lag; under a race between the check and the mint the lag
   would be a reversed diff, induction would run over it, and the publish
   would then refuse the result. Nothing lands.
5. **`merge` is for raw `Commit`s, not transactions.** A transaction's
   commit never merges with a winner; a moved head is a refusal. That is
   unchanged, and it is what makes "refuse, do not rebase" the only
   outcome to pin by test.

No assumption found required stopping to ask; the base change was built.
