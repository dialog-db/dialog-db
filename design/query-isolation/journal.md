# A read follows the head it should have pinned: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | *What do we know?* Every read of a branch took its root from the shared head cell at the moment of the read, so a commit through another handle moved the root under a query, a transaction or a commit's settlement. The crate had one type for "the line" and "the line at a revision". Pinning per owned `Source` (the first fix) closed the query environment and left the borrowed paths open. | *Where are we at?* Building: the split into `Live` and `View` is in, transactions pin at open and commit against the pin, the harness sweeps every read path, and the harness has been run against main. |
| **Agenda** | *What do we need to know?* Why `commit-succeed` moved +6.1% (volatile, not gated) against main. Whether a snapshot should become a `View` that owns its caches (left as a `Live` kind for now). Whether `lag_delta` should refuse a watermark that is not an ancestor of the pin, rather than diffing against it (only reachable in the race window the publish closes). | *Where are we going?* Landing #758, then the perf sweep against main before the final push. |

## 2026-11-09, Delimiting: what the first fix left open

**Asked.** The maintainer reviewed the first fix (pin the owned `Source`,
which the query environment holds) and decided: prefer the stronger
guarantee over the smaller diff, and test as much as possible.

**Learned.** `SourceRef`, the borrowed line, served reading and
committing alike. `Transaction::<&Branch>::query()` built each query from
the live `SourceRef::Branch(self.line)`, so two queries of one
transaction pinned twice, and the commit took `base = branch.revision()`
a third time. `Select::new(branch)` read `source.root()` live, correct
only because one scan takes one root. Nothing in the types stopped a new
read path from being written live.

**Decided.** Pin at the unit of isolation and let the types carry it: a
`View` for every read, a `Live` for what needs the head, a transaction
that pins when it opens and commits against its pin.

## 2026-11-10, Building: the split, the transaction pin, the harness

**Learned, about the base.** The settlement and the conflict detection
assume nothing about the base that pinning it contradicts, with one
condition: the version the publish CAS's against must be captured with
the base, in one read of the cell. A base pinned at open and a
checkpoint taken at commit would publish a revision built on the old
head over the new one, under an edition the new head already took.
`Line::pin` for `&Branch` reads the cell's edition once;
`Cell::checkpoint_at` publishes against that version. The settlement
memo, keyed by the heads a read observed, was built for the commit to
reuse what the transaction read; on main a moved head made it miss and
the commit settle against the new head instead. The details are in the
topic file.

**Learned, about `lag_delta`.** It diffs the watermark against the head.
With the head pinned, a watermark ahead of the pin (another writer
induced past it) is the moved-head case. The commit refuses that case
before computing the lag; in the race between the check and the mint the
lag would be a reversed diff and the publish would refuse the result.
Left as is, noted on the agenda.

**Learned, about the existing suite.** Every existing test of the
repository crate (695 with the new ones) passes with transactions pinned
at open and refused on a moved head:
nothing in the crate opened a transaction, moved the head through the
same handle, and expected the commit to build on the moved head.

**Built.** `Live<'a>` and `View<'a>` in `source.rs`, `Source` as the owned
view; every read path on `View`, `Commit` and `BlobArchive` on `Live`,
pinning per operation; `Transaction<L: Line>` with `L::Pin`; the
`Interposing` env in `helpers.rs`, which runs a commit through another
handle of the branch under the k-th tree node read or the k-th blob load
of the path under test; a sweep of k over the first 40 reads of each kind
for ten paths (`Branch::query`, `Branch::select`, `claims().select()`,
two queries of one transaction, a staged commit's induction, a
transaction's settlement, the oracle's reference read, one subscription
poll, a snapshot query as the control, and the log), each asserting the
rows equal the uninterposed run; the transaction base tests (refused with
`VersionMismatch`, the head stays where the other commit put it, nothing
of the transaction lands; the staged form and the empty transaction too;
the snapshot transaction likewise); and the observation test (the heads a
`ReadObservation` records are the pinned ones, the pending edition is the
pin's successor).

**Harness on main** (`427546f`, the harness and the ten paths ported to
main's `SourceRef`, 40 reads of each kind per path, reporting rather than
asserting). Under the k-th tree node read, the commit landed and the
path read another revision at:

| path | node read: commit landed under | node read: another revision at | blob load: landed under | blob load: another revision at |
|---|---|---|---|---|
| `query` | 1..40 | every k | 1..5 | every k |
| `select` | 1..40 | every k | 1..5 | every k |
| `transaction` (two queries) | 1..40 | every k | 1..5 | every k |
| `subscription` (one poll) | 1..40 | every k | 1..5 | every k |
| `induction` (a staged commit) | 1..40 | every k | none | none |
| `succession` (a settlement) | 1..25 (all it reads) | 1..21 | none | none |
| `oracle` | 1..21 (all it reads) | 1..17 | none | none |
| `claims` (one scan) | 1..4 (all it reads) | never | none | none |
| `snapshot` (the control) | 1..40 | never | 1..5 | never |
| `log` | none (record memo) | never | none | none |

Every multi-scan path read another revision on main at every point the
commit could land under, the settlement and the oracle at every point but
their last few reads (by then every cell had been read), and under every
spilled-value load the paths that read biographies. The single-scan
paths and the snapshot held, as the first fix's analysis said. On the
branch the same sweep reads the pinned revision at every k for every
path (`reads_one_revision_under_a_commit::*`).

The first run fired no blob load at all: the spilled-value cache is
shared by every line the environment holds and keyed by content, and the
fixture's biographies were identical in every repository, so after the
first fixture they were read from the cache. The biographies now carry
the repository's DID, which is the run above.

## 2026-11-10, Building: perf against main

Swept both on this machine (`dialog-perf sweep`, callgrind), then
compared main to the branch. Every gated scenario is within tolerance and
no counter moved. The point reads and commits:

| scenario | main | branch | change |
|---|--:|--:|--:|
| read-point | 22,657,849 | 22,421,088 | -1.0% |
| number-point | 33,289,092 | 33,435,655 | +0.4% |
| rule-point | 51,290,704 | 50,950,321 | -0.7% |
| commit-rows | 197,496,154 | 197,664,069 | +0.1% |
| commit-batch | 316,517,759 | 316,836,486 | +0.1% |
| commit-hot-key | 194,935,960 | 195,123,089 | +0.1% |
| commit-derived | 118,332,720 | 118,027,401 | -0.3% |
| commit-succeed | 1,353,186,862 | 1,435,531,995 | +6.1% |

`commit-succeed` is marked volatile and not gated; it is the scenario
whose commits run the settlement this change pins, so its move is on the
agenda to look at rather than to dismiss as noise.
