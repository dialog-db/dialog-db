# A replica cannot read or write what it held once a pull moves its head: journal

## Reflective inquiry

|  | Understanding | Activity |
|---|---|---|
| **Status** | A head adopted by root leaves every changed leaf by reference; reads and writes of keys the replica held then fetch. Measured in dialog for reads and for repeat writes. `Download::QueriedRanges` removes both, and leaves a first write to a key only read fetching as before. | Building. Column E is decided and built: `pull(Download::QueriedRanges)`, `Pull::retry`, with tests over a remote. |
| **Agenda** | Whether the reported slow load, the slow edits and the pulsing dot in tonk are this problem (two open hypotheses). What `Download::QueriedRanges` costs on the Perf and Replication scenarios, which do not ask for it yet. Whether a spilled value a changed leaf references should come along. | The pull request's gates. Tonk moves to this dialog, pulls its spaces with `Download::QueriedRanges` and a retry, and the two tonk hypotheses are tested in the browser. |

## 2026-11-10, Diagnosing: from a slow load to what a pull leaves behind

**Asked.** Why does a space that was loaded before load slowly on a slow
link, and not at all offline? The maintainer's hypothesis was that load
waits on sync.

**Learned.** Three hypotheses, taken in turn in tonk's browser build with
latency injected at TCP level:

- Load waits on sync. Refuted: with every block local, load time was the
  same on a fast link, at 600 ms and 2 s round trips, with the service
  refusing and with it hanging.
- The page's assets or the page-to-worker traffic are throttled. Refuted
  for a space with no account: production loaded in 0.8 s warm and 1.5 s
  cold, the same under emulated Fast 3G and offline.
- The replica's head names blocks it does not hold. Confirmed: after
  another device changed the space and this one pulled while showing
  another page, the page took 13.8 s at 600 ms round trips and failed
  with the network down, on `Storage error: block ... missing`.

The maintainer's expectation, that live subscriptions fetch whatever is
missed, holds for a page that stays open. It fails for a key that no query
is reading when the head moves, and for writes, which read leaves a query
did not.

**Decided.** Not a decision of approach. A first change in tonk, fetching
everything by which the two heads differ, was built and withdrawn by the
maintainer: it fetched 44 blocks where the query needed 7.

## 2026-11-10 to 2026-11-11, Directing: what brings held keys along

**Asked.** What should be fetched, and where should that live? The
maintainer: only what is needed to answer queries; found by descending the
subtrees the replica holds; in dialog; the head switches only when that is
local, and a write made meanwhile is reconciled as a pull already does.

**Learned.**

- A write reads the leaves it writes to, in the entity and attribute
  orders and in history. So "what was queried" does not cover what a write
  needs; "what the replica holds" covers a write to a key written before,
  and not a first write to a key only read.
- In dialog a commit made during a pull fails the pull, and the caller
  pulls again. The reconciling is the caller's.
- `land` can prepare an upstream again under the write lock. With the
  follow inside `prepare_upstream`, that prepare can fetch. Open.

**Decided.** Not decided. Column D was built before the matrix existed,
because the maintainer asked for the behaviour and the measurements the
matrix needed did not exist without it. The decision of column is still
the maintainer's, and it rests on two open hypotheses: that the gates
accept D's cost, and that tonk's symptoms are this problem.

**Backtracked.** From Building to Directing: the code came first. The
matrix was written after, with the status quo and the alternatives the
code had skipped.

## 2026-11-11, Directing to Building: the caller asks

**Asked.** Should every pull follow what is held (D), or should a caller
ask for it (E)? Is there a good reason a pull fails when a commit
overtakes it?

**Learned.** The maintainer: how much a pull downloads should be an
explicit decision of the caller; switching what a plain pull does is not
wanted. A pull overtaken by a commit should be able to rebase until it
lands, by a modifier on the pull.

Nothing in dialog needs a pull to fail when overtaken: `land` already
prepares again under the write lock for a second upstream of one pull.
What a failed pull gives a caller is the choice of when to try again,
which a limit expresses.

With the fetch in a download, not in `prepare_upstream`, the open risk of
a fetch under the write lock is gone: `land`'s second prepare follows
nothing.

**Decided.** Column E, by the maintainer. `Download::overlapping` beside
`within` and `operational`; `Pull::retry(Limit)`, default no retry. The
row "can be forgotten by a caller" is the cost accepted: tonk has to ask
in each place it pulls a space, and it forgot once for the account
branch. Rests on the two open tonk hypotheses.

## 2026-11-12, Building: the extent is the pull's argument

**Asked.** Is a modifier on a pull download (`pull().download().overlapping()`)
explicit enough?

**Learned.** The maintainer: no. A modifier is easy to miss, and missing
it is how the cheap answer became the one callers got. The decision should
be forced: `pull(Download::…)`, with the old pull download removed.

**Decided.** `Branch::pull`, `Branch::download` and `Branch::replicate`
take a `Download`; `Pull::download` and `PullDownload` are removed. The
download runs between the merge and the publish inside `perform` and
`prepare`, outside `land`, so the second prepare `land` makes under the
write lock still fetches nothing. This answers the matrix row "can be
forgotten by a caller" for column E: a caller cannot leave it out, only
choose wrongly.

The variant names are the maintainer's. `QueriedRanges` also covers the
ranges a replica wrote, which the name does not say; left as named.
