# A replica that holds every block still pays to warm it: direction

Decision: what lets a speculative walk tell that it has nothing to fetch,
without withholding a walk a partial replica needs. For: a walk over a
tree whose every block is local can fetch nothing and is run anyway,
because nothing records that a tree is wholly local.

Decided 2026-11-12: column D, by the agent, for the maintainer to read
cold (journal, 2026-11-12). It rests on hypotheses H1 to H3 in the
[claims ledger](claims.md), and leaves H4 open: whether tonk's walks are
on trees D can ever record.

| criterion \ approach | A. Status quo: walk whenever the repository tracks a remote | B. Memo of walks: skip a (root, range) already walked to the end | C. Demand first: run speculation only while the evaluation's stream is pending | D. Root record: skip a walk whose root is recorded wholly local; commits on a recorded or empty tree and full downloads record | E. Subtree record: the walk stops at any node recorded as heading a wholly local subtree | F. Fewer hints: coalesce an evaluation's point hints into fewer ranges |
|---|---|---|---|---|---|---|
| a member whose tree was built here, adopted by a local pull (`query-member`) | 🔴 503 walks over 1,020 cached nodes, about 8% of the phase | 🟡 the first walk of each (root, range) runs; #767 says point probes rarely repeat, so most of the 503 remain; not measured | 🟢 walks run only on pending polls, which a local evaluation rarely has | 🟢 no walk (H2): main's commits from an empty tree each record their tree, and the member adopts main's last tree by hash | 🟢 one lookup per hint at the root, given the same writers as D at the root | 🟡 fewer walks over the same nodes, never none |
| a partial replica's hinted range (`it_fetches_a_hinted_range_a_partial_replica_lacks`, `it_resumes_a_hint_a_dropped_driver_did_not_finish`) | 🟢 walks and fetches: both tests pass | the first walk fetches as today; a repeat is skipped, and was local by then | 🔴 prefetch waits for the first stall; measured in #770 | 🟢 as A: both fixtures adopt by `MinimalRanges`, so no root is recorded and the walk runs | 🟢 walks until a recorded subtree, fetches the rest; the fixtures record nothing | 🟡 a coalesced range covers keys no probe asked for, and its walk fetches them; ? how many |
| what a wrong answer costs | nothing to be wrong about | an entry names blocks a finished walk left local, and the archive has no delete: it cannot become wrong, only unused | no record | a wrongly recorded tree skips walks; the demand read fetches what it needs, as with speculation off; no wrong result while only the walk reads the record | the same as D, per subtree | no record |
| a head adopted by reference (catchup and join clients) | walks; the soak baselines are this | as A for each first walk | 🔴 rejected in #770: delays the prefetch catchup measures | 🟢 as A: an adopted tree is not recorded (H3) | walks stop at subtrees shared with an earlier recorded tree; fetches are the same, local reads fewer; ? whether soak counts move | 🟡 what the client fetches ahead changes, so soak numbers move |
| a partly local replica (tonk's members, pulled by `QueriedRanges` per [held-across-pull](../held-across-pull.md)) | walks every hint; walks over held regions fetch nothing | as for the member | as for the partial replica | 🟡 no help: a tree a pull adopts or grafts is never recorded; whether tonk's walks are on such trees is H4 | 🟢 subtrees recorded before a pull keep their hashes after it, so a walk descends only where the upstream changed; ? how a replica that never downloaded everything comes to record a subtree | 🟡 fewer walks, the same nodes |
| a member after a restart | walks | the memo is gone; walks | no state | 🟡 in memory, walks until a full download records its tree: [design-persistence.md](design-persistence.md) | 🟡 the same, with far more entries to persist | no state |
| what is written, and by whom | nothing | one entry per completed walk, by the driver | nothing | one tree hash per commit on a recorded or empty tree, and per full download: two writers | 🟡 a hash per recorded subtree, at every node mint, at download, and by any walk that covered a subtree with nothing absent: every path that writes a tree is a writer | nothing; each evaluator's hinting changes |
| the catchup soak's "the first poll's hints should drain" | holds | holds | 🔴 broken (#770) | holds: a skipped walk pops its hint and ends | holds | holds |
| what it adds | nothing | a bounded memo per driver or per environment | a check in `Driven::poll_next` | a small set per repository beside the routes; two write sites, one read site | 🔴 a write at every node mint, a rule that records a node when all its children are recorded, a set sized by the tree rather than by its heads | changes in the probe pipelines and merge joins that hint; ? whether speculation keeps its lead time |

Reading the matrix:

- D is the only column that removes the scenario's walks without changing
  anything a partial replica or an adopted head does. Its one way of being
  wrong is a skipped walk, which costs what switching speculation off
  costs and no more.
- E dominates D on the partly local replica, and costs a writer on every
  path that makes a tree. That cost is worth paying only if tonk's walks
  are on partly local trees (H4). D's record is E's record at one node, so
  building D does not close E off: E adds writers and moves the lookup
  from the root into the descent.
- F is not an alternative to D but a complement: it reduces walks
  everywhere, local or not, and changes what a partial replica fetches.
  It is its own topic if wanted.
- B and C are kept so they are not proposed again: B was rejected in #767,
  C in #770.

Open questions the matrix does not settle:

- `?` H4: whether tonk's member branches that showed about 18.5K walks
  per query are wholly local, partly local, or adopted by reference. It
  decides whether D does anything for tonk at all, and whether E or the
  persistence step comes next.
- `?` Whether a scoped download (`Operational`, `Ranges`) should record a
  (tree, scope) pair: every hint's range lies in the data orders, which
  `Operational` covers, so a profile branch would qualify.
- `?` Whether a pull's replay arm, which rewrites the local tree with what
  the upstream changed, leaves a recorded base's successor wholly local.
  If H1 holds for it, it is a third writer.
